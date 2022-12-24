/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.parquet;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import io.airlift.slice.BasicSliceInput;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.ParquetDecodingException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;

public class BloomFilterStore
{
    // since bloomfilter header is relatively small(18 bytes when testing) we can read in a larger buffer(BlockSplitBloomFilter.HEADER_SIZE*4 in this case)
    // and get actual bytes used when deserializing header in order to calculate the correct offset for bloomfilter data.
    private static final int MAX_HEADER_LENGTH = BlockSplitBloomFilter.HEADER_SIZE * 4;

    @Nullable
    private Map<ColumnPath, BloomFilter> bloomFilterStore;
    private final ParquetDataSource dataSource;
    private final List<BloomFilterHeaderMetadata> bloomFilterHeaderReferences;

    public BloomFilterStore(ParquetDataSource dataSource, BlockMetaData block, Set<ColumnPath> columnsFiltered)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        requireNonNull(block, "block is null");
        requireNonNull(columnsFiltered, "columnsFiltered is null");

        ImmutableList.Builder<BloomFilterHeaderMetadata> bloomFilterOffsetMapBuilder = ImmutableList.builder();
        for (ColumnChunkMetaData column : block.getColumns()) {
            ColumnPath path = column.getPath();

            if (hasBloomFilter(column) && columnsFiltered.contains(path)) {
                bloomFilterOffsetMapBuilder.add(new BloomFilterHeaderMetadata(path, column.getBloomFilterOffset()));
            }
        }
        this.bloomFilterHeaderReferences = bloomFilterOffsetMapBuilder.build();
    }

    public Optional<BloomFilter> getBloomFilter(ColumnPath columnPath)
    {
        if (bloomFilterStore == null) {
            bloomFilterStore = loadBloomFilters(dataSource, bloomFilterHeaderReferences);
        }

        return Optional.ofNullable(bloomFilterStore.get(columnPath));
    }

    public static boolean hasBloomFilter(ColumnChunkMetaData columnMetaData)
    {
        return columnMetaData.getBloomFilterOffset() > 0;
    }

    private static boolean bloomFilterSupported(ColumnPath columnPath, BloomFilterHeader bloomFilterHeader)
    {
        int numBytes = bloomFilterHeader.getNumBytes();
        if (numBytes <= 0 || numBytes > BlockSplitBloomFilter.UPPER_BOUND_BYTES) {
            throw new ParquetDecodingException(String.format("Column: %s has bloom filter number of bytes value of %d, which is out of bound of lower limit: %d and upper limit: %d", columnPath, numBytes, 0, BlockSplitBloomFilter.UPPER_BOUND_BYTES));
        }
        return bloomFilterHeader.getHash().isSetXXHASH() && bloomFilterHeader.getAlgorithm().isSetBLOCK() && bloomFilterHeader.getCompression().isSetUNCOMPRESSED();
    }

    private static Map<ColumnPath, BloomFilter> loadBloomFilters(ParquetDataSource dataSource, List<BloomFilterHeaderMetadata> bloomFilterHeaderMetadata)
    {
        ListMultimap<ColumnPath, DiskRange> bloomFilterHeaderRanges = ArrayListMultimap.create(bloomFilterHeaderMetadata.size(), 1);
        for (BloomFilterHeaderMetadata column : bloomFilterHeaderMetadata) {
            bloomFilterHeaderRanges.put(column.path(), new DiskRange(column.headerOffSet(), MAX_HEADER_LENGTH));
        }
        Multimap<ColumnPath, ChunkReader> headerChunkReaders = dataSource.planRead(bloomFilterHeaderRanges, newSimpleAggregatedMemoryContext());

        ListMultimap<ColumnPath, DiskRange> bloomFilterDataRanges = ArrayListMultimap.create(bloomFilterHeaderMetadata.size(), 1);

        try {
            for (BloomFilterHeaderMetadata column : bloomFilterHeaderMetadata) {
                BasicSliceInput sliceInput = getOnlyElement(headerChunkReaders.get(column.path())).readUnchecked().getInput();
                BloomFilterHeader bloomFilterHeader = Util.readBloomFilterHeader(sliceInput);

                if (bloomFilterSupported(column.path(), bloomFilterHeader)) {
                    long headerSize = sliceInput.position();
                    long bloomFilterDataOffSet = column.headerOffSet() + headerSize;

                    bloomFilterDataRanges.put(column.path(), new DiskRange(bloomFilterDataOffSet, bloomFilterHeader.getNumBytes()));
                }
            }
        }
        catch (IOException exception) {
            throw new RuntimeException("Failed to read Bloom filter header", exception);
        }
        finally {
            headerChunkReaders.values().forEach(ChunkReader::free);
        }

        Multimap<ColumnPath, ChunkReader> dataChunkReaders = dataSource.planRead(bloomFilterDataRanges, newSimpleAggregatedMemoryContext());
        try {
            return bloomFilterHeaderMetadata.stream()
                    .filter(column -> dataChunkReaders.containsKey(column.path()))
                    .collect(toImmutableMap(
                            BloomFilterHeaderMetadata::path,
                            column -> new BlockSplitBloomFilter(getOnlyElement(dataChunkReaders.get(column.path())).readUnchecked().getBytes())));
        }
        finally {
            dataChunkReaders.values().forEach(ChunkReader::free);
        }
    }

    private record BloomFilterHeaderMetadata(ColumnPath path, long headerOffSet)
    {
        private BloomFilterHeaderMetadata(ColumnPath path, long headerOffSet)
        {
            this.path = requireNonNull(path, "path is null");
            this.headerOffSet = headerOffSet;
        }
    }
}
