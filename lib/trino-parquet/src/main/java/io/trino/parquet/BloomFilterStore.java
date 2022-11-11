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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import io.airlift.slice.BasicSliceInput;
import org.apache.hadoop.hive.common.jsonexplain.Op;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.parquet.reader.MetadataReader.getExpectedFooterSize;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class BloomFilterStore
{
    private Map<ColumnPath, BloomFilter> bloomFilterStore;
    private final ParquetDataSource dataSource;
    private final long rowGroupBloomFilterEnd;
    private final List<BloomFilterHeaderMetadata> bloomFilterHeaderReferences;

    public BloomFilterStore(ParquetDataSource dataSource, ParquetMetadata parquetMetadata, BlockMetaData block, Set<ColumnPath> columnsFiltered)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        requireNonNull(parquetMetadata, "parquetMetadata is null");
        requireNonNull(block, "block is null");
        requireNonNull(columnsFiltered, "columnsFiltered is null");

        rowGroupBloomFilterEnd = getRowGroupBloomFilterEnd(dataSource, parquetMetadata, block);
        ImmutableList.Builder<BloomFilterHeaderMetadata> bloomFilterMetaDataBuilder = new ImmutableList.Builder<>();
        for (ColumnChunkMetaData column : block.getColumns()) {
            ColumnPath path = column.getPath();

            if (bloomFilterEnabled(column) && columnsFiltered.contains(path)) {
                bloomFilterMetaDataBuilder.add(new BloomFilterHeaderMetadata(path, column.getBloomFilterOffset()));
            }
        }
        bloomFilterHeaderReferences = Ordering.natural().sortedCopy(bloomFilterMetaDataBuilder.build());
    }

    public Optional<BloomFilter> readBloomFilter(ColumnPath columnPath)
    {
        if (bloomFilterStore == null) {
            bloomFilterStore = loadBloomFilters(dataSource, bloomFilterHeaderReferences, rowGroupBloomFilterEnd);
        }

        return Optional.ofNullable(bloomFilterStore.getOrDefault(columnPath, null));
    }

    public static boolean bloomFilterEnabled(ColumnChunkMetaData columnMetaData)
    {
        return columnMetaData.getBloomFilterOffset() > 0;
    }

    private static boolean bloomFilterSupported(BloomFilterHeader bloomFilterHeader)
    {
        int numBytes = bloomFilterHeader.getNumBytes();
        if (numBytes <= 0 || numBytes > BlockSplitBloomFilter.UPPER_BOUND_BYTES) {
            return false;
        }
        return bloomFilterHeader.getHash().isSetXXHASH() && bloomFilterHeader.getAlgorithm().isSetBLOCK() && bloomFilterHeader.getCompression().isSetUNCOMPRESSED();
    }

    private static long getRowGroupBloomFilterEnd(ParquetDataSource dataSource, ParquetMetadata parquetMetadata, BlockMetaData readingBlock) {
        Optional<Long> rowGroupBloomFilterStartOptional = getRowGroupBloomFilterStart(readingBlock);
        long rowGroupBloomFilterStart = rowGroupBloomFilterStartOptional.get();

        if (rowGroupBloomFilterStart < readingBlock.getStartingPos()) {
            // when bloom filter written before the current rowgroup data
            return readingBlock.getStartingPos();
        } else {
            // todo: consider more offsets, such as offsetindex and columnindex
           //  long footerSize = min(dataSource.getEstimatedSize(), getExpectedFooterSize());
            long endSize = dataSource.getEstimatedSize();
            long result = endSize;

            // when bloom filter written after all the row group data
            for (BlockMetaData blockMetaData : parquetMetadata.getBlocks()) {
                Optional<Long> currentBlockBloomFilterStart = getRowGroupBloomFilterStart(blockMetaData);
                if (currentBlockBloomFilterStart.isPresent() && currentBlockBloomFilterStart.get() > rowGroupBloomFilterStart) {
                    result = Math.min(result, currentBlockBloomFilterStart.get());
                }
            }
            return result;
        }
    }

    private static Optional<Long> getRowGroupBloomFilterStart(BlockMetaData block) {
        return block.getColumns().stream()
                .filter(BloomFilterStore::bloomFilterEnabled)
                .map(ColumnChunkMetaData::getBloomFilterOffset)
                .min(Long::compare);
    }

    private static Map<ColumnPath, BloomFilter> loadBloomFilters(ParquetDataSource dataSource, List<BloomFilterHeaderMetadata> bloomFilterHeaderMetadataList, long rowGroupBloomFilterEnd)
    {
        ListMultimap<ColumnPath, DiskRange> bloomFilterHeaderRanges = ArrayListMultimap.create(bloomFilterHeaderMetadataList.size(), 1);
        for (int i = 0; i < bloomFilterHeaderMetadataList.size(); i++) {
            long bloomFilterStart = bloomFilterHeaderMetadataList.get(i).headerOffSet();
            long bloomFilterEnd;

            if ((i + 1) < bloomFilterHeaderMetadataList.size()) {
                bloomFilterEnd = bloomFilterHeaderMetadataList.get(i + 1).headerOffSet();
            } else {
                bloomFilterEnd = rowGroupBloomFilterEnd;
            }
            bloomFilterHeaderRanges.put(bloomFilterHeaderMetadataList.get(i).path(), new DiskRange(bloomFilterStart, toIntExact(bloomFilterEnd-bloomFilterStart)));
        }

        Multimap<ColumnPath, ChunkReader> headerChunkReaders = dataSource.planRead(bloomFilterHeaderRanges);
        ImmutableMap.Builder<ColumnPath, BloomFilter> builder = new ImmutableMap.Builder<>();
        try {
            for (BloomFilterHeaderMetadata bloomFilterHeaderMetadata : bloomFilterHeaderMetadataList) {
                if (headerChunkReaders.containsKey(bloomFilterHeaderMetadata.path())) {
                    BasicSliceInput sliceInput = getOnlyElement(headerChunkReaders.get(bloomFilterHeaderMetadata.path())).readUnchecked().getInput();
                    BloomFilterHeader bloomFilterHeader = Util.readBloomFilterHeader(sliceInput);
                    if (bloomFilterSupported(bloomFilterHeader)) {
                        byte[] bitset = new byte[bloomFilterHeader.getNumBytes()];
                        sliceInput.readFully(bitset);
                        builder.put(bloomFilterHeaderMetadata.path(), new BlockSplitBloomFilter(bitset));
                    }
                }
            }
        } catch (IOException exception) {
            throw new RuntimeException("Failed to read Bloom filter header", exception);
        } finally {
            headerChunkReaders.values().forEach(ChunkReader::free);
        }

        return builder.build();
    }

    private record BloomFilterHeaderMetadata(ColumnPath path, long headerOffSet) implements Comparable<BloomFilterHeaderMetadata>
    {
        private BloomFilterHeaderMetadata(ColumnPath path, long headerOffSet)
        {
            this.path = requireNonNull(path, "path is null");
            this.headerOffSet = headerOffSet;
        }

        @Override
        public int compareTo(BloomFilterHeaderMetadata that) {
            return Long.compare(this.headerOffSet, that.headerOffSet);
        }
    }
}
