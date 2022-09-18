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
package io.trino.plugin.hive.parquet;

import com.google.common.io.Resources;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.parquet.BloomFilterStore;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBloomFilter
{
    public static final ConnectorSession SESSION = getHiveSession(new HiveConfig().setHiveStorageFormat(HiveStorageFormat.PARQUET));

    @Test
    public void testReadBloomFilterParquet()
            throws Exception
    {
        String[] testNames = {"hello", "parquet", "bloom", "filter"};
        VarcharType columnType = createVarcharType(255);

        File parquetFile = new File(Resources.getResource("bloom-filter.parquet").toURI());
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT);
        TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
        TrinoInputFile inputFile = fileSystem.newInputFile(parquetFile.getPath());

        TrinoParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());

        ColumnChunkMetaData columnChunkMetaData = getOnlyElement(getOnlyElement(parquetMetadata.getBlocks()).getColumns());

        BloomFilterStore bloomFilterStore = new BloomFilterStore(dataSource, getOnlyElement(parquetMetadata.getBlocks()), Set.of(columnChunkMetaData.getPath()));
        Optional<BloomFilter> bloomFilterOptional = bloomFilterStore.readBloomFilter(columnChunkMetaData.getPath());
        assertTrue(bloomFilterOptional.isPresent());
        BloomFilter bloomfilter = bloomFilterOptional.get();

        for (String testName : testNames) {
            Slice testSlice = Slices.utf8Slice(testName);
            long testHash = getBloomFilterHash(bloomfilter, testSlice, columnType);
            assertTrue(bloomfilter.findHash(testHash));
        }

        Slice testSlice = Slices.utf8Slice("not_exist");
        long testHash = getBloomFilterHash(bloomfilter, testSlice, columnType);
        assertFalse(bloomfilter.findHash(testHash));
    }

    @Test
    public void testReadBloomFilterParquetNonExist()
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("no-bloom-filter.parquet").toURI());
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT);
        TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
        TrinoInputFile inputFile = fileSystem.newInputFile(parquetFile.getPath());

        TrinoParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());

        ColumnChunkMetaData columnChunkMetaData = getOnlyElement(getOnlyElement(parquetMetadata.getBlocks()).getColumns());
        BloomFilterStore bloomFilterStore = new BloomFilterStore(dataSource, getOnlyElement(parquetMetadata.getBlocks()), Set.of(columnChunkMetaData.getPath()));
        Optional<BloomFilter> bloomFilterOptional = bloomFilterStore.readBloomFilter(columnChunkMetaData.getPath());
        assertFalse(bloomFilterOptional.isPresent());
    }

    private static long getBloomFilterHash(BloomFilter bloomFilter, Slice predicateValue, Type sqlType)
            throws UnsupportedOperationException
    {
        if (sqlType instanceof VarcharType) {
            return bloomFilter.hash(Binary.fromConstantByteBuffer(predicateValue.toByteBuffer()));
        }
        else {
            throw new UnsupportedOperationException("Unsupported type " + sqlType);
        }
    }
}
