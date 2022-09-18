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

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.parquet.BloomFilterStore;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.util.TestHiveBucketing.toNativeContainerValue;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.ParquetOutputFormat.BLOOM_FILTER_ENABLED;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.apache.parquet.hadoop.metadata.ColumnPath.fromDotString;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBloomFilterStore
{
    private static final String COLUMN_NAME = "test_column";
    private static final int DOMAIN_COMPACTION_THRESHOLD = 32;
    private static final ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet");

    // here PrimitiveType#getPrimitiveTypeName is dummy, since predicate matches is via column name
    ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {COLUMN_NAME}, new PrimitiveType(REQUIRED, BINARY, COLUMN_NAME), 0, 0);

    @DataProvider
    public Object[][] bloomFilterTypeTests()
    {
        return new Object[][] {
                {
                        new BloomFilterTypeTestCase(
                                Arrays.asList("hello", "parquet", "bloom", "filter"),
                                Arrays.asList("NotExist", "fdsvit"),
                                createVarcharType(255),
                                javaStringObjectInspector)
                },
                {
                        new BloomFilterTypeTestCase(
                                Arrays.asList(12321, 3344, 72334, 321),
                                Arrays.asList(89899, 897773),
                                INTEGER,
                                javaIntObjectInspector)
                },
                {
                        new BloomFilterTypeTestCase(
                                Arrays.asList(892.22d, 341112.2222d, 43232.222121d, 99988.22d),
                                Arrays.asList(321.44d, 776541.3214d),
                                DOUBLE,
                                javaDoubleObjectInspector)
                },
                {
                        new BloomFilterTypeTestCase(
                                Arrays.asList(32.22f, 341112.2222f, 43232.222121f, 32322.22f),
                                Arrays.asList(321.44f, 321.3214f),
                                REAL,
                                javaFloatObjectInspector)
                }
        };
    }

    @Test(dataProvider = "bloomFilterTypeTests")
    public void testReadBloomFilter(BloomFilterTypeTestCase typeTestCase)
            throws Exception
    {
        try (tempFile) {
            BloomFilterStore bloomFilterEnabled = generateBloomFilterStore(tempFile, true, typeTestCase.hiveValuesExist, typeTestCase.objectInspector);
            assertTrue(bloomFilterEnabled.readBloomFilter(fromDotString(COLUMN_NAME)).isPresent());
            BloomFilter bloomFilter = bloomFilterEnabled.readBloomFilter(fromDotString(COLUMN_NAME)).get();

            for (Object data : typeTestCase.nativeValuesExist) {
                assertTrue(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, data, typeTestCase.sqlType));
            }
            for (Object data : typeTestCase.nativeValuesNotExist) {
                assertFalse(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, data, typeTestCase.sqlType));
            }
        }

        try (tempFile) {
            BloomFilterStore bloomFilterNotEnabled = generateBloomFilterStore(tempFile, false, typeTestCase.hiveValuesExist, typeTestCase.objectInspector);
            assertTrue(bloomFilterNotEnabled.readBloomFilter(fromDotString(COLUMN_NAME)).isEmpty());
        }
    }

    @Test(dataProvider = "bloomFilterTypeTests")
    public void testMatchesWithBloomFilter(BloomFilterTypeTestCase typeTestCase)
            throws Exception
    {
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, typeTestCase.hiveValuesExist, typeTestCase.objectInspector);

            TupleDomain<ColumnDescriptor> predicateMatches = withColumnDomains(singletonMap(columnDescriptor, multipleValues(typeTestCase.sqlType, typeTestCase.nativeValuesExist)));
            TupleDomainParquetPredicate tupleDomainParquetPredicateMatches = new TupleDomainParquetPredicate(predicateMatches, singletonList(columnDescriptor), UTC, DOMAIN_COMPACTION_THRESHOLD);
            // bloomfilter store has the column, and values match
            assertTrue(tupleDomainParquetPredicateMatches.matches(bloomFilterStore));

            TupleDomain<ColumnDescriptor> predicateNotMatches = withColumnDomains(singletonMap(columnDescriptor, multipleValues(typeTestCase.sqlType, typeTestCase.nativeValuesNotExist)));
            TupleDomainParquetPredicate tupleDomainParquetPredicateNotMatches = new TupleDomainParquetPredicate(predicateNotMatches, singletonList(columnDescriptor), UTC, DOMAIN_COMPACTION_THRESHOLD);
            // bloomfilter store has the column, but values not match
            assertFalse(tupleDomainParquetPredicateNotMatches.matches(bloomFilterStore));

            ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {"non_exist_path"}, Types.optional(BINARY).named("Test column"), 0, 0);
            TupleDomain<ColumnDescriptor> predicateNotMatchesWrongColumn = withColumnDomains(singletonMap(columnDescriptor, multipleValues(typeTestCase.sqlType, typeTestCase.nativeValuesNotExist)));
            TupleDomainParquetPredicate testColumnNonExistPredicate = new TupleDomainParquetPredicate(predicateNotMatchesWrongColumn, singletonList(columnDescriptor), UTC, DOMAIN_COMPACTION_THRESHOLD);
            // bloomfilter store does not have the column
            assertTrue(testColumnNonExistPredicate.matches(bloomFilterStore));
        }
    }

    @Test
    public void testMatchesWithBloomFilterExpand()
            throws Exception
    {
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList(60, 61, 62, 63, 64, 65), javaIntObjectInspector);

            TupleDomain<ColumnDescriptor> tupleDomainMatches = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(INTEGER,
                    ImmutableList.of(Range.range(INTEGER, 60L, true, 68L, true))), false)));
            TupleDomainParquetPredicate tupleDomainParquetPredicateMatches = new TupleDomainParquetPredicate(tupleDomainMatches, singletonList(columnDescriptor), UTC, DOMAIN_COMPACTION_THRESHOLD);
            // bloomfilter store has the column, and ranges overlap
            assertTrue(tupleDomainParquetPredicateMatches.matches(bloomFilterStore));

            TupleDomain<ColumnDescriptor> tupleDomainNonMatches = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(INTEGER,
                    ImmutableList.of(Range.range(INTEGER, -68L, true, -60L, true))), false)));
            // bloomfilter store has the column, but ranges not overlap
            TupleDomainParquetPredicate tupleDomainParquetPredicateNotMatches = new TupleDomainParquetPredicate(tupleDomainNonMatches, singletonList(columnDescriptor), UTC, DOMAIN_COMPACTION_THRESHOLD);
            assertFalse(tupleDomainParquetPredicateNotMatches.matches(bloomFilterStore));
        }
    }

    private static BloomFilterStore generateBloomFilterStore(ParquetTester.TempFile tempFile, boolean enableBloomFilter, List<Object> testValues, ObjectInspector objectInspector)
            throws Exception
    {
        List<ObjectInspector> objectInspectors = singletonList(objectInspector);
        List<String> columnNames = ImmutableList.of(COLUMN_NAME);

        JobConf jobConf = new JobConf(newEmptyConfiguration());
        jobConf.setEnum(WRITER_VERSION, PARQUET_1_0);
        jobConf.setBoolean(BLOOM_FILTER_ENABLED, enableBloomFilter);

        ParquetTester.writeParquetColumn(
                jobConf,
                tempFile.getFile(),
                CompressionCodecName.SNAPPY,
                ParquetTester.createTableProperties(columnNames, objectInspectors),
                getStandardStructObjectInspector(columnNames, objectInspectors),
                new Iterator<?>[] {testValues.iterator()},
                Optional.empty(),
                false,
                DateTimeZone.getDefault());

        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT);
        TrinoFileSystem fileSystem = fileSystemFactory.create(getHiveSession(new HiveConfig().setHiveStorageFormat(HiveStorageFormat.PARQUET)));
        TrinoInputFile inputFile = fileSystem.newInputFile(tempFile.getFile().getPath());
        TrinoParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        ColumnChunkMetaData columnChunkMetaData = getOnlyElement(getOnlyElement(parquetMetadata.getBlocks()).getColumns());

        return new BloomFilterStore(dataSource, getOnlyElement(parquetMetadata.getBlocks()), Set.of(columnChunkMetaData.getPath()));
    }

    private static class BloomFilterTypeTestCase
    {
        private final List<Object> nativeValuesExist;
        private final List<Object> nativeValuesNotExist;
        private final List<Object> hiveValuesExist;
        private final Type sqlType;
        private final ObjectInspector objectInspector;

        private BloomFilterTypeTestCase(List<Object> hiveValuesExist, List<Object> hiveValuesNotExist, Type sqlType, ObjectInspector objectInspector)
        {
            this.sqlType = requireNonNull(sqlType);
            this.objectInspector = requireNonNull(objectInspector);
            this.hiveValuesExist = requireNonNull(hiveValuesExist);

            nativeValuesExist = hiveValuesExist.stream().map(data -> {
                return toNativeContainerValue(sqlType, data);
            }).collect(Collectors.toList());
            nativeValuesNotExist = hiveValuesNotExist.stream().map(data -> {
                return toNativeContainerValue(sqlType, data);
            }).collect(Collectors.toList());
        }
    }
}
