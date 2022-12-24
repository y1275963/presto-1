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
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.io.File.createTempFile;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.ParquetOutputFormat.BLOOM_FILTER_ENABLED;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveParquetWithBloomFilters
        extends AbstractTestQueryFramework
{
    private static final String COLUMN_NAME = "dataColumn";
    private static final List<String> TEST_VALUES = Arrays.asList("hello", "parquet", "bloom", "filter");

    private static final String NOT_EXIST_VALUE = "not_exist";

    @Test
    public void testBloomFilterBasedRowGroupPruning()
            throws Exception
    {
        try (TempDataLocation tempDataLocation = new TempDataLocation("testFile", ".parquet", "testDir")) {
            String tableName = "parquet_with_bloom_filters_" + randomNameSuffix();

            createParquetBloomFilterSource(tempDataLocation.dataFile);
            assertUpdate(
                    format(
                            "CREATE TABLE %s" +
                                    " (%s varchar) " +
                                    "WITH ( " +
                                    "   format = 'PARQUET', " +
                                    "   external_location = '%s' " +
                                    ")",
                            tableName, COLUMN_NAME, tempDataLocation.dataDirectory));

            assertQueryStats(
                    getSession(),
                    String.format(String.format("select * from %s where %s in ('%s')", tableName, COLUMN_NAME, NOT_EXIST_VALUE)),
                    queryStats -> {
                        assertThat(queryStats.getPhysicalInputPositions()).isEqualTo(0);
                        assertThat(queryStats.getProcessedInputPositions()).isEqualTo(0);
                    },
                    results -> assertThat(results.getRowCount()).isEqualTo(0));

            assertQueryStats(
                    disableBloomFilters(getSession()),
                    String.format(String.format("select * from %s where %s in (%s)", tableName, COLUMN_NAME, "'not_exist'")),
                    queryStats -> {
                        assertThat(queryStats.getPhysicalInputPositions()).isGreaterThan(0);
                        assertThat(queryStats.getProcessedInputPositions()).isEqualTo(queryStats.getPhysicalInputPositions());
                    },
                    results -> assertThat(results.getRowCount()).isEqualTo(0));
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .addHiveProperty("parquet.use-bloom-filter", "true")
                .build();
    }

    private Session disableBloomFilters(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "parquet_use_bloom_filter", "false")
                .build();
    }

    private void createParquetBloomFilterSource(File tempFile)
            throws Exception
    {
        List<ObjectInspector> objectInspectors = singletonList(javaStringObjectInspector);
        List<String> columnNames = ImmutableList.of(COLUMN_NAME);

        JobConf jobConf = new JobConf(newEmptyConfiguration());
        jobConf.setEnum(WRITER_VERSION, PARQUET_1_0);
        jobConf.setBoolean(BLOOM_FILTER_ENABLED, true);

        ParquetTester.writeParquetColumn(
                jobConf,
                tempFile,
                CompressionCodecName.SNAPPY,
                ParquetTester.createTableProperties(columnNames, objectInspectors),
                getStandardStructObjectInspector(columnNames, objectInspectors),
                new Iterator<?>[] {TEST_VALUES.iterator()},
                Optional.empty(),
                false,
                DateTimeZone.getDefault());
    }

    private static class TempDataLocation
            implements Closeable
    {
        final File dataFile;
        final File dataDirectory;

        private TempDataLocation(String prefix, String suffix, String tempDirectoryName)
                throws IOException
        {
            Path testDirectory = Files.createTempDirectory(tempDirectoryName);
            dataDirectory = testDirectory.toFile();

            dataFile = createTempFile(prefix, suffix, dataDirectory);
            verify(dataFile.delete());
        }

        @Override
        public void close()
                throws IOException
        {
            // delete file such as .testFile9051268317012772696.parquet.crc and testFile9051268317012772696.parquet
            for (File file : dataDirectory.listFiles()) {
                if (!file.delete()) {
                    verify(!file.exists());
                }
            }

            if (!dataDirectory.delete()) {
                verify(!dataFile.exists());
            }
        }
    }
}
