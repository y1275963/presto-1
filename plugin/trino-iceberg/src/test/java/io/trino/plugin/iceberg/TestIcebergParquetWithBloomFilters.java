package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.parquet.ParquetTester;
import io.trino.testing.BaseTestParquetWithBloomFilters;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.parquet.TestHiveParquetWithBloomFilters.writeParquetBloomFilterSource;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.apache.parquet.hadoop.ParquetOutputFormat.BLOOM_FILTER_ENABLED;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;

public class TestIcebergParquetWithBloomFilters
    extends BaseTestParquetWithBloomFilters
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder().build();
        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        queryRunner.installPlugin(new TestingHivePlugin());
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", dataDirectory.toString())
                .put("hive.security", "allow-all")
                .buildOrThrow());

        return queryRunner;
    }

    @Override
    protected String createParquetTableWithBloomFilter(String columnName, List<Integer> testValues)
    {
        String tableName = "parquet_with_bloom_filters_" + randomNameSuffix();
        String hiveTableName = format("hive.tpch.%s", tableName);
        String icebergTableName = format("iceberg.tpch.%s", tableName);
        assertUpdate(
                format(
                        "CREATE TABLE %s (%s INT) WITH (format = 'PARQUET')",
                        hiveTableName,
                        columnName));


        Path tableLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableName));
        Path fileLocation = tableLocation.resolve("bloomFilterFile.parquet");

        writeParquetBloomFilterSource(fileLocation.toFile(), columnName, testValues);
        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "', 'false')");

        return icebergTableName;
    }
}
