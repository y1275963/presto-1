package io.trino.parquet.writer;

public record BloomFilterColumnConfig(String columnName, double ndv, long maxSize)
{
}
