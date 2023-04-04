package io.trino.parquet.writer;

import com.google.common.collect.ImmutableMap;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;

import java.util.Map;
import java.util.Optional;

public class BloomFilterWriteStore
{
    private Map<String, BloomFilter> bloomFilters;

    public BloomFilterWriteStore()
    {
        bloomFilters = ImmutableMap.of();
    }

    public BloomFilterWriteStore(String path, BloomFilter bloomFilter)
    {
        this.bloomFilters = ImmutableMap.of(path, bloomFilter);
    }

    public BloomFilterWriteStore(Map<String, BloomFilter> bloomFilters)
    {
        this.bloomFilters = bloomFilters;
    }

    public boolean isEmpty()
    {
        return bloomFilters.isEmpty();
    }

    public Optional<BloomFilter> getBloomFilter(String path) {
        return Optional.ofNullable(bloomFilters.get(path));
    }

    public static class BloomFilterWriteStoreBuilder
    {
        private ImmutableMap.Builder<String, BloomFilter> filterBuilders = new ImmutableMap.Builder();

        public void addAll(BloomFilterWriteStore bloomFilterWriteStore)
        {
            if (!bloomFilterWriteStore.isEmpty()) {
                filterBuilders.putAll(bloomFilterWriteStore.bloomFilters);
            }
        }

        public BloomFilterWriteStore build()
        {
            return new BloomFilterWriteStore(filterBuilders.buildOrThrow());
        }
    }
}
