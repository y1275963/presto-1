package io.trino.parquet;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.io.api.Binary;

import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public class BloomFilterUtil
{
    public static boolean typeSupported(Type type) {
        return getBloomFilterCalculateFunction(type).isPresent();
    }

    // calculate predicateValue's bloom filter hash value, given predicateValue is in type sqlType

    /**
     *  Given the predicate value is of sqlType, calculate hash value for the predicate value using the specified Bloom filter
     *
     * @param bloomFilter the Bloom filter to use for computing the hash value
     * @param predicateValue the value to compute the hash value for
     * @param sqlType the SQL data type of the predicate value
     * @return an optional long containing the computed hash value, or an empty Optional if the type is not supported
     */
    public static Optional<Long> getBloomFilterHash(BloomFilter bloomFilter, Object predicateValue, Type sqlType) {
        Optional<BiFunction<Object, BloomFilter, Long>> calculateFunction = getBloomFilterCalculateFunction(sqlType);

        if (calculateFunction.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(calculateFunction.get().apply(predicateValue, bloomFilter));
    }

    /**
     *  Returns a biFunction that calculates the Bloom filter hash of a predicate value using the specified Bloom filter,
     *  given the SQL data type of the predicate value. Or Optional.empty() if the type is not supported.
     *
     * @param sqlType the SQL data type of the predicate value
     */
    public static Optional<BiFunction<Object, BloomFilter, Long>> getBloomFilterCalculateFunction(Type sqlType)
    {
        // TODO: Support TIMESTAMP, CHAR and DECIMAL
        if (sqlType == TINYINT || sqlType == SMALLINT || sqlType == INTEGER || sqlType == DATE) {
            return Optional.of((predicateValue, bloomFilter) -> bloomFilter.hash(toIntExact(((Number) predicateValue).longValue())));
        }
        if (sqlType == BIGINT) {
            return Optional.of((predicateValue, bloomFilter) -> bloomFilter.hash(((Number) predicateValue).longValue()));
        }
        else if (sqlType == DOUBLE) {
            return Optional.of((predicateValue, bloomFilter) -> bloomFilter.hash(((Double) predicateValue).doubleValue()));
        }
        else if (sqlType == REAL) {
            return Optional.of((predicateValue, bloomFilter) -> bloomFilter.hash(intBitsToFloat(toIntExact(((Number) predicateValue).longValue()))));
        }
        else if (sqlType instanceof VarcharType || sqlType instanceof VarbinaryType) {
            return Optional.of((predicateValue, bloomFilter) -> bloomFilter.hash(Binary.fromConstantByteBuffer(((Slice) predicateValue).toByteBuffer())));
        }
        else if (sqlType instanceof UuidType) {
            return Optional.of((predicateValue, bloomFilter) -> bloomFilter.hash(Binary.fromConstantByteArray(((Slice) predicateValue).getBytes())));
        }

        return Optional.empty();
    }
}
