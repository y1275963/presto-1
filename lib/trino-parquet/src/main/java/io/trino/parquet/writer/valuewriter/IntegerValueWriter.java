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
package io.trino.parquet.writer.valuewriter;

import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static io.trino.parquet.BloomFilterUtil.getBloomFilterCalculateFunction;
import static java.util.Objects.requireNonNull;

public class IntegerValueWriter
        extends PrimitiveValueWriter
{
    private final Type type;
    private final Optional<Consumer<Object>> updateBloomFilterOptional;

    public IntegerValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType, Optional<BloomFilter> bloomFilterOptional)
    {
        super(parquetType, valuesWriter, bloomFilterOptional);
        this.type = requireNonNull(type, "type is null");

        Optional<BiFunction<Object, BloomFilter, Long>> bloomFilterHashFunctionOptional = getBloomFilterCalculateFunction(type);
        if (bloomFilterHashFunctionOptional.isPresent() && bloomFilterOptional.isPresent()) {
            BiFunction<Object, BloomFilter, Long> computeFunction = bloomFilterHashFunctionOptional.get();
            BloomFilter bloomFilter = bloomFilterOptional.get();

            updateBloomFilterOptional = Optional.of((predicate) -> {
                long bloomFilterHash = computeFunction.apply(predicate, bloomFilter);
                bloomFilter.insertHash(bloomFilterHash);
            });
        } else {
            updateBloomFilterOptional = Optional.empty();
        }
    }

    @Override
    public void write(Block block)
    {

        for (int i = 0; i < block.getPositionCount(); ++i) {
            if (!block.isNull(i)) {
                int value = (int) type.getLong(block, i);
                getValueWriter().writeInteger(value);
                getStatistics().updateStats(value);

                updateBloomFilterOptional.ifPresent(updateBloomFilter -> {
                    updateBloomFilter.accept(value);
                });
            }
        }
    }
}
