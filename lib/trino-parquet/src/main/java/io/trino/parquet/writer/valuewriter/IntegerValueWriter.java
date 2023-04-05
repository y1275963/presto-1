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

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class IntegerValueWriter
        extends PrimitiveValueWriter
{
    private final Type type;

    public IntegerValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType, Optional<BloomFilter> bloomFilterOptional)
    {
        super(parquetType, valuesWriter, bloomFilterOptional);
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public void write(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); ++i) {
            if (!block.isNull(i)) {
                int value = (int) type.getLong(block, i);
                getValueWriter().writeInteger(value);
                getStatistics().updateStats(value);
                // todo: update bloomfilter here
                if (getBloomFilter().isPresent()) {

                    long hashValue = getHash(getBloomFilter().get(), value);
                    getBloomFilter().get().insertHash(hashValue);
//                    System.out.println("bloomfilter is available");
                }
            }
        }
    }

    private static long getHash(BloomFilter bloomFilter, int value) {
        return bloomFilter.hash(toIntExact(((Number) value).longValue()));
    }
}
