/*
 *    Copyright 2023 Deep BI, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package bi.deep;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import javax.validation.ValidationException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ExactDistinctCountAggregatorFactory extends AggregatorFactory {

    private final String name;
    private final String fieldName;
    private final Integer maxNumberOfValues;
    private final Boolean failOnLimitExceeded;

    @JsonCreator
    public ExactDistinctCountAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("maxNumberOfValues") Integer maxNumberOfValues,
            @JsonProperty("failOnLimitExceeded") Boolean failOnLimitExceeded
    ) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(fieldName);

        this.name = name;
        this.fieldName = fieldName;

        if (maxNumberOfValues != null && maxNumberOfValues <= 0) {
            throw new ValidationException("Invalid maxNumberOfValues -> '" + maxNumberOfValues + '\'');
        }

        this.failOnLimitExceeded = failOnLimitExceeded != null && failOnLimitExceeded;

        this.maxNumberOfValues = maxNumberOfValues != null ? maxNumberOfValues : 10000;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory columnFactory) {
        DimensionSelector selector = makeDimensionSelector(columnFactory);
        if (selector instanceof DimensionSelector.NullDimensionSelectorHolder) {
            throw new ValidationException("There is no column: " + fieldName);
        }
        return new ExactDistinctCountAggregator(
                selector,
                Sets.newHashSet(),
                maxNumberOfValues,
                failOnLimitExceeded
        );
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory) {
        DimensionSelector selector = makeDimensionSelector(columnFactory);
        if (selector instanceof DimensionSelector.NullDimensionSelectorHolder) {
            throw new ValidationException("There is no column: " + fieldName);
        }
        return new ExactDistinctCountBufferAggregator(
                selector,
                maxNumberOfValues,
                failOnLimitExceeded
        );
    }

    @Override
    public AggregatorFactory withName(String newName) {
        return new ExactDistinctCountAggregatorFactory(newName, getFieldName(), maxNumberOfValues, failOnLimitExceeded);
    }

    private DimensionSelector makeDimensionSelector(final ColumnSelectorFactory columnFactory) {
        return columnFactory.makeDimensionSelector(new DefaultDimensionSpec(fieldName, fieldName));
    }

    @Override
    public Comparator getComparator() {
        return (o, o1) -> Longs.compare(((Number) o).longValue(), ((Number) o1).longValue());
    }

    @Override
    public Object combine(Object lhs, Object rhs) {
        if (lhs == null && rhs == null) {
            return 0L;
        }
        if (rhs == null) {
            return ((Number) lhs).longValue();
        }
        if (lhs == null) {
            return ((Number) rhs).longValue();
        }
        return ((Number) lhs).longValue() + ((Number) rhs).longValue();
    }

    @Override
    public AggregateCombiner makeAggregateCombiner() {
        return new LongSumAggregateCombiner();
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new LongSumAggregatorFactory(name, name);
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(
                new ExactDistinctCountAggregatorFactory(fieldName, fieldName, maxNumberOfValues, failOnLimitExceeded)
        );
    }

    @Override
    public Object deserialize(Object object) {
        return object;
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object object) {
        return object;
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @JsonProperty
    public Integer getMaxNumberOfValues() {
        return maxNumberOfValues;
    }

    @JsonProperty
    public Boolean getFailOnLimitExceeded() {
        return failOnLimitExceeded;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(fieldName);
    }

    @Override
    public byte[] getCacheKey() {
        byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
        byte[] bitMapFactoryCacheKey = StringUtils.toUtf8(this.getClass().getSimpleName());
        byte[] maxValuesBytes = StringUtils.toUtf8(maxNumberOfValues.toString());
        byte[] failOnLimitExceededBytes = StringUtils.toUtf8(failOnLimitExceeded.toString());
        return ByteBuffer.allocate(4 + fieldNameBytes.length + bitMapFactoryCacheKey.length + maxValuesBytes.length + failOnLimitExceededBytes.length)
                .put(AggregatorUtil.DISTINCT_COUNT_CACHE_KEY)
                .put(fieldNameBytes)
                .put(AggregatorUtil.STRING_SEPARATOR)
                .put(bitMapFactoryCacheKey)
                .put(AggregatorUtil.STRING_SEPARATOR)
                .put(maxValuesBytes)
                .put(AggregatorUtil.STRING_SEPARATOR)
                .put(failOnLimitExceededBytes)
                .array();
    }

    @Override
    public ColumnType getIntermediateType() {
        return ColumnType.LONG;
    }

    @Override
    public ColumnType getResultType() {
        return ColumnType.LONG;
    }

    @Override
    public int getMaxIntermediateSize() {
        return Long.BYTES;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExactDistinctCountAggregatorFactory that = (ExactDistinctCountAggregatorFactory) o;

        if (!fieldName.equals(that.fieldName)) {
            return false;
        }
        if (!maxNumberOfValues.equals(that.maxNumberOfValues)) {
            return false;
        }
        if (failOnLimitExceeded.booleanValue() != that.failOnLimitExceeded.booleanValue()) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + fieldName.hashCode() + maxNumberOfValues.hashCode() + failOnLimitExceeded.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ExactDistinctCountAggregatorFactory{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", maxNumberOfValues=" + maxNumberOfValues +
                ", failOnLimitExceeded=" + failOnLimitExceeded.toString() +
                '}';
    }
}
