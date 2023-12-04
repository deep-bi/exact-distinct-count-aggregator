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

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.DimensionSelector;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class ExactDistinctCountAggregator implements Aggregator {
    private final List<DimensionSelector> selectors;
    private final Set<Integer> set;
    private final Integer maxNumberOfValues;
    private final boolean failOnLimitExceeded;
    private boolean achievedLimit;
    private static final String NULL = "NULL";


    public ExactDistinctCountAggregator(
            List<DimensionSelector> selectors,
            Set<Integer> set,
            Integer maxNumberOfValues,
            boolean failOnLimitExceeded
    ) {
        this.selectors = selectors;
        this.set = set;
        this.maxNumberOfValues = maxNumberOfValues;
        this.failOnLimitExceeded = failOnLimitExceeded;
    }

    @Override
    public void aggregate() {
        if (achievedLimit) {
            return;
        }

        if (set.size() >= maxNumberOfValues) {
            if (contains(set, selectors)) {
                return;
            }
            if (failOnLimitExceeded) {
                throw new RuntimeException("Reached max number of values: " + maxNumberOfValues);
            } else {
                achievedLimit = true;
                LoggerFactory.getLogger(this.getClass()).warn("Reached max number of values, result is limited");
                return;
            }
        }

        add(set, selectors);
    }

    public static boolean contains(Set<Integer> set, List<DimensionSelector> selectors) {
        return selectors.stream().map(ExactDistinctCountAggregator::getCurrentObjectHashCode).allMatch(set::contains);
    }

    public static void add(Set<Integer> set, List<DimensionSelector> selectors) {
        selectors.forEach(selector -> set.add(getCurrentObjectHashCode(selector)));
    }

    public static int getCurrentObjectHashCode(final DimensionSelector selector){
        return selector.getObject() == null ? NULL.hashCode() : selector.getObject().hashCode();
    }

    @Override
    public Object get() {
        return set;
    }


    @Override
    public void close() {

    }


    @Override
    public float getFloat() {
        throw new UnsupportedOperationException("ExactDistinctCountAggregator does not support getFloat()");
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException("ExactDistinctCountAggregator does not support getLong()");
    }

    @Override
    public double getDouble() {
        throw new UnsupportedOperationException("ExactDistinctCountAggregator does not support getDouble()");
    }
}
