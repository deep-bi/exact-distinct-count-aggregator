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

import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashSet;

public class ExactDistinctCountAggregatorCombiner extends ObjectAggregateCombiner<HashSet<Object>> {

    private static final ExactDistinctCountAggregatorCombiner INSTANCE = new ExactDistinctCountAggregatorCombiner();
    private static final Logger LOG = LoggerFactory.getLogger(ExactDistinctCountAggregatorCombiner.class);

    @Nullable
    private HashSet<Object> combinedSet;

    @Override
    public void reset(ColumnValueSelector selector) {
        LOG.debug("reset");
        combinedSet = null;
        fold(selector);
    }

    @Override
    public void fold(ColumnValueSelector selector) {
        LOG.debug("fold");
        HashSet<Object> other = (HashSet<Object>) selector.getObject();
        if (other == null) {
            return;
        }
        if (combinedSet == null) {
            combinedSet = new HashSet<>(other);
        }
        combinedSet.addAll(other);
    }

    @Nullable
    @Override
    public HashSet<Object> getObject() {
        LOG.debug("getObj");
        return combinedSet;
    }

    @Override
    public Class classOfObject() {
        LOG.debug("getClassOfObj");
        return HashSet.class;
    }

    public static ExactDistinctCountAggregatorCombiner getInstance() {
        return INSTANCE;
    }
}
