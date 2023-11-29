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


import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashSet;

public class HashSetDimensionSelector implements DimensionSelector {
    private final ColumnValueSelector<HashSet<Object>> valueSelector;
    private final HashSet<Object> hashSet;
    private static final Logger LOG = LoggerFactory.getLogger(ExactDistinctCountBufferAggregator.class);

    public HashSetDimensionSelector(ColumnValueSelector<HashSet<Object>> valueSelector) {
        this.valueSelector = valueSelector;
        hashSet = valueSelector.getObject() == null ? Sets.newHashSet() : valueSelector.getObject();
    }

    @Override
    public IndexedInts getRow() {
        return new HashSetIndexInts(hashSet);
    }

    @Override
    public ValueMatcher makeValueMatcher(@Nullable String value) {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
    }

    @Override
    public ValueMatcher makeValueMatcher(Predicate<String> predicate) {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
    }


    @Override
    public int hashCode() {
        return valueSelector.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        HashSetDimensionSelector that = (HashSetDimensionSelector) obj;
        return valueSelector.equals(that.valueSelector);
    }

    @Nullable
    @Override
    public Object getObject() {
        return hashSet;
    }

    @Override
    public Class<?> classOfObject() {
        return HashSet.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
        inspector.visit("valueSelector", valueSelector);
    }

    @Override
    public int getValueCardinality() {
        return hashSet.size();
    }

    @Nullable
    @Override
    public String lookupName(int id) {
        LOG.debug("lookupName " + id);
        Object value = valueSelector.getObject();
        if (value instanceof HashSet) {
            HashSet set = (HashSet) value;
            Object[] array = set.toArray();
            if (id >= 0 && id < array.length) {
                LOG.debug("lookupName " + id + " " + array[id].toString());
                return array[id].toString();
            }
        }
        return null;
    }

    @Override
    public boolean nameLookupPossibleInAdvance() {
        return false;
    }

    @Nullable
    @Override
    public IdLookup idLookup() {
        LOG.debug("idLookup");
        return name -> {
            Object value = valueSelector.getObject();
            if (value instanceof HashSet) {
                HashSet set = (HashSet) value;
                Object[] array = set.toArray();
                for (int i = 0; i < array.length; i++) {
                    if (array[i].toString().equals(name)) {
                        return i;
                    }
                }
            }
            return -1;
        };
    }
}
