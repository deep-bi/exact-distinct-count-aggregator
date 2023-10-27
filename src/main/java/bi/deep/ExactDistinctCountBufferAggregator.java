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

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Set;

public class ExactDistinctCountBufferAggregator implements BufferAggregator {

    private final DimensionSelector selector;
    private final Int2ObjectMap<Set<Object>> mutableSetCollection = new Int2ObjectOpenHashMap<>();
    private final Integer maxNumberOfValues;
    private final boolean failOnLimitExceeded;
    private boolean achievedLimit;

    public ExactDistinctCountBufferAggregator(DimensionSelector selector, Integer maxNumberOfValues, boolean failOnLimitExceeded) {
        this.selector = selector;
        this.maxNumberOfValues = maxNumberOfValues;
        this.failOnLimitExceeded = failOnLimitExceeded;
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        byteBuffer.putLong(i, 0L);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int position) {
        if (achievedLimit){
            return;
        }

        if (selector.getObject() == null || selector.getObject().equals("")) {
            return;
        }
        Set<Object> mutableSet = getMutableSet(position);
        IndexedInts row = selector.getRow();

        for (int i = 0, rowSize = row.size(); i < rowSize && !achievedLimit; i++) {
            if (mutableSet.size() >= maxNumberOfValues) {
                if (failOnLimitExceeded) {
                    throw new RuntimeException("Reached max number of values: " + maxNumberOfValues);
                } else {
                    achievedLimit = true;
                    LoggerFactory.getLogger(this.getClass()).warn("Reached max number of values, result is limited");
                    return;
                }
            }
            int index = row.get(i);
            mutableSet.add(index);
        }

        byteBuffer.putLong(position, mutableSet.size());
    }

    private Set<Object> getMutableSet(int position) {
        Set<Object> mutableSet = mutableSetCollection.get(position);
        if (mutableSet == null) {
            mutableSet = Sets.newHashSet();
            mutableSetCollection.put(position, mutableSet);
        }
        return mutableSet;
    }

    @Nullable
    @Override
    public Object get(ByteBuffer byteBuffer, int i) {
        return byteBuffer.getLong(i);
    }

    @Override
    public float getFloat(ByteBuffer byteBuffer, int i) {
        return byteBuffer.getLong(i);
    }

    @Override
    public long getLong(ByteBuffer byteBuffer, int i) {
        return byteBuffer.getLong(i);
    }

    @Override
    public void close() {
        mutableSetCollection.clear();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
        inspector.visit("selector", selector);
    }
}
