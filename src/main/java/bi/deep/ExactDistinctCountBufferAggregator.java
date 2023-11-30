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
import io.github.resilience4j.core.lang.NonNull;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DimensionSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.HashSet;

public class ExactDistinctCountBufferAggregator implements BufferAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(ExactDistinctCountBufferAggregator.class);
    private final DimensionSelector selector; // SingleValueQueryableDimensionSelector
    private final Integer maxNumberOfValues;
    private final boolean failOnLimitExceeded;
    private boolean achievedLimit;

    public ExactDistinctCountBufferAggregator(DimensionSelector selector, Integer maxNumberOfValues, boolean failOnLimitExceeded) {
        LOG.debug("buf constructor");
        this.selector = selector;
        this.maxNumberOfValues = maxNumberOfValues;
        this.failOnLimitExceeded = failOnLimitExceeded;
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        LOG.debug("buf init position " + i);
        LOG.debug(selector.getClass().getSimpleName());
        HashSet<Integer> mutableSet = Sets.newHashSet();
        byte[] byteValue = SerializationUtils.serialize(mutableSet);
        ByteBuffer mutationBuffer = byteBuffer.duplicate();
        mutationBuffer.position(i);
        mutationBuffer.putInt(byteValue.length);
        mutationBuffer.put(byteValue);
    }

    @Override
    public void aggregate(@NonNull ByteBuffer byteBuffer, int position) {
        if (achievedLimit) {
            return;
        }

        HashSet<Integer> mutableSet = getMutableSet(byteBuffer, position);

        if (mutableSet.size() >= maxNumberOfValues) {
            if (failOnLimitExceeded) {
                throw new RuntimeException("Reached max number of values: " + maxNumberOfValues);
            } else {
                achievedLimit = true;
                LOG.warn("Reached max number of values, result is limited");
                return;
            }
        }

        mutableSet.add(selector.getObject() == null ? "NULL".hashCode() : selector.getObject().hashCode());

        byte[] byteValue = SerializationUtils.serialize(mutableSet);
        byteBuffer.position(position);
        byteBuffer.putInt(byteValue.length);
        byteBuffer.put(byteValue);

    }

    private HashSet<Integer> getMutableSet(final ByteBuffer buffer, final int position) {
        ByteBuffer mutationBuffer = buffer.duplicate();
        mutationBuffer.position(position);
        final int size = mutationBuffer.getInt();
        final byte[] bytes = new byte[size];
        mutationBuffer.get(bytes);
        return SerializationUtils.deserialize(bytes);
    }

    @Nullable
    @Override
    public Object get(@NonNull ByteBuffer byteBuffer, int i) {
        HashSet<Integer> mutableSet = getMutableSet(byteBuffer, i);
        LOG.debug("Returning " + mutableSet.toString() + "with size " + mutableSet.size());
        return mutableSet;
    }

    @Override
    public float getFloat(@NonNull ByteBuffer byteBuffer, int i) {
        throw new UnsupportedOperationException("ExactDistinctCountBufferAggregator does not support getFloat()");
    }

    @Override
    public long getLong(@NonNull ByteBuffer byteBuffer, int i) {
        throw new UnsupportedOperationException("ExactDistinctCountBufferAggregator does not support getLong()");
    }

    @Override
    public void close() {

    }


    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
        inspector.visit("selector", selector);
    }
}
