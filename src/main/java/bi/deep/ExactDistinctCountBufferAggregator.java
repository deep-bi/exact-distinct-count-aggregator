package bi.deep;

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Set;

public class ExactDistinctCountBufferAggregator implements BufferAggregator {

    private final DimensionSelector selector;
    private final Int2ObjectMap<Set<Object>> mutableSetCollection = new Int2ObjectOpenHashMap<>();
    private final Integer maxNumberOfValues;

    public ExactDistinctCountBufferAggregator(DimensionSelector selector, Integer maxNumberOfValues) {
        this.selector = selector;
        this.maxNumberOfValues = maxNumberOfValues;
    }

    @Override
    public void init(ByteBuffer byteBuffer, int i) {
        byteBuffer.putLong(i, 0L);
    }

    @Override
    public void aggregate(ByteBuffer byteBuffer, int position) {
        if (selector.getObject() == null || selector.getObject().equals("")) {
            return;
        }
        Set<Object> mutableSet = getMutableSet(position);
        IndexedInts row = selector.getRow();
        for (int i = 0, rowSize = row.size(); i < rowSize; i++) {
            if (mutableSet.size() >= maxNumberOfValues) {
                throw new RuntimeException("Reached max number of rows");
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
