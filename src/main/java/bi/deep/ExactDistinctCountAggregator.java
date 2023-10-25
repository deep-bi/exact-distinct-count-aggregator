package bi.deep;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import java.util.Set;

public class ExactDistinctCountAggregator implements Aggregator {

    private final DimensionSelector selector;
    private final Set<Object> set;
    private final Integer maxNumberOfValues;

    public ExactDistinctCountAggregator(
            DimensionSelector selector,
            Set<Object> set,
            Integer maxNumberOfValues
    ) {
        this.selector = selector;
        this.set = set;
        this.maxNumberOfValues = maxNumberOfValues;
    }

    @Override
    public void aggregate() {
        if (selector.getObject() == null || selector.getObject().equals("")) {
            return;
        }
        IndexedInts row = selector.getRow();
        for (int i = 0, rowSize = row.size(); i < rowSize; i++) {
            if (set.size() >= maxNumberOfValues) {
                throw new RuntimeException("Reached max number of values: " + maxNumberOfValues);
            }
            int index = row.get(i);
            set.add(index);
        }
    }

    @Override
    public Object get() {
        return set.size();
    }

    @Override
    public float getFloat() {
        return (float) set.size();
    }

    @Override
    public void close() {
        set.clear();
    }

    @Override
    public long getLong() {
        return set.size();
    }

    @Override
    public double getDouble() {
        return set.size();
    }
}
