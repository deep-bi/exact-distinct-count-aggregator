package bi.deep;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class ExactDistinctCountAggregator implements Aggregator {

    private final DimensionSelector selector;
    private final Set<Object> set;
    private final Integer maxNumberOfValues;
    private final boolean failOnLimitExceeded;
    private boolean achievedLimit;

    public ExactDistinctCountAggregator(
            DimensionSelector selector,
            Set<Object> set,
            Integer maxNumberOfValues,
            boolean failOnLimitExceeded
    ) {
        this.selector = selector;
        this.set = set;
        this.maxNumberOfValues = maxNumberOfValues;
        this.failOnLimitExceeded = failOnLimitExceeded;
    }

    @Override
    public void aggregate() {
        if (achievedLimit) {
            return;
        }

        if (selector.getObject() == null || selector.getObject().equals("")) {
            return;
        }
        IndexedInts row = selector.getRow();


        for (int i = 0, rowSize = row.size(); i < rowSize && !achievedLimit; i++) {
            if (set.size() >= maxNumberOfValues) {
                if (failOnLimitExceeded) {
                    throw new RuntimeException("Reached max number of values: " + maxNumberOfValues);
                } else {
                    achievedLimit = true;
                    LoggerFactory.getLogger(this.getClass()).warn("Reached max number of values, result is limited");
                    return;
                }
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
