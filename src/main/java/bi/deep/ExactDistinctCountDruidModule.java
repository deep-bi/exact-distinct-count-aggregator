package bi.deep;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class ExactDistinctCountDruidModule implements DruidModule {

    public static final String EXACT_DISTINCT_COUNT = "exactDistinctCount";

    @Override
    public void configure(Binder binder) {

    }

    @Override
    public List<? extends Module> getJacksonModules() {
        return ImmutableList.of(
                new SimpleModule("ExactDistinctCountModule").registerSubtypes(
                        new NamedType(ExactDistinctCountAggregatorFactory.class, EXACT_DISTINCT_COUNT)
                )
        );
    }
}
