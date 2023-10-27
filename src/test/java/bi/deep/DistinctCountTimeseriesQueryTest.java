/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package bi.deep;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.DefaultTimeseriesQueryMetrics;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class DistinctCountTimeseriesQueryTest extends InitializedNullHandlingTest
{

  @Test
  public void testTimeseriesWithDistinctCountAgg() throws Exception
  {
    TimeseriesQueryEngine engine = new TimeseriesQueryEngine();

    IncrementalIndex index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.SECOND)
                .withMetrics(new CountAggregatorFactory("cnt"))
                .build()
        )
        .setMaxRowCount(1000)
        .build();

    String visitor_id = "visitor_id";
    String client_type = "client_type";
    DateTime time = DateTimes.of("2016-03-04T00:00:00.000Z");
    long timestamp = time.getMillis();
    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.of(visitor_id, "0", client_type, "iphone")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.of(visitor_id, "1", client_type, "iphone")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.of(visitor_id, "2", client_type, "android")
        )
    );

    TimeseriesQuery queryToFail = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      Lists.newArrayList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          new ExactDistinctCountAggregatorFactory("UV", visitor_id, 2, true)
                                      )
                                  )
                                  .build();

      Assert.assertThrows(RuntimeException.class, () -> engine.process(queryToFail, new IncrementalIndexStorageAdapter(index), new DefaultTimeseriesQueryMetrics()).toList());


    TimeseriesQuery partialQuery = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      Lists.newArrayList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          new ExactDistinctCountAggregatorFactory("UV", visitor_id, 2, false)
                                      )
                                  )
                                  .build();

    final Iterable<Result<TimeseriesResultValue>> results =
        engine.process(partialQuery, new IncrementalIndexStorageAdapter(index), new DefaultTimeseriesQueryMetrics()).toList();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            time,
            new TimeseriesResultValue(
                ImmutableMap.of("UV", 2, "rows", 3L)
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);


    TimeseriesQuery fullQuery = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      Lists.newArrayList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          new ExactDistinctCountAggregatorFactory("UV", visitor_id, 3, true)
                                      )
                                  )
                                  .build();

    final Iterable<Result<TimeseriesResultValue>> fullResults =
        engine.process(fullQuery, new IncrementalIndexStorageAdapter(index), new DefaultTimeseriesQueryMetrics()).toList();

    List<Result<TimeseriesResultValue>> fullExpectedResults = Collections.singletonList(
        new Result<>(
            time,
            new TimeseriesResultValue(
                ImmutableMap.of("UV", 3, "rows", 3L)
            )
        )
    );
    TestHelper.assertExpectedResults(fullExpectedResults, fullResults);
  }
}
