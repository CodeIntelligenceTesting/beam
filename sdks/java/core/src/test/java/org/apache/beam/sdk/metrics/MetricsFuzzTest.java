/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.metrics;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import com.code_intelligence.jazzer.mutation.annotation.NotNull;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class MetricsFuzzTest {

  @FuzzTest(maxExecutions = 1)
  public void fuzzMetrics(@NotNull FuzzedDataProvider data) {
    try {
      PipelineOptions options = PipelineOptionsFactory.create();
      options.setRunner(DirectRunner.class);
      Pipeline p = Pipeline.create(options);

      int itemCount = data.consumeInt(1, 10);
      List<String> items = new ArrayList<>();
      for (int i = 0; i < itemCount; i++) {
        items.add(data.consumeString(20));
      }

      PCollection<String> col = p.apply(Create.of(items));

      col.apply(
          "fuzzMetrics",
          ParDo.of(
              new DoFn<String, String>() {
                private final Counter counter = Metrics.counter("ns1", "counter1");
                private final Distribution dist = Metrics.distribution("ns1", "dist1");
                private final Gauge gauge = Metrics.gauge("ns1", "gauge1");

                @ProcessElement
                public void process(@Element String element) {
                  long val = data.consumeLong(1, 10);

                  counter.inc();
                  counter.inc(val);
                  dist.update(val);
                  gauge.set(val);
                }
              }));

      p.run().waitUntilFinish();

      MetricResults results = p.run().metrics();

      results
          .queryMetrics(MetricsFilter.builder().build())
          .getCounters()
          .forEach(mc -> mc.getCommitted());

      results
          .queryMetrics(MetricsFilter.builder().build())
          .getDistributions()
          .forEach(
              md -> {
                long unusedCount = md.getCommitted().getCount();
                long unusedSum = md.getCommitted().getSum();
                long unusedMin = md.getCommitted().getMin();
                long unusedMax = md.getCommitted().getMax();
              });

      results
          .queryMetrics(MetricsFilter.builder().build())
          .getGauges()
          .forEach(mg -> mg.getCommitted());

    } catch (IllegalArgumentException ignored) {
    }
  }
}
