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
package org.apache.beam.sdk.transforms;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import com.code_intelligence.jazzer.mutation.annotation.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class TransformsFuzzTest {
  @FuzzTest
  void fuzzRegexTransform(@NotNull String regex, @NotNull String inputText) {
    if (regex.isEmpty()) {
      return;
    }
    try {
      Pattern.compile(regex);
    } catch (PatternSyntaxException e) {
      return;
    }

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<String> input = p.apply(Create.of(inputText));
    PCollection<List<String>> matches = input.apply(Regex.allMatches(regex));
    p.run().waitUntilFinish();
  }

  @FuzzTest(maxExecutions = 1)
  void fuzzTransforms(FuzzedDataProvider data) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    // Create input data from fuzzer
    int numElements = data.consumeInt(1, 1000);
    List<String> input = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      input.add(data.consumeString(50)); // random bounded strings
    }

    PCollection<String> pc = p.apply("CreateInput", Create.of(input));

    // Randomly pick a transform
    switch (data.consumeInt(0, 7)) {
      case 0:
        pc.apply(
            "MapElements",
            MapElements.into(TypeDescriptors.strings())
                .via(s -> s == null ? "" : s.toUpperCase(Locale.ROOT)));
        break;
      case 1:
        pc.apply("Filter", Filter.by(s -> s != null && s.contains("a")));
        break;
      case 2:
        pc.apply(
                "GroupByKey",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                    .via(s -> KV.of(s, s)))
            .apply(GroupByKey.create());
        break;
      case 3:
        pc.apply(
            "Combine",
            Combine.globally(
                    (Iterable<String> strs) -> {
                      StringBuilder sb = new StringBuilder();
                      for (String s : strs) {
                        if (s != null) sb.append(s);
                      }
                      return sb.toString();
                    })
                .withoutDefaults());
        break;
      case 4:
        pc.apply("Partition", Partition.of(3, (elem, n) -> elem.length() % n));
        break;
      case 5:
        pc.apply(
            "Window",
            Window.<String>into(
                FixedWindows.of(Duration.standardSeconds(data.consumeInt(1, 100)))));
        break;
      case 6:
        // Flatten requires multiple collections
        PCollection<String> pc2 = p.apply("CreateOther", Create.of(input));
        PCollectionList.of(pc).and(pc2).apply("Flatten", Flatten.pCollections());
        break;
    }

    // Run pipeline
    p.run().waitUntilFinish();
  }
}
