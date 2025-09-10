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
package org.apache.beam.sdk;

import com.code_intelligence.jazzer.junit.FuzzTest;
import com.code_intelligence.jazzer.mutation.annotation.NotNull;
import java.util.List;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PipelineFuzzTest {
  @FuzzTest(maxExecutions = 1)
  void fuzzPipeline(@NotNull List<String> inputs) throws RuntimeException {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    try {
      PCollection<String> inputCollection =
          p.apply(Create.of(inputs).withCoder(StringUtf8Coder.of()));
      PCollection<String> processed =
          inputCollection
              .apply(
                  "MapToUpper",
                  MapElements.into(TypeDescriptors.strings())
                      .via(
                          (SerializableFunction<String, String>)
                              s -> s == null ? "NULL" : s.toUpperCase()))
              .apply("FilterNonEmpty", Filter.by(s -> !s.isEmpty()))
              .apply(
                  "AddPrefix",
                  MapElements.into(TypeDescriptors.strings())
                      .via((SerializableFunction<String, String>) s -> "PREFIX_" + s));
      p.run().waitUntilFinish();
    } catch (RuntimeException e) {
      // Check if the exception is the expected Coder failure
      if (e.getMessage() != null
          && e.getMessage()
              .contains("Unable to apply Create Create.Values using Coder StringUtf8Coder")) {
        return;
      }
      throw new RuntimeException(e);
    }
  }
}
