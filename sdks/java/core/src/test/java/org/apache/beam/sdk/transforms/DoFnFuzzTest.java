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

import com.code_intelligence.jazzer.junit.FuzzTest;
import com.code_intelligence.jazzer.mutation.annotation.NotNull;
import com.code_intelligence.jazzer.mutation.annotation.WithSize;
import java.util.List;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class DoFnFuzzTest {
  static class SimpleDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> out) {
      out.output(element);
    }
  }

  static class MultiOutputDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> out) {
      if (element != null && !element.isEmpty()) {
        for (int i = 0; i < element.length(); i++) {
          char c = element.charAt(i);
          out.output(String.valueOf(c));
        }
      }
    }
  }

  static class SimpleStatefulDoFn extends DoFn<KV<String, String>, String> {
    @StateId("counter")
    private final StateSpec<ValueState<Integer>> counterSpec = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(
        @Element KV<String, String> element,
        @StateId("counter") ValueState<Integer> counter,
        OutputReceiver<String> out) {
      Integer count = counter.read();
      count = (count == null) ? 1 : count + 1;
      counter.write(count);
      out.output(element.getValue() + "_" + count);
    }
  }

  @FuzzTest
  public void fuzzSimpleDoFn(@NotNull @WithSize(min = 1) List<String> inputs)
      throws RuntimeException {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    try {
      PCollection<String> input = p.apply(Create.of(inputs));
      input.apply(ParDo.of(new SimpleDoFn()));
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

  @FuzzTest
  public void fuzzMultiOutputDoFn(@NotNull @WithSize(min = 1) List<String> inputs)
      throws RuntimeException {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    try {
      PCollection<String> input = p.apply(Create.of(inputs));
      input.apply(ParDo.of(new MultiOutputDoFn()));
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

  @FuzzTest(maxExecutions = 1)
  public void fuzzStatefulDoFn(@NotNull @WithSize(min = 1) List<String> inputs)
      throws RuntimeException {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    try {
      PCollection<String> input = p.apply(Create.of(inputs));
      PCollection<KV<String, String>> keyedInput = input.apply(WithKeys.of("constant-key"));
      keyedInput.apply(ParDo.of(new SimpleStatefulDoFn()));
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
