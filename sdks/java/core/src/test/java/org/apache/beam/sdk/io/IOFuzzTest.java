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
package org.apache.beam.sdk.io;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class IOFuzzTest {
  @FuzzTest(maxExecutions = 1)
  void fuzzIO(FuzzedDataProvider data) throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    int numRecords = data.consumeInt(1, 100);
    List<String> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      records.add(data.consumeString(100));
    }

    PCollection<String> input =
        p.apply("CreateInput", Create.of(records).withCoder(StringUtf8Coder.of()));

    switch (data.consumeInt(0, 1)) {
      case 0:
        {
          Path tmpFile = Files.createTempFile("beam-fuzz", ".txt");
          ResourceId resource = LocalResources.fromFile(tmpFile.toFile(), false);

          input.apply("WriteTextIO", TextIO.write().to(resource.toString()).withoutSharding());
          p.run().waitUntilFinish();

          Pipeline p2 = Pipeline.create(options);
          p2.apply("ReadTextIO", TextIO.read().from(resource.toString()))
              .apply("Map", MapElements.into(TypeDescriptors.strings()).via(String::toLowerCase));
          p2.run().waitUntilFinish();
          break;
        }
      case 1:
        {
          Path tmpFile = Files.createTempFile("beam-fuzz", ".tfrecord");
          String path = tmpFile.toAbsolutePath().toString();

          List<byte[]> bytes = new ArrayList<>();
          for (String s : records) {
            bytes.add(s.getBytes(StandardCharsets.UTF_8));
          }
          p = Pipeline.create(options);
          p.apply("CreateBytes", Create.of(bytes))
              .apply("WriteTF", TFRecordIO.write().to(path).withoutSharding());
          p.run().waitUntilFinish();

          Pipeline p2 = Pipeline.create(options);
          p2.apply("ReadTF", TFRecordIO.read().from(path + "*"))
              .apply("MapBytes", MapElements.into(TypeDescriptors.integers()).via(b -> b.length));
          p2.run().waitUntilFinish();
          break;
        }
    }
  }
}
