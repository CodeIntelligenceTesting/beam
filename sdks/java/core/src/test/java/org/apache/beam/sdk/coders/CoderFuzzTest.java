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
package org.apache.beam.sdk.coders;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.output.ByteArrayOutputStream;

public class CoderFuzzTest {
  private static final List<Coder<?>> CODERS =
      Arrays.asList(
          StringUtf8Coder.of(),
          VarIntCoder.of(),
          BigEndianIntegerCoder.of(),
          ByteArrayCoder.of(),
          DoubleCoder.of(),
          BooleanCoder.of(),
          IterableCoder.of(StringUtf8Coder.of()),
          ListCoder.of(VarIntCoder.of()),
          KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

  @FuzzTest(maxExecutions = 1)
  void fuzzCoder(FuzzedDataProvider fdp) throws IOException {
    Coder<Object> coder = (Coder<Object>) fdp.pickValue(CODERS);

    try {
      Object value = randomValueForCoder(coder, fdp);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      coder.encode(value, out);
      byte[] encoded = out.toByteArray();
      ByteArrayInputStream in = new ByteArrayInputStream(encoded);
      Object decoded = coder.decode(in);
    } catch (CoderException ignored) {
    }
  }

  private Object randomValueForCoder(Coder<?> coder, FuzzedDataProvider fdp) {
    if (coder instanceof StringUtf8Coder) {
      return fdp.consumeString(100);
    } else if (coder instanceof VarIntCoder || coder instanceof BigEndianIntegerCoder) {
      return fdp.consumeInt();
    } else if (coder instanceof DoubleCoder) {
      return fdp.consumeDouble();
    } else if (coder instanceof BooleanCoder) {
      return fdp.consumeBoolean();
    } else if (coder instanceof ByteArrayCoder) {
      return fdp.consumeBytes(50);
    } else if (coder instanceof IterableCoder) {
      int size = fdp.consumeInt(0, 5);
      List<String> list = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        list.add(fdp.consumeString(50));
      }
      return list;
    } else if (coder instanceof ListCoder) {
      int size = fdp.consumeInt(0, 5);
      List<Integer> list = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        list.add(fdp.consumeInt());
      }
      return list;
    } else if (coder instanceof KvCoder) {
      return KV.of(fdp.consumeString(50), fdp.consumeInt());
    }

    return null;
  }
}
