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
package org.apache.beam.sdk.schemas;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.transforms.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class SchemaCoderFuzzTest {
  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addStringField("name")
          .addInt32Field("age")
          .addBooleanField("active")
          .build();

  private static final Coder<Row> CODER = RowCoder.of(TEST_SCHEMA);

  @FuzzTest(maxDuration = "15m", maxExecutions = 1)
  public void fuzzSchemaCoder(FuzzedDataProvider data) throws IOException {
    byte[] bytes = data.consumeBytes(1000);
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      CODER.decode(in);
    } catch (CoderException ignored) {
    }
  }

  @FuzzTest(maxDuration = "30m", maxExecutions = 1)
  public void fuzzComplexSchemaOperations(FuzzedDataProvider data) throws Exception {
    Schema complexSchema = buildComplexSchema(data);

    testRowOperations(data, complexSchema);
    testSchemaTransformations(data, complexSchema);
    testFieldTypeEdgeCases();
    testCoderVariations(data, complexSchema);
  }

  @FuzzTest(maxDuration = "15m", maxExecutions = 1)
  public void fuzzSchemaTransforms(FuzzedDataProvider data) {
    try {
      PipelineOptions options = PipelineOptionsFactory.create();
      options.setRunner(DirectRunner.class);
      Pipeline p = Pipeline.create(options);

      int fieldCount = data.consumeInt(1, 5); // keep schema small
      Schema.Builder builder = Schema.builder();
      for (int i = 0; i < fieldCount; i++) {
        builder.addStringField("f" + i);
      }
      Schema schema = builder.build();

      Row.Builder rowBuilder = Row.withSchema(schema);
      for (int i = 0; i < fieldCount; i++) {
        rowBuilder.addValue(data.consumeString(20));
      }
      Row row = rowBuilder.build();

      PCollection<Row> rows = p.apply(Create.of(row).withRowSchema(schema));

      List<String> fieldNames = new ArrayList<>();
      for (int i = 0; i < fieldCount; i++) {
        fieldNames.add("f" + i);
      }

      if (!fieldNames.isEmpty()) {
        int selectCount = data.consumeInt(1, fieldNames.size());
        String[] selected = fieldNames.subList(0, selectCount).toArray(new String[0]);
        rows.apply("select", Select.fieldNames(selected));
      }

      if (!fieldNames.isEmpty()) {
        int dropIndex = data.consumeInt(0, fieldNames.size() - 1);
        rows.apply("drop", DropFields.fields(fieldNames.get(dropIndex)));
      }

      if (fieldNames.size() > 1) {
        String oldName = fieldNames.get(0);
        String newName = "renamed_" + data.consumeString(10);
        rows.apply("rename", RenameFields.<Row>create().rename(oldName, newName));
      }

      String newName = "new_" + Math.abs(data.consumeInt(0, 9999));
      String defaultValue = "def_" + data.consumeString(6);
      rows.apply(
          "add", AddFields.<Row>create().field(newName, Schema.FieldType.STRING, defaultValue));

      Schema.Builder castSb = Schema.builder();
      for (int i = 0; i < Math.max(1, fieldCount); i++) {
        castSb.addNullableField("x" + i, Schema.FieldType.STRING);
      }
      Schema castSchema = castSb.build();

      rows.apply("cast", Cast.widening(castSchema));

      p.run().waitUntilFinish();

    } catch (IllegalArgumentException | UnsupportedOperationException ignored) {
    }
  }

  private Schema buildComplexSchema(FuzzedDataProvider data) {
    Schema.Builder builder = Schema.builder();
    int fieldCount = data.consumeInt(1, 8);

    for (int i = 0; i < fieldCount; i++) {
      String fieldName = "field_" + i;
      boolean nullable = data.consumeBoolean();

      switch (data.consumeInt(0, 11)) {
        case 0:
          builder.addField(fieldName, Schema.FieldType.STRING.withNullable(nullable));
          break;
        case 1:
          builder.addField(fieldName, Schema.FieldType.INT64.withNullable(nullable));
          break;
        case 2:
          builder.addField(fieldName, Schema.FieldType.FLOAT.withNullable(nullable));
          break;
        case 3:
          builder.addField(fieldName, Schema.FieldType.DECIMAL.withNullable(nullable));
          break;
        case 4:
          builder.addField(fieldName, Schema.FieldType.DATETIME.withNullable(nullable));
          break;
        case 5:
          builder.addField(fieldName, Schema.FieldType.BYTES.withNullable(nullable));
          break;
        case 6:
          Schema.FieldType elementType =
              data.consumeBoolean() ? Schema.FieldType.STRING : Schema.FieldType.INT32;
          builder.addField(fieldName, Schema.FieldType.array(elementType).withNullable(nullable));
          break;
        case 7:
          Schema.FieldType valueType =
              data.consumeBoolean() ? Schema.FieldType.INT32 : Schema.FieldType.DOUBLE;
          builder.addField(
              fieldName,
              Schema.FieldType.map(Schema.FieldType.STRING, valueType).withNullable(nullable));
          break;
        case 8:
          Schema nestedSchema = buildNestedSchema(data, 2);
          builder.addField(fieldName, Schema.FieldType.row(nestedSchema).withNullable(nullable));
          break;
        case 9:
          builder.addField(
              fieldName,
              Schema.FieldType.logicalType(EnumerationType.create("A", "B", "C"))
                  .withNullable(nullable));
          break;
        case 10:
          builder.addField(
              fieldName, Schema.FieldType.iterable(Schema.FieldType.STRING).withNullable(nullable));
          break;
        case 11:
          Schema schemaWithMetadata =
              Schema.builder()
                  .addStringField("meta_field")
                  .setOptions(
                      Schema.Options.builder()
                          .setOption("custom_option", Schema.FieldType.STRING, "test_value")
                          .build())
                  .build();
          builder.addField(
              fieldName, Schema.FieldType.row(schemaWithMetadata).withNullable(nullable));
          break;
      }
    }

    if (data.consumeBoolean()) {
      builder.setOptions(
          Schema.Options.builder()
              .setOption("schema_version", Schema.FieldType.STRING, data.consumeString(10))
              .setOption("created_by", Schema.FieldType.STRING, "fuzz_test")
              .build());
    }

    return builder.build();
  }

  private Schema buildNestedSchema(FuzzedDataProvider data, int maxDepth) {
    if (maxDepth <= 0) {
      return Schema.builder().addStringField("leaf").build();
    }
    Schema.Builder builder = Schema.builder();
    int fieldCount = data.consumeInt(1, 3);
    for (int i = 0; i < fieldCount; i++) {
      if (data.consumeBoolean() && maxDepth > 1) {
        builder.addRowField("nested_" + i, buildNestedSchema(data, maxDepth - 1));
      } else {
        builder.addStringField("simple_" + i);
      }
    }
    return builder.build();
  }

  private void testRowOperations(FuzzedDataProvider data, Schema schema) throws Exception {
    Row row = generateRandomRow(data, schema);

    for (int i = 0; i < schema.getFieldCount(); i++) {
      Schema.Field field = schema.getField(i);
      row.getValue(i);
      row.getValue(field.getName());
      testTypedGetters(row, field, i);
    }

    if (schema.getFieldCount() > 0) {
      Row.Builder modifiedBuilder = Row.withSchema(row.getSchema()).addValues(row.getValues());
      int randomIndex = data.consumeInt(0, schema.getFieldCount() - 1);
      Schema.Field field = schema.getField(randomIndex);
      Object newValue = generateValueForField(data, field);
      if (newValue != null) {
        try {
          modifiedBuilder.withFieldValue(field.getName(), newValue);
        } catch (IllegalStateException ignored) {
        }

        Row modifiedRow = modifiedBuilder.build();
      }
    }
  }

  private void testTypedGetters(Row row, Schema.Field field, int index) {
    try {
      switch (field.getType().getTypeName()) {
        case STRING:
          row.getString(index);
          row.getString(field.getName());
          break;
        case INT32:
          row.getInt32(index);
          row.getInt32(field.getName());
          break;
        case INT64:
          row.getInt64(index);
          row.getInt64(field.getName());
          break;
        case DOUBLE:
          row.getDouble(index);
          row.getDouble(field.getName());
          break;
        case FLOAT:
          row.getFloat(index);
          row.getFloat(field.getName());
          break;
        case BOOLEAN:
          row.getBoolean(index);
          row.getBoolean(field.getName());
          break;
        case DECIMAL:
          row.getDecimal(index);
          row.getDecimal(field.getName());
          break;
        case DATETIME:
          row.getDateTime(index);
          row.getDateTime(field.getName());
          break;
        case BYTES:
          row.getBytes(index);
          row.getBytes(field.getName());
          break;
        case ARRAY:
          row.getArray(index);
          row.getArray(field.getName());
          break;
        case MAP:
          row.getMap(index);
          row.getMap(field.getName());
          break;
        case ROW:
          row.getRow(index);
          row.getRow(field.getName());
          break;
        default:
          break;
      }
    } catch (Exception ignored) {
    }
  }

  private void testSchemaTransformations(FuzzedDataProvider data, Schema schema) {
    if (schema.getFieldCount() > 1) {
      int numFieldsToSelect = data.consumeInt(1, schema.getFieldCount());
      Schema.Builder selectedBuilder = Schema.builder();
      for (int i = 0; i < numFieldsToSelect; i++) {
        int fieldIndex = data.consumeInt(0, schema.getFieldCount() - 1);
        Schema.Field field = schema.getField(fieldIndex);
        selectedBuilder.addField(field);
      }
      selectedBuilder.build();
    }

    if (schema.getFieldCount() > 0) {
      Schema.Builder renamedBuilder = Schema.builder();
      for (Schema.Field field : schema.getFields()) {
        renamedBuilder.addField(Schema.Field.of("renamed_" + field.getName(), field.getType()));
      }
      renamedBuilder.build();
    }
  }

  private void testFieldTypeEdgeCases() {
    try {
      Schema.FieldType nestedArray =
          Schema.FieldType.array(Schema.FieldType.array(Schema.FieldType.STRING));
      Schema complexValueSchema =
          Schema.builder().addStringField("key").addInt32Field("value").build();
      Schema.FieldType complexMap =
          Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.row(complexValueSchema));
      Schema deepSchema =
          Schema.builder()
              .addRowField(
                  "level1",
                  Schema.builder()
                      .addRowField("level2", Schema.builder().addStringField("deep_field").build())
                      .build())
              .build();
    } catch (Exception ignored) {
    }
  }

  private void testCoderVariations(FuzzedDataProvider data, Schema schema) throws Exception {
    Row row = generateRandomRow(data, schema);
    Coder<Row> standardCoder = SchemaCoder.of(schema);

    try {
      standardCoder.verifyDeterministic();
    } catch (Exception ignored) {
    }
    try {
      standardCoder.structuralValue(row);
    } catch (Exception ignored) {
    }

    ByteArrayOutputStream out1 = new ByteArrayOutputStream();
    standardCoder.encode(row, out1);

    ByteArrayInputStream in1 = new ByteArrayInputStream(out1.toByteArray());
    Row decoded1 = standardCoder.decode(in1);
  }

  private Row generateRandomRow(FuzzedDataProvider data, Schema schema) {
    Row.Builder builder = Row.withSchema(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.addValue(generateValueForField(data, field));
    }
    return builder.build();
  }

  private Object generateValueForField(FuzzedDataProvider data, Schema.Field field) {
    Schema.FieldType type = field.getType();

    // Handle nullable fields
    if (type.getNullable() && data.consumeBoolean()) {
      switch (type.getTypeName()) {
        case ARRAY:
        case ITERABLE:
          return new ArrayList<>();
        case MAP:
          return new HashMap<>();
        default:
          return null;
      }
    }

    switch (type.getTypeName()) {
      case STRING:
        return data.consumeString(data.consumeInt(0, 100));
      case INT32:
        return data.consumeInt();
      case INT64:
        return data.consumeLong();
      case DOUBLE:
        return data.consumeDouble();
      case FLOAT:
        return data.consumeFloat();
      case BOOLEAN:
        return data.consumeBoolean();
      case DECIMAL:
        return BigDecimal.valueOf(data.consumeDouble());
      case DATETIME:
        long millis = data.consumeLong(0, System.currentTimeMillis());
        return new org.joda.time.Instant(millis);
      case BYTES:
        return data.consumeBytes(data.consumeInt(0, 100));

      case ARRAY:
      case ITERABLE:
        int arraySize = data.consumeInt(0, 5);
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < arraySize; i++) {
          list.add(generateValueForFieldType(data, type.getCollectionElementType()));
        }
        return list;

      case MAP:
        int mapSize = data.consumeInt(0, 3);
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < mapSize; i++) {
          Object key = generateValueForFieldType(data, type.getMapKeyType());
          Object value = generateValueForFieldType(data, type.getMapValueType());
          if (key != null) {
            map.put(key, value);
          }
        }
        return map;

      case ROW:
        return generateRandomRow(data, type.getRowSchema());

      case LOGICAL_TYPE:
        if (type.getLogicalType() instanceof EnumerationType) {
          EnumerationType enumType = (EnumerationType) type.getLogicalType();
          List<String> values = enumType.getValues();
          if (!values.isEmpty()) {
            String selected = values.get(data.consumeInt(0, values.size() - 1));
            return enumType.valueOf(selected); // ✅ Beam-compatible
          }
        }
        return null;

      default:
        return null;
    }
  }

  // Helper for FieldType (used in ARRAY, MAP, etc.)
  private Object generateValueForFieldType(FuzzedDataProvider data, Schema.FieldType type) {
    // Create a temporary field to reuse the same logic
    Schema.Field tempField = Schema.Field.of("temp", type);
    return generateValueForField(data, tempField);
  }
}
