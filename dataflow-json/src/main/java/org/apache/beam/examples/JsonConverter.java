package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.io.IOException;

public class JsonConverter extends SimpleFunction<StructuredRecord, StructuredRecord> {
  private Schema schema;      // Expected output schema.
  private String inputField;  // Field with the json to convert.

  public JsonConverter(String inputField, Schema schemaOut) {
    this.schema = schemaOut;
    this.inputField = inputField;
  }

  public static String print(StructuredRecord record) {
    Schema schema = record.getSchema();
    if (schema.getType() == Schema.Type.RECORD) {
      String s = "";
      for (Schema.Field field : record.getSchema().getFields()) {
        s += field + ": " + record.get(field.getName()).toString() + ",";
      }
      return s;
    } else {
      return schema.getType().toString();
    }
  }

  @Override
  public StructuredRecord apply(StructuredRecord message) {
    byte[] json = message.get(inputField);
    try {
      return StructuredRecordStringConverter.fromJsonString(new String(json), schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
