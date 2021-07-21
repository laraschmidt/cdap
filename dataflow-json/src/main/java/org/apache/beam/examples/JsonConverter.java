package org.apache.beam.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.util.Map;

public class JsonConverter extends SimpleFunction<StructuredRecord, StructuredRecord> {
  private Schema schema;      // Expected output schema.
  private String inputField;  // Field with the json to convert.

  public JsonConverter(String inputField, Schema schemaOut) {
    this.schema = schemaOut;
    this.inputField = inputField;
  }

  @Override
  public StructuredRecord apply(StructuredRecord message) {
    String json = message.get(inputField);
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, String> map = mapper.readValue(json, Map.class);
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      for (Schema.Field field : schema.getFields()) {
        builder.convertAndSet(field.getName(), map.get(field.getName()));
      }
      return builder.build();
    } catch (JsonProcessingException ex) {
      throw new RuntimeException(ex);
    }
  }
}
