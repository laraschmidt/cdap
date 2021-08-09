package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.Arrays;

public class GetFieldAsRecord implements SerializableFunction<StructuredRecord, StructuredRecord> {
  private Schema.Field field;  // Name of the field to get.
  private Schema recordSchema;

  public GetFieldAsRecord(Schema.Field field) {
    this.field = field;
    recordSchema = Schema.recordOf("etlSchemaBody", field);
  }

  public StructuredRecord apply(StructuredRecord message) {
    // schema == null || schema.getType() != Schema.Type.RECORD || schema.getFields().size() < 1
    System.out.println("Getting key" + field.getName() + " " + JsonConverter.print(message));
    return StructuredRecord.builder(recordSchema).set(field.getName(), message.get(field.getName())).build();
  }
}
