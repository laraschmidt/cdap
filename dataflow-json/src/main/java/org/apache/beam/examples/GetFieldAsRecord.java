package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class GetFieldAsRecord implements SerializableFunction<StructuredRecord, StructuredRecord> {
  private String fieldName;  // Name of the field to get.
  private Schema outputSchema;  // Schema of the output StructuredRecord.

  public GetFieldAsRecord(String fieldName, Schema outputSchema) {
    this.fieldName = fieldName;
    this.outputSchema = outputSchema;
  }

  public StructuredRecord apply(StructuredRecord message) {
    return StructuredRecord.builder(outputSchema).set(fieldName, message.get(fieldName)).build();
  }
}
