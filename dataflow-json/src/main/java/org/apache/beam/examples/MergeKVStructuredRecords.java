package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;

// A DoFn that merges a KV or StructuredRecord into a single StructuredRecord with fields from both.
public class MergeKVStructuredRecords extends SimpleFunction<KV<StructuredRecord, StructuredRecord>, StructuredRecord> {
  @Override
  public StructuredRecord apply(KV<StructuredRecord, StructuredRecord> message) {
    System.out.println("MERGING");
    ArrayList<Schema.Field> fields = new ArrayList<>();
    StructuredRecord[] records = {message.getKey(), message.getValue()};
    for (StructuredRecord record : records) {
      Schema schema = record.getSchema();
      if (schema.getType() != Schema.Type.RECORD) {
        throw new RuntimeException("Expected record types.");
      }
      for (Schema.Field field : schema.getFields()) {
        fields.add(field);
      }
    }

    Schema outputSchema = Schema.recordOf("etl", fields);
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (StructuredRecord record : records) {
      for (Schema.Field field : record.getSchema().getFields()) {
        builder.set(field.getName(), record.get(field.getName()));
      }
    }
    System.out.println("OUTPUT!" + builder.build().toString());
    return builder.build();
  }
}
