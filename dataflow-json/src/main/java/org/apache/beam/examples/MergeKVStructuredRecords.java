package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

// A DoFn that merges a KV or StructuredRecord into a single StructuredRecord with fields from both.
public class MergeKVStructuredRecords extends SimpleFunction<KV<StructuredRecord, StructuredRecord>, StructuredRecord> {
  @Override
  public StructuredRecord apply(KV<StructuredRecord, StructuredRecord> message) {
    Schema union = Schema.unionOf(message.getKey().getSchema(), message.getValue().getSchema());
    StructuredRecord.Builder builder = StructuredRecord.builder(union);
    StructuredRecord[] records = {message.getKey(), message.getValue()};
    for (StructuredRecord record : records) {
      for (Schema.Field field : record.getSchema().getFields()) {
        builder.set(field.getName(), record.get(field.getName()));
      }
    }
    return builder.build();
  }
}
