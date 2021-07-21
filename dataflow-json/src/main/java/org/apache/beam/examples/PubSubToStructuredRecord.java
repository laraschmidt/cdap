package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class PubSubToStructuredRecord extends SimpleFunction<PubsubMessage, StructuredRecord> {
  private Schema schema;  // Expected schema of the output.

  public PubSubToStructuredRecord(Schema schema) {
    this.schema = schema;
  }

  @Override
  public StructuredRecord apply(PubsubMessage message) {
    // For now, just treat it as a message.
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    builder.set("message", message.getPayload());
    return builder.build();
    // TODO: Actually use the schema and read other fields.
  }
}
