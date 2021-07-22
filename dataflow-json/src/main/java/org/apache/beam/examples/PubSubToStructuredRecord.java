package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class PubSubToStructuredRecord extends DoFn<PubsubMessage, StructuredRecord> {
  private Schema schema;  // Expected schema of the output.

  public PubSubToStructuredRecord(Schema schema) {
    this.schema = schema;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    PubsubMessage message = context.element();
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getField("message") != null) {
      builder.set("message", message.getPayload());
    }
    // TODO: Figure out why we are getting null ids.
    String id = message.getMessageId() == null ? "1" : message.getMessageId();
    if (schema.getField("id") != null) {
      builder.set("id", id);
    }
    if (schema.getField("attributes") != null) {
      builder.set("attributes", message.getAttributeMap());
    }
    if (schema.getField("timestamp") != null) {
      builder.set("timestamp", context.timestamp().toDateTime());
    }
    context.output(builder.build());
  }
}
