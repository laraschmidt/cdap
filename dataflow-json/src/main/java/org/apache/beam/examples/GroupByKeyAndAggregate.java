package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.batch.aggregator.function.Sum;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

// A PTransform that groups by key and aggregates based on given aggregator.
public class GroupByKeyAndAggregate extends PTransform<PCollection<StructuredRecord>,
  PCollection<StructuredRecord>> {
  String groupByField;     // The field in inputSchema to group by.
  Schema inputSchema;      // The input schema.
  String valueField;       // The field to use in aggregation.
  String outputValueField; // The filed to output the result as.

  Schema keySchema;        // The schema of the key, generated from groupByField and inputSchema.

  public GroupByKeyAndAggregate(Schema inputSchema, String groupByField, String valueField, String outputValueField) {
    this.groupByField = groupByField;
    this.inputSchema = inputSchema;
    this.valueField = valueField;
    this.outputValueField = outputValueField;

    keySchema = Schema.of(inputSchema.getField(groupByField).getSchema().getType());
  }

  @Override
  public PCollection<StructuredRecord> expand(PCollection<StructuredRecord> input) {
    Combine.CombineFn<StructuredRecord, ?, StructuredRecord> a =
      new AggregateCombiner(() -> {
        return new Sum(valueField, inputSchema);
      }, outputValueField);

    PCollection<KV<StructuredRecord, StructuredRecord>> withKeys =
      input.apply("get keys", WithKeys.of(new GetFieldAsRecord(groupByField, keySchema)));
    PCollection<KV<StructuredRecord, Iterable<StructuredRecord>>> grouped =
      withKeys.apply(GroupByKey.<StructuredRecord, StructuredRecord>create());
    PCollection<KV<StructuredRecord, StructuredRecord>> reduced = grouped.apply("Combine", Combine.groupedValues(a));
    return reduced.apply("reduce", MapElements.via(new MergeKVStructuredRecords()));
  }
}
