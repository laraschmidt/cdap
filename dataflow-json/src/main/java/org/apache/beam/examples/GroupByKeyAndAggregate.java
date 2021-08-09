package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

// A PTransform that groups by key and aggregates based on given aggregator.
public class GroupByKeyAndAggregate extends PTransform<PCollection<StructuredRecord>,
  PCollection<StructuredRecord>> {
  Schema.Field groupByField;     // The field in inputSchema to group by.
  Combine.CombineFn<StructuredRecord, ?, StructuredRecord> combiner;

  public GroupByKeyAndAggregate(Schema.Field groupByField,
                                Combine.CombineFn<StructuredRecord, ?, StructuredRecord> combiner) {
    this.groupByField = groupByField;
    this.combiner = combiner;
  }

  @Override
  public PCollection<StructuredRecord> expand(PCollection<StructuredRecord> input) {
    KvCoder<StructuredRecord, StructuredRecord> kvCoder = KvCoder.of(new StructuredRecordCoder(groupByField), new StructuredRecordCoder());
    return input.apply("get keys", WithKeys.of(new GetFieldAsRecord(groupByField)))
      .apply(Window.into(FixedWindows.of(Duration.standardMinutes(3)))).setCoder(kvCoder)
      .apply(GroupByKey.<StructuredRecord, StructuredRecord>create())
      .apply("combine", Combine.groupedValues(combiner)).setCoder(kvCoder)
      .apply("reduce", MapElements.via(new MergeKVStructuredRecords()));
  }
}
