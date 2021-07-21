package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.batch.aggregator.function.AggregateFunction;
import org.apache.beam.sdk.transforms.Combine;

// A combiner that uses AggregateFunctions to combine.
public class AggregateCombiner<T, V extends AggregateFunction>
  extends Combine.CombineFn<StructuredRecord, AggregateFunction<T, V>, StructuredRecord> {
  AggregateFactory<T, V> factory;  // A factory that generates new AggregateFunctions.
  String outputFieldName;          // The field name of the output.

  public AggregateCombiner(AggregateFactory<T, V> factory, String outputFieldName) {
    this.factory = factory;
    this.outputFieldName = outputFieldName;
  }

  @Override
  public AggregateFunction<T, V> createAccumulator() {
    return factory.create();
  }

  @Override
  public AggregateFunction<T, V> addInput(AggregateFunction<T, V> accum, StructuredRecord input) {
    accum.mergeValue(input);
    return accum;
  }

  @Override
  public AggregateFunction<T, V> mergeAccumulators(Iterable<AggregateFunction<T, V>> accums) {
    AggregateFunction<T, V> merged = factory.create();
    for (AggregateFunction<T, V> accum : accums) {
      merged.mergeAggregates((V) accum);
    }
    return merged;
  }

  @Override
  public StructuredRecord extractOutput(AggregateFunction<T, V> accum) {
    return StructuredRecord.builder(accum.getOutputSchema()).set(outputFieldName, accum.getAggregate()).build();
  }
}
