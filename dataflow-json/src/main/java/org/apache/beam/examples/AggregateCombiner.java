package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.batch.aggregator.function.AggregateFunction;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;

import java.util.Arrays;

// A combiner that uses AggregateFunctions to combine.
public class AggregateCombiner<T>
  extends Combine.CombineFn<StructuredRecord, AggregateFunction<T, AggregateFunction>, StructuredRecord> {
  AggregateFactory<T, AggregateFunction> factory;  // A factory that generates new AggregateFunctions.
  Schema.Field outputField;          // The field name of the output.
  Schema schema;

  public AggregateCombiner(AggregateFactory<T, AggregateFunction> factory, String outputFieldName) {
    System.out.println("HEY");
    this.factory = factory;
    outputField = Schema.Field.of(outputFieldName, factory.create().getOutputSchema());
    schema = Schema.recordOf("etlSchemaBody", Arrays.asList(outputField));
  }

  @Override
  public AggregateFunction<T, AggregateFunction> createAccumulator() {
    System.out.println("CREATE");
    return factory.create();

  }

  @Override
  public AggregateFunction<T, AggregateFunction> addInput(AggregateFunction<T, AggregateFunction> accum, StructuredRecord input) {
    System.out.println("ADD INPUT");
    accum.mergeValue(input);
    return accum;
  }

  @Override
  public AggregateFunction<T, AggregateFunction> mergeAccumulators(Iterable<AggregateFunction<T, AggregateFunction>> accums) {
    System.out.println("ACCUM");
    AggregateFunction<T, AggregateFunction> merged = factory.create();
    for (AggregateFunction<T, AggregateFunction> accum : accums) {
      merged.mergeAggregates(accum);
    }
    return merged;
  }

  @Override
  public Coder<AggregateFunction<T, AggregateFunction>> getAccumulatorCoder(CoderRegistry registry, Coder<StructuredRecord> inputCoder) throws CannotProvideCoderException {
    return new AggregateFunctionCoder<>();
  }

  @Override
  public StructuredRecord extractOutput(AggregateFunction<T, AggregateFunction> accum) {
    return StructuredRecord.builder(schema).set(outputField.getName(), accum.getAggregate()).build();
  }

  public Schema.Field getOutputField() {
    return outputField;
  }
}
