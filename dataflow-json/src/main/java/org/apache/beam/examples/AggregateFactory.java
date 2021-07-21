package org.apache.beam.examples;

import io.cdap.plugin.batch.aggregator.function.AggregateFunction;

public interface AggregateFactory<T, V extends AggregateFunction> {
  AggregateFunction<T, V> create();
}
