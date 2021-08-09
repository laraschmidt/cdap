package org.apache.beam.examples;

import io.cdap.plugin.batch.aggregator.function.AggregateFunction;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;

public class AggregateFunctionCoder<T> extends Coder<AggregateFunction<T, AggregateFunction>> {
  public static AggregateFunctionCoder of() {
    return new AggregateFunctionCoder();
  }

  @Override
  public void encode(AggregateFunction<T, AggregateFunction> value, OutputStream outStream) throws CoderException, IOException {
    ObjectOutputStream obj = new ObjectOutputStream(outStream);
    obj.writeObject(value);
  }

  @Override
  public AggregateFunction<T, AggregateFunction> decode(InputStream inStream) throws CoderException, IOException {
    ObjectInputStream obj = new ObjectInputStream(inStream);
    try {
      return (AggregateFunction<T, AggregateFunction>) obj.readObject();
    } catch(ClassNotFoundException e) {
      throw new CoderException(e);
    }

  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
}
