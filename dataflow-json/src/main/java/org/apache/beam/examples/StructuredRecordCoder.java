package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;

// TODO: Figure out if it's safe to use the java serialization here (equals seems pretty straight-forward here.).
// If it is not, then add actual serialization handling - but for that I need to understand this Schema class
// better.
public class StructuredRecordCoder extends Coder<StructuredRecord> {
  public static StructuredRecordCoder of() {
    return new StructuredRecordCoder();
  }

  @Override
  public void encode(StructuredRecord value, OutputStream outStream) throws CoderException, IOException {
    ObjectOutputStream obj = new ObjectOutputStream(outStream);
    obj.writeObject(value);
  }

  @Override
  public StructuredRecord decode(InputStream inStream) throws CoderException, IOException {
    ObjectInputStream obj = new ObjectInputStream(inStream);
    try {
      return (StructuredRecord) obj.readObject();
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
