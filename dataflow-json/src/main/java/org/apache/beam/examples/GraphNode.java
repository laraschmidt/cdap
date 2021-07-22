package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;

public class GraphNode {
  public String name;
  public ArrayList<GraphNode> children = new ArrayList<>();
  public GraphNode parent = null;
  public PCollection<StructuredRecord> output = null;
  public ETLStage config = null;
  public Schema outputSchema = null;

  public GraphNode(String name) {
    this.name = name;
  }

  public Schema getFieldSchema(String fieldName) {
    return outputSchema.getField(fieldName).getSchema();
  }
}
