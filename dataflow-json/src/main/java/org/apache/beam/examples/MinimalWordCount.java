/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.proto.Connection;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.plugin.batch.aggregator.function.Count;
import io.cdap.plugin.batch.aggregator.function.Sum;
import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class MinimalWordCount {

  public static class StructuredRecordToString extends SimpleFunction<StructuredRecord, String> {
    @Override
    public String apply(StructuredRecord record) {
      return record.toString();
    }
  }

  public static Collection<GraphNode> getNodesFromFile(String file) {

    StringBuilder contentBuilder = new StringBuilder();
    try (Stream<String> stream = Files.lines(Paths.get(file), StandardCharsets.UTF_8)) {
      stream.forEach(s -> contentBuilder.append(s).append("\n"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Gson gson = new Gson();
    Type configType = new TypeToken<AppRequest<DataStreamsConfig>>() {
    }.getType();
    AppRequest<DataStreamsConfig> appRequest = gson.fromJson(contentBuilder.toString(), configType);

    // Go through connections and set them up.
    HashMap<String, GraphNode> nodes = new HashMap();
    for (Connection connection : appRequest.getConfig().getConnections()) {
      if (!nodes.containsKey(connection.getFrom())) {
        nodes.put(connection.getFrom(), new GraphNode(connection.getFrom()));
      }
      if (!nodes.containsKey(connection.getTo())) {
        nodes.put(connection.getTo(), new GraphNode(connection.getTo()));
      }

      GraphNode from = nodes.get(connection.getFrom());
      GraphNode to = nodes.get(connection.getTo());
      from.children.add(to);
      to.parent = from;
    }

    for (ETLStage stage : appRequest.getConfig().getStages()) {
      if (nodes.containsKey(stage.getName())) {
        nodes.get(stage.getName()).config = stage;
      }
    }
    return nodes.values();
  }

  public static Schema schemaFromJson(String json) {
    try {
      return Schema.parseJson(json);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    Collection<GraphNode> nodes =
      getNodesFromFile("/Users/laraschmidt/Documents/cdap/cdap/dataflow-json/pipeline.json");
    // Find all nodes with no start.
    Queue<GraphNode> queue = new LinkedList();
    for (GraphNode node : nodes) {
      if (node.parent == null) {
        queue.add(node);
      }
    }

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
    options.setTempLocation("gs://clouddfe-laraschmidt/temp");
    Pipeline pipeline = Pipeline.create(options);

    // Process nodes.
    while (!queue.isEmpty()) {
      GraphNode node = queue.remove();
      for (GraphNode child : node.children) {
        queue.add(child);
      }
      Map<String, String> props = node.config.getPlugin().getPluginProperties().getProperties();
      if (node.config.getPlugin().getName().equals("GoogleSubscriber")) {
        String topic = "projects/google.com:clouddfe/topics/" + props.get("topic");
        System.out.println(topic);
        node.outputSchema = schemaFromJson(props.get("schema"));
        node.output = pipeline.apply(PubsubIO.readMessages().fromTopic(topic))
          .apply("Convert to StructuredRecord", ParDo.of(new PubSubToStructuredRecord(node.outputSchema)));
      } else if (node.config.getPlugin().getName().equals("JSONParser")) {
        // TODO: Support generic transforms.
        node.outputSchema = schemaFromJson(props.get("schema"));
        String inputField = props.get("field");
        System.out.println(node.parent);
        System.out.println(node.parent.output);
        node.output = node.parent.output.apply("json converter",
                                               MapElements.via(new JsonConverter(inputField, node.outputSchema)));
      } else if (node.config.getPlugin().getName().equals("GroupByAggregate")) {
        String groupByField = props.get("groupByFields");
        Schema groupByFieldSchema = node.parent.getFieldSchema(groupByField);
        // TODO: multiple combiners
        Pattern pattern = Pattern.compile("(.*):(.*)\\((.*)\\)");
        Matcher matcher = pattern.matcher(props.get("aggregates"));
        matcher.matches();
        String outputValueField = matcher.group(1);
        String aggregate = matcher.group(2);
        String combinedField = matcher.group(3);
        Schema CombinedFieldSchema = node.parent.getFieldSchema(combinedField);
        AggregateCombiner<?, ?> combiner = null;
        if (aggregate.equals("Sum")) {
          combiner = new AggregateCombiner((AggregateFactory & Serializable)() -> {
            return new Sum(combinedField, CombinedFieldSchema);
          }, outputValueField);
        } else if (aggregate.equals("Count")) {
          combiner = new AggregateCombiner((AggregateFactory & Serializable)() -> {
            return new Count(combinedField);
          }, outputValueField);
        }
        node.output = node.parent.output
          .apply("group by key", new GroupByKeyAndAggregate(groupByField, groupByFieldSchema, combiner));
        node.outputSchema = Schema.unionOf(groupByFieldSchema, combiner.getOutputSchema());
      } else if (node.config.getPlugin().getName().equals("GCS")) {
        String path = props.get("path");
        // TODO: Support more than text here.;
        node.parent.output
          .apply("tostring", MapElements.via(new StructuredRecordToString()))
          .apply("Write Files to GCS", new WriteOneFilePerWindow(path, 1));
      }
    }

    pipeline.run();
  }
}
