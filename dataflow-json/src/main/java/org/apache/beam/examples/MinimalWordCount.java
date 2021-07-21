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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class MinimalWordCount {

  public static class StructuredRecordToString extends SimpleFunction<StructuredRecord, String> {
    @Override
    public String apply(StructuredRecord record) {
      return record.toString();
    }
  }

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
    options.setTempLocation("gs://clouddfe-laraschmidt/temp");

    Pipeline pipeline = Pipeline.create(options);

    // Fix pubsub. Get json working.

    pipeline
      .apply("Read PubSub Messages", PubsubIO.readMessages().fromTopic("projects/google.com:clouddfe/topics/laraschmidt-topic"))
      .apply("Convert to StructuredRecord", MapElements.via(new PubSubToStructuredRecord(null)))
      .apply("json converter", MapElements.via(new JsonConverter(null, null)))
      .apply("group by key", new GroupByKeyAndAggregate(null, null, null, null))
      .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
      .apply("tostring", MapElements.via(new StructuredRecordToString()))
      .apply("Write Files to GCS", new WriteOneFilePerWindow("gs://clouddfe-laraschmidt/stream-out", 1));

    pipeline.run();
  }
}
