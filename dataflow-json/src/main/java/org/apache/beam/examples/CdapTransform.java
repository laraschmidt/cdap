package org.apache.beam.examples;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataException;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.etl.api.Arguments;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


public class CdapTransform extends DoFn<StructuredRecord, StructuredRecord> implements Serializable {
  private final Transform<StructuredRecord, StructuredRecord> transform;
  SimpleStageConfigurer sc;

  public CdapTransform(Transform<StructuredRecord, StructuredRecord>  transform, Schema inputSchema) {
    this.transform = transform;
    sc = new SimpleStageConfigurer(inputSchema);
    transform.configurePipeline(new SimplePipelineConfigurer(sc));
   try {
     transform.prepareRun(new SimpleStageSubmitterContext());
     transform.initialize(new SimpleTransformContext());
   } catch (Exception e) {
     throw new RuntimeException(e);
   }
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    SimpleEmitter emitter = new SimpleEmitter(context);
    try {
      transform.transform(context.element(), emitter);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Schema outputSchema() {
    return sc.outputSchema;
  }

  private class SimpleEmitter implements Emitter<StructuredRecord> {
    ProcessContext context;

    public SimpleEmitter(ProcessContext context) {
      this.context = context;
    }

    @Override
    public void emitAlert(Map<String, String> payload) {}

    @Override
    public void emit(StructuredRecord value) {
      System.out.println("EMITTING " + JsonConverter.print(value));
      context.output(value);
    }

    @Override
    public void emitError(InvalidEntry<StructuredRecord> invalidEntry) {}
  }

  private static class SimplePipelineConfigurer implements PipelineConfigurer, Serializable {
    StageConfigurer stage;

    public SimplePipelineConfigurer(StageConfigurer stage) {
      this.stage = stage;
    }

    @Override
    public StageConfigurer getStageConfigurer() {
      return stage;
    }

    @Override
    public Engine getEngine() {
      return null;
    }

    @Override
    public void setPipelineProperties(Map<String, String> properties) {
    }

    @Override
    public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    }

    @Override
    public void addDatasetType(Class<? extends Dataset> datasetClass) {
    }

    @Override
    public void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    }

    @Override
    public void createDataset(String datasetName, String typeName) {
    }

    @Override
    public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
    }

    @Override
    public void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    }

    @Nullable
    @Override
    public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties, PluginSelector selector) {
      return null;
    }

    @Nullable
    @Override
    public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties, PluginSelector selector) {
      return null;
    }
  }

  private static class SimpleStageConfigurer implements StageConfigurer, Serializable {
    Schema inputSchema;
    public Schema outputSchema;

    public SimpleStageConfigurer(Schema inputSchema) {
      this.inputSchema = inputSchema;
    }

    @Nullable
    @Override
    public Schema getInputSchema() {
      return inputSchema;
    }

    @Override
    public void setOutputSchema(@Nullable Schema outputSchema) {
      this.outputSchema = outputSchema;
    }

    @Override
    public void setErrorSchema(@Nullable Schema errorSchema) {

    }

    @Override
    public FailureCollector getFailureCollector() {
      return new BasicFailureCollector();
    }
  }

  private static class SimpleStageSubmitterContext implements StageSubmitterContext, Serializable {

    public SimpleStageSubmitterContext() {}

    @Override
    public FailureCollector getFailureCollector() {
      return new BasicFailureCollector();
    }

    @Override
    public String getStageName() {
      return null;
    }

    @Override
    public String getNamespace() {
      return null;
    }

    @Override
    public String getPipelineName() {
      return null;
    }

    @Override
    public long getLogicalStartTime() {
      return 0;
    }

    @Override
    public StageMetrics getMetrics() {
      return null;
    }

    @Override
    public PluginProperties getPluginProperties() {
      return null;
    }

    @Override
    public PluginProperties getPluginProperties(String pluginId) {
      return null;
    }

    @Override
    public <T> Class<T> loadPluginClass(String pluginId) {
      return null;
    }

    @Override
    public <T> T newPluginInstance(String pluginId) throws InstantiationException {
      return null;
    }

    @Nullable
    @Override
    public Schema getInputSchema() {
      return null;
    }

    @Override
    public Map<String, Schema> getInputSchemas() {
      return null;
    }

    @Nullable
    @Override
    public Schema getOutputSchema() {
      return null;
    }

    @Override
    public Map<String, Schema> getOutputPortSchemas() {
      return null;
    }

    @Override
    public SettableArguments getArguments() {
      return null;
    }

    @Nullable
    @Override
    public URL getServiceURL(String applicationId, String serviceId) {
      return null;
    }

    @Nullable
    @Override
    public URL getServiceURL(String serviceId) {
      return null;
    }

    @Override
    public void createTopic(String topic) throws TopicAlreadyExistsException, IOException {

    }

    @Override
    public void createTopic(String topic, Map<String, String> properties) throws TopicAlreadyExistsException, IOException {

    }

    @Override
    public Map<String, String> getTopicProperties(String topic) throws TopicNotFoundException, IOException {
      return null;
    }

    @Override
    public void updateTopic(String topic, Map<String, String> properties) throws TopicNotFoundException, IOException {

    }

    @Override
    public void deleteTopic(String topic) throws TopicNotFoundException, IOException {

    }

    @Override
    public MessagePublisher getMessagePublisher() {
      return null;
    }

    @Override
    public MessagePublisher getDirectMessagePublisher() {
      return null;
    }

    @Override
    public MessageFetcher getMessageFetcher() {
      return null;
    }

    @Override
    public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
      return null;
    }

    @Override
    public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) throws MetadataException {
      return null;
    }

    @Override
    public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws MetadataException {
      return null;
    }

    @Override
    public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {

    }

    @Override
    public void addTags(MetadataEntity metadataEntity, String... tags) {

    }

    @Override
    public void addTags(MetadataEntity metadataEntity, Iterable<String> tags) {

    }

    @Override
    public void removeMetadata(MetadataEntity metadataEntity) {

    }

    @Override
    public void removeProperties(MetadataEntity metadataEntity) {

    }

    @Override
    public void removeProperties(MetadataEntity metadataEntity, String... keys) {

    }

    @Override
    public void removeTags(MetadataEntity metadataEntity) {

    }

    @Override
    public void removeTags(MetadataEntity metadataEntity, String... tags) {

    }

    @Override
    public void record(List<FieldOperation> fieldOperations) {

    }
  }

  private static class SimpleTransformContext implements TransformContext, Serializable {

    @Override
    public FailureCollector getFailureCollector() {
      return new BasicFailureCollector();
    }

    @Override
    public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
      return null;
    }

    @Override
    public String getStageName() {
      return null;
    }

    @Override
    public String getNamespace() {
      return null;
    }

    @Override
    public String getPipelineName() {
      return null;
    }

    @Override
    public long getLogicalStartTime() {
      return 0;
    }

    @Override
    public StageMetrics getMetrics() {
      return null;
    }

    @Override
    public PluginProperties getPluginProperties() {
      return null;
    }

    @Override
    public PluginProperties getPluginProperties(String pluginId) {
      return null;
    }

    @Override
    public <T> Class<T> loadPluginClass(String pluginId) {
      return null;
    }

    @Override
    public <T> T newPluginInstance(String pluginId) throws InstantiationException {
      return null;
    }

    @Nullable
    @Override
    public Schema getInputSchema() {
      return null;
    }

    @Override
    public Map<String, Schema> getInputSchemas() {
      return null;
    }

    @Nullable
    @Override
    public Schema getOutputSchema() {
      return null;
    }

    @Override
    public Map<String, Schema> getOutputPortSchemas() {
      return null;
    }

    @Override
    public Arguments getArguments() {
      return null;
    }

    @Nullable
    @Override
    public URL getServiceURL(String applicationId, String serviceId) {
      return null;
    }

    @Nullable
    @Override
    public URL getServiceURL(String serviceId) {
      return null;
    }

    @Override
    public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) throws MetadataException {
      return null;
    }

    @Override
    public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws MetadataException {
      return null;
    }

    @Override
    public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {

    }

    @Override
    public void addTags(MetadataEntity metadataEntity, String... tags) {

    }

    @Override
    public void addTags(MetadataEntity metadataEntity, Iterable<String> tags) {

    }

    @Override
    public void removeMetadata(MetadataEntity metadataEntity) {

    }

    @Override
    public void removeProperties(MetadataEntity metadataEntity) {

    }

    @Override
    public void removeProperties(MetadataEntity metadataEntity, String... keys) {

    }

    @Override
    public void removeTags(MetadataEntity metadataEntity) {

    }

    @Override
    public void removeTags(MetadataEntity metadataEntity, String... tags) {

    }

    @Override
    public void record(List<FieldOperation> fieldOperations) {

    }
  }

  private static class BasicFailureCollector implements FailureCollector, Serializable {
    private final List<ValidationFailure> failures;


    public BasicFailureCollector() {
      this.failures = new ArrayList<>();
    }

    @Override
    public ValidationFailure addFailure(String message, @Nullable String correctiveAction) {
      ValidationFailure failure = new ValidationFailure(message, correctiveAction);
      failures.add(failure);
      return failure;
    }

    @Override
    public ValidationException getOrThrowException() throws ValidationException {
      if (failures.isEmpty()) {
        return new ValidationException(failures);
      }
      throw new ValidationException(failures);
    }

    public List<ValidationFailure> getValidationFailures() {
      return failures;
    }
  }


}
