/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.templates.SpannerToSource.Options;
import com.google.cloud.teleport.v2.templates.common.TrimmedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.sinks.DataSink;
import com.google.cloud.teleport.v2.templates.transforms.AssignShardIdFn;
import com.google.cloud.teleport.v2.templates.transforms.FilterRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.OrderRecordsAndWriteToSinkFn;
import com.google.cloud.teleport.v2.templates.transforms.PreprocessRecordsFn;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests Spanner Change streams data and writes them to a sink. Currently supported
 * sinks are Pubsub and Kafka.
 */
@Template(
    name = "Spanner_to_Source",
    category = TemplateCategory.STREAMING,
    displayName = "Spanner Change Streams to Sink",
    description =
        "Streaming pipeline. Ingests data from Spanner Change Streams, orders them, and"
            + " writes them to a sink.",
    optionsClass = Options.class,
    flexContainerName = "spanner-to-source",
    contactInformation = "https://cloud.google.com/support",
    hidden = true,
    streaming = true)
public class SpannerToSource {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSource.class);

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {

    @TemplateParameter.Text(
        order = 1,
        optional = false,
        description = "Name of the change stream to read from",
        helpText =
            "This is the name of the Spanner change stream that the pipeline will read from.")
    String getChangeStreamName();

    void setChangeStreamName(String value);

    @TemplateParameter.Text(
        order = 2,
        optional = false,
        description = "Cloud Spanner Instance Id.",
        helpText =
            "This is the name of the Cloud Spanner instance where the changestream is present.")
    String getInstanceId();

    void setInstanceId(String value);

    @TemplateParameter.Text(
        order = 3,
        optional = false,
        description = "Cloud Spanner Database Id.",
        helpText =
            "This is the name of the Cloud Spanner database that the changestream is monitoring")
    String getDatabaseId();

    void setDatabaseId(String value);

    @TemplateParameter.ProjectId(
        order = 4,
        optional = false,
        description = "Cloud Spanner Project Id.",
        helpText = "This is the name of the Cloud Spanner project.")
    String getSpannerProjectId();

    void setSpannerProjectId(String projectId);

    @TemplateParameter.Text(
        order = 5,
        optional = false,
        description = "Cloud Spanner Instance to store metadata when reading from changestreams",
        helpText =
            "This is the instance to store the metadata used by the connector to control the"
                + " consumption of the change stream API data.")
    String getMetadataInstance();

    void setMetadataInstance(String value);

    @TemplateParameter.Text(
        order = 6,
        optional = false,
        description = "Cloud Spanner Database to store metadata when reading from changestreams",
        helpText =
            "This is the database to store the metadata used by the connector to control the"
                + " consumption of the change stream API data.")
    String getMetadataDatabase();

    void setMetadataDatabase(String value);

    @TemplateParameter.Text(
        order = 7,
        optional = true,
        description = "Changes are read from the given timestamp",
        helpText = "Read changes from the given timestamp.")
    @Default.String("")
    String getStartTimestamp();

    void setStartTimestamp(String value);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        description = "Changes are read until the given timestamp",
        helpText =
            "Read changes until the given timestamp. If no timestamp provided, reads indefinitely.")
    @Default.String("")
    String getEndTimestamp();

    void setEndTimestamp(String value);

    @TemplateParameter.Long(
        order = 9,
        optional = true,
        description = "Time interval to increment the Stateful timer.",
        helpText =
            "The timer gets incremented by the specified time interval in seconds. By default, the"
                + " next timer is set to the current real time.")
    @Default.Long(0)
    Long getIncrementInterval();

    void setIncrementInterval(Long value);

    @TemplateParameter.Enum(
        order = 10,
        optional = true,
        enumOptions = {@TemplateEnumOption("pubsub"), @TemplateEnumOption("kafka")},
        description = "Type of sink to write the data to",
        helpText = "The type of sink where the data will get written to.")
    String getSinkType();

    void setSinkType(String value);

    @TemplateParameter.PubsubTopic(
        order = 11,
        optional = true,
        description =
            "PubSub topic where records will get written to, in the format of"
                + " 'projects/your-project-id/topics/your-topic-name'",
        helpText =
            "PubSub topic where records will get written to, in the format of"
                + " 'projects/your-project-id/topics/your-topic-name'. Must be provided if sink is"
                + " pubsub.")
    String getPubSubDataTopicId();

    void setPubSubDataTopicId(String value);

    @TemplateParameter.PubsubTopic(
        order = 12,
        optional = true,
        description =
            "PubSub topic where error records will get written to , in the format of"
                + " 'projects/your-project-id/topics/your-topic-name'",
        helpText =
            "PubSub topic where error records will get written to, in the format of"
                + " 'projects/your-project-id/topics/your-topic-name'. Must be provided if sink is"
                + " pubsub.")
    String getPubSubErrorTopicId();

    void setPubSubErrorTopicId(String value);

    @TemplateParameter.Text(
        order = 13,
        optional = true,
        description = "Endpoint for pubsub",
        helpText = "Endpoint for pubsub. Must be provided if sink is pubsub.")
    @Default.String("")
    String getPubSubEndpoint();

    void setPubSubEndpoint(String value);

    @TemplateParameter.GcsReadFile(
        order = 14,
        optional = true,
        description = "Path to GCS file containing Kafka cluster details",
        helpText =
            "This is the path to GCS file containing Kafka cluster details. Must be provided if"
                + " sink is kafka.")
    String getKafkaClusterFilePath();

    void setKafkaClusterFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 15,
        optional = true,
        description = "Path to GCS file containing the the Source shard details",
        helpText =
            "Path to GCS file containing connection profile info for source shards. Must be"
                + " provided if sink is kafka.")
    String getSourceShardsFilePath();

    void setSourceShardsFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 16,
        optional = true,
        description = "Session File Path in Cloud Storage",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " HarbourBridge")
    String getSessionFilePath();

    void setSessionFilePath(String value);

    @TemplateParameter.Enum(
        order = 17,
        optional = true,
        enumOptions = {@TemplateEnumOption("none"), @TemplateEnumOption("forward_migration")},
        description = "Filtration mode",
        helpText =
            "Mode of Filtration, decides how to drop certain records based on a criteria. Currently"
                + " supported modes are: none (filter nothing), forward_migration (filter records"
                + " written via the forward migration pipeline). Defaults to forward_migration.")
    @Default.String("forward_migration")
    String getFiltrationMode();

    void setFiltrationMode(String value);

    @TemplateParameter.Long(
        order = 18,
        optional = true,
        description = "Parallel connections allowed on source.",
        helpText = "The number of parallel connections allowed on source.")
    @Default.Long(500)
    Long getSourceParallelConnections();

    void setSourceParallelConnections(Long value);

    @TemplateParameter.Text(
        order = 19,
        optional = true,
        regexes = {
          "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3})"
        },
        groupName = "Source",
        description = "Connection URL to connect to the source database.",
        helpText =
            "The JDBC connection URL string. For example, `jdbc:mysql://some-host:3306/sampledb`."
                + " Can be passed in as a string that's Base64-encoded and then encrypted with a"
                + " Cloud KMS key. Currently supported sources: MySQL",
        example = "jdbc:mysql://some-host:3306/sampledb")
    String getSourceConnectionURL();

    void setSourceConnectionURL(String connectionURL);

    @TemplateParameter.Text(
        order = 20,
        optional = true,
        regexes = {"^[a-zA-Z0-9_;!*&=@#-:\\/]+$"},
        groupName = "Source",
        description = "JDBC connection property string.",
        helpText =
            "Properties string to use for the JDBC connection. Format of the string must be"
                + " [propertyName=property;]*.",
        example = "unicode=true;characterEncoding=UTF-8")
    String getSourceConnectionProperties();

    void setSourceConnectionProperties(String connectionProperties);

    @TemplateParameter.Text(
        order = 21,
        optional = true,
        regexes = {"^.+$"},
        groupName = "Source",
        description = "JDBC connection username.",
        helpText =
            "The username to be used for the JDBC connection. Can be passed in as a Base64-encoded"
                + " string encrypted with a Cloud KMS key.")
    String getUsername();

    void setUsername(String username);

    @TemplateParameter.Password(
        order = 22,
        optional = true,
        groupName = "Source",
        description = "JDBC connection password.",
        helpText =
            "The password to be used for the JDBC connection. Can be passed in as a Base64-encoded"
                + " string encrypted with a Cloud KMS key.")
    String getPassword();

    void setPassword(String password);

    @TemplateParameter.Text(
        order = 23,
        optional = true,
        description = "SourceDB timezone offset",
        helpText =
            "This is the timezone offset from UTC for the source database. Example value: +10:00")
    @Default.String("+00:00")
    String getSourceDbTimezoneOffset();

    void setSourceDbTimezoneOffset(String value);
  }

  private static void validateSinkParams(Options options) {
    if (options.getSinkType().equals(Constants.PUBSUB_SINK)) {
      if (options.getPubSubDataTopicId().equals("")
          || options.getPubSubErrorTopicId().equals("")
          || options.getPubSubEndpoint().equals("")) {
        throw new IllegalArgumentException(
            "need to provide data topic, error topic and endpoint for pubsub sink.");
      }
    } else {
      if (options.getKafkaClusterFilePath() == null) {
        throw new IllegalArgumentException(
            "need to provide a valid GCS file path containing kafka cluster config for kafka"
                + " sink.");
      }
      if (options.getSourceShardsFilePath() == null) {
        throw new IllegalArgumentException(
            "need to provide a valid GCS file path containing source shard details for kafka"
                + " sink.");
      }
    }
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Spanner change streams to sink");

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);

    // validateSinkParams(options);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);
    // We disable auto-scaling as scaling operations accumulate records in the buffer which can
    // cause the pipeline to crash.
    pipeline
        .getOptions()
        .as(DataflowPipelineWorkerPoolOptions.class)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.NONE);

    DataSink dataSink = null;
    /* if (options.getSinkType().equals(Constants.PUBSUB_SINK)) {
      dataSink =
          new PubSubSink(
              options.getPubSubDataTopicId(),
              options.getPubSubErrorTopicId(),
              options.getPubSubEndpoint());
    } else {
      dataSink =
          new KafkaSink(options.getKafkaClusterFilePath(), options.getSourceShardsFilePath());
    }*/

    Schema schema = SessionFileReader.read(options.getSessionFilePath());

    // Prepare Spanner config
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));

    Ddl ddl = InformationSchemaReader.getInformationSchemaAsDdl(spannerConfig);

    pipeline
        .apply(getReadChangeStreamDoFn(options, spannerConfig))
        .apply(ParDo.of(new FilterRecordsFn(options.getFiltrationMode())))
        .apply(ParDo.of(new PreprocessRecordsFn()))
        .setCoder(SerializableCoder.of(TrimmedDataChangeRecord.class))
        .apply(
            ParDo.of(
                new AssignShardIdFn(
                    spannerConfig,
                    schema,
                    ddl,
                    options.getSourceParallelConnections()))) // this should output key as
        // shard_table_pk
        .apply(
            ParDo.of(
                new OrderRecordsAndWriteToSinkFn(
                    options.getIncrementInterval(),
                    dataSink,
                    schema,
                    options.getSourceConnectionURL(),
                    options.getSourceConnectionProperties(),
                    options.getUsername(),
                    options.getPassword(),
                    options.getSourceDbTimezoneOffset())));
    return pipeline.run();
  }

  public static SpannerIO.ReadChangeStream getReadChangeStreamDoFn(
      Options options, SpannerConfig spannerConfig) {

    Timestamp startTime = Timestamp.now();
    if (!options.getStartTimestamp().equals("")) {
      startTime = Timestamp.parseTimestamp(options.getStartTimestamp());
    }
    SpannerIO.ReadChangeStream readChangeStreamDoFn =
        SpannerIO.readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withChangeStreamName(options.getChangeStreamName())
            .withMetadataInstance(options.getMetadataInstance())
            .withMetadataDatabase(options.getMetadataDatabase())
            .withInclusiveStartAt(startTime);
    if (!options.getEndTimestamp().equals("")) {
      return readChangeStreamDoFn.withInclusiveEndAt(
          Timestamp.parseTimestamp(options.getEndTimestamp()));
    }
    return readChangeStreamDoFn;
  }
}
