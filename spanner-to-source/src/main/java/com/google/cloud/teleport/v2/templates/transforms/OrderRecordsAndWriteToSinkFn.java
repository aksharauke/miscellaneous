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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.templates.common.InputRecordProcessor;
import com.google.cloud.teleport.v2.templates.common.MySqlDao;
import com.google.cloud.teleport.v2.templates.common.TrimmedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.sinks.DataSink;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OnTimer;
import org.apache.beam.sdk.transforms.DoFn.OnTimerContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class buffers all records in a BagState and triggers a timer. On expiry, it sorts the
 * records ready to be written and writes them to Sink.
 */
public class OrderRecordsAndWriteToSinkFn extends DoFn<KV<String, TrimmedDataChangeRecord>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(OrderRecordsAndWriteToSinkFn.class);

  private long incrementIntervalInMilisec = 0;

  private DataSink dataSink;
  private final Schema schema;

  private final String sourceConnectionIUrl;
  private final String sourceConnectionProperties;
  private final String userName;
  private final String password;
  private final String sourceDbTimezoneOffset;
  private transient MySqlDao mySqlDao;

  private final Counter numRecProcessedMetric =
      Metrics.counter(OrderRecordsAndWriteToSinkFn.class, "records_processed");

  private final Distribution lagMetric =
      Metrics.distribution(OrderRecordsAndWriteToSinkFn.class, "replication_lag_in_milli");

  public OrderRecordsAndWriteToSinkFn(
      long incrementIntervalInMilisec,
      DataSink dataSink,
      Schema schema,
      String sourceConnectionIUrl,
      String sourceConnectionProperties,
      String userName,
      String password,
      String sourceDbTimezoneOffset) {
    this.incrementIntervalInMilisec = incrementIntervalInMilisec;
    this.dataSink = dataSink;
    this.schema = schema;
    this.sourceConnectionIUrl = sourceConnectionIUrl;
    this.sourceConnectionProperties = sourceConnectionProperties;
    this.userName = userName;
    this.password = password;
    this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    mySqlDao = new MySqlDao(sourceConnectionIUrl, userName, password, sourceConnectionProperties);
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() throws Exception {
    if (mySqlDao != null) {
      mySqlDao.cleanup();
    }
  }

  @SuppressWarnings("unused")
  @TimerId("timer")
  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @StateId("buffer")
  private final StateSpec<BagState<TrimmedDataChangeRecord>> buffer =
      StateSpecs.bag(SerializableCoder.of(TrimmedDataChangeRecord.class));

  @StateId("keyString")
  private final StateSpec<ValueState<String>> keyString = StateSpecs.value(StringUtf8Coder.of());

  @StateId("stopProcessing")
  private final StateSpec<ValueState<Boolean>> stopProcessing = StateSpecs.value(BooleanCoder.of());

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("buffer") BagState<TrimmedDataChangeRecord> buffer,
      @TimerId("timer") Timer timer,
      @StateId("keyString") ValueState<String> keyString,
      @StateId("stopProcessing") ValueState<Boolean> stopProcessing) {
    KV<String, TrimmedDataChangeRecord> element = c.element();
    Boolean failedShard = stopProcessing.read();
    if (failedShard != null && failedShard) {
      return;
    }
    buffer.add(element.getValue());
    String shardId = keyString.read();
    // Set timer if not already running.
    if (shardId == null) {
      Instant commitTimestamp =
          new Instant(element.getValue().getCommitTimestamp().toSqlTimestamp());
      Instant outputTimestamp = commitTimestamp.plus(Duration.millis(incrementIntervalInMilisec));
      timer.set(outputTimestamp);
      keyString.write(element.getKey());
    }
  }

  @OnTimer("timer")
  public void onExpiry(
      OnTimerContext context,
      @StateId("buffer") BagState<TrimmedDataChangeRecord> buffer,
      @TimerId("timer") Timer timer,
      @StateId("keyString") ValueState<String> keyString,
      @StateId("stopProcessing") ValueState<Boolean> stopProcessing) {
    String shardId = keyString.read();
    /*  LOG.info(
    "Shard " + shardId + ": started timer processing for expiry time: " + context.timestamp());*/
    if (!buffer.isEmpty().read()) {
      final List<TrimmedDataChangeRecord> records =
          StreamSupport.stream(buffer.read().spliterator(), false).collect(Collectors.toList());
      buffer.clear();

      long writeBack = 0;
      int totalCount = records.size();
      List<TrimmedDataChangeRecord> recordsToProcess = new ArrayList<TrimmedDataChangeRecord>();
      for (TrimmedDataChangeRecord record : records) {
        Instant recordCommitTimestamp = new Instant(record.getCommitTimestamp().toSqlTimestamp());
        // When the watermark passes time T, this means that all records with
        // event time < T have been processed and successfully committed. Since the
        // timer fires when the watermark passes the expiration time, we should
        // only output records with event time < expiration time.
        if (recordCommitTimestamp.isBefore(context.timestamp())) {
          recordsToProcess.add(record);
        } else {
          writeBack++;
          buffer.add(record);
        }
      }
      /* LOG.info(
      "Shard "
          + shardId
          + ": Total "
          + totalCount
          + " records found. Wrote back "
          + writeBack
          + " recs. Outputting "
          + recordsToProcess.size()
          + " recs for expiry time: "
          + context.timestamp());*/

      if (!recordsToProcess.isEmpty()) {
        // Order the records in place, and output them.
        Instant sortStartTime = Instant.now();
        /* We want per PK to output the latest change steam record,
        so sort the list then iterate and only take the latest per PK and output that. */
        Collections.sort(
            recordsToProcess,
            Comparator.comparing(TrimmedDataChangeRecord::getCommitTimestamp)
                .thenComparing(TrimmedDataChangeRecord::getServerTransactionId)
                .thenComparing(TrimmedDataChangeRecord::getRecordSequence));
        Map<Long, TrimmedDataChangeRecord> recordsPerPK = new HashMap<>();
        boolean earliestTsSaved = false;
        TrimmedDataChangeRecord earliestRec = null;
        for (TrimmedDataChangeRecord rec : recordsToProcess) {
          if (!earliestTsSaved) {
            earliestRec = rec;
            earliestTsSaved = true;
          }
          Long pkValue = rec.getUserId();
          recordsPerPK.put(pkValue, rec);
        }

        InputRecordProcessor.processRecords(
            new ArrayList<>(recordsPerPK.values()),
            schema,
            mySqlDao,
            shardId,
            sourceDbTimezoneOffset);
        Instant instTime = Instant.now();
        Instant commitTsInst =
            new Instant(earliestRec.getCommitTimestamp().toSqlTimestamp().toInstant().toString());
        Duration replicationLagDuration = new Duration(commitTsInst, instTime);
        lagMetric.update(replicationLagDuration.getMillis()); // update the lag metric
        // LOG.info("Replication lag in milisec: {}", replicationLagDuration.getMillis());
        numRecProcessedMetric.inc(recordsPerPK.size());
        LOG.info(
            "Expired at {}, written records for key {}", context.timestamp().toString(), shardId);
      } else {
        LOG.info(
            "Shard " + shardId + ": Expired at {} with no records", context.timestamp().toString());
      }
    }

    Instant nextTimer = context.timestamp().plus(Duration.millis(incrementIntervalInMilisec));
    // 0 is the default, which means set next timer as current timestamp.
    if (incrementIntervalInMilisec == 0) {
      nextTimer = Instant.now();
    }
    timer.set(nextTimer);
  }
}
