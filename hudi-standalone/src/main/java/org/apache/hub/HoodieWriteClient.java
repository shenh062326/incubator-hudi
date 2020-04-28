/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hub;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hub.exception.*;
import org.apache.hub.func.CopyOnWriteLazyInsertIterable;
import org.apache.hub.table.Partitioner;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hub.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hub.config.HoodieCompactionConfig;
import org.apache.hub.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hub.index.HoodieIndex;
import org.apache.hub.metrics.HoodieMetrics;
import org.apache.hub.table.HoodieTable;
import org.apache.hub.table.WorkloadProfile;
import org.apache.hub.table.WorkloadStat;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Hoodie Write Client helps you build tables on HDFS [insert()] and then perform efficient mutations on an HDFS
 * table [upsert()]
 * <p>
 * Note that, at any given time, there can only be one Spark job performing these operations on a Hoodie table.
 */
public class HoodieWriteClient<T extends HoodieRecordPayload> extends AbstractHoodieWriteClient<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieWriteClient.class);
  private static final String LOOKUP_STR = "lookup";
  private final boolean rollbackPending;
  private final transient HoodieMetrics metrics;
  private final transient HoodieCleanClient<T> cleanClient;
  private transient Timer.Context compactionTimer;

  /**
   * Create a wirte client, without cleaning up failed/inflight commits.
   *
   * @param conf Configuration
   * @param clientConfig instance of HoodieWriteConfig
   */
  public HoodieWriteClient(Configuration conf, HoodieWriteConfig clientConfig) {
    this(conf, clientConfig, false);
  }

  /**
   * Create a wirte client, with new hudi index.
   *
   * @param conf Configuration
   * @param clientConfig instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   */
  public HoodieWriteClient(Configuration conf, HoodieWriteConfig clientConfig, boolean rollbackPending) {
    this(conf, clientConfig, rollbackPending, HoodieIndex.createIndex(clientConfig));
  }

  @VisibleForTesting
  HoodieWriteClient(Configuration conf, HoodieWriteConfig clientConfig, boolean rollbackPending, HoodieIndex index) {
    this(conf, clientConfig, rollbackPending, index, Option.empty());
  }

  /**
   *  Create a wirte client, allows to specify all parameters.
   *
   * @param conf Configuration
   * @param clientConfig instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   * @param timelineService Timeline Service that runs as part of write client.
   */
  public HoodieWriteClient(Configuration conf, HoodieWriteConfig clientConfig, boolean rollbackPending,
      HoodieIndex index, Option<EmbeddedTimelineService> timelineService) {
    super(conf, index, clientConfig, timelineService);
    this.metrics = new HoodieMetrics(config, config.getTableName());
    this.rollbackPending = rollbackPending;
    this.cleanClient = new HoodieCleanClient<>(conf, config, metrics, timelineService);
  }

  /**
   * Filter out HoodieRecords that already exists in the output folder. This is useful in deduplication.
   *
   * @param hoodieRecords Input RDD of Hoodie records.
   * @return A subset of hoodieRecords RDD, with existing records filtered out.
   */
  public List<HoodieRecord<T>> filterExists(List<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, conf);
    Timer.Context indexTimer = metrics.getIndexCtx();
    List<HoodieRecord<T>> recordsWithLocation = getIndex().tagLocation(hoodieRecords, table);
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.stream().filter(v1 -> !v1.isCurrentLocationKnown()).collect(Collectors.toList());
  }

  /**
   * Upsert a batch of new records into Hoodie table at the supplied commitTime.
   *
   * @param records List of hoodieRecords to upsert
   * @param commitTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> upsert(List<HoodieRecord<T>> records, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.UPSERT);
    try {
      // De-dupe/merge if needed
      List<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(config.shouldCombineBeforeUpsert(), records, config.getUpsertShuffleParallelism());

      Timer.Context indexTimer = metrics.getIndexCtx();
      // perform index loop up to get existing location of records
      List<HoodieRecord<T>> taggedRecords = getIndex().tagLocation(dedupedRecords, table);
      metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
      return upsertRecordsInternal(taggedRecords, commitTime, table, true);
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + commitTime, e);
    }
  }

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied commitTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * @param preppedRecords Prepared HoodieRecords to upsert
   * @param commitTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> upsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.UPSERT_PREPPED);
    try {
      return upsertRecordsInternal(preppedRecords, commitTime, table, true);
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert prepared records for commit time " + commitTime, e);
    }
  }

  /**
   * Inserts the given HoodieRecords, into the table. This API is intended to be used for normal writes.
   * <p>
   * This implementation skips the index check and is able to leverage benefits such as small file handling/blocking
   * alignment, as with upsert(), by profiling the workload
   *
   * @param records HoodieRecords to insert
   * @param commitTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> insert(List<HoodieRecord<T>> records, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.INSERT);
    try {
      // De-dupe/merge if needed
      List<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(config.shouldCombineBeforeInsert(), records, config.getInsertShuffleParallelism());

      return upsertRecordsInternal(dedupedRecords, commitTime, table, false);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to insert for commit time " + commitTime, e);
    }
  }

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied commitTime.
   * <p>
   * This implementation skips the index check, skips de-duping and is able to leverage benefits such as small file
   * handling/blocking alignment, as with insert(), by profiling the workload. The prepared HoodieRecords should be
   * de-duped if needed.
   *
   * @param preppedRecords HoodieRecords to insert
   * @param commitTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> insertPreppedRecords(List<HoodieRecord<T>> preppedRecords, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.INSERT_PREPPED);
    try {
      return upsertRecordsInternal(preppedRecords, commitTime, table, false);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to insert prepared records for commit time " + commitTime, e);
    }
  }

  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> records) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, conf);
    return getIndex().tagLocation(records, table);
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing table to Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link HoodieWriteClient#insert(List, String)}. Optionally
   * it allows users to specify their own partitioner. If specified then it will be used for repartitioning records. See
   *
   * @param records HoodieRecords to insert
   * @param commitTime Instant time of the commit
   * into hoodie.
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.BULK_INSERT);
    try {
      // De-dupe/merge if needed

      List<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(config.shouldCombineBeforeInsert(), records, config.getInsertShuffleParallelism());

      return bulkInsertInternal(records, commitTime, table);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to bulk insert for commit time " + commitTime, e);
    }
  }


  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing table to Hoodie). The input records should contain no
   * duplicates if needed.
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link HoodieWriteClient#insert(List, String)}.
   *
   * @param preppedRecords HoodieRecords to insert
   * @param commitTime Instant time of the commit
   * @return List[WriteStatus] - WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> bulkInsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.BULK_INSERT_PREPPED);
    try {
      return bulkInsertInternal(preppedRecords, commitTime, table);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to bulk insert prepared records for commit time " + commitTime, e);
    }
  }

  protected static Comparator<HoodieRecord> partitionComparator = (HoodieRecord leftRecord, HoodieRecord rightRecord) -> {
      String leftPartition = leftRecord.getPartitionPath();
      String rightPartition = rightRecord.getPartitionPath();
      return leftPartition.compareTo(rightPartition);
  };


  /**
   * Deletes a list of {@link HoodieKey}s from the Hoodie table, at the supplied commitTime {@link HoodieKey}s will be
   * deduped and non existant keys will be removed before deleting.
   *
   * @param keys {@link List} of {@link HoodieKey}s to be deleted
   * @param commitTime Commit time handle
   * @return List[WriteStatus] - WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> delete(List<HoodieKey> keys, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.DELETE);
    try {
      // De-dupe/merge if needed
      List<HoodieKey> dedupedKeys =
          config.shouldCombineBeforeDelete() ? deduplicateKeys(keys) : keys;

      List<HoodieRecord<T>> dedupedRecords = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        dedupedRecords.add(new HoodieRecord(keys.get(i), new EmptyHoodieRecordPayload()));
      }

      Timer.Context indexTimer = metrics.getIndexCtx();
      // perform index loop up to get existing location of records
      List<HoodieRecord<T>> taggedRecords = getIndex().tagLocation(dedupedRecords, table);
      // filter out non existant keys/records
      List<HoodieRecord<T>> taggedValidRecords = taggedRecords.stream().filter(HoodieRecord::isCurrentLocationKnown)
          .collect(Collectors.toList());
      if (!taggedValidRecords.isEmpty()) {
        metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
        return upsertRecordsInternal(taggedValidRecords, commitTime, table, true);
      } else {
        // if entire set of keys are non existent
        saveWorkloadProfileMetadataToInflight(new WorkloadProfile(new ArrayList<>()), table, commitTime);
        List<WriteStatus> writeStatusRDD = new ArrayList<>();
        commitOnAutoCommit(commitTime, writeStatusRDD, table.getMetaClient().getCommitActionType());
        return writeStatusRDD;
      }
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to delete for commit time " + commitTime, e);
    }
  }

  // TODO should add testcase
  private List<WriteStatus> bulkInsertInternal(List<HoodieRecord<T>> dedupedRecords, String commitTime,
      HoodieTable<T> table) {

    // sort record by partition, TODO, can cause oom.
    dedupedRecords.sort(partitionComparator);

    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
        table.getMetaClient().getCommitActionType(), commitTime), Option.empty());

    CopyOnWriteLazyInsertIterable<T> iterable = new CopyOnWriteLazyInsertIterable<>(
        dedupedRecords.iterator(), config, commitTime, table, FSUtils.createNewFileIdPfx());

    List<WriteStatus> writeStatus = null;
    while (iterable.hasNext()){
      writeStatus = iterable.next();
    }

    return updateIndexAndCommitIfNeeded(writeStatus, table, commitTime);
  }

  private List<HoodieRecord<T>> combineOnCondition(boolean condition, List<HoodieRecord<T>> records,
      int parallelism) {
    return condition ? deduplicateRecords(records, parallelism) : records;
  }

  /**
   * Save the workload profile in an intermediate file (here re-using commit files) This is useful when performing
   * rollback for MOR tables. Only updates are recorded in the workload profile metadata since updates to log blocks
   * are unknown across batches Inserts (which are new parquet files) are rolled back based on commit time. // TODO :
   * Create a new WorkloadProfile metadata file instead of using HoodieCommitMetadata
   */
  private void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, HoodieTable<T> table, String commitTime)
      throws HoodieCommitException {
    try {
      HoodieCommitMetadata metadata = new HoodieCommitMetadata();
      profile.getPartitionPaths().forEach(path -> {
        WorkloadStat partitionStat = profile.getWorkloadStat(path.toString());
        partitionStat.getUpdateLocationToCount().forEach((key, value) -> {
          HoodieWriteStat writeStat = new HoodieWriteStat();
          writeStat.setFileId(key);
          // TODO : Write baseCommitTime is possible here ?
          writeStat.setPrevCommit(value.getKey());
          writeStat.setNumUpdateWrites(value.getValue());
          metadata.addWriteStat(path.toString(), writeStat);
        });
      });

      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      String commitActionType = table.getMetaClient().getCommitActionType();
      HoodieInstant requested = new HoodieInstant(State.REQUESTED, commitActionType, commitTime);
      activeTimeline.transitionRequestedToInflight(requested,
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException io) {
      throw new HoodieCommitException("Failed to commit " + commitTime + " unable to save inflight metadata ", io);
    }
  }

  private List<WriteStatus> upsertRecordsInternal(List<HoodieRecord<T>> preppedRecords, String commitTime,
      HoodieTable<T> hoodieTable, final boolean isUpsert) {

    WorkloadProfile profile = null;
    if (hoodieTable.isWorkloadProfileNeeded()) {
      profile = new WorkloadProfile(preppedRecords);
      LOG.info("Workload profile :" + profile);
      saveWorkloadProfileMetadataToInflight(profile, hoodieTable, commitTime);
    }

    // partition using the insert partitioner
    final Partitioner partitioner = getPartitioner(hoodieTable, isUpsert, profile);
    Map<Integer, List<Tuple2<Integer, HoodieRecord<T>>>> partitionedRecords =
        partition(preppedRecords, partitioner);

    List<WriteStatus> writeStatuses = new ArrayList<>();
    for(Map.Entry<Integer, List<Tuple2<Integer, HoodieRecord<T>>>> entry: partitionedRecords.entrySet()) {
      int partition = entry.getKey();
      Iterator<HoodieRecord<T>> recordItr = entry.getValue().stream().map(tuple -> tuple._2).iterator();
      Iterator<List<WriteStatus>> writeStatuse = null;
      if (isUpsert) {
        writeStatuse = hoodieTable.handleUpsertPartition(commitTime, partition, recordItr, partitioner);
      } else {
        writeStatuse = hoodieTable.handleInsertPartition(commitTime, partition, recordItr, partitioner);
      }
      while (writeStatuse.hasNext()) {
        writeStatuses.addAll(writeStatuse.next());
      }
    }
    return updateIndexAndCommitIfNeeded(writeStatuses, hoodieTable, commitTime);
  }

  private Map<Integer, List<Tuple2<Integer, HoodieRecord<T>>>> partition(
      List<HoodieRecord<T>> preppedRecords, Partitioner partitioner) {
    Map<Integer, List<Tuple2<Integer, HoodieRecord<T>>>> partition2Records =
        preppedRecords.stream().map(record -> {
          int numPartition = partitioner.getPartition(
              new Tuple2<>(record.getKey(), Option.ofNullable(record.getCurrentLocation())));
          return new Tuple2<>(numPartition, record);
        }).collect(Collectors.groupingBy(Tuple2::_1));
    return partition2Records;
  }

  private Partitioner getPartitioner(HoodieTable table, boolean isUpsert, WorkloadProfile profile) {
    if (isUpsert) {
      return table.getUpsertPartitioner(profile);
    } else {
      return table.getInsertPartitioner(profile);
    }
  }

  @Override
  protected void postCommit(HoodieCommitMetadata metadata, String instantTime,
      Option<Map<String, String>> extraMetadata) throws IOException {

    // Do an inline compaction if enabled
    if (config.isInlineCompaction()) {
      metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "true");
      forceCompact(extraMetadata);
    } else {
      metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false");
    }
    // We cannot have unbounded commit files. Archive commits if we have to archive
    // HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(config, createMetaClient(true));
    // archiveLog.archiveIfRequired(jsc);
    if (config.isAutoClean()) {
      // Call clean to cleanup if there is anything to cleanup after the commit,
      LOG.info("Auto cleaning is enabled. Running cleaner now");
      clean(instantTime);
    } else {
      LOG.info("Auto cleaning is not enabled. Not running cleaner now");
    }
  }


  /**
   * Savepoint a specific commit. Latest version of data files as of the passed in commitTime will be referenced in the
   * savepoint and will never be cleaned. The savepointed commit will never be rolledback or archived.
   * <p>
   * This gives an option to rollback the state to the savepoint anytime. Savepoint needs to be manually created and
   * deleted.
   * <p>
   * Savepoint should be on a commit that could not have been cleaned.
   *
   * @param user - User creating the savepoint
   * @param comment - Comment for the savepoint
   * @return true if the savepoint was created successfully
   */
  public boolean savepoint(String user, String comment) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, conf);
    if (table.getCompletedCommitsTimeline().empty()) {
      throw new HoodieSavepointException("Could not savepoint. Commit timeline is empty");
    }
    if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
      throw new UnsupportedOperationException("Savepointing is not supported or MergeOnRead table types");
    }

    String latestCommit = table.getCompletedCommitsTimeline().lastInstant().get().getTimestamp();
    LOG.info("Savepointing latest commit " + latestCommit);
    return savepoint(latestCommit, user, comment);
  }

  /**
   * Savepoint a specific commit. Latest version of data files as of the passed in commitTime will be referenced in the
   * savepoint and will never be cleaned. The savepointed commit will never be rolledback or archived.
   * <p>
   * This gives an option to rollback the state to the savepoint anytime. Savepoint needs to be manually created and
   * deleted.
   * <p>
   * Savepoint should be on a commit that could not have been cleaned.
   *
   * @param commitTime - commit that should be savepointed
   * @param user - User creating the savepoint
   * @param comment - Comment for the savepoint
   * @return true if the savepoint was created successfully
   */
  public boolean savepoint(String commitTime, String user, String comment) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, conf);
    if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
      throw new UnsupportedOperationException("Savepointing is not supported or MergeOnRead table types");
    }
    Option<HoodieInstant> cleanInstant = table.getCompletedCleanTimeline().lastInstant();

    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);
    if (!table.getCompletedCommitsTimeline().containsInstant(commitInstant)) {
      throw new HoodieSavepointException("Could not savepoint non-existing commit " + commitInstant);
    }

    try {
      // Check the last commit that was not cleaned and check if savepoint time is > that commit
      String lastCommitRetained;
      if (cleanInstant.isPresent()) {
        HoodieCleanMetadata cleanMetadata = AvroUtils
            .deserializeHoodieCleanMetadata(table.getActiveTimeline().getInstantDetails(cleanInstant.get()).get());
        lastCommitRetained = cleanMetadata.getEarliestCommitToRetain();
      } else {
        lastCommitRetained = table.getCompletedCommitsTimeline().firstInstant().get().getTimestamp();
      }

      // Cannot allow savepoint time on a commit that could have been cleaned
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(commitTime, lastCommitRetained, HoodieTimeline.GREATER_OR_EQUAL),
          "Could not savepoint commit " + commitTime + " as this is beyond the lookup window " + lastCommitRetained);

      TableFileSystemView.BaseFileOnlyView view = table.getBaseFileOnlyView();

      Map<String, List<String>> latestFilesMap =
          FSUtils.getAllPartitionPaths(fs, table.getMetaClient().getBasePath(),
          config.shouldAssumeDatePartitioning()).stream().map(partitionPath -> {
        // Scan all partitions files with this commit time
        LOG.info("Collecting latest files in partition path " + partitionPath);
        List<String> latestFiles = view.getLatestBaseFilesBeforeOrOn(partitionPath, commitTime)
            .map(HoodieBaseFile::getFileName).collect(Collectors.toList());
        return new Tuple2<>(partitionPath, latestFiles);
      }).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

      HoodieSavepointMetadata metadata = AvroUtils.convertSavepointMetadata(user, comment, latestFilesMap);
      // Nothing to save in the savepoint
      table.getActiveTimeline().createNewInstant(
          new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, commitTime));
      table.getActiveTimeline()
          .saveAsComplete(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, commitTime),
              AvroUtils.serializeSavepointMetadata(metadata));
      LOG.info("Savepoint " + commitTime + " created");
      return true;
    } catch (IOException e) {
      throw new HoodieSavepointException("Failed to savepoint " + commitTime, e);
    }
  }

  /**
   * Delete a savepoint that was created. Once the savepoint is deleted, the commit can be rolledback and cleaner may
   * clean up data files.
   *
   * @param savepointTime - delete the savepoint
   * @return true if the savepoint was deleted successfully
   */
  public void deleteSavepoint(String savepointTime) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, conf);
    if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
      throw new UnsupportedOperationException("Savepointing is not supported or MergeOnRead table types");
    }
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      LOG.warn("No savepoint present " + savepointTime);
      return;
    }

    activeTimeline.revertToInflight(savePoint);
    activeTimeline.deleteInflight(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, savepointTime));
    LOG.info("Savepoint " + savepointTime + " deleted");
  }

  /**
   * Delete a compaction request that is pending.
   *
   * NOTE - This is an Admin operation. With async compaction, this is expected to be called with async compaction and
   * write shutdown. Otherwise, async compactor could fail with errors
   *
   * @param compactionTime - delete the compaction time
   */
  private void deleteRequestedCompaction(String compactionTime) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, conf);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieInstant compactionRequestedInstant =
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionTime);
    boolean isCompactionInstantInRequestedState =
        table.getActiveTimeline().filterPendingCompactionTimeline().containsInstant(compactionRequestedInstant);
    HoodieTimeline commitTimeline = table.getCompletedCommitTimeline();
    if (commitTimeline.empty() && !commitTimeline.findInstantsAfter(compactionTime, Integer.MAX_VALUE).empty()) {
      throw new HoodieRollbackException(
          "Found commits after time :" + compactionTime + ", please rollback greater commits first");
    }
    if (isCompactionInstantInRequestedState) {
      activeTimeline.deleteCompactionRequested(compactionRequestedInstant);
    } else {
      throw new IllegalArgumentException("Compaction is not in requested state " + compactionTime);
    }
    LOG.info("Compaction " + compactionTime + " deleted");
  }

  /**
   * Rollback the state to the savepoint. WARNING: This rollsback recent commits and deleted data files. Queries
   * accessing the files will mostly fail. This should be done during a downtime.
   *
   * @param savepointTime - savepoint time to rollback to
   * @return true if the savepoint was rollecback to successfully
   */
  public boolean rollbackToSavepoint(String savepointTime) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, conf);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

    // Rollback to savepoint is expected to be a manual operation and no concurrent write or compaction is expected
    // to be running. Rollback to savepoint also removes any pending compaction actions that are generated after
    // savepoint time. Allowing pending compaction to be retained is not safe as those workload could be referencing
    // file-slices that will be rolled-back as part of this operation
    HoodieTimeline commitTimeline = table.getMetaClient().getCommitsAndCompactionTimeline();

    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      throw new HoodieRollbackException("No savepoint for commitTime " + savepointTime);
    }

    List<String> commitsToRollback = commitTimeline.findInstantsAfter(savepointTime, Integer.MAX_VALUE).getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    LOG.info("Rolling back commits " + commitsToRollback);

    restoreToInstant(savepointTime);

    // Make sure the rollback was successful
    Option<HoodieInstant> lastInstant =
        activeTimeline.reload().getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants().lastInstant();
    Preconditions.checkArgument(lastInstant.isPresent());
    Preconditions.checkArgument(lastInstant.get().getTimestamp().equals(savepointTime),
        savepointTime + "is not the last commit after rolling back " + commitsToRollback + ", last commit was "
            + lastInstant.get().getTimestamp());
    return true;
  }

  /**
   * Rollback the (inflight/committed) record changes with the given commit time. Three steps: (1) Atomically unpublish
   * this commit (2) clean indexing data, (3) clean new generated parquet files. (4) Finally delete .commit or .inflight
   * file.
   *
   * @param commitTime Instant time of the commit
   * @return {@code true} If rollback the record changes successfully. {@code false} otherwise
   */
  public boolean rollback(final String commitTime) throws HoodieRollbackException {
    rollbackInternal(commitTime);
    return true;
  }

  /**
   * NOTE : This action requires all writers (ingest and compact) to a table to be stopped before proceeding. Revert
   * the (inflight/committed) record changes for all commits after the provided @param. Four steps: (1) Atomically
   * unpublish this commit (2) clean indexing data, (3) clean new generated parquet/log files and/or append rollback to
   * existing log files. (4) Finally delete .commit, .inflight, .compaction.inflight or .compaction.requested file
   *
   * @param instantTime Instant time to which restoration is requested
   */
  public void restoreToInstant(final String instantTime) throws HoodieRollbackException {

    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, conf);
    // Get all the commits on the timeline after the provided commit time
    List<HoodieInstant> instantsToRollback = table.getActiveTimeline().getCommitsAndCompactionTimeline()
        .getReverseOrderedInstants()
        .filter(instant -> HoodieActiveTimeline.GREATER.test(instant.getTimestamp(), instantTime))
        .collect(Collectors.toList());
    // Start a rollback instant for all commits to be rolled back
    String startRollbackInstant = HoodieActiveTimeline.createNewInstantTime();
    // Start the timer
    final Timer.Context context = startContext();
    ImmutableMap.Builder<String, List<HoodieRollbackStat>> instantsToStats = ImmutableMap.builder();
    table.getActiveTimeline().createNewInstant(
        new HoodieInstant(true, HoodieTimeline.RESTORE_ACTION, startRollbackInstant));
    instantsToRollback.forEach(instant -> {
      try {
        switch (instant.getAction()) {
          case HoodieTimeline.COMMIT_ACTION:
          case HoodieTimeline.DELTA_COMMIT_ACTION:
            List<HoodieRollbackStat> statsForInstant = doRollbackAndGetStats(instant);
            instantsToStats.put(instant.getTimestamp(), statsForInstant);
            break;
          case HoodieTimeline.COMPACTION_ACTION:
            // TODO : Get file status and create a rollback stat and file
            // TODO : Delete the .aux files along with the instant file, okay for now since the archival process will
            // delete these files when it does not see a corresponding instant file under .hoodie
            List<HoodieRollbackStat> statsForCompaction = doRollbackAndGetStats(instant);
            instantsToStats.put(instant.getTimestamp(), statsForCompaction);
            LOG.info("Deleted compaction instant " + instant);
            break;
          default:
            throw new IllegalArgumentException("invalid action name " + instant.getAction());
        }
      } catch (IOException io) {
        throw new HoodieRollbackException("unable to rollback instant " + instant, io);
      }
    });
    try {
      finishRestore(context, instantsToStats.build(),
          instantsToRollback.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList()),
          startRollbackInstant, instantTime);
    } catch (IOException io) {
      throw new HoodieRollbackException("unable to rollback instants " + instantsToRollback, io);
    }
  }

  private Timer.Context startContext() {
    return metrics.getRollbackCtx();
  }

  private void finishRestore(final Timer.Context context, Map<String, List<HoodieRollbackStat>> commitToStats,
      List<String> commitsToRollback, final String startRestoreTime, final String restoreToInstant) throws IOException {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, conf);
    Option<Long> durationInMs = Option.empty();
    long numFilesDeleted = 0L;
    for (Map.Entry<String, List<HoodieRollbackStat>> commitToStat : commitToStats.entrySet()) {
      List<HoodieRollbackStat> stats = commitToStat.getValue();
      numFilesDeleted = stats.stream().mapToLong(stat -> stat.getSuccessDeleteFiles().size()).sum();
    }
    if (context != null) {
      durationInMs = Option.of(metrics.getDurationInMs(context.stop()));
      metrics.updateRollbackMetrics(durationInMs.get(), numFilesDeleted);
    }
    HoodieRestoreMetadata restoreMetadata =
        AvroUtils.convertRestoreMetadata(startRestoreTime, durationInMs, commitsToRollback, commitToStats);
    table.getActiveTimeline().saveAsComplete(new HoodieInstant(true, HoodieTimeline.RESTORE_ACTION, startRestoreTime),
        AvroUtils.serializeRestoreMetadata(restoreMetadata));
    LOG.info("Commits " + commitsToRollback + " rollback is complete. Restored table to " + restoreToInstant);

    if (!table.getActiveTimeline().getCleanerTimeline().empty()) {
      LOG.info("Cleaning up older restore meta files");
      // Cleanup of older cleaner meta files
      // TODO - make the commit archival generic and archive rollback metadata
      FSUtils.deleteOlderRollbackMetaFiles(fs, table.getMetaClient().getMetaPath(),
          table.getActiveTimeline().getRestoreTimeline().getInstants());
    }
  }

  /**
   * Releases any resources used by the client.
   */
  @Override
  public void close() {
    // Stop timeline-server if running
    super.close();
    //this.cleanClient.close();
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   */
  public void clean() throws HoodieIOException {
    cleanClient.clean();
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   *
   * @param startCleanTime Cleaner Instant Timestamp
   * @throws HoodieIOException in case of any IOException
   */
  protected HoodieCleanMetadata clean(String startCleanTime) throws HoodieIOException {
    return cleanClient.clean(startCleanTime);
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete).
   */
  public String startCommit() {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackPending) {
      // Only rollback pending commit/delta-commits. Do not touch compaction commits
      rollbackPendingCommits();
    }
    String commitTime = HoodieActiveTimeline.createNewInstantTime();
    startCommit(commitTime);
    return commitTime;
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete).
   *
   * @param instantTime Instant time to be generated
   */
  public void startCommitWithTime(String instantTime) {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackPending) {
      // Only rollback inflight commit/delta-commits. Do not touch compaction commits
      rollbackPendingCommits();
    }
    startCommit(instantTime);
  }

  private void startCommit(String instantTime) {
    LOG.info("Generate a new instant time " + instantTime);
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // if there are pending compactions, their instantTime must not be greater than that of this instant time
    metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().ifPresent(latestPending -> {
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(latestPending.getTimestamp(), instantTime, HoodieTimeline.LESSER),
          "Latest pending compaction instant time must be earlier than this instant time. Latest Compaction :"
              + latestPending + ",  Ingesting at " + instantTime);
    });
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, conf);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    String commitActionType = table.getMetaClient().getCommitActionType();
    activeTimeline.createNewInstant(new HoodieInstant(State.REQUESTED, commitActionType, instantTime));
  }

  /**
   * Schedules a new compaction instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleCompaction(Option<Map<String, String>> extraMetadata) throws IOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    LOG.info("Generate a new instant time " + instantTime);
    boolean notEmpty = scheduleCompactionAtInstant(instantTime, extraMetadata);
    return notEmpty ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new compaction instant with passed-in instant time.
   *
   * @param instantTime Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata)
      throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // if there are inflight writes, their instantTime must not be less than that of compaction instant time
    metaClient.getCommitsTimeline().filterPendingExcludingCompaction().firstInstant().ifPresent(earliestInflight -> {
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(earliestInflight.getTimestamp(), instantTime, HoodieTimeline.GREATER),
          "Earliest write inflight instant time must be later than compaction time. Earliest :" + earliestInflight
              + ", Compaction scheduled at " + instantTime);
    });
    // Committed and pending compaction instants should have strictly lower timestamps
    List<HoodieInstant> conflictingInstants = metaClient
        .getActiveTimeline().getCommitsAndCompactionTimeline().getInstants().filter(instant -> HoodieTimeline
            .compareTimestamps(instant.getTimestamp(), instantTime, HoodieTimeline.GREATER_OR_EQUAL))
        .collect(Collectors.toList());
    Preconditions.checkArgument(conflictingInstants.isEmpty(),
        "Following instants have timestamps >= compactionInstant (" + instantTime + ") Instants :"
            + conflictingInstants);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, conf);
    HoodieCompactionPlan workload = table.scheduleCompaction(conf, instantTime);
    if (workload != null && (workload.getOperations() != null) && (!workload.getOperations().isEmpty())) {
      extraMetadata.ifPresent(workload::setExtraMetadata);
      HoodieInstant compactionInstant =
          new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
      metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
          AvroUtils.serializeCompactionPlan(workload));
      return true;
    }
    return false;
  }

  /**
   * Performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return List of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> compact(String compactionInstantTime) throws IOException {
    return compact(compactionInstantTime, config.shouldAutoCommit());
  }

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @param writeStatuses RDD of WriteStatus to inspect errors and counts
   * @param extraMetadata Extra Metadata to be stored
   */
  public void commitCompaction(String compactionInstantTime, List<WriteStatus> writeStatuses,
                               Option<Map<String, String>> extraMetadata) throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, conf);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieCompactionPlan compactionPlan = AvroUtils.deserializeCompactionPlan(
        timeline.readCompactionPlanAsBytes(HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get());
    // Merge extra meta-data passed by user with the one already in inflight compaction
    Option<Map<String, String>> mergedMetaData = extraMetadata.map(m -> {
      Map<String, String> merged = new HashMap<>();
      Map<String, String> extraMetaDataFromInstantFile = compactionPlan.getExtraMetadata();
      if (extraMetaDataFromInstantFile != null) {
        merged.putAll(extraMetaDataFromInstantFile);
      }
      // Overwrite/Merge with the user-passed meta-data
      merged.putAll(m);
      return Option.of(merged);
    }).orElseGet(() -> Option.ofNullable(compactionPlan.getExtraMetadata()));
    commitCompaction(writeStatuses, table, compactionInstantTime, true, mergedMetaData);
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param records hoodieRecords to deduplicate
   * @param parallelism parallelism or partitions to be used while reducing/deduplicating
   * @return List of HoodieRecord already be deduplicated
   */
  List<HoodieRecord<T>> deduplicateRecords(List<HoodieRecord<T>> records, int parallelism) {
    // TODO support it
    boolean isIndexingGlobal = getIndex().isGlobal();

    Map<Object, List<Tuple2<Object, HoodieRecord<T>>>> key2Records = records.stream().map(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      return new Tuple2<>(key, record);
    }).collect(Collectors.groupingBy(Tuple2::_1));

    List<HoodieRecord<T>> res = new ArrayList<>();
    for (Map.Entry<Object, List<Tuple2<Object, HoodieRecord<T>>>> entry : key2Records.entrySet()) {

      Optional<HoodieRecord<T>> record = entry.getValue().stream()
          .map(pair -> pair._2())
          .reduce((rec1, rec2) -> {
            @SuppressWarnings("unchecked")
            T reducedData = (T) rec1.getData().preCombine(rec2.getData());
            // we cannot allow the user to change the key or partitionPath, since that will affect
            // everything
            // so pick it from one of the records.
            return new HoodieRecord<T>(rec1.getKey(), reducedData);
          });
      assert record.isPresent();
      res.add(record.get());
    }

    return res;
  }


  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param keys List of HoodieKey to deduplicate
   * @return List of HoodieKey already be deduplicated
   */
  List<HoodieKey> deduplicateKeys(List<HoodieKey> keys) {
    boolean isIndexingGlobal = getIndex().isGlobal();
    if (isIndexingGlobal) {
      Map<String, HoodieKey> recordKey2HoodieKey = new HashMap<>();
      keys.stream().map(key -> recordKey2HoodieKey.put(key.getRecordKey(), key));
      List<HoodieKey> res = new ArrayList<>();
      res.addAll(recordKey2HoodieKey.values());
      return res;
    } else {
      Set<HoodieKey> res = keys.stream().collect(Collectors.toSet());
      return res.stream().collect(Collectors.toList());
    }
  }

  /**
   * Cleanup all pending commits.
   */
  private void rollbackPendingCommits() {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, conf);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline().filterPendingExcludingCompaction();
    List<String> commits = inflightTimeline.getReverseOrderedInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    for (String commit : commits) {
      rollback(commit);
    }
  }

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return List of Write Status
   */
  private List<WriteStatus> compact(String compactionInstantTime, boolean autoCommit) throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, conf);
    HoodieTimeline pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      // inflight compaction - Needs to rollback first deleting new parquet files before we run compaction.
      rollbackInflightCompaction(inflightInstant, table);
      // refresh table
      metaClient = createMetaClient(true);
      table = HoodieTable.getHoodieTable(metaClient, config, conf);
      pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    }

    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(instant)) {
      return runCompaction(instant, metaClient.getActiveTimeline(), autoCommit);
    } else {
      throw new IllegalStateException(
          "No Compaction request available at " + compactionInstantTime + " to run compaction");
    }
  }

  /**
   * Perform compaction operations as specified in the compaction commit file.
   *
   * @param compactionInstant Compacton Instant time
   * @param activeTimeline Active Timeline
   * @param autoCommit Commit after compaction
   * @return List of Write Status
   */
  private List<WriteStatus> runCompaction(HoodieInstant compactionInstant, HoodieActiveTimeline activeTimeline,
                                          boolean autoCommit) throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.getCompactionPlan(metaClient, compactionInstant.getTimestamp());
    // Mark instant as compaction inflight
    activeTimeline.transitionCompactionRequestedToInflight(compactionInstant);
    compactionTimer = metrics.getCompactionCtx();
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, conf);
    List<WriteStatus> statuses = table.compact(conf, compactionInstant.getTimestamp(), compactionPlan);
    // Force compaction action
    // statuses.persist(config.getWriteStatusStorageLevel());
    // pass extra-metada so that it gets stored in commit file automatically
    commitCompaction(statuses, table, compactionInstant.getTimestamp(), autoCommit,
        Option.ofNullable(compactionPlan.getExtraMetadata()));
    return statuses;
  }

  /**
   * Commit Compaction and track metrics.
   *
   * @param compactedStatuses Compaction Write status
   * @param table Hoodie Table
   * @param compactionCommitTime Compaction Commit Time
   * @param autoCommit Auto Commit
   * @param extraMetadata Extra Metadata to store
   */
  protected void commitCompaction(List<WriteStatus> compactedStatuses, HoodieTable<T> table,
                                  String compactionCommitTime, boolean autoCommit, Option<Map<String, String>> extraMetadata) {
    if (autoCommit) {
      HoodieCommitMetadata metadata = doCompactionCommit(table, compactedStatuses, compactionCommitTime, extraMetadata);
      if (compactionTimer != null) {
        long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
        try {
          metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(compactionCommitTime).getTime(),
              durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION);
        } catch (ParseException e) {
          throw new HoodieCommitException("Commit time is not of valid format.Failed to commit compaction "
              + config.getBasePath() + " at time " + compactionCommitTime, e);
        }
      }
      LOG.info("Compacted successfully on commit " + compactionCommitTime);
    } else {
      LOG.info("Compaction did not run for commit " + compactionCommitTime);
    }
  }

  /**
   * Rollback failed compactions. Inflight rollbacks for compactions revert the .inflight file to the .requested file
   *
   * @param inflightInstant Inflight Compaction Instant
   * @param table Hoodie Table
   */
  @VisibleForTesting
  void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieTable table) throws IOException {
    table.rollback(conf, inflightInstant, false);
    // Revert instant state file
    table.getActiveTimeline().revertCompactionInflightToRequested(inflightInstant);
  }

  private HoodieCommitMetadata doCompactionCommit(HoodieTable<T> table, List<WriteStatus> writeStatuses,
      String compactionCommitTime, Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    List<HoodieWriteStat> updateStatusMap = writeStatuses.stream()
        .map(WriteStatus::getStat).collect(Collectors.toList());

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }

    // Finalize write
    finalizeWrite(table, compactionCommitTime, updateStatusMap);

    // Copy extraMetadata
    extraMetadata.ifPresent(m -> {
      m.forEach(metadata::addMetadata);
    });

    LOG.info("Committing Compaction " + compactionCommitTime + ". Finished with result " + metadata);
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    try {
      activeTimeline.transitionCompactionInflightToComplete(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionCommitTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + metaClient.getBasePath() + " at time " + compactionCommitTime, e);
    }
    return metadata;
  }

  /**
   * Performs a compaction operation on a table, serially before or after an insert/upsert action.
   */
  private Option<String> forceCompact(Option<Map<String, String>> extraMetadata) throws IOException {
    Option<String> compactionInstantTimeOpt = scheduleCompaction(extraMetadata);
    compactionInstantTimeOpt.ifPresent(compactionInstantTime -> {
      try {
        // inline compaction should auto commit as the user is never given control
        compact(compactionInstantTime, true);
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    });
    return compactionInstantTimeOpt;
  }
}