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

package org.apache.hub.io.compact;

import org.apache.hadoop.conf.Configuration;
import org.apache.hub.WriteStatus;
import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView.SliceView;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hub.config.HoodieWriteConfig;
import org.apache.hub.io.compact.strategy.CompactionStrategy;
import org.apache.hub.table.HoodieCopyOnWriteTable;
import org.apache.hub.table.HoodieTable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

/**
 * Compacts a hoodie table with merge on read storage. Computes all possible compactions,
 * passes it through a CompactionFilter and executes all the compactions and writes a new version of base files and make
 * a normal commit
 *
 * @see HoodieCompactor
 */
public class HoodieMergeOnReadTableCompactor implements HoodieCompactor {

  private static final Logger LOG = LogManager.getLogger(HoodieMergeOnReadTableCompactor.class);

  @Override
  public List<WriteStatus> compact(Configuration conf, HoodieCompactionPlan compactionPlan,
                                      HoodieTable hoodieTable, HoodieWriteConfig config, String compactionInstantTime) throws IOException {
    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      return new ArrayList<WriteStatus>();
    }
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    // Compacting is very similar to applying updates to existing file
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, conf);
    List<CompactionOperation> operations = compactionPlan.getOperations().stream()
        .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
    LOG.info("Compactor compacting " + operations + " files");

    return operations.stream().map(s -> compact(table, metaClient, config, s, compactionInstantTime))
        .reduce((list1, list2) -> {
          list1.addAll(list2);
          return list1;
        }).get();
  }

  private List<WriteStatus> compact(HoodieCopyOnWriteTable hoodieCopyOnWriteTable, HoodieTableMetaClient metaClient,
      HoodieWriteConfig config, CompactionOperation operation, String commitTime) {
    FileSystem fs = metaClient.getFs();

    Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    LOG.info("Compacting base " + operation.getDataFileName() + " with delta files " + operation.getDeltaFileNames()
        + " for commit " + commitTime);
    // TODO - FIX THIS
    // Reads the entire avro file. Always only specific blocks should be read from the avro file
    // (failure recover).
    // Load all the delta commits since the last compaction commit and get all the blocks to be
    // loaded and load it using CompositeAvroLogReader
    // Since a DeltaCommit is not defined yet, reading all the records. revisit this soon.
    String maxInstantTime = metaClient
        .getActiveTimeline().getTimelineOfActions(Sets.newHashSet(HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
        .filterCompletedInstants().lastInstant().get().getTimestamp();
    LOG.info("MaxMemoryPerCompaction => " + config.getMaxMemoryPerCompaction());

    List<String> logFiles = operation.getDeltaFileNames().stream().map(
        p -> new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), operation.getPartitionPath()), p).toString())
        .collect(toList());
    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, metaClient.getBasePath(), logFiles,
        readerSchema, maxInstantTime, config.getMaxMemoryPerCompaction(), config.getCompactionLazyBlockReadEnabled(),
        config.getCompactionReverseLogReadEnabled(), config.getMaxDFSStreamBufferSize(),
        config.getSpillableMapBasePath());
    if (!scanner.iterator().hasNext()) {
      return Lists.<WriteStatus>newArrayList();
    }

    Option<HoodieBaseFile> oldDataFileOpt =
        operation.getBaseFile(metaClient.getBasePath(), operation.getPartitionPath());

    // Compacting is very similar to applying updates to existing file
    Iterator<List<WriteStatus>> result;
    // If the dataFile is present, there is a base parquet file present, perform updates else perform inserts into a
    // new base parquet file.
    if (oldDataFileOpt.isPresent()) {
      result = hoodieCopyOnWriteTable.handleUpdate(commitTime, operation.getFileId(), scanner.getRecords(),
          oldDataFileOpt.get());
    } else {
      result = hoodieCopyOnWriteTable.handleInsert(commitTime, operation.getPartitionPath(), operation.getFileId(),
          scanner.iterator());
    }
    Iterable<List<WriteStatus>> resultIterable = () -> result;
    return StreamSupport.stream(resultIterable.spliterator(), false).flatMap(Collection::stream).peek(s -> {
      s.getStat().setTotalUpdatedRecordsCompacted(scanner.getNumMergedRecordsInLog());
      s.getStat().setTotalLogFilesCompacted(scanner.getTotalLogFiles());
      s.getStat().setTotalLogRecords(scanner.getTotalLogRecords());
      s.getStat().setPartitionPath(operation.getPartitionPath());
      s.getStat()
          .setTotalLogSizeCompacted(operation.getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
      s.getStat().setTotalLogBlocks(scanner.getTotalLogBlocks());
      s.getStat().setTotalCorruptLogBlock(scanner.getTotalCorruptBlocks());
      s.getStat().setTotalRollbackBlocks(scanner.getTotalRollbacks());
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalScanTime(scanner.getTotalTimeTakenToReadAndMergeBlocks());
      s.getStat().setRuntimeStats(runtimeStats);
    }).collect(toList());
  }

  class Accumulator {
    long value = 0;
    Accumulator() {}
    void add (long inc) {
      value += inc;
    }

    long getValue() {
      return value;
    }
  }

  @Override
  public HoodieCompactionPlan generateCompactionPlan(Configuration conf, HoodieTable hoodieTable,
                                                     HoodieWriteConfig config,
                                                     String compactionCommitTime,
                                                     Set<HoodieFileGroupId> fgIdsInPendingCompactions)
      throws IOException {

    Accumulator totalLogFiles = new Accumulator();
    Accumulator totalFileSlices = new Accumulator();

    Preconditions.checkArgument(hoodieTable.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ,
        "Can only compact table of type " + HoodieTableType.MERGE_ON_READ + " and not "
            + hoodieTable.getMetaClient().getTableType().name());

    // TODO : check if maxMemory is not greater than JVM or spark.executor memory
    // TODO - rollback any compactions in flight
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    LOG.info("Compacting " + metaClient.getBasePath() + " with commit " + compactionCommitTime);
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(),
        config.shouldAssumeDatePartitioning());

    // filter the partition paths if needed to reduce list status
    partitionPaths = config.getCompactionStrategy().filterPartitionPaths(config, partitionPaths);

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no compaction plan
      return null;
    }

    SliceView fileSystemView = hoodieTable.getSliceView();
    LOG.info("Compaction looking for files to compact in " + partitionPaths + " partitions");

    List<HoodieCompactionOperation> operations = partitionPaths.stream().flatMap(partitionPath -> fileSystemView
        .getLatestFileSlices(partitionPath)
        .filter(slice -> !fgIdsInPendingCompactions.contains(slice.getFileGroupId())).map(s -> {
          List<HoodieLogFile> logFiles =
              s.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
          totalLogFiles.add((long) logFiles.size());
          totalFileSlices.add(1L);
          // Avro generated classes are not inheriting Serializable. Using CompactionOperation POJO
          // for spark Map operations and collecting them finally in Avro generated classes for storing
          // into meta files.
          Option<HoodieBaseFile> dataFile = s.getBaseFile();
          return new CompactionOperation(dataFile, partitionPath, logFiles,
              config.getCompactionStrategy().captureMetrics(config, dataFile, partitionPath, logFiles));
        }).filter(c -> !c.getDeltaFileNames().isEmpty()))
        .map(CompactionUtils::buildHoodieCompactionOperation).collect(toList());

    LOG.info("Total of " + operations.size() + " compactions are retrieved");
    LOG.info("Total number of latest files slices " + totalFileSlices.getValue());
    LOG.info("Total number of log files " + totalLogFiles.getValue());
    LOG.info("Total number of file slices " + totalFileSlices.getValue());
    // Filter the compactions with the passed in filter. This lets us choose most effective
    // compactions only
    HoodieCompactionPlan compactionPlan = config.getCompactionStrategy().generateCompactionPlan(config, operations,
        CompactionUtils.getAllPendingCompactionPlans(metaClient).stream().map(Pair::getValue).collect(toList()));
    Preconditions.checkArgument(
        compactionPlan.getOperations().stream().noneMatch(
            op -> fgIdsInPendingCompactions.contains(new HoodieFileGroupId(op.getPartitionPath(), op.getFileId()))),
        "Bad Compaction Plan. FileId MUST NOT have multiple pending compactions. "
            + "Please fix your strategy implementation. FileIdsWithPendingCompactions :" + fgIdsInPendingCompactions
            + ", Selected workload :" + compactionPlan);
    if (compactionPlan.getOperations().isEmpty()) {
      LOG.warn("After filtering, Nothing to compact for " + metaClient.getBasePath());
    }
    return compactionPlan;
  }
}
