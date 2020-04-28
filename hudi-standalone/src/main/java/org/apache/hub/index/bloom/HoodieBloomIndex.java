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

package org.apache.hub.index.bloom;

import org.apache.hub.WriteStatus;
import org.apache.hub.io.HoodieKeyLookupHandle;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hub.config.HoodieIndexConfig;
import org.apache.hub.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hub.index.HoodieIndex;
import org.apache.hub.io.HoodieRangeInfoHandle;
import org.apache.hub.table.HoodieTable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.jooq.lambda.Seq;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Tuple2;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Indexing mechanism based on bloom filter. Each parquet file includes its row_key bloom filter in its metadata.
 */
public class HoodieBloomIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {

  // we need to limit the join such that it stays within 1.5GB per Spark partition. (SPARK-1476)
  private static final int SPARK_MAXIMUM_BYTES_PER_PARTITION = 1500 * 1024 * 1024;
  // this is how much a triplet of (partitionPath, fileId, recordKey) costs.
  private static final int BYTES_PER_PARTITION_FILE_KEY_TRIPLET = 300;
  private static final Logger LOG = LogManager.getLogger(HoodieBloomIndex.class);
  private static int MAX_ITEMS_PER_SHUFFLE_PARTITION =
      SPARK_MAXIMUM_BYTES_PER_PARTITION / BYTES_PER_PARTITION_FILE_KEY_TRIPLET;

  public HoodieBloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public List<Tuple2<HoodieKey, Option<Pair<String, String>>>> fetchRecordLocation(
      List<HoodieKey> hoodieKeys, HoodieTable<T> hoodieTable) {
    throw new HoodieException("fetchRecordLocation not implement");
  }

  @Override
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> recordRDD,
      HoodieTable<T> hoodieTable) {
    List<Tuple2<HoodieKey, HoodieRecordLocation>> keyFilenamePairRecords = lookupIndex(recordRDD, hoodieTable);
    return tagLocationBacktoRecords(keyFilenamePairRecords, recordRDD);
  }

  @Override
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatus, HoodieTable<T> hoodieTable)
      throws HoodieIndexException {
    return writeStatus;
  }

  private List<Tuple2<HoodieKey, HoodieRecordLocation>> lookupIndex(
      List<HoodieRecord<T>> recordList,
      final HoodieTable hoodieTable) {

    Map<String, Long> recordsPerPartition = recordList.stream()
        .map(HoodieRecord::getPartitionPath)
        .collect(groupingBy(Function.identity(), Collectors.counting()));

    List<String> affectedPartitionPathList = new ArrayList<>(recordsPerPartition.keySet());

    List<Tuple2<String, BloomIndexFileInfo>> fileInfoList =
        loadInvolvedFiles(affectedPartitionPathList, hoodieTable);
    final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo =
        fileInfoList.stream().collect(groupingBy(Tuple2::_1, mapping(Tuple2::_2, toList())));

    Map<String, Long> comparisonsPerFileGroup =
        computeComparisonsPerFileGroup(recordsPerPartition, partitionToFileInfo, recordList);

    return findMatchingFilesForRecordKeys(partitionToFileInfo, recordList, hoodieTable);
  }

  List<Tuple2<HoodieKey, HoodieRecordLocation>> findMatchingFilesForRecordKeys(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      List<HoodieRecord<T>> recordList, HoodieTable hoodieTable) {
    List<Tuple2<String, HoodieKey>> fileComparisonsRDD =
        explodeRecordRDDWithFileComparisons(partitionToFileIndexInfo, recordList);

    List<HoodieKeyLookupHandle.KeyLookupResult> keyLookupResults =
        new KeyCheck(fileComparisonsRDD.iterator(), hoodieTable, config).computeNext();

    return keyLookupResults.stream()
        .filter(lr -> lr.getMatchingRecordKeys().size() > 0)
        .flatMap(lookupResult -> lookupResult.getMatchingRecordKeys().stream()
            .map(recordKey -> new Tuple2<>(new HoodieKey(recordKey, lookupResult.getPartitionPath()),
                new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId())))
            .collect(toList()).stream()).collect(toList());
  }

  private Map<String, Long> computeComparisonsPerFileGroup(final Map<String, Long> recordsPerPartition,
                                                           final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo,
                                                           List<HoodieRecord<T>> recordList) {
    Map<String, Long> fileToComparisons;
    if (config.getBloomIndexPruneByRanges()) {
      // we will just try exploding the input and then count to determine comparisons
      // FIX(vc): Only do sampling here and extrapolate?
      fileToComparisons = explodeRecordRDDWithFileComparisons(partitionToFileInfo, recordList)
          .stream().map(item -> item._1).collect(groupingBy(Function.identity(), Collectors.counting()));
    } else {
      fileToComparisons = new HashMap<>();
      partitionToFileInfo.entrySet().stream().forEach(e -> {
        for (BloomIndexFileInfo fileInfo : e.getValue()) {
          // each file needs to be compared against all the records coming into the partition
          fileToComparisons.put(fileInfo.getFileId(), recordsPerPartition.get(e.getKey()));
        }
      });
    }
    return fileToComparisons;
  }

  List<Tuple2<String, HoodieKey>> explodeRecordRDDWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      List<HoodieRecord<T>> recordList) {
    IndexFileFilter indexFileFilter =
        config.useBloomIndexTreebasedFilter() ? new IntervalTreeBasedIndexFileFilter(partitionToFileIndexInfo)
            : new ListBasedIndexFileFilter(partitionToFileIndexInfo);

    return recordList.stream().flatMap(record -> {
      String recordKey = record.getRecordKey();
      String partitionPath = record.getPartitionPath();

      return indexFileFilter.getMatchingFilesAndPartition(partitionPath, recordKey).stream()
          .map(partitionFileIdPair -> new Tuple2<>(partitionFileIdPair.getRight(),
              new HoodieKey(recordKey, partitionPath)))
          .collect(toList()).stream();
    }).collect(toList());
  }

  /**
   * Compute the minimum parallelism needed to play well with the spark 2GB limitation.. The index lookup can be skewed
   * in three dimensions : #files, #partitions, #records
   * <p>
   * To be able to smoothly handle skews, we need to compute how to split each partitions into subpartitions. We do it
   * here, in a way that keeps the amount of each Spark join partition to < 2GB.
   * <p>
   * If {@link HoodieIndexConfig#BLOOM_INDEX_PARALLELISM_PROP} is specified as a NON-zero number, then that is used
   * explicitly.
   */
  int computeSafeParallelism(Map<String, Long> recordsPerPartition, Map<String, Long> comparisonsPerFileGroup) {
    long totalComparisons = comparisonsPerFileGroup.values().stream().mapToLong(Long::longValue).sum();
    long totalFiles = comparisonsPerFileGroup.size();
    long totalRecords = recordsPerPartition.values().stream().mapToLong(Long::longValue).sum();
    int parallelism = (int) (totalComparisons / MAX_ITEMS_PER_SHUFFLE_PARTITION + 1);
    LOG.info(String.format(
        "TotalRecords %d, TotalFiles %d, TotalAffectedPartitions %d, TotalComparisons %d, SafeParallelism %d",
        totalRecords, totalFiles, recordsPerPartition.size(), totalComparisons, parallelism));
    return parallelism;
  }

  /**
   * Its crucial to pick the right parallelism.
   * <p>
   * totalSubPartitions : this is deemed safe limit, to be nice with Spark. inputParallelism : typically number of input
   * file splits
   * <p>
   * We pick the max such that, we are always safe, but go higher if say a there are a lot of input files. (otherwise,
   * we will fallback to number of partitions in input and end up with slow performance)
   */
  private int determineParallelism(int inputParallelism, int totalSubPartitions) {
    // If bloom index parallelism is set, use it to to check against the input parallelism and
    // take the max
    int indexParallelism = Math.max(inputParallelism, config.getBloomIndexParallelism());
    int joinParallelism = Math.max(totalSubPartitions, indexParallelism);
    LOG.info("InputParallelism: ${" + inputParallelism + "}, IndexParallelism: ${"
        + config.getBloomIndexParallelism() + "}, TotalSubParts: ${" + totalSubPartitions + "}, "
        + "Join Parallelism set to : " + joinParallelism);
    return joinParallelism;
  }

  /**
   * Load all involved files as <Partition, filename> pair RDD.
   */
  @VisibleForTesting
  List<Tuple2<String, BloomIndexFileInfo>> loadInvolvedFiles(List<String> partitions,
      final HoodieTable hoodieTable) {

    // Obtain the latest data files from all the partitions.
    List<Pair<String, String>> partitionPathFileIDList =
        partitions.stream().flatMap(partitionPath -> {
          Option<HoodieInstant> latestCommitTime =
              hoodieTable.getMetaClient().getCommitsTimeline().filterCompletedInstants().lastInstant();
          List<Pair<String, String>> filteredFiles = new ArrayList<>();
          if (latestCommitTime.isPresent()) {
            filteredFiles = hoodieTable.getBaseFileOnlyView()
                .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime.get().getTimestamp())
                .map(f -> Pair.of(partitionPath, f.getFileId())).collect(toList());
          }
          return filteredFiles.stream();
        }).collect(toList());

    if (config.getBloomIndexPruneByRanges()) {
      // also obtain file ranges, if range pruning is enabled
      return partitionPathFileIDList.stream().map(pf -> {
        try {
          HoodieRangeInfoHandle<T> rangeInfoHandle = new HoodieRangeInfoHandle<T>(config, hoodieTable, pf);
          String[] minMaxKeys = rangeInfoHandle.getMinMaxKeys();
          return new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue(), minMaxKeys[0], minMaxKeys[1]));
        } catch (MetadataNotFoundException me) {
          LOG.warn("Unable to find range metadata in file :" + pf);
          return new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue()));
        }
      }).collect(Collectors.toList());
    } else {
      return partitionPathFileIDList.stream()
          .map(pf -> new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue()))).collect(toList());
    }
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    // Nope, don't need to do anything.
    return true;
  }

  /**
   * This is not global, since we depend on the partitionPath to do the lookup.
   */
  @Override
  public boolean isGlobal() {
    return false;
  }

  /**
   * No indexes into log files yet.
   */
  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  /**
   * Bloom filters are stored, into the same data files.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  HoodieRecord<T> getTaggedRecord(HoodieRecord<T> inputRecord, Option<HoodieRecordLocation> location) {
    HoodieRecord<T> record = inputRecord;
    if (location.isPresent()) {
      // When you have a record in multiple files in the same partition, then rowKeyRecordPairRDD
      // will have 2 entries with the same exact in memory copy of the HoodieRecord and the 2
      // separate filenames that the record is found in. This will result in setting
      // currentLocation 2 times and it will fail the second time. So creating a new in memory
      // copy of the hoodie record.
      record = new HoodieRecord<>(inputRecord);
      record.unseal();
      record.setCurrentLocation(location.get());
      record.seal();
    }
    return record;
  }

  /**
   * Tag the <rowKey, filename> back to the original HoodieRecord RDD.
   */
  protected List<HoodieRecord<T>> tagLocationBacktoRecords(
      List<Tuple2<HoodieKey, HoodieRecordLocation>> keyFilenamePairRecords,
      List<HoodieRecord<T>> recordList) {

    List<Tuple2<HoodieKey, HoodieRecord<T>>> keyRecordPairRecords =
        recordList.stream().map(record -> new Tuple2<>(record.getKey(), record)).collect(toList());


    // Here as the recordRDD might have more data than rowKeyRDD (some rowKeys' fileId is null),
    // so we do left outer join.
    return Seq.seq(keyRecordPairRecords).flatMap(v1 -> Seq.seq(keyFilenamePairRecords)
        .filter(v2 -> Objects.equals(v1._1, v2._1))
        .onEmpty(null)
        .map(v2 -> new Tuple2<>(v1, v2))
    ).map(pair ->
        {
          Tuple2<HoodieKey, HoodieRecordLocation> hoodieKeyHoodieRecordLocationTuple2 = pair._2;
          Option<HoodieRecordLocation> location = null;
          if(hoodieKeyHoodieRecordLocationTuple2 == null){
            location = Option.empty();
          }else{
            location = Option.of(hoodieKeyHoodieRecordLocationTuple2._2);
          }
          return getTaggedRecord(pair._1._2, location);
        }
    ).collect(toList());
  }
}
