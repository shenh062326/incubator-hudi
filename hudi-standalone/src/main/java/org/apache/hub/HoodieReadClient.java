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
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hub.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hub.config.HoodieIndexConfig;
import org.apache.hub.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hub.index.HoodieIndex;
import org.apache.hub.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Provides an RDD based API for accessing/filtering Hoodie tables, based on keys.
 */
public class HoodieReadClient<T extends HoodieRecordPayload> extends AbstractHoodieClient {

  private static final Logger LOG = LogManager.getLogger(HoodieReadClient.class);

  /**
   * TODO: We need to persist the index type into hoodie.properties and be able to access the index just with a simple
   * basepath pointing to the table. Until, then just always assume a BloomIndex
   */
  private final transient HoodieIndex<T> index;
  private final HoodieTimeline commitTimeline;
  private HoodieTable hoodieTable;

  /**
   * @param basePath path to Hoodie table
   */
  public HoodieReadClient(Configuration conf, String basePath, Option<EmbeddedTimelineService> timelineService) {
    this(conf, HoodieWriteConfig.newBuilder().withPath(basePath)
        // by default we use HoodieBloomIndex
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build(),
        timelineService);
  }

  /**
   * @param basePath path to Hoodie table
   */
  public HoodieReadClient(Configuration conf, String basePath) {
    this(conf, basePath, Option.empty());
  }

  /**
   * @param clientConfig instance of HoodieWriteConfig
   */
  public HoodieReadClient(Configuration conf, HoodieWriteConfig clientConfig) {
    this(conf, clientConfig, Option.empty());
  }

  /**
   * @param clientConfig instance of HoodieWriteConfig
   */
  public HoodieReadClient(Configuration conf, HoodieWriteConfig clientConfig,
                          Option<EmbeddedTimelineService> timelineService) {
    super(conf, clientConfig, timelineService);
    final String basePath = clientConfig.getBasePath();
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(conf, basePath, true);
    this.hoodieTable = HoodieTable.getHoodieTable(metaClient, clientConfig, conf);
    this.commitTimeline = metaClient.getCommitTimeline().filterCompletedInstants();
    this.index = HoodieIndex.createIndex(clientConfig);
  }

  /**
   * Adds support for accessing Hoodie built tables from SparkSQL, as you normally would.
   *
   * @return SparkConf object to be used to construct the SparkContext by caller
   */
  /*public static SparkConf addHoodieSupport(SparkConf conf) {
    conf.set("spark.sql.hive.convertMetastoreParquet", "false");
    return conf;
  }*/

  private Option<String> convertToDataFilePath(Option<Pair<String, String>> partitionPathFileIDPair) {
    if (partitionPathFileIDPair.isPresent()) {
      HoodieBaseFile dataFile = hoodieTable.getBaseFileOnlyView()
          .getLatestBaseFile(partitionPathFileIDPair.get().getLeft(), partitionPathFileIDPair.get().getRight()).get();
      return Option.of(dataFile.getPath());
    } else {
      return Option.empty();
    }
  }

  /**
   * Checks if the given [Keys] exists in the hoodie table and returns [Key, Option[FullFilePath]] If the optional
   * FullFilePath value is not present, then the key is not found. If the FullFilePath value is present, it is the path
   * component (without scheme) of the URI underlying file
   */
  /*public JavaPairRDD<HoodieKey, Option<String>> checkExists(List<HoodieKey> hoodieKeys) {
    return index.fetchRecordLocation(hoodieKeys, jsc, hoodieTable);
  }*/

  /**
   * Filter out HoodieRecords that already exists in the output folder. This is useful in deduplication.
   *
   * @param hoodieRecords Input RDD of Hoodie records.
   * @return A subset of hoodieRecords RDD, with existing records filtered out.
   */
  public List<HoodieRecord<T>> filterExists(List<HoodieRecord<T>> hoodieRecords) {
    List<HoodieRecord<T>> recordsWithLocation = tagLocation(hoodieRecords);
    return recordsWithLocation.stream().filter(v1 -> !v1.isCurrentLocationKnown()).collect(Collectors.toList());

  }

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains the row (if it is actually
   * present). Input RDD should contain no duplicates if needed.
   *
   * @param hoodieRecords Input RDD of Hoodie records
   * @return Tagged RDD of Hoodie records
   */
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> hoodieRecords) throws HoodieIndexException {
    return index.tagLocation(hoodieRecords, hoodieTable);
  }

  /**
   * Return all pending compactions with instant time for clients to decide what to compact next.
   * 
   * @return
   */
  public List<Pair<String, HoodieCompactionPlan>> getPendingCompactions() {
    HoodieTableMetaClient metaClient =
        new HoodieTableMetaClient(conf, hoodieTable.getMetaClient().getBasePath(), true);
    return CompactionUtils.getAllPendingCompactionPlans(metaClient).stream()
        .map(
            instantWorkloadPair -> Pair.of(instantWorkloadPair.getKey().getTimestamp(), instantWorkloadPair.getValue()))
        .collect(Collectors.toList());
  }
}
