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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hub.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hub.table.HoodieTable;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * This filter will only work with hoodie table since it will only load partitions with .hoodie_partition_metadata
 * file in it.
 */
public class HoodieGlobalBloomIndex<T extends HoodieRecordPayload> extends HoodieBloomIndex<T> {

  public HoodieGlobalBloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  /**
   * Load all involved files as <Partition, filename> pair RDD from all partitions in the table.
   */
  @Override
  @VisibleForTesting
  List<Tuple2<String, BloomIndexFileInfo>> loadInvolvedFiles(List<String> partitions,
                                                             final HoodieTable hoodieTable) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    try {
      List<String> allPartitionPaths = FSUtils.getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(),
          config.shouldAssumeDatePartitioning());
      return super.loadInvolvedFiles(allPartitionPaths, hoodieTable);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to load all partitions", e);
    }
  }

  /**
   * For each incoming record, produce N output records, 1 each for each file against which the record's key needs to be
   * checked. For tables, where the keys have a definite insert order (e.g: timestamp as prefix), the number of files
   * to be compared gets cut down a lot from range pruning.
   * <p>
   * Sub-partition to ensure the records can be looked up against files & also prune file<=>record comparisons based on
   * recordKey ranges in the index info. the partition path of the incoming record (partitionRecordKeyPairRDD._2()) will
   * be ignored since the search scope should be bigger than that
   */

  @Override
  @VisibleForTesting
  List<Tuple2<String, HoodieKey>> explodeRecordRDDWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      List<HoodieRecord<T>> recordList) {
    throw new UnsupportedOperationException("unsupport tagLocationBacktoRecords");
  }

  /**
   * Tagging for global index should only consider the record key.
   */
  @Override
  protected List<HoodieRecord<T>> tagLocationBacktoRecords(
      List<Tuple2<HoodieKey, HoodieRecordLocation>> keyLocationPairRDD,
      List<HoodieRecord<T>> recordList) {
    throw new UnsupportedOperationException("unsupport tagLocationBacktoRecords");
  }

  @Override
  public boolean isGlobal() {
    return true;
  }
}
