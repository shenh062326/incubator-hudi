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

package org.apache.hub.index;

import org.apache.hub.WriteStatus;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hub.config.HoodieWriteConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hub.index.bloom.HoodieBloomIndex;
import org.apache.hub.index.bloom.HoodieGlobalBloomIndex;
import org.apache.hub.table.HoodieTable;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Base class for different types of indexes to determine the mapping from uuid.
 */
public abstract class HoodieIndex<T extends HoodieRecordPayload> implements Serializable {

  protected final HoodieWriteConfig config;

  protected HoodieIndex(HoodieWriteConfig config) {
    this.config = config;
  }

  public static <T extends HoodieRecordPayload> HoodieIndex<T> createIndex(HoodieWriteConfig config)
      throws HoodieIndexException {
    switch (config.getIndexType()) {
      case INMEMORY:
        return new InMemoryHashIndex<>(config);
      case BLOOM:
        return new HoodieBloomIndex<>(config);
      case GLOBAL_BLOOM:
        return new HoodieGlobalBloomIndex<>(config);
      default:
        throw new HoodieIndexException("Index type unspecified, set " + config.getIndexType());
    }
  }

  /**
   * Checks if the given [Keys] exists in the hoodie table and returns [Key, Option[partitionPath, fileID]] If the
   * optional is empty, then the key is not found.
   */
  public abstract List<Tuple2<HoodieKey, Option<Pair<String, String>>>> fetchRecordLocation(
      List<HoodieKey> hoodieKeys, HoodieTable<T> hoodieTable);

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains the row (if it is actually
   * present).
   */
  public abstract List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> recordRDD,
                                                    HoodieTable<T> hoodieTable) throws HoodieIndexException;

  /**
   * Extracts the location of written records, and updates the index.
   * <p>
   * TODO(vc): We may need to propagate the record as well in a WriteStatus class
   */
  public abstract List<WriteStatus> updateLocation(List<WriteStatus> writeStatus,
      HoodieTable<T> hoodieTable) throws HoodieIndexException;

  /**
   * Rollback the efffects of the commit made at commitTime.
   */
  public abstract boolean rollbackCommit(String commitTime);

  /**
   * An index is `global` if {@link HoodieKey} to fileID mapping, does not depend on the `partitionPath`. Such an
   * implementation is able to obtain the same mapping, for two hoodie keys with same `recordKey` but different
   * `partitionPath`
   *
   * @return whether or not, the index implementation is global in nature
   */
  public abstract boolean isGlobal();

  /**
   * This is used by storage to determine, if its safe to send inserts, straight to the log, i.e having a
   * {@link FileSlice}, with no data file.
   *
   * @return Returns true/false depending on whether the impl has this capability
   */
  public abstract boolean canIndexLogFiles();

  /**
   * An index is "implicit" with respect to storage, if just writing new data to a file slice, updates the index as
   * well. This is used by storage, to save memory footprint in certain cases.
   */
  public abstract boolean isImplicitWithStorage();

  /**
   * Each index type should implement it's own logic to release any resources acquired during the process.
   */
  public void close() {}

  public enum IndexType {
    HBASE, INMEMORY, BLOOM, GLOBAL_BLOOM
  }
}
