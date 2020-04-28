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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hub.config.HoodieWriteConfig;
import org.apache.hub.table.HoodieTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Hoodie Index implementation backed by an in-memory Hash map.
 * <p>
 * ONLY USE FOR LOCAL TESTING
 */
public class InMemoryHashIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {

  private static ConcurrentMap<HoodieKey, HoodieRecordLocation> recordLocationMap;

  public InMemoryHashIndex(HoodieWriteConfig config) {
    super(config);
    synchronized (InMemoryHashIndex.class) {
      if (recordLocationMap == null) {
        recordLocationMap = new ConcurrentHashMap<>();
      }
    }
  }

  @Override
  public List<Tuple2<HoodieKey, Option<Pair<String, String>>>> fetchRecordLocation(
      List<HoodieKey> hoodieKeys, HoodieTable<T> hoodieTable) {
    throw new HoodieException("InMemory index does not implement check exist yet");
  }

  @Override
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> recordRDD,
      HoodieTable<T> hoodieTable) {
    List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
    Iterator<HoodieRecord<T>> hoodieRecordIterator = recordRDD.iterator();
    while (hoodieRecordIterator.hasNext()) {
      HoodieRecord<T> rec = hoodieRecordIterator.next();
      if (recordLocationMap.containsKey(rec.getKey())) {
        rec.unseal();
        rec.setCurrentLocation(recordLocationMap.get(rec.getKey()));
        rec.seal();
      }
      taggedRecords.add(rec);
    }
    return taggedRecords;
  }

  @Override
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatusRDD,
      HoodieTable<T> hoodieTable) {
    return writeStatusRDD.stream().map(writeStatus -> {
      for (HoodieRecord record : writeStatus.getWrittenRecords()) {
        if (!writeStatus.isErrored(record.getKey())) {
          HoodieKey key = record.getKey();
          Option<HoodieRecordLocation> newLocation = record.getNewLocation();
          if (newLocation.isPresent()) {
            recordLocationMap.put(key, newLocation.get());
          } else {
            // Delete existing index for a deleted record
            recordLocationMap.remove(key);
          }
        }
      }
      return writeStatus;
    }).collect(Collectors.toList());
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    return true;
  }

  /**
   * Only looks up by recordKey.
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * Mapping is available in HBase already.
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * Index needs to be explicitly updated after storage write.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }
}
