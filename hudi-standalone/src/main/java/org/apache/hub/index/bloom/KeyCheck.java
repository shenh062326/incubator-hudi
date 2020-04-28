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

import com.google.common.collect.Maps;
import org.apache.hub.config.HoodieWriteConfig;
import org.apache.hub.io.HoodieKeyLookupHandle;
import org.apache.hub.table.HoodieTable;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class KeyCheck {

  private final HoodieTable hoodieTable;

  private final HoodieWriteConfig config;
  private Map<Pair<String, String>, HoodieKeyLookupHandle> keyLookupHandleMaps = Maps.newHashMap();
  private Iterator<Tuple2<String, HoodieKey>> filePartitionRecordKeyTripletItr;

  KeyCheck(Iterator<Tuple2<String, HoodieKey>> filePartitionRecordKeyTripletItr,
           HoodieTable hoodieTable, HoodieWriteConfig config) {
    this.filePartitionRecordKeyTripletItr = filePartitionRecordKeyTripletItr;
    this.hoodieTable = hoodieTable;
    this.config = config;
  }

  protected List<HoodieKeyLookupHandle.KeyLookupResult> computeNext() {

    List<HoodieKeyLookupHandle.KeyLookupResult> ret = new ArrayList<>();
    try {
      // process one file in each go.
      while (filePartitionRecordKeyTripletItr.hasNext()) {
        Tuple2<String, HoodieKey> currentTuple = filePartitionRecordKeyTripletItr.next();
        String fileId = currentTuple._1;
        String partitionPath = currentTuple._2.getPartitionPath();
        String recordKey = currentTuple._2.getRecordKey();
        Pair<String, String> partitionPathFilePair = Pair.of(partitionPath, fileId);

        HoodieKeyLookupHandle keyLookupHandle = keyLookupHandleMaps.get(partitionPathFilePair);
        // lazily init state
        if (keyLookupHandle == null) {
          keyLookupHandle = new HoodieKeyLookupHandle(config, hoodieTable, partitionPathFilePair);
          keyLookupHandleMaps.put(partitionPathFilePair, keyLookupHandle);
          keyLookupHandle.addKey(recordKey);
        } else {
          keyLookupHandle.addKey(recordKey);
        }
      }

      keyLookupHandleMaps.values().forEach(keyLookupHandle -> ret.add(keyLookupHandle.getLookupResult()));
    } catch (Throwable e) {
      if (e instanceof HoodieException) {
        throw e;
      }
      throw new HoodieIndexException("Error checking bloom filter index. ", e);
    }

    return ret;
  }
}
