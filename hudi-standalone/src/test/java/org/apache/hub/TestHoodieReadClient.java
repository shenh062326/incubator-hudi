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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hub.config.HoodieWriteConfig;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
/**
 * Test-cases for covering HoodieReadClient APIs
 */
public class TestHoodieReadClient extends TestHoodieClientBase {

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.insert.
   */
  @Test
  public void testReadFilterExistAfterInsert() throws Exception {
    testReadFilterExist(getConfig(), HoodieWriteClient::insert);
  }

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.insertPrepped.
   */
  @Test
  public void testReadFilterExistAfterInsertPrepped() throws Exception {
    testReadFilterExist(getConfig(), HoodieWriteClient::insertPreppedRecords);
  }

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.bulkInsert.
   */
  @Test
  public void testReadFilterExistAfterBulkInsert() throws Exception {
    testReadFilterExist(getConfigBuilder().withBulkInsertParallelism(1).build(), HoodieWriteClient::bulkInsert);
  }

  /**
   * Test ReadFilter API after writing new records using HoodieWriteClient.bulkInsertPrepped.
   */
  @Test
  public void testReadFilterExistAfterBulkInsertPrepped() throws Exception {
    testReadFilterExist(getConfigBuilder().withBulkInsertParallelism(1).build(),
        (writeClient, recordRDD, commitTime) -> {
          return writeClient.bulkInsertPreppedRecords(recordRDD, commitTime);
        });
  }

  /**
   * Helper to write new records using one of HoodieWriteClient's write API and use ReadClient to test filterExists()
   * API works correctly.
   *
   * @param config Hoodie Write Config
   * @param writeFn Write Function for writing records
   * @throws Exception in case of error
   */
  private void testReadFilterExist(HoodieWriteConfig config,
                                   Function3<List<WriteStatus>, HoodieWriteClient, List<HoodieRecord>, String> writeFn) throws Exception {
    try (HoodieWriteClient writeClient = getHoodieWriteClient(config);
         HoodieReadClient readClient = getHoodieReadClient(config.getBasePath());) {
      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      List<HoodieRecord> recordsRDD = records;

      List<HoodieRecord> filteredRDD = readClient.filterExists(recordsRDD);

      // Should not find any files
      assertEquals(100, filteredRDD.size());

      List<HoodieRecord> smallRecordsRDD = records.subList(0, 75);
      // We create three parquet file, each having one record. (3 different partitions)
      List<WriteStatus> statuses = writeFn.apply(writeClient, smallRecordsRDD, newCommitTime);
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      try (HoodieReadClient anotherReadClient = getHoodieReadClient(config.getBasePath());) {
        filteredRDD = anotherReadClient.filterExists(recordsRDD);
        List<HoodieRecord> result = filteredRDD;
        // Check results
        assertEquals(25, result.size());
      }
    }
  }

  /**
   * Test tagLocation API after insert().
   */
  @Test
  public void testTagLocationAfterInsert() throws Exception {
    testTagLocation(getConfig(), HoodieWriteClient::insert, HoodieWriteClient::upsert, false);
  }

  /**
   * Test tagLocation API after insertPrepped().
   */
  @Test
  public void testTagLocationAfterInsertPrepped() throws Exception {
    testTagLocation(getConfig(), HoodieWriteClient::insertPreppedRecords, HoodieWriteClient::upsertPreppedRecords,
        true);
  }

  /**
   * Test tagLocation API after bulk-insert().
   */
  @Test
  public void testTagLocationAfterBulkInsert() throws Exception {
    testTagLocation(getConfigBuilder().withBulkInsertParallelism(1).build(), HoodieWriteClient::bulkInsert,
        HoodieWriteClient::upsert, false);
  }

  /**
   * Test tagLocation API after bulkInsertPrepped().
   */
  @Test
  public void testTagLocationAfterBulkInsertPrepped() throws Exception {
    testTagLocation(
        getConfigBuilder().withBulkInsertParallelism(1).build(), (writeClient, recordRDD, commitTime) -> writeClient
            .bulkInsertPreppedRecords(recordRDD, commitTime),
        HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Helper method to test tagLocation after using different HoodieWriteClient write APIS.
   *
   * @param hoodieWriteConfig Write Config
   * @param insertFn Hoodie Write Client first Insert API
   * @param updateFn Hoodie Write Client upsert API
   * @param isPrepped isPrepped flag.
   * @throws Exception in case of error
   */
  private void testTagLocation(HoodieWriteConfig hoodieWriteConfig,
                               Function3<List<WriteStatus>, HoodieWriteClient, List<HoodieRecord>, String> insertFn,
                               Function3<List<WriteStatus>, HoodieWriteClient, List<HoodieRecord>, String> updateFn, boolean isPrepped)
      throws Exception {
    try (HoodieWriteClient client = getHoodieWriteClient(hoodieWriteConfig);) {
      // Write 1 (only inserts)
      String newCommitTime = "001";
      String initCommitTime = "000";
      int numRecords = 200;
      List<WriteStatus> result = insertFirstBatch(hoodieWriteConfig, client, newCommitTime, initCommitTime,
          numRecords, insertFn, isPrepped, true, numRecords);
      // Construct HoodieRecord from the WriteStatus but set HoodieKey, Data and HoodieRecordLocation accordingly
      // since they have been modified in the DAG
      List<HoodieRecord> recordRDD =
          result.stream().map(WriteStatus::getWrittenRecords).flatMap(Collection::stream)
              .map(record -> new HoodieRecord(record.getKey(), null)).collect(Collectors.toList());
      // Should have 100 records in table (check using Index), all in locations marked at commit
      HoodieReadClient readClient = getHoodieReadClient(hoodieWriteConfig.getBasePath());
      List<HoodieRecord> taggedRecords = readClient.tagLocation(recordRDD);
      checkTaggedRecords(taggedRecords, newCommitTime);

      // Write 2 (updates)
      String prevCommitTime = newCommitTime;
      newCommitTime = "004";
      numRecords = 100;
      String commitTimeBetweenPrevAndNew = "002";
      result = updateBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), initCommitTime, numRecords, updateFn, isPrepped, true,
          numRecords, 200, 2);
      recordRDD =
          result.stream().map(WriteStatus::getWrittenRecords).flatMap(Collection::stream)
              .map(record -> new HoodieRecord(record.getKey(), null)).collect(Collectors.toList());
      // Index should be able to locate all updates in correct locations.
      readClient = getHoodieReadClient(hoodieWriteConfig.getBasePath());
      taggedRecords = readClient.tagLocation(recordRDD);
      checkTaggedRecords(taggedRecords, newCommitTime);
    }
  }
}
