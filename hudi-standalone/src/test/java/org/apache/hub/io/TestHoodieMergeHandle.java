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

package org.apache.hub.io;

import org.apache.avro.generic.GenericRecord;
import org.apache.hub.HoodieClientTestHarness;
import org.apache.hub.HoodieWriteClient;
import org.apache.hub.WriteStatus;
import org.apache.hub.common.HoodieClientTestUtils;
import org.apache.hub.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hub.config.HoodieCompactionConfig;
import org.apache.hub.config.HoodieIndexConfig;
import org.apache.hub.config.HoodieStorageConfig;
import org.apache.hub.config.HoodieWriteConfig;
import org.apache.hub.index.HoodieIndex;

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

@SuppressWarnings("unchecked")
public class TestHoodieMergeHandle extends HoodieClientTestHarness {

  @Before
  public void setUp() throws Exception {
    initPath();
    initFileSystem();
    initTestDataGenerator();
    initMetaClient();
  }

  @After
  public void tearDown() throws Exception {
    cleanupFileSystem();
    cleanupTestDataGenerator();
    cleanupMetaClient();
  }

  private HoodieWriteClient getWriteClient(HoodieWriteConfig config) throws Exception {
    return new HoodieWriteClient(conf, config);
  }

  @Test
  public void testUpsertsForMultipleRecordsInSameFile() throws Exception {
    // Create records in a single partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    dataGen = new HoodieTestDataGenerator(new String[] {partitionPath});

    // Build a write config with bulkinsertparallelism set
    HoodieWriteConfig cfg = getConfigBuilder().build();
    try (HoodieWriteClient client = getWriteClient(cfg);) {
      FileSystem fs = FSUtils.getFs(basePath, conf);

      /**
       * Write 1 (only inserts) This will do a bulk insert of 44 records of which there are 2 records repeated 21 times
       * each. id1 (21 records), id2 (21 records), id3, id4
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 4);
      HoodieRecord record1 = records.get(0);
      HoodieRecord record2 = records.get(1);
      for (int i = 0; i < 20; i++) {
        HoodieRecord dup = dataGen.generateUpdateRecord(record1.getKey(), newCommitTime);
        records.add(dup);
      }
      for (int i = 0; i < 20; i++) {
        HoodieRecord dup = dataGen.generateUpdateRecord(record2.getKey(), newCommitTime);
        records.add(dup);
      }
      List<HoodieRecord> writeRecords = records;
      List<WriteStatus> statuses = client.bulkInsert(writeRecords, newCommitTime);
      assertNoWriteErrors(statuses);

      // verify that there is a commit
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
      assertEquals("Expecting a single commit.", 1,
          timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants());
      Assert.assertEquals("Latest commit should be 001", newCommitTime, timeline.lastInstant().get().getTimestamp());
      assertEquals("Must contain 44 records", records.size(),
          HoodieClientTestUtils.readCommit(basePath, timeline, newCommitTime).size());

      /**
       * Write 2 (insert) This will do a bulk insert of 1 record with the same row_key as record1 in the previous insert
       * - id1. At this point, we will have 2 files with the row_keys as shown here - File 1 - id1 (21 records), id2 (21
       * records), id3, id4 File 2 - id1
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      // Do 1 more bulk insert with the same dup record1
      List<HoodieRecord> newRecords = new ArrayList<>();
      HoodieRecord sameAsRecord1 = dataGen.generateUpdateRecord(record1.getKey(), newCommitTime);
      newRecords.add(sameAsRecord1);
      writeRecords = newRecords;
      statuses = client.bulkInsert(writeRecords, newCommitTime);
      assertNoWriteErrors(statuses);

      // verify that there are 2 commits
      metaClient = HoodieTableMetaClient.reload(metaClient);
      timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
      assertEquals("Expecting two commits.", 2, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants());
      Assert.assertEquals("Latest commit should be 002", newCommitTime, timeline.lastInstant().get().getTimestamp());
      List<GenericRecord> dataSet = getRecords();
      assertEquals("Must contain 45 records", 45, dataSet.size());

      /**
       * Write 3 (insert) This will bulk insert 2 new completely new records. At this point, we will have 2 files with
       * the row_keys as shown here - File 1 - id1 (21 records), id2 (21 records), id3, id4 File 2 - id1 File 3 - id5,
       * id6
       */
      newCommitTime = "003";
      client.startCommitWithTime(newCommitTime);
      newRecords = dataGen.generateInserts(newCommitTime, 2);
      writeRecords = newRecords;
      statuses = client.bulkInsert(writeRecords, newCommitTime);
      assertNoWriteErrors(statuses);

      // verify that there are now 3 commits
      metaClient = HoodieTableMetaClient.reload(metaClient);
      timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
      assertEquals("Expecting three commits.", 3, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants());
      Assert.assertEquals("Latest commit should be 003", newCommitTime, timeline.lastInstant().get().getTimestamp());
      dataSet = getRecords();
      assertEquals("Must contain 47 records", 47, dataSet.size());

      /**
       * Write 4 (updates) This will generate 2 upsert records with id1 and id2. The rider and driver names in the
       * update records will be rider-004 and driver-004. After the upsert is complete, all the records with id1 in File
       * 1 and File 2 must be updated, all the records with id2 in File 2 must also be updated. Also, none of the other
       * records in File 1, File 2 and File 3 must be updated.
       */
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> updateRecords = new ArrayList<>();

      // This exists in 001 and 002 and should be updated in both
      sameAsRecord1 = dataGen.generateUpdateRecord(record1.getKey(), newCommitTime);
      updateRecords.add(sameAsRecord1);

      // This exists in 001 and should be updated
      HoodieRecord sameAsRecord2 = dataGen.generateUpdateRecord(record2.getKey(), newCommitTime);
      updateRecords.add(sameAsRecord2);
      List<HoodieRecord> updateRecordsRDD = updateRecords;
      statuses = client.upsert(updateRecordsRDD, newCommitTime);

      // Verify there are no errors
      assertNoWriteErrors(statuses);

      // verify there are now 4 commits
      timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
      assertEquals("Expecting four commits.", 4, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants());
      Assert.assertEquals("Latest commit should be 004", timeline.lastInstant().get().getTimestamp(), newCommitTime);

      // Check the entire dataset has 47 records still
      dataSet = getRecords();
      assertEquals("Must contain 47 records", 47, dataSet.size());
      List<GenericRecord> rows = dataSet;
      int record1Count = 0;
      int record2Count = 0;
      for (GenericRecord row : rows) {
        if (row.get("_hoodie_record_key").toString().equals(record1.getKey().getRecordKey())) {
          record1Count++;
          // assert each duplicate record is updated
          // assertEquals(row.get("rider"), "rider-004");
          // assertEquals(row.get("driver"), "driver-004");
        } else if (row.get("_hoodie_record_key").toString().equals(record2.getKey().getRecordKey())) {
          record2Count++;
          // assert each duplicate record is updated
          // assertEquals(row.get("rider"), "rider-004");
          // assertEquals(row.get("driver"), "driver-004");
        } else {
          // assertNotEquals(row.get("rider"), "rider-004");
          // assertNotEquals(row.get("driver"), "rider-004");
        }
      }
      // Assert that id1 record count which has been updated to rider-004 and driver-004 is 22, which is the total
      // number of records with row_key id1
      assertEquals(22, record1Count);

      // Assert that id2 record count which has been updated to rider-004 and driver-004 is 21, which is the total
      // number of records with row_key id2
      assertEquals(21, record2Count);
    }
  }

  @Test
  public void testHoodieMergeHandleWriteStatMetrics() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfigBuilder().build();
    try (HoodieWriteClient writeClient = getWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      List<HoodieRecord> recordsRDD = records;
      List<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);

      // All records should be inserts into new parquet
      //Assert.assertTrue(statuses.stream()
      //    .filter(status -> status.getStat().getPrevCommit() != HoodieWriteStat.NULL_COMMIT).count() > 0);
      // Num writes should be equal to the number of records inserted
      Assert.assertEquals(
          (long) statuses.stream().map(status -> status.getStat().getNumWrites()).reduce((a, b) -> a + b).get(), 100);
      // Num update writes should be equal to the number of records updated
      Assert.assertEquals(
          (long) statuses.stream().map(status -> status.getStat().getNumUpdateWrites()).reduce((a, b) -> a + b).get(),
          0);
      // Num update writes should be equal to the number of insert records converted to updates as part of small file
      // handling
      Assert.assertEquals(
          (long) statuses.stream().map(status -> status.getStat().getNumInserts()).reduce((a, b) -> a + b).get(), 100);

      // Update all the 100 records
      metaClient = HoodieTableMetaClient.reload(metaClient);

      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      List<HoodieRecord> updatedRecordsRDD = updatedRecords;
      statuses = writeClient.upsert(updatedRecordsRDD, newCommitTime);

      // All records should be upserts into existing parquet
      Assert.assertEquals(
          statuses.stream().filter(status -> status.getStat().getPrevCommit() == HoodieWriteStat.NULL_COMMIT).count(),
          0);
      // Num writes should be equal to the number of records inserted
      Assert.assertEquals(
          (long) statuses.stream().map(status -> status.getStat().getNumWrites()).reduce((a, b) -> a + b).get(), 100);
      // Num update writes should be equal to the number of records updated
      Assert.assertEquals(
          (long) statuses.stream().map(status -> status.getStat().getNumUpdateWrites()).reduce((a, b) -> a + b).get(),
          100);
      // Num update writes should be equal to the number of insert records converted to updates as part of small file
      // handling
      Assert.assertEquals(
          (long) statuses.stream().map(status -> status.getStat().getNumInserts()).reduce((a, b) -> a + b).get(), 0);

      newCommitTime = "102";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> allRecords = dataGen.generateInserts(newCommitTime, 100);
      allRecords.addAll(updatedRecords);
      List<HoodieRecord> allRecordsRDD = allRecords;
      statuses = writeClient.upsert(allRecordsRDD, newCommitTime);

      // All records should be upserts into existing parquet (with inserts as updates small file handled)
      //Assert.assertEquals((long) statuses.stream()
      //    .filter(status -> status.getStat().getPrevCommit() == HoodieWriteStat.NULL_COMMIT).count(), 0);
      // Num writes should be equal to the total number of records written
      Assert.assertEquals(
          (long) statuses.stream().map(status -> status.getStat().getNumWrites()).reduce((a, b) -> a + b).get(), 200);
      // Num update writes should be equal to the number of records updated (including inserts converted as updates)
      Assert.assertEquals(
          (long) statuses.stream().map(status -> status.getStat().getNumUpdateWrites()).reduce((a, b) -> a + b).get(),
          100);
      // Num update writes should be equal to the number of insert records converted to updates as part of small file
      // handling
      Assert.assertEquals(
          (long) statuses.stream().map(status -> status.getStat().getNumInserts()).reduce((a, b) -> a + b).get(), 100);
      // Verify all records have location set
      statuses.forEach(writeStatus -> {
        writeStatus.getWrittenRecords().forEach(r -> {
          // Ensure New Location is set
          Assert.assertTrue(r.getNewLocation().isPresent());
        });
      });
    }
  }

  private List<GenericRecord> getRecords() {
    // Check the entire dataset has 8 records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    List<GenericRecord> dataSet = HoodieClientTestUtils.read(basePath, fs, fullPartitionPaths);
    return dataSet;
  }

  /**
   * Assert no failures in writing hoodie files.
   *
   * @param statuses List of Write Status
   */
  void assertNoWriteErrors(List<WriteStatus> statuses) {
    // Verify there are no errors
    for (WriteStatus status : statuses) {
      assertFalse("Errors found in write of " + status.getFileId(), status.hasErrors());
    }
  }

  HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withBulkInsertParallelism(2).withWriteStatusClass(TestWriteStatus.class);
  }

  /**
   * Overridden so that we can capture and inspect all success records.
   */
  public static class TestWriteStatus extends WriteStatus {

    public TestWriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
      // Track Success Records
      super(true, failureFraction);
    }
  }
}
