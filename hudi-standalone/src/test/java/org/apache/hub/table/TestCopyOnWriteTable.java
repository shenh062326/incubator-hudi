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

package org.apache.hub.table;

import org.apache.hub.HoodieClientTestHarness;
import org.apache.hub.WriteStatus;
import org.apache.hub.common.HoodieClientTestUtils;
import org.apache.hub.common.HoodieTestDataGenerator;
import org.apache.hub.common.TestRawTripPayload;
import org.apache.hub.common.TestRawTripPayload.MetadataMergeWriteStatus;
import org.apache.hudi.common.bloom.filter.BloomFilter;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hub.config.HoodieCompactionConfig;
import org.apache.hub.config.HoodieStorageConfig;
import org.apache.hub.config.HoodieWriteConfig;
import org.apache.hub.io.HoodieCreateHandle;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCopyOnWriteTable extends HoodieClientTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestCopyOnWriteTable.class);

  @Before
  public void setUp() throws Exception {
    initPath();
    initMetaClient();
    initTestDataGenerator();
    initFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    cleanupMetaClient();
    cleanupFileSystem();
    cleanupTestDataGenerator();
  }

  @Test
  public void testMakeNewPath() throws Exception {
    String fileName = UUID.randomUUID().toString();
    String partitionPath = "2016/05/04";

    String commitTime = HoodieTestUtils.makeNewCommitTime();
    HoodieWriteConfig config = makeHoodieClientConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, conf);

    HoodieRecord record = mock(HoodieRecord.class);
    when(record.getPartitionPath()).thenReturn(partitionPath);
    String writeToken = FSUtils.makeWriteToken(0, 1,
        2);
    HoodieCreateHandle io = new HoodieCreateHandle(config, commitTime, table, partitionPath, fileName);
    Pair<Path, String> newPathWithWriteToken =
        Pair.of(io.makeNewPath(record.getPartitionPath()), writeToken);


    Assert.assertEquals(newPathWithWriteToken.getKey().toString(), this.basePath + "/" + partitionPath + "/"
        + FSUtils.makeDataFileName(commitTime, newPathWithWriteToken.getRight(), fileName));
  }

  private HoodieWriteConfig makeHoodieClientConfig() throws Exception {
    return makeHoodieClientConfigBuilder().build();
  }

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder() throws Exception {
    // Prepare the AvroParquetIO
    String schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr);
  }

  // TODO (weiy): Add testcases for crossing file writing.
  @Test
  public void testUpdateRecords() throws Exception {
    // Prepare the AvroParquetIO
    HoodieWriteConfig config = makeHoodieClientConfig();
    String firstCommitTime = HoodieTestUtils.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    String partitionPath = "/2016/01/31";
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, conf);

    // Get some records belong to the same partition (2016/01/31)
    String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    String recordStr4 = "{\"_row_key\":\"8eb5b87d-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":51}";

    List<HoodieRecord> records = new ArrayList<>();
    TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
    records.add(new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));
    TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
    records.add(new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2));
    TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
    records.add(new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3));

    // Insert new records
    final HoodieCopyOnWriteTable cowTable = table;
    Arrays.asList(1).stream().map(x -> {
      return cowTable.handleInsert(firstCommitTime, FSUtils.createNewFileIdPfx(), records.iterator());
    }).map(x -> HoodieClientTestUtils.collectStatuses(x)).collect(Collectors.toList());

    // We should have a parquet file generated (TODO: better control # files after we revise
    // AvroParquetIO)
    File parquetFile = null;
    for (File file : new File(this.basePath + partitionPath).listFiles()) {
      if (file.getName().endsWith(".parquet")) {
        parquetFile = file;
        break;
      }
    }
    assertTrue(parquetFile != null);

    // Read out the bloom filter and make sure filter can answer record exist or not
    Path parquetFilePath = new Path(parquetFile.getAbsolutePath());
    BloomFilter filter = ParquetUtils.readBloomFilterFromParquetMetadata(conf, parquetFilePath);
    for (HoodieRecord record : records) {
      assertTrue(filter.mightContain(record.getRecordKey()));
    }
    // Create a commit file
    new File(this.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + FSUtils.getCommitTime(parquetFile.getName()) + ".commit").createNewFile();

    // Read the parquet file, check the record content
    List<GenericRecord> fileRecords = ParquetUtils.readAvroRecords(conf, parquetFilePath);
    GenericRecord newRecord;
    int index = 0;
    for (GenericRecord record : fileRecords) {
      assertTrue(record.get("_row_key").toString().equals(records.get(index).getRecordKey()));
      index++;
    }

    // We update the 1st record & add a new record
    String updateRecordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    TestRawTripPayload updateRowChanges1 = new TestRawTripPayload(updateRecordStr1);
    HoodieRecord updatedRecord1 = new HoodieRecord(
        new HoodieKey(updateRowChanges1.getRowKey(), updateRowChanges1.getPartitionPath()), updateRowChanges1);
    updatedRecord1.unseal();
    updatedRecord1.setCurrentLocation(new HoodieRecordLocation(null, FSUtils.getFileId(parquetFile.getName())));
    updatedRecord1.seal();

    TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
    HoodieRecord insertedRecord1 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    List<HoodieRecord> updatedRecords = Arrays.asList(updatedRecord1, insertedRecord1);

    Thread.sleep(1000);
    String newCommitTime = HoodieTestUtils.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    final HoodieCopyOnWriteTable newTable = new HoodieCopyOnWriteTable(config, conf);
    List<WriteStatus> statuses = new ArrayList<>();
    Iterator<List<WriteStatus>> iter =
        newTable.handleUpdate(newCommitTime, updatedRecord1.getCurrentLocation().getFileId(),
            updatedRecords.iterator());
    while(iter.hasNext()) {
      statuses.addAll(iter.next());
    }

    // Check the updated file
    File updatedParquetFile = null;
    for (File file : new File(basePath + "/2016/01/31").listFiles()) {
      if (file.getName().endsWith(".parquet")) {
        if (FSUtils.getFileId(file.getName()).equals(FSUtils.getFileId(parquetFile.getName()))
            && HoodieTimeline.compareTimestamps(FSUtils.getCommitTime(file.getName()),
            FSUtils.getCommitTime(parquetFile.getName()), HoodieTimeline.GREATER)) {
          updatedParquetFile = file;
          break;
        }
      }
    }
    assertNotNull(updatedParquetFile);
    // Check whether the record has been updated
    Path updatedParquetFilePath = new Path(updatedParquetFile.getAbsolutePath());
    BloomFilter updatedFilter =
        ParquetUtils.readBloomFilterFromParquetMetadata(conf, updatedParquetFilePath);
    for (HoodieRecord record : records) {
      // No change to the _row_key
      assertTrue(updatedFilter.mightContain(record.getRecordKey()));
    }

    assertTrue(updatedFilter.mightContain(insertedRecord1.getRecordKey()));
    records.add(insertedRecord1);// add this so it can further check below

    ParquetReader updatedReader = ParquetReader.builder(new AvroReadSupport<>(), updatedParquetFilePath).build();
    index = 0;
    while ((newRecord = (GenericRecord) updatedReader.read()) != null) {
      assertEquals(newRecord.get("_row_key").toString(), records.get(index).getRecordKey());
      if (index == 0) {
        assertEquals("15", newRecord.get("number").toString());
      }
      index++;
    }
    updatedReader.close();
    // Also check the numRecordsWritten
    WriteStatus writeStatus = statuses.get(0);
    assertEquals("Should be only one file generated", 1, statuses.size());
    assertEquals(4, writeStatus.getStat().getNumWrites());// 3 rewritten records + 1 new record
  }

  private List<HoodieRecord> newHoodieRecords(int n, String time) throws Exception {
    List<HoodieRecord> records = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      String recordStr =
          String.format("{\"_row_key\":\"%s\",\"time\":\"%s\",\"number\":%d}", UUID.randomUUID().toString(), time, i);
      TestRawTripPayload rowChange = new TestRawTripPayload(recordStr);
      records.add(new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange));
    }
    return records;
  }

  // Check if record level metadata is aggregated properly at the end of write.
  @Test
  public void testMetadataAggregateFromWriteStatus() throws Exception {
    // Prepare the AvroParquetIO
    HoodieWriteConfig config =
        makeHoodieClientConfigBuilder().withWriteStatusClass(MetadataMergeWriteStatus.class).build();
    String firstCommitTime = HoodieTestUtils.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, conf);

    // Get some records belong to the same partition (2016/01/31)
    String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";

    List<HoodieRecord> records = new ArrayList<>();
    TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
    records.add(new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));
    TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
    records.add(new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2));
    TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
    records.add(new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3));

    // Insert new records
    List<WriteStatus> writeStatuses = new ArrayList<>();
    Iterator<List<WriteStatus>> iter =
        table.handleInsert(firstCommitTime, FSUtils.createNewFileIdPfx(), records.iterator());
    while(iter.hasNext()) {
      writeStatuses.addAll(iter.next());
    }

    Map<String, String> allWriteStatusMergedMetadataMap =
        MetadataMergeWriteStatus.mergeMetadataForWriteStatuses(writeStatuses);
    assertTrue(allWriteStatusMergedMetadataMap.containsKey("InputRecordCount_1506582000"));
    // For metadata key InputRecordCount_1506582000, value is 2 for each record. So sum of this
    // should be 2 * 3
    assertEquals("6", allWriteStatusMergedMetadataMap.get("InputRecordCount_1506582000"));
  }

  @Test
  public void testInsertRecords() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfig();
    String commitTime = HoodieTestUtils.makeNewCommitTime();
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, conf);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Case 1:
    // 10 records for partition 1, 1 record for partition 2.
    List<HoodieRecord> records = newHoodieRecords(10, "2016-01-31T03:16:41.415Z");
    records.addAll(newHoodieRecords(1, "2016-02-01T03:16:41.415Z"));

    // Insert new records
    final List<HoodieRecord> recs2 = records;
    List<WriteStatus> returnedStatuses = new ArrayList<>();
    Iterator<List<WriteStatus>> iter =
        table.handleInsert(commitTime, FSUtils.createNewFileIdPfx(), recs2.iterator());
    while(iter.hasNext()) {
      returnedStatuses.addAll(iter.next());
    }

    // TODO: check the actual files and make sure 11 records, total were written.
    assertEquals(2, returnedStatuses.size());
    assertEquals("2016/01/31", returnedStatuses.get(0).getPartitionPath());
    assertEquals(0, returnedStatuses.get(0).getFailedRecords().size());
    assertEquals(10, returnedStatuses.get(0).getTotalRecords());
    assertEquals("2016/02/01", returnedStatuses.get(1).getPartitionPath());
    assertEquals(0, returnedStatuses.get(0).getFailedRecords().size());
    assertEquals(1, returnedStatuses.get(1).getTotalRecords());

    // Case 2:
    // 1 record for partition 1, 5 record for partition 2, 1 records for partition 3.
    records = newHoodieRecords(1, "2016-01-31T03:16:41.415Z");
    records.addAll(newHoodieRecords(5, "2016-02-01T03:16:41.415Z"));
    records.addAll(newHoodieRecords(1, "2016-02-02T03:16:41.415Z"));

    // Insert new records
    final List<HoodieRecord> recs3 = records;


    returnedStatuses = new ArrayList<>();
    Iterator<List<WriteStatus>> iter1 =
        table.handleInsert(commitTime, FSUtils.createNewFileIdPfx(), recs3.iterator());
    while(iter1.hasNext()) {
      returnedStatuses.addAll(iter1.next());
    }

    assertEquals(3, returnedStatuses.size());
    assertEquals("2016/01/31", returnedStatuses.get(0).getPartitionPath());
    assertEquals(1, returnedStatuses.get(0).getTotalRecords());

    assertEquals("2016/02/01", returnedStatuses.get(1).getPartitionPath());
    assertEquals(5, returnedStatuses.get(1).getTotalRecords());

    assertEquals("2016/02/02", returnedStatuses.get(2).getPartitionPath());
    assertEquals(1, returnedStatuses.get(2).getTotalRecords());

  }

  @Test
  public void testFileSizeUpsertRecords() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder().withStorageConfig(HoodieStorageConfig.newBuilder()
        .limitFileSize(64 * 1024).parquetBlockSize(64 * 1024).parquetPageSize(64 * 1024).build()).build();
    String commitTime = HoodieTestUtils.makeNewCommitTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, conf);

    List<HoodieRecord> records = new ArrayList<>();
    // Approx 1150 records are written for block size of 64KB
    for (int i = 0; i < 2000; i++) {
      String recordStr = "{\"_row_key\":\"" + UUID.randomUUID().toString()
          + "\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":" + i + "}";
      TestRawTripPayload rowChange = new TestRawTripPayload(recordStr);
      records.add(new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange));
    }

    // Insert new records
    List<WriteStatus> ws = new ArrayList<>();
    Iterator<List<WriteStatus>> iter =
        table.handleInsert(commitTime, FSUtils.createNewFileIdPfx(), records.iterator());
    while(iter.hasNext()) {
      ws.addAll(iter.next());
    }

    // Check the updated file
    int counts = 0;
    for (File file : new File(basePath + "/2016/01/31").listFiles()) {
      if (file.getName().endsWith(".parquet") && FSUtils.getCommitTime(file.getName()).equals(commitTime)) {
        LOG.info(file.getName() + "-" + file.length());
        counts++;
      }
    }
    assertEquals("If the number of records are more than 1150, then there should be a new file", 3, counts);
  }

  @Test
  public void testInsertUpsertWithHoodieAvroPayload() throws Exception {
    HoodieWriteConfig config = makeHoodieClientConfigBuilder()
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1000 * 1024).build()).build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    final HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, conf);
    String commitTime = "000";
    // Perform inserts of 100 records to test CreateHandle and BufferedExecutor
    final List<HoodieRecord> inserts = dataGen.generateInsertsWithHoodieAvroPayload(commitTime, 100);
    List<WriteStatus> ws = new ArrayList<>();
    Iterator<List<WriteStatus>> iter =
        table.handleInsert(commitTime, UUID.randomUUID().toString(), inserts.iterator());
    while(iter.hasNext()) {
      ws.addAll(iter.next());
    }

    WriteStatus writeStatus = ws.get(0);
    String fileId = writeStatus.getFileId();
    metaClient.getFs().create(new Path(basePath + "/.hoodie/000.commit")).close();
    final HoodieCopyOnWriteTable table2 = new HoodieCopyOnWriteTable(config, conf);

    final List<HoodieRecord> updates =
        dataGen.generateUpdatesWithHoodieAvroPayload(commitTime, writeStatus.getWrittenRecords());

    table2.handleUpdate("001", fileId, updates.iterator());
  }

  @After
  public void cleanup() {
    if (basePath != null) {
      new File(basePath).delete();
    }
  }
}
