/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hub;

import org.apache.hub.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hub.common.HoodieTestDataGenerator;
import org.apache.hudi.common.minicluster.HdfsTestService;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FSUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The test harness for resource initialization and cleanup.
 */
public abstract class HoodieClientTestHarness extends HoodieCommonTestHarness implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieClientTestHarness.class);

  public Configuration conf = new Configuration();
  protected transient FileSystem fs;
  protected transient HoodieTestDataGenerator dataGen = null;
  protected transient ExecutorService executorService;
  protected transient HoodieTableMetaClient metaClient;
  private static AtomicInteger instantGen = new AtomicInteger(1);

  public String getNextInstant() {
    return String.format("%09d", instantGen.getAndIncrement());
  }

  // dfs
  protected String dfsBasePath;
  protected transient HdfsTestService hdfsTestService;
  protected transient MiniDFSCluster dfsCluster;
  protected transient DistributedFileSystem dfs;

  /**
   * Initializes resource group for the subclasses of {@link TestHoodieClientBase}.
   *
   * @throws IOException
   */
  public void initResources() throws IOException {
    initPath();
    //initSparkContexts();
    initTestDataGenerator();
    initFileSystem();
    initMetaClient();
  }

  /**
   * Cleanups resource group for the subclasses of {@link TestHoodieClientBase}.
   *
   * @throws IOException
   */
  public void cleanupResources() throws IOException {
    cleanupMetaClient();
    //cleanupSparkContexts();
    cleanupTestDataGenerator();
    cleanupFileSystem();
  }

  /**
   * Initializes a file system with the hadoop configuration of Spark context.
   */
  protected void initFileSystem() {
    initFileSystemWithConfiguration(conf);
  }

  /**
   * Initializes file system with a default empty configuration.
   */
  protected void initFileSystemWithDefaultConfiguration() {
    initFileSystemWithConfiguration(new Configuration());
  }

  /**
   * Cleanups file system.
   *
   * @throws IOException
   */
  protected void cleanupFileSystem() throws IOException {
    if (fs != null) {
      LOG.warn("Closing file-system instance used in previous test-run");
      fs.close();
    }
  }

  /**
   * Initializes an instance of {@link HoodieTableMetaClient} with a special table type specified by
   * {@code getTableType()}.
   *
   * @throws IOException
   */
  protected void initMetaClient() throws IOException {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    metaClient = HoodieTestUtils.init(conf, basePath, getTableType());
  }

  /**
   * Cleanups table type.
   */
  protected void cleanupMetaClient() {
    metaClient = null;
  }

  /**
   * Initializes a test data generator which used to generate test datas.
   *
   */
  protected void initTestDataGenerator() {
    dataGen = new HoodieTestDataGenerator();
  }

  /**
   * Cleanups test data generator.
   *
   */
  protected void cleanupTestDataGenerator() {
    dataGen = null;
  }

  /**
   * Initializes a distributed file system and base directory.
   *
   * @throws IOException
   */
  protected void initDFS() throws IOException {
    FileSystem.closeAll();
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);

    // Create a temp folder as the base path
    dfs = dfsCluster.getFileSystem();
    dfsBasePath = dfs.getWorkingDirectory().toString();
    dfs.mkdirs(new Path(dfsBasePath));
  }

  /**
   * Cleanups the distributed file system.
   *
   * @throws IOException
   */
  protected void cleanupDFS() throws IOException {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
      dfsCluster.shutdown();
    }
    // Need to closeAll to clear FileSystem.Cache, required because DFS and LocalFS used in the
    // same JVM
    FileSystem.closeAll();
  }

  /**
   * Initializes executor service with a fixed thread pool.
   *
   * @param threadNum specify the capacity of the fixed thread pool
   */
  protected void initExecutorServiceWithFixedThreadPool(int threadNum) {
    executorService = Executors.newFixedThreadPool(threadNum);
  }

  /**
   * Cleanups the executor service.
   */
  protected void cleanupExecutorService() {
    if (this.executorService != null) {
      this.executorService.shutdownNow();
      this.executorService = null;
    }
  }

  private void initFileSystemWithConfiguration(Configuration configuration) {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    fs = FSUtils.getFs(basePath, configuration);
    if (fs instanceof LocalFileSystem) {
      LocalFileSystem lfs = (LocalFileSystem) fs;
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

}
