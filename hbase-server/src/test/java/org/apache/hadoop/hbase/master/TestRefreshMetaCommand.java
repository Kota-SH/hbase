/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import static org.mockito.Mockito.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestRefreshMetaCommand {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRefreshMetaCommand.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRefreshMetaCommand.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private RefreshMetaCommand command;
  private FileSystem fs;
  RefreshMetaCommand cmdSpy;

  @Before
  public void setup() throws Exception {
    TEST_UTIL.startMiniCluster();
    command = new RefreshMetaCommand(TEST_UTIL.getAdmin(), false);
    fs = TEST_UTIL.getTestFileSystem();
    cmdSpy = spy(command);
  }

  @After
  public void tearDown() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.cleanupTestDir();
  }

  @Test public void test0() {
    LOG.info("helloo");
  }

  @Test
  public void testNoChangesNeededSpy() throws IOException, MetaRefreshException, InterruptedException {
    // Create a spy of the command to intercept method calls

    // Setup test regions
    List<RegionInfo> currentRegions = createTestRegions(3);

    // Mock methods to return our test data
    doReturn(true).when(cmdSpy).checkForReadOnlyMode();
    doReturn(currentRegions).when(cmdSpy).getCurrentRegionsUsingMTA();
    doReturn(currentRegions).when(cmdSpy).scanBackingStorage();

    // Execute command
    cmdSpy.execute();

    // Verify needsUpdate was called
    verify(cmdSpy).needsUpdate(currentRegions, currentRegions);

    // Verify updateMetaTable was never called since no update was needed
    verify(cmdSpy, never()).updateMetaTable(any(), any());
  }

  @Test
  public void testNewRegions() throws IOException, MetaRefreshException, InterruptedException {
    // Arrange: Setup mock regions
    List<RegionInfo> existingRegions = createTestRegions(2);
    List<RegionInfo> updatedRegions = new ArrayList<>(existingRegions);
    updatedRegions.add(createRegionInfo("test", "new_region"));

    // Mock current regions from meta table
    doReturn(true).when(cmdSpy).checkForReadOnlyMode();
    doReturn(existingRegions).when(cmdSpy).getCurrentRegionsUsingMTA();
    doReturn(updatedRegions).when(cmdSpy).scanBackingStorage();

    // Act: Execute the command
    cmdSpy.execute();

    // Assert: Verify new region was added
    verify(cmdSpy, times(1))
      .updateMetaTable(existingRegions, updatedRegions);
  }

//
//  @Test
//  public void testRemovedRegions() throws IOException, MetaRefreshException {
//    // Setup mock regions
//    List<RegionInfo> currentRegions = createTestRegions(3);
//    List<RegionInfo> latestRegions = new ArrayList<>(currentRegions.subList(0, 2));
//
//
//    // Mock current regions from meta table
//    when(MetaTableAccessor.getAllRegions(connection, false)).thenReturn(currentRegions);
//
//    // Mock backing storage scan
//    mockBackingStorage(latestRegions);
//
//    // Execute command
//    command.execute();
//
//    // Verify region was removed
//    verify(metaTable, times(1)).delete(any(Delete.class));
//
//
//  }
//
//  @Test
//  public void testRegionBoundaryChanges() throws IOException, MetaRefreshException {
//    // Setup mock regions
//    List<RegionInfo> currentRegions = createTestRegions(2);
//    List<RegionInfo> latestRegions = new ArrayList<>();
//    for (RegionInfo region : currentRegions) {
//      // Create new region with same name but different boundaries
//      latestRegions.add(createRegionInfo(region.getTable().getNameAsString(),
//        region.getRegionNameAsString(),
//        Bytes.toBytes("new_start"),
//        Bytes.toBytes("new_end")));
//    }
//
//
//    // Mock current regions from meta table
//    when(MetaTableAccessor.getAllRegions(connection, false)).thenReturn(currentRegions);
//
//    // Mock backing storage scan
//    mockBackingStorage(latestRegions);
//
//    // Execute command
//    command.execute();
//
//    // Verify regions were updated
//    verify(metaTable, times(2)).put(any(Put.class));
//
//
//  }
//
//  @Test
//  public void testSystemDirectories() throws IOException, MetaRefreshException {
//    // Setup mock regions including system directories
//    List<RegionInfo> currentRegions = createTestRegions(2);
//    List<RegionInfo> latestRegions = new ArrayList<>(currentRegions);
//
//
//    // Mock current regions from meta table
//    when(MetaTableAccessor.getAllRegions(connection, false)).thenReturn(currentRegions);
//
//    // Mock backing storage with system directories
//    FileStatus[] tableDirs = new FileStatus[] {
//      createFileStatus("test1", true),
//      createFileStatus(".tmp", true),
//      createFileStatus(".archive", true)
//    };
//    when(fs.listStatus(any(Path.class))).thenReturn(tableDirs);
//
//    // Execute command
//    command.execute();
//
//    // Verify system directories were skipped
//    verify(metaTable, never()).put(any(Put.class));
//    verify(metaTable, never()).delete(any(Delete.class));
//
//
//  }
//
//  @Test
//  public void testErrorHandling() throws IOException {
//    // Setup mock regions
//    List<RegionInfo> currentRegions = createTestRegions(2);
//    List<RegionInfo> latestRegions = new ArrayList<>(currentRegions);
//
//
//    // Mock current regions from meta table
//    when(MetaTableAccessor.getAllRegions(connection, false)).thenReturn(currentRegions);
//
//    // Mock backing storage scan with error
//    when(fs.listStatus(any(Path.class))).thenThrow(new IOException("Test error"));
//
//    // Execute command and expect exception
//    try {
//      command.execute();
//      fail("Should have thrown IOException");
//    } catch (IOException | MetaRefreshException e) {
//      assertEquals("Test error", e.getMessage());
//    }
//
//
//  }
//
//  @Test
//  public void testColumnFamilyChanges() throws IOException, MetaRefreshException {
//    // Setup mock regions
//    List<RegionInfo> currentRegions = createTestRegions(2);
//    List<RegionInfo> latestRegions = new ArrayList<>(currentRegions);
//
//
//    // Mock current regions from meta table
//    when(MetaTableAccessor.getAllRegions(connection, false)).thenReturn(currentRegions);
//
//    // Mock backing storage with different family directories
//    mockBackingStorageWithFamilyChanges(latestRegions);
//
//    // Execute command
//    command.execute();
//
//    // Verify regions were updated due to family changes
//    verify(metaTable, times(2)).put(any(Put.class));
//
//
//  }
//
//  @Test
//  public void testRefreshMetaWithMismatchedRegions() throws Exception, MetaRefreshException {
//    TableName tableName = TableName.valueOf("testRefreshMetaWithMismatchedRegions");
//
//
//    // Create a table with some regions
//    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
//      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("f")))
//      .build();
//    admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 5);
//
//    // Get current regions from meta
//    List<RegionInfo> currentRegions = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tableName);
//    assertTrue("Table should have regions", currentRegions.size() > 0);
//
//    // Create a directory structure that differs from meta
//    FileSystem fs = TEST_UTIL.getTestFileSystem();
//    Path tableDir = new Path(TEST_UTIL.getDataTestDirOnTestFS(), tableName.getNameAsString());
//    fs.mkdirs(tableDir);
//    Path regionDir = new Path(tableDir, "fakeRegion");
//    fs.mkdirs(new Path(regionDir, "f"));
//
//    // Run refresh command
//    RefreshMetaCommand command = new RefreshMetaCommand(admin, true);
//    command.execute();
//
//    // Verify meta was not corrupted
//    List<RegionInfo> afterRegions = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tableName);
//    assertEquals("Number of regions should not change", currentRegions.size(), afterRegions.size());
//
//    // Cleanup
//    admin.disableTable(tableName);
//    admin.deleteTable(tableName);
//
//
//  }
//
//  @Test
//  public void testRefreshMetaWithReadOnlyCheck() throws Exception {
//    TableName tableName = TableName.valueOf("testRefreshMetaWithReadOnlyCheck");
//
//    // Create a table
//    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
//      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("f")))
//      .build();
//    admin.createTable(desc);
//    assertFalse(admin.isReadOnly());
//
//    // Try refresh without force flag
//    RefreshMetaCommand command = new RefreshMetaCommand(admin, false);
//
//    assertThrows(MetaRefreshException.class, command::execute);
//    admin.disableTable(tableName);
//  }

  private List<RegionInfo> createTestRegions(int count) {
    List<RegionInfo> regions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      regions.add(createRegionInfo("test", "region" + i));
    }
    LOG.info("hehe: {}", regions);
    return regions;
  }

  private RegionInfo createRegionInfo(String tableName, String regionName) {
    return createRegionInfo(tableName, regionName, Bytes.toBytes("start"), Bytes.toBytes("end"));
  }

  private RegionInfo createRegionInfo(String tableName, String regionName, byte[] startKey, byte[] endKey) {
    return RegionInfoBuilder.newBuilder(TableName.valueOf(tableName))
      .setStartKey(startKey)
      .setEndKey(endKey)
      .build();
  }

  private FileStatus createFileStatus(String name, boolean isDir) {
    return new FileStatus(0, isDir, 0, 0, 0, new Path(name));
  }

  private void mockBackingStorage(List<RegionInfo> regions) throws IOException {
    // Mock table directories
    FileStatus[] tableDirs = new FileStatus[regions.size()];
    for (int i = 0; i < regions.size(); i++) {
      tableDirs[i] = createFileStatus("table" + i, true);
    }
    when(fs.listStatus(any(Path.class))).thenReturn(tableDirs);


    // Mock region directories
    for (RegionInfo region : regions) {
      FileStatus[] regionDirs = new FileStatus[] {
        createFileStatus(region.getRegionNameAsString(), true)
      };
      when(fs.listStatus(eq(new Path("table" + regions.indexOf(region))))).thenReturn(regionDirs);

      // Mock family directories
      FileStatus[] familyDirs = new FileStatus[] {
        createFileStatus("family1", true),
        createFileStatus("family2", true)
      };
      when(fs.listStatus(eq(new Path(region.getRegionNameAsString())))).thenReturn(familyDirs);
    }


  }

  private void mockBackingStorageWithFamilyChanges(List<RegionInfo> regions) throws IOException {
    // Mock table directories
    FileStatus[] tableDirs = new FileStatus[regions.size()];
    for (int i = 0; i < regions.size(); i++) {
      tableDirs[i] = createFileStatus("table" + i, true);
    }
    when(fs.listStatus(any(Path.class))).thenReturn(tableDirs);


    // Mock region directories with changed family structure
    for (RegionInfo region : regions) {
      FileStatus[] regionDirs = new FileStatus[] {
        createFileStatus(region.getRegionNameAsString(), true)
      };
      when(fs.listStatus(eq(new Path("table" + regions.indexOf(region))))).thenReturn(regionDirs);

      // Mock family directories with changes
      FileStatus[] familyDirs = new FileStatus[] {
        createFileStatus("family1", true),
        createFileStatus("family3", true),  // Changed from family2 to family3
        createFileStatus("family4", true)   // New family
      };
      when(fs.listStatus(eq(new Path(region.getRegionNameAsString())))).thenReturn(familyDirs);
    }
  }
}
