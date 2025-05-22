package org.apache.hadoop.hbase.master.procedure;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.assertProcNotFailed;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestRefreshMetaProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(TestRefreshMetaProcedure.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private ProcedureExecutor<MasterProcedureEnv> procExecutor;

  @Before
  public void setup() throws Exception {
    LOG.info("Starting mini cluster");
    TEST_UTIL.startMiniCluster();
    procExecutor = TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    LOG.info("Started procedure executor");
    TableName tableName = TableName.valueOf("testRefreshMeta");
    TEST_UTIL.createTable(tableName, Bytes.toBytes("cf"));
    LOG.info("Created table: {}", tableName);
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("Shutting down mini cluster");
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void refreshMetaProcedureExecutesSuccessfully() throws Exception {
    LOG.info("Running refreshMetaProcedureExecutesSuccessfully test");
    // Arrange
    List<RegionInfo> currentRegions = TEST_UTIL.getAdmin().getRegions(TableName.META_TABLE_NAME);

    RefreshMetaProcedure procedure = spy(new RefreshMetaProcedure());
    LOG.info("a1");
    doReturn(currentRegions).when(procedure).getCurrentRegions(any());
    LOG.info("a2");
//    doReturn(new ArrayList<>()).when(procedure).scanBackingStorage(any());
    doReturn(currentRegions).when(procedure).scanBackingStorage(any());
    LOG.info("a3");

    // Act
    long procId = procExecutor.submitProcedure(procedure);
//    TEST_UTIL.getAdmin().refreshMeta();
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
//
//    // Assert
    LOG.info("lola: {}", procExecutor.getResult(procId).getState());
    assertProcNotFailed(procExecutor.getResult(procId));
//    assertTrue(procedure.needsUpdate(currentRegions, currentRegions));
//    verify(procedure, times(1)).needsUpdate(any(), any());
//    verify(procedure, times(0)).compareAndUpdateRegions(any(), any(), any());
  }

//  @Test
//  public void refreshMetaProcedureHandlesEmptyRegions() throws Exception {
//    // Arrange
//    RefreshMetaProcedure procedure = spy(new RefreshMetaProcedure(TEST_UTIL.getAdmin(), false));
//    doReturn(new ArrayList<>()).when(procedure).getCurrentRegions();
//    doReturn(new ArrayList<>()).when(procedure).scanBackingStorage();
//
//    // Act
//    long procId = procExecutor.submitProcedure(procedure);
//    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
//
//    // Assert
////    assertEquals(ProcedureExecutor.ProcedureState.SUCCESS, procExecutor.getResult(procId).getState());
//    verify(procedure, never()).compareAndUpdateRegions(any(), any());
//  }
//
//  @Test
//  public void refreshMetaProcedureFailsOnException() throws Exception {
//    // Arrange
//    RefreshMetaProcedure procedure = spy(new RefreshMetaProcedure(TEST_UTIL.getAdmin(), false));
//    doThrow(new RuntimeException("Simulated failure")).when(procedure).getCurrentRegions();
//
//    // Act
//    long procId = procExecutor.submitProcedure(procedure);
//    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
//
//    // Assert
//    assertEquals(ProcedureExecutor.ProcedureState.FAILED, procExecutor.getResult(procId).getState());
//  }
//
//  @Test
//  public void handlesNullRegionLists() throws Exception {
//    RefreshMetaProcedure procedure = spy(new RefreshMetaProcedure(TEST_UTIL.getAdmin(), false));
//    doReturn(null).when(procedure).getCurrentRegions();
//    doReturn(null).when(procedure).scanBackingStorage();
//
//    long procId = procExecutor.submitProcedure(procedure);
//    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
//
//    assertEquals(ProcedureExecutor.ProcedureState.SUCCESS, procExecutor.getResult(procId).getState());
//    verify(procedure, never()).compareAndUpdateRegions(any(), any());
//  }
//
//  @Test
//  public void handlesDuplicateRegions() throws Exception {
//    List<RegionInfo> duplicateRegions = createTestRegions(2);
//    duplicateRegions.addAll(createTestRegions(2)); // Add duplicates
//
//    RefreshMetaProcedure procedure = spy(new RefreshMetaProcedure(TEST_UTIL.getAdmin(), false));
//    doReturn(duplicateRegions).when(procedure).getCurrentRegions();
//    doReturn(duplicateRegions).when(procedure).scanBackingStorage();
//
//    long procId = procExecutor.submitProcedure(procedure);
//    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
//
//    assertEquals(ProcedureExecutor.ProcedureState.SUCCESS, procExecutor.getResult(procId).getState());
//    verify(procedure, times(1)).compareAndUpdateRegions(any(), any());
//  }
//
//  @Test
//  public void detectsBoundaryChanges() throws Exception {
//    List<RegionInfo> currentRegions = createTestRegions(1);
//    List<RegionInfo> latestRegions = new ArrayList<>(currentRegions);
//    latestRegions.get(0).setEndKey(Bytes.toBytes("newEndKey")); // Modify boundary
//
//    RefreshMetaProcedure procedure = spy(new RefreshMetaProcedure(TEST_UTIL.getAdmin(), false));
//    doReturn(currentRegions).when(procedure).getCurrentRegions();
//    doReturn(latestRegions).when(procedure).scanBackingStorage();
//
//    long procId = procExecutor.submitProcedure(procedure);
//    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
//
//    assertEquals(ProcedureExecutor.ProcedureState.SUCCESS, procExecutor.getResult(procId).getState());
//    verify(procedure, times(1)).compareAndUpdateRegions(currentRegions, latestRegions);
//  }
//
//  @Test
//  public void retriesOnPartialUpdateFailure() throws Exception {
//    List<RegionInfo> currentRegions = createTestRegions(1);
//    List<RegionInfo> latestRegions = createTestRegions(2);
//
//    RefreshMetaProcedure procedure = spy(new RefreshMetaProcedure(TEST_UTIL.getAdmin(), false));
//    doReturn(currentRegions).when(procedure).getCurrentRegions();
//    doReturn(latestRegions).when(procedure).scanBackingStorage();
//    doThrow(new IOException("Simulated failure"))
//      .doNothing()
//      .when(procedure).compareAndUpdateRegions(currentRegions, latestRegions);
//
//    long procId = procExecutor.submitProcedure(procedure);
//    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
//
//    assertEquals(ProcedureExecutor.ProcedureState.SUCCESS, procExecutor.getResult(procId).getState());
//    verify(procedure, times(2)).compareAndUpdateRegions(currentRegions, latestRegions);
//  }
//
//  @Test
//  public void handlesLargeRegionLists() throws Exception {
//    List<RegionInfo> largeRegionList = createTestRegions(1000);
//
//    RefreshMetaProcedure procedure = spy(new RefreshMetaProcedure(TEST_UTIL.getAdmin(), false));
//    doReturn(largeRegionList).when(procedure).getCurrentRegions();
//    doReturn(largeRegionList).when(procedure).scanBackingStorage();
//
//    long procId = procExecutor.submitProcedure(procedure);
//    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
//
//    assertEquals(ProcedureExecutor.ProcedureState.SUCCESS, procExecutor.getResult(procId).getState());
//    verify(procedure, times(1)).compareAndUpdateRegions(any(), any());
//  }

  private List<RegionInfo> createTestRegions(int count) {
    List<RegionInfo> regions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      byte[] start = Bytes.toBytes("start" + i);
      byte[] end   = Bytes.toBytes("start" + (i+1));
      regions.add(RegionInfoBuilder
        .newBuilder(TableName.valueOf("test"))
        .setStartKey(start)
        .setEndKey(  end)
        .build());
    }
    return regions;
  }
}
