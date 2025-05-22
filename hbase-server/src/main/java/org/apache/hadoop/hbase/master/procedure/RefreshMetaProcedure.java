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

package org.apache.hadoop.hbase.master.procedure;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshMetaState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@InterfaceAudience.Private
public class RefreshMetaProcedure extends AbstractStateMachineTableProcedure<RefreshMetaState> {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshMetaProcedure.class);

  private List<RegionInfo> currentRegions;
  private List<RegionInfo> latestRegions;
  private static final int CHUNK_SIZE = 100;

  public RefreshMetaProcedure() {
    super();
    LOG.info("Instantiate RefreshMetaProcedure");
  }

  public RefreshMetaProcedure(MasterProcedureEnv env) {
    super(env);
  }

  /**
   * @return
   */
  @Override public TableName getTableName() {
    return TableName.META_TABLE_NAME;
  }

  /**
   * @return
   */
  @Override public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  /**
   * @param env env
   * @param refreshMetaState state to execute
   */
  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RefreshMetaState refreshMetaState) {
    LOG.info("gggg: {}, and {}", refreshMetaState, refreshMetaState.getNumber());
    switch (refreshMetaState) {
      case REFRESH_META_INIT:
        CompletableFuture.runAsync(() -> {
          LOG.info("Get current Regions from the hbase:meta table");
          try {
            currentRegions = getCurrentRegions(env.getMasterServices().getConnection());
            LOG.info("Current regions: {}", currentRegions);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).thenRun(() -> {
          setNextState(RefreshMetaState.REFRESH_META_SCAN_STORAGE);
          env.getProcedureScheduler().addBack(this);
        }).exceptionally(e -> {
          LOG.error("Error during REFRESH_META_INIT ", e);
          setFailure("REFRESH_META_INIT ", e);
          return null;
        });
        return Flow.NO_MORE_STATE;

      case REFRESH_META_SCAN_STORAGE:
        CompletableFuture.runAsync(() -> {
          LOG.info("Scanning Backing Storage for region directories");
          try {
            latestRegions = scanBackingStorage(env.getMasterServices().getConnection());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).thenRun(() -> {
          setNextState(RefreshMetaState.REFRESH_META_UPDATE);
          env.getProcedureScheduler().addBack(this);
        }).exceptionally(e -> {
          LOG.error("Error during REFRESH_META_SCAN_STORAGE ", e);
          setFailure("REFRESH_META_SCAN_STORAGE ", e);
          return null;
        });
        return Flow.NO_MORE_STATE;

      case REFRESH_META_UPDATE:
        CompletableFuture.runAsync(() -> {
          LOG.info("Comparing the current regions with the backing storage and updating if necessary");
          try {
            if (needsUpdate(currentRegions, latestRegions)) {
              LOG.info("jjjjj Current regions: {}, latest regions: {}", currentRegions, latestRegions);
              compareAndUpdateRegions(currentRegions, latestRegions, env.getMasterServices().getConnection());
            } else {
              LOG.info("No update needed");
            }
          } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
          }
        }).thenRun(() -> {
          setNextState(RefreshMetaState.REFRESH_META_COMPLETE);
          env.getProcedureScheduler().addBack(this);
        }).exceptionally(e -> {
          LOG.error("Error during REFRESH_META_UPDATE ", e);
          setFailure("REFRESH_META_UPDATE ", e);
          return null;
        });
        return Flow.NO_MORE_STATE;

      case REFRESH_META_COMPLETE:
        LOG.info("Refresh meta procedure completed.");
        return Flow.NO_MORE_STATE;

      default:
        throw new UnsupportedOperationException("Unhandled state: " + refreshMetaState);
    }
  }

  /**
   * Compares the current regions with the latest regions and updates the meta table if necessary.
   *
   * @param current The current list of RegionInfo objects.
   * @param latest  The latest list of RegionInfo objects.
   * @param connection The connection to the HBase cluster.
   * @throws IOException          if an error occurs while accessing the meta table.
   * @throws InterruptedException if the operation is interrupted.
   */
  void compareAndUpdateRegions(List<RegionInfo> current, List<RegionInfo> latest,
    Connection connection)
    throws IOException, InterruptedException {
      // 1. Build all mutationes
      List<Put> puts = new ArrayList<>();
      List<Delete> deletes = new ArrayList<>();
      Set<RegionInfo> currentSet = new HashSet<>(current);
      Set<RegionInfo> latestSet  = new HashSet<>(latest);

      // Adds
      for (RegionInfo ri : latest) {
        if (!currentSet.contains(ri)) {
          puts.add(MetaTableAccessor.makePutFromRegionInfo(ri));
        }
      }

      // Removes
      for (RegionInfo r : current) {
        if (!latestSet.contains(r)) {
          deletes.add(MetaTableAccessor.makeDeleteFromRegionInfo(r, EnvironmentEdgeManager.currentTime()));
        }
      }
      // Updates
      for (RegionInfo r : latest) {
        if (currentSet.contains(r)) {
          RegionInfo old = current.stream()
            .filter(c -> c.getRegionNameAsString().equals(r.getRegionNameAsString()))
            .findFirst().get();
          if (!Arrays.equals(old.getStartKey(), r.getStartKey()) ||
            !Arrays.equals(old.getEndKey(),   r.getEndKey())) {
            puts.add(MetaTableAccessor.makePutFromRegionInfo(r));
          }
        }
      }

      LOG.info("Updating with puts: {}", puts);
      MetaTableAccessor.putsToMetaTable(connection, puts);
      LOG.info("Updating with deletes: {}", deletes);
      MetaTableAccessor.deleteFromMetaTable(connection, deletes);

//        // 2. Chunk and submit. Retry for failures using MetaTableAccessor.
//        for (int i = 0; i < diffs.size(); i += CHUNK_SIZE) {
//          int end = Math.min(diffs.size(), i + CHUNK_SIZE);
//          List<Mutation> chunk = diffs.subList(i, end);
//          boolean success = false;
//          for (int attempt = 1; attempt <= 3 && !success; attempt++) {
//            try {
//              MetaTableAccessor.mutateMetaTable(admin.getConnection(), chunk);
//              success = true;
//            } catch (IOException e) {
//              LOG.warn("Chunk {}–{} failed on attempt {}/3", i, end, attempt, e);
//              if (attempt == 3) throw e;
//            }
//          }
//        }
  }

  /**
   * Compares the current regions with the latest regions and determines if an update is needed.
   *
   * @param current The current list of RegionInfo objects.
   * @param latest  The latest list of RegionInfo objects.
   * @return true if an update is needed, false otherwise.
   */
  boolean needsUpdate(List<RegionInfo> current, List<RegionInfo> latest) {
    Set<RegionInfo> currentSet = new HashSet<>(current);
    Set<RegionInfo> latestSet  = new HashSet<>(latest);
    LOG.info("hhhh2: {}, and {}", latest, latestSet);
    LOG.info("hhhh1: {}, and {}", current, currentSet);
    LOG.info("hhhh3: {}, and {}", current.equals(currentSet.stream().toList()),
      latest.equals(latestSet.stream().toList()));
    LOG.info("Current regions: {}, latest regions: {}", currentSet, latestSet);
    if (currentSet.size() != latestSet.size()) {
      LOG.info("Region count mismatch: current={}, latest={}", currentSet.size(), latestSet.size());
      return true;
    }
    if (!currentSet.equals(latestSet)) {
      LOG.info("Region set mismatch detected");
      return true;
    }
    for (RegionInfo region : current) {
      RegionInfo latestRegion = latest.stream()
        .filter(r -> r.getRegionNameAsString().equals(region.getRegionNameAsString()))
        .findFirst().orElse(null);
      if (latestRegion == null) {
        LOG.info("Region {} not found in latest set", region.getRegionNameAsString());
        return true;
      }

      if (hasBoundaryChanged(region, latestRegion)) {
        LOG.info("Region boundaries changed for {}", region.getRegionNameAsString());
        return true;
      }
    }
    return false;
  }

  private boolean hasBoundaryChanged(RegionInfo region, RegionInfo other) {
    return (!Arrays.equals(region.getStartKey(), other.getStartKey()) ||
      !Arrays.equals(region.getEndKey(), other.getEndKey()));
  }

  private boolean hasBoundaryChanged(RegionInfo region, Set<RegionInfo> currentSet) {
    return currentSet.stream()
      .filter(r -> r.getRegionNameAsString().equals(region.getRegionNameAsString()))
      .findFirst()
      .map(r -> hasBoundaryChanged(region, r))
      .orElse(false);
  }

  /**
   * Scans the backing storage for region directories and returns a list of RegionInfo objects.
   * This method assumes that the region directories are located under the 'data' area of HDFS.
   *
   * @return List of RegionInfo objects representing the regions found in the backing storage.
   * @throws IOException if an error occurs while accessing the file system or reading region directories.
   */
  List<RegionInfo> scanBackingStorage(Connection connection) throws IOException {
    List<RegionInfo> regions = new ArrayList<>();
    Configuration conf = connection.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path rootDir = CommonFSUtils.getRootDir(conf);

    // only look under the 'data' area, not WALs, archive, etc.
    Path dataDir = new Path(rootDir, HConstants.BASE_NAMESPACE_DIR);
    LOG.info("Scanning directory structure under dataDir: {}", dataDir);
    logDirectoryStructure(fs, dataDir, 0);
    if (!fs.exists(dataDir)) return regions;

    // first level: namespaces (e.g. "default", "system", ...)
    for (FileStatus nsDir : fs.listStatus(dataDir)) {
      if (!nsDir.isDirectory()) continue;
      String ns = nsDir.getPath().getName();
      if (ns.startsWith(".") || ns.startsWith("-")) continue;

      // second level: table directories
      for (FileStatus tblDir : fs.listStatus(nsDir.getPath())) {
        if (!tblDir.isDirectory()) continue;
        String tableName = tblDir.getPath().getName();
        if (tableName.startsWith(".") || tableName.startsWith("-")) continue;

        // third level: region directories
        for (FileStatus regionDir : fs.listStatus(tblDir.getPath())) {
          if (!regionDir.isDirectory()) continue;
          String regionName = regionDir.getPath().getName();
          if (regionName.startsWith(".")) continue;
          LOG.info("Found region dir: {}", regionName);

          try {
            RegionInfo ri = CatalogFamilyFormat
              .parseRegionInfoFromRegionName(Bytes.toBytes(regionName));
            if (ri != null) {
              regions.add(ri);
            }
          } catch (Exception e) {
            LOG.warn("Failed to parse region name: {}", regionName, e);
          }
        }
      }
    }
    return regions;
  }
  /**
   * Recursively logs the directory structure under the given path.
   */
  private void logDirectoryStructure(FileSystem fs, Path dir, int depth) throws IOException {
    String indent = "  ".repeat(depth);
    FileStatus[] statuses = fs.listStatus(dir);
    for (FileStatus status : statuses) {
      LOG.info("{}- {}", indent, status.getPath());
      if (status.isDirectory()) {
        logDirectoryStructure(fs, status.getPath(), depth + 1);
      }
    }
  }

  List<RegionInfo> getCurrentRegions(Connection connection) throws IOException {
    LOG.info("bbbb");
    return MetaTableAccessor.getAllRegions(connection, true);
  }

  /**
   * @param env
   * @param refreshMetaState state to rollback
   * @throws IOException
   * @throws InterruptedException
   */
  @Override protected void rollbackState(MasterProcedureEnv env, RefreshMetaState refreshMetaState)
    throws IOException, InterruptedException {
    LOG.info("Not supported rollback for state: " + refreshMetaState);
  }

  @Override
  protected RefreshMetaState getState(int stateId) {
    return RefreshMetaState.values()[stateId];
  }

  /**
   * @param refreshMetaState the state enum object
   * @return
   */
  @Override protected int getStateId(RefreshMetaState refreshMetaState) {
    return refreshMetaState.getNumber();
  }

  @Override
  protected RefreshMetaState getInitialState() {
    return RefreshMetaState.REFRESH_META_INIT;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    // Serialize procedure state
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    // Deserialize procedure state
  }
}
