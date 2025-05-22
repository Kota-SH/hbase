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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@InterfaceAudience.Private
public class RefreshMetaCommand {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshMetaCommand.class);
  private static final int CHUNK_SIZE = 100;
  private final Admin admin;
  private final boolean force;

  public RefreshMetaCommand (Admin admin, boolean force) {
    this.admin = admin;
    this.force = force;
  }

  public void execute() throws IOException, MetaRefreshException {

//    checkForReadOnlyMode();

    try {
      List<RegionInfo> currentRegions = getCurrentRegionsUsingMTA();
      List<RegionInfo> latestRegions = scanBackingStorage();
      if (needsUpdate(currentRegions, latestRegions)) {
        updateMetaTable(currentRegions, latestRegions);
      }
    } catch (IOException | InterruptedException ioe) {
      LOG.error("Failed to refresh meta: ", ioe);
    }
  }

  boolean checkForReadOnlyMode() throws IOException, MetaRefreshException {
    if (!force && !checkIsReadOnly()) {
      throw new MetaRefreshException("Meta refresh can only be done on read only instances");
    }
    return true;
  }

  private boolean checkIsReadOnly() {
//    LOG.info("Che: {}", (admin.getConnection().getConfiguration()
//      .getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false)));
//    return (admin.getConnection().getConfiguration()
//      .getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false));
    return true;
  }

  List<RegionInfo> getCurrentRegionsUsingMTA() throws IOException {
    return MetaTableAccessor.getAllRegions(admin.getConnection(), true);
  }

  List<RegionInfo> getCurrentRegions() throws IOException {
    List<RegionInfo> regions = new ArrayList<>();

    try (Table metaTable = admin.getConnection().getTable(TableName.META_TABLE_NAME)) {
      Scan scan = new Scan();
      scan.addFamily(HConstants.CATALOG_FAMILY);

      try(ResultScanner scanner = metaTable.getScanner(scan)) {
        for (Result r : scanner) {
          RegionInfo ri = CatalogFamilyFormat.getRegionInfo(r);
          if (ri != null) {
            // Only include regions that are in valid states.
            if (isValidRegionState(ri, r)) {
              regions.add(ri);
            }
          }
        }
      }
    }
    return regions;
  }

  private boolean isValidRegionState(RegionInfo ri, Result result) {
    Cell stateCell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER);
    if (stateCell == null) {
      return false;
    }
    // Only include regions that are Open, Splitting, Merging
    String state = Bytes.toString(stateCell.getValueArray(), stateCell.getValueOffset(), stateCell.getValueLength());
    return state.equals("OPEN") || state.equals("SPLITTING") || state.equals("MERGING");
  }

  /**
   * Scans the backing storage for region directories and returns a list of RegionInfo objects.
   * This method assumes that the region directories are located under the 'data' area of HDFS.
   *
   * @return List of RegionInfo objects representing the regions found in the backing storage.
   * @throws IOException if an error occurs while accessing the file system or reading region directories.
   */
  List<RegionInfo> scanBackingStorage() throws IOException {
    List<RegionInfo> regions = new ArrayList<>();
    Configuration conf = admin.getConnection().getConfiguration();
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

  void updateMetaTable(List<RegionInfo> current, List<RegionInfo> latest)
    throws IOException, InterruptedException {
    // 1. Build all mutationes
    List<Mutation> diffs = new ArrayList<>();
    Set<RegionInfo> currentSet = new HashSet<>(current);
    Set<RegionInfo> latestSet  = new HashSet<>(latest);

    // Adds
    for (RegionInfo r : latest) {
      if (!currentSet.contains(r)) {
        diffs.add(MetaTableAccessor.makePutFromRegionInfo(r));
      }
    }
    // Removes
    for (RegionInfo r : current) {
      if (!latestSet.contains(r)) {
        diffs.add(MetaTableAccessor.makeDeleteFromRegionInfo(r, EnvironmentEdgeManager.currentTime()));
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
          diffs.add(MetaTableAccessor.makePutFromRegionInfo(r));
        }
      }
    }

    // 2. Chunk and submit. Retry for failures.
    try (Table meta = admin.getConnection().getTable(TableName.META_TABLE_NAME)) {
      for (int i = 0; i < diffs.size(); i += CHUNK_SIZE) {
        int end = Math.min(diffs.size(), i + CHUNK_SIZE);
        List<Mutation> chunk = diffs.subList(i, end);
        boolean success = false;
        for (int attempt = 1; attempt <= 3 && !success; attempt++) {
          try {
            meta.batch(chunk, new Object[chunk.size()]);
            success = true;
          } catch (IOException | InterruptedException e) {
            LOG.warn("Chunk {}–{} failed on attempt {}/3", i, end, attempt, e);
            if (attempt == 3) throw e;
          }
        }
      }
    }
  }

}
