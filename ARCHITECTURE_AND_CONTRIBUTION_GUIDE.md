# Apache HBase Architecture & Contribution Guide

This guide provides a comprehensive overview of the Apache HBase architecture, codebase structure, and opportunities for contribution.

## 1. High-Level Architecture

Apache HBase is a distributed, scalable, big data store modeled after Google's Bigtable. It runs on top of Apache Hadoop HDFS and provides random read/write access.

### Core Components

*   **Client**: The entry point for interacting with HBase. It uses the `Connection` interface to communicate with the Master and RegionServers. It caches region locations to minimize Master load.
*   **HMaster (Master)**: The coordinator of the cluster.
    *   **AssignmentManager**: Manages the assignment of regions to RegionServers.
    *   **LoadBalancer**: Balances the cluster load by moving regions.
    *   **CatalogJanitor**: Cleans up the `hbase:meta` table.
    *   **Procedures**: Executes distributed tasks (e.g., table creation, region splitting).
*   **HRegionServer**: The worker node that handles data read/write requests.
    *   **HRegion**: Manages a set of rows (a range of keys).
    *   **HStore**: Corresponds to a Column Family in a region.
    *   **MemStore**: In-memory write buffer.
    *   **HFile**: The on-disk storage format (on HDFS).
    *   **WAL (Write Ahead Log)**: Ensures durability of writes before they are persisted to HFiles.
*   **ZooKeeper**: Maintains cluster state (Master election, RegionServer registry, schema location).

## 2. Data Model & Storage

*   **Cell**: The fundamental unit of data. Contains:
    *   Row Key
    *   Column Family
    *   Column Qualifier
    *   Timestamp
    *   Type (Put, Delete, etc.)
    *   Value
*   **HFile**: The underlying storage file format. It is a sorted key/value map.
*   **WAL**: A log file where mutations are written sequentially before being applied to the MemStore.

## 3. Codebase Structure

The project is organized as a multi-module Maven project. Key modules include:

*   **hbase-common**: Core utilities and basic types (e.g., `Cell`, `KeyValue`, `HBaseConfiguration`).
*   **hbase-client**: Client API (e.g., `Connection`, `Table`, `Admin`, `Put`, `Get`, `Scan`).
*   **hbase-server**: Server-side implementation (`HMaster`, `HRegionServer`, `WAL`, `HStore`).
*   **hbase-protocol-shaded**: Protobuf definitions for RPC communication (`HBase.proto`, `Client.proto`).
*   **hbase-zookeeper**: ZooKeeper coordination logic (`ZKWatcher`).
*   **hbase-hadoop-compat**: Interfaces for Hadoop compatibility.

## 4. Build and Test

### Building the Project

HBase uses Apache Maven. To build the project skipping tests (for speed):

```bash
mvn clean package -DskipTests
```

### Running Tests

To run specific tests:

```bash
mvn test -pl hbase-server -Dtest=TestRegionPlacement
```

**Note**: HBase tests can be resource-intensive. Ensure you have sufficient memory configured for Maven.

## 5. Contribution Opportunities

Here are some areas identified in the codebase that may need attention. These are good starting points for contribution.

### Potential Tasks (TODOs)

*   **AssignmentManager.java**: There are TODOs related to "handle multiple meta" regions. This suggests work needed to support scaling `hbase:meta` across multiple regions.
*   **Get.java**: A comment indicates a logic issue: *"Can't have two gets the same just because on same row."*
*   **RegionInfoBuilder.java**: *"How come Meta regions still do not have encoded region names? Fix."*
*   **ConnectionUtils.java**: *"Fix this. Not all connections from server side should have 10 times the retries."*

### Ignored/Disabled Tests

The following tests are marked with `@Ignore`, indicating they are broken or flaky and need fixing:

*   `hbase-server/src/test/java/org/apache/hadoop/hbase/master/TestRegionPlacement.java`: "Test for unfinished feature"
*   `hbase-server/src/test/java/org/apache/hadoop/hbase/master/TestMasterBalanceThrottling.java`: "SimpleLoadBalancer seems borked"
*   `hbase-server/src/test/java/org/apache/hadoop/hbase/util/TestHBaseFsckMOB.java`
*   `hbase-server/src/test/java/org/apache/hadoop/hbase/client/TestFromClientSide5.java`: "Flakey: HBASE-8989"

### How to Contribute

1.  **Pick an Issue**: Select a TODO or a broken test.
2.  **Verify**: Run the test locally to confirm it fails (for ignored tests, remove the `@Ignore` annotation).
3.  **Fix**: Implement the fix or feature.
4.  **Test**: Run relevant tests to ensure no regressions.
5.  **Submit**: Create a Pull Request with a clear description.

## 6. Resources

*   **JIRA**: https://issues.apache.org/jira/browse/HBASE
*   **Mailing Lists**: dev@hbase.apache.org
*   **Design Docs**: Located in `dev-support/design-docs/` in this repo.
