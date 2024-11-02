# CS 739: Distributed Key-Value Store with Raft

## Overview
This project implements a distributed key-value store using Raft consensus protocol for replication and strong consistency. It uses Rocksdb as backend. It supports dynamic membership changes.

## Key Features
- Strong consistency through Raft consensus
- Dynamic membership changes (add/remove nodes)
- Clean and unclean failure handling
- Multi-client support with load balancing
- Comprehensive test coverage for consistency and availability

## System Requirements

### Required Versions
- OS: Ubuntu 20.04 LTS or higher
- Rust: 1.70.0 or higher
- RocksDB: 6.20.3
- CMake: 3.16 or higher
- GCC/G++: 9.0 or higher

### Install Dependencies
```bash
git clone https://github.com/hippohwj/RaftKV.git
cd flexkv-rocks/flexkv-rocksdb

# Build Rust components
cargo build --release

# Build C++ test suite
cd integration_test
mkdir build && cd build
cmake ..
make
```
### Launch Cluster
Before running the test, the raft cluster should be launched in advance.

```bash
# Using script
./flexkv-rocks/flexkv-rocksdb/launch-cluster.sh [configure_file]

```
### Terminate Cluster
```bash
./flexkv-rocks/flexkv-rocksdb/terminate_cluster.sh [configure_file]

```
### Configure File Format
The following example demonstrates the format of configure_file, each row stands for a server node, where the first col is node id, the second is host + http port,  the last col is host + rpc port. 
The second col and the third col should have the same host.
```
1,127.0.0.1:21001,127.0.0.1:22001
2,127.0.0.1:21002,127.0.0.1:22002
3,127.0.0.1:21003,127.0.0.1:22003
```


## Tests

### Consistency Tests (integration_test/consistency/)
- single_threaded.cpp: Validates Read-After-Write consistency by performing writes followed by immediate reads, verifying data visibility and consistency across nodes.
- multi_threaded.cpp: The multi-thread version of the first consistency test.
- concurrent_test.cpp: Multi-client concurrent operations with mixed reads/writes to validate consistency under contention.

### Availability Tests (integration_test/)

- /failure/failure.cpp: Tests clean/unclean node failures, recovery process, and data persistence validation.
- /availability/membership_test.cpp: Validates membership changes, including node addition/removal and quorum maintenance.

### Performance Tests (integration_test/performance/)
- performance.cpp: Measures throughput and latency under different workload patterns.

### Workload Files (workloads/)
- uniform.txt: Random key distribution
- hot_10_90.txt: 10% keys receive 90% of requests
- read_benchmark.txt: Read-heavy workload
- write_benchmark.txt: Write-heavy workload

## Running Tests
```bash
./integration_test/build/single_threaded_test
./integration_test/build/failure_test
./integration_test/build/performance_test [config_file] [workload_file] [thread_num]
```
## Code Structure
```
├── README.md                  # Project README file
├── flexkv-rocks/flexkv-rocksdb   # KV-store with raft support and rocksdb backend
│   ├── ffi.rs                 # rust FFI to support C interfaces in P3 writeup
│   ├── client                 # Client in rust
│   ├── network                # http service 
│   ...
├── flexkv                  # Raft library and implementation
│   ├── core               # Core logics of Raft (including the membership change)
│   │   ...
│   ├── storage                # Log related functions
│   │   ...
...

```

## Acknowledgement 
Prior professional work of one group member served as a basis for this project.

## Contributors
### Team Quorum: 
- Ziqi Liao
- Wenjie Hu
- Varshita Rodda

