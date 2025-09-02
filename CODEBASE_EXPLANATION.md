# Ceph Codebase Architecture Explanation

## Overview

Ceph is a unified, distributed storage system designed to provide excellent performance, reliability, and scalability. The codebase is organized into several major components that work together to provide object storage, block storage, and file system interfaces.

## High-Level Architecture

Ceph consists of several key daemons and components:

1. **RADOS** (Reliable Autonomic Distributed Object Store) - The foundation
2. **RGW** (RADOS Gateway) - Object storage interface (S3/Swift compatible)
3. **RBD** (RADOS Block Device) - Block storage interface
4. **CephFS** - POSIX-compliant distributed file system
5. **MON** (Monitor) - Cluster state and configuration management
6. **OSD** (Object Storage Daemon) - Data storage and replication
7. **MDS** (Metadata Server) - File system metadata management

## Major Source Code Directories

### `/src/client` - Client-Side Components

This directory contains the client-side code for interacting with Ceph services:

#### SyntheticClient (`SyntheticClient.h`, `SyntheticClient.cc`)
- **Purpose**: A comprehensive testing framework for filesystem operations
- **Key Features**:
  - File system stress testing and benchmarking
  - Trace replay functionality for reproducing workloads
  - Various test modes including random operations, directory traversal, and load generation
  - Performance measurement and validation tools

**Key Methods**:
```cpp
int run()                    // Main execution loop with different test modes
int play_trace()            // Replay recorded filesystem traces
int make_dirs()             // Create directory hierarchies for testing
int write_file()            // Write test data to files
int read_file()             // Read and verify file contents
```

#### Other Client Components
- **Client.h/cc**: Main CephFS client implementation
- **Inode.h/cc**: In-memory inode representation
- **Dentry.h/cc**: Directory entry management
- **MetaRequest.h/cc**: Metadata operation requests

### `/src/rgw` - RADOS Gateway (Object Storage)

The RGW provides S3 and Swift-compatible object storage interfaces:

#### Core RGW Components (`rgw_appmain.cc`)
- **Purpose**: Main application initialization and configuration
- **Key Features**:
  - Frontend initialization for different protocols (S3, Swift, STS, IAM)
  - Storage backend configuration and setup
  - Service discovery and API endpoint registration
  - Performance monitoring and metrics collection

**Key Initialization Methods**:
```cpp
void init_frontends1()      // Initialize protocol frontends
int init_storage()          // Setup storage backends
void init_perfcounters()    // Setup performance monitoring
void cond_init_apis()       // Conditionally initialize API endpoints
```

#### Object Operations (`rgw_op.cc`)
- **Purpose**: Core object storage operations and access control
- **Key Features**:
  - Access Control Policy (ACP) management
  - Object and bucket policy enforcement
  - S3-compatible ACL processing
  - IAM policy evaluation

#### Storage Driver (`rgw_rados.h`)
- **Purpose**: Interface between RGW and RADOS storage
- **Key Features**:
  - Object read/write operations
  - Metadata management
  - Multipart upload support
  - Storage class and placement rules

### `/src/mds` - Metadata Server (File System)

The MDS manages metadata for CephFS:

#### MDSRank (`MDSRank.cc`)
- **Purpose**: Core metadata server functionality
- **Key Features**:
  - Directory and file metadata management
  - Subtree export/import for load balancing
  - Administrative commands for cluster management
  - Performance metrics and monitoring

**Key Administrative Functions**:
```cpp
void command_export_dir()      // Export directory subtrees
void command_dump_tree()       // Debug tree structure
void command_get_subtrees()    // List managed subtrees
```

### `/src/cls` - Class Extensions

Object class extensions that run on OSDs:

#### RGW Classes (`cls_rgw.cc`)
- **Purpose**: Server-side logic for RGW operations
- **Key Features**:
  - Bucket directory management
  - Object lifecycle management
  - Atomic metadata operations
  - Garbage collection support

**Key Operations**:
```cpp
int rgw_bucket_prepare_op()    // Prepare bucket operations
int rgw_dir_suggest_changes()  // Directory change suggestions
int rgw_obj_remove()           // Object removal operations
```

### `/src/erasure-code` - Data Protection

Erasure coding implementations for data durability:

#### Plugin System (`ErasureCodePlugin.cc`)
- **Purpose**: Dynamic loading and management of erasure code algorithms
- **Key Features**:
  - Plugin discovery and loading
  - Algorithm factory pattern
  - Support for different erasure code implementations (Reed-Solomon, LRC, etc.)

**Key Plugin Management**:
```cpp
class ErasureCodePluginRegistry {
    int factory()              // Create erasure code instances
    int load()                 // Load plugin libraries
    int preload()              // Preload common plugins
};
```

### `/src/messages` - Inter-Component Communication

Message classes for distributed communication:

#### MDS Messages (`MExportDirPrep.h`)
- **Purpose**: Messages for metadata server coordination
- **Key Features**:
  - Directory export preparation
  - Subtree migration coordination
  - Metadata consistency maintenance

**Message Structure**:
```cpp
class MExportDirPrep {
    dirfrag_t dirfrag;                    // Directory fragment identifier
    std::list<dirfrag_t> bounds;          // Export boundaries
    std::list<ceph::buffer::list> traces; // Operation traces
    std::set<mds_rank_t> bystanders;      // Affected MDS ranks
};
```

## Key Design Patterns

### 1. **Plugin Architecture**
- Erasure code algorithms are loaded as plugins
- Allows for extensible storage algorithms
- Runtime algorithm selection based on configuration

### 2. **Message Passing**
- All inter-component communication uses typed messages
- Asynchronous operation model
- Network-transparent distributed operations

### 3. **Object Class Extensions**
- Server-side logic runs close to data
- Reduces network overhead for complex operations
- Atomic operations on object metadata

### 4. **Hierarchical Namespace**
- CephFS provides POSIX semantics
- MDS manages metadata hierarchy
- Scalable through subtree partitioning

## Testing and Validation

### SyntheticClient Testing Framework
The SyntheticClient provides comprehensive testing capabilities:

- **Trace Replay**: Record and replay real workloads
- **Stress Testing**: Generate high-load scenarios
- **Correctness Validation**: Verify data integrity
- **Performance Benchmarking**: Measure throughput and latency

## Configuration and Deployment

The codebase supports multiple deployment scenarios:

1. **Development Testing**: `vstart.sh` for local clusters
2. **Container Deployment**: Docker/Podman support
3. **Production Deployment**: RPM/DEB packages
4. **Cloud Integration**: Integration with cloud providers

## Build System

- **CMake**: Primary build system
- **Ninja**: Fast incremental builds
- **Containerized Builds**: Support for multiple distributions
- **Dependency Management**: Bundled and system libraries

This architecture provides a scalable, reliable distributed storage system that can serve as the foundation for cloud storage, backup systems, and high-performance computing environments.