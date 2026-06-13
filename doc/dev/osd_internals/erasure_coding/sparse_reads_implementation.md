# EC Support for Sparse Reads and Logical Allocation - Implementation Plan

## Overview

This document provides an implementation plan for adding EC pool support for sparse reads and logical allocation tracking. The work is organized into stories with clear acceptance criteria, enabling incremental delivery and testing.

**Target Use Cases:**
- RBD encrypted images (LUKS2) on EC pools
- CephFS fscrypt on EC pools

**Implementation Approach:**

OSD-side zero detection enabled per-pool using `FLAG_TRACK_ZERO_BLOCKS` pool flag. This provides a centralized solution benefiting all RADOS clients.

## Story Organization

Stories are organized into epics representing major functional areas. Each story includes:

- **Description**: What needs to be implemented
- **Acceptance Criteria**: Testable conditions for story completion
- **Technical Tasks**: Implementation steps
- **Files Modified**: Specific file paths
- **Dependencies**: Required prior stories
- **Story Points**: Fibonacci scale (1, 2, 3, 5, 8, 13, 21)

## Epic 1: Core Metadata Infrastructure

### US-1.1: Store Force-Allocated Extents in Object Metadata

**Description:**

Add `force_allocated_extents` field to `object_info_t` to store which 4K-aligned blocks contain explicitly written zeros. This metadata must be preserved across recovery operations.

**Acceptance Criteria:**

1. `object_info_t` contains `interval_set<uint64_t> force_allocated_extents` field
2. Field is properly encoded/decoded with version compatibility
3. Empty interval_set requires minimal storage overhead
4. Unit tests verify encoding/decoding correctness
5. Backward compatibility maintained with older OI versions

**Technical Tasks:**

- Add `force_allocated_extents` field to `object_info_t` in `src/osd/osd_types.h`
- Implement ENCODE_START/ENCODE_FINISH with version bump
- Add decode logic with version checking
- Create unit tests in `src/test/osd/test_osd_types.cc`
- Verify empty interval_set storage overhead

**Files Modified:**
- `src/osd/osd_types.h`
- `src/osd/osd_types.cc`
- `src/test/osd/test_osd_types.cc`

**Dependencies:** None

**Story Points:** 3

### US-1.2: Add Pool Flag for Zero-Block Tracking

**Description:**

Add `FLAG_TRACK_ZERO_BLOCKS` pool flag to enable zero-block tracking on a per-pool basis. Users can enable it via `ceph osd pool set <pool> set_pool_flags <flag_value> --yes-i-really-mean-it`.

**Acceptance Criteria:**

1. `FLAG_TRACK_ZERO_BLOCKS` flag defined in `pg_pool_t` flags enum
2. Flag value doesn't conflict with existing pool flags
3. Flag is documented in code comments
4. OSD checks pool flag before performing zero-detection
5. Unit tests verify flag can be set and read

**Technical Tasks:**

- Add `FLAG_TRACK_ZERO_BLOCKS` to `pg_pool_t` flags enum in `src/osd/osd_types.h`
- Document flag behavior in comments
- Add logic to check pool flag in write path
- Add unit tests

**Files Modified:**
- `src/osd/osd_types.h`
- `src/osd/PrimaryLogPG.cc`
- `src/test/osd/test_osd_types.cc`

**Dependencies:** None

**Story Points:** 2

### US-1.3: Implement Extent Management Utilities

**Description:**

Create utility functions for managing force-allocated extent intervals, including adding, removing, merging, and splitting extents.

**Acceptance Criteria:**

1. Can add 4K-aligned extents to `force_allocated_extents`
2. Can remove extents when overwritten with non-zero data
3. Handles extent merging automatically
4. Handles extent splitting for partial overwrites
5. Handles truncate/zero operations correctly
6. Unit tests cover all edge cases (overlaps, merges, splits)

**Technical Tasks:**

- Create `add_force_allocated_extent()` helper
- Create `remove_force_allocated_extent()` helper
- Create `truncate_force_allocated_extents()` helper
- Implement extent merging logic
- Implement extent splitting logic
- Add comprehensive unit tests

**Files Modified:**
- `src/osd/osd_types.h`
- `src/osd/osd_types.cc`
- `src/test/osd/test_osd_types.cc`

**Dependencies:** US-1.1

**Story Points:** 5

## Epic 2: Zero Detection Implementation

### US-2.1: Implement Two-Stage Zero Detection

**Description:**

Implement optimized zero-block detection algorithm with quick check (first 8 bytes) and full check (ISA-L mem_is_zero) to minimize performance impact.

**Acceptance Criteria:**

1. Quick check: Tests first 8 bytes (uint64_t) of each 4K block
2. Full check: Uses ISA-L `mem_is_zero()` only when quick check passes
3. Processes data in 4K blocks
4. Returns `interval_set<uint64_t>` of zero-block extents
5. Performance benchmarks show < 1% overhead for encrypted workloads
6. Unit tests verify correct detection for various patterns

**Technical Tasks:**

- Implement `detect_zero_blocks()` function
- Add quick check (first 8 bytes)
- Add full check using ISA-L
- Build interval_set of results
- Create performance benchmarks
- Add unit tests for edge cases (partial zeros, aligned/unaligned)

**Files Modified:**
- `src/osd/ECBackend.cc`
- `src/osd/ECBackend.h`
- `src/test/osd/test_ec_backend.cc`
- `src/test/osd/bench_zero_detection.cc` (new)

**Dependencies:** None

**Story Points:** 5

### US-2.2: Enable Zero Detection on Write Operations

**Description:**

Integrate zero detection into the write path, checking the pool flag and performing detection when enabled for the pool.

**Acceptance Criteria:**

1. Checks if pool has `FLAG_TRACK_ZERO_BLOCKS` flag set
2. Performs zero detection on all writes to pools with flag enabled
3. Updates `force_allocated_extents` in OI with detected zeros
4. Handles WRITE, WRITEFULL, WRITESAME operations
5. Unit tests verify tracking works correctly
6. Integration tests verify end-to-end behavior

**Technical Tasks:**

- Check pool flag in write path
- Call zero detection function when pool flag is set
- Update OI with results
- Handle different write operation types
- Add unit and integration tests

**Files Modified:**
- `src/osd/PrimaryLogPG.cc`
- `src/osd/ECBackend.cc`
- `src/test/osd/test_pg.cc`

**Dependencies:** US-1.1, US-1.2, US-1.3, US-2.1

**Story Points:** 8

### US-2.3: Handle WRITE Operation Extent Updates

**Description:**

Implement extent update logic for WRITE operations. Non-zero data can overlay existing force-allocated extents without updating the extent set.

**Acceptance Criteria:**

1. Adds detected zero-block extents to `force_allocated_extents`
2. Non-zero data can overlay existing extents without removing them
3. Unit tests verify WRITE extent updates
4. Integration tests verify extent state after writes

**Technical Tasks:**

- Implement extent update logic for WRITE
- Handle overlay of non-zero data over existing extents
- Add unit and integration tests

**Files Modified:**
- `src/osd/PrimaryLogPG.cc`
- `src/osd/ECBackend.cc`
- `src/test/osd/test_pg.cc`

**Dependencies:** US-2.2

**Story Points:** 2

### US-2.4: Handle WRITEFULL Operation Extent Updates

**Description:**

Implement extent update logic for WRITEFULL operations. WRITEFULL replaces all object data, so existing extents must be cleared first.

**Acceptance Criteria:**

1. Clears all existing `force_allocated_extents` before write
2. Adds new zero-block extents from WRITEFULL data
3. Unit tests verify WRITEFULL clears and rebuilds extents
4. Integration tests verify extent state after WRITEFULL

**Technical Tasks:**

- Clear existing extents on WRITEFULL
- Add new zero-block extents
- Add unit and integration tests

**Files Modified:**
- `src/osd/PrimaryLogPG.cc`
- `src/osd/ECBackend.cc`
- `src/test/osd/test_pg.cc`

**Dependencies:** US-2.3

**Story Points:** 1

### US-2.5: Handle WRITESAME Operation Extent Updates

**Description:**

Implement extent update logic for WRITESAME operations. Only add extents if writing zeros.

**Acceptance Criteria:**

1. If writing zeros: adds extent for the written range
2. If writing non-zero data: no extent updates
3. Unit tests verify WRITESAME extent handling
4. Integration tests verify extent state

**Technical Tasks:**

- Check if WRITESAME data is zeros
- Add extent if zeros, skip if non-zero
- Add unit and integration tests

**Files Modified:**
- `src/osd/PrimaryLogPG.cc`
- `src/osd/ECBackend.cc`
- `src/test/osd/test_pg.cc`

**Dependencies:** US-2.3

**Story Points:** 1

### US-2.6: Handle TRUNCATE/TRIMTRUNC Operation Extent Updates

**Description:**

Implement extent update logic for TRUNCATE and TRIMTRUNC operations. Remove extents beyond the new object size.

**Acceptance Criteria:**

1. Removes `force_allocated_extents` beyond new size
2. Preserves extents within new size
3. Unit tests verify truncate extent handling
4. Integration tests verify extent state after truncate

**Technical Tasks:**

- Remove extents beyond truncate point
- Add unit and integration tests

**Files Modified:**
- `src/osd/PrimaryLogPG.cc`
- `src/osd/ECBackend.cc`
- `src/test/osd/test_pg.cc`

**Dependencies:** US-2.3

**Story Points:** 1

### US-2.7: Handle ZERO Operation Extent Updates

**Description:**

Implement extent update logic for ZERO operations. ZERO explicitly deallocates storage, so force-allocated extents must be removed.

**Acceptance Criteria:**

1. Removes `force_allocated_extents` for zeroed regions
2. ZERO operation deallocates storage as expected
3. Unit tests verify ZERO removes extents
4. Integration tests verify extent state after ZERO

**Technical Tasks:**

- Remove extents for ZERO operation ranges
- Add unit and integration tests

**Files Modified:**
- `src/osd/PrimaryLogPG.cc`
- `src/osd/ECBackend.cc`
- `src/test/osd/test_pg.cc`

**Dependencies:** US-2.3

**Story Points:** 1

## Epic 3: Recovery and Reconstruction

### US-3.1: Preserve Zero-Block Allocation During Recovery

**Description:**

Modify EC recovery to consult `force_allocated_extents` when deciding whether to allocate or deallocate zero blocks.

**Acceptance Criteria:**

1. Recovery reads `force_allocated_extents` from OI
2. For blocks in `force_allocated_extents`: writes zeros as allocated
3. For zero blocks NOT in `force_allocated_extents`: marks as unallocated
4. For non-zero blocks: writes as-is
5. Unit tests verify allocation preservation
6. Integration tests verify recovery with various extent patterns
7. Degraded read tests verify correct allocation during recovery

**Technical Tasks:**

- Modify `ECBackend::RecoveryBackend::continue_recovery_op()`
- Read `force_allocated_extents` from OI
- Check each 4K block against extent map
- Write allocated zeros for force-allocated blocks
- Mark unallocated for other zero blocks
- Add unit and integration tests

**Files Modified:**
- `src/osd/ECBackend.cc`
- `src/osd/ECBackendL.cc`
- `src/test/osd/test_ec_backend.cc`
- `qa/suites/rados/ec/recovery/` (new tests)

**Dependencies:** US-1.1, US-1.3, US-2.2

**Story Points:** 8

### US-3.2: Preserve Zero-Block Allocation During Reconstruction

**Description:**

Modify EC reconstruction to apply the same allocation rules as recovery, ensuring sparse reads during degraded operations return correct allocation information.

**Acceptance Criteria:**

1. Reconstruction reads `force_allocated_extents` from OI
2. Applies same rules as recovery for allocation decisions
3. Sparse reads during reconstruction return correct allocation
4. Unit tests verify reconstruction behavior
5. Integration tests verify degraded sparse reads

**Technical Tasks:**

- Modify `ECCommon::ReadPipeline::objects_read_and_reconstruct()`
- Add zero detection to reconstruction path
- Read `force_allocated_extents` from OI
- Apply allocation rules
- Add unit and integration tests

**Files Modified:**
- `src/osd/ECCommon.cc`
- `src/osd/ECBackend.cc`
- `src/osd/ECBackendL.cc`
- `src/test/osd/test_ec_backend.cc`

**Dependencies:** US-3.1

**Story Points:** 8

## Epic 4: Sparse Read Operations

### US-4.1: Implement MAPEXT for EC Pools

**Description:**

Enable MAPEXT operation on EC pools by querying BlueStore allocation on each shard, merging results, and consulting force-allocated extents during recovery.

**Acceptance Criteria:**

1. MAPEXT no longer returns `-EOPNOTSUPP` on EC pools
2. Queries BlueStore for physical allocation on each shard
3. Merges allocation information across shards
4. Consults `force_allocated_extents` during recovery/reconstruction
5. Returns accurate extent map
6. Unit tests verify correct extent maps
7. Integration tests verify MAPEXT during normal and degraded operations

**Technical Tasks:**

- Remove `-EOPNOTSUPP` return for EC pools
- Implement shard allocation queries
- Implement cross-shard merging logic
- Add force-allocated extent consultation
- Add unit and integration tests

**Files Modified:**
- `src/osd/PrimaryLogPG.cc`
- `src/osd/ECBackend.cc`
- `src/osd/ECBackendL.cc`
- `src/test/osd/test_ec_backend.cc`

**Dependencies:** US-3.1, US-3.2

**Story Points:** 8

### US-4.2: Implement SPARSE_READ for EC Pools

**Description:**

Enable SPARSE_READ to return accurate allocation information on EC pools by querying BlueStore, merging results across shards, and consulting force-allocated extents.

**Acceptance Criteria:**

1. SPARSE_READ queries BlueStore allocation on each shard
2. Merges allocation results across shards
3. Consults `force_allocated_extents` during recovery/reconstruction
4. Returns data with accurate extent map
5. Unallocated regions are not transferred
6. Unit tests verify correct sparse read behavior
7. Integration tests verify normal and degraded operations

**Technical Tasks:**

- Implement allocation queries in sparse read path
- Implement cross-shard merging
- Add force-allocated extent consultation
- Optimize data transfer (skip unallocated)
- Add unit and integration tests

**Files Modified:**
- `src/osd/ECBackend.cc`
- `src/osd/ECBackendL.cc`
- `src/test/osd/test_ec_backend.cc`

**Dependencies:** US-4.1

**Story Points:** 8

### US-4.3: Update Direct EC Reads for Allocation Tracking

**Description:**

Modify direct EC reads to include force-allocated extent information in shard messages and correctly merge allocation state across shards.

**Acceptance Criteria:**

1. Direct reads include `force_allocated_extents` in shard messages
2. Allocation information is merged from multiple shards
3. Returns consistent allocation state
4. Unit tests verify shard merging logic
5. Integration tests verify direct reads with tracking enabled

**Technical Tasks:**

- Add `force_allocated_extents` to shard message protocol
- Implement allocation merging logic
- Update direct read path
- Add unit and integration tests

**Files Modified:**
- `src/osd/ECBackend.cc`
- `src/osd/ECBackendL.cc`
- `src/osd/ECMsgTypes.h`
- `src/test/osd/test_ec_backend.cc`

**Dependencies:** US-4.2

**Story Points:** 5

## Epic 5: Pool Configuration and Documentation

### US-5.1: Document Pool Flag Usage

**Description:**

Document how to enable zero-block tracking on EC pools using the pool flag, including CLI commands and use cases.

**Acceptance Criteria:**

1. Documentation explains `FLAG_TRACK_ZERO_BLOCKS` pool flag
2. Includes CLI command examples
3. Documents when to enable the flag (encrypted workloads)
4. Documents performance implications
5. Documentation is clear and complete

**Technical Tasks:**

- Document pool flag in EC pool documentation
- Add CLI command examples
- Document use cases
- Add performance notes

**Files Modified:**
- `doc/rados/operations/erasure-code.rst`
- `doc/rados/operations/pools.rst`

**Dependencies:** US-1.2

**Story Points:** 2

## Epic 6: Testing and Validation

### US-6.1a: Basic Integration Testing

**Description:**

Create foundational integration test suite for encrypted images on EC pools, covering basic operations and sparse reads. This establishes the test infrastructure for subsequent testing work.

**Acceptance Criteria:**

1. Test suite structure created in qa/suites
2. Tests cover encrypted image creation on EC pool (RBD LUKS2)
3. Tests cover basic read/write operations with zero-block tracking
4. Tests cover sparse read operations (MAPEXT, SPARSE_READ)
5. Tests run in CI pipeline
6. All basic tests pass consistently

**Technical Tasks:**

- Create test suite structure in qa/suites/rbd/ec_encrypted/
- Implement encrypted image creation tests
- Implement basic read/write operation tests
- Implement sparse read operation tests (MAPEXT, SPARSE_READ)
- Add to CI configuration
- Document test execution procedures

**Files Modified:**
- `qa/suites/rbd/ec_encrypted/` (new test suite)
- `qa/suites/rbd/ec_encrypted/tasks/` (test tasks)
- `.github/workflows/` or Jenkins config (CI integration)

**Dependencies:** US-2.7, US-4.2

**Story Points:** 5

### US-6.1b: Recovery and Reconstruction Testing

**Description:**

Create comprehensive tests for recovery and reconstruction scenarios, verifying that allocation state is correctly preserved through various failure conditions.

**Acceptance Criteria:**

1. Tests cover recovery scenarios (OSD down/up)
2. Tests cover reconstruction scenarios (degraded reads)
3. Tests cover multiple recovery cycles
4. Tests verify allocation state preservation through recovery
5. Tests cover stress scenarios with various failure patterns
6. All recovery tests pass consistently

**Technical Tasks:**

- Implement OSD failure/recovery tests
- Implement degraded read tests
- Implement multiple recovery cycle tests
- Implement allocation state verification tests
- Implement stress tests with various failure patterns
- Document recovery test scenarios

**Files Modified:**
- `qa/suites/rbd/ec_encrypted/tasks/` (recovery test tasks)
- `qa/suites/rados/ec/recovery/` (EC recovery tests)

**Dependencies:** US-6.1a, US-3.2, US-4.3

**Story Points:** 8

### US-6.1c: Teuthology Test Integration

**Description:**

Modify existing Teuthology test suites to include EC sparse read testing, and create new Teuthology tasks for automated testing in the Ceph lab environment.

**Acceptance Criteria:**

1. Existing EC test suites updated to include sparse read tests
2. New Teuthology tasks created for zero-block tracking scenarios
3. Tests can be scheduled and run in Sepia lab
4. Test results are properly reported
5. Documentation for running Teuthology tests
6. Tests pass in lab environment

**Technical Tasks:**

- Identify existing EC test suites to modify
- Add sparse read test tasks to existing suites
- Create new Teuthology tasks for zero-block tracking
- Test execution in Sepia lab
- Create test scheduling configurations
- Document Teuthology test procedures

**Files Modified:**
- `qa/suites/rados/ec/` (existing EC test suites)
- `qa/tasks/` (new Teuthology tasks)
- `qa/suites/rbd/ec_encrypted/` (Teuthology configurations)

**Dependencies:** US-6.1a

**Story Points:** 5

### US-6.2: Performance Benchmarking and Validation

**Description:**

Create benchmarks to measure the performance impact of zero-block tracking and verify it meets the < 1% overhead requirement for encrypted workloads.

**Acceptance Criteria:**

1. Benchmark write path overhead with tracking enabled vs disabled
2. Benchmark recovery performance with tracking
3. Benchmark sparse read performance
4. Benchmark encrypted vs non-encrypted workloads
5. Write overhead < 1% for encrypted workloads
6. Results documented and reviewed
7. Performance meets acceptance criteria

**Technical Tasks:**

- Create write path benchmarks
- Create recovery benchmarks
- Create sparse read benchmarks
- Run benchmarks on representative hardware
- Document results
- Analyze and optimize if needed

**Files Modified:**
- `src/test/osd/bench_ec_zero_tracking.cc` (new)
- `doc/dev/osd_internals/erasure_coding/sparse_reads_performance.rst` (new)

**Dependencies:** US-2.7, US-3.2, US-4.2

**Story Points:** 5

### US-6.3: Backward Compatibility Testing

**Description:**

Test mixed-version clusters and upgrade scenarios to ensure backward compatibility and smooth upgrade paths.

**Acceptance Criteria:**

1. Old OSD with new client works (graceful degradation)
2. New OSD with old client works (backward compatible)
3. Upgrade path tested (old → new)
4. Downgrade path tested (new → old)
5. Compatibility matrix documented
6. All compatibility tests pass

**Technical Tasks:**

- Create mixed-version test scenarios
- Test old OSD + new client
- Test new OSD + old client
- Test upgrade path
- Test downgrade path
- Document compatibility matrix

**Files Modified:**
- `qa/suites/upgrade/ec_sparse_reads/` (new tests)
- `doc/releases/` (compatibility notes)

**Dependencies:** US-5.1

**Story Points:** 5

## Epic 7: Documentation

### US-7.1: Update RBD Encryption Documentation

**Description:**

Update RBD encryption documentation to explain EC pool support, including setup instructions, usage examples, and limitations.

**Acceptance Criteria:**

1. Documentation explains EC pool support for encrypted images
2. Includes setup instructions
3. Includes usage examples (tested and working)
4. Documents limitations and requirements
5. Documents performance characteristics
6. Documentation is clear and complete

**Technical Tasks:**

- Update RBD encryption documentation
- Add EC pool section
- Add setup instructions
- Add usage examples
- Document limitations
- Add performance notes

**Files Modified:**
- `doc/rbd/rbd-encryption.rst`
- `doc/rbd/rbd-config-ref.rst`

**Dependencies:** US-6.1

**Story Points:** 3

### US-7.2: Update EC Pool Documentation

**Description:**

Update EC pool documentation to explain sparse read support, force-allocated extent tracking, and configuration options.

**Acceptance Criteria:**

1. Documentation explains sparse read support
2. Explains force-allocated extent tracking
3. Documents configuration options
4. Documents performance implications
5. Documentation is clear and complete

**Technical Tasks:**

- Update EC pool documentation
- Add sparse read section
- Document tracking mechanism
- Document configuration
- Add performance notes

**Files Modified:**
- `doc/rados/operations/erasure-code.rst`
- `doc/rados/configuration/osd-config-ref.rst`

**Dependencies:** US-6.1

**Story Points:** 3

### US-7.3: Create Release Notes

**Description:**

Write release notes explaining the new EC sparse read feature, highlighting benefits and documenting any upgrade considerations.

**Acceptance Criteria:**

1. Release notes explain new feature
2. Highlight benefits (encrypted images on EC)
3. Document any breaking changes
4. Document upgrade considerations
5. Link to detailed documentation

**Technical Tasks:**

- Write release notes
- Explain feature and benefits
- Document considerations
- Add links to documentation

**Files Modified:**
- `doc/releases/` (appropriate release file)

**Dependencies:** US-7.1, US-7.2

**Story Points:** 2

## Epic 8: Client Configuration Support

### US-8.1: Add RBD EC Pool Validation for Encryption

**Description:**

Add validation when enabling encryption on RBD images to verify that EC data pools have the `FLAG_TRACK_ZERO_BLOCKS` flag enabled. This prevents users from enabling encryption on EC pools that don't support sparse reads.

**Acceptance Criteria:**

1. Encryption load checks if data pool is EC
2. If EC, verifies `FLAG_TRACK_ZERO_BLOCKS` is set
3. Returns clear error if flag not set
4. Works for both image creation and encryption load on existing images
5. Unit tests verify validation logic
6. Integration tests verify error handling

**Technical Tasks:**

- Add pool flag query in encryption load path
- Implement EC pool detection
- Check for `FLAG_TRACK_ZERO_BLOCKS` flag
- Add clear error messages
- Add unit and integration tests

**Files Modified:**
- `src/librbd/crypto/LoadRequest.cc`
- `src/librbd/api/Image.cc`
- `src/test/librbd/crypto/test_LoadRequest.cc`

**Dependencies:** US-1.2, US-5.1

**Story Points:** 3

### US-8.2: Update RBD CLI for EC Encryption Support

**Description:**

Update RBD CLI tools to support enabling encryption on EC pools and provide helpful guidance when the pool doesn't have sparse read support enabled.

**Acceptance Criteria:**

1. `rbd encryption format` works on EC pools with `FLAG_TRACK_ZERO_BLOCKS`
2. Clear error message when flag not set, with instructions to enable it
3. `rbd info` shows encryption status and pool type
4. Documentation updated with EC pool requirements
5. CLI help text updated
6. Integration tests verify CLI behavior

**Technical Tasks:**

- Update `rbd encryption format` command
- Add pool flag detection and error messages
- Update `rbd info` output
- Update CLI help text
- Add integration tests

**Files Modified:**
- `src/tools/rbd/action/Encryption.cc`
- `src/tools/rbd/action/Info.cc`
- `src/test/rbd/test_cli.sh`

**Dependencies:** US-8.1

**Story Points:** 3

### US-8.3: Verify CephFS fscrypt Compatibility

**Description:**

Verify that CephFS fscrypt works correctly with EC pools that have sparse read support enabled. CephFS fscrypt operates at the MDS level and should work transparently, but needs validation.

**Acceptance Criteria:**

1. CephFS fscrypt can be enabled on filesystems using EC data pools
2. File encryption/decryption works correctly
3. Sparse reads return correct allocation information
4. Performance is acceptable
5. Integration tests verify fscrypt on EC pools
6. Documentation updated

**Technical Tasks:**

- Test fscrypt with EC pools
- Verify sparse read behavior
- Run performance tests
- Add integration tests
- Document any limitations

**Files Modified:**
- `qa/suites/fs/fscrypt/` (new tests)
- `doc/cephfs/fscrypt.rst`

**Dependencies:** US-4.2, US-5.1

**Story Points:** 5

### US-8.4: Document Client Configuration Requirements

**Description:**

Create comprehensive documentation explaining how to configure RBD and CephFS clients to use encryption on EC pools, including prerequisites, setup steps, and troubleshooting.

**Acceptance Criteria:**

1. Documentation explains EC pool requirements for encryption
2. Step-by-step setup instructions for RBD
3. Step-by-step setup instructions for CephFS
4. Troubleshooting section for common issues
5. Performance considerations documented
6. Examples tested and working

**Technical Tasks:**

- Write RBD encryption on EC pools guide
- Write CephFS fscrypt on EC pools guide
- Add troubleshooting section
- Add performance notes
- Test all examples

**Files Modified:**
- `doc/rbd/rbd-encryption.rst`
- `doc/cephfs/fscrypt.rst`
- `doc/rados/operations/erasure-code.rst`

**Dependencies:** US-8.2, US-8.3

**Story Points:** 3

## Story Points Summary

### Epic 1: Core Metadata Infrastructure
- US-1.1: 3 points
- US-1.2: 2 points
- US-1.3: 5 points

**Epic 1 Total: 10 points**

### Epic 2: Zero Detection Implementation
- US-2.1: 5 points
- US-2.2: 8 points
- US-2.3: 2 points
- US-2.4: 1 point
- US-2.5: 1 point
- US-2.6: 1 point
- US-2.7: 1 point

**Epic 2 Total: 19 points**

### Epic 3: Recovery and Reconstruction
- US-3.1: 8 points
- US-3.2: 8 points

**Epic 3 Total: 16 points**

### Epic 4: Sparse Read Operations
- US-4.1: 8 points
- US-4.2: 8 points
- US-4.3: 5 points

**Epic 4 Total: 21 points**

### Epic 5: Pool Configuration and Documentation
- US-5.1: 2 points

**Epic 5 Total: 2 points**

### Epic 6: Testing and Validation
- US-6.1a: 5 points
- US-6.1b: 8 points
- US-6.1c: 5 points
- US-6.2: 5 points
- US-6.3: 5 points

**Epic 6 Total: 28 points**

### Epic 7: Documentation
- US-7.1: 3 points
- US-7.2: 3 points
- US-7.3: 2 points

**Epic 7 Total: 8 points**

### Epic 8: Client Configuration Support
- US-8.1: 3 points
- US-8.2: 3 points
- US-8.3: 5 points
- US-8.4: 3 points

**Epic 8 Total: 14 points**

## Overall Summary

**Total Stories:** 33

**Total Story Points:** 118

**Critical Path:**

1. Epic 1 (Core Infrastructure): 10 points
2. Epic 2 (Zero Detection): 19 points
3. Epic 3 (Recovery): 16 points
4. Epic 4 (Sparse Reads): 21 points
5. Epic 5 (Pool Configuration): 2 points (parallel with Epic 6)
6. Epic 6 (Testing): 28 points
7. Epic 7 (Documentation): 8 points
8. Epic 8 (Client Configuration): 14 points (can start after Epic 5)

**Velocity Assumptions:**

- Team velocity: 15-20 points per 2-week sprint
- Estimated duration: 7-8 sprints (14-16 weeks)
- Team size: 2-3 developers working in parallel

**Single Developer Estimate:**

Assuming a single developer with typical velocity of 8-10 points per week:
- **118 story points ÷ 9 points/week = ~13.1 weeks**
- **Rounded estimate: 13-14 person-weeks**

This assumes:
- Experienced developer familiar with Ceph codebase
- No major blockers or dependencies on external teams
- Includes time for testing, code review iterations, and documentation
- Does not include time for design reviews or architectural discussions

**Parallelization Strategy:**

- Epics 1-4 can be partially parallelized (infrastructure → detection → recovery → reads)
- Epic 5 can run parallel with Epic 6
- Epic 7 can start once Epic 6 begins
- Epic 8 can start after Epic 5 completes (pool flag available) and run parallel with Epics 6-7

**Notes on Estimation:**

- Story points use Fibonacci scale (1, 2, 3, 5, 8, 13, 21)
- Points reflect complexity, uncertainty, and effort
- Actual velocity will vary by team experience and familiarity with codebase
- First sprint may have lower velocity as team ramps up
- Single developer estimate assumes consistent velocity without context switching

## Delivery Milestones

**Milestone 1: Core Infrastructure Complete**
- Epics 1-2 complete
- Zero detection working
- Basic unit tests passing

**Milestone 2: Recovery Support Complete**
- Epic 3 complete
- Allocation preserved through recovery
- Recovery tests passing

**Milestone 3: Feature Complete**
- Epics 1-5 complete
- All operations working
- Client integration complete

**Milestone 4: Production Ready**
- All epics complete
- All tests passing
- Documentation complete
- Performance validated