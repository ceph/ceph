/*
* Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <ostream>
#include <ranges>

/**
 * @class SplitOp
 * @brief Base class for splitting operations across multiple OSDs for improved performance.
 *
 * SplitOp implements a pattern for parallelizing operations by distributing them across
 * multiple OSDs in a pool. This optimization is particularly effective for:
 * - Erasure-coded pools: operations can be distributed across data shards
 * - Replicated pools: large operations can be split across replicas
 *
 * The split operation pattern works as follows:
 * 1. Validation: Check if the operation is eligible for splitting
 * 2. Creation: Create sub-operations targeting different OSDs
 * 3. Execution: Send sub-operations in parallel
 * 4. Assembly: Reconstruct results from sub-operation responses
 * 5. Completion: Return assembled results to the client
 *
 * Key features:
 * - Consistency protection: Ensures all sub-operations see the same object version
 * - Automatic fallback: Falls back to normal operation if splitting is not beneficial
 * - Error handling: Properly handles partial failures and version mismatches
 *
 * Currently optimized for read operations, with potential for future write support.
 *
 * @see ECSplitOp for erasure-coded pool implementation
 * @see ReplicaSplitOp for replicated pool implementation
 */
class SplitOp {

 protected:
  using extent = std::pair<uint64_t, uint64_t>;
  using extents_map = std::map<uint64_t, uint64_t>;
  using extent_set = interval_set<uint64_t, std::map, false>;

  using ExtentPredicate = std::function<bool(const extent&)>;
  using extent_map_subrange = std::ranges::subrange<extents_map::const_iterator>;
  using extent_map_subrange_view = std::ranges::take_while_view<extent_map_subrange, ExtentPredicate>;
  using extent_variant = std::variant<std::ranges::single_view<extent>, extent_map_subrange_view, extent_map_subrange>;
  using buffer_appender = std::function<void(bufferlist &, uint64_t *, extent_variant)>;

  /**
   * @struct ECChunkInfo
   * @brief Information about a single chunk in an erasure-coded stripe iteration.
   *
   * This structure holds the mapping between logical object offsets and physical
   * shard locations for a single chunk within an EC stripe. Used by ECStripeIterator
   * to track position during stripe traversal.
   */
  struct ECChunkInfo {
    uint64_t ro_offset;
    uint64_t shard_offset;
    uint64_t length;
    raw_shard_id_t raw_shard;

    friend std::ostream & operator<<(std::ostream &os, const ECChunkInfo &chunk_info) {
      return os
          << "ro_offset: " << chunk_info.ro_offset
          << " shard_offset: " << chunk_info.shard_offset
          << " length: " << chunk_info.length
          << " raw_shard: " << (int)chunk_info.raw_shard;
    }
  };

  /**
   * @class ECStripeIterator
   * @brief Iterator for traversing erasure-coded stripes chunk by chunk.
   *
   * ECStripeIterator provides a standard C++ iterator interface for walking through
   * an erasure-coded stripe, yielding information about each chunk's location in both
   * the logical object space and the physical shard space.
   *
   * The iterator handles the complex mapping between:
   * - Logical object offsets (ro_offset)
   * - Physical shard offsets (shard_offset)
   * - Shard identifiers (raw_shard)
   * - Chunk boundaries and lengths
   *
   * Algorithm:
   * - Starts at a given offset and iterates through chunks sequentially
   * - Automatically wraps around shards when reaching the end of a stripe
   * - Handles partial chunks at the beginning and end of the range
   * - Terminates when all requested data has been covered
   *
   * @note Conforms to std::input_iterator concept
   */
  class ECStripeIterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = ECChunkInfo;
    using difference_type = std::ptrdiff_t;
    using pointer = ECChunkInfo*;
    using reference = ECChunkInfo&;

    ECStripeIterator() = default;

    // Constructor for the "begin" iterator
    ECStripeIterator(
      uint64_t start_offset,
      uint64_t total_len,
      uint32_t chunk_s,
      uint32_t data_chunks)
      : chunk_size(chunk_s),
        data_chunk_count(data_chunks) {
      end_offset = start_offset + total_len;
      current_info.ro_offset = start_offset;
      uint64_t chunk = start_offset / chunk_size;
      current_info.length = std::min(total_len, (chunk + 1) * chunk_size - start_offset);

      current_info.raw_shard = raw_shard_id_t(chunk % data_chunk_count);
      current_info.shard_offset = (chunk / data_chunk_count) * chunk_size +
        start_offset % chunk_size;
    }

    value_type operator*() const {
      return current_info;
    }

    // Pre-increment
    ECStripeIterator& operator++() {
      current_info.ro_offset += current_info.length;
      current_info.shard_offset += current_info.length - chunk_size;
      current_info.length = std::min(chunk_size, end_offset - current_info.ro_offset);
      ceph_assert(current_info.ro_offset <= end_offset);
      ++current_info.raw_shard;
      if (std::cmp_equal((int)current_info.raw_shard, data_chunk_count)) {
        current_info.shard_offset += chunk_size;
        current_info.raw_shard = raw_shard_id_t(0);
      }
      return *this;
    }

    // post-increment
    ECStripeIterator operator++(int) {
      ECStripeIterator tmp = *this;
      ++(*this);
      return tmp;
    }

    bool operator!=(const ECStripeIterator& other) const {
      // This is only here to terminate the loop!
      return current_info.length != other.current_info.length;
    }

    bool operator==(const ECStripeIterator& other) const {
      return !(*this != other);
    }
    value_type current_info{};

  private:
    uint64_t end_offset = 0;
    uint64_t chunk_size = 0;
    uint32_t data_chunk_count = 0;
  };

  /**
   * @class ECStripeView
   * @brief Range view for iterating over erasure-coded stripes.
   *
   * ECStripeView provides a range-based interface for traversing an erasure-coded
   * stripe. It encapsulates the stripe geometry (chunk size, data chunk count) and
   * provides begin()/end() iterators for use in range-based for loops.
   *
   * Usage:
   * @code
   * ECStripeView stripe_view(offset, length, pool_info);
   * for (auto chunk_info : stripe_view) {
   *   // Process each chunk
   * }
   * @endcode
   *
   * @param offset Starting offset in the logical object
   * @param length Total length to iterate over
   * @param pi Pool information containing stripe geometry
   */
  class ECStripeView {
   public:
    ECStripeView(
      uint64_t offset,
      uint64_t length,
      const pg_pool_t *pi)
      : start_offset(offset),
        total_length(length),
        data_chunk_count(pi->nonprimary_shards.size() + 1),
        chunk_size(pi->get_stripe_width() / data_chunk_count) {
    }

    ECStripeIterator begin() const {
      return ECStripeIterator(start_offset, total_length, chunk_size, data_chunk_count);
    }

    ECStripeIterator end() const {
      ECStripeIterator end_iter;
      end_iter.current_info.length = 0;
      return end_iter;
    }

    uint64_t start_offset;
    uint64_t total_length;
    uint32_t data_chunk_count;
    uint64_t chunk_size;
  };

  static_assert(std::input_iterator<ECStripeIterator>,
                "ECStripeIterator does not conform to the std::input_iterator concept");

  /**
   * @struct Details
   * @brief Holds response data and metadata for a single operation in a sub-read.
   *
   * Contains the buffer, return value, error code, and optional extent map
   * returned from executing one operation within a SubRead.
   */
  struct Details {
    bufferlist bl;
    int rval;
    boost::system::error_code ec;
    std::optional<extents_map> e;
  };

  /**
   * @struct InternalVersion
   * @brief Holds internal version information for torn read protection.
   *
   * Used to detect version mismatches across parallel sub-operations,
   * ensuring all operations see the same object version.
   */
  struct InternalVersion {
    boost::system::error_code ec;
    bufferlist bl;
  };

  /**
   * @struct SubRead
   * @brief Represents a single sub-operation sent to one OSD.
   *
   * Contains the operation descriptor, response details for each operation,
   * return code, and optional version information for consistency checking.
   *
   * @param count Number of operations in this sub-read
   */
  struct SubRead {
    ::ObjectOperation rd;
    mini_flat_map<int, Details> details;
    int rc = -EIO;
    std::optional<InternalVersion> internal_version;

    SubRead(int count) : details(count) {}
  };

  /**
   * @struct Finisher
   * @brief Completion callback for sub-operations.
   *
   * This structure self-destructs on IO completion, using a legacy C++ pattern
   * (no shared_ptr). The finish callback records the return code, while the
   * shared_ptr to the parent SplitOp handles overall completion.
   *
   * @note Self-destructs when called
   */
  struct Finisher : Context {
    std::shared_ptr<SplitOp> split_read;
    SubRead &sub_read;

    Finisher(std::shared_ptr<SplitOp> split_read, SubRead &sub_read) : split_read(split_read), sub_read(sub_read) {}
    void finish(int r) override {
      sub_read.rc = r;
    }
  };

  int assemble_rc() const;
  virtual std::pair<extent_set, bufferlist> assemble_buffer_sparse_read(int ops_index) const = 0;
  virtual void assemble_buffer_read(bufferlist &bl_out, int ops_index) const = 0;
  virtual void init_read(OSDOp &op, bool sparse, int ops_index) = 0;
  virtual bool version_mismatch() const = 0;
  void init(OSDOp &op, int ops_index);

  Objecter::Op *orig_op;
  Objecter &objecter;
  mini_flat_map<int, SubRead> sub_reads;
  CephContext *cct;
  
  /**
   * Abort flag pattern for split operation creation:
   *
   * This flag implements a multi-stage validation and creation pattern that
   * minimizes wasted work when a split operation cannot be completed:
   *
   * 1. Cheap validation tests run first in validate() and create() before
   *    creating the split op object (e.g., checking pool flags, operation types)
   *
   * 2. If validation fails, return false immediately without creating split op
   *
   * 3. If validation passes, create the split op and begin initialization
   *
   * 4. During init_read() and init(), set abort=true if problems are detected
   *    that prevent successful operation (e.g., missing OSDs, invalid state)
   *
   * 5. After initialization, check abort flag in create() and discard the
   *    split op if set (return false to fall back to normal operation)
   *
   * 6. The complete() method only runs for successfully sent operations where
   *    abort=false, ensuring cleanup only happens for valid split ops
   *
   * This pattern ensures expensive initialization work is only done when likely
   * to succeed, while still catching edge cases that can only be detected during
   * the creation process itself.
   */
  bool abort = false;
  int flags = 0;
  int reference_sub_read = -1;
  std::map<int, std::vector<int>> op_offset_map;

 public:
 /**
  * @brief Construct a SplitOp.
  * @param op Original operation to be split
  * @param objecter Objecter instance for OSD communication
  * @param cct CephContext for logging and configuration
  * @param count Number of sub-operations to create
  */
 SplitOp(Objecter::Op *op, Objecter &objecter, CephContext *cct, int count) : orig_op(op), objecter(objecter), sub_reads(count), cct(cct) {}
 
 virtual ~SplitOp() = default;
 
 /**
  * @brief Complete the split operation by assembling results.
  *
  * Called when all sub-operations have completed. Assembles the results
  * from individual sub-operations, handles version mismatches, and invokes
  * the original operation's completion handlers.
  */
 void complete();
 
 /**
  * @brief Prepare a single-chunk operation for direct execution.
  *
  * When an operation can be satisfied by reading from a single OSD (e.g.,
  * single chunk in EC pool), this method configures the operation for
  * direct execution without splitting.
  *
  * @param op Operation to prepare
  * @param objecter Objecter instance
  * @param cct CephContext for logging
  */
 static void prepare_single_op(Objecter::Op *op, Objecter &objecter, CephContext *cct);
 
 /**
  * @brief Add version tracking to sub-operations for consistency.
  *
  * Ensures all sub-operations read the same object version by adding
  * internal version queries. If versions mismatch, the operation is retried.
  */
 void protect_torn_reads();
 
 /**
  * @brief Create and initialize a split operation.
  *
  * Main entry point for split operation creation. Validates the operation,
  * creates appropriate sub-operations, and sends them to target OSDs.
  *
  * Uses a multi-stage abort pattern:
  * 1. Cheap validation (pool checks, operation types)
  * 2. Object creation (may set abort flag)
  * 3. Initialization (may set abort if OSDs missing)
  * 4. Final validation before sending
  *
  * @param op Operation to potentially split
  * @param objecter Objecter instance
  * @param sul Shared lock for OSD map access
  * @param cct CephContext for logging
  * @return true if operation was split and sent, false to use normal path
  */
 static bool create(Objecter::Op *op, Objecter &objecter,
   shunique_lock<ceph::shared_mutex>& sul, CephContext *cct);
};

/**
 * @class ECSplitOp
 * @brief Split operation implementation for erasure-coded pools.
 *
 * ECSplitOp handles splitting operations across data shards in an erasure-coded
 * pool. It implements the stripe iteration and buffer assembly logic specific to
 * EC pools, where data is distributed across multiple shards according to the
 * erasure coding scheme.
 *
 * Key responsibilities:
 * - Iterate through EC stripes to determine which shards to read
 * - Assemble data from multiple shards in the correct order
 * - Handle sparse reads with extent maps
 * - Verify version consistency across shards
 *
 * @see ECStripeIterator for stripe traversal algorithm
 * @see ECStripeView for range-based stripe iteration
 */
class ECSplitOp : public SplitOp{
 public:
  using SplitOp::SplitOp;
  
  /**
   * @brief Assemble sparse read results from EC shards.
   *
   * Iterates through the EC stripe, collecting extent maps and data buffers
   * from each shard in the correct order to reconstruct the logical object view.
   *
   * @param ops_index Index of the operation in the operation list
   * @return Pair of extent set and assembled buffer
   */
  std::pair<extent_set, bufferlist> assemble_buffer_sparse_read(int ops_index) const override;
  
  /**
   * @brief Assemble dense read results from EC shards.
   *
   * Iterates through the EC stripe, collecting data buffers from each shard
   * in the correct order to reconstruct the contiguous logical object data.
   *
   * @param bl_out Output buffer to append assembled data
   * @param ops_index Index of the operation in the operation list
   */
  void assemble_buffer_read(bufferlist &bl_out, int ops_index) const override;
  
  /**
   * @brief Initialize read sub-operations for EC pool.
   *
   * Determines which shards need to be read based on the stripe geometry,
   * creates sub-operations for each required shard, and validates that all
   * target OSDs are available.
   *
   * @param op Operation descriptor
   * @param sparse Whether this is a sparse read
   * @param ops_index Index of the operation in the operation list
   */
  void init_read(OSDOp &op, bool sparse, int ops_index) override;
  
  /**
   * @brief Check for version mismatches across EC shards.
   *
   * Compares the internal versions returned by each shard to ensure all
   * sub-operations read the same object version. Returns true if any
   * mismatch is detected.
   *
   * @return true if versions mismatch, false if consistent
   */
  bool version_mismatch() const override;
  
  /**
   * @brief Construct an ECSplitOp.
   * @param op Original operation to be split
   * @param objecter Objecter instance
   * @param cct CephContext for logging
   * @param count Number of shards in the EC pool
   */
  ECSplitOp(Objecter::Op *op, Objecter &objecter, CephContext *cct, int count);
  
  ~ECSplitOp() {
    complete();
  }
};

/**
 * @class ReplicaSplitOp
 * @brief Split operation implementation for replicated pools.
 *
 * ReplicaSplitOp handles splitting large operations across replicas in a
 * replicated pool. It divides the operation into chunks and distributes them
 * across available replicas for parallel execution.
 *
 * Key responsibilities:
 * - Divide operations into appropriately-sized chunks
 * - Distribute chunks across available replicas
 * - Assemble results in the correct order
 * - Verify version consistency across replicas
 *
 * The chunk size is determined by configuration and the number of available
 * replicas, with a minimum threshold to ensure splitting is beneficial.
 */
class ReplicaSplitOp : public SplitOp {
 public:
  using SplitOp::SplitOp;
  
  /**
   * @brief Assemble sparse read results from replicas.
   *
   * Collects extent maps and data buffers from each replica sub-operation
   * and combines them into a single result.
   *
   * @param ops_index Index of the operation in the operation list
   * @return Pair of extent set and assembled buffer
   */
  std::pair<extent_set, bufferlist> assemble_buffer_sparse_read(int ops_index) const override;
  
  /**
   * @brief Assemble dense read results from replicas.
   *
   * Collects data buffers from each replica sub-operation and appends them
   * in order to reconstruct the complete result.
   *
   * @param bl_out Output buffer to append assembled data
   * @param ops_index Index of the operation in the operation list
   */
  void assemble_buffer_read(bufferlist &bl_out, int ops_index) const override;
  
  /**
   * @brief Initialize read sub-operations for replicated pool.
   *
   * Divides the operation into chunks and creates sub-operations targeting
   * different replicas. The chunk size and distribution are determined by
   * configuration and the number of available OSDs.
   *
   * @param op Operation descriptor
   * @param sparse Whether this is a sparse read
   * @param ops_index Index of the operation in the operation list
   */
  void init_read(OSDOp &op, bool sparse, int ops_index) override;
  
  /**
   * @brief Check for version mismatches across replicas.
   *
   * Compares the versions returned by each replica to ensure all
   * sub-operations read the same object version. Returns true if any
   * mismatch is detected.
   *
   * @return true if versions mismatch, false if consistent
   */
  bool version_mismatch() const override;
  
  /**
   * @brief Construct a ReplicaSplitOp.
   * @param op Original operation to be split
   * @param objecter Objecter instance
   * @param cct CephContext for logging
   * @param pool_size Number of replicas in the pool
   */
  ReplicaSplitOp(Objecter::Op *op, Objecter &objecter, CephContext *cct, int pool_size) :
    SplitOp(op, objecter, cct, pool_size) {}
  
  ~ReplicaSplitOp() {
    complete();
  }
};

