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

  // A simple struct to hold the data for each step of the iteration.
  struct ECChunkInfo {
    uint64_t ro_offset;
    uint64_t shard_offset;
    uint64_t length;
    shard_id_t shard;

    friend std::ostream & operator<<(std::ostream &os, const ECChunkInfo &obj) {
      return os
          << "ro_offset: " << obj.ro_offset
          << " shard_offset: " << obj.shard_offset
          << " length: " << obj.length
          << " shard: " << obj.shard;
    }
  };

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

      current_info.shard = shard_id_t(chunk % data_chunk_count);
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
      ++current_info.shard;
      if (unsigned(current_info.shard) == data_chunk_count) {
        current_info.shard_offset += chunk_size;
        current_info.shard = shard_id_t(0);
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


  // The custom range class that provides begin() and end()
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

  struct Details {
    bufferlist bl;
    int rval;
    boost::system::error_code ec;
    std::optional<extents_map> e;
  };

  struct InternalVersion {
    boost::system::error_code ec;
    bufferlist bl;
  };

  struct SubRead {
    ::ObjectOperation rd;
    mini_flat_map<int, Details> details;
    int rc = -EIO;
    std::optional<InternalVersion> internal_version;

    SubRead(int count) : details(count) {}
  };

  // This structure self-destructs on each IO completions, using a legacy
  // C++ pattern (no shared_ptr). We use the finish callback to record the
  // RC, but otherwise rely on the shared_ptr destroying ec_read to deal with
  // completion of the parent IO.
  struct Finisher : Context {
    std::shared_ptr<SplitOp> split_read;
    SubRead &sub_read;

    Finisher(std::shared_ptr<SplitOp> split_read, SubRead &sub_read) : split_read(split_read), sub_read(sub_read) {}
    void finish(int r) override {
      sub_read.rc = r;
    }
  };

  int assemble_rc();
  virtual std::pair<extent_set, bufferlist> assemble_buffer_sparse_read(int ops_index) = 0;
  virtual void assemble_buffer_read(bufferlist &bl_out, int ops_index) = 0;
  virtual void init_read(OSDOp &op, bool sparse, int ops_index) = 0;
  void init(OSDOp &op, int ops_index);

  Objecter::Op *orig_op;
  Objecter &objecter;
  mini_flat_map<shard_id_t, SubRead> sub_reads;
  CephContext *cct;
  bool abort = false; // Last minute abort... We want to keep this to a minimum.
  int flags = 0;
  std::optional<shard_id_t> primary_shard;
  std::map<shard_id_t, std::vector<int>> op_offset_map;

 public:
  SplitOp(Objecter::Op *op, Objecter &objecter, CephContext *cct, int count) : orig_op(op), objecter(objecter), sub_reads(count), cct(cct) {}
  virtual ~SplitOp() = default;
  void complete();
  static void prepare_single_op(Objecter::Op *op, Objecter &objecter);
  void protect_torn_reads();
  static bool create(Objecter::Op *op, Objecter &objecter,
    shunique_lock<ceph::shared_mutex>& sul, ceph_tid_t *ptid, CephContext *cct);
};

class ECSplitOp : public SplitOp{
 public:
  using SplitOp::SplitOp;
  std::pair<extent_set, bufferlist> assemble_buffer_sparse_read(int ops_index) override;
  void assemble_buffer_read(bufferlist &bl_out, int ops_index) override;
  void init_read(OSDOp &op, bool sparse, int ops_index) override;
  ECSplitOp(Objecter::Op *op, Objecter &objecter, CephContext *cct, int count);
  ~ECSplitOp() {
    complete();
  }
};

class ReplicaSplitOp : public SplitOp {
 public:
  using SplitOp::SplitOp;
  std::pair<extent_set, bufferlist> assemble_buffer_sparse_read(int ops_index) override;
  void assemble_buffer_read(bufferlist &bl_out, int ops_index) override;
  void init_read(OSDOp &op, bool sparse, int ops_index) override;
  ReplicaSplitOp(Objecter::Op *op, Objecter &objecter, CephContext *cct, int pool_size);
  ~ReplicaSplitOp() {
    complete();
  }
};

