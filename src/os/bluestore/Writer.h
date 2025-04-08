// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef BLUESTORE_WRITER
#define BLUESTORE_WRITER

#include "BlueStore.h"
#include "Allocator.h"

class BlueStore::Writer {
public:
  using exmp_it = extent_map_t::iterator;
  using P = BlueStore::printer;

  // Data that is to be put to object.
  struct blob_data_t {
    //uint32_t location;        // There is no need for each chunk to have separate location.
    uint32_t real_length;       // Size of object data covered by this chunk. Same as object_data.length().
    uint32_t compressed_length; // Size of compressed representation. 0 or disk_data.length().
    bufferlist disk_data;       // Bitstream to got o disk. Its either same as object_data,
                                // or contains compressed data. Block aligned.
    bufferlist object_data;     // Object data. Needed to put into caches.
    bool is_compressed() const {return compressed_length != 0;}
    blob_data_t()
      : real_length(0), compressed_length(0) {}
    blob_data_t(
      uint32_t real_length, uint32_t compressed_length,
      const bufferlist& disk_data, const bufferlist& object_data)
      : real_length(real_length), compressed_length(compressed_length),
        disk_data(disk_data), object_data(object_data) {};
  };
  using blob_vec = std::vector<blob_data_t>;
  struct blob_data_printer {
    const blob_vec& blobs;
    uint32_t base_position;
    blob_data_printer(const blob_vec& blobs, uint32_t base_position)
    : blobs(blobs), base_position(base_position) {}
  };

  struct write_divertor {
    virtual ~write_divertor() = default;
    virtual void write(
      uint64_t disk_offset, const bufferlist& data, bool deferred) = 0;
  };
  struct read_divertor {
    virtual ~read_divertor() = default;
    virtual bufferlist read(uint32_t object_offset, uint32_t object_length) = 0;
  };
  Writer(BlueStore* bstore, TransContext* txc, WriteContext* wctx, OnodeRef o)
    :left_shard_bound(0), right_shard_bound(OBJECT_MAX_SIZE)
    , bstore(bstore), txc(txc), wctx(wctx), onode(o) {
      pp_mode = debug_level_to_pp_mode(bstore->cct);
    }
public:
  void do_write(
    uint32_t location,
    bufferlist& data
  );

  void do_write_with_blobs(
    uint32_t location,
    uint32_t data_end,
    uint32_t ref_end,
    blob_vec& blobs
  );

  void debug_iterate_buffers(
    std::function<void(uint32_t offset, const bufferlist& data)> data_callback
  );

  write_divertor* test_write_divertor = nullptr;
  read_divertor* test_read_divertor = nullptr;
  std::vector<BlobRef> pruned_blobs;
  volatile_statfs statfs_delta;
  uint32_t left_shard_bound;  // if sharding is in effect,
  uint32_t right_shard_bound; // do not cross this line
  uint32_t left_affected_range;
  uint32_t right_affected_range;
private:
  BlueStore* bstore;
  TransContext* txc;
  WriteContext* wctx;
  OnodeRef onode;
  PExtentVector released;   //filled by punch_hole
  PExtentVector allocated;  //filled by alloc()
  bool do_deferred = false;
  // note: disk_allocs.it is uninitialized.
  //       it must be initialized in do_write
  struct {
    PExtentVector::iterator it;  //iterator
    uint32_t                pos; //in-iterator position
  } disk_allocs; //disk locations to use when placing data
  uint16_t pp_mode = 0; //pretty print mode
  uint16_t debug_level_to_pp_mode(CephContext* cct);

  inline void _crop_allocs_to_io(
    PExtentVector& disk_extents,
    uint32_t crop_front,
    uint32_t crop_back);

  inline exmp_it _find_mutable_blob_left(
    exmp_it it,
    uint32_t search_begin, // only interested in blobs that are
    uint32_t search_end,   // within range [begin - end)
    uint32_t mapmust_begin,// for 'unused' case: the area
    uint32_t mapmust_end); // [begin - end) must be mapped 

  inline exmp_it _find_mutable_blob_right(
    exmp_it it,
    uint32_t search_begin,  // only interested in blobs that are
    uint32_t search_end,    // within range [begin - end)
    uint32_t mapmust_begin, // for 'unused' case: the area
    uint32_t mapmust_end);  // [begin - end) must be mapped 

  inline void _schedule_io_masked(
    uint64_t disk_offset,
    bufferlist data,
    uint64_t mask,
    uint32_t chunk_size);

  inline void _schedule_io(
    const PExtentVector& disk_extents,
    bufferlist data);

  //Take `length` space from `this.disk_allocs` and put it to `dst`.
  void _get_disk_space(
    uint32_t length,
    PExtentVector& dst);

  inline bufferlist _read_self(
    uint32_t offset,
    uint32_t length);

  inline void _maybe_expand_blob(
    Blob* blob,
    uint32_t new_blob_size);

  inline void _blob_put_data(
    Blob* blob,
    uint32_t in_blob_offset,
    bufferlist disk_data);

  void _split_data(
    uint32_t location,
    bufferlist& data,
    blob_vec& bd);

  void _align_to_disk_block(
    uint32_t& location,
    uint32_t& ref_end,
    blob_vec& blobs);

  void _place_extent_in_blob(
    Extent* target,
    uint32_t map_begin,
    uint32_t map_end,
    uint32_t in_blob_offset);

  void _maybe_meld_with_prev_extent(exmp_it after_punch_it);

  inline void _blob_put_data_subau(
    Blob* blob,
    uint32_t in_blob_offset,
    bufferlist disk_data);

  inline void _blob_put_data_allocate(
    Blob* blob,
    uint32_t in_blob_offset,
    bufferlist disk_data);

  inline void _blob_put_data_subau_allocate(
    Blob* blob,
    uint32_t in_blob_offset,
    bufferlist disk_data);

  BlobRef _blob_create_with_data(
    uint32_t in_blob_offset,
    bufferlist& disk_data);

  BlobRef _blob_create_full(
    bufferlist& disk_data);

  BlobRef _blob_create_full_compressed(
    bufferlist& disk_data,
    uint32_t compressed_length,
    bufferlist& object_data);

  void _try_reuse_allocated_l(
    exmp_it after_punch_it,   // hint, we could have found it ourselves
    uint32_t& logical_offset, // will fix value if something consumed
    uint32_t ref_end_offset,  // useful when data is padded
    blob_data_t& bd);           // modified when consumed

  void _try_reuse_allocated_r(
    exmp_it after_punch_it,   // hint, we could have found it ourselves
    uint32_t& end_offset,     // will fix value if something consumed
    uint32_t ref_end_offset,  // useful when data is padded
    blob_data_t& bd);           // modified when consumed

  void _try_put_data_on_allocated(
  uint32_t& logical_offset, 
  uint32_t& end_offset,
  uint32_t& ref_end_offset,
  blob_vec& bd,
  exmp_it after_punch_it);

  void _do_put_new_blobs(
    uint32_t logical_offset, 
    uint32_t ref_end_offset,
    blob_vec::iterator& bd_it,
    blob_vec::iterator bd_end);

  void _do_put_blobs(
    uint32_t logical_offset, 
    uint32_t data_end_offset,
    uint32_t ref_end_offset,
    blob_vec& bd,
    exmp_it after_punch_it);

  std::pair<bool, uint32_t> _write_expand_l(
    uint32_t logical_offset);

  std::pair<bool, uint32_t> _write_expand_r(
    uint32_t end_offset);

  void _collect_released_allocated();

  void _defer_or_allocate(uint32_t need_size);
};

std::ostream& operator<<(std::ostream& out, const BlueStore::Writer::blob_data_printer& printer);

#endif // BLUESTORE_WRITER
