// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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
#include "BlueStore.h"
#include "common/pretty_binary.h"
#include "simple_bitmap.h"
using namespace std;

// kv store prefixes, copied from BlueStore.cc
const string PREFIX_OBJ = "O";         // object name -> onode_t

#undef dout_prefix
#define dout_prefix *_dout << "bs.onode_scan "
#undef dout_context
#define dout_context cct
#define dout_subsys ceph_subsys_bluestore

int get_key_shared_blob(const string& key, uint64_t *sbid);
bool is_extent_shard_key(const string& key);
template<typename S>
  int get_key_object(const S& key, ghobject_t *oid);
int get_key_extent_shard(const string& key, string *onode_key, uint32_t *offset);

struct bool_vector_t {
  static constexpr uint8_t null_gen = 0;
  std::vector<uint8_t> gen_markers;
  uint8_t current_gen = 1;
  void mark(uint32_t pos) {
    if (gen_markers.size() <= pos) {
      gen_markers.resize(pos * 5 / 4 + 1);
    }
    gen_markers[pos] = current_gen;
  }
  bool check(uint32_t pos) {
    return pos < gen_markers.size() && gen_markers[pos] == current_gen;
  }
  void reset() {
    current_gen++;
    if (current_gen == null_gen) {
      // visited all possible generation markers, we need to clear table
      memset(gen_markers.data(), null_gen, gen_markers.size());
      current_gen++; // step out of invalid generation
    }
  }
};

class BlueStore::Decoder_AllocationsAndStatFS : public BlueStore::ExtentMap::ExtentDecoder {
  using Extent = BlueStore::Extent;
  BlueStore &store;
  read_alloc_stats_t &stats;
  SimpleBitmap &sbmap;
  uint8_t min_alloc_size_order;
  Extent extent;
  ghobject_t oid;
  volatile_statfs *per_pool_statfs = nullptr;
  bool_vector_t is_local_blob_compressed;
  bool_vector_t is_spanning_blob_compressed;
  BlobRef blob = new Blob(nullptr);

  void _consume_new_blob(bool spanning, uint64_t extent_no, uint64_t sbid, BlobRef b);

protected:
  BlobRef decode_create_blob(
  bptr_c_it_t& p, __u8 struct_v, uint64_t* sbid, bool include_ref_map, Collection* c) override;
  void consume_blobid(Extent *, bool spanning, uint64_t blobid) override;
  void consume_blob(Extent *le, uint64_t extent_no, uint64_t sbid, BlobRef b) override;
  void consume_spanning_blob(uint64_t sbid, BlobRef b) override;
  Extent *get_next_extent() override {
    ++stats.extent_count;
    extent = Extent();
    return &extent;
  }
  void add_extent(Extent *) override {}

public:
  Decoder_AllocationsAndStatFS(
    BlueStore &_store,
    read_alloc_stats_t &_stats,
    SimpleBitmap &_sbmap,
    uint8_t _min_alloc_size_order)
    : store(_store), stats(_stats), sbmap(_sbmap), min_alloc_size_order(_min_alloc_size_order) {}
  const ghobject_t &get_oid() const { return oid; }
  void reset(const ghobject_t _oid, volatile_statfs *_per_pool_statfs);
  void reset_new_shard();
};

BlueStore::BlobRef BlueStore::Decoder_AllocationsAndStatFS::decode_create_blob(
  bptr_c_it_t& p,
  __u8 struct_v,
  uint64_t* sbid,
  bool include_ref_map,
  Collection* c) {
  // reuse same blob all over
  blob->decode<false>(p, struct_v, sbid, include_ref_map, c);
  return blob;
}

void BlueStore::Decoder_AllocationsAndStatFS::_consume_new_blob(
  bool spanning,
  uint64_t extent_no,
  uint64_t sbid,
  BlobRef b)
{
  [[maybe_unused]] auto cct = store.cct;
  ceph_assert(per_pool_statfs);
  ceph_assert(oid != ghobject_t());

  auto &blob = b->get_blob();
  bool compressed = blob.is_compressed();
  if(spanning) {
    dout(20) << __func__ << " spanning " << b->id << dendl;
    ceph_assert(b->id >= 0);
    if (compressed) {
      is_spanning_blob_compressed.mark(b->id);
    }
    ++stats.spanning_blob_count;
  } else {
    dout(20) << __func__ << " local " << extent_no << dendl;
    if (compressed) {
      is_local_blob_compressed.mark(extent_no);
    }
  }

  uint64_t new_marked_allocations = 0;
  for (auto &pe : blob.get_extents()) {
    if (pe.offset != bluestore_pextent_t::INVALID_OFFSET) {
      new_marked_allocations += sbmap.set_atomic(
        pe.offset >> min_alloc_size_order, pe.length >> min_alloc_size_order);
    }
  }
  per_pool_statfs->allocated() += new_marked_allocations
                                  << min_alloc_size_order;
  if (compressed) {
    per_pool_statfs->compressed_allocated() +=
        new_marked_allocations << min_alloc_size_order;
    per_pool_statfs->compressed() += blob.get_compressed_payload_length();
    ++stats.compressed_blob_count;
  }
}

void BlueStore::Decoder_AllocationsAndStatFS::consume_blobid(
  Extent* le, bool spanning, uint64_t blobid)
{
  [[maybe_unused]] auto cct = store.cct;
  dout(20) << __func__ << " " << spanning << " " << blobid << dendl;
  auto& vec = spanning ? is_spanning_blob_compressed : is_local_blob_compressed;
  per_pool_statfs->stored() += le->length;
  if (vec.check(blobid)) {
    per_pool_statfs->compressed_original() += le->length;
  }
}

void BlueStore::Decoder_AllocationsAndStatFS::consume_blob(
  Extent* le, uint64_t extent_no, uint64_t sbid, BlobRef b)
{
  _consume_new_blob(false, extent_no, sbid, b);
  per_pool_statfs->stored() += le->length;
  if (b->get_blob().is_compressed()) {
    per_pool_statfs->compressed_original() += le->length;
  }
}

void BlueStore::Decoder_AllocationsAndStatFS::consume_spanning_blob(
  uint64_t sbid, BlobRef b)
{
  _consume_new_blob(true, 0/*doesn't matter*/, sbid, b);
}

void BlueStore::Decoder_AllocationsAndStatFS::reset(
  const ghobject_t _oid, volatile_statfs* _per_pool_statfs)
{
  oid = _oid;
  per_pool_statfs = _per_pool_statfs;
  is_local_blob_compressed.reset();
  is_spanning_blob_compressed.reset();
}

void BlueStore::Decoder_AllocationsAndStatFS::reset_new_shard() {
  is_local_blob_compressed.reset();
}


class BlueStore::OnodeScanMT {
  BlueStore& store;
  SimpleBitmap *sbmap;
  read_alloc_stats_t& stats;
  vector<KeyValueDB::keyrange_t> chunks;
  uint32_t chunk_pos = 0;
  uint64_t total = 0;
  uint64_t interval = 1'000'000;
  ceph::mutex lock = ceph::make_mutex("BlueStore::OnodeScanMT::lock");
  void report_progress(uint32_t thread_no, uint64_t no_completed) {
    auto &cct = store.cct;
    std::lock_guard l(lock);
    if (total / interval != (total + no_completed) / interval) {
      dout(5) << __func__ << " processed objects count = "
        << (total + no_completed) / interval * interval << dendl;
    }
    total += no_completed;
  }

  bool ask_for_work(
    string& start_key,
    string& upper_bound_key) {
    std::lock_guard l(lock);
    if (chunk_pos < chunks.size()) {
      start_key = chunks[chunk_pos].first_key;
      upper_bound_key = chunks[chunk_pos].upper_bound;
      chunk_pos++;
      return true;
    } else {
      return false;
    }
  }

  int scan_onodes_range(
    read_alloc_stats_t& stats,
    const string& start_key,
    const string& upper_bound_key)
  {
    auto& db = store.db;
    auto& cct = store.cct;
    auto it = db->get_iterator(PREFIX_OBJ, KeyValueDB::ITERATOR_NOCACHE);
    if (!it) {
      derr << "failed getting onode's iterator" << dendl;
      return -ENOENT;
    }

    uint64_t kv_count = 0;
    uint64_t last_completed = 0;
    uint64_t count_interval = 100'000;
    Decoder_AllocationsAndStatFS edecoder(store, stats, *sbmap,
                                          store.min_alloc_size_order);
    it->lower_bound(start_key);
    // skip to first key that is beginning of Onode
    while (it->valid() && is_extent_shard_key(it->key())) {
      it->next();
    }
    // iterate over all onodes in requested range
    for (; it->valid(); it->next(), kv_count++) {
      if (kv_count && (kv_count % count_interval == 0)) {
        report_progress(0, kv_count - last_completed);
        last_completed = kv_count;
      }
      auto key = it->key();
      auto okey = key;
      dout(20) << __func__ << " decode onode " << pretty_binary_string(key) << dendl;
      ghobject_t oid;
      if (!is_extent_shard_key(it->key())) {
        if (it->key() >= upper_bound_key) {
          // Drag iteration after upper bound until new onode is found.
          break;
        }
        int r = get_key_object(okey, &oid);
        if (r != 0) {
          derr << __func__
               << " failed to decode onode key = " << pretty_binary_string(okey)
               << dendl;
          continue;
        }
        edecoder.reset(oid,
                       &stats.actual_pool_vstatfs[oid.hobj.get_logical_pool()]);
        Onode dummy_on(cct);
        Onode::decode_raw(&dummy_on, it->value(), edecoder, store.segment_size != 0);
        ++stats.onode_count;
      } else {
        edecoder.reset_new_shard();
        uint32_t offset;
        get_key_extent_shard(key, &okey, &offset);
        int r = get_key_object(okey, &oid);
        if (r != 0) {
          derr << __func__
               << " failed to decode onode key= " << pretty_binary_string(okey)
               << " from extent key= " << pretty_binary_string(key) << dendl;
          continue;
        }
        if (oid != edecoder.get_oid()) {
          continue;
        }
        edecoder.decode_some(it->value(), nullptr);
        ++stats.shard_count;
      }
    }
    report_progress(0, kv_count - last_completed);
    return 0;
  }

  void scanner_thread(
    uint32_t thread_no,
    read_alloc_stats_t& stats)
  {
    [[maybe_unused]] auto& cct = store.cct;
    string start_key;
    string upper_bound_key;
    while(ask_for_work(start_key, upper_bound_key)) {
      dout(10) << "thread " << thread_no << " runs: " << pretty_binary_string(start_key)
        << "..." << pretty_binary_string(upper_bound_key) << dendl;
      scan_onodes_range(stats, start_key, upper_bound_key);
    }
  }

public:
  OnodeScanMT(BlueStore& store, SimpleBitmap *sbmap, read_alloc_stats_t& stats)
  : store(store), sbmap(sbmap), stats(stats) {}

  int scan() {
    [[maybe_unused]] auto& cct = store.cct;
    std::vector<read_alloc_stats_t> thr_stats;
    std::vector<thread> thr;

    //bluestore_allocation_recovery_threads
    size_t num_threads = cct->_conf.get_val<uint64_t>("bluestore_allocation_recovery_threads");
    ceph_assert(num_threads > 0);
    std::vector<double> timers;
    store.db->util_divide_key_range(
      PREFIX_OBJ, "", string(100, '\377'), num_threads, 50'000'000, 0.05, chunks);
    for (size_t i = 0; i < chunks.size(); i++) {
      dout(10) << i << ": " << pretty_binary_string(chunks[i].first_key)
        << "..." << pretty_binary_string(chunks[i].upper_bound) << dendl;
    }

    num_threads = std::min(num_threads, chunks.size());
    thr_stats.resize(num_threads);
    thr.resize(num_threads);
    timers.resize(num_threads);
    for (size_t i = 0; i < num_threads; i++) {
      thr[i] = std::thread(
        &BlueStore::OnodeScanMT::scanner_thread, this, i, std::ref(thr_stats[i]));
    }
    for (size_t i = 0; i < num_threads; i++) {
      thr[i].join();
      stats.onode_count += thr_stats[i].onode_count;
      stats.shard_count += thr_stats[i].shard_count;
      stats.shared_blob_count += thr_stats[i].shared_blob_count;
      stats.compressed_blob_count += thr_stats[i].compressed_blob_count;
      stats.spanning_blob_count += thr_stats[i].spanning_blob_count;
      stats.skipped_illegal_extent += thr_stats[i].skipped_illegal_extent;
      stats.extent_count += thr_stats[i].extent_count;
      stats.insert_count += thr_stats[i].insert_count;
      for (auto &k : thr_stats[i].actual_pool_vstatfs) {
        stats.actual_pool_vstatfs[k.first] += k.second;
      }
    }
    return 0;
  }
};

int BlueStore::read_allocation_from_onodes_mt(SimpleBitmap *sbmap, read_alloc_stats_t& stats)
{
  OnodeScanMT scaner(*this, sbmap, stats);
  scaner.scan();
  return 0;
}

