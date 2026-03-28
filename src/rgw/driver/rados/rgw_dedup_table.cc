// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_dedup_table.h"
#include "include/ceph_assert.h"
#include <cstring>
#include <iostream>
#include <algorithm>

namespace rgw::dedup {

  //---------------------------------------------------------------------------
  dedup_table_t::dedup_table_t(const DoutPrefixProvider* _dpp,
                               uint32_t _head_object_size,
                               uint32_t _min_obj_size_for_dedup,
                               uint32_t _max_obj_size_for_split,
                               uint8_t *p_slab,
                               uint64_t slab_size)
  {
    dpp = _dpp;
    head_object_size = _head_object_size;
    min_obj_size_for_dedup = _min_obj_size_for_dedup;
    max_obj_size_for_split = _max_obj_size_for_split;
    memset(p_slab, 0, slab_size);
    hash_tab = (table_entry_t*)p_slab;
    entries_count = slab_size/sizeof(table_entry_t);
    occupied_count = 0;
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::reset_counters()
  {
    for (uint32_t tab_idx = 0; tab_idx < entries_count; tab_idx++) {
      auto &val = hash_tab[tab_idx].val;
      if (val.is_occupied()) {
        // we no longer need the counter, reuse it to count actual dedup
        val.reset_count();
      }
    }
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::remove_singletons_and_redistribute_keys(const char* step,
                                                              bool reset_count)
  {
    // stat counters
    uint64_t redistributed_search_total = 0;
    uint64_t redistributed_search_max = 0;
    uint64_t redistributed_loopback = 0;
    uint64_t redistributed_perfect = 0;
    uint64_t redistributed_clear = 0, clear = 0;
    uint64_t redistributed_not_needed = 0;

    auto remove_entry = [this](uint32_t tab_idx) {
      hash_tab[tab_idx].val.clear_flags();
      occupied_count--;
    };

    for (uint32_t tab_idx = 0; tab_idx < entries_count; tab_idx++) {
      auto &val = hash_tab[tab_idx].val;
      if (!val.is_occupied()) {
        continue;
      }

      if (val.not_enough_copies()) {
        remove_entry(tab_idx);
        clear++;
        continue;
      }

      const key_t &key = hash_tab[tab_idx].key;
      uint32_t key_idx = key.hash() % entries_count;
      // redistribute if key is not in direct hashing location
      if (key_idx != tab_idx) {
        uint64_t count = 1;
        uint32_t idx = key_idx;
        while (hash_tab[idx].val.is_occupied()        &&
               !hash_tab[idx].val.not_enough_copies() &&
               (hash_tab[idx].key != key)) {
          count++;
          idx = (idx + 1) % entries_count;
        }

        if (idx != tab_idx) {
          if (idx == key_idx) {
            redistributed_perfect++;
          }

          if (hash_tab[idx].val.is_occupied()) {
            ceph_assert(hash_tab[idx].key != key);
            ceph_assert(hash_tab[idx].val.not_enough_copies());
            remove_entry(idx);
            redistributed_clear++;
          }

          hash_tab[idx] = hash_tab[tab_idx];
          // mark the old location as free
          hash_tab[tab_idx].val.clear_flags();
        }
        else {
          // we are back where we started, there is no free entry between the
          // perfect hashing and the allocated entry
          redistributed_loopback++;
        }

        redistributed_search_max = std::max(redistributed_search_max, count);
        redistributed_search_total += count;
      }
      else {
        redistributed_not_needed++;
      }
    }

    if (reset_count) {
      reset_counters();
    }
    ldpp_dout(dpp, 10) << __func__ << "::" << step
                       << "::redistributed_search_max=" << redistributed_search_max
                       << "::redistributed_search_total=" << redistributed_search_total
                       << (clear ? "::clear=" : "::") << clear
                       << (redistributed_clear ? "::redistributed_clear=" : "::") << redistributed_clear
                       << (redistributed_perfect ? "::redistributed_perfect=" : "::") << redistributed_perfect
                       << (redistributed_not_needed ? "::redistributed_not_needed=" : "::") << redistributed_not_needed
                       << (redistributed_loopback ? "::redistributed_loopback=" : "::") << redistributed_loopback
                       << dendl;
  }

  //---------------------------------------------------------------------------
  // find_entry() assumes that entries are not removed during operation
  // remove_entry() is only called from remove_singletons_and_redistribute_keys()
  //       doing a linear pass over the array.

  uint32_t dedup_table_t::find_entry(const key_t *p_key) const
  {
    uint32_t idx = p_key->hash() % entries_count;

    // search until we either find the key, or find an empty slot.
    while (hash_tab[idx].val.is_occupied() && (hash_tab[idx].key != *p_key)) {
      idx = (idx + 1) % entries_count;
    }
    return idx;
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::inc_counters(const key_t *p_key,
                                   dedup_stats_t *p_objs_stats,
                                   uint64_t *p_duplicate_head_bytes)
  {
    // This is an approximation only since size is stored in 4KB resolution
    uint64_t byte_size_approx = disk_blocks_to_byte_size(p_key->size_4k_units);

    uint64_t dup_bytes_approx = calc_deduped_bytes(head_object_size,
                                                   min_obj_size_for_dedup,
                                                   max_obj_size_for_split,
                                                   p_key->num_parts,
                                                   byte_size_approx);
    p_objs_stats->duplicate_count ++;
    p_objs_stats->dedup_bytes_estimate += dup_bytes_approx;

    // object smaller than max_obj_size_for_split will split their head
    // and won't dup it
    if (!p_key->multipart_object() &&
        (byte_size_approx > max_obj_size_for_split) &&
        p_duplicate_head_bytes ) {
      // single part objects duplicate the head object when dedup is used
      *p_duplicate_head_bytes += head_object_size;
    }
  }

  //---------------------------------------------------------------------------
  int dedup_table_t::add_entry(key_t *p_key,
                               disk_block_id_t block_id,
                               record_id_t rec_id,
                               bool shared_manifest,
                               dedup_stats_t *p_objs_stats,
                               uint64_t *p_duplicate_head_bytes)
  {
    if (occupied_count >= entries_count) {
      return -EOVERFLOW;
    }

    value_t new_val(block_id, rec_id, shared_manifest);
    uint32_t idx = find_entry(p_key);
    value_t &val = hash_tab[idx].val;
    if (!val.is_occupied()) {
      occupied_count++;
      ceph_assert(occupied_count <= entries_count);

      hash_tab[idx].key = *p_key;
      hash_tab[idx].val = new_val;
      ldpp_dout(dpp, 20) << __func__ << "::new entry, idx=" << idx << dendl;
      ceph_assert(val.count == 1);
    }
    else {
      ceph_assert(hash_tab[idx].key == *p_key);

      if (val.count <= MAX_COPIES_PER_OBJ) {
        val.count ++;
        if(p_objs_stats) {
          ldpp_dout(dpp, 20) << __func__ << "::entry exists, idx=" << idx
                             << "::count=" << val.count << dendl;
          inc_counters(p_key, p_objs_stats, p_duplicate_head_bytes);
        }
      }

      if (!val.has_shared_manifest() && shared_manifest) {
        // replace value!
        ldpp_dout(dpp, 20) << __func__ << "::Replace with shared_manifest::["
                           << val.block_idx << "/" << (int)val.rec_id << "] -> ["
                           << block_id << "/" << (int)rec_id << "]" << dendl;
        new_val.count = val.count;
        val = new_val;
      }
      ceph_assert(val.count > 1);
    }
    ldpp_dout(dpp, 20) << __func__ << "::IDX=" << idx << "::COUNT="<< val.count << dendl;
    return 0;
  }

  //---------------------------------------------------------------------------
  uint32_t dedup_table_t::update_entry(key_t *p_key,
                                       disk_block_id_t block_id,
                                       record_id_t rec_id,
                                       bool shared_manifest,
                                       bool inc_counters)
  {
    uint32_t idx = find_entry(p_key);
    ceph_assert(hash_tab[idx].key == *p_key);
    value_t &val = hash_tab[idx].val;
    ceph_assert(val.is_occupied());

    // need to overwrite the block_idx/rec_id from the first pass
    // unless already set with shared_manifest with the correct block-id/rec-id
    // We only set the shared_manifest flag on the second pass where we
    // got valid block-id/rec-id
    if (!val.has_shared_manifest()) {
      // replace value!
      value_t new_val(block_id, rec_id, shared_manifest);
      new_val.count = val.count;
      ldpp_dout(dpp, 20) << __func__ << "::Replaced table entry::["
                         << val.block_idx << "/" << (int)val.rec_id << "] -> ["
                         << block_id << "/" << (int)rec_id << "]" << dendl;

      val = new_val;
    }
    // caller already checks for MAX_COPIES_PER_OBJ, but better safe...
    if (inc_counters && (val.count <= MAX_COPIES_PER_OBJ)) {
      val.count ++;
    }
    ldpp_dout(dpp, 20) << __func__ << "::count=" << val.get_count()
                       << "::shared_manifest=" << val.has_shared_manifest()
                       << "::block_id=" << val.get_src_block_id() << dendl;

    return val.count;
  }

  //---------------------------------------------------------------------------
  int dedup_table_t::set_src_mode(const key_t *p_key,
                                  disk_block_id_t block_id,
                                  record_id_t rec_id,
                                  bool set_shared_manifest_src,
                                  bool set_has_valid_hash_src)
  {
    uint32_t idx = find_entry(p_key);
    value_t &val = hash_tab[idx].val;
    if (val.is_occupied()) {
      if (val.block_idx == block_id && val.rec_id == rec_id) {
        if (set_shared_manifest_src) {
          val.set_shared_manifest_src();
        }
        if (set_has_valid_hash_src) {
          val.set_has_valid_hash_src();
        }
        return 0;
      }
    }

    return -ENOENT;
  }

  //---------------------------------------------------------------------------
  int dedup_table_t::get_val(const key_t *p_key, struct value_t *p_val /*OUT*/)
  {
    uint32_t idx = find_entry(p_key);
    const value_t &val = hash_tab[idx].val;
    if (val.is_occupied()) {
      ldpp_dout(dpp, 20) << __func__ << "::count=" << val.get_count() << dendl;
      *p_val = val;
      return 0;
    }
    else {
      return -ENOENT;
    }
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::count_duplicates(dedup_stats_t *p_objs_stats)
  {
    for (uint32_t tab_idx = 0; tab_idx < entries_count; tab_idx++) {
      if (!hash_tab[tab_idx].val.is_occupied()) {
        continue;
      }

      if (hash_tab[tab_idx].val.is_singleton()) {
        p_objs_stats->singleton_count++;
      }
      else {
        ceph_assert(hash_tab[tab_idx].val.count > 1);
        p_objs_stats->unique_count ++;
      }
    }
  }

} // namespace rgw::dedup

#if 0
#include <climits>
#include <cstdlib>
#include <iostream>
#include <cmath>
#include <iomanip>
#include <random>

//---------------------------------------------------------------------------
int main()
{
  static constexpr unsigned MAX_ENTRIES = 1024;
  rgw::dedup::key_t *key_tab = new rgw::dedup::key_t[MAX_ENTRIES];
  if (!key_tab) {
    std::cerr << "faild alloc!" << std::endl;
    return 1;
  }
  rgw::dedup::key_t *p_key = key_tab;
  //rgw::dedup::dedup_table_t tab(MAX_ENTRIES + MAX_ENTRIES/5);
  rgw::dedup::dedup_table_t tab(MAX_ENTRIES);

  std::cout << "sizeof(key)=" << sizeof(rgw::dedup::key_t) << std::endl;
  // Seed with a real random value, if available
  std::random_device r;
  // Choose a random mean between 1 ULLONG_MAX
  std::default_random_engine e1(r());
  std::uniform_int_distribution<uint64_t> uniform_dist(1, std::numeric_limits<uint64_t>::max());

  for (unsigned i = 0; i < MAX_ENTRIES; i++) {
    uint64_t md5_high  = uniform_dist(e1);
    uint64_t md5_low   = uniform_dist(e1);
    uint32_t size_4k_units  = std::rand();
    uint16_t num_parts = std::rand();
    //std::cout << std::hex << md5_high << "::" << md5_low << "::" << block_id << std::endl;
    rgw::dedup::key_t key(md5_high, md5_low, size_4k_units, num_parts);
    *p_key = key;
    p_key++;
  }
  work_shard_t work_shard = 3;
  for (unsigned i = 0; i < MAX_ENTRIES; i++) {
    disk_block_id_t block_id(worker_id, std::rand());
    tab.add_entry(key_tab+i, block_id, 0, false, false);
  }
  double avg = (double)total / MAX_ENTRIES;
  std::cout << "Insert::num entries=" << MAX_ENTRIES << ", total=" << total
            << ", avg=" << avg << ", max=" << max << std::endl;
  std::cout << "==========================================\n";

  total = 0;
  max = 0;
  for (unsigned i = 0; i < MAX_ENTRIES; i++) {
    tab.find_entry(key_tab+i);
  }
  avg = (double)total / MAX_ENTRIES;
  std::cout << "Find::num entries=" << MAX_ENTRIES << ", total=" << total
            << ", avg=" << avg << ", max=" << max << std::endl;
  std::cout << "==========================================\n";
  tab.remove_singletons_and_redistribute_keys();
  tab.print_redistribute_stats();
  tab.stat_counters_reset();
  std::cout << "==========================================\n";
  total = 0;
  max = 0;
  uint32_t cnt = 0;
  for (unsigned i = 0; i < MAX_ENTRIES; i++) {
    rgw::dedup::key_t *p_key = key_tab+i;
    tab.find_entry(p_key);
    cnt++;
#if 0
    if (p_key->md5_high % 5 == 0) {
      tab.find_entry(p_key);
      cnt++;
    }
#endif
  }
  avg = (double)total / cnt;
  std::cout << "num entries=" << cnt << ", total=" << total
            << ", avg=" << avg << ", max=" << max << std::endl;
}
#endif
