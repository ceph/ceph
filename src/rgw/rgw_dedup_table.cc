// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
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

namespace rgw::dedup {

  //---------------------------------------------------------------------------
  dedup_table_t::dedup_table_t(const DoutPrefixProvider* _dpp,
                               uint32_t _head_object_size,
                               uint8_t *p_slab,
                               uint64_t slab_size)
  {
    dpp = _dpp;
    head_object_size = _head_object_size;
    memset(p_slab, 0, slab_size);
    hash_tab = (table_entry_t*)p_slab;
    entries_count = slab_size/sizeof(table_entry_t);
    values_count = 0;
    occupied_count = 0;
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::remove_singletons_and_redistribute_keys()
  {
    for (uint32_t tab_idx = 0; tab_idx < entries_count; tab_idx++) {
      if (!hash_tab[tab_idx].val.is_occupied()) {
        continue;
      }

      if (hash_tab[tab_idx].val.is_singleton()) {
        hash_tab[tab_idx].val.clear_flags();
        redistributed_clear++;
        continue;
      }

      const key_t &key = hash_tab[tab_idx].key;
      // This is an approximation only since size is stored in 4KB resolution
      uint64_t byte_size_approx = disk_blocks_to_byte_size(key.size_4k_units);
      if (!key.multipart_object() && (byte_size_approx <= head_object_size)) {
        hash_tab[tab_idx].val.clear_flags();
        redistributed_clear++;
        continue;
      }

      uint32_t key_idx = key.hash() % entries_count;
      if (key_idx != tab_idx) {
        uint64_t count = 1;
        redistributed_count++;
        uint32_t idx = key_idx;
        while (hash_tab[idx].val.is_occupied()   &&
               !hash_tab[idx].val.is_singleton() &&
               (hash_tab[idx].key != key)) {
          count++;
          idx = (idx + 1) % entries_count;
        }

        if (idx != tab_idx) {
          if (hash_tab[idx].val.is_occupied() && hash_tab[idx].val.is_singleton() ) {
            redistributed_clear++;
          }
          if (idx == key_idx) {
            redistributed_perfect++;
          }
          hash_tab[idx] = hash_tab[tab_idx];
          hash_tab[tab_idx].val.clear_flags();
        }
        else {
          redistributed_loopback++;
        }

        redistributed_search_max = std::max(redistributed_search_max, count);
        redistributed_search_total += count;
      }
      else {
        redistributed_not_needed++;
      }
    }
  }

  //---------------------------------------------------------------------------
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
  int dedup_table_t::add_entry(key_t *p_key,
                               disk_block_id_t block_id,
                               record_id_t rec_id,
                               bool shared_manifest)
  {
    value_t new_val(block_id, rec_id, shared_manifest);
    uint32_t idx = find_entry(p_key);
    value_t &val = hash_tab[idx].val;
    if (!val.is_occupied()) {
      if (occupied_count < entries_count) {
        occupied_count++;
      }
      else {
        return -EOVERFLOW;
      }

      hash_tab[idx].key = *p_key;
      hash_tab[idx].val = new_val;
      ldpp_dout(dpp, 20) << __func__ << "::add new entry" << dendl;
      ceph_assert(val.count == 1);
    }
    else {
      ceph_assert(hash_tab[idx].key == *p_key);
      val.count ++;
      if (!val.has_shared_manifest() && shared_manifest) {
        // replace value!
        ldpp_dout(dpp, 20) << __func__ << "::Replace with shared_manifest::["
                           << val.block_idx << "/" << (int)val.rec_id << "] -> ["
                           << block_id << "/" << (int)rec_id << "]" << dendl;
        new_val.count = val.count;
        hash_tab[idx].val = new_val;
      }
      ceph_assert(val.count > 1);
    }
    values_count++;
    ldpp_dout(dpp, 20) << __func__ << "::COUNT="<< val.count << dendl;
    return 0;
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::update_entry(key_t *p_key,
                                   disk_block_id_t block_id,
                                   record_id_t rec_id,
                                   bool shared_manifest)
  {
    uint32_t idx = find_entry(p_key);
    ceph_assert(hash_tab[idx].key == *p_key);
    value_t &val = hash_tab[idx].val;
    ceph_assert(val.is_occupied());
    // we only update non-singletons since we purge singletons after the first pass
    ceph_assert(val.count > 1);

    // need to overwrite the block_idx/rec_id from the first pass
    // unless already set with shared_manifest with the correct block-id/rec-id
    // We only set the shared_manifest flag on the second pass where we
    // got valid block-id/rec-id
    if (!val.has_shared_manifest()) {
      // replace value!
      value_t new_val(block_id, rec_id, shared_manifest);
      new_val.count = val.count;
      hash_tab[idx].val = new_val;
      ldpp_dout(dpp, 20) << __func__ << "::Replaced table entry::["
                         << val.block_idx << "/" << (int)val.rec_id << "] -> ["
                         << block_id << "/" << (int)rec_id << "]" << dendl;
    }
  }

  //---------------------------------------------------------------------------
  int dedup_table_t::set_shared_manifest_src_mode(const key_t *p_key,
                                                  disk_block_id_t block_id,
                                                  record_id_t rec_id)
  {
    uint32_t idx = find_entry(p_key);
    value_t &val = hash_tab[idx].val;
    if (val.is_occupied()) {
      if (val.block_idx == block_id && val.rec_id == rec_id) {
        val.set_shared_manifest_src();
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
    if (!val.is_occupied()) {
      return -ENOENT;
    }

    *p_val = val;
    return 0;
  }

  //---------------------------------------------------------------------------
  void dedup_table_t::count_duplicates(dedup_stats_t *p_small_objs,
                                       dedup_stats_t *p_big_objs,
                                       uint64_t *p_duplicate_head_bytes)
  {
    for (uint32_t tab_idx = 0; tab_idx < entries_count; tab_idx++) {
      if (!hash_tab[tab_idx].val.is_occupied()) {
        continue;
      }

      const key_t &key = hash_tab[tab_idx].key;
      // This is an approximation only since size is stored in 4KB resolution
      uint64_t byte_size_approx = disk_blocks_to_byte_size(key.size_4k_units);
      uint32_t duplicate_count = (hash_tab[tab_idx].val.count -1);

      // skip small single part objects which we can't dedup
      if (!key.multipart_object() && (byte_size_approx <= head_object_size)) {
        if (hash_tab[tab_idx].val.is_singleton()) {
          p_small_objs->singleton_count++;
        }
        else {
          p_small_objs->duplicate_count += duplicate_count;
          p_small_objs->unique_count ++;
          p_small_objs->dedup_bytes_estimate += (duplicate_count * byte_size_approx);
        }
        continue;
      }

      if (hash_tab[tab_idx].val.is_singleton()) {
        p_big_objs->singleton_count++;
      }
      else {
        ceph_assert(hash_tab[tab_idx].val.count > 1);
        uint64_t dup_bytes_approx = calc_deduped_bytes(head_object_size,
                                                       key.num_parts,
                                                       byte_size_approx);
        p_big_objs->dedup_bytes_estimate += (duplicate_count * dup_bytes_approx);
        p_big_objs->duplicate_count += duplicate_count;
        p_big_objs->unique_count ++;

        if (!key.multipart_object()) {
          // single part objects duplicate the head object when dedup is used
          uint64_t dup_head_bytes = duplicate_count * head_object_size;
          *p_duplicate_head_bytes += dup_head_bytes;
        }
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
