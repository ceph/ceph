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

#pragma once
#include <cstdint>
#include <cstddef>
#include <iterator>
#include "common/dout.h"
#include "rgw_dedup_store.h"
namespace rgw::dedup {

  // 24 Bytes key
  struct key_t {
    key_t() { ;}
    key_t(uint64_t _md5_high,
          uint64_t _md5_low,
          uint32_t _size_4k_units,
          uint16_t _num_parts,
          uint8_t  _stor_class_idx) {
      md5_high       = _md5_high;
      md5_low        = _md5_low;
      size_4k_units  = _size_4k_units;
      num_parts      = _num_parts;
      stor_class_idx = _stor_class_idx;
      pad8           = 0;
    }

    bool operator==(const struct key_t& other) const {
      return (memcmp(this, &other, sizeof(other)) == 0);
    }

    bool operator!=(const struct key_t& other) const {
      return !operator==(other);
    }

    uint64_t hash() const {
      // The MD5 is already a hashing function so no need for another hash
      return this->md5_low;
    }

    bool multipart_object() const {
      return num_parts > 0;
    }

    uint64_t md5_high;      // High Bytes of the Object Data MD5
    uint64_t md5_low;       // Low  Bytes of the Object Data MD5
    uint32_t size_4k_units; // Object size in 4KB units max out at 16TB (AWS MAX-SIZE is 5TB)
    uint16_t num_parts;     // How many parts were used in multipart upload (AWS MAX-PART is 10,000)
    uint8_t  stor_class_idx;// storage class id
    uint8_t  pad8;
  } __attribute__((__packed__));
  static_assert(sizeof(key_t) == 24);

  class dedup_table_t {
  public:
    // 8 Bytes Value
    struct value_t {
      value_t() {
        this->block_idx = 0xFFFFFFFF;
        this->count  = 0;
        this->rec_id = 0xFF;
        this->flags.clear();
      }

      value_t(disk_block_id_t block_id, record_id_t rec_id, bool shared_manifest) {
        this->block_idx = block_id;
        this->count  = 1;
        this->rec_id = rec_id;
        this->flags.clear();
        this->flags.set_occupied();
        if (shared_manifest) {
          flags.set_shared_manifest();
        }
      }

      inline void clear_flags() { flags.clear(); }
      inline bool has_shared_manifest() const {return flags.has_shared_manifest(); }
      inline void set_shared_manifest_src() { this->flags.set_shared_manifest(); }
      inline bool is_singleton() const { return (count == 1); }
      inline bool is_occupied() const { return flags.is_occupied(); }
      inline void set_occupied() { this->flags.set_occupied();  }
      inline void clear_occupied() { this->flags.clear_occupied(); }

      disk_block_id_t block_idx; // 32 bits
      uint16_t        count;     // 16 bits
      record_id_t     rec_id;    //  8 bits
      dedup_flags_t   flags;     //  8 bits
    } __attribute__((__packed__));
    static_assert(sizeof(value_t) == 8);

    dedup_table_t(const DoutPrefixProvider* _dpp,
                  uint32_t _head_object_size,
                  uint8_t *p_slab,
                  uint64_t slab_size);
    int add_entry(key_t *p_key, disk_block_id_t block_id, record_id_t rec_id,
                  bool shared_manifest);
    void update_entry(key_t *p_key, disk_block_id_t block_id, record_id_t rec_id,
                      bool shared_manifest);

    int  get_val(const key_t *p_key, struct value_t *p_val /*OUT*/);

    int set_shared_manifest_src_mode(const key_t *p_key,
                                     disk_block_id_t block_id,
                                     record_id_t rec_id);

    void count_duplicates(dedup_stats_t *p_small_objs_stat,
                          dedup_stats_t *p_big_objs_stat,
                          uint64_t *p_duplicate_head_bytes);

    void remove_singletons_and_redistribute_keys();
  private:
    // 32 Bytes unified entries
    struct table_entry_t {
      key_t key;
      value_t val;
    } __attribute__((__packed__));
    static_assert(sizeof(table_entry_t) == 32);

    uint32_t find_entry(const key_t *p_key) const;
    uint32_t       values_count = 0;
    uint32_t       entries_count = 0;
    uint32_t       occupied_count = 0;
    uint32_t       head_object_size = (4ULL * 1024 * 1024);
    table_entry_t *hash_tab = nullptr;

    // stat counters
    uint64_t redistributed_count = 0;
    uint64_t redistributed_search_total = 0;
    uint64_t redistributed_search_max = 0;
    uint64_t redistributed_loopback = 0;
    uint64_t redistributed_perfect = 0;
    uint64_t redistributed_clear = 0;
    uint64_t redistributed_not_needed = 0;
    const DoutPrefixProvider* dpp;
  };

} //namespace rgw::dedup
