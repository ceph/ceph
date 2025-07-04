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
#include <string>
#include "include/rados/buffer.h"
#include "include/encoding.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include <time.h>
#include "include/utime.h"
#include "include/encoding.h"
#include "common/dout.h"

//#define FULL_DEDUP_SUPPORT
namespace rgw::dedup {
  using work_shard_t   = uint16_t;
  using md5_shard_t    = uint16_t;

  // settings to help debug small systems
  const work_shard_t MIN_WORK_SHARD = 2;
  const md5_shard_t  MIN_MD5_SHARD  = 4;

  // Those are the correct values for production system
  const work_shard_t MAX_WORK_SHARD = 255;
  const md5_shard_t  MAX_MD5_SHARD  = 512;

  const work_shard_t NULL_WORK_SHARD = 0xFFFF;
  const md5_shard_t  NULL_MD5_SHARD  = 0xFFFF;
  const unsigned     NULL_SHARD      = 0xFFFF;

  // work_shard  is an 8 bits int with 255 legal values for the first iteration
  // and one value (0xFF) reserved for second iteration
  const unsigned     WORK_SHARD_HARD_LIMIT = 0x0FF;
  // md5_shard_t is a 12 bits int with 4096 possible values
  const unsigned     MD5_SHARD_HARD_LIMIT  = 0xFFF;

  static_assert(MAX_WORK_SHARD < NULL_WORK_SHARD);
  static_assert(MAX_WORK_SHARD < NULL_SHARD);
  static_assert(MAX_WORK_SHARD <= WORK_SHARD_HARD_LIMIT);
  static_assert(MAX_MD5_SHARD  < NULL_MD5_SHARD);
  static_assert(MAX_MD5_SHARD  < NULL_SHARD);
  static_assert(MAX_MD5_SHARD  <= MD5_SHARD_HARD_LIMIT);

  //---------------------------------------------------------------------------
  enum dedup_req_type_t {
    DEDUP_TYPE_NONE     = 0,
    DEDUP_TYPE_ESTIMATE = 1,
    DEDUP_TYPE_FULL     = 2
  };

  std::ostream& operator<<(std::ostream &out, const dedup_req_type_t& dedup_type);
  struct __attribute__ ((packed)) dedup_flags_t {
  private:
    static constexpr uint8_t RGW_DEDUP_FLAG_SHA256_CALCULATED = 0x01; // REC
    static constexpr uint8_t RGW_DEDUP_FLAG_SHARED_MANIFEST   = 0x02; // REC + TAB
    static constexpr uint8_t RGW_DEDUP_FLAG_OCCUPIED          = 0x04; // TAB
    static constexpr uint8_t RGW_DEDUP_FLAG_FASTLANE          = 0x08; // REC

  public:
    dedup_flags_t() : flags(0) {}
    dedup_flags_t(uint8_t _flags) : flags(_flags) {}
    inline void clear() { this->flags = 0; }
    inline bool sha256_calculated() const { return ((flags & RGW_DEDUP_FLAG_SHA256_CALCULATED) != 0); }
    inline void set_sha256_calculated()  { flags |= RGW_DEDUP_FLAG_SHA256_CALCULATED; }
    inline bool has_shared_manifest() const { return ((flags & RGW_DEDUP_FLAG_SHARED_MANIFEST) != 0); }
    inline void set_shared_manifest() { flags |= RGW_DEDUP_FLAG_SHARED_MANIFEST; }
    inline bool is_occupied() const {return ((this->flags & RGW_DEDUP_FLAG_OCCUPIED) != 0); }
    inline void set_occupied() {this->flags |= RGW_DEDUP_FLAG_OCCUPIED; }
    inline void clear_occupied() { this->flags &= ~RGW_DEDUP_FLAG_OCCUPIED; }
    inline bool is_fastlane()  const { return ((flags & RGW_DEDUP_FLAG_FASTLANE) != 0); }
    inline void set_fastlane()  { flags |= RGW_DEDUP_FLAG_FASTLANE; }
  private:
    uint8_t flags;
  };

  struct dedup_stats_t {
    dedup_stats_t& operator+=(const dedup_stats_t& other);

    uint64_t singleton_count = 0;
    uint64_t unique_count = 0;
    uint64_t duplicate_count = 0;
    uint64_t dedup_bytes_estimate = 0;
  };

  std::ostream& operator<<(std::ostream &out, const dedup_stats_t& stats);
  void encode(const dedup_stats_t& ds, ceph::bufferlist& bl);
  void decode(dedup_stats_t& ds, ceph::bufferlist::const_iterator& bl);

  struct worker_stats_t {
    worker_stats_t& operator +=(const worker_stats_t& other);
    void dump(Formatter *f) const;

    uint64_t ingress_obj = 0;
    uint64_t ingress_obj_bytes = 0;
    uint64_t egress_records = 0;
    uint64_t egress_blocks = 0;
    uint64_t egress_slabs = 0;

    uint64_t single_part_objs = 0;
    uint64_t multipart_objs = 0;
    uint64_t small_multipart_obj = 0;

    uint64_t default_storage_class_objs = 0;
    uint64_t default_storage_class_objs_bytes = 0;

    uint64_t non_default_storage_class_objs = 0;
    uint64_t non_default_storage_class_objs_bytes = 0;

    uint64_t ingress_corrupted_etag = 0;

    uint64_t ingress_skip_too_small_bytes = 0;
    uint64_t ingress_skip_too_small = 0;

    uint64_t ingress_skip_too_small_64KB_bytes = 0;
    uint64_t ingress_skip_too_small_64KB = 0;

    utime_t  duration = {0, 0};
  };
  std::ostream& operator<<(std::ostream &out, const worker_stats_t &s);
  void encode(const worker_stats_t& w, ceph::bufferlist& bl);
  void decode(worker_stats_t& w, ceph::bufferlist::const_iterator& bl);


  struct md5_stats_t {
    md5_stats_t& operator +=(const md5_stats_t& other);
    void dump(Formatter *f) const;

    dedup_stats_t small_objs_stat;
    dedup_stats_t big_objs_stat;
    uint64_t ingress_failed_load_bucket = 0;
    uint64_t ingress_failed_get_object = 0;
    uint64_t ingress_failed_get_obj_attrs = 0;
    uint64_t ingress_corrupted_etag = 0;
    uint64_t ingress_corrupted_obj_attrs = 0;
    uint64_t ingress_skip_encrypted = 0;
    uint64_t ingress_skip_encrypted_bytes = 0;
    uint64_t ingress_skip_compressed = 0;
    uint64_t ingress_skip_compressed_bytes = 0;
    uint64_t ingress_skip_changed_objs = 0;

    uint64_t shared_manifest_dedup_bytes = 0;
    uint64_t skipped_shared_manifest = 0;
    uint64_t skipped_purged_small = 0;
    uint64_t skipped_singleton = 0;
    uint64_t skipped_singleton_bytes = 0;
    uint64_t skipped_source_record = 0;
    uint64_t duplicate_records = 0;
    uint64_t size_mismatch = 0;
    uint64_t sha256_mismatch = 0;
    uint64_t failed_src_load = 0;
    uint64_t failed_rec_load = 0;
    uint64_t failed_block_load = 0;

    uint64_t valid_sha256_attrs = 0;
    uint64_t invalid_sha256_attrs = 0;
    uint64_t set_sha256_attrs = 0;
    uint64_t skip_sha256_cmp = 0;

    uint64_t set_shared_manifest_src = 0;
    uint64_t loaded_objects = 0;
    uint64_t processed_objects = 0;
    // counter is using on-disk size affected by block-size
    uint64_t dup_head_bytes_estimate = 0; //duplicate_head_bytes
    uint64_t deduped_objects = 0;
    // counter is using s3 byte size disregarding the on-disk size affected by block-size
    uint64_t deduped_objects_bytes = 0;
    uint64_t dup_head_bytes = 0;
    uint64_t failed_dedup = 0;
    uint64_t failed_table_load = 0;
    uint64_t failed_map_overflow = 0;
    utime_t  duration = {0, 0};
  };
  std::ostream &operator<<(std::ostream &out, const md5_stats_t &s);
  void encode(const md5_stats_t& m, ceph::bufferlist& bl);
  void decode(md5_stats_t& m, ceph::bufferlist::const_iterator& bl);

  struct parsed_etag_t {
    uint64_t md5_high;  // High Bytes of the Object Data MD5
    uint64_t md5_low;   // Low  Bytes of the Object Data MD5
    uint16_t num_parts; // How many parts were used in multipart upload
                        // Setting num_parts to zero when multipart is not used
  };

#define DIV_UP(a, b) ( ((a)+(b-1)) / b)
  // CEPH min allocation unit on disk is 4KB
  // TBD: take from config
  static constexpr uint64_t DISK_ALLOC_SIZE = 4*1024;
  // 16 bytes hexstring  -> 8 Byte uint64_t
  static inline constexpr unsigned HEX_UNIT_SIZE = 16;

  //---------------------------------------------------------------------------
  static inline uint64_t byte_size_to_disk_blocks(uint64_t byte_size) {
    return DIV_UP(byte_size, DISK_ALLOC_SIZE);
  }

  //---------------------------------------------------------------------------
  static inline uint64_t disk_blocks_to_byte_size(uint64_t disk_blocks) {
    return disk_blocks * DISK_ALLOC_SIZE;
  }

  //---------------------------------------------------------------------------
  // ceph store full blocks so need to round up and multiply by block_size
  static inline uint64_t calc_on_disk_byte_size(uint64_t byte_size) {
    uint64_t size_4k_units = byte_size_to_disk_blocks(byte_size);
    return disk_blocks_to_byte_size(size_4k_units);
  }

  enum urgent_msg_t {
    URGENT_MSG_NONE    = 0,
    URGENT_MSG_ABORT   = 1,
    URGENT_MSG_PASUE   = 2,
    URGENT_MSG_RESUME  = 3,
    URGENT_MSG_RESTART = 4,
    URGENT_MSG_INVALID = 5
  };

  const char* get_urgent_msg_names(int msg);
  bool hex2int(const char *p, const char *p_end, uint64_t *p_val);
  bool parse_etag_string(const std::string& etag, parsed_etag_t *parsed_etag);
  void etag_to_bufferlist(uint64_t md5_high, uint64_t md5_low, uint16_t num_parts,
                          ceph::bufferlist *bl);
  const char* get_next_data_ptr(bufferlist::const_iterator &bl_itr,
                                char data_buff[],
                                size_t len,
                                const DoutPrefixProvider* dpp);

  //---------------------------------------------------------------------------
  static inline void build_oid(const std::string &bucket_id,
                               const std::string &obj_name,
                               std::string *oid)
  {
    *oid = bucket_id + "_" + obj_name;
  }

  //---------------------------------------------------------------------------
  static inline uint64_t calc_deduped_bytes(uint64_t head_obj_size,
                                            uint16_t num_parts,
                                            uint64_t size_bytes)
  {
    if (num_parts > 0) {
      // multipart objects with an empty head i.e. we achive full dedup
      return size_bytes;
    }
    else {
      // reduce the head size
      if (size_bytes > head_obj_size) {
        return size_bytes - head_obj_size;
      }
      else {
        return 0;
      }
    }
  }

} //namespace rgw::dedup
