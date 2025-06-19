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
  static constexpr const char* DEDUP_WATCH_OBJ = "DEDUP_WATCH_OBJ";
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

  struct worker_stats_t {
    worker_stats_t& operator +=(const worker_stats_t& other) {
      this->ingress_obj += other.ingress_obj;
      this->ingress_obj_bytes += other.ingress_obj_bytes;
      this->egress_records += other.egress_records;
      this->egress_blocks += other.egress_blocks;
      this->egress_slabs += other.egress_slabs;
      this->single_part_objs += other.single_part_objs;
      this->multipart_objs += other.multipart_objs;
      this->small_multipart_obj += other.small_multipart_obj;
      this->default_storage_class_objs += other.default_storage_class_objs;
      this->default_storage_class_objs_bytes += other.default_storage_class_objs_bytes;
      this->non_default_storage_class_objs += other.non_default_storage_class_objs;
      this->non_default_storage_class_objs_bytes += other.non_default_storage_class_objs_bytes;
      this->ingress_corrupted_etag += other.ingress_corrupted_etag;
      this->ingress_skip_too_small_bytes += other.ingress_skip_too_small_bytes;
      this->ingress_skip_too_small += other.ingress_skip_too_small;
      this->ingress_skip_too_small_64KB_bytes += other.ingress_skip_too_small_64KB_bytes;
      this->ingress_skip_too_small_64KB += other.ingress_skip_too_small_64KB;

      return *this;
    }

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

  inline void encode(const worker_stats_t& w, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(w.ingress_obj, bl);
    encode(w.ingress_obj_bytes, bl);
    encode(w.egress_records, bl);
    encode(w.egress_blocks, bl);
    encode(w.egress_slabs, bl);

    encode(w.single_part_objs, bl);
    encode(w.multipart_objs, bl);
    encode(w.small_multipart_obj, bl);

    encode(w.default_storage_class_objs, bl);
    encode(w.default_storage_class_objs_bytes, bl);
    encode(w.non_default_storage_class_objs, bl);
    encode(w.non_default_storage_class_objs_bytes, bl);

    encode(w.ingress_corrupted_etag, bl);

    encode(w.ingress_skip_too_small_bytes, bl);
    encode(w.ingress_skip_too_small, bl);

    encode(w.ingress_skip_too_small_64KB_bytes, bl);
    encode(w.ingress_skip_too_small_64KB, bl);

    encode(w.duration, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(worker_stats_t& w, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(w.ingress_obj, bl);
    decode(w.ingress_obj_bytes, bl);
    decode(w.egress_records, bl);
    decode(w.egress_blocks, bl);
    decode(w.egress_slabs, bl);
    decode(w.single_part_objs, bl);
    decode(w.multipart_objs, bl);
    decode(w.small_multipart_obj, bl);
    decode(w.default_storage_class_objs, bl);
    decode(w.default_storage_class_objs_bytes, bl);
    decode(w.non_default_storage_class_objs, bl);
    decode(w.non_default_storage_class_objs_bytes, bl);
    decode(w.ingress_corrupted_etag, bl);
    decode(w.ingress_skip_too_small_bytes, bl);
    decode(w.ingress_skip_too_small, bl);
    decode(w.ingress_skip_too_small_64KB_bytes, bl);
    decode(w.ingress_skip_too_small_64KB, bl);

    decode(w.duration, bl);
    DECODE_FINISH(bl);
  }

  struct md5_stats_t {
    md5_stats_t& operator +=(const md5_stats_t& other) {
      this->ingress_failed_load_bucket    += other.ingress_failed_load_bucket;
      this->ingress_failed_get_object     += other.ingress_failed_get_object;
      this->ingress_failed_get_obj_attrs  += other.ingress_failed_get_obj_attrs;
      this->ingress_corrupted_etag        += other.ingress_corrupted_etag;
      this->ingress_corrupted_obj_attrs   += other.ingress_corrupted_obj_attrs;
      this->ingress_skip_encrypted        += other.ingress_skip_encrypted;
      this->ingress_skip_encrypted_bytes  += other.ingress_skip_encrypted_bytes;
      this->ingress_skip_compressed       += other.ingress_skip_compressed;
      this->ingress_skip_compressed_bytes += other.ingress_skip_compressed_bytes;
      this->ingress_skip_changed_objs     += other.ingress_skip_changed_objs;
      this->shared_manifest_dedup_bytes   += other.shared_manifest_dedup_bytes;

      this->skipped_shared_manifest += other.skipped_shared_manifest;
      this->skipped_singleton       += other.skipped_singleton;
      this->skipped_singleton_bytes += other.skipped_singleton_bytes;
      this->skipped_source_record   += other.skipped_source_record;
      this->duplicate_records       += other.duplicate_records;
      this->size_mismatch           += other.size_mismatch;
      this->sha256_mismatch         += other.sha256_mismatch;
      this->failed_src_load         += other.failed_src_load;
      this->failed_rec_load         += other.failed_rec_load;
      this->failed_block_load       += other.failed_block_load;

      this->valid_sha256_attrs      += other.valid_sha256_attrs;
      this->invalid_sha256_attrs    += other.invalid_sha256_attrs;
      this->set_sha256_attrs        += other.set_sha256_attrs;
      this->skip_sha256_cmp         += other.skip_sha256_cmp;

      this->set_shared_manifest_src += other.set_shared_manifest_src;
      this->loaded_objects          += other.loaded_objects;
      this->processed_objects       += other.processed_objects;
      this->singleton_count         += other.singleton_count;
      this->duplicate_count         += other.duplicate_count;
      this->dedup_bytes_estimate    += other.dedup_bytes_estimate;
      this->unique_count            += other.unique_count;
      this->deduped_objects         += other.deduped_objects;
      this->deduped_objects_bytes   += other.deduped_objects_bytes;

      this->failed_dedup            += other.failed_dedup;
      this->failed_table_load       += other.failed_table_load;
      this->failed_map_overflow     += other.failed_map_overflow;
      return *this;
    }
    void dump(Formatter *f) const;

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
    uint64_t singleton_count = 0;
    uint64_t duplicate_count = 0;
    // counter is using on-disk size affected by block-size
    uint64_t dedup_bytes_estimate = 0;
    uint64_t unique_count = 0;
    uint64_t deduped_objects = 0;
    // counter is using s3 byte size disregarding the on-disk size affected by block-size
    uint64_t deduped_objects_bytes = 0;
    uint64_t failed_dedup = 0;
    uint64_t failed_table_load = 0;
    uint64_t failed_map_overflow = 0;
    utime_t  duration = {0, 0};
  };
  std::ostream &operator<<(std::ostream &out, const md5_stats_t &s);
  inline void encode(const md5_stats_t& m, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);

    encode(m.ingress_failed_load_bucket, bl);
    encode(m.ingress_failed_get_object, bl);
    encode(m.ingress_failed_get_obj_attrs, bl);
    encode(m.ingress_corrupted_etag, bl);
    encode(m.ingress_corrupted_obj_attrs, bl);
    encode(m.ingress_skip_encrypted, bl);
    encode(m.ingress_skip_encrypted_bytes, bl);
    encode(m.ingress_skip_compressed, bl);
    encode(m.ingress_skip_compressed_bytes, bl);
    encode(m.ingress_skip_changed_objs, bl);
    encode(m.shared_manifest_dedup_bytes, bl);

    encode(m.skipped_shared_manifest, bl);
    encode(m.skipped_singleton, bl);
    encode(m.skipped_singleton_bytes, bl);
    encode(m.skipped_source_record, bl);
    encode(m.duplicate_records, bl);
    encode(m.size_mismatch, bl);
    encode(m.sha256_mismatch, bl);
    encode(m.failed_src_load, bl);
    encode(m.failed_rec_load, bl);
    encode(m.failed_block_load, bl);

    encode(m.valid_sha256_attrs, bl);
    encode(m.invalid_sha256_attrs, bl);
    encode(m.set_sha256_attrs, bl);
    encode(m.skip_sha256_cmp, bl);
    encode(m.set_shared_manifest_src, bl);

    encode(m.loaded_objects, bl);
    encode(m.processed_objects, bl);
    encode(m.singleton_count, bl);
    encode(m.duplicate_count, bl);
    encode(m.dedup_bytes_estimate, bl);
    encode(m.unique_count, bl);
    encode(m.deduped_objects, bl);
    encode(m.deduped_objects_bytes, bl);
    encode(m.failed_dedup, bl);
    encode(m.failed_table_load, bl);
    encode(m.failed_map_overflow, bl);

    encode(m.duration, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(md5_stats_t& m, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(m.ingress_failed_load_bucket, bl);
    decode(m.ingress_failed_get_object, bl);
    decode(m.ingress_failed_get_obj_attrs, bl);
    decode(m.ingress_corrupted_etag, bl);
    decode(m.ingress_corrupted_obj_attrs, bl);
    decode(m.ingress_skip_encrypted, bl);
    decode(m.ingress_skip_encrypted_bytes, bl);
    decode(m.ingress_skip_compressed, bl);
    decode(m.ingress_skip_compressed_bytes, bl);
    decode(m.ingress_skip_changed_objs, bl);
    decode(m.shared_manifest_dedup_bytes, bl);

    decode(m.skipped_shared_manifest, bl);
    decode(m.skipped_singleton, bl);
    decode(m.skipped_singleton_bytes, bl);
    decode(m.skipped_source_record, bl);
    decode(m.duplicate_records, bl);
    decode(m.size_mismatch, bl);
    decode(m.sha256_mismatch, bl);
    decode(m.failed_src_load, bl);
    decode(m.failed_rec_load, bl);
    decode(m.failed_block_load, bl);

    decode(m.valid_sha256_attrs, bl);
    decode(m.invalid_sha256_attrs, bl);
    decode(m.set_sha256_attrs, bl);
    decode(m.skip_sha256_cmp, bl);
    decode(m.set_shared_manifest_src, bl);

    decode(m.loaded_objects, bl);
    decode(m.processed_objects, bl);
    decode(m.singleton_count, bl);
    decode(m.duplicate_count, bl);
    decode(m.dedup_bytes_estimate, bl);
    decode(m.unique_count, bl);
    decode(m.deduped_objects, bl);
    decode(m.deduped_objects_bytes, bl);
    decode(m.failed_dedup, bl);
    decode(m.failed_table_load, bl);
    decode(m.failed_map_overflow, bl);

    decode(m.duration, bl);
    DECODE_FINISH(bl);
  }

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
