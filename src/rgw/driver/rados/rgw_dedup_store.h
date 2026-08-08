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

#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_realm_reloader.h"
#include <string>
#include <unordered_map>
#include <vector>
#include <variant>
#include <iostream>
#include <ostream>
#include <cstring>
#include <string>
#include "include/rados/rados_types.hpp"
#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "rgw_dedup_utils.h"
#include "BLAKE3/c/blake3.h"

namespace rgw::dedup {
  struct key_t;
#define CEPHTOH_16 le16toh
#define CEPHTOH_32 le32toh
#define CEPHTOH_64 le64toh
#define HTOCEPH_16 htole16
#define HTOCEPH_32 htole32
#define HTOCEPH_64 htole64

  static constexpr unsigned HASH_UNITS = BLAKE3_OUT_LEN/sizeof(uint64_t);
  static constexpr unsigned DISK_BLOCK_SIZE  = 8*1024;
  // we use 16 bit offset
  static_assert(DISK_BLOCK_SIZE < 64*1024);
  static constexpr unsigned DISK_BLOCK_COUNT = 256;
  static_assert(DISK_BLOCK_COUNT <= (4*1024*1024/DISK_BLOCK_SIZE));
  static constexpr unsigned MAX_REC_IN_BLOCK = 32;
  // we use 8bit record indices
  static_assert(MAX_REC_IN_BLOCK < 0xFF);
  using slab_id_t      = uint32_t;
  using block_offset_t = uint16_t;
  using record_id_t    = uint8_t;

  // disk_rec_id_t is a 48-bit in-memory address pointing directly to a record.
  // Replaces the old 32-bit disk_block_id_t. The rec_id field (previously stored
  // separately in value_t) is now absorbed into the address.
  // This type is never serialized to disk/network; uses packed bit-fields.
  //
  // Layout (LSB to MSB on little-endian):
  // | work_shard | slab_id  | block_id | rec_id | rsv |
  // | 8 bits     | 24 bits  | 9 bits   | 6 bits | 1   |
  //
  // sizeof(disk_rec_id_t) == 6 bytes via __attribute__((packed)).
  struct __attribute__ ((packed)) disk_rec_id_t
  {
    work_shard_t  work_shard : 8;
    uint32_t      slab_id    : 24;
    uint16_t      block_id   : 9;
    uint8_t       rec_id     : 6;
    uint8_t       rsv        : 1;

    disk_rec_id_t()
      : work_shard(0), slab_id(0), block_id(0), rec_id(0), rsv(0) {}

    disk_rec_id_t(work_shard_t ws, uint32_t sid, uint16_t bid)
      : work_shard(ws), slab_id(sid), block_id(bid), rec_id(0), rsv(0) {
      ceph_assert(ws <= MAX_WORK_SHARD);
    }

    disk_rec_id_t(work_shard_t ws, uint32_t sid, uint16_t bid, record_id_t rid)
      : work_shard(ws), slab_id(sid), block_id(bid), rec_id(rid), rsv(0) {
      ceph_assert(ws <= MAX_WORK_SHARD);
      ceph_assert(rid < MAX_REC_IN_BLOCK);
    }

    inline bool operator ==(const disk_rec_id_t &other) const {
      return memcmp(this, &other, sizeof(*this)) == 0;
    }

    friend std::ostream& operator<<(std::ostream& os, const disk_rec_id_t& rec_id);

    std::string get_slab_name(md5_shard_t md5_shard) const;
    std::string get_coarse_slab_name(uint16_t group_id) const;

    inline void set_rec_id(record_id_t rid) {
      ceph_assert(rid < MAX_REC_IN_BLOCK);
      this->rec_id = rid;
    }
  };
  static_assert(sizeof(disk_rec_id_t) == 6);

  struct __attribute__ ((packed)) record_flags_t {
  private:
    static constexpr uint8_t RGW_RECORD_FLAG_HAS_VALID_HASH  = 0x01;
    static constexpr uint8_t RGW_RECORD_FLAG_SHARED_MANIFEST = 0x02;
    static constexpr uint8_t RGW_RECORD_FLAG_HASH_CALCULATED = 0x04;
    static constexpr uint8_t RGW_RECORD_FLAG_FASTLANE        = 0x08;
    static constexpr uint8_t RGW_RECORD_FLAG_SPLIT_HEAD      = 0x10;
    static constexpr uint8_t RGW_RECORD_FLAG_TAIL_REFTAG     = 0x20;
  public:
    record_flags_t() : flags(0) {}
    record_flags_t(uint8_t _flags) : flags(_flags) {}
    inline void clear() { this->flags = 0; }
    inline bool hash_calculated() const { return ((flags & RGW_RECORD_FLAG_HASH_CALCULATED) != 0); }
    inline void set_hash_calculated()  { flags |= RGW_RECORD_FLAG_HASH_CALCULATED; }
    inline void clear_hash_calculated()  { flags &= ~RGW_RECORD_FLAG_HASH_CALCULATED; }
    inline bool has_valid_hash() const { return ((flags & RGW_RECORD_FLAG_HAS_VALID_HASH) != 0); }
    inline void set_has_valid_hash()  { flags |= RGW_RECORD_FLAG_HAS_VALID_HASH; }
    inline bool has_shared_manifest() const { return ((flags & RGW_RECORD_FLAG_SHARED_MANIFEST) != 0); }
    inline void set_shared_manifest() { flags |= RGW_RECORD_FLAG_SHARED_MANIFEST; }
    inline bool is_fastlane()  const { return ((flags & RGW_RECORD_FLAG_FASTLANE) != 0); }
    inline void set_fastlane()  { flags |= RGW_RECORD_FLAG_FASTLANE; }
    inline bool is_split_head() const { return ((flags & RGW_RECORD_FLAG_SPLIT_HEAD) != 0); }
    inline void set_split_head() { flags |= RGW_RECORD_FLAG_SPLIT_HEAD; }
    inline bool is_ref_tag_from_tail() const { return ((flags & RGW_RECORD_FLAG_TAIL_REFTAG) != 0); }
    inline void set_ref_tag_from_tail() { flags |= RGW_RECORD_FLAG_TAIL_REFTAG; }
  private:
    uint8_t flags;
  };

  struct disk_record_t
  {
    disk_record_t(const char *buff);
    disk_record_t(const rgw::sal::Bucket *p_bucket,
                  const std::string      &obj_name,
                  const parsed_etag_t    *p_parsed_etag,
                  const std::string      &instance,
                  uint64_t                obj_size,
                  const std::string      &storage_class);
    disk_record_t() {}
    size_t serialize(char *buff) const;
    size_t length() const;
    int validate(const char *caller,
                 const DoutPrefixProvider* dpp,
                 disk_rec_id_t rec_addr,
                 record_id_t rec_id) const;
    inline bool multipart_object() { return (this->s.num_parts > 0); }
    struct packed_rec_t
    {
      uint64_t      hash[4];         // 4 * 8 Bytes of HASH
      uint64_t      shared_manifest; // 64bit hash of the SRC object manifest
      uint64_t      md5_high;        // High Bytes of the Object Data MD5
      uint64_t      md5_low;         // Low  Bytes of the Object Data MD5
      uint64_t      obj_bytes_size;

      uint16_t      num_parts;       // For multipart upload (AWS MAX-PART is 10,000)
      uint16_t      obj_name_len;
      uint16_t      bucket_name_len;
      uint16_t      bucket_id_len;

      uint16_t      tenant_name_len;
      uint16_t      instance_len;
      uint16_t      stor_class_len;
      uint16_t      ref_tag_len;
      uint16_t      manifest_len;

      uint8_t       rec_version;     // allows changing record format
      record_flags_t flags;           // 1 Byte flags
      uint8_t       pad[6];
    }s;
    std::string obj_name;
    // TBD: find pool name making it easier to get ioctx
    std::string bucket_name;
    std::string bucket_id;
    std::string tenant_name;
    std::string ref_tag;
    std::string instance;
    std::string stor_class;
    bufferlist  manifest_bl;
  };
  static_assert(BLAKE3_OUT_LEN == sizeof(disk_record_t::packed_rec_t::hash));
  static_assert(sizeof(disk_record_t::packed_rec_t) == sizeof(uint64_t)*12);
  std::ostream &operator<<(std::ostream &stream, const disk_record_t & rec);

  static constexpr unsigned BLOCK_MAGIC = 0xFACE;
  static constexpr unsigned LAST_BLOCK_MAGIC = 0xCAD7;
  struct  __attribute__ ((packed)) disk_block_header_t {
    void deserialize();
    int verify(uint16_t expected_block_idx, const DoutPrefixProvider* dpp);
    uint16_t        offset;
    uint16_t        rec_count;
    uint16_t        block_idx;
    uint16_t        rec_offsets[MAX_REC_IN_BLOCK];
  };
  static constexpr unsigned MAX_REC_SIZE = (DISK_BLOCK_SIZE - sizeof(disk_block_header_t));

  struct  __attribute__ ((packed)) disk_block_t
  {
    const disk_block_header_t* get_header() const { return (disk_block_header_t*)data; }
    disk_block_header_t* get_header() { return (disk_block_header_t*)data; }
    bool is_empty() const { return (get_header()->rec_count == 0); }

    void init(uint16_t block_idx);
    record_id_t add_record(const disk_record_t *p_rec, const DoutPrefixProvider *dpp);
    void close_block(const DoutPrefixProvider* dpp, bool has_more);
    char data[DISK_BLOCK_SIZE];
  };

  int load_record(librados::IoCtx          &ioctx,
                  const disk_record_t      *p_tgt_rec,
                  disk_record_t            *p_src_rec, /* OUT */
                  disk_rec_id_t             rec_addr,
                  md5_shard_t               md5_shard,
                  const DoutPrefixProvider *dpp);

  int load_slab(librados::IoCtx &ioctx,
                bufferlist &bl,
                shard_t shard,
                work_shard_t worker_id,
                uint32_t slab_id,
                const DoutPrefixProvider* dpp,
                bool is_coarse);

  int store_slab(librados::IoCtx &ioctx,
                 bufferlist &bl,
                 shard_t shard,
                 work_shard_t worker_id,
                 uint32_t slab_id,
                 const DoutPrefixProvider* dpp,
                 bool is_coarse);

  //---------------------------------------------------------------------------
  // Forward-only iterator over all records in a slab sequence.
  // Encapsulates the slab→block→record traversal with robust error handling
  // (retry on slab load failure, skip bad blocks) and detailed logging.
  // Does NOT validate records or handle heartbeat/pause/stop — caller's responsibility.
  class slab_record_iterator_t
  {
  public:
    struct record_ref_t {
      disk_record_t  rec;        // deserialized record in host format
      disk_rec_id_t  rec_addr;   // full address: (worker_id, slab_id, block_id, rec_id)
    };

    slab_record_iterator_t(const DoutPrefixProvider *dpp,
                           librados::IoCtx &ioctx,
                           uint32_t shard_or_group,
                           work_shard_t worker_id,
                           bool is_coarse);

    // Advances to the next record.
    // Returns true if a record is available (access via operator*).
    // Returns false on EOF or error (distinguish via error()).
    bool next();

    // Current record — valid only after next() returns true
    record_ref_t& operator*() { return d_ref; }
    record_ref_t* operator->() { return &d_ref; }

    // 0 if iteration ended normally (EOF), negative on fatal error
    int error() const { return d_error; }

    uint32_t slab_count() const { return d_slab_count; }
    uint32_t failed_block_count() const { return d_failed_block_count; }
    uint32_t missing_last_block_marker_count() const { return d_missing_last_block_marker; }

  private:
    bool assign_next_record();
    bool check_for_more_blocks();
    bool open_next_block();
    bool load_next_slab();

    record_ref_t               d_ref;
    int                        d_error               = 0;

    const DoutPrefixProvider  *dpp;
    librados::IoCtx           &d_ioctx;
    uint32_t                   d_shard_or_group;
    work_shard_t               d_worker_id;
    bool                       d_is_coarse;

    // slab-level state
    bufferlist                 d_bl;
    uint32_t                   d_slab_id             = 0;
    uint32_t                   d_slab_count          = 0;
    int                        d_slab_failure_count  = 0;
    bool                       d_has_more_slabs      = true;

    // block-level state
    char                       d_block_buff[sizeof(disk_block_t)];
    bufferlist::const_iterator d_bl_itr;
    disk_block_header_t       *d_p_header            = nullptr;
    const char                *d_p_block             = nullptr;
    uint16_t                   d_block_num           = 0;
    int                        d_block_failure_count = 0;
    bool                       d_has_more_blocks     = false;

    // record-level state within current block
    unsigned                   d_rec_idx             = 0;

    // stats
    uint32_t                   d_failed_block_count  = 0;
    uint32_t                   d_missing_last_block_marker = 0;

    static constexpr int MAX_OBJ_LOAD_FAILURE = 3;
    static constexpr int MAX_BAD_BLOCKS = 2;
  };

  class disk_block_array_t;
  class disk_block_seq_t
  {
    friend class disk_block_array_t;
  public:
    disk_block_seq_t(const DoutPrefixProvider* dpp_in,
                     disk_block_t *p_arr_in,
                     work_shard_t worker_id,
                     md5_shard_t md5_shard,
                     worker_stats_t *p_stats_in);
    int flush_disk_records(librados::IoCtx &ioctx);
    md5_shard_t get_md5_shard() { return d_md5_shard; }
    int add_record(librados::IoCtx     &ioctx,
                   const disk_record_t *p_rec,
                   disk_rec_id_t       *p_rec_addr); // OUT
    disk_block_seq_t() {;}

  private:
    void activate(const DoutPrefixProvider* _dpp,
                  disk_block_t *_p_arr,
                  work_shard_t worker_id,
                  md5_shard_t md5_shard,
                  worker_stats_t *p_stats,
                  bool coarse);
    inline const disk_block_t* last_block() { return &p_arr[DISK_BLOCK_COUNT-1]; }
    int flush(librados::IoCtx &ioctx);
    void slab_reset() {
      d_block_id = 0;
      p_curr_block = p_arr;
      p_curr_block->init(d_block_id);
    }

    disk_block_t   *p_arr         = nullptr;
    disk_block_t   *p_curr_block  = nullptr;
    worker_stats_t *p_stats       = nullptr;
    const DoutPrefixProvider *dpp = nullptr;
    uint32_t        d_slab_id     = 0;
    uint16_t        d_block_id    = 0;
    work_shard_t    d_worker_id   = NULL_WORK_SHARD;
    md5_shard_t     d_md5_shard   = NULL_MD5_SHARD;
    bool            d_coarse      = false;
  };

  class disk_block_array_t
  {
  public:

    // Single-pass: num_buffers = num_md5_shards, route by md5_low % N

    // Phase 1 (coarse): num_buffers = G = ceil(sqrt(N)),
    //                   route by md5_shard / shards_per_group → group_id
    // Phase 2 (fine):   num_buffers = shards_per_group = ceil(N/G),
    //                   route by md5_shard - group_id * shards_per_group → buffer_idx
    enum class mode_t { SINGLE_PASS, PHASE1_COARSE, PHASE2_FINE };
    disk_block_array_t(const DoutPrefixProvider* _dpp,
                       uint8_t *raw_mem,
                       uint64_t raw_mem_size,
                       work_shard_t worker_id,
                       worker_stats_t *p_worker_stats,
                       md5_shard_t num_md5_shards,
                       uint32_t num_groups = 0,
                       uint32_t shards_per_group = 0,
                       uint32_t group_id = 0,
                       mode_t mode = mode_t::SINGLE_PASS);

    void flush_output_buffers(const DoutPrefixProvider* dpp,
                              librados::IoCtx &ioctx);
    disk_block_seq_t* get_shard_block_seq(uint64_t md5_low) {
      md5_shard_t md5_shard = md5_low % d_num_md5_shards;
      uint32_t idx;
      switch (d_mode) {
      case mode_t::PHASE1_COARSE:
        idx = md5_shard / d_shards_per_group;
        break;
      case mode_t::PHASE2_FINE:
        idx = md5_shard - d_group_id * d_shards_per_group;
        break;
      default: // SINGLE_PASS
        idx = md5_shard;
        break;
      }
      ceph_assert(idx < d_disk_arr.size());
      return &d_disk_arr[idx];
    }

    //private:
    std::vector<disk_block_seq_t> d_disk_arr;
    work_shard_t      d_worker_id;
    md5_shard_t       d_num_md5_shards;
    uint32_t          d_shards_per_group = 0;
    uint32_t          d_group_id         = 0;
    mode_t            d_mode             = mode_t::SINGLE_PASS;
  };
} //namespace rgw::dedup
