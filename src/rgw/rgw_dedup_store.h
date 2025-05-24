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
#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_realm_reloader.h"
#include <string>
#include <unordered_map>
#include <variant>
#include <iostream>
#include <ostream>
#include <cstring>
#include <string>
#include "include/rados/rados_types.hpp"
#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "rgw_dedup_utils.h"

namespace rgw::dedup {
  struct key_t;
#define CEPHTOH_16 le16toh
#define CEPHTOH_32 le32toh
#define CEPHTOH_64 le64toh
#define HTOCEPH_16 htole16
#define HTOCEPH_32 htole32
#define HTOCEPH_64 htole64

  static inline constexpr unsigned DISK_BLOCK_SIZE  = 8*1024;
  // we use 16 bit offset
  static_assert(DISK_BLOCK_SIZE < 64*1024);
  static constexpr unsigned DISK_BLOCK_COUNT = 256;
  static_assert(DISK_BLOCK_COUNT <= (4*1024*1024/DISK_BLOCK_SIZE));
  static constexpr unsigned MAX_REC_IN_BLOCK = 32;
  // we use 8bit record indices
  static_assert(MAX_REC_IN_BLOCK < 0xFF);
  using slab_id_t      = uint16_t;
  using block_offset_t = uint8_t;
  using record_id_t    = uint8_t;

  // disk_block_id_t is a 32 bits concataion of shard_id, slab_id and block_off
  // ---8---- | -------16------- | ---8----
  // shard_id |      slab_id     | block_off
  struct __attribute__ ((packed)) disk_block_id_t
  {
  public:
    disk_block_id_t() {
      block_id = 0;
    }

    disk_block_id_t(work_shard_t shard_id, uint32_t seq_number) {
      ceph_assert((seq_number & SEQ_NUMBER_MASK) == seq_number);
      ceph_assert(shard_id <= MAX_WORK_SHARD);
      block_id = (uint32_t)shard_id << OBJ_SHARD_SHIFT | seq_number;
    }

    disk_block_id_t& operator =(const disk_block_id_t &other) {
      this->block_id = other.block_id;
      return *this;
    }

    inline disk_block_id_t& operator =(uint32_t val) {
      this->block_id = val;
      return *this;
    }

    inline bool operator ==(const disk_block_id_t &other) const {
      return (this->block_id == other.block_id);
    }

    inline explicit operator uint32_t() const {
      return this->block_id;
    }

    friend std::ostream& operator<<(std::ostream& os, const disk_block_id_t& block_id);

    std::string get_slab_name(md5_shard_t md5_shard) const;

    static inline slab_id_t seq_num_to_slab_id(uint32_t seq_number) {
      return (seq_number & SLAB_ID_MASK) >> SLAB_ID_SHIFT;
    }

    static inline uint32_t slab_id_to_seq_num(uint32_t slab_id) {
      return (slab_id << SLAB_ID_SHIFT);
    }

    inline block_offset_t get_block_offset() const {
      return get_block_offset(get_seq_num());
    }

    inline work_shard_t get_work_shard_id() const {
      return (block_id & OBJ_SHARD_MASK) >> OBJ_SHARD_SHIFT;
    }

  private:
    inline uint32_t get_seq_num() const {
      return (block_id & SEQ_NUMBER_MASK);
    }

    inline slab_id_t get_slab_id() const {
      return seq_num_to_slab_id(get_seq_num());
    }

    inline block_offset_t get_block_offset(uint32_t seq_number) const {
      return (seq_number & BLOCK_OFF_MASK);
    }

    static constexpr uint32_t OBJ_SHARD_SHIFT  = 24;
    static constexpr uint32_t OBJ_SHARD_MASK   = 0xFF000000;

    static constexpr uint32_t SEQ_NUMBER_SHIFT = 0;
    static constexpr uint32_t SEQ_NUMBER_MASK  = 0x00FFFFFF;

    static constexpr uint32_t SLAB_ID_SHIFT    = 8;
    static constexpr uint32_t SLAB_ID_MASK     = 0x00FFFF00;

    static constexpr uint32_t BLOCK_OFF_SHIFT  = 0;
    static constexpr uint32_t BLOCK_OFF_MASK   = 0x000000FF;

    uint32_t block_id;
  };

  struct disk_record_t
  {
    disk_record_t(const char *buff);
    disk_record_t(const rgw::sal::Bucket *p_bucket,
                  const std::string      &obj_name,
                  const parsed_etag_t    *p_parsed_etag,
                  uint64_t                obj_size,
                  const std::string      &storage_class);
    disk_record_t() {}
    size_t serialize(char *buff) const;
    size_t length() const;
    int validate(const char *caller,
                 const DoutPrefixProvider* dpp,
                 disk_block_id_t block_id,
                 record_id_t rec_id) const;
    inline bool has_shared_manifest() const { return s.flags.has_shared_manifest(); }
    inline void set_shared_manifest() { s.flags.set_shared_manifest(); }

    struct __attribute__ ((packed)) packed_rec_t
    {
      uint8_t       rec_version;     // allows changing record format
      dedup_flags_t flags;           // 1 Byte flags
      uint16_t      num_parts;       // For multipart upload (AWS MAX-PART is 10,000)
      uint16_t      obj_name_len;
      uint16_t      bucket_name_len;

      uint64_t      md5_high;        // High Bytes of the Object Data MD5
      uint64_t      md5_low;         // Low  Bytes of the Object Data MD5
      uint64_t      obj_bytes_size;
      uint64_t      object_version;

      uint16_t      bucket_id_len;
      uint16_t      tenant_name_len;
      uint16_t      stor_class_len;
      uint16_t      ref_tag_len;

      uint16_t      manifest_len;
      uint8_t       pad[6];

      uint64_t      shared_manifest; // 64bit hash of the SRC object manifest
      uint64_t      sha256[4];       // 4 * 8 Bytes of SHA256
    }s;
    std::string obj_name;
    // TBD: find pool name making it easier to get ioctx
    std::string bucket_name;
    std::string bucket_id;
    std::string tenant_name;
    std::string ref_tag;
    std::string stor_class;
    bufferlist  manifest_bl;
  };

  std::ostream &operator<<(std::ostream &stream, const disk_record_t & rec);

  static constexpr unsigned BLOCK_MAGIC = 0xFACE;
  static constexpr unsigned LAST_BLOCK_MAGIC = 0xCAD7;
  struct  __attribute__ ((packed)) disk_block_header_t {
    void deserialize();
    int verify(disk_block_id_t block_id, const DoutPrefixProvider* dpp);
    uint16_t        offset;
    uint16_t        rec_count;
    disk_block_id_t block_id;
    uint16_t        rec_offsets[MAX_REC_IN_BLOCK];
  };
  static constexpr unsigned MAX_REC_SIZE = (DISK_BLOCK_SIZE - sizeof(disk_block_header_t));

  struct  __attribute__ ((packed)) disk_block_t
  {
    const disk_block_header_t* get_header() const { return (disk_block_header_t*)data; }
    disk_block_header_t* get_header() { return (disk_block_header_t*)data; }
    bool is_empty() const { return (get_header()->rec_count == 0); }

    void init(work_shard_t worker_id, uint32_t seq_number);
    record_id_t add_record(const disk_record_t *p_rec, const DoutPrefixProvider *dpp);
    void close_block(const DoutPrefixProvider* dpp, bool has_more);
    disk_block_id_t get_block_id() {
      disk_block_header_t *p_header = get_header();
      return p_header->block_id;
    }
    char data[DISK_BLOCK_SIZE];
  };

  int load_record(librados::IoCtx          &ioctx,
                  const disk_record_t      *p_tgt_rec,
                  disk_record_t            *p_src_rec, /* OUT */
                  disk_block_id_t           block_id,
                  record_id_t               rec_id,
                  md5_shard_t               md5_shard,
                  const DoutPrefixProvider *dpp);

  int load_slab(librados::IoCtx &ioctx,
                bufferlist &bl,
                md5_shard_t md5_shard,
                work_shard_t worker_id,
                uint32_t seq_number,
                const DoutPrefixProvider* dpp);

  int store_slab(librados::IoCtx &ioctx,
                 bufferlist &bl,
                 md5_shard_t md5_shard,
                 work_shard_t worker_id,
                 uint32_t seq_number,
                 const DoutPrefixProvider* dpp);

  class disk_block_array_t;
  class disk_block_seq_t
  {
    friend class disk_block_array_t;
  public:
    struct record_info_t {
      disk_block_id_t block_id;
      record_id_t     rec_id;
    };

    disk_block_seq_t(const DoutPrefixProvider* dpp_in,
                     disk_block_t *p_arr_in,
                     work_shard_t worker_id,
                     md5_shard_t md5_shard,
                     worker_stats_t *p_stats_in);
    int flush_disk_records(librados::IoCtx &ioctx);
    md5_shard_t get_md5_shard() { return d_md5_shard; }
    int add_record(librados::IoCtx     &ioctx,
                   const disk_record_t *p_rec, // IN-OUT
                   record_info_t       *p_rec_info); // OUT-PARAM

  private:
    disk_block_seq_t() {;}
    void activate(const DoutPrefixProvider* _dpp,
                  disk_block_t *_p_arr,
                  work_shard_t worker_id,
                  md5_shard_t md5_shard,
                  worker_stats_t *p_stats);
    inline const disk_block_t* last_block() { return &p_arr[DISK_BLOCK_COUNT-1]; }
    int flush(librados::IoCtx &ioctx);
    void slab_reset() {
      p_curr_block = p_arr;
      p_curr_block->init(d_worker_id, d_seq_number);
    }

    disk_block_t   *p_arr         = nullptr;
    disk_block_t   *p_curr_block  = nullptr;
    worker_stats_t *p_stats       = nullptr;
    const DoutPrefixProvider *dpp = nullptr;
    uint32_t        d_seq_number  = 0;
    work_shard_t    d_worker_id   = NULL_WORK_SHARD;
    md5_shard_t     d_md5_shard   = NULL_MD5_SHARD;
  };

  class disk_block_array_t
  {
  public:
    disk_block_array_t(const DoutPrefixProvider* _dpp,
                       uint8_t *raw_mem,
                       uint64_t raw_mem_size,
                       work_shard_t worker_id,
                       worker_stats_t *p_worker_stats,
                       md5_shard_t num_md5_shards);
    void flush_output_buffers(const DoutPrefixProvider* dpp,
                              librados::IoCtx &ioctx);
    disk_block_seq_t* get_shard_block_seq(uint64_t md5_low) {
      md5_shard_t md5_shard = md5_low % d_num_md5_shards;
      return d_disk_arr + md5_shard;
    }

    //private:
    disk_block_seq_t  d_disk_arr[MAX_MD5_SHARD];
    work_shard_t      d_worker_id;
    md5_shard_t       d_num_md5_shards;
  };
} //namespace rgw::dedup
