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
  //static inline constexpr unsigned DISK_BLOCK_COUNT = std::min(((2*1024*1024)/DISK_BLOCK_SIZE), 256);
  static inline constexpr unsigned DISK_BLOCK_COUNT = 256;

  using slab_id_t      = uint16_t;
  using block_offset_t = uint8_t;
  using record_id_t    = uint8_t;

  static constexpr work_shard_t NULL_WORK_SHARD = 0xFF;
  static constexpr md5_shard_t  NULL_MD5_SHARD  = 0xFF;
  static constexpr unsigned     NULL_SHARD      = 0xFF;
  struct __attribute__ ((packed)) disk_block_id_t
  {
  public:
    disk_block_id_t() {
      block_id = 0;
    }

    disk_block_id_t(work_shard_t work_shard_id, uint32_t seq_number/*, record_id_t rec_id = 0*/) {
      ceph_assert((SEQ_NUMBER_MASK & seq_number) == seq_number);
      block_id = (uint32_t)work_shard_id << OBJ_SHARD_SHIFT | seq_number;
    }

    disk_block_id_t& operator =(const disk_block_id_t &other) {
      this->block_id = other.block_id;
      return *this;
    }

    inline disk_block_id_t& operator =(uint32_t val) {
      this->block_id = val;
      return *this;
    }

    inline bool operator ==(const disk_block_id_t &other) {
      return (this->block_id == other.block_id);
    }

    inline explicit operator uint32_t() const {
      return this->block_id;
    }

    friend std::ostream& operator<<(std::ostream& os, const disk_block_id_t& block_id);

    std::string get_slab_name(md5_shard_t md5_shard) const;

    inline slab_id_t get_slab_id(uint32_t seq_number) const {
      return (seq_number & SLAB_ID_MASK) >> SLAB_ID_SHIFT;
    }

    inline block_offset_t get_block_offset() const {
      return get_block_offset(get_seq_num());
    }

  private:
    inline work_shard_t get_work_shard_id() const {
      return (block_id & OBJ_SHARD_MASK) >> OBJ_SHARD_SHIFT;
    }

    inline uint32_t get_seq_num() const {
      return (block_id & SEQ_NUMBER_MASK);
    }

    inline slab_id_t get_slab_id() const {
      return get_slab_id(get_seq_num());
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
    disk_record_t() {}
    bool operator==(const disk_record_t &other) const;
    size_t serialize(char *buff) const;
    size_t length() const;
    inline bool has_shared_manifest() const { return s.flags.has_shared_manifest(); }
    inline bool has_valid_sha256() const { return s.flags.has_valid_sha256(); }
    inline void set_shared_manifest() { s.flags.set_shared_manifest(); }
    inline void set_valid_sha256()  { s.flags.set_valid_sha256(); }

    struct __attribute__ ((packed)) packed_rec_t
    {
      dedup_flags_t flags;	// 1 Byte flags
      uint8_t       pad8;
      uint16_t      num_parts;       // For multipart upload (AWS MAX-PART is 10,000)
      uint32_t      size_4k_units;   // 4KB units max out at 16TB (AWS MAX-SIZE is 5TB)

      uint64_t      md5_high;        // High Bytes of the Object Data MD5
      uint64_t      md5_low;         // Low  Bytes of the Object Data MD5
      uint64_t      version;
      uint64_t      shared_manifest; // 64bit hash of the SRC object manifest
      // all zeros for Dedicated-Manifest-Object
      uint64_t      sha256[4];	 // 4 * 8 Bytes of SHA256

      uint16_t      manifest_len;
      uint16_t      obj_name_len;
      uint16_t      bucket_name_len;
      uint16_t      pad16;
    }s;
    std::string obj_name;
    std::string bucket_name;
    bufferlist  manifest_bl;
  };

  std::ostream &operator<<(std::ostream &stream, const disk_record_t & rec);

  static constexpr unsigned BLOCK_MAGIC = 0xFACE;
  static constexpr unsigned LAST_BLOCK_MAGIC = 0xCAD7;
  static constexpr unsigned AVG_REC_SIZE = 400;
  //static constexpr unsigned MAX_REC_IN_BLOCK = DISK_BLOCK_SIZE / AVG_REC_SIZE;
  static constexpr unsigned MAX_REC_IN_BLOCK = 32;
  struct  __attribute__ ((packed)) disk_block_header_t {
    void deserialize();
    int verify(disk_block_id_t block_id, const DoutPrefixProvider* dpp);
    uint16_t        offset;
    uint16_t        rec_count;
    disk_block_id_t block_id;
    uint16_t        rec_offsets[MAX_REC_IN_BLOCK];
  };

  struct  __attribute__ ((packed)) disk_block_t
  {
    const disk_block_header_t* get_header() const { return (disk_block_header_t*)data; }
    disk_block_header_t* get_header() { return (disk_block_header_t*)data; }
    bool is_empty() const { return (get_header()->rec_count == 0); }

    void init(work_shard_t worker_id, uint32_t seq_number);
    int add_record(const disk_record_t *p_rec, const DoutPrefixProvider *dpp);
    void close_block(const DoutPrefixProvider* dpp, bool has_more);
    char data[DISK_BLOCK_SIZE];
  };

  int load_record(librados::IoCtx          *p_ioctx,
		  disk_record_t            *p_rec, /* OUT */
		  disk_block_id_t           block_id,
		  record_id_t               rec_id,
		  md5_shard_t               md5_shard,
		  const struct key_t       *p_key,
		  const DoutPrefixProvider *dpp);

  int load_slab(librados::IoCtx *p_ioctx,
		bufferlist &bl,
		md5_shard_t md5_shard,
		work_shard_t worker_id,
		uint32_t seq_number,
		const DoutPrefixProvider* dpp);

  int store_slab(librados::IoCtx *p_ioctx,
		 bufferlist &bl,
		 md5_shard_t md5_shard,
		 work_shard_t worker_id,
		 uint32_t seq_number,
		 const DoutPrefixProvider* dpp);

  class disk_block_array_t
  {
  public:
    disk_block_array_t(const DoutPrefixProvider* _dpp, md5_shard_t md5_shard) {
      d_md5_shard = md5_shard;
      d_worker_id = NULL_WORK_SHARD;
      dpp = _dpp;
      slab_reset();
    }

    void set_worker_id(work_shard_t worker_id, worker_stats_t *_p_stats) {
      memset(d_arr, 0, sizeof(d_arr));
      d_worker_id  = worker_id;
      d_seq_number = 0;
      p_stats      = _p_stats;
      slab_reset();
    }

    md5_shard_t get_md5_shard() {
      return d_md5_shard;
    }

    int add_record(librados::IoCtx        *p_ioctx,
		   const rgw::sal::Bucket *p_bucket,
		   const rgw::sal::Object *p_obj,
		   const parsed_etag_t    *p_parsed_etag,
		   uint64_t                obj_size);
    int flush_disk_records(librados::IoCtx *p_ioctx);

  private:
    inline const disk_block_t* last_block() { return &d_arr[DISK_BLOCK_COUNT-1]; }
    int flush(librados::IoCtx *p_ioctx);
    int fill_disk_record(disk_record_t          *p_rec,
			 const rgw::sal::Bucket *p_bucket,
			 const rgw::sal::Object *p_obj,
			 const parsed_etag_t    *p_parsed_etag,
			 uint64_t                obj_size);
    void slab_reset() {
      p_curr_block = d_arr;
      p_curr_block->init(d_worker_id, d_seq_number);
    }

    disk_block_t    d_arr[DISK_BLOCK_COUNT];
    disk_block_t   *p_curr_block = nullptr;
    work_shard_t    d_md5_shard = 0;
    md5_shard_t     d_worker_id = 0;
    uint32_t        d_seq_number = 0;
    const DoutPrefixProvider* dpp;
    worker_stats_t *p_stats = nullptr;
  };
} //namespace rgw::dedup
