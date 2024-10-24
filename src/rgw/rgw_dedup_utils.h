#pragma once
#include <string>
#include "include/rados/buffer.h"
#include "include/encoding.h"
#include <time.h>
#include "include/utime.h"

namespace rgw::dedup {
  using work_shard_t   = uint8_t;
  using md5_shard_t    = uint8_t;
  const work_shard_t MAX_WORK_SHARD = 12;
  const md5_shard_t  MAX_MD5_SHARD  = 12;

  struct __attribute__ ((packed)) dedup_flags_t {
  private:
    static constexpr uint8_t RGW_DEDUP_FLAG_SHA256          = 0x01;
    static constexpr uint8_t RGW_DEDUP_FLAG_SHARED_MANIFEST = 0x02;
    static constexpr uint8_t RGW_DEDUP_FLAG_SINGLETON       = 0x04;
    static constexpr uint8_t RGW_DEDUP_FLAG_OCCUPIED        = 0x08;
    static constexpr uint8_t RGW_DEDUP_FLAG_PG_VER          = 0x10;

  public:
    dedup_flags_t() : flags(0) {}
    dedup_flags_t(uint8_t _flags) : flags(_flags) {}
    inline void clear() { this->flags = 0; }
    inline bool has_shared_manifest() const { return ((flags & RGW_DEDUP_FLAG_SHARED_MANIFEST) != 0); }
    inline bool has_valid_sha256() const { return ((flags & RGW_DEDUP_FLAG_SHA256) != 0); }
    inline void set_shared_manifest() { flags |= RGW_DEDUP_FLAG_SHARED_MANIFEST; }
    inline void set_valid_sha256()  { flags |= RGW_DEDUP_FLAG_SHA256; }
    inline bool is_singleton() const { return ((flags & RGW_DEDUP_FLAG_SINGLETON) != 0); }
    inline void clear_singleton() { this->flags &= ~RGW_DEDUP_FLAG_SINGLETON; }
    inline bool is_occupied() const {return ((this->flags & RGW_DEDUP_FLAG_OCCUPIED) != 0); }
    inline void set_occupied() {this->flags |= RGW_DEDUP_FLAG_OCCUPIED; }
    inline void clear_occupied() { this->flags &= ~RGW_DEDUP_FLAG_OCCUPIED; }
    inline void set_singleton_occupied() {this->flags |= (RGW_DEDUP_FLAG_OCCUPIED | RGW_DEDUP_FLAG_SINGLETON); }

  private:
    uint8_t flags;
  };

  struct worker_stats_t {
    void reset() {
      memset(&this->ingress_obj, 0, offsetof(worker_stats_t, duration));
#if 1
      ingress_obj = 0;
      egress_records = 0;
      egress_blocks = 0;
      egress_slabs = 0;
      valid_sha256 = 0;
      invalid_sha256 = 0;
      ingress_failed_get_object = 0;
      ingress_failed_get_obj_attrs = 0;
      ingress_skip_too_small = 0;
      ingress_skip_encrypted = 0;
      ingress_skip_compressed = 0;
#endif
      duration.tv.tv_sec = 0;
      duration.tv.tv_nsec = 0;
    }
    worker_stats_t& operator +=(const worker_stats_t& other) {
      this->ingress_obj += other.ingress_obj;
      this->egress_records += other.egress_records;
      this->egress_blocks += other.egress_blocks;
      this->egress_slabs += other.egress_slabs;
      this->valid_sha256 += other.valid_sha256;
      this->invalid_sha256 += other.invalid_sha256;
      this->ingress_failed_get_object += other.ingress_failed_get_object;
      this->ingress_failed_get_obj_attrs += other.ingress_failed_get_obj_attrs;
      this->ingress_skip_too_small += other.ingress_skip_too_small;
      this->ingress_skip_encrypted += other.ingress_skip_encrypted;
      this->ingress_skip_compressed += other.ingress_skip_compressed;

      return *this;
    }
    uint64_t ingress_obj = 0;
    uint64_t egress_records = 0;
    uint64_t egress_blocks = 0;
    uint64_t egress_slabs = 0;

    uint64_t valid_sha256 = 0;
    uint64_t invalid_sha256 = 0;

    uint64_t ingress_failed_get_object = 0;
    uint64_t ingress_failed_get_obj_attrs = 0;

    uint64_t ingress_skip_too_small = 0;
    uint64_t ingress_skip_encrypted = 0;
    uint64_t ingress_skip_compressed = 0;
    utime_t  duration = {0, 0};
  };
  std::ostream& operator<<(std::ostream &out, const worker_stats_t &s);
  inline void encode(const worker_stats_t& w, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(w.ingress_obj, bl);
    encode(w.egress_records, bl);
    encode(w.egress_blocks, bl);
    encode(w.egress_slabs, bl);

    encode(w.valid_sha256, bl);
    encode(w.invalid_sha256, bl);
    encode(w.ingress_failed_get_object, bl);
    encode(w.ingress_failed_get_obj_attrs, bl);

    encode(w.ingress_skip_too_small, bl);
    encode(w.ingress_skip_encrypted, bl);
    encode(w.ingress_skip_compressed, bl);

    encode(w.duration, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(worker_stats_t& w, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(w.ingress_obj, bl);
    decode(w.egress_records, bl);
    decode(w.egress_blocks, bl);
    decode(w.egress_slabs, bl);
    decode(w.valid_sha256, bl);
    decode(w.invalid_sha256, bl);
    decode(w.ingress_failed_get_object, bl);
    decode(w.ingress_failed_get_obj_attrs, bl);
    decode(w.ingress_skip_too_small, bl);
    decode(w.ingress_skip_encrypted, bl);
    decode(w.ingress_skip_compressed, bl);

    decode(w.duration, bl);
    DECODE_FINISH(bl);
  }

  struct md5_stats_t {
    void reset() {
      memset(&this->skipped_shared_manifest, 0, offsetof(md5_stats_t, duration));
#if 1
      this->skipped_shared_manifest = 0;
      this->skipped_singleton       = 0;
      this->skipped_source_record   = 0;
      this->skipped_duplicate       = 0;
      this->skipped_bad_sha256      = 0;

      this->set_shared_manifest     = 0;
      this->loaded_objects          = 0;
      this->processed_objects       = 0;
      this->singleton_count         = 0;
      this->duplicate_count         = 0;
      this->unique_count            = 0;
      this->deduped_objects         = 0;
#endif
      duration.tv.tv_sec = 0, duration.tv.tv_nsec = 0;
    }
    uint64_t get_skipped_total() const {
      return (skipped_source_record + skipped_singleton + skipped_shared_manifest +
	      skipped_duplicate + skipped_bad_sha256);
    }
    md5_stats_t& operator +=(const md5_stats_t& other) {
      this->skipped_shared_manifest += other.skipped_shared_manifest ;
      this->skipped_singleton       += other.skipped_singleton ;
      this->skipped_source_record   += other.skipped_source_record ;
      this->skipped_duplicate       += other.skipped_duplicate ;
      this->skipped_bad_sha256      += other.skipped_bad_sha256 ;

      this->set_shared_manifest     += other.set_shared_manifest;
      this->skip_sha256_cmp         += other.skip_sha256_cmp;
      this->loaded_objects          += other.loaded_objects;
      this->processed_objects       += other.processed_objects;
      this->singleton_count         += other.singleton_count;
      this->duplicate_count         += other.duplicate_count;
      this->unique_count            += other.unique_count;
      this->deduped_objects         += other.deduped_objects;
      return *this;
    }
    uint64_t skipped_shared_manifest = 0;
    uint64_t skipped_singleton = 0;
    uint64_t skipped_source_record = 0;
    uint64_t skipped_duplicate = 0;
    uint64_t skipped_bad_sha256 = 0;

    uint64_t set_shared_manifest = 0;
    uint64_t skip_sha256_cmp = 0;

    uint64_t loaded_objects = 0;
    uint64_t processed_objects = 0;
    uint64_t singleton_count = 0;
    uint64_t duplicate_count = 0;
    uint64_t unique_count = 0;
    uint64_t deduped_objects = 0;

    utime_t  duration = {0, 0};
  };
  std::ostream &operator<<(std::ostream &out, const md5_stats_t &s);
  inline void encode(const md5_stats_t& m, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(m.skipped_shared_manifest, bl);
    encode(m.skipped_singleton, bl);
    encode(m.skipped_source_record, bl);
    encode(m.skipped_duplicate, bl);
    encode(m.skipped_bad_sha256, bl);
    encode(m.set_shared_manifest, bl);
    encode(m.skip_sha256_cmp, bl);

    encode(m.loaded_objects, bl);
    encode(m.processed_objects, bl);
    encode(m.singleton_count, bl);
    encode(m.duplicate_count, bl);
    encode(m.unique_count, bl);
    encode(m.deduped_objects, bl);
    encode(m.duration, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(md5_stats_t& m, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(m.skipped_shared_manifest, bl);
    decode(m.skipped_singleton, bl);
    decode(m.skipped_source_record, bl);
    decode(m.skipped_duplicate, bl);
    decode(m.skipped_bad_sha256, bl);
    decode(m.set_shared_manifest, bl);
    decode(m.skip_sha256_cmp, bl);

    decode(m.loaded_objects, bl);
    decode(m.processed_objects, bl);
    decode(m.singleton_count, bl);
    decode(m.duplicate_count, bl);
    decode(m.unique_count, bl);
    decode(m.deduped_objects, bl);
    decode(m.duration, bl);
    DECODE_FINISH(bl);
  }

  struct parsed_etag_t {
    uint64_t md5_high;  // High Bytes of the Object Data MD5
    uint64_t md5_low;   // Low  Bytes of the Object Data MD5
    uint16_t num_parts; // How many parts were used in multipart upload
  };

  uint64_t hex2int(const char *p, const char* p_end);
  uint16_t dec2int(const char *p, const char* p_end);
  uint16_t get_num_parts(const std::string & etag);
  void parse_etag_string(const std::string& etag, parsed_etag_t *parsed_etag);
  void etag_to_bufferlist(uint64_t md5_high, uint64_t md5_low, uint16_t num_parts, ceph::bufferlist *bl);
  void sha256_to_bufferlist(uint64_t sha256a, uint64_t sha256b,
			    uint64_t sha256c, uint64_t sha256d,
			    ceph::bufferlist *bl);
  std::string calc_refcount_tag_hash(const std::string &bucket_name, const std::string &obj_name);
  // 16 bytes hexstring  -> 8 Byte uint64_t
  static inline constexpr unsigned HEX_UNIT_SIZE = 16;

} //namespace rgw::dedup
