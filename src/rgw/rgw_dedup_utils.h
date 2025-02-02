#pragma once
#include <string>
#include "include/rados/buffer.h"
#include "include/encoding.h"
#include <time.h>
#include "include/utime.h"

namespace rgw::dedup {
  static constexpr const char* DEDUP_WATCH_OBJ = "DEDUP_WATCH_OBJ";
  static constexpr uint64_t HEAD_OBJ_SIZE = 4*1024*1024; // 4MB
  using work_shard_t   = uint8_t;
  using md5_shard_t    = uint8_t;
#if 1
  // REMOVE-ME
  // temporary settings to help debug small systems
  const work_shard_t MAX_WORK_SHARD = 4;
  const md5_shard_t  MAX_MD5_SHARD  = 4;
#else
  // Those are the correct values for production system
  // can go as high as 0xFF-1
  const work_shard_t MAX_WORK_SHARD = 128;
  const md5_shard_t  MAX_MD5_SHARD  = 128;
#endif
  const work_shard_t NULL_WORK_SHARD = 0xFF;
  const md5_shard_t  NULL_MD5_SHARD  = 0xFF;
  const unsigned     NULL_SHARD      = 0xFF;

  // we use a single byte to store shard-id (work/MD5) so max must be no higher than 0xFF
  // We reseve the value 0xFF for NULL_WORK_SHARD/NULL_MD5_SHARD so MAX must be lower than 0xFF
  static_assert(MAX_WORK_SHARD < NULL_SHARD);
  static_assert(MAX_MD5_SHARD  < NULL_SHARD);
  struct __attribute__ ((packed)) dedup_flags_t {
  private:
    static constexpr uint8_t RGW_DEDUP_FLAG_SHA256          = 0x01;
    static constexpr uint8_t RGW_DEDUP_FLAG_SHARED_MANIFEST = 0x02;
    static constexpr uint8_t RGW_DEDUP_FLAG_SINGLETON       = 0x04;
    static constexpr uint8_t RGW_DEDUP_FLAG_OCCUPIED        = 0x08;
    static constexpr uint8_t RGW_DEDUP_FLAG_PG_VER          = 0x10;
    static constexpr uint8_t RGW_DEDUP_FLAG_FASTLANE        = 0x20;

  public:
    dedup_flags_t() : flags(0) {}
    dedup_flags_t(uint8_t _flags) : flags(_flags) {}
    inline void clear() { this->flags = 0; }
    inline bool has_shared_manifest() const { return ((flags & RGW_DEDUP_FLAG_SHARED_MANIFEST) != 0); }
    inline bool has_valid_sha256() const { return ((flags & RGW_DEDUP_FLAG_SHA256) != 0); }
    inline void set_shared_manifest() { flags |= RGW_DEDUP_FLAG_SHARED_MANIFEST; }
    inline void set_valid_sha256()  { flags |= RGW_DEDUP_FLAG_SHA256; }
    inline void set_fastlane()  { flags |= RGW_DEDUP_FLAG_FASTLANE; }
    inline bool is_fastlane()  const { return ((flags & RGW_DEDUP_FLAG_FASTLANE) != 0); }
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
      duration.tv.tv_sec = 0;
      duration.tv.tv_nsec = 0;
    }
    worker_stats_t& operator +=(const worker_stats_t& other) {
      this->ingress_obj += other.ingress_obj;
      this->ingress_obj_bytes += other.ingress_obj_bytes;
      this->egress_records += other.egress_records;
      this->egress_blocks += other.egress_blocks;
      this->egress_slabs += other.egress_slabs;
      this->single_part_objs += other.single_part_objs;
      this->multipart_objs += other.multipart_objs;
      this->small_multipart_obj += other.small_multipart_obj;
      this->valid_sha256 += other.valid_sha256;
      this->invalid_sha256 += other.invalid_sha256;
      this->default_storage_class_objs += other.default_storage_class_objs;
      this->default_storage_class_objs_bytes += other.default_storage_class_objs_bytes;
      this->non_default_storage_class_objs += other.non_default_storage_class_objs;
      this->non_default_storage_class_objs_bytes += other.non_default_storage_class_objs_bytes;
      this->ingress_failed_get_object += other.ingress_failed_get_object;
      this->ingress_failed_get_obj_attrs += other.ingress_failed_get_obj_attrs;
      this->ingress_skip_too_small_bytes += other.ingress_skip_too_small_bytes;
      this->ingress_skip_too_small += other.ingress_skip_too_small;
      this->ingress_skip_too_small_64KB_bytes += other.ingress_skip_too_small_64KB_bytes;
      this->ingress_skip_too_small_64KB += other.ingress_skip_too_small_64KB;

      return *this;
    }
    uint64_t ingress_obj = 0;
    uint64_t ingress_obj_bytes = 0;
    uint64_t egress_records = 0;
    uint64_t egress_blocks = 0;
    uint64_t egress_slabs = 0;

    uint64_t single_part_objs = 0;
    uint64_t multipart_objs = 0;
    uint64_t small_multipart_obj = 0;

    uint64_t valid_sha256 = 0;
    uint64_t invalid_sha256 = 0;

    uint64_t default_storage_class_objs = 0;
    uint64_t default_storage_class_objs_bytes = 0;

    uint64_t non_default_storage_class_objs = 0;
    uint64_t non_default_storage_class_objs_bytes = 0;

    uint64_t ingress_failed_get_object = 0;
    uint64_t ingress_failed_get_obj_attrs = 0;

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

    encode(w.valid_sha256, bl);
    encode(w.invalid_sha256, bl);

    encode(w.default_storage_class_objs, bl);
    encode(w.default_storage_class_objs_bytes, bl);
    encode(w.non_default_storage_class_objs, bl);
    encode(w.non_default_storage_class_objs_bytes, bl);

    encode(w.ingress_failed_get_object, bl);
    encode(w.ingress_failed_get_obj_attrs, bl);

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
    decode(w.valid_sha256, bl);
    decode(w.invalid_sha256, bl);
    decode(w.default_storage_class_objs, bl);
    decode(w.default_storage_class_objs_bytes, bl);
    decode(w.non_default_storage_class_objs, bl);
    decode(w.non_default_storage_class_objs_bytes, bl);
    decode(w.ingress_failed_get_object, bl);
    decode(w.ingress_failed_get_obj_attrs, bl);
    decode(w.ingress_skip_too_small_bytes, bl);
    decode(w.ingress_skip_too_small, bl);
    decode(w.ingress_skip_too_small_64KB_bytes, bl);
    decode(w.ingress_skip_too_small_64KB, bl);

    decode(w.duration, bl);
    DECODE_FINISH(bl);
  }

  struct md5_stats_t {
    void reset() {
      memset(&this->skipped_shared_manifest, 0, offsetof(md5_stats_t, duration));
      duration.tv.tv_sec = 0, duration.tv.tv_nsec = 0;
    }
    uint64_t get_skipped_total() const {
      return (skipped_source_record + skipped_singleton + skipped_shared_manifest +
	      ingress_skip_encrypted + ingress_skip_compressed + ingress_skip_changed_objs +
	      skipped_duplicate + skipped_bad_sha256 + skipped_failed_src_load);
    }
    md5_stats_t& operator +=(const md5_stats_t& other) {
      // rados_bytes_before_dedup should be identical on all systems
      this->rados_bytes_before_dedup = std::max(this->rados_bytes_before_dedup,
						other.rados_bytes_before_dedup);
      this->ingress_failed_get_object    += other.ingress_failed_get_object;
      this->ingress_failed_get_obj_attrs += other.ingress_failed_get_obj_attrs;
      this->ingress_skip_encrypted       += other.ingress_skip_encrypted;
      this->ingress_skip_encrypted_bytes += other.ingress_skip_encrypted_bytes;
      this->ingress_skip_compressed      += other.ingress_skip_compressed;
      this->ingress_skip_compressed_bytes+= other.ingress_skip_compressed_bytes;
      this->ingress_skip_changed_objs    += other.ingress_skip_changed_objs;

      this->skipped_shared_manifest += other.skipped_shared_manifest;
      this->skipped_singleton       += other.skipped_singleton;
      this->skipped_singleton_bytes += other.skipped_singleton_bytes;
      this->skipped_source_record   += other.skipped_source_record;
      this->skipped_duplicate       += other.skipped_duplicate;
      this->skipped_bad_sha256      += other.skipped_bad_sha256;
      this->skipped_failed_src_load += other.skipped_failed_src_load;

      this->valid_sha256_attrs      += other.valid_sha256_attrs;
      this->invalid_sha256_attrs    += other.invalid_sha256_attrs;
      this->skip_sha256_cmp         += other.skip_sha256_cmp;

      this->set_shared_manifest     += other.set_shared_manifest;
      this->loaded_objects          += other.loaded_objects;
      this->processed_objects       += other.processed_objects;
      this->singleton_count         += other.singleton_count;
      this->duplicate_count         += other.duplicate_count;
      this->duplicated_blocks_bytes += other.duplicated_blocks_bytes;
      this->unique_count            += other.unique_count;
      this->deduped_objects         += other.deduped_objects;
      this->deduped_objects_bytes   += other.deduped_objects_bytes;

      this->failed_dedup            += other.failed_dedup;
      return *this;
    }

    uint64_t rados_bytes_before_dedup = 0;
    uint64_t ingress_failed_get_object = 0;
    uint64_t ingress_failed_get_obj_attrs = 0;

    uint64_t ingress_skip_encrypted = 0;
    uint64_t ingress_skip_encrypted_bytes = 0;
    uint64_t ingress_skip_compressed = 0;
    uint64_t ingress_skip_compressed_bytes = 0;
    uint64_t ingress_skip_changed_objs = 0;

    uint64_t skipped_shared_manifest = 0;
    uint64_t skipped_singleton = 0;
    uint64_t skipped_singleton_bytes = 0;
    uint64_t skipped_source_record = 0;
    uint64_t skipped_duplicate = 0;
    uint64_t skipped_bad_sha256 = 0;
    uint64_t skipped_failed_src_load = 0;

    uint64_t valid_sha256_attrs = 0;
    uint64_t invalid_sha256_attrs = 0;
    uint64_t skip_sha256_cmp = 0;

    uint64_t set_shared_manifest = 0;
    uint64_t loaded_objects = 0;
    uint64_t processed_objects = 0;
    uint64_t singleton_count = 0;
    uint64_t duplicate_count = 0;
    uint64_t duplicated_blocks_bytes = 0;
    uint64_t unique_count = 0;
    uint64_t deduped_objects = 0;
    uint64_t deduped_objects_bytes = 0;
    uint64_t failed_dedup = 0;

    utime_t  duration = {0, 0};
  };
  std::ostream &operator<<(std::ostream &out, const md5_stats_t &s);
  inline void encode(const md5_stats_t& m, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);

    encode(m.rados_bytes_before_dedup, bl);
    encode(m.ingress_failed_get_object, bl);
    encode(m.ingress_failed_get_obj_attrs, bl);
    encode(m.ingress_skip_encrypted, bl);
    encode(m.ingress_skip_encrypted_bytes, bl);
    encode(m.ingress_skip_compressed, bl);
    encode(m.ingress_skip_compressed_bytes, bl);
    encode(m.ingress_skip_changed_objs, bl);

    encode(m.skipped_shared_manifest, bl);
    encode(m.skipped_singleton, bl);
    encode(m.skipped_singleton_bytes, bl);
    encode(m.skipped_source_record, bl);
    encode(m.skipped_duplicate, bl);
    encode(m.skipped_bad_sha256, bl);
    encode(m.skipped_failed_src_load, bl);

    encode(m.valid_sha256_attrs, bl);
    encode(m.invalid_sha256_attrs, bl);
    encode(m.skip_sha256_cmp, bl);
    encode(m.set_shared_manifest, bl);

    encode(m.loaded_objects, bl);
    encode(m.processed_objects, bl);
    encode(m.singleton_count, bl);
    encode(m.duplicate_count, bl);
    encode(m.duplicated_blocks_bytes, bl);
    encode(m.unique_count, bl);
    encode(m.deduped_objects, bl);
    encode(m.deduped_objects_bytes, bl);
    encode(m.failed_dedup, bl);

    encode(m.duration, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(md5_stats_t& m, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(m.rados_bytes_before_dedup, bl);
    decode(m.ingress_failed_get_object, bl);
    decode(m.ingress_failed_get_obj_attrs, bl);
    decode(m.ingress_skip_encrypted, bl);
    decode(m.ingress_skip_encrypted_bytes, bl);
    decode(m.ingress_skip_compressed, bl);
    decode(m.ingress_skip_compressed_bytes, bl);
    decode(m.ingress_skip_changed_objs, bl);

    decode(m.skipped_shared_manifest, bl);
    decode(m.skipped_singleton, bl);
    decode(m.skipped_singleton_bytes, bl);
    decode(m.skipped_source_record, bl);
    decode(m.skipped_duplicate, bl);
    decode(m.skipped_bad_sha256, bl);
    decode(m.skipped_failed_src_load, bl);

    decode(m.valid_sha256_attrs, bl);
    decode(m.invalid_sha256_attrs, bl);
    decode(m.skip_sha256_cmp, bl);
    decode(m.set_shared_manifest, bl);

    decode(m.loaded_objects, bl);
    decode(m.processed_objects, bl);
    decode(m.singleton_count, bl);
    decode(m.duplicate_count, bl);
    decode(m.duplicated_blocks_bytes, bl);
    decode(m.unique_count, bl);
    decode(m.deduped_objects, bl);
    decode(m.deduped_objects_bytes, bl);
    decode(m.failed_dedup, bl);

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

  enum urgent_msg_t {
    URGENT_MSG_NONE    = 0,
    URGENT_MSG_ABORT   = 1,
    URGENT_MSG_PASUE   = 2,
    URGENT_MSG_RESUME  = 3,
    URGENT_MSG_RESTART = 4,
    URGENT_MSG_INVALID = 5
  };

  const char* get_urgent_msg_names(int msg);
  uint64_t hex2int(const char *p, const char* p_end);
  uint16_t dec2int(const char *p, const char* p_end);
  uint16_t get_num_parts(const std::string & etag);
  void parse_etag_string(const std::string& etag, parsed_etag_t *parsed_etag);
  void etag_to_bufferlist(uint64_t md5_high, uint64_t md5_low, uint16_t num_parts, ceph::bufferlist *bl);
  void sha256_to_bufferlist(uint64_t sha256a, uint64_t sha256b,
			    uint64_t sha256c, uint64_t sha256d,
			    ceph::bufferlist *bl);
  std::string calc_refcount_tag_hash(const std::string &bucket_name, const std::string &obj_name);

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
