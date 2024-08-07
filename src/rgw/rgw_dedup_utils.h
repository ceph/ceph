#pragma once
#include <string>
#include "include/rados/buffer.h"
namespace rgw::dedup {
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
