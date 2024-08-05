#pragma once
#include <string>
#include "include/rados/buffer.h"
namespace rgw::dedup {
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
  std::string calc_refcount_tag_hash(const std::string &bucket_name, const std::string &obj_name);
  // 16 bytes hexstring  -> 8 Byte uint64_t
  static inline constexpr unsigned HEX_UNIT_SIZE = 16;

} //namespace rgw::dedup
