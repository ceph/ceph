#ifndef CEPH_CLS_RGW_RATELIMIT_OPS_H
#define CEPH_CLS_RGW_RATELIMIT_OPS_H

#include "include/types.h"
#include "include/rados/cls_traits.hpp"
#include "rgw_ratelimit_core.h"

struct cls_rgw_ratelimit_consume_op {
  std::string key;
  uint8_t op_type = 0;
  RGWRateLimitInfo info;
  int64_t ts_ns = 0;
  int64_t interval = 60;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(op_type, bl);
    encode(info, bl);
    encode(ts_ns, bl);
    encode(interval, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(key, bl);
    decode(op_type, bl);
    decode(info, bl);
    decode(ts_ns, bl);
    decode(interval, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_ratelimit_consume_op)

struct cls_rgw_ratelimit_consume_reply {
  int64_t delay = 0;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(delay, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(delay, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_ratelimit_consume_reply)

struct cls_rgw_ratelimit_giveback_op {
  std::string key;
  uint8_t op_type = 0;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(op_type, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(key, bl);
    decode(op_type, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_ratelimit_giveback_op)

struct cls_rgw_ratelimit_decrease_bytes_op {
  std::string key;
  bool is_read = true;
  int64_t amount = 0;
  RGWRateLimitInfo info;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(is_read, bl);
    encode(amount, bl);
    encode(info, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(key, bl);
    decode(is_read, bl);
    decode(amount, bl);
    decode(info, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_ratelimit_decrease_bytes_op)

namespace cls::rgw::ratelimit {
struct ClassId {
  static constexpr auto name = "rgw_ratelimit";
};
namespace method {
constexpr auto consume = ClsMethod<RdWrTag, ClassId>("consume");
constexpr auto giveback = ClsMethod<RdWrTag, ClassId>("giveback");
constexpr auto decrease_bytes = ClsMethod<RdWrTag, ClassId>("decrease_bytes");
}
} // namespace cls::rgw::ratelimit

#endif
