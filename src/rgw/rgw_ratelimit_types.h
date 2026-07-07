#pragma once

#include "include/buffer.h"
#include "include/encoding.h"

class Formatter;
class JSONObj;

struct RGWRateLimitInfo {
  int64_t max_write_ops;
  int64_t max_read_ops;
  int64_t max_list_ops;
  int64_t max_delete_ops;
  int64_t max_write_bytes;
  int64_t max_read_bytes;
  bool enabled = false;
  RGWRateLimitInfo()
    : max_write_ops(0), max_read_ops(0), max_list_ops(0), max_delete_ops(0),
      max_write_bytes(0), max_read_bytes(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(max_write_ops, bl);
    encode(max_read_ops, bl);
    encode(max_list_ops, bl);
    encode(max_delete_ops, bl);
    encode(max_write_bytes, bl);
    encode(max_read_bytes, bl);
    encode(enabled, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(max_write_ops, bl);
    decode(max_read_ops, bl);
    if (struct_v >= 2) {
      decode(max_list_ops, bl);
    } else {
      max_list_ops = 0;
    }
    if (struct_v >= 2) {
      decode(max_delete_ops, bl);
    } else {
      max_delete_ops = 0;
    }
    decode(max_write_bytes, bl);
    decode(max_read_bytes, bl);
    decode(enabled, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRateLimitInfo)
