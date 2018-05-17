#ifndef CEPH_CLS_OTP_OPS_H
#define CEPH_CLS_OTP_OPS_H

#include "include/types.h"
#include "include/utime.h"
#include "cls/otp/cls_otp_types.h"

struct cls_otp_set_otp_op
{
  list<rados::cls::otp::otp_info_t> entries;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_set_otp_op)

struct cls_otp_check_otp_op
{
  string id;
  string val;
  string token;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(val, bl);
    encode(token, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(val, bl);
    decode(token, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_check_otp_op)

struct cls_otp_get_result_op
{
  string token;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(token, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(token, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_get_result_op)

struct cls_otp_get_result_reply
{
  rados::cls::otp::otp_check_t result;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(result, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(result, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_get_result_reply)

struct cls_otp_remove_otp_op
{
  list<string> ids;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(ids, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(ids, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_remove_otp_op)

struct cls_otp_get_otp_op
{
  bool get_all{false};
  list<string> ids;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(get_all, bl);
    encode(ids, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(get_all, bl);
    decode(ids, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_get_otp_op)

struct cls_otp_get_otp_reply
{
  list<rados::cls::otp::otp_info_t> found_entries;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(found_entries, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(found_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_get_otp_reply)

struct cls_otp_get_current_time_op
{
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_get_current_time_op)

struct cls_otp_get_current_time_reply
{
  ceph::real_time time;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(time, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(time, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_get_current_time_reply)

#endif
