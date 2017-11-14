#ifndef CEPH_CLS_OTP_OPS_H
#define CEPH_CLS_OTP_OPS_H

#include "include/types.h"
#include "include/utime.h"
#include "cls/otp/cls_otp_types.h"

struct cls_otp_set_otp_op
{
  list<rados::cls::otp::otp_info_t> entries;

  cls_otp_set_otp_op() = default;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_set_otp_op)

struct cls_otp_check_otp_op
{
  string id;
  string val;

  cls_otp_check_otp_op() = default;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(id, bl);
    ::encode(val, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(id, bl);
    ::decode(val, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_check_otp_op)

struct cls_otp_remove_otp_op
{
  list<string> ids;

  cls_otp_remove_otp_op() = default;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(ids, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(ids, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_remove_otp_op)

struct cls_otp_get_otp_op
{
  bool get_all{false};
  list<string> ids;

  cls_otp_get_otp_op() = default;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(get_all, bl);
    ::encode(ids, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(get_all, bl);
    ::decode(ids, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_get_otp_op)

struct cls_otp_get_otp_reply
{
  list<rados::cls::otp::otp_info_t> found_entries;

  cls_otp_get_otp_reply() = default;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(found_entries, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(found_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_otp_get_otp_reply)

#endif
