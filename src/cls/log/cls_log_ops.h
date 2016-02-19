// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_LOG_OPS_H
#define CEPH_CLS_LOG_OPS_H

#include "include/types.h"
#include "cls_log_types.h"

struct cls_log_add_op {
  list<cls_log_entry> entries;
  bool monotonic_inc;

  cls_log_add_op() : monotonic_inc(true) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(entries, bl);
    ::encode(monotonic_inc, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(entries, bl);
    if (struct_v >= 2) {
      ::decode(monotonic_inc, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_log_add_op)

struct cls_log_list_op {
  utime_t from_time;
  string marker; /* if not empty, overrides from_time */
  utime_t to_time; /* not inclusive */
  int max_entries; /* upperbound to returned num of entries
                      might return less than that and still be truncated */

  cls_log_list_op() : max_entries(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(from_time, bl);
    ::encode(marker, bl);
    ::encode(to_time, bl);
    ::encode(max_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(from_time, bl);
    ::decode(marker, bl);
    ::decode(to_time, bl);
    ::decode(max_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_log_list_op)

struct cls_log_list_ret {
  list<cls_log_entry> entries;
  string marker;
  bool truncated;

  cls_log_list_ret() : truncated(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ::encode(marker, bl);
    ::encode(truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    ::decode(marker, bl);
    ::decode(truncated, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_log_list_ret)


/*
 * operation will return 0 when successfully removed but not done. Will return
 * -ENODATA when done, so caller needs to repeat sending request until that.
 */
struct cls_log_trim_op {
  utime_t from_time;
  utime_t to_time; /* inclusive */
  string from_marker;
  string to_marker;

  cls_log_trim_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(from_time, bl);
    ::encode(to_time, bl);
    ::encode(from_marker, bl);
    ::encode(to_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(from_time, bl);
    ::decode(to_time, bl);
    if (struct_v >= 2) {
    ::decode(from_marker, bl);
    ::decode(to_marker, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_log_trim_op)

struct cls_log_info_op {
  cls_log_info_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    // currently empty request
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    // currently empty request
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_log_info_op)

struct cls_log_info_ret {
  cls_log_header header;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(header, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(header, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_log_info_ret)

#endif
