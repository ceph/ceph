// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_TIMEINDEX_OPS_H
#define CEPH_CLS_TIMEINDEX_OPS_H

#include <string>
#include <vector>

#include "common/ceph_time.h"

#include "cls_timeindex_types.h"

struct cls_timeindex_add_op {
  std::vector<cls_timeindex_entry> entries;

  cls_timeindex_add_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_timeindex_add_op)

struct cls_timeindex_list_op {
  ceph::real_time from_time;
  std::string marker; /* if not empty, overrides from_time */
  ceph::real_time to_time; /* not inclusive */
  int max_entries = 0; /* upperbound to returned num of entries might
			  return less than that and still be
			  truncated */

  cls_timeindex_list_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(from_time, bl);
    encode(marker, bl);
    encode(to_time, bl);
    encode(max_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(from_time, bl);
    decode(marker, bl);
    decode(to_time, bl);
    decode(max_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_timeindex_list_op)

struct cls_timeindex_list_ret {
  std::vector<cls_timeindex_entry> entries;
  std::string marker;
  bool truncated = false;

  cls_timeindex_list_ret() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl);
    encode(marker, bl);
    encode(truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    decode(marker, bl);
    decode(truncated, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_timeindex_list_ret)


/*
 * operation will return 0 when successfully removed but not done. Will return
 * -ENODATA when done, so caller needs to repeat sending request until that.
 */
struct cls_timeindex_trim_op {
  ceph::real_time from_time;
  ceph::real_time to_time; /* inclusive */
  std::string from_marker;
  std::string to_marker;

  cls_timeindex_trim_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(from_time, bl);
    encode(to_time, bl);
    encode(from_marker, bl);
    encode(to_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(from_time, bl);
    decode(to_time, bl);
    decode(from_marker, bl);
    decode(to_marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_timeindex_trim_op)

#endif /* CEPH_CLS_TIMEINDEX_OPS_H */
