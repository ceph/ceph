// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_TIMEINDEX_OPS_H
#define CEPH_CLS_TIMEINDEX_OPS_H

#include "common/ceph_json.h"
#include "cls_timeindex_types.h"

struct cls_timeindex_add_op {
  std::list<cls_timeindex_entry> entries;

  cls_timeindex_add_op() {}

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
  utime_t from_time;
  std::string marker; /* if not empty, overrides from_time */
  utime_t to_time; /* not inclusive */
  int max_entries; /* upperbound to returned num of entries
                      might return less than that and still be truncated */

  cls_timeindex_list_op() : max_entries(0) {}

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

   void dump(ceph::Formatter *f) const {
    f->open_object_section("from_time");
    from_time.dump(f);
    f->close_section();
    f->dump_string("marker", marker);
    f->open_object_section("to_time");
    to_time.dump(f);
    f->close_section();
    f->dump_int("max_entries", max_entries);
  }

  static void generate_test_instances(std::list<cls_timeindex_list_op*>& o) {
    o.push_back(new cls_timeindex_list_op);
    o.push_back(new cls_timeindex_list_op);
    o.back()->from_time = utime_t(1, 2);
    o.back()->marker = "marker";
    o.back()->to_time = utime_t(3, 4);
    o.back()->max_entries = 5;
  }
};
WRITE_CLASS_ENCODER(cls_timeindex_list_op)

struct cls_timeindex_list_ret {
  std::list<cls_timeindex_entry> entries;
  std::string marker;
  bool truncated;

  cls_timeindex_list_ret() : truncated(false) {}

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

  void dump(ceph::Formatter *f) const {
    encode_json("entries", entries, f);
    f->dump_string("marker", marker);
    f->dump_bool("truncated", truncated);
  }

  static void generate_test_instances(std::list<cls_timeindex_list_ret*>& o) {
    o.push_back(new cls_timeindex_list_ret);
    o.push_back(new cls_timeindex_list_ret);
    o.back()->entries.push_back(cls_timeindex_entry());
    o.back()->entries.back().key_ts = utime_t(1, 2);
    o.back()->entries.back().key_ext = "key_ext";
    o.back()->entries.back().value.append("value");
    o.back()->marker = "marker";
    o.back()->truncated = true;
  }
};
WRITE_CLASS_ENCODER(cls_timeindex_list_ret)


/*
 * operation will return 0 when successfully removed but not done. Will return
 * -ENODATA when done, so caller needs to repeat sending request until that.
 */
struct cls_timeindex_trim_op {
  utime_t from_time;
  utime_t to_time; /* inclusive */
  std::string from_marker;
  std::string to_marker;

  cls_timeindex_trim_op() {}

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
