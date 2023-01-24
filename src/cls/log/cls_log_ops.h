// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_LOG_OPS_H
#define CEPH_CLS_LOG_OPS_H

#include <string>
#include <vector>

#include "common/ceph_json.h"
#include "common/ceph_time.h"

#include "cls_log_types.h"

namespace cls::log::ops {
struct add_op {
  std::vector<entry> entries;
  bool monotonic_inc = true;

  add_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(entries, bl);
    encode(monotonic_inc, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(entries, bl);
    if (struct_v >= 2) {
      decode(monotonic_inc, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const {
    encode_json("entries", entries, f);
    encode_json("monotonic_inc", monotonic_inc, f);
  }

  static void generate_test_instances(std::list<add_op *>& l) {
    using namespace std::literals;
    l.push_back(new add_op);
    l.push_back(new add_op);
    l.back()->entries.push_back(entry{});
    l.back()->entries.push_back(entry{});
    l.back()->entries.back().section = "section";
    l.back()->entries.back().name = "name";
    l.back()->entries.back().timestamp = ceph::real_time{1s + 2ns};
    l.back()->entries.back().data.append("data");
    l.back()->entries.back().id = "id";
  }
};
WRITE_CLASS_ENCODER(add_op)

struct list_op {
  ceph::real_time from_time;
  std::string marker; /* if not empty, overrides from_time */
  ceph::real_time to_time; /* not inclusive */
  int max_entries = 0; /* upperbound to returned num of entries
			  might return less than that and still be truncated */

  list_op() = default;

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

  void dump(ceph::Formatter* f) const {
    f->dump_stream("from_time") << from_time;
    f->dump_string("marker", marker);
    f->dump_stream("to_time") << to_time;
    f->dump_int("max_entries", max_entries);
  }
  static void generate_test_instances(std::list<list_op*>& ls) {
    using namespace std::literals;
    ls.push_back(new list_op);
    ls.push_back(new list_op);
    ls.back()->from_time = ceph::real_time{1s + 2ns};
    ls.back()->marker = "marker";
    ls.back()->to_time = ceph::real_time{3s + 4ns};
    ls.back()->max_entries = 5;
  }
};
WRITE_CLASS_ENCODER(list_op)

struct list_ret {
  std::vector<entry> entries;
  std::string marker;
  bool truncated;

  list_ret() : truncated(false) {}

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

  void dump(ceph::Formatter* f) const {
    encode_json("entries", entries, f);
    f->dump_string("marker", marker);
    f->dump_bool("truncated", truncated);
  }
  static void generate_test_instances(std::list<list_ret*>& ls) {
    using namespace std::literals;
    ls.push_back(new list_ret);
    ls.push_back(new list_ret);
    ls.back()->entries.push_back(entry{});
    ls.back()->entries.push_back(entry{});
    ls.back()->entries.back().section = "section";
    ls.back()->entries.back().name = "name";
    ls.back()->entries.back().timestamp = ceph::real_time{1s + 2ns};
    ls.back()->entries.back().data.append("data");
    ls.back()->entries.back().id = "id";
    ls.back()->marker = "marker";
    ls.back()->truncated = true;
  }
};
WRITE_CLASS_ENCODER(list_ret)

/*
 * operation will return 0 when successfully removed but not done. Will return
 * -ENODATA when done, so caller needs to repeat sending request until that.
 */
struct trim_op {
  ceph::real_time from_time;
  ceph::real_time to_time; /* inclusive */
  std::string from_marker;
  std::string to_marker;

  trim_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(from_time, bl);
    encode(to_time, bl);
    encode(from_marker, bl);
    encode(to_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(from_time, bl);
    decode(to_time, bl);
    if (struct_v >= 2) {
    decode(from_marker, bl);
    decode(to_marker, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const {
    f->dump_stream("from_time") << from_time;
    f->dump_stream("to_time") << to_time;
    f->dump_string("from_marker", from_marker);
    f->dump_string("to_marker", to_marker);
  }
  static void generate_test_instances(std::list<trim_op*>& ls) {
    using namespace std::literals;
    ls.push_back(new trim_op);
    ls.push_back(new trim_op);
    ls.back()->from_time = ceph::real_time{1s + 2ns};
    ls.back()->to_time = ceph::real_time(3s + 4ns);
    ls.back()->from_marker = "from_marker";
    ls.back()->to_marker = "to_marker";
  }
};
WRITE_CLASS_ENCODER(trim_op)

struct info_op {
  info_op() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    // currently empty request
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    // currently empty request
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const {
  }

  static void generate_test_instances(std::list<info_op*>& ls) {
    ls.push_back(new info_op);
  }
};
WRITE_CLASS_ENCODER(info_op)

struct info_ret {
  cls::log::header header;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(this->header, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(this->header, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(info_ret)
} // namespace cls::log::ops

#endif
