// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_CLS_LOG_TYPES_H
#define CEPH_CLS_LOG_TYPES_H

#include <string>

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/types.h"
#include "include/utime.h"

#include "common/ceph_json.h"
#include "common/Formatter.h"

class JSONObj;
class JSONDecoder;

struct cls_log_entry {
  std::string id;
  std::string section;
  std::string name;
  utime_t timestamp;
  ceph::buffer::list data;

  cls_log_entry() = default;

  cls_log_entry(ceph::real_time timestamp, std::string section,
		std::string name, ceph::buffer::list&& data)
    : section(std::move(section)), name(std::move(name)),
      timestamp(utime_t(timestamp)), data(std::move(data)) {}

  cls_log_entry(utime_t timestamp, std::string section,
		std::string name, ceph::buffer::list&& data)
    : section(std::move(section)), name(std::move(name)), timestamp(timestamp),
      data(std::move(data)) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(section, bl);
    encode(name, bl);
    encode(timestamp, bl);
    encode(data, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(section, bl);
    decode(name, bl);
    decode(timestamp, bl);
    decode(data, bl);
    if (struct_v >= 2)
      decode(id, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const {
    encode_json("section", section, f);
    encode_json("name", name, f);
    encode_json("timestamp", timestamp, f);
    encode_json("data", data, f);
    encode_json("id", id, f);
  }

  void decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("section", section, obj);
    JSONDecoder::decode_json("name", name, obj);
    JSONDecoder::decode_json("timestamp", timestamp, obj);
    JSONDecoder::decode_json("data", data, obj);
    JSONDecoder::decode_json("id", id, obj);
  }

  static void generate_test_instances(std::list<cls_log_entry *>& l) {
    l.push_back(new cls_log_entry{});
    l.push_back(new cls_log_entry);
    l.back()->id = "test_id";
    l.back()->section = "test_section";
    l.back()->name = "test_name";
    l.back()->timestamp = utime_t();
    ceph::buffer::list bl;
    ceph::encode(std::string("Test"), bl, 0);
    l.back()->data = bl;
  }
};
WRITE_CLASS_ENCODER(cls_log_entry)

struct cls_log_header {
  std::string max_marker;
  utime_t max_time;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_marker, bl);
    encode(max_time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max_marker, bl);
    decode(max_time, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const {
    f->dump_string("max_marker", max_marker);
    f->dump_stream("max_time") << max_time;
  }
  static void generate_test_instances(std::list<cls_log_header*>& o) {
    o.push_back(new cls_log_header);
    o.push_back(new cls_log_header);
    o.back()->max_marker = "test_marker";
    o.back()->max_time = utime_t();
  }
};
inline bool operator ==(const cls_log_header& lhs, const cls_log_header& rhs) {
  return (lhs.max_marker == rhs.max_marker &&
	  lhs.max_time == rhs.max_time);
}
inline bool operator !=(const cls_log_header& lhs, const cls_log_header& rhs) {
  return !(lhs == rhs);
}
WRITE_CLASS_ENCODER(cls_log_header)


#endif
