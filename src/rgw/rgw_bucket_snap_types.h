// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <map>
#include "include/types.h"
#include "common/Formatter.h"
#include "common/ceph_time.h"
#include "common/ceph_json.h"
#include "rgw_xml.h"



struct rgw_bucket_snap_id {
  static constexpr uint64_t SNAP_UNDEFINED = 0;
  static constexpr uint64_t SNAP_MIN       = 1;
  uint64_t snap_id;

  constexpr rgw_bucket_snap_id() : snap_id(SNAP_UNDEFINED) {}
  rgw_bucket_snap_id(uint64_t snap_id) : snap_id(snap_id) {}

  std::string to_string() const {
    return std::to_string(snap_id);
  }

  void init() {
    snap_id = SNAP_MIN;
  }

  void init(uint64_t _snap_id) {
    if (_snap_id != SNAP_UNDEFINED) {
      snap_id = _snap_id;
    } else {
      snap_id = SNAP_MIN;
    }
  }

  void init(rgw_bucket_snap_id _snap_id) {
    init(_snap_id.snap_id);
  }

  bool init_from_str(const std::string& s);

  void reset() {
    snap_id = SNAP_UNDEFINED;
  }

  uint64_t operator++() {
    return ++snap_id;
  }

  uint64_t operator++(int) {
    return snap_id++;
  }

  void encode(bufferlist& bl) const {
    /* no version control for this type */
    ceph::encode(snap_id, bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    /* no version control for this type */
    ceph::decode(snap_id, bl);
  }

  auto operator<=>(const rgw_bucket_snap_id&) const = default;

  operator uint64_t() const {
    return snap_id;
  }

  bool is_set() const {
    return snap_id != SNAP_UNDEFINED;
  }

  rgw_bucket_snap_id get_or(rgw_bucket_snap_id other) const {
    if (is_set()) {
      return *this;
    }
    return other;
  }

  bool is_min() const {
    return snap_id == SNAP_MIN;
  }

  static rgw_bucket_snap_id min() {
    return rgw_bucket_snap_id(SNAP_MIN);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_snap_id)

static inline std::ostream& operator<<(std::ostream& os, const rgw_bucket_snap_id& snap_id)
{
  os << snap_id.snap_id;
  return os;
}

static inline void encode_json(const char *name, const rgw_bucket_snap_id& val, ceph::Formatter *f)
{
  encode_json(name, val.snap_id, f);
}

static inline void encode_xml(const char *name, const rgw_bucket_snap_id& val, ceph::Formatter *f)
{
  encode_xml(name, val.snap_id, f);
}

static inline void decode_json_obj(rgw_bucket_snap_id& val, JSONObj *obj)
{
  decode_json_obj(val.snap_id, obj);
}

struct rgw_bucket_snap_range {
  rgw_bucket_snap_id start; /* first snapshot in the range, would not be included in results */
  rgw_bucket_snap_id end;

  bool contains(rgw_bucket_snap_id snap_id) const {
    if (start.is_set()) {
      if (start == end) {
        return snap_id == start;
      }
      return snap_id > start && snap_id <= end;
    }
    return snap_id <= end;
  }

  bool is_set() const {
    return start.is_set() || end.is_set();
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(start, bl);
    encode(end, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(start, bl);
    decode(end, bl);
    DECODE_FINISH(bl);
  }

  void dump_xml(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_bucket_snap_range);

static inline std::ostream& operator<<(std::ostream& os, const rgw_bucket_snap_range& snap_range)
{
  os << "[" << snap_range.start << "," << snap_range.end << "]";
  return os;
}
