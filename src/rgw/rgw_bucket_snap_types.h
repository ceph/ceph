// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <map>
#include "include/types.h"
#include "common/Formatter.h"
#include "common/ceph_time.h"
#include "common/ceph_json.h"



struct rgw_bucket_snap_id {
  static constexpr uint64_t SNAP_UNDEFINED = (uint64_t)-1;
  static constexpr uint64_t SNAP_MIN       = (uint64_t)0;
  uint64_t snap_id;

  constexpr rgw_bucket_snap_id() : snap_id(SNAP_UNDEFINED) {}
  rgw_bucket_snap_id(uint64_t snap_id) : snap_id(snap_id) {}

  std::string to_string() const {
    return std::to_string(snap_id);
  }

  void init(uint64_t _snap_id) {
    snap_id = _snap_id;
  }

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

  /* note that we treat undefined snap id as bigger than defined snap ids */
  bool operator<(const rgw_bucket_snap_id& rhs) const {
    return snap_id < rhs.snap_id;
  }
  bool operator<=(const rgw_bucket_snap_id& rhs) const {
    return snap_id <= rhs.snap_id;
  }
  bool operator>(const rgw_bucket_snap_id& rhs) const {
    return snap_id > rhs.snap_id;
  }
  bool operator>=(const rgw_bucket_snap_id& rhs) const {
    return snap_id >= rhs.snap_id;
  }
  bool operator==(const rgw_bucket_snap_id& rhs) const {
    return snap_id == rhs.snap_id;
  }

  operator long long() const {
    return snap_id;
  }

  bool is_set() const {
    return snap_id != SNAP_UNDEFINED;
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_snap_id)

static inline std::ostream& operator<<(std::ostream& os, const rgw_bucket_snap_id& snap_id)
{
  os << (int64_t)snap_id.snap_id;
  return os;
}

static inline void encode_json(const char *name, const rgw_bucket_snap_id& val, ceph::Formatter *f)
{
  encode_json(name, val.snap_id, f);
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

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_bucket_snap_range);

static inline std::ostream& operator<<(std::ostream& os, const rgw_bucket_snap_range& snap_range)
{
  os << "[" << snap_range.start << "," << snap_range.end << "]";
  return os;
}

struct rgw_bucket_snap_info {
  std::string name;
  std::string description;
  ceph::real_time creation_time;
  uint32_t flags;

  enum Flags {
    MARKED_FOR_REMOVAL = 0x1,
  };

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    encode(description, bl);
    encode(creation_time, bl);
    encode(flags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(name, bl);
    decode(description, bl);
    decode(creation_time, bl);
    decode(flags, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_bucket_snap_info)

struct rgw_bucket_snap {
  rgw_bucket_snap_id id;
  rgw_bucket_snap_info info;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(info, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(info, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_bucket_snap)

struct rgw_bucket_snap_revert_info {
  rgw_bucket_snap_id id;
  rgw_bucket_snap_id revert_to_id;
  std::string description;
  ceph::real_time creation_time;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(revert_to_id, bl);
    encode(description, bl);
    encode(creation_time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(revert_to_id, bl);
    decode(description, bl);
    decode(creation_time, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_bucket_snap_revert_info);
