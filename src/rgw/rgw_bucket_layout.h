// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/* N.B., this header defines fundamental serialized types.  Do not
 * introduce changes or include files which can only be compiled in
 * radosgw or OSD contexts (e.g., rgw_sal.h, rgw_common.h)
 */

#pragma once

#include <optional>
#include <string>
#include "include/encoding.h"
#include "common/ceph_json.h"

namespace rgw {

enum class BucketIndexType : uint8_t {
  Normal, // normal hash-based sharded index layout
  Indexless, // no bucket index, so listing is unsupported
};

std::string_view to_string(const BucketIndexType& t);
bool parse(std::string_view str, BucketIndexType& t);
void encode_json_impl(const char *name, const BucketIndexType& t, ceph::Formatter *f);
void decode_json_obj(BucketIndexType& t, JSONObj *obj);

inline std::ostream& operator<<(std::ostream& out, const BucketIndexType& t)
{
  return out << to_string(t);
}

enum class BucketHashType : uint8_t {
  Mod, // rjenkins hash of object name, modulo num_shards
};

std::string_view to_string(const BucketHashType& t);
bool parse(std::string_view str, BucketHashType& t);
void encode_json_impl(const char *name, const BucketHashType& t, ceph::Formatter *f);
void decode_json_obj(BucketHashType& t, JSONObj *obj);

struct bucket_index_normal_layout {
  uint32_t num_shards = 1;

  BucketHashType hash_type = BucketHashType::Mod;

  friend std::ostream& operator<<(std::ostream& out, const bucket_index_normal_layout& l) {
    out << "num_shards=" << l.num_shards << ", hash_type=" << to_string(l.hash_type);
    return out;
  }
};

inline bool operator==(const bucket_index_normal_layout& l,
                       const bucket_index_normal_layout& r) {
  return l.num_shards == r.num_shards
      && l.hash_type == r.hash_type;
}
inline bool operator!=(const bucket_index_normal_layout& l,
                       const bucket_index_normal_layout& r) {
  return !(l == r);
}

void encode(const bucket_index_normal_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_normal_layout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_index_normal_layout& l, ceph::Formatter *f);
void decode_json_obj(bucket_index_normal_layout& l, JSONObj *obj);

struct bucket_index_layout {
  BucketIndexType type = BucketIndexType::Normal;

  // TODO: variant of layout types?
  bucket_index_normal_layout normal;

  friend std::ostream& operator<<(std::ostream& out, const bucket_index_layout& l) {
    out << "type=" << to_string(l.type) << ", normal=" << l.normal;
    return out;
  }
};

inline bool operator==(const bucket_index_layout& l,
                       const bucket_index_layout& r) {
  return l.type == r.type && l.normal == r.normal;
}
inline bool operator!=(const bucket_index_layout& l,
                       const bucket_index_layout& r) {
  return !(l == r);
}

void encode(const bucket_index_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_layout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_index_layout& l, ceph::Formatter *f);
void decode_json_obj(bucket_index_layout& l, JSONObj *obj);

struct bucket_index_layout_generation {
  uint64_t gen = 0;
  bucket_index_layout layout;

  friend std::ostream& operator<<(std::ostream& out, const bucket_index_layout_generation& g) {
    out << "gen=" << g.gen;
    return out;
  }
};

inline bool operator==(const bucket_index_layout_generation& l,
                       const bucket_index_layout_generation& r) {
  return l.gen == r.gen && l.layout == r.layout;
}
inline bool operator!=(const bucket_index_layout_generation& l,
                       const bucket_index_layout_generation& r) {
  return !(l == r);
}

void encode(const bucket_index_layout_generation& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_layout_generation& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_index_layout_generation& l, ceph::Formatter *f);
void decode_json_obj(bucket_index_layout_generation& l, JSONObj *obj);


enum class BucketLogType : uint8_t {
  // colocated with bucket index, so the log layout matches the index layout
  InIndex,
};

std::string_view to_string(const BucketLogType& t);
bool parse(std::string_view str, BucketLogType& t);
void encode_json_impl(const char *name, const BucketLogType& t, ceph::Formatter *f);
void decode_json_obj(BucketLogType& t, JSONObj *obj);

inline std::ostream& operator<<(std::ostream& out, const BucketLogType &log_type)
{
  switch (log_type) {
    case BucketLogType::InIndex:
      return out << "InIndex";
    default:
      return out << "Unknown";
  }
}

struct bucket_index_log_layout {
  uint64_t gen = 0;
  bucket_index_normal_layout layout;
  operator bucket_index_layout_generation() const {
    bucket_index_layout_generation bilg;
    bilg.gen = gen;
    bilg.layout.type = BucketIndexType::Normal;
    bilg.layout.normal = layout;
    return bilg;
  }
};

void encode(const bucket_index_log_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_log_layout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_index_log_layout& l, ceph::Formatter *f);
void decode_json_obj(bucket_index_log_layout& l, JSONObj *obj);

struct bucket_log_layout {
  BucketLogType type = BucketLogType::InIndex;

  bucket_index_log_layout in_index;

  friend std::ostream& operator<<(std::ostream& out, const bucket_log_layout& l) {
    out << "type=" << to_string(l.type);
    return out;
  }
};

void encode(const bucket_log_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_log_layout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_log_layout& l, ceph::Formatter *f);
void decode_json_obj(bucket_log_layout& l, JSONObj *obj);

struct bucket_log_layout_generation {
  uint64_t gen = 0;
  bucket_log_layout layout;

  friend std::ostream& operator<<(std::ostream& out, const bucket_log_layout_generation& g) {
    out << "gen=" << g.gen << ", layout=[ " << g.layout << " ]";
    return out;
  }
};

void encode(const bucket_log_layout_generation& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_log_layout_generation& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const bucket_log_layout_generation& l, ceph::Formatter *f);
void decode_json_obj(bucket_log_layout_generation& l, JSONObj *obj);

// return a log layout that shares its layout with the index
inline bucket_log_layout_generation log_layout_from_index(
    uint64_t gen, const bucket_index_layout_generation& index)
{
  return {gen, {BucketLogType::InIndex, {index.gen, index.layout.normal}}};
}

inline auto matches_gen(uint64_t gen)
{
  return [gen] (const bucket_log_layout_generation& l) { return l.gen == gen; };
}

inline bucket_index_layout_generation log_to_index_layout(const bucket_log_layout_generation& log_layout)
{
  ceph_assert(log_layout.layout.type == BucketLogType::InIndex);
  bucket_index_layout_generation index;
  index.gen = log_layout.layout.in_index.gen;
  index.layout.normal = log_layout.layout.in_index.layout;
  return index;
}

enum class BucketReshardState : uint8_t {
  None,
  InProgress,
};
std::string_view to_string(const BucketReshardState& s);
bool parse(std::string_view str, BucketReshardState& s);
void encode_json_impl(const char *name, const BucketReshardState& s, ceph::Formatter *f);
void decode_json_obj(BucketReshardState& s, JSONObj *obj);

// describes the layout of bucket index objects
struct BucketLayout {
  BucketReshardState resharding = BucketReshardState::None;

  // current bucket index layout
  bucket_index_layout_generation current_index;

  // target index layout of a resharding operation
  std::optional<bucket_index_layout_generation> target_index;

  // history of untrimmed bucket log layout generations, with the current
  // generation at the back()
  std::vector<bucket_log_layout_generation> logs;

  friend std::ostream& operator<<(std::ostream& out, const BucketLayout& l) {
    std::stringstream ss;
    if (l.target_index) {
      ss << *l.target_index;
    } else {
      ss << "none";
    }
    out << "resharding=" << to_string(l.resharding) <<
      ", current_index=[" << l.current_index << "], target_index=[" <<
      ss.str() << "], logs.size()=" << l.logs.size();

    return out;
  }
};

void encode(const BucketLayout& l, bufferlist& bl, uint64_t f=0);
void decode(BucketLayout& l, bufferlist::const_iterator& bl);
void encode_json_impl(const char *name, const BucketLayout& l, ceph::Formatter *f);
void decode_json_obj(BucketLayout& l, JSONObj *obj);


inline uint32_t num_shards(const bucket_index_normal_layout& index) {
  // old buckets used num_shards=0 to mean 1
  return index.num_shards > 0 ? index.num_shards : 1;
}
inline uint32_t num_shards(const bucket_index_layout& index) {
  ceph_assert(index.type == BucketIndexType::Normal);
  return num_shards(index.normal);
}
inline uint32_t num_shards(const bucket_index_layout_generation& index) {
  return num_shards(index.layout);
}
inline uint32_t current_num_shards(const BucketLayout& layout) {
  return num_shards(layout.current_index);
}
inline bool is_layout_indexless(const bucket_index_layout_generation& layout) {
  return layout.layout.type == BucketIndexType::Indexless;
}
inline bool is_layout_reshardable(const bucket_index_layout_generation& layout) {
  return layout.layout.type == BucketIndexType::Normal;
}
inline bool is_layout_reshardable(const BucketLayout& layout) {
  return is_layout_reshardable(layout.current_index);
}
inline std::string_view current_layout_desc(const BucketLayout& layout) {
  return rgw::to_string(layout.current_index.layout.type);
}

} // namespace rgw
