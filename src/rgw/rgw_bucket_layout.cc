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

#include <boost/algorithm/string.hpp>
#include "rgw_bucket_layout.h"
#include "include/utime.h"

namespace rgw {

// BucketIndexType
std::string_view to_string(const BucketIndexType& t)
{
  switch (t) {
  case BucketIndexType::Normal: return "Normal";
  case BucketIndexType::Indexless: return "Indexless";
  default: return "Unknown";
  }
}
bool parse(std::string_view str, BucketIndexType& t)
{
  if (boost::iequals(str, "Normal")) {
    t = BucketIndexType::Normal;
    return true;
  }
  if (boost::iequals(str, "Indexless")) {
    t = BucketIndexType::Indexless;
    return true;
  }
  return false;
}
void encode_json_impl(const char *name, const BucketIndexType& t, ceph::Formatter *f)
{
  encode_json(name, to_string(t), f);
}
void decode_json_obj(BucketIndexType& t, JSONObj *obj)
{
  std::string str;
  decode_json_obj(str, obj);
  parse(str, t);
}

// BucketHashType
std::string_view to_string(const BucketHashType& t)
{
  switch (t) {
  case BucketHashType::Mod: return "Mod";
  default: return "Unknown";
  }
}
bool parse(std::string_view str, BucketHashType& t)
{
  if (boost::iequals(str, "Mod")) {
    t = BucketHashType::Mod;
    return true;
  }
  return false;
}
void encode_json_impl(const char *name, const BucketHashType& t, ceph::Formatter *f)
{
  encode_json(name, to_string(t), f);
}
void decode_json_obj(BucketHashType& t, JSONObj *obj)
{
  std::string str;
  decode_json_obj(str, obj);
  parse(str, t);
}

// bucket_index_normal_layout
void encode(const bucket_index_normal_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(2, 1, bl);
  encode(l.num_shards, bl);
  encode(l.hash_type, bl);
  encode(l.min_num_shards, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_index_normal_layout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(2, bl);
  decode(l.num_shards, bl);
  decode(l.hash_type, bl);
  if (struct_v >= 2) {
    decode(l.min_num_shards, bl);
  }
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const bucket_index_normal_layout& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("num_shards", l.num_shards, f);
  encode_json("hash_type", l.hash_type, f);
  encode_json("min_num_shards", l.min_num_shards, f);
  f->close_section();
}
void decode_json_obj(bucket_index_normal_layout& l, JSONObj *obj)
{
  JSONDecoder::decode_json("num_shards", l.num_shards, obj);
  JSONDecoder::decode_json("hash_type", l.hash_type, obj);

  // if not set in json, set to default value of 1
  JSONDecoder::decode_json("min_num_shards", l.min_num_shards, obj, 1);
}

// bucket_index_layout
void encode(const bucket_index_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.type, bl);
  switch (l.type) {
  case BucketIndexType::Normal:
    encode(l.normal, bl);
    break;
  case BucketIndexType::Indexless:
    break;
  }
  ENCODE_FINISH(bl);
}
void decode(bucket_index_layout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(l.type, bl);
  switch (l.type) {
  case BucketIndexType::Normal:
    decode(l.normal, bl);
    break;
  case BucketIndexType::Indexless:
    break;
  }
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const bucket_index_layout& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("type", l.type, f);
  encode_json("normal", l.normal, f);
  f->close_section();
}
void decode_json_obj(bucket_index_layout& l, JSONObj *obj)
{
  JSONDecoder::decode_json("type", l.type, obj);
  JSONDecoder::decode_json("normal", l.normal, obj);
}

// bucket_index_layout_generation
void encode(const bucket_index_layout_generation& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.gen, bl);
  encode(l.layout, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_index_layout_generation& l, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(l.gen, bl);
  decode(l.layout, bl);
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const bucket_index_layout_generation& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("gen", l.gen, f);
  encode_json("layout", l.layout, f);
  f->close_section();
}
void decode_json_obj(bucket_index_layout_generation& l, JSONObj *obj)
{
  JSONDecoder::decode_json("gen", l.gen, obj);
  JSONDecoder::decode_json("layout", l.layout, obj);
}

// BucketLogType
std::string_view to_string(const BucketLogType& t)
{
  switch (t) {
  case BucketLogType::InIndex: return "InIndex";
  case BucketLogType::Deleted: return "Deleted";
  default: return "Unknown";
  }
}
bool parse(std::string_view str, BucketLogType& t)
{
  if (boost::iequals(str, "InIndex")) {
    t = BucketLogType::InIndex;
    return true;
  }
  if (boost::iequals(str, "Deleted")) {
    t = BucketLogType::Deleted;
    return true;
  }
  return false;
}
void encode_json_impl(const char *name, const BucketLogType& t, ceph::Formatter *f)
{
  encode_json(name, to_string(t), f);
}
void decode_json_obj(BucketLogType& t, JSONObj *obj)
{
  std::string str;
  decode_json_obj(str, obj);
  parse(str, t);
}

// bucket_index_log_layout
void encode(const bucket_index_log_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.gen, bl);
  encode(l.layout, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_index_log_layout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(l.gen, bl);
  decode(l.layout, bl);
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const bucket_index_log_layout& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("gen", l.gen, f);
  encode_json("layout", l.layout, f);
  f->close_section();
}
void decode_json_obj(bucket_index_log_layout& l, JSONObj *obj)
{
  JSONDecoder::decode_json("gen", l.gen, obj);
  JSONDecoder::decode_json("layout", l.layout, obj);
}

// bucket_log_layout
void encode(const bucket_log_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.type, bl);
  switch (l.type) {
  case BucketLogType::InIndex:
    encode(l.in_index, bl);
    break;
  case BucketLogType::Deleted:
    break;
  }
  ENCODE_FINISH(bl);
}
void decode(bucket_log_layout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(l.type, bl);
  switch (l.type) {
  case BucketLogType::InIndex:
    decode(l.in_index, bl);
    break;
  case BucketLogType::Deleted:
    break;
  }
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const bucket_log_layout& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("type", l.type, f);
  if (l.type == BucketLogType::InIndex) {
    encode_json("in_index", l.in_index, f);
  }
  f->close_section();
}
void decode_json_obj(bucket_log_layout& l, JSONObj *obj)
{
  JSONDecoder::decode_json("type", l.type, obj);
  if (l.type == BucketLogType::InIndex) {
    JSONDecoder::decode_json("in_index", l.in_index, obj);
  }
}

// bucket_log_layout_generation
void encode(const bucket_log_layout_generation& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.gen, bl);
  encode(l.layout, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_log_layout_generation& l, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(l.gen, bl);
  decode(l.layout, bl);
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const bucket_log_layout_generation& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("gen", l.gen, f);
  encode_json("layout", l.layout, f);
  f->close_section();
}
void decode_json_obj(bucket_log_layout_generation& l, JSONObj *obj)
{
  JSONDecoder::decode_json("gen", l.gen, obj);
  JSONDecoder::decode_json("layout", l.layout, obj);
}

// BucketReshardState
std::string_view to_string(const BucketReshardState& s)
{
  switch (s) {
  case BucketReshardState::None: return "None";
  case BucketReshardState::InLogrecord: return "InLogrecord";
  case BucketReshardState::InProgress: return "InProgress";
  default: return "Unknown";
  }
}
bool parse(std::string_view str, BucketReshardState& s)
{
  if (boost::iequals(str, "None")) {
    s = BucketReshardState::None;
    return true;
  }
  if (boost::iequals(str, "InLogrecord")) {
    s = BucketReshardState::InLogrecord;
    return true;
  }
  if (boost::iequals(str, "InProgress")) {
    s = BucketReshardState::InProgress;
    return true;
  }
  return false;
}
void encode_json_impl(const char *name, const BucketReshardState& s, ceph::Formatter *f)
{
  encode_json(name, to_string(s), f);
}
void decode_json_obj(BucketReshardState& s, JSONObj *obj)
{
  std::string str;
  decode_json_obj(str, obj);
  parse(str, s);
}


// BucketLayout
void encode(const BucketLayout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(3, 1, bl);
  encode(l.resharding, bl);
  encode(l.current_index, bl);
  encode(l.target_index, bl);
  encode(l.logs, bl);
  encode(l.judge_reshard_lock_time, bl);
  ENCODE_FINISH(bl);
}
void decode(BucketLayout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(3, bl);
  decode(l.resharding, bl);
  decode(l.current_index, bl);
  decode(l.target_index, bl);
  if (struct_v < 2) {
    l.logs.clear();
    // initialize the log layout to match the current index layout
    if (l.current_index.layout.type == BucketIndexType::Normal) {
      l.logs.push_back(log_layout_from_index(0, l.current_index));
    }
  } else {
    decode(l.logs, bl);
  }
  if (struct_v >= 3) {
    decode(l.judge_reshard_lock_time, bl);
  }
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const BucketLayout& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("resharding", l.resharding, f);
  encode_json("current_index", l.current_index, f);
  if (l.target_index) {
    encode_json("target_index", *l.target_index, f);
  }
  f->open_array_section("logs");
  for (const auto& log : l.logs) {
    encode_json("log", log, f);
  }
  f->close_section(); // logs[]
  utime_t jt(l.judge_reshard_lock_time);
  encode_json("judge_reshard_lock_time", jt, f);
  f->close_section();
}
void decode_json_obj(BucketLayout& l, JSONObj *obj)
{
  JSONDecoder::decode_json("resharding", l.resharding, obj);
  JSONDecoder::decode_json("current_index", l.current_index, obj);
  JSONDecoder::decode_json("target_index", l.target_index, obj);
  JSONDecoder::decode_json("logs", l.logs, obj);
  utime_t ut;
  JSONDecoder::decode_json("judge_reshard_lock_time", ut, obj);
  l.judge_reshard_lock_time = ut.to_real_time();
}

} // namespace rgw
