// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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
#include "common/versioned_variant.h"

namespace rgw {

const int32_t BIShardIdent::NULL_ID = -1;


// BucketIndexType
std::string_view to_string(const BucketIndexType& t)
{
  switch (t) {
  case BucketIndexType::Hashed: return "Hashed";
  case BucketIndexType::Indexless: return "Indexless";
  case BucketIndexType::Ordered: return "Ordered";
  default: return "Unknown";
  }
}
bool parse(std::string_view str, BucketIndexType& t)
{
  if (boost::iequals(str, "Hashed") ||
      boost::iequals(str, "Normal"))
  {
    // Normal was used historically up to tentacle
    t = BucketIndexType::Hashed;
    return true;
  }
  if (boost::iequals(str, "Indexless")) {
    t = BucketIndexType::Indexless;
    return true;
  }
  if (boost::iequals(str, "Ordered")) {
    t = BucketIndexType::Ordered;
    return true;
  }
  return false;
}
void parse(std::string_view str, std::optional<BucketIndexType>& val) {
  BucketIndexType v;
  bool result = parse(str, v);
  if (result) {
    val = v;
  } else {
    val = std::nullopt;
  }
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

// bucket_index_hashed_layout
void encode(const bucket_index_hashed_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(2, 1, bl);
  encode(l.num_shards, bl);
  encode(l.hash_type, bl);
  encode(l.min_num_shards, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_index_hashed_layout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(2, bl);
  decode(l.num_shards, bl);
  decode(l.hash_type, bl);
  if (struct_v >= 2) {
    decode(l.min_num_shards, bl);
  }
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const bucket_index_hashed_layout& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("num_shards", l.num_shards, f);
  encode_json("hash_type", l.hash_type, f);
  encode_json("min_num_shards", l.min_num_shards, f);
  f->close_section();
}
void decode_json_obj(bucket_index_hashed_layout& l, JSONObj *obj)
{
  JSONDecoder::decode_json("num_shards", l.num_shards, obj);
  JSONDecoder::decode_json("hash_type", l.hash_type, obj);

  // if not set in json, set to default value of 1
  JSONDecoder::decode_json("min_num_shards", l.min_num_shards, obj, 1);
}

// bucket_index_hashed_layout
void encode(const bucket_index_ordered_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.num_shards, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_index_ordered_layout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(l.num_shards, bl);
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const bucket_index_ordered_layout& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("num_shards", l.num_shards, f);
  f->close_section();
}
void decode_json_obj(bucket_index_ordered_layout& l, JSONObj *obj)
{
  JSONDecoder::decode_json("num_shards", l.num_shards, obj);
}


// bucket_index_layout
void encode(const bucket_index_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(2, 1, bl);
  encode(l.type, bl);
  ceph::converted_variant::encode(l.specs, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_index_layout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(2, bl);
  decode(l.type, bl);
  ceph::converted_variant::decode(l.specs, bl);
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const bucket_index_layout& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("type", l.type, f);
  switch(l.type) {
  case BucketIndexType::Hashed:
    encode_json("normal", std::get<bucket_index_hashed_layout>(l.specs), f);
    break;
  case BucketIndexType::Ordered:
    encode_json("normal", std::get<bucket_index_ordered_layout>(l.specs), f);
    break;
  case BucketIndexType::Indexless:
    // empty
    break;
  } // switch
  f->close_section();
}
void decode_json_obj(bucket_index_layout& l, JSONObj *obj)
{
  JSONDecoder::decode_json("type", l.type, obj);
  switch(l.type) {
  case BucketIndexType::Hashed:
  {
    bucket_index_hashed_layout temp;
    JSONDecoder::decode_json("normal", temp, obj);
    l.specs = temp;
  }
    break;
  case BucketIndexType::Ordered:
  {
    bucket_index_ordered_layout temp;
    JSONDecoder::decode_json("normal", temp, obj);
    l.specs = temp;
  }
  break;
  case BucketIndexType::Indexless:
    // empty
    break;
  } // switch
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
  ceph::converted_variant::encode(l.layout, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_index_log_layout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(l.gen, bl);
  ceph::converted_variant::decode(l.layout, bl);
  DECODE_FINISH(bl);
}
void encode_json_impl(const char *name, const bucket_index_log_layout& l, ceph::Formatter *f)
{
  f->open_object_section(name);
  encode_json("gen", l.gen, f);
  if (const bucket_index_hashed_layout* pval = std::get_if<bucket_index_hashed_layout>(&l.layout)) {
    encode_json("layout_type", BucketIndexType::Hashed, f);
    encode_json("layout", *pval, f);
  } else if (const bucket_index_ordered_layout* pval = std::get_if<bucket_index_ordered_layout>(&l.layout)) {
    encode_json("layout_type", BucketIndexType::Ordered, f);
    encode_json("layout", *pval, f);
  } else {
// OBI: best way to handle?
    encode_json("layout_type", "unknown", f);
  }
  f->close_section();
}
void decode_json_obj(bucket_index_log_layout& l, JSONObj *obj)
{
  JSONDecoder::decode_json("gen", l.gen, obj);
  BucketIndexType index_type;
  JSONDecoder::decode_json("layout_type", index_type, obj);

  bucket_index_hashed_layout hashed_layout;
  bucket_index_ordered_layout ordered_layout;

  switch (index_type) {
  case BucketIndexType::Hashed:
    JSONDecoder::decode_json("layout", hashed_layout, obj);
    l.layout = hashed_layout;
    break;
  case BucketIndexType::Ordered:
    JSONDecoder::decode_json("layout", ordered_layout, obj);
    l.layout = ordered_layout;
    break;
  default:
    throw JSONDecoder::err("unable to decode BucketIndexType");
  }
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
    if (l.current_index.layout.type == BucketIndexType::Hashed) {
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


std::ostream& operator<<(std::ostream& out, const bucket_index_layout& l) {
  out << "type=" << to_string(l.type) << ", typed_layout={ ";

  switch (l.type) {
  case BucketIndexType::Hashed:
  {
    const auto* p1 = std::get_if<bucket_index_hashed_layout>(&l.specs);
    if (p1) {
      out << *p1;
    } else {
      out << "ERROR: MISMATCHED VARIANT";
    }
  }
  break;
  case BucketIndexType::Ordered:
  {
    const auto* p2 = std::get_if<bucket_index_ordered_layout>(&l.specs);
    if (p2) {
      out << *p2;
    } else {
      out << "ERROR: MISMATCHED VARIANT";
    }
  }
  break;
  default:
    out << "ERROR: UNKNOWN VARIANT";
  }
  out << " }";
  return out;
}


BucketIndexType bucket_index_type(const LayoutVariant& l) {
  if (std::holds_alternative<bucket_index_hashed_layout>(l)) {
    return BucketIndexType::Hashed;
  } else if (std::holds_alternative<bucket_index_ordered_layout>(l)) {
    return BucketIndexType::Ordered;
  } else {
    throw std::invalid_argument("unknown layout variant");
  }
}


std::unique_ptr<BIShardIdent> BIShardIdent::get_null_shard() {
  return std::make_unique<HashedShardIdent>(NULL_ID);
}

void BIShardIdent::encode_base(const BIShardIdent* b, bufferlist& bl) {
  ENCODE_START(1, 1, bl);
  if (b) {
    ceph::encode(static_cast<uint8_t>(b->get_type()), bl);
    b->encode(bl);
  } else {
    ceph::encode(static_cast<uint8_t>(Type::NullIdent), bl);
  }
  ENCODE_FINISH(bl);
}

BIShardIdent* BIShardIdent::decode_base(bufferlist::const_iterator& bl) {
  BIShardIdent* b = nullptr;

  DECODE_START(1, bl);
  uint8_t type_helper;
  ceph::decode(type_helper, bl);

  Type type = static_cast<Type>(type_helper);
  if (type == Type::NullIdent) {
    return nullptr;
  }
        
  if (type == Type::HashedIdent) {
    b = new HashedShardIdent();
  } else if (type == Type::OrderedIdent) {
    b = new OrderedShardIdent();
  }
        
  if (b) {
    b->decode(bl);
  }
  DECODE_FINISH(bl);

  return b;
}

std::string HashedShardIdent::to_string() const {
  const static std::string prefix = "hashed_shard_id:";
  return prefix + std::to_string(index);
}

std::size_t HashedShardIdent::get_hash() const {
  return std::hash<HashedShardIdent>{}(*this);
}

bool HashedShardIdent::operator==(const BIShardIdent& rhs) const {
  auto& rhs2 = dynamic_cast<const HashedShardIdent&>(rhs);
  return index == rhs2.index;
}

bool HashedShardIdent::operator<(const BIShardIdent& rhs) const {
  auto& rhs2 = dynamic_cast<const HashedShardIdent&>(rhs);
  return index < rhs2.index;
}

void HashedShardIdent::encode(bufferlist& bl) const{
  ENCODE_START(1, 1, bl);
  ceph::encode(index, bl);
  ENCODE_FINISH(bl);
}

void HashedShardIdent::decode(bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  ceph::decode(index, bl);
  DECODE_FINISH(bl);
}

std::string OrderedShardIdent::to_string() const {
  std::stringstream ss;
  ss << "ordered_shard_id:";

  auto i = indices.cbegin();
  if (i == indices.cend()) {
    ss << "NULL";
  } else {
    ss << *i;

    while (++i != indices.cend()) {
      ss << "," << *i;
    }
  }

  return ss.str();
}

std::size_t OrderedShardIdent::get_hash() const {
  return std::hash<OrderedShardIdent>{}(*this);
}

bool OrderedShardIdent::operator==(const BIShardIdent& rhs) const {
  auto& rhs2 = dynamic_cast<const OrderedShardIdent&>(rhs);

  if (indices.size() != rhs2.indices.size()) {
    return false;
  }

  auto i1 = indices.cbegin();
  auto i2 = rhs2.indices.cbegin();
  for ( ; i1 != indices.cend() ; ++i1, ++i2) {
    if (*i1 != *i2) {
      return false;
    }
  }

  return true;
}

bool OrderedShardIdent::operator<(const BIShardIdent& rhs) const {
  const auto& rhs2 = dynamic_cast<const OrderedShardIdent&>(rhs);

  auto i1 = indices.cbegin();
  auto i2 = rhs2.indices.cbegin();
  for ( ; i1 != indices.cend() && i2 != rhs2.indices.cend() ; ++i1, ++i2) {
    if (*i1 < *i2) {
      return true;
    } else if (*i2 < *i1) {
      return false;
    }
  }

  // they're the same up to shared length

  return indices.size() < rhs2.indices.size();
}

void OrderedShardIdent::encode(bufferlist& bl) const{
  ENCODE_START(1, 1, bl);
  ceph::encode(indices, bl);
  ENCODE_FINISH(bl);
}

void OrderedShardIdent::decode(bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  ceph::decode(indices, bl);
  DECODE_FINISH(bl);
}

int create_layout_generation(
  int generation,
  BucketIndexType index_type,
  int num_shards,
  const bucket_index_layout& current_layout,
  bucket_index_layout_generation* result)
{
  result->gen = generation;
  result->layout.type = index_type;

  if (index_type == BucketIndexType::Hashed) {
    bucket_index_hashed_layout specs;
    specs.num_shards = num_shards;

    const bucket_index_hashed_layout* old_specs =
      std::get_if<bucket_index_hashed_layout>(&current_layout.specs);
    specs.min_num_shards = old_specs ? old_specs->min_num_shards : 11;

    result->layout.specs = specs;
  } else if (index_type == BucketIndexType::Ordered) {
    bucket_index_ordered_layout specs;
    specs.num_shards = num_shards;

    result->layout.specs = specs;
  } else {
    return -EINVAL;
  }

  return 0;
}


} // namespace rgw
