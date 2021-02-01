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

#include "rgw_bucket_layout.h"

namespace rgw {

void encode(const bucket_index_normal_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.num_shards, bl);
  encode(l.hash_type, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_index_normal_layout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(l.num_shards, bl);
  decode(l.hash_type, bl);
  DECODE_FINISH(bl);
}

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

void encode(const bucket_log_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.type, bl);
  switch (l.type) {
  case BucketLogType::InIndex:
    encode(l.in_index, bl);
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
  }
  DECODE_FINISH(bl);
}

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

void encode(const BucketLayout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(2, 1, bl);
  encode(l.resharding, bl);
  encode(l.current_index, bl);
  encode(l.target_index, bl);
  encode(l.logs, bl);
  ENCODE_FINISH(bl);
}
void decode(BucketLayout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(2, bl);
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
  DECODE_FINISH(bl);
}

} // namespace rgw
