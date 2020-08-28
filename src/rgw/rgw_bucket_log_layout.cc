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

#include "rgw_bucket_log_layout.h"

namespace rgw {

void encode(const bucket_log_index_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.num_shards, bl);
  encode(l.log_hash_type, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_log_index_layout& l, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(l.num_shards, bl);
  decode(l.log_hash_type, bl);
  DECODE_FINISH(bl);
}

void encode(const bucket_log_layout& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.type, bl);
  switch (l.type) {
  case BucketLogType::InIndex:
    encode(l.index_log, bl);
    break;
  case BucketLogType::FIFO:
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
    decode(l.index_log, bl);
    break;
  case BucketLogType::FIFO:
    break;
  }
  DECODE_FINISH(bl);
}

void encode(const bucket_log_layout_generation& l, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(l.log_gen, bl);
  encode(l.log_layout, bl);
  ENCODE_FINISH(bl);
}
void decode(bucket_log_layout_generation& l, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(l.log_gen, bl);
  decode(l.log_layout, bl);
  DECODE_FINISH(bl);
}

} // namespace rgw
