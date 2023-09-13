// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include "types.h"
#include "include/encoding.h"

namespace cls::cmpomap {

struct cmp_vals_op {
  Mode mode;
  Op comparison;
  ComparisonMap values;
  std::optional<ceph::bufferlist> default_value;
};

inline void encode(const cmp_vals_op& o, ceph::bufferlist& bl, uint64_t f=0)
{
  ENCODE_START(1, 1, bl);
  encode(o.mode, bl);
  encode(o.comparison, bl);
  encode(o.values, bl);
  encode(o.default_value, bl);
  ENCODE_FINISH(bl);
}

inline void decode(cmp_vals_op& o, ceph::bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(o.mode, bl);
  decode(o.comparison, bl);
  decode(o.values, bl);
  decode(o.default_value, bl);
  DECODE_FINISH(bl);
}

struct cmp_set_vals_op {
  Mode mode;
  Op comparison;
  ComparisonMap values;
  std::optional<ceph::bufferlist> default_value;
};

inline void encode(const cmp_set_vals_op& o, ceph::bufferlist& bl, uint64_t f=0)
{
  ENCODE_START(1, 1, bl);
  encode(o.mode, bl);
  encode(o.comparison, bl);
  encode(o.values, bl);
  encode(o.default_value, bl);
  ENCODE_FINISH(bl);
}

inline void decode(cmp_set_vals_op& o, ceph::bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(o.mode, bl);
  decode(o.comparison, bl);
  decode(o.values, bl);
  decode(o.default_value, bl);
  DECODE_FINISH(bl);
}

struct cmp_rm_keys_op {
  Mode mode;
  Op comparison;
  ComparisonMap values;
};

inline void encode(const cmp_rm_keys_op& o, ceph::bufferlist& bl, uint64_t f=0)
{
  ENCODE_START(1, 1, bl);
  encode(o.mode, bl);
  encode(o.comparison, bl);
  encode(o.values, bl);
  ENCODE_FINISH(bl);
}

inline void decode(cmp_rm_keys_op& o, ceph::bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(o.mode, bl);
  decode(o.comparison, bl);
  decode(o.values, bl);
  DECODE_FINISH(bl);
}

} // namespace cls::cmpomap
