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

#include "include/rados/librados.hpp"
#include "client.h"
#include "ops.h"

namespace cls::cmpomap {

int cmp_vals(librados::ObjectReadOperation& op,
             Mode mode, Op comparison, ComparisonMap values,
             std::optional<ceph::bufferlist> default_value)
{
  if (values.size() > max_keys) {
    return -E2BIG;
  }
  cmp_vals_op call;
  call.mode = mode;
  call.comparison = comparison;
  call.values = std::move(values);
  call.default_value = std::move(default_value);

  bufferlist in;
  encode(call, in);
  op.exec("cmpomap", "cmp_vals", in);
  return 0;
}

int cmp_set_vals(librados::ObjectWriteOperation& op,
                 Mode mode, Op comparison, ComparisonMap values,
                 std::optional<ceph::bufferlist> default_value)
{
  if (values.size() > max_keys) {
    return -E2BIG;
  }
  cmp_set_vals_op call;
  call.mode = mode;
  call.comparison = comparison;
  call.values = std::move(values);
  call.default_value = std::move(default_value);

  bufferlist in;
  encode(call, in);
  op.exec("cmpomap", "cmp_set_vals", in);
  return 0;
}

int cmp_rm_keys(librados::ObjectWriteOperation& op,
                Mode mode, Op comparison, ComparisonMap values)
{
  if (values.size() > max_keys) {
    return -E2BIG;
  }
  cmp_rm_keys_op call;
  call.mode = mode;
  call.comparison = comparison;
  call.values = std::move(values);

  bufferlist in;
  encode(call, in);
  op.exec("cmpomap", "cmp_rm_keys", in);
  return 0;
}

} // namespace cls::cmpomap
