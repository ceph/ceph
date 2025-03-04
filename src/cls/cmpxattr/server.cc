// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "objclass/objclass.h"
#include "ops.h"
#include "common/errno.h"
CLS_VER(1,0)
CLS_NAME(cmpxattr)

using namespace cls::cmpxattr;

// returns negative error codes or 0/1 for failed/successful comparisons
template <typename T>
static int compare_values(Op op, const T& lhs, const T& rhs)
{
  switch (op) {
  case Op::EQ:  return (lhs == rhs);
  case Op::NE:  return (lhs != rhs);
  case Op::GT:  return (lhs > rhs);
  case Op::GTE: return (lhs >= rhs);
  case Op::LT:  return (lhs < rhs);
  case Op::LTE: return (lhs <= rhs);
  default:      return -EINVAL;
  }
}

static int compare_values_u64(Op op, uint64_t lhs, const bufferlist& value)
{
  // empty values compare as 0 for backward compat
  uint64_t rhs = 0;
  if (value.length()) {
    try {
      // decode existing value as rhs
      auto p = value.cbegin();
      using ceph::decode;
      decode(rhs, p);
    } catch (const buffer::error&) {
      // failures to decode existing values are reported as EIO
      return -EIO;
    }
  }
  return compare_values(op, lhs, rhs);
}

static int compare_value(Mode mode, Op op, const bufferlist& input,
			 const bufferlist& value)
{
  switch (mode) {
  case Mode::String:
    return compare_values(op, input, value);
  case Mode::U64:
    try {
      // decode input value as lhs
      uint64_t lhs;
      auto p = input.cbegin();
      using ceph::decode;
      decode(lhs, p);
      return compare_values_u64(op, lhs, value);
    } catch (const buffer::error&) {
      // failures to decode input values are reported as EINVAL
      return -EINVAL;
    }
  default:
    return -EINVAL;
  }
}

//-------------------------------------------------------------------------------------
static int cmp_vals_set_vals(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cmp_vals_set_vals_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const buffer::error&) {
    CLS_LOG(4, "ERROR: %s: failed to decode input", __func__);
    return -EINVAL;
  }

  // read the values for each key to compare
  std::map<std::string, bufferlist> cmp_values;
  for (const auto& kv : op.cmp_pairs) {
    const std::string &key = kv.first;
    ceph::bufferlist bl;
    int ret = cls_cxx_getxattr(hctx, key.c_str(), &bl);
    if (ret < 0 && ret != -ENODATA) {
      CLS_LOG(4, "ERROR: %s: cls_cxx_getxattr() for key=%s ret=%d",
	      __func__, key.c_str(), ret);
      return ret;
    } else {
      // ENODATA will generate an empty value bl
      cmp_values[key] = bl;
    }
  }

  auto v = cmp_values.begin();
  for (const auto& [key, input] : op.cmp_pairs) {
    auto k = cmp_values.end();
    bufferlist value;
    if (v != cmp_values.end() && v->first == key) {
      value = std::move(v->second);
      k = v++;
      CLS_LOG(20, "%s::comparing key=%s mode=%d op=%d",
	      __func__, key.c_str(), (int)op.mode, (int)op.comparison);
    } else {
      CLS_LOG(10, "%s:: missing key=%s, abort operation", __func__, key.c_str());
      return -EINVAL;
    }

    int ret = 0;
    // an empty input value will match an empty value or a non-existing key
    if (input.length() == 0 && (op.comparison == Op::EQ)) {
      if (value.length() == 0) {
	ret = true;
      }
      else {
	CLS_LOG(10, "%s:: key=%s exists!", __func__, key.c_str());
	return -EEXIST;
      }
    }
    else {
      ret = compare_value(op.mode, op.comparison, input, value);
    }
    if (ret <= 0) {
      // unsuccessful comparison
      CLS_LOG(10, "%s:: failed compare key=%s ret=%d", __func__, key.c_str(), ret);
      // set the failing key in the returned bl
      encode(key, *out);
      encode(value, *out);
      return ret;
    }

    // successful comparison
    CLS_LOG(20, "%s:: successful comparison key=%s", __func__, key.c_str());
  }

  // if arrived here all keys in the cmp_pairs passed check
  // overwrite all key/values in the set_pairs
  for (const auto& [key, value] : op.set_pairs) {
    auto inbl = const_cast<ceph::bufferlist *>(&value);
    int ret = cls_cxx_setxattr(hctx, key.c_str(), inbl);
    if (ret == 0) {
      CLS_LOG(20, "%s:: successful set xattr key=%s", __func__, key.c_str());
    }
    else {
      CLS_LOG(4, "ERROR: %s failed to set xattr key=%s ret=%d", __func__, key.c_str(), ret);
      return ret;
    }
  }

  return 0;
}

CLS_INIT(cmpxattr)
{
  CLS_LOG(10, "Loaded cmpxattr class!");
  cls_handle_t h_class;
  cls_method_handle_t h_cmp_vals_set_vals;
  cls_register("cmpxattr", &h_class);
  cls_register_cxx_method(h_class, "cmp_vals_set_vals", CLS_METHOD_RD | CLS_METHOD_WR,
			  cmp_vals_set_vals, &h_cmp_vals_set_vals);
}
