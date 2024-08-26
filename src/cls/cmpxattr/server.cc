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
#include "server.h"
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

static int cmp_vals_set_vals(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cmp_vals_set_vals_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const buffer::error&) {
    CLS_LOG(0, "ERROR: %s: failed to decode input", __func__);
    return -EINVAL;
  }

  // read the values for each key to compare
  std::map<std::string, bufferlist> cmp_values;
  for (const auto& kv : op.cmp_pairs) {
    const std::string &key = kv.first;
    ceph::bufferlist bl;
    int ret = cls_cxx_getxattr(hctx, key.c_str(), &bl);
    if (ret < 0) {
      CLS_LOG(4, "ERROR: %s: cls_cxx_getxattr() for key=%s ret=%d",
	      __func__, key.c_str(), ret);
      return -1;
    } else {
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

    int ret = compare_value(op.mode, op.comparison, input, value);
    if (ret <= 0) {
      // unsuccessful comparison
      CLS_LOG(10, "%s:: failed compare key=%s ret=%d", __func__, key.c_str(), ret);
      return -1;
    }

    // successful comparison
    CLS_LOG(20, "%s:: successful comparison key=%s", __func__, key.c_str());
  }

  // if arrived here all keys in the cmp_pairs passed check
  // overwrite all key/values in the set_pairs
  for (const auto& [key, value] : op.set_pairs) {
    int ret = cls_cxx_setxattr(hctx, key.c_str(), &value);
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

//===========================================================================
// A server side locking facility using the server internal clock.
// This guarantees a consistent view over multiple unsynchronized RGWs
//
// Create a lock object if doesn't exist with your name and curr time
// If exists and you are the owner -> update time to now
// If exists and you are *NOT* the owner:
//    -> If duration since lock-time is higher than allowed_duration:
//         -> Break the lock and set a new lock under your name with curr time
//    -> Otherwise, fail operation
static int lock_update(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  lock_update_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const buffer::error&) {
    CLS_LOG(0, "%s:: failed to decode input", __func__);
    return -EINVAL;
  }

  bool lock_exists = false;
  named_time_lock_t curr_lock;
  {
    ceph::bufferlist bl;
    int ret = cls_cxx_getxattr(hctx, op.key_name.c_str(), &bl);
    CLS_LOG(10, "%s::caller info: key=%s, owner=%s max_timeout={%d, %d}",
	    __func__, op.key_name.c_str(), op.owner.c_str(),
	    op.max_lock_duration.tv.tv_sec, op.max_lock_duration.tv.tv_nsec);

    if (ret >= 0) {
      lock_exists = true;
      try {
	auto p = bl.cbegin();
	decode(curr_lock, p);
      } catch (const buffer::error&) {
	CLS_LOG(0, "%s:: failed to decode named_time_lock", __func__);
	// TBD: should we force lock???
	return -EINVAL;
      }
      utime_t duration = ceph_clock_now() - curr_lock.lock_time;
      CLS_LOG(10, "%s::lock info: owner=%s duration={%d, %d} max_duration={%d, %d}",
	      __func__,
	      curr_lock.owner.c_str(), duration.tv.tv_sec, duration.tv.tv_nsec,
	      curr_lock.max_lock_duration.tv.tv_sec,
	      curr_lock.max_lock_duration.tv.tv_nsec);
    }
    else {
      lock_exists = false;
      curr_lock.owner = op.owner;
      // No lock exists, set the op values as the object lock paramters
      CLS_LOG(10, "%s::No Lock was found for key=%s (ret=%d) -> Setting a new lock!",
	      __func__, op.key_name.c_str(), ret);
    }
  }

  if (lock_exists && (curr_lock.owner != op.owner)) {
    // attempt and break the lock
    utime_t duration = ceph_clock_now() - curr_lock.lock_time;
    if (duration < curr_lock.max_lock_duration) {
      CLS_LOG(10, "%s::Failed lock for new_owner=%s (curr_owner=%s)",
	      __func__, op.owner.c_str(), curr_lock.owner.c_str());
      return -EBUSY;
    }
    else {
      CLS_LOG(10, "%s::Broke lock! prev owner=%s, new owner=%s",
	      __func__, curr_lock.owner.c_str(), op.owner.c_str());
    }
  }

  if (op.op_flags.is_set_lock()) {
    curr_lock.owner = op.owner;
    curr_lock.max_lock_duration = op.max_lock_duration;
    curr_lock.lock_time = ceph_clock_now();
    bufferlist bl;
    encode(curr_lock, bl);
    int ret = cls_cxx_setxattr(hctx, op.key_name.c_str(), &bl);
    if (ret == 0) {
      CLS_LOG(20, "%s::successfully set xattr key=%s", __func__, op.key_name.c_str());
    }
    else {
      CLS_LOG(4, "%s::failed to set xattr key=%s ret=%d (%s)",
	      __func__, op.key_name.c_str(), ret, cpp_strerror(ret).c_str());
      return ret;
    }
  }

  if (op.op_flags.is_set_epoch()) {
    utime_t epoch = ceph_clock_now();
    bufferlist time_bl;
    encode(epoch, time_bl);
    int ret = cls_cxx_setxattr(hctx, "epoch", &time_bl);
    if (ret != 0) {
      CLS_LOG(4, "%s::failed to set xattr epoch ret=%d (%s)",
	      __func__, ret, cpp_strerror(ret).c_str());
      return ret;
    }
    CLS_LOG(20, "%s::successfully set epoch xattr - {%d:%d}",
	    __func__, epoch.tv.tv_sec, epoch.tv.tv_nsec);
  }

  if (op.op_flags.is_mark_completed()) {
    ceph_assert(op.in_bl.length());
    int ret = cls_cxx_setxattr(hctx, "completion_stats", &op.in_bl);
    if (ret != 0) {
      CLS_LOG(4, "%s::failed to set xattr completion_time ret=%d (%s)",
	      __func__, ret, cpp_strerror(ret).c_str());
      return ret;
    }
    CLS_LOG(20, "%s::successfully set completion_stats", __func__);
  }

  return 0;
}

CLS_INIT(cmpxattr)
{
  CLS_LOG(1, "Loaded cmpxattr class!");

  cls_handle_t h_class;
  cls_method_handle_t h_cmp_vals_set_vals;
  cls_method_handle_t h_lock_update;

  cls_register("cmpxattr", &h_class);

  cls_register_cxx_method(h_class, "cmp_vals_set_vals", CLS_METHOD_RD | CLS_METHOD_WR,
			  cmp_vals_set_vals, &h_cmp_vals_set_vals);
  cls_register_cxx_method(h_class, "lock_update", CLS_METHOD_RD | CLS_METHOD_WR,
			  lock_update, &h_lock_update);
}
