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

#include "objclass/objclass.h"
#include "ops.h"

CLS_VER(1,0)
CLS_NAME(cmpomap)

using namespace cls::cmpomap;

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

static int cmp_vals(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cmp_vals_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const buffer::error&) {
    CLS_LOG(1, "ERROR: cmp_vals(): failed to decode input");
    return -EINVAL;
  }

  // collect the keys we need to read
  std::set<std::string> keys;
  for (const auto& kv : op.values) {
    keys.insert(kv.first);
  }

  // read the values for each key to compare
  std::map<std::string, bufferlist> values;
  int r = cls_cxx_map_get_vals_by_keys(hctx, keys, &values);
  if (r < 0) {
    CLS_LOG(4, "ERROR: cmp_vals() failed to read values r=%d", r);
    return r;
  }

  auto v = values.cbegin();
  for (const auto& [key, input] : op.values) {
    bufferlist value;
    if (v != values.end() && v->first == key) {
      value = std::move(v->second);
      ++v;
      CLS_LOG(20, "cmp_vals() comparing key=%s mode=%d op=%d",
              key.c_str(), (int)op.mode, (int)op.comparison);
    } else if (!op.default_value) {
      CLS_LOG(20, "cmp_vals() missing key=%s", key.c_str());
      return -ECANCELED;
    } else {
      // use optional default for missing keys
      value = *op.default_value;
      CLS_LOG(20, "cmp_vals() comparing missing key=%s mode=%d op=%d",
              key.c_str(), (int)op.mode, (int)op.comparison);
    }

    r = compare_value(op.mode, op.comparison, input, value);
    if (r < 0) {
      CLS_LOG(10, "cmp_vals() failed to compare key=%s r=%d", key.c_str(), r);
      return r;
    }
    if (r == 0) {
      CLS_LOG(10, "cmp_vals() comparison at key=%s returned false", key.c_str());
      return -ECANCELED;
    }
    CLS_LOG(20, "cmp_vals() comparison at key=%s returned true", key.c_str());
  }

  return 0;
}

static int cmp_set_vals(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cmp_set_vals_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const buffer::error&) {
    CLS_LOG(1, "ERROR: cmp_set_vals(): failed to decode input");
    return -EINVAL;
  }

  // collect the keys we need to read
  std::set<std::string> keys;
  for (const auto& kv : op.values) {
    keys.insert(kv.first);
  }

  // read the values for each key to compare
  std::map<std::string, bufferlist> values;
  int r = cls_cxx_map_get_vals_by_keys(hctx, keys, &values);
  if (r < 0) {
    CLS_LOG(4, "ERROR: cmp_set_vals() failed to read values r=%d", r);
    return r;
  }

  auto v = values.begin();
  for (const auto& [key, input] : op.values) {
    auto k = values.end();
    bufferlist value;
    if (v != values.end() && v->first == key) {
      value = std::move(v->second);
      k = v++;
      CLS_LOG(20, "cmp_set_vals() comparing key=%s mode=%d op=%d",
              key.c_str(), (int)op.mode, (int)op.comparison);
    } else if (!op.default_value) {
      CLS_LOG(20, "cmp_set_vals() missing key=%s", key.c_str());
      continue;
    } else {
      // use optional default for missing keys
      value = *op.default_value;
      CLS_LOG(20, "cmp_set_vals() comparing missing key=%s mode=%d op=%d",
              key.c_str(), (int)op.mode, (int)op.comparison);
    }

    r = compare_value(op.mode, op.comparison, input, value);
    if (r == -EIO) {
      r = 0; // treat EIO as a failed comparison
    }
    if (r < 0) {
      CLS_LOG(10, "cmp_set_vals() failed to compare key=%s r=%d",
              key.c_str(), r);
      return r;
    }
    if (r == 0) {
      // unsuccessful comparison
      if (k != values.end()) {
        values.erase(k); // remove this key from the values to overwrite
        CLS_LOG(20, "cmp_set_vals() not overwriting key=%s", key.c_str());
      } else {
        CLS_LOG(20, "cmp_set_vals() not writing missing key=%s", key.c_str());
      }
    } else {
      // successful comparison
      if (k != values.end()) {
        // overwrite the value
        k->second = std::move(input);
        CLS_LOG(20, "cmp_set_vals() overwriting key=%s", key.c_str());
      } else {
        // insert the value
        values.emplace(key, std::move(input));
        CLS_LOG(20, "cmp_set_vals() overwriting missing key=%s", key.c_str());
      }
    }
  }

  if (values.empty()) {
    CLS_LOG(20, "cmp_set_vals() has no values to overwrite");
    return 0;
  }

  CLS_LOG(20, "cmp_set_vals() overwriting count=%d", (int)values.size());
  return cls_cxx_map_set_vals(hctx, &values);
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
  //int r = cls_cxx_map_get_vals_by_keys(hctx, cmp_keys, &cmp_values);
  for (const auto& kv : op.cmp_pairs) {
    const std::string &key = kv.first;
    //std::cerr << __func__ << "::calling cls_cxx_getxattr key=" << key << std::endl;
    ceph::bufferlist bl;
    //int ret = cls_cxx_map_get_val(hctx, key, &bl);
    int ret = cls_cxx_getxattr(hctx, key.c_str(), &bl);
    if (ret < 0) {
      CLS_LOG(4, "ERROR: %s: cls_cxx_getxattr() for key=%s ret=%d",
	      __func__, key.c_str(), ret);
      std::cerr << __func__ << "::failed getxattr() for key=" << key << ", ret=" << ret << std::endl;
      return -1;
    } else {
      cmp_values[key] = bl;
    }
  }
  //std::cerr << __func__ << "::cmp_values.size() = " << cmp_values.size() << std::endl;
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
      CLS_LOG(20, "%s:: missing key=%s, abort operation", __func__, key.c_str());
      std::cerr << __func__ << "::missing  key=" << key << std::endl;
      return -EINVAL;
    }

    int ret = compare_value(op.mode, op.comparison, input, value);
    if (ret <= 0) {
      // unsuccessful comparison
      CLS_LOG(10, "%s:: failed compare key=%s ret=%d", __func__, key.c_str(), ret);
      std::cerr << __func__ << "::failed compare key=" << key
		<< ", input=" << input << ", value=" << value << std::endl;
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
      std::cerr << __func__ << "::successful set xattr key=" << key << std::endl;
    }
    else {
      CLS_LOG(4, "ERROR: %s failed to set xattr key=%s ret=%d", __func__, key.c_str(), ret);
      std::cerr << __func__ << "::failed to set xattr key=" << key << ", ret=" << ret  << std::endl;
      return ret;
    }
  }

  return 0;
}

static int cmp_rm_keys(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cmp_rm_keys_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const buffer::error&) {
    CLS_LOG(1, "ERROR: cmp_rm_keys(): failed to decode input");
    return -EINVAL;
  }

  // collect the keys we need to read
  std::set<std::string> keys;
  for (const auto& kv : op.values) {
    keys.insert(kv.first);
  }

  // read the values for each key to compare
  std::map<std::string, bufferlist> values;
  int r = cls_cxx_map_get_vals_by_keys(hctx, keys, &values);
  if (r < 0) {
    CLS_LOG(4, "ERROR: cmp_rm_keys() failed to read values r=%d", r);
    return r;
  }

  auto v = values.cbegin();
  for (const auto& [key, input] : op.values) {
    if (v == values.end() || v->first != key) {
      CLS_LOG(20, "cmp_rm_keys() missing key=%s", key.c_str());
      continue;
    }
    CLS_LOG(20, "cmp_rm_keys() comparing key=%s mode=%d op=%d",
            key.c_str(), (int)op.mode, (int)op.comparison);

    const bufferlist& value = v->second;
    ++v;

    r = compare_value(op.mode, op.comparison, input, value);
    if (r == -EIO) {
      r = 0; // treat EIO as a failed comparison
    }
    if (r < 0) {
      CLS_LOG(10, "cmp_rm_keys() failed to compare key=%s r=%d",
              key.c_str(), r);
      return r;
    }
    if (r == 0) {
      // unsuccessful comparison
      CLS_LOG(20, "cmp_rm_keys() preserving key=%s", key.c_str());
    } else {
      // successful comparison
      CLS_LOG(20, "cmp_rm_keys() removing key=%s", key.c_str());
      r = cls_cxx_map_remove_key(hctx, key);
      if (r < 0) {
        CLS_LOG(1, "ERROR: cmp_rm_keys() failed to remove key=%s r=%d",
                key.c_str(), r);
        return r;
      }
    }
  }

  return 0;
}

CLS_INIT(cmpomap)
{
  CLS_LOG(1, "Loaded cmpomap class!");

  cls_handle_t h_class;
  cls_method_handle_t h_cmp_vals;
  cls_method_handle_t h_cmp_set_vals;
  cls_method_handle_t h_cmp_vals_set_vals;
  cls_method_handle_t h_cmp_rm_keys;

  cls_register("cmpomap", &h_class);

  cls_register_cxx_method(h_class, "cmp_vals", CLS_METHOD_RD,
                          cmp_vals, &h_cmp_vals);
  cls_register_cxx_method(h_class, "cmp_set_vals", CLS_METHOD_RD | CLS_METHOD_WR,
                          cmp_set_vals, &h_cmp_set_vals);
  cls_register_cxx_method(h_class, "cmp_vals_set_vals", CLS_METHOD_RD | CLS_METHOD_WR,
			  cmp_vals_set_vals, &h_cmp_vals_set_vals);
  cls_register_cxx_method(h_class, "cmp_rm_keys", CLS_METHOD_RD | CLS_METHOD_WR,
                          cmp_rm_keys, &h_cmp_rm_keys);
}
