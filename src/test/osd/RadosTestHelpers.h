// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cerrno>
#include <expected>
#include <sstream>
#include <string>
#include "include/rados/librados.hpp"
#include "include/buffer.h"
#include "json_spirit/json_spirit.h"

/**
 * query_pool_flag - query a named boolean pool flag via the monitor.
 *
 * Sends an "osd pool get" mon_command for the given pool and flag variable
 * name, parses the JSON response, and returns the flag value on success or a
 * negative errno on failure via std::expected<bool, int>.
 *
 * This function is limited to boolean pool variables.
 * The full set of supported flag_var names is:
 *   supports_omap, allow_ec_overwrites, allow_ec_optimizations,
 *   hashpspool, eio, nodelete, bulk, nopgchange, nosizechange,
 *   write_fadvise_dontneed, noscrub, nodeep-scrub, use_gmt_hitset.
 * Passing a non-boolean variable (e.g. "pg_num", "crush_rule") will cause
 * the monitor to return an integer or string value; this function will
 * detect the type mismatch and return std::unexpected(-EINVAL).
 *
 * @param pool_name  Name of the pool to query.
 * @param flag_var   The boolean pool variable name to query
 *                   (e.g. "supports_omap").
 * @param rados      An initialised and connected Rados handle.
 *
 * @return std::expected<bool, int> containing:
 *           - the flag value (true/false) on success, or
 *           - a negative errno on failure:
 *               - the RADOS error code when mon_command itself fails,
 *               - -ENODATA when the response is empty or the key is absent,
 *               - -EINVAL  when the response cannot be parsed or the value is
 *                          not a boolean.
 */
inline std::expected<bool, int> query_pool_flag(
    const std::string& pool_name,
    const std::string& flag_var,
    librados::Rados& rados)
{
  ceph::bufferlist inbl, outbl;
  std::ostringstream cmd;
  cmd << "{\"prefix\": \"osd pool get\", "
      << "\"pool\": \"" << pool_name << "\", "
      << "\"var\": \"" << flag_var << "\", "
      << "\"format\": \"json\"}";

  int ret = rados.mon_command(cmd.str(), std::move(inbl), &outbl, nullptr);
  if (ret < 0) {
    return std::unexpected(ret);
  }

  std::string outstr = outbl.to_str();
  if (outstr.empty()) {
    return std::unexpected(-ENODATA);
  }

  try {
    json_spirit::Value v;
    if (!json_spirit::read(outstr, v)) {
      return std::unexpected(-EINVAL);
    }
    if (v.type() != json_spirit::obj_type) {
      return std::unexpected(-EINVAL);
    }
    const json_spirit::Object& obj = v.get_obj();
    for (json_spirit::Object::size_type i = 0; i < obj.size(); i++) {
      const json_spirit::Pair& pair = obj[i];
      if (pair.name_ == flag_var) {
        if (pair.value_.type() == json_spirit::bool_type) {
          return pair.value_.get_bool();
        }
        return std::unexpected(-EINVAL);
      }
    }
    return std::unexpected(-ENODATA);
  } catch (const std::exception&) {
    return std::unexpected(-EINVAL);
  }
}
