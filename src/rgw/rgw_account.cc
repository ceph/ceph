// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_account.h"

#include <algorithm>
#include <fmt/format.h>

#include "common/random_string.h"
#include "common/utf8.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::account {

// account ids start with 'RGW' followed by 17 numeric digits
static constexpr std::string_view id_prefix = "RGW";
static constexpr std::size_t id_len = 20;

std::string generate_id(CephContext* cct)
{
  // fill with random numeric digits
  std::string id = gen_rand_numeric(cct, id_len);
  // overwrite the prefix bytes
  std::copy(id_prefix.begin(), id_prefix.end(), id.begin());
  return id;
}

bool validate_id(std::string_view id, std::string* err_msg)
{
  if (id.size() != id_len) {
    if (err_msg) {
      *err_msg = fmt::format("account id must be {} bytes long", id_len);
    }
    return false;
  }
  if (id.compare(0, id_prefix.size(), id_prefix) != 0) {
    if (err_msg) {
      *err_msg = fmt::format("account id must start with {}", id_prefix);
    }
    return false;
  }
  auto suffix = id.substr(id_prefix.size());
  // all remaining bytes must be digits
  constexpr auto digit = [] (int c) { return std::isdigit(c); };
  if (!std::all_of(suffix.begin(), suffix.end(), digit)) {
    if (err_msg) {
      *err_msg = "account id must end with numeric digits";
    }
    return false;
  }
  return true;
}

bool validate_name(std::string_view name, std::string* err_msg)
{
  if (name.empty()) {
    if (err_msg) {
      *err_msg = "account name must not be empty";
    }
    return false;
  }
  // must not contain the tenant delimiter $
  if (name.find('$') != name.npos) {
    if (err_msg) {
      *err_msg = "account name must not contain $";
    }
    return false;
  }
  // must not contain the metadata section delimeter :
  if (name.find(':') != name.npos) {
    if (err_msg) {
      *err_msg = "account name must not contain :";
    }
    return false;
  }
  // must be valid utf8
  if (check_utf8(name.data(), name.size()) != 0) {
    if (err_msg) {
      *err_msg = "account name must be valid utf8";
    }
    return false;
  }
  return true;
}

} // namespace rgw::account
