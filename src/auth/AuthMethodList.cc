// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "AuthMethodList.h"
#include "common/debug.h"
#include "include/ceph_fs.h" // for CEPH_AUTH_*
#include "include/str_list.h"

#include <algorithm> // for std::find()
#include <cerrno>

const static int dout_subsys = ceph_subsys_auth;


AuthMethodList::AuthMethodList(CephContext *cct, std::string str)
{
  std::list<std::string> sup_list;
  get_str_list(str, sup_list);
  if (sup_list.empty()) {
    lderr(cct) << "WARNING: empty auth protocol list" << dendl;
  }
  for (auto iter = sup_list.begin(); iter != sup_list.end(); ++iter) {
    ldout(cct, 5) << "adding auth protocol: " << *iter << dendl;
    auto method = parse_method(*iter);
    if (method == CEPH_AUTH_UNKNOWN) {
      lderr(cct) << "WARNING: unknown auth protocol defined: " << *iter << dendl;
    }
    auth_supported.push_back(method);
  }
  if (auth_supported.empty()) {
    lderr(cct) << "WARNING: no auth protocol defined, use 'cephx' by default" << dendl;
    auth_supported.push_back(CEPH_AUTH_CEPHX);
  }
}

__u32 AuthMethodList::parse_method(std::string_view name)
{
  if (name == "cephx") {
    return CEPH_AUTH_CEPHX;
  } else if (name == "none") {
    return CEPH_AUTH_NONE;
  } else if (name == "gss") {
    return CEPH_AUTH_GSS;
  } else {
    return CEPH_AUTH_UNKNOWN;
  }
}

int AuthMethodList::validate_method_list(std::string *value, std::string *error)
{
  std::list<std::string> methods;
  get_str_list(*value, methods);
  if (methods.empty()) {
    *error = "at least one auth method is required: cephx, none or gss";
    return -EINVAL;
  }
  for (const auto& method : methods) {
    if (parse_method(method) == CEPH_AUTH_UNKNOWN) {
      *error = "unknown auth method '" + method +
               "', expected cephx, none or gss";
      return -EINVAL;
    }
  }
  return 0;
}

bool AuthMethodList::is_supported_auth(int auth_type)
{
  return std::find(auth_supported.begin(), auth_supported.end(), auth_type) != auth_supported.end();
}

int AuthMethodList::pick(const std::set<__u32>& supported)
{
  for (auto p = supported.rbegin(); p != supported.rend(); ++p)
    if (is_supported_auth(*p))
      return *p;
  return CEPH_AUTH_UNKNOWN;
}

void AuthMethodList::remove_supported_auth(int auth_type)
{
  for (auto p = auth_supported.begin(); p != auth_supported.end(); ) {
    if (*p == (__u32)auth_type)
      auth_supported.erase(p++);
    else 
      ++p;
  }
}
