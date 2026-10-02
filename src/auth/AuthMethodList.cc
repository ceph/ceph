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
#include <iterator>

const static int dout_subsys = ceph_subsys_auth;


AuthMethodList::AuthMethodList(CephContext *cct, std::string str)
{
  const auto sup_list = get_str_vec(str);

  if (sup_list.empty()) {
    lderr(cct) << "WARNING: empty auth protocol list" << dendl;
  }

  auth_supported.reserve(std::size(sup_list));

  for (const auto& method : sup_list) {
    ldout(cct, 5) << "adding auth protocol: " << method << dendl;
    if (method == "cephx") {
      auth_supported.push_back(CEPH_AUTH_CEPHX);
    } else if (method == "none") {
      auth_supported.push_back(CEPH_AUTH_NONE);
    } else if (method == "gss") {
      auth_supported.push_back(CEPH_AUTH_GSS);
    } else {
      auth_supported.push_back(CEPH_AUTH_UNKNOWN);
      lderr(cct) << "WARNING: unknown auth protocol defined: " << method << dendl;
    }
  }

  if (auth_supported.empty()) {
    lderr(cct) << "WARNING: no auth protocol defined, use 'cephx' by default" << dendl;
    auth_supported.push_back(CEPH_AUTH_CEPHX);
  }
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
  std::erase(auth_supported, static_cast<__u32>(auth_type));
}
