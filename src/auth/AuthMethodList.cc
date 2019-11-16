// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include <algorithm>
#include "common/debug.h"
#include "common/str_util.h"

#include "AuthMethodList.h"

const static int dout_subsys = ceph_subsys_auth;


AuthMethodList::AuthMethodList(CephContext *cct, std::string str)
{
  bool sup_list_empty = true;
  ceph::substr_do(
    str,
    [&](auto&& s) {
      ldout(cct, 5) << "adding auth protocol: " << s << dendl;
      sup_list_empty = false;
      if (s.compare("cephx") == 0) {
	auth_supported.push_back(CEPH_AUTH_CEPHX);
      } else if (s.compare("none") == 0) {
	auth_supported.push_back(CEPH_AUTH_NONE);
      } else if (s.compare("gss") == 0) {
	auth_supported.push_back(CEPH_AUTH_GSS);
      } else {
	auth_supported.push_back(CEPH_AUTH_UNKNOWN);
	lderr(cct) << "WARNING: unknown auth protocol defined: " << s
		   << dendl;
      }
    });

  if (sup_list_empty) {
    lderr(cct) << "WARNING: empty auth protocol list" << dendl;
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
  for (set<__u32>::const_reverse_iterator p = supported.rbegin(); p != supported.rend(); ++p)
    if (is_supported_auth(*p))
      return *p;
  return CEPH_AUTH_UNKNOWN;
}

void AuthMethodList::remove_supported_auth(int auth_type)
{
  for (list<__u32>::iterator p = auth_supported.begin(); p != auth_supported.end(); ) {
    if (*p == (__u32)auth_type)
      auth_supported.erase(p++);
    else 
      ++p;
  }
}
