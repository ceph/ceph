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

#include "common/Mutex.h"
#include "include/str_list.h"
#include "config.h"

#define DOUT_SUBSYS auth

static bool _supported_initialized = false;
static Mutex _supported_lock("auth_service_handler_init");
static map<int, bool> auth_supported;

static void _init_supported(void)
{
  string str = g_conf.supported_auth;
  list<string> sup_list;
  get_str_list(str, sup_list);
  for (list<string>::iterator iter = sup_list.begin(); iter != sup_list.end(); ++iter) {
    if (iter->compare("cephx") == 0) {
      dout(10) << "supporting cephx auth protocol" << dendl;
      auth_supported[CEPH_AUTH_CEPHX] = true;
    } else if (iter->compare("none") == 0) {
      auth_supported[CEPH_AUTH_NONE] = true;
      dout(10) << "supporting *none* auth protocol" << dendl;
    } else {
      dout(0) << "WARNING: unknown auth protocol defined: " << *iter << dendl;
    }
  }
  _supported_initialized = true;
}


bool is_supported_auth(int auth_type)
{
  {
    Mutex::Locker lock(_supported_lock);
    if (!_supported_initialized) {
      _init_supported();
    }
  }
  return auth_supported[auth_type];
}


