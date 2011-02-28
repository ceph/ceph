// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "Auth.h"
#include "AuthAuthorizeHandler.h"
#include "cephx/CephxAuthorizeHandler.h"
#include "none/AuthNoneAuthorizeHandler.h"
#include "AuthSupported.h"
#include "common/debug.h"
#include "common/Mutex.h"

static bool _initialized = false;
static Mutex _lock("auth_service_handler_init");
static map<int, AuthAuthorizeHandler *> authorizers;

static void _init_authorizers(void)
{
  if (is_supported_auth(CEPH_AUTH_NONE)) {
    authorizers[CEPH_AUTH_NONE] = new AuthNoneAuthorizeHandler(); 
  }
  if (is_supported_auth(CEPH_AUTH_CEPHX)) {
    authorizers[CEPH_AUTH_CEPHX] = new CephxAuthorizeHandler(); 
  }
  _initialized = true;
}

AuthAuthorizeHandler *get_authorize_handler(int protocol)
{
  Mutex::Locker l(_lock);
  if (!_initialized) {
   _init_authorizers();
  }

  map<int, AuthAuthorizeHandler *>::iterator iter = authorizers.find(protocol);
  if (iter != authorizers.end())
    return iter->second;

  dout(0) << "get_authorize_handler protocol " << protocol << " not supported" << dendl;

  return NULL;
}
