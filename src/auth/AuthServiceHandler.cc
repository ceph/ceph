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

#include "AuthServiceHandler.h"
#include "cephx/CephxServiceHandler.h"

AuthServiceHandler *get_auth_service_handler(KeyServer *ks, set<__u32>& supported)
{
  if (supported.count(CEPH_AUTH_CEPHX))
    return new CephxServiceHandler(ks);
  return NULL;
}


