// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __CEPH_OSDCAPS_H
#define __CEPH_OSDCAPS_H

#include "include/types.h"

#define OSD_POOL_CAP_R 0x01
#define OSD_POOL_CAP_W 0x02
#define OSD_POOL_CAP_X 0x04

typedef __u8 rwx_t;

static inline ostream& operator<<(ostream& out, rwx_t p) {
  if (p & OSD_POOL_CAP_R)
    out << "r";
  if (p & OSD_POOL_CAP_W)
    out << "w";
  if (p & OSD_POOL_CAP_X)
    out << "x";
  return out;
}


struct OSDPoolCap {
  rwx_t allow;
  rwx_t deny;
  OSDPoolCap() : allow(0), deny(0) {}
};

static inline ostream& operator<<(ostream& out, const OSDPoolCap& pc) {
  return out << "(allow " << pc.allow << ", deny " << pc.deny << ")";
}

struct OSDCaps {
  map<int, OSDPoolCap> pools_map;
  rwx_t default_action;
  bool get_next_token(string s, size_t& pos, string& token);
  bool is_rwx(string& token, rwx_t& cap_val);
  
  OSDCaps() : default_action(0) {}
  bool parse(bufferlist::iterator& iter);
  int get_pool_cap(int pool_id);
};

static inline ostream& operator<<(ostream& out, const OSDCaps& c) {
  return out << "osdcaps(pools=" << c.pools_map << " default=" << c.default_action << ")";
}

#endif
