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
 * OSDCaps: Hold the capabilities associated with a single authenticated 
 * user key. These are specified by text strings of the form
 * "allow r" (which allows reading anything on the OSD)
 * "allow rwx auid foo[,bar,baz]" (which allows full access to listed auids)
 *  "allow rwx pool foo[,bar,baz]" (which allows full access to listed pools)
 * "allow *" (which allows full access to EVERYTHING)
 *
 * The OSD assumes that anyone with * caps is an admin and has full
 * message permissions. This means that only the monitor and the OSDs
 * should get *
 */

#ifndef __CEPH_OSDCAPS_H
#define __CEPH_OSDCAPS_H

#include "include/types.h"

#define OSD_POOL_CAP_R 0x01
#define OSD_POOL_CAP_W 0x02
#define OSD_POOL_CAP_X 0x04

#define OSD_POOL_CAP_ALL (OSD_POOL_CAP_R | OSD_POOL_CAP_W | OSD_POOL_CAP_X)

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


struct OSDCap {
  rwx_t allow;
  rwx_t deny;
  OSDCap() : allow(0), deny(0) {}
};

static inline ostream& operator<<(ostream& out, const OSDCap& pc) {
  return out << "(allow " << pc.allow << ", deny " << pc.deny << ")";
}

struct OSDCaps {
  map<int, OSDCap> pools_map;
  map<int, OSDCap> auid_map;
  rwx_t default_action;
  bool allow_all;
  int peer_type;
  uint64_t auid;

  bool get_next_token(string s, size_t& pos, string& token);
  bool is_rwx(string& token, rwx_t& cap_val);
  
  OSDCaps() : default_action(0), allow_all(false),
	      auid(CEPH_AUTH_UID_DEFAULT) {}
  bool parse(bufferlist::iterator& iter);
  int get_pool_cap(int pool_id, uint64_t uid = CEPH_AUTH_UID_DEFAULT);
  bool is_mon() { return CEPH_ENTITY_TYPE_MON == peer_type; }
  bool is_osd() { return CEPH_ENTITY_TYPE_OSD == peer_type; }
  bool is_mds() { return CEPH_ENTITY_TYPE_MDS == peer_type; }
  void set_allow_all(bool allow) { allow_all = allow; }
  void set_peer_type (int pt) { peer_type = pt; }
  void set_auid(uint64_t uid) { auid = uid; }
};

static inline ostream& operator<<(ostream& out, const OSDCaps& c) {
  return out << "osdcaps(pools=" << c.pools_map << " default=" << c.default_action << ")";
}

#endif
