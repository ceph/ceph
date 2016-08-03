// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CLIENT_USERPERM_H
#define CEPH_CLIENT_USERPERM_H

struct UserPerm
{
private:
  uid_t m_uid;
  gid_t m_gid;
  int gid_count;
  gid_t *gids;
  bool alloced_gids;
public:
  UserPerm() : m_uid(-1), m_gid(-1), gid_count(0),
	       gids(NULL), alloced_gids(false) {}
  UserPerm(int uid, int gid) : m_uid(uid), m_gid(gid), gid_count(0),
			       gids(NULL), alloced_gids(false) {}
  UserPerm(const UserPerm& o) {
    m_uid = o.m_uid;
    m_gid = o.m_gid;
    gid_count = o.gid_count;
    gids = new gid_t[gid_count];
    alloced_gids = true;
    for (int i = 0; i < gid_count; ++i) {
      gids[i] = o.gids[i];
    }
  }
  ~UserPerm() {
    if (alloced_gids)
      delete gids;
  }
  // FIXME: stop doing a deep-copy all the time. We need it on stuff
  // that lasts longer than a single "syscall", but not for MetaRequests et al
  uid_t uid() const { return m_uid; }
  gid_t gid() const { return m_gid; }
  bool gid_in_groups(gid_t gid) const {
    if (gid == m_gid) return true;
    for (int i = 0; i < gid_count; ++i) {
      if (gid == gids[i]) return true;
    }
    return false;
  }
  int get_gids(const gid_t **_gids) const { *_gids = gids; return gid_count; }
  void init_gids(gid_t* _gids, int count) {
    gids = _gids;
    gid_count = count;
  }
  void take_gids() { alloced_gids = true; }
};


#endif
