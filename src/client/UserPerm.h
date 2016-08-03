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
public:
  UserPerm() : m_uid(-1), m_gid(-1) {}
  UserPerm(int uid, int gid) : m_uid(uid), m_gid(gid) {}
  // the readdir code relies on UserPerm copy-constructor being a deep copy!
  uid_t uid() const { return m_uid; }
  gid_t gid() const { return m_gid; }
};


#endif
