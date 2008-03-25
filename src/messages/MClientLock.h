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


#ifndef __MCLIENTLOCK_H
#define __MCLIENTLOCK_H

#include "msg/Message.h"
#include "mds/SimpleLock.h"

static const char *get_clientlock_action_name(int a) {
  switch (a) {
  case CEPH_MDS_LOCK_REVOKE: return "revoke";
  case CEPH_MDS_LOCK_RELEASE: return "release";
  case CEPH_MDS_LOCK_RENEW: return "renew";
  default: assert(0); return 0;
  }
}


struct MClientLock : public Message {
  __u8 lock_type;
  __u8 action;
  __u16 mask;
  __u64 ino;
  string dname;

  MClientLock() : Message(CEPH_MSG_CLIENT_LOCK) {}
  MClientLock(int l, int ac, int m, __u64 i) :
    Message(CEPH_MSG_CLIENT_LOCK),
    lock_type(l), action(ac), mask(m), ino(i) {}
  MClientLock(int l, int ac, int m, __u64 i, const string& d) :
    Message(CEPH_MSG_CLIENT_LOCK),
    lock_type(l), action(ac), mask(m), ino(i), dname(d) {}
  MClientLock(SimpleLock *lock, int ac, int m, __u64 i) :
    Message(CEPH_MSG_CLIENT_LOCK),
    lock_type(lock->get_type()),
    action(ac), mask(m), ino(i) {}
  MClientLock(SimpleLock *lock, int ac, int m, __u64 i, const string& d) :
    Message(CEPH_MSG_CLIENT_LOCK),
    lock_type(lock->get_type()),
    action(ac), mask(m), ino(i), dname(d) {}

  const char *get_type_name() { return "client_lock"; }
  void print(ostream& out) {
    out << "client_lock(a=" << get_clientlock_action_name(action)
	<< " " << get_lock_type_name(lock_type)
	<< " mask " << mask;
    out << " " << inodeno_t(ino);
    if (dname.length())
      out << "/" << dname;
    out << ")";
  }
  
  void decode_payload() {
    int off = 0;
    ::_decode(lock_type, payload, off);
    ::_decode(mask, payload, off);
    ::_decode(action, payload, off);
    ::_decode(ino, payload, off);
    ::_decode(dname, payload, off);
  }
  virtual void encode_payload() {
    ::_encode(lock_type, payload);
    ::_encode(mask, payload);
    ::_encode(action, payload);
    ::_encode(ino, payload);
    ::_encode(dname, payload);
  }

};

#endif
