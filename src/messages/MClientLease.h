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


#ifndef __MCLIENTLEASE_H
#define __MCLIENTLEASE_H

#include "msg/Message.h"
#include "mds/SimpleLock.h"

static const char *get_lease_action_name(int a) {
  switch (a) {
  case CEPH_MDS_LEASE_REVOKE: return "revoke";
  case CEPH_MDS_LEASE_RELEASE: return "release";
  case CEPH_MDS_LEASE_RENEW: return "renew";
  default: assert(0); return 0;
  }
}


struct MClientLease : public Message {
  struct ceph_mds_lease h;
  string dname;
  
  int get_action() { return h.action; }
  int get_mask() { return le16_to_cpu(h.mask); }
  inodeno_t get_ino() { return inodeno_t(le64_to_cpu(h.ino)); }

  MClientLease() : Message(CEPH_MSG_CLIENT_LEASE) {}
  MClientLease(int ac, int m, __u64 i) :
    Message(CEPH_MSG_CLIENT_LEASE) {
    h.action = ac;
    h.mask = cpu_to_le16(m);
    h.ino = cpu_to_le64(i);
  }
  MClientLease(int ac, int m, __u64 i, const string& d) :
    Message(CEPH_MSG_CLIENT_LEASE),
    dname(d) {
    h.action = ac;
    h.mask = cpu_to_le16(m);
    h.ino = cpu_to_le64(i);
  }

  const char *get_type_name() { return "client_lease"; }
  void print(ostream& out) {
    out << "client_lease(a=" << get_lease_action_name(get_action())
	<< " mask " << get_mask();
    out << " " << get_ino();
    if (dname.length())
      out << "/" << dname;
    out << ")";
  }
  
  void decode_payload() {
    int off = 0;
    ::_decode(h, payload, off);
    ::_decode(dname, payload, off);
  }
  virtual void encode_payload() {
    ::_encode(h, payload);
    ::_encode(dname, payload);
  }

};

#endif
