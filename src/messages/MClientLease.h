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


#ifndef CEPH_MCLIENTLEASE_H
#define CEPH_MCLIENTLEASE_H

#include "msg/Message.h"

struct MClientLease : public Message {
  struct ceph_mds_lease h;
  string dname;
  
  int get_action() const { return h.action; }
  ceph_seq_t get_seq() const { return h.seq; }
  int get_mask() const { return h.mask; }
  inodeno_t get_ino() const { return inodeno_t(h.ino); }
  snapid_t get_first() const { return snapid_t(h.first); }
  snapid_t get_last() const { return snapid_t(h.last); }

  MClientLease() : Message(CEPH_MSG_CLIENT_LEASE) {}
  MClientLease(int ac, ceph_seq_t seq, int m, uint64_t i, uint64_t sf, uint64_t sl) :
    Message(CEPH_MSG_CLIENT_LEASE) {
    h.action = ac;
    h.seq = seq;
    h.mask = m;
    h.ino = i;
    h.first = sf;
    h.last = sl;
    h.duration_ms = 0;
  }
  MClientLease(int ac, ceph_seq_t seq, int m, uint64_t i, uint64_t sf, uint64_t sl, const string& d) :
    Message(CEPH_MSG_CLIENT_LEASE),
    dname(d) {
    h.action = ac;
    h.seq = seq;
    h.mask = m;
    h.ino = i;
    h.first = sf;
    h.last = sl;
    h.duration_ms = 0;
  }
private:
  ~MClientLease() {}

public:
  const char *get_type_name() const { return "client_lease"; }
  void print(ostream& out) const {
    out << "client_lease(a=" << ceph_lease_op_name(get_action())
	<< " seq " << get_seq()
	<< " mask " << get_mask();
    out << " " << get_ino();
    if (h.last != CEPH_NOSNAP)
      out << " [" << snapid_t(h.first) << "," << snapid_t(h.last) << "]";
    if (dname.length())
      out << "/" << dname;
    out << ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(h, p);
    ::decode(dname, p);
  }
  virtual void encode_payload(uint64_t features) {
    ::encode(h, payload);
    ::encode(dname, payload);
  }

};

#endif
