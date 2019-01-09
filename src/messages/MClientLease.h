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

#include <string_view>

#include "msg/Message.h"

class MClientLease : public MessageInstance<MClientLease> {
public:
  friend factory;

  struct ceph_mds_lease h;
  std::string dname;
  
  int get_action() const { return h.action; }
  ceph_seq_t get_seq() const { return h.seq; }
  int get_mask() const { return h.mask; }
  inodeno_t get_ino() const { return inodeno_t(h.ino); }
  snapid_t get_first() const { return snapid_t(h.first); }
  snapid_t get_last() const { return snapid_t(h.last); }

protected:
  MClientLease() : MessageInstance(CEPH_MSG_CLIENT_LEASE) {}
  MClientLease(const MClientLease& m) :
    MessageInstance(CEPH_MSG_CLIENT_LEASE),
    h(m.h),
    dname(m.dname) {}
  MClientLease(int ac, ceph_seq_t seq, int m, uint64_t i, uint64_t sf, uint64_t sl) :
    MessageInstance(CEPH_MSG_CLIENT_LEASE) {
    h.action = ac;
    h.seq = seq;
    h.mask = m;
    h.ino = i;
    h.first = sf;
    h.last = sl;
    h.duration_ms = 0;
  }
  MClientLease(int ac, ceph_seq_t seq, int m, uint64_t i, uint64_t sf, uint64_t sl, std::string_view d) :
    MessageInstance(CEPH_MSG_CLIENT_LEASE),
    dname(d) {
    h.action = ac;
    h.seq = seq;
    h.mask = m;
    h.ino = i;
    h.first = sf;
    h.last = sl;
    h.duration_ms = 0;
  }
  ~MClientLease() override {}

public:
  std::string_view get_type_name() const override { return "client_lease"; }
  void print(ostream& out) const override {
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
  
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(h, p);
    decode(dname, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(h, payload);
    encode(dname, payload);
  }

};

#endif
