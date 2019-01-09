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

#ifndef CEPH_MCLIENTSNAP_H
#define CEPH_MCLIENTSNAP_H

#include "msg/Message.h"

class MClientSnap : public MessageInstance<MClientSnap> {
public:
  friend factory;

  ceph_mds_snap_head head;
  bufferlist bl;
  
  // (for split only)
  vector<inodeno_t> split_inos;
  vector<inodeno_t> split_realms;

protected:
  MClientSnap(int o=0) : 
    MessageInstance(CEPH_MSG_CLIENT_SNAP) {
    memset(&head, 0, sizeof(head));
    head.op = o;
  }
  ~MClientSnap() override {}

public:  
  std::string_view get_type_name() const override { return "client_snap"; }
  void print(ostream& out) const override {
    out << "client_snap(" << ceph_snap_op_name(head.op);
    if (head.split)
      out << " split=" << inodeno_t(head.split);
    out << " tracelen=" << bl.length();
    out << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    head.num_split_inos = split_inos.size();
    head.num_split_realms = split_realms.size();
    head.trace_len = bl.length();
    encode(head, payload);
    encode_nohead(split_inos, payload);
    encode_nohead(split_realms, payload);
    encode_nohead(bl, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(head, p);
    decode_nohead(head.num_split_inos, split_inos, p);
    decode_nohead(head.num_split_realms, split_realms, p);
    decode_nohead(head.trace_len, bl, p);
    ceph_assert(p.end());
  }

};

#endif
