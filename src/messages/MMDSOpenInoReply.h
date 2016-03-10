// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MDSOPENINOREPLY_H
#define CEPH_MDSOPENINOREPLY_H

#include "msg/Message.h"

struct MMDSOpenInoReply : public Message {
  inodeno_t ino;
  vector<inode_backpointer_t> ancestors;
  mds_rank_t hint;
  int32_t error;

  MMDSOpenInoReply() : Message(MSG_MDS_OPENINOREPLY), error(0) {}
  MMDSOpenInoReply(ceph_tid_t t, inodeno_t i, mds_rank_t h=MDS_RANK_NONE, int e=0) :
    Message(MSG_MDS_OPENINOREPLY), ino(i), hint(h), error(e) {
    header.tid = t;
  }

  const char *get_type_name() const { return "openinoreply"; }
  void print(ostream &out) const {
    out << "openinoreply(" << header.tid << " "
	<< ino << " " << hint << " " << ancestors << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(ino, payload);
    ::encode(ancestors, payload);
    ::encode(hint, payload);
    ::encode(error, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
    ::decode(ancestors, p);
    ::decode(hint, p);
    ::decode(error, p);
  }
};
REGISTER_MESSAGE(MMDSOpenInoReply, MSG_MDS_OPENINOREPLY);
#endif
