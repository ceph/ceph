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

#ifndef CEPH_MDSOPENINO_H
#define CEPH_MDSOPENINO_H

#include "msg/Message.h"

struct MMDSOpenIno : public Message {
  inodeno_t ino;
  vector<inode_backpointer_t> ancestors;

  MMDSOpenIno() : Message(MSG_MDS_OPENINO) {}
  MMDSOpenIno(ceph_tid_t t, inodeno_t i, vector<inode_backpointer_t>& a) :
    Message(MSG_MDS_OPENINO), ino(i), ancestors(a) {
    header.tid = t;
  }

  const char *get_type_name() const { return "openino"; }
  void print(ostream &out) const {
    out << "openino(" << header.tid << " " << ino << " " << ancestors << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(ino, payload);
    ::encode(ancestors, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
    ::decode(ancestors, p);
  }
};
REGISTER_MESSAGE(MMDSOpenIno, MSG_MDS_OPENINO);
#endif
