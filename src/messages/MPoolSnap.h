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

#ifndef __MPOOLSNAP_H
#define __MPOOLSNAP_H


class MPoolSnap : public Message {
public:
  ceph_fsid_t fsid;
  tid_t tid;
  int pool;
  string name;
  bool create;

  MPoolSnap() : Message(MSG_POOLSNAP) {}
  MPoolSnap( ceph_fsid_t& f, tid_t t, int p, string& n, bool c) :
    Message(MSG_POOLSNAP), fsid(f), tid(t), pool(p), name(n), create(c) {}

  const char *get_type_name() { return "poolsnap"; }
  void print(ostream& out) {
    out << "poolsnap(" << tid << " " << name << ")";
  }

  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(tid, payload);
    ::encode(pool, payload);
    ::encode(name, payload);
    ::encode(create, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(tid, p);
    ::decode(pool, p);
    ::decode(name, p);
    ::decode(create, p);
  }
};

#endif
