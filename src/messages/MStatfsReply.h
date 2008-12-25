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


#ifndef __MSTATFSREPLY_H
#define __MSTATFSREPLY_H

class MStatfsReply : public Message {
public:
  struct ceph_mon_statfs_reply h;

  MStatfsReply() : Message(CEPH_MSG_STATFS_REPLY) {}
  MStatfsReply(ceph_fsid_t &f, tid_t t) : Message(CEPH_MSG_STATFS_REPLY) {
    h.fsid = f;
    h.tid = t;
  }

  const char *get_type_name() { return "statfs_reply"; }
  void print(ostream& out) {
    out << "statfs_reply(" << h.tid << ")";
  }

  void encode_payload() {
    ::encode(h, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(h, p);
  }
};

#endif
