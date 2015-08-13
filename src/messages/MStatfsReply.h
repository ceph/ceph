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


#ifndef CEPH_MSTATFSREPLY_H
#define CEPH_MSTATFSREPLY_H

class MStatfsReply : public Message {
public:
  struct ceph_mon_statfs_reply h;

  MStatfsReply() : Message(CEPH_MSG_STATFS_REPLY) {}
  MStatfsReply(uuid_d &f, ceph_tid_t t, epoch_t epoch) : Message(CEPH_MSG_STATFS_REPLY) {
    memcpy(&h.fsid, f.bytes(), sizeof(h.fsid));
    header.tid = t;
    h.version = epoch;
  }

  const char *get_type_name() const { return "statfs_reply"; }
  void print(ostream& out) const {
    out << "statfs_reply(" << header.tid << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(h, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(h, p);
  }
};

#endif
