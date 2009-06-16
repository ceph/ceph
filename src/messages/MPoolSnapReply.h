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

#ifndef __MPOOLSNAPREPLY_H
#define __MPOOLSNAPREPLY_H


class MPoolSnapReply : public Message {
public:
  ceph_fsid_t fsid;
  tid_t tid;
  int replyCode;
  int epoch;


  MPoolSnapReply() : Message(MSG_POOLSNAPREPLY) {}
  MPoolSnapReply( ceph_fsid_t& f, tid_t t, int rc, int e) :
    Message(MSG_POOLSNAPREPLY), fsid(f), tid(t), replyCode(rc), epoch(e) {}

  const char *get_type_name() { return "poolsnapreply"; }

  void print(ostream& out) {
    out << "poolsnapreply(" << tid <<")";
  }

  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(tid, payload);
    ::encode(replyCode, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(tid, p);
    ::decode(replyCode, p);
  }
};

#endif
