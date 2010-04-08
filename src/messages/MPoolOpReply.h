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

#ifndef __MPOOLOPREPLY_H
#define __MPOOLOPREPLY_H


class MPoolOpReply : public PaxosServiceMessage {
public:
  ceph_fsid_t fsid;
  int replyCode;
  epoch_t epoch;

  MPoolOpReply() : PaxosServiceMessage(MSG_POOLOPREPLY, 0) {}
  MPoolOpReply( ceph_fsid_t& f, tid_t t, int rc, int e, version_t v) :
    PaxosServiceMessage(MSG_POOLOPREPLY, v), fsid(f), replyCode(rc), epoch(e) {
    set_tid(t);
  }

  const char *get_type_name() { return "poolopreply"; }

  void print(ostream& out) {
    out << "poolopreply(reply:" << strerror(-replyCode) << ", "
	<< get_tid() << " v" << version << ")";
  }

  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(replyCode, payload);
    ::encode(epoch, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(replyCode, p);
    ::decode(epoch, p);
  }
};

#endif
