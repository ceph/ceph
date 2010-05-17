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
  __u32 replyCode;
  epoch_t epoch;
  bufferlist *response_data;

  //Once you pass in a response_data pointer it becomes the responsibility
  // of the Message. Don't delete it! (This reduces memory copies.)
  MPoolOpReply() : PaxosServiceMessage(CEPH_MSG_POOLOP_REPLY, 0), response_data(NULL)
  {}
  MPoolOpReply( ceph_fsid_t& f, tid_t t, int rc, int e, version_t v) :
    PaxosServiceMessage(CEPH_MSG_POOLOP_REPLY, v), fsid(f), replyCode(rc),
    epoch(e), response_data(NULL) {
    set_tid(t);
  }
  MPoolOpReply( ceph_fsid_t& f, tid_t t, int rc, int e, version_t v,
		bufferlist *blp) :
    PaxosServiceMessage(CEPH_MSG_POOLOP_REPLY, v), fsid(f), replyCode(rc),
    epoch(e), response_data(blp) {
    set_tid(t);
  }
  ~MPoolOpReply() { if(response_data) delete response_data; }

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
    if (response_data) {
      ::encode(true, payload);
      ::encode(*response_data, payload);
    } else ::encode(false, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(replyCode, p);
    ::decode(epoch, p);
    bool has_response_data;
    ::decode(has_response_data, p);
    if (has_response_data) {
      response_data = new bufferlist();
      ::decode(*response_data, p);
    } else response_data = NULL;
  }
};

#endif
