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


#ifndef __MCLIENTREQUESTFORWARD_H
#define __MCLIENTREQUESTFORWARD_H

class MClientRequestForward : public Message {
  tid_t tid;
  int32_t dest_mds;
  int32_t num_fwd;

 public:
  MClientRequestForward() : Message(CEPH_MSG_CLIENT_REQUEST_FORWARD) {}
  MClientRequestForward(tid_t t, int dm, int nf) : 
    Message(CEPH_MSG_CLIENT_REQUEST_FORWARD),
    tid(t), dest_mds(dm), num_fwd(nf) { }

  tid_t get_tid() { return tid; }
  int get_dest_mds() { return dest_mds; }
  int get_num_fwd() { return num_fwd; }

  const char *get_type_name() { return "cfwd"; }
  void print(ostream& o) {
    o << "client_request_forward(" << tid
      << " to " << dest_mds
      << " num_fwd=" << num_fwd
      << ")";
  }

  void encode_payload() {
    payload.append((char*)&tid, sizeof(tid));
    payload.append((char*)&dest_mds, sizeof(dest_mds));
    payload.append((char*)&num_fwd, sizeof(num_fwd));
  }

  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(tid), (char*)&tid);
    off += sizeof(tid);
    payload.copy(off, sizeof(dest_mds), (char*)&dest_mds);
    off += sizeof(dest_mds);
    payload.copy(off, sizeof(num_fwd), (char*)&num_fwd);
    off += sizeof(num_fwd);
  }
};

#endif
