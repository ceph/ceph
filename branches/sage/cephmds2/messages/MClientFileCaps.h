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

#ifndef __MCLIENTFILECAPS_H
#define __MCLIENTFILECAPS_H

#include "msg/Message.h"
#include "mds/Capability.h"

class MClientFileCaps : public Message {
 public:
  static const int OP_GRANT   = 0;  // mds->client grant.
  static const int OP_ACK     = 1;  // client->mds ack (if prior grant was a recall)
  static const int OP_RELEASE = 2;  // mds closed the cap
  static const int OP_STALE   = 3;  // mds has exported the cap
  static const int OP_REAP    = 4;  // mds has imported the cap from get_mds()
  static const char* get_opname(int op) {
    switch (op) {
    case OP_GRANT: return "grant";
    case OP_ACK: return "ack";
    case OP_RELEASE: return "release";
    case OP_STALE: return "stale";
    case OP_REAP: return "reap";
    default: assert(0); return 0;
    }
  }

 private:
  int32_t op; 
  inode_t inode;
  capseq_t seq;
  int32_t caps;
  int32_t wanted;
  
  int32_t mds;

 public:
  inodeno_t get_ino() { return inode.ino; }
  inode_t&  get_inode() { return inode; }
  int       get_caps() { return caps; }
  int       get_wanted() { return wanted; }
  long      get_seq() { return seq; }

  // for cap migration
  int       get_mds() { return mds; }
  int       get_op() { return op; }

  void set_caps(int c) { caps = c; }
  void set_wanted(int w) { wanted = w; }

  void set_mds(int m) { mds = m; }
  void set_op(int s) { op = s; }

  MClientFileCaps() {}
  MClientFileCaps(int op_,
		  inode_t& inode_,
                  long seq_,
                  int caps_,
                  int wanted_,
                  int mds_=0) :
    Message(MSG_CLIENT_FILECAPS),
    op(op_),
    inode(inode_),
    seq(seq_),
    caps(caps_),
    wanted(wanted_),
    mds(mds_) { }

  char *get_type_name() { return "Cfcap";}
  void print(ostream& out) {
    out << "client_file_caps(" << get_opname(op)
	<< " " << inode.ino
	<< " seq " << seq
	<< " caps " << cap_string(caps) 
	<< " wanted" << cap_string(wanted) 
	<< ")";
  }
  
  void decode_payload() {
    int off = 0;
    ::_decode(op, payload, off);
    ::_decode(seq, payload, off);
    ::_decode(inode, payload, off);
    ::_decode(caps, payload, off);
    ::_decode(wanted, payload, off);
    ::_decode(mds, payload, off);
  }
  void encode_payload() {
    ::_encode(op, payload);
    ::_encode(seq, payload);
    ::_encode(inode, payload);
    ::_encode(caps, payload);
    ::_encode(wanted, payload);
    ::_encode(mds, payload);
  }
};

#endif
