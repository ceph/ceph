// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

class MClientFileCaps : public Message {
 public:
  static const int OP_ACK     = 0;  // mds->client or client->mds update.  FIXME?
  static const int OP_RELEASE = 1;  // mds closed the cap
  static const int OP_STALE   = 2;  // mds has exported the cap
  static const int OP_REAP    = 3;  // mds has imported the cap from get_mds()

 private:
  inode_t   inode;
  int       caps;
  long      seq;
  int       wanted;
  
  int       special;   // stale || reap;  in conjunction w/ mds value
  int       mds;

 public:
  inodeno_t get_ino() { return inode.ino; }
  inode_t&  get_inode() { return inode; }
  int       get_caps() { return caps; }
  int       get_wanted() { return wanted; }
  long      get_seq() { return seq; }

  // for cap migration
  int       get_mds() { return mds; }
  int       get_special() { return special; }

  void set_caps(int c) { caps = c; }
  void set_wanted(int w) { wanted = w; }

  void set_mds(int m) { mds = m; }
  void set_special(int s) { special = s; }

  MClientFileCaps() {}
  MClientFileCaps(inode_t& inode,
                  long seq,
                  int caps,
                  int wanted,
                  int special = OP_ACK,
                  int mds=0) :
    Message(MSG_CLIENT_FILECAPS) {
    this->inode = inode;
    this->seq = seq;
    this->caps = caps;
    this->wanted = wanted;
    this->special = special;
    this->mds = mds;
  }
  char *get_type_name() { return "Cfcap";}
  
  void decode_payload() {
    int off = 0;
    ::_decode(seq, payload, off);
    ::_decode(inode, payload, off);
    ::_decode(caps, payload, off);
    ::_decode(wanted, payload, off);
    ::_decode(mds, payload, off);
    ::_decode(special, payload, off);
  }
  void encode_payload() {
    ::_encode(seq, payload);
    ::_encode(inode, payload);
    ::_encode(caps, payload);
    ::_encode(wanted, payload);
    ::_encode(mds, payload);
    ::_encode(special, payload);
  }
};

#endif
