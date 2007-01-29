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

#ifndef __MMDSCACHEREJOIN_H
#define __MMDSCACHEREJOIN_H

#include "msg/Message.h"

#include "include/types.h"


class MMDSCacheRejoin : public Message {
  
  struct InodeState {
	int hardlock;     // hardlock state
	int filelock;     // filelock state
	int caps_wanted;  // what caps bits i want
	InodeState(int cw=0, int hl=-1, int fl=-1) : hardlock(hl), filelock(fl), caps_wanted(cw) {}
  };

  map<inodeno_t, InodeState> inodes;
  map<inodeno_t, map<string, int> > dentries;
  set<inodeno_t> dirs;

 public:
  MMDSCacheRejoin() : Message(MSG_MDS_CACHEREJOIN) {}

  char *get_type_name() { return "cache_rejoin"; }

  void print(ostream& out) {
    out << "cache_rejoin" << endl;
  }

  void add_dir(inodeno_t dirino) {
	dirs.insert(dirino);
  }
  void add_dentry(inodeno_t dirino, const string& dn, int ls) {
	dentries[dirino][dn] = ls;
  }
  void add_inode(inodeno_t ino, int hl, int fl, int cw) {
	inodes[ino] = InodeState(cw,hl,fl);
  }
  
  void encode_payload() {
	::_encode(inodes, payload);
	::_encode(dirs, payload);
  }
  void decode_payload() {
	int off = 0;
	::_decode(inodes, payload, off);
	::_decode(dirs, payload, off);
  }
};

#endif
