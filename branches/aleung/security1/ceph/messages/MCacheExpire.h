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


#ifndef __MCACHEEXPIRE_H
#define __MCACHEEXPIRE_H

class MCacheExpire : public Message {
  int from;
  map<inodeno_t, int> inodes;
  map<inodeno_t, int> dirs;
  map<inodeno_t, map<string,int> > dentries;

 public:
  int get_from() { return from; }
  map<inodeno_t,int>& get_inodes() { return inodes; }
  map<inodeno_t,int>& get_dirs() { return dirs; }
  map<inodeno_t, map<string,int> >& get_dentries() { return dentries; }

  MCacheExpire() {}
  MCacheExpire(int f) : 
    Message(MSG_MDS_CACHEEXPIRE),
    from(f) { }

  virtual char *get_type_name() { return "CEx";}
  
  void add_inode(inodeno_t ino, int nonce) {
    inodes[ino] = nonce;
  }
  void add_dir(inodeno_t ino, int nonce) {
    dirs[ino] = nonce;
  }
  void add_dentry(inodeno_t dirino, const string& dn, int nonce) {
    dentries[dirino][dn] = nonce;
  }
  void add_dentries(inodeno_t dirino, map<string,int>& dmap) {
    dentries[dirino] = dmap;
  }

  void decode_payload() {
    int off = 0;

    payload.copy(off, sizeof(from), (char*)&from);
    off += sizeof(from);

    ::_decode(inodes, payload, off);
    ::_decode(dirs, payload, off);

    int n;
    payload.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      inodeno_t ino;
      payload.copy(off, sizeof(ino), (char*)&ino);
      off += sizeof(ino);
      ::_decode(dentries[ino], payload, off);
    }
  }

  void encode_payload() {
    payload.append((char*)&from, sizeof(from));
    ::_encode(inodes, payload);
    ::_encode(dirs, payload);

    int n = dentries.size();
    payload.append((char*)&n, sizeof(n));
    for (map<inodeno_t, map<string,int> >::iterator p = dentries.begin();
	 p != dentries.end();
	 ++p) {
      payload.append((char*)&p->first, sizeof(p->first));
      ::_encode(p->second, payload);
    }
  }
};

#endif
