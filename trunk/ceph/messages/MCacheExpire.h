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
  map<inodeno_t, int> inodes;
  map<inodeno_t, int> dirs;
  int from;

 public:
  map<inodeno_t,int>& get_inodes() { return inodes; }
  map<inodeno_t,int>& get_dirs() { return dirs; }
  int get_from() { return from; }

  MCacheExpire() {}
  MCacheExpire(int from) : Message(MSG_MDS_CACHEEXPIRE) {
    this->from = from;
  }
  virtual char *get_type_name() { return "CEx";}
  
  void add_inode(inodeno_t ino, int nonce) {
    inodes.insert(pair<inodeno_t,int>(ino,nonce));
  }
  void add_dir(inodeno_t ino, int nonce) {
    dirs.insert(pair<inodeno_t,int>(ino,nonce));
  }

  virtual void decode_payload(crope& s, int& off) {
    int n;

    s.copy(off, sizeof(from), (char*)&from);
    off += sizeof(from);

    // inodes
    s.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      inodeno_t ino;
      int nonce;
      s.copy(off, sizeof(ino), (char*)&ino);
      off += sizeof(ino);
      s.copy(off, sizeof(int), (char*)&nonce);
      off += sizeof(int);
      inodes.insert(pair<inodeno_t, int>(ino,nonce));
    }

    // dirs
    s.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      inodeno_t ino;
      int nonce;
      s.copy(off, sizeof(ino), (char*)&ino);
      off += sizeof(ino);
      s.copy(off, sizeof(int), (char*)&nonce);
      off += sizeof(int);
      dirs.insert(pair<inodeno_t, int>(ino,nonce));
    }
  }
  
  void rope_map(crope& s, map<inodeno_t,int>& mp) {
    int n = mp.size();
    s.append((char*)&n, sizeof(int));
    for (map<inodeno_t,int>::iterator it = mp.begin();
         it != mp.end(); 
         it++) {
      inodeno_t ino = it->first;
      int nonce = it->second;
      s.append((char*)&ino, sizeof(ino));
      s.append((char*)&nonce, sizeof(nonce));
    }    
  }

  virtual void encode_payload(crope& s) {
    s.append((char*)&from, sizeof(from));
    rope_map(s, inodes);
    rope_map(s, dirs);
  }
};

#endif
