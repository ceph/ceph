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

#ifndef __MCACHEEXPIRE_H
#define __MCACHEEXPIRE_H

class MCacheExpire : public Message {
  int from;

public:
  /*
    group things by realm (auth delgation root), since that's how auth is determined.
    that makes it less work to process when exports are in progress.
  */
  struct realm {
    map<inodeno_t, int> inodes;
    map<dirfrag_t, int> dirs;
    map<dirfrag_t, map<string,int> > dentries;
  };
  map<dirfrag_t, realm> realms;

  int get_from() { return from; }

  MCacheExpire() {}
  MCacheExpire(int f) : 
    Message(MSG_MDS_CACHEEXPIRE),
    from(f) { }

  virtual const char *get_type_name() { return "CEx";}
  
  void add_inode(dirfrag_t r, inodeno_t ino, int nonce) {
    realms[r].inodes[ino] = nonce;
  }
  void add_dir(dirfrag_t r, dirfrag_t df, int nonce) {
    realms[r].dirs[df] = nonce;
  }
  void add_dentry(dirfrag_t r, dirfrag_t df, const string& dn, int nonce) {
    realms[r].dentries[df][dn] = nonce;
  }

  void add_realm(dirfrag_t df, realm& r) {
    realm& myr = realms[df];
    for (map<inodeno_t, int>::iterator p = r.inodes.begin();
	 p != r.inodes.end();
	 ++p)
      myr.inodes[p->first] = p->second;
    for (map<dirfrag_t, int>::iterator p = r.dirs.begin();
	 p != r.dirs.end();
	 ++p)
      myr.dirs[p->first] = p->second;
    for (map<dirfrag_t, map<string,int> >::iterator p = r.dentries.begin();
	 p != r.dentries.end();
	 ++p)
      for (map<string,int>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) 
	myr.dentries[p->first][q->first] = q->second;
  }

  void decode_payload() {
    int off = 0;

    payload.copy(off, sizeof(from), (char*)&from);
    off += sizeof(from);

    int nr;
    payload.copy(off, sizeof(nr), (char*)&nr);
    off += sizeof(nr);

    while (nr--) {
      dirfrag_t r;
      payload.copy(off, sizeof(r), (char*)&r);
      off += sizeof(r);
      
      ::_decode(realms[r].inodes, payload, off);
      ::_decode(realms[r].dirs, payload, off);

      int n;
      payload.copy(off, sizeof(int), (char*)&n);
      off += sizeof(int);
      for (int i=0; i<n; i++) {
	dirfrag_t df;
	payload.copy(off, sizeof(df), (char*)&df);
	off += sizeof(df);
	::_decode(realms[r].dentries[df], payload, off);
      }
    }
  }
    
  void encode_payload() {
    payload.append((char*)&from, sizeof(from));

    int nr = realms.size();
    payload.append((char*)&nr, sizeof(nr));

    for (map<dirfrag_t,realm>::iterator q = realms.begin();
	 q != realms.end();
	 ++q) {
      payload.append((char*)&q->first, sizeof(q->first));

      ::_encode(q->second.inodes, payload);
      ::_encode(q->second.dirs, payload);
      
      int n = q->second.dentries.size();
      payload.append((char*)&n, sizeof(n));
      for (map<dirfrag_t, map<string,int> >::iterator p = q->second.dentries.begin();
	   p != q->second.dentries.end();
	   ++p) {
	payload.append((char*)&p->first, sizeof(p->first));
	::_encode(p->second, payload);
      }
    }
  }
};

#endif
