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
  __s32 from;

public:
  /*
    group things by realm (auth delgation root), since that's how auth is determined.
    that makes it less work to process when exports are in progress.
  */
  struct realm {
    map<inodeno_t, __s32> inodes;
    map<dirfrag_t, __s32> dirs;
    map<dirfrag_t, map<nstring,__s32> > dentries;

    void encode(bufferlist &bl) const {
      ::encode(inodes, bl);
      ::encode(dirs, bl);
      ::encode(dentries, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(inodes, bl);
      ::decode(dirs, bl);
      ::decode(dentries, bl);
    }
  };
  WRITE_CLASS_ENCODER(realm)

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
  void add_dentry(dirfrag_t r, dirfrag_t df, const nstring& dn, int nonce) {
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
    for (map<dirfrag_t, map<nstring,int> >::iterator p = r.dentries.begin();
	 p != r.dentries.end();
	 ++p)
      for (map<nstring,int>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) 
	myr.dentries[p->first][q->first] = q->second;
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(from, p);
    ::decode(realms, p);
  }
    
  void encode_payload() {
    ::encode(from, payload);
    ::encode(realms, payload);
  }
};

WRITE_CLASS_ENCODER(MCacheExpire::realm)

#endif
