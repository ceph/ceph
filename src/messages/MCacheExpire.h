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
    map<vinodeno_t, __s32> inodes;
    map<dirfrag_t, __s32> dirs;
    map<dirfrag_t, map<pair<nstring,snapid_t>,__s32> > dentries;

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
private:
  ~MCacheExpire() {}

public:
  virtual const char *get_type_name() { return "cache_expire";}
  
  void add_inode(dirfrag_t r, vinodeno_t vino, int nonce) {
    realms[r].inodes[vino] = nonce;
  }
  void add_dir(dirfrag_t r, dirfrag_t df, int nonce) {
    realms[r].dirs[df] = nonce;
  }
  void add_dentry(dirfrag_t r, dirfrag_t df, const nstring& dn, snapid_t last, int nonce) {
    realms[r].dentries[df][pair<nstring,snapid_t>(dn,last)] = nonce;
  }

  void add_realm(dirfrag_t df, realm& r) {
    realms[df] = r;
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
