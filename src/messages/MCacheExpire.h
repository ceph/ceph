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

#ifndef CEPH_MCACHEEXPIRE_H
#define CEPH_MCACHEEXPIRE_H

#include "mds/mdstypes.h"

class MCacheExpire : public Message {
  __s32 from;

public:
  /*
    group things by realm (auth delgation root), since that's how auth is determined.
    that makes it less work to process when exports are in progress.
  */
  struct realm {
    map<vinodeno_t, uint32_t> inodes;
    map<dirfrag_t, uint32_t> dirs;
    map<dirfrag_t, map<pair<string,snapid_t>,uint32_t> > dentries;

    void merge(realm& o) {
      inodes.insert(o.inodes.begin(), o.inodes.end());
      dirs.insert(o.dirs.begin(), o.dirs.end());
      for (map<dirfrag_t,map<pair<string,snapid_t>,uint32_t> >::iterator p = o.dentries.begin();
	   p != o.dentries.end();
	   ++p) {
	if (dentries.count(p->first) == 0)
	  dentries[p->first] = p->second;
	else
	  dentries[p->first].insert(p->second.begin(), p->second.end());
      }
    }

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

  MCacheExpire() : Message(MSG_MDS_CACHEEXPIRE), from(-1) {}
  MCacheExpire(int f) : 
    Message(MSG_MDS_CACHEEXPIRE),
    from(f) { }
private:
  ~MCacheExpire() {}

public:
  virtual const char *get_type_name() const { return "cache_expire";}
  
  void add_inode(dirfrag_t r, vinodeno_t vino, unsigned nonce) {
    realms[r].inodes[vino] = nonce;
  }
  void add_dir(dirfrag_t r, dirfrag_t df, unsigned nonce) {
    realms[r].dirs[df] = nonce;
  }
  void add_dentry(dirfrag_t r, dirfrag_t df, const string& dn, snapid_t last, unsigned nonce) {
    realms[r].dentries[df][pair<string,snapid_t>(dn,last)] = nonce;
  }

  void add_realm(dirfrag_t df, realm& r) {
    if (realms.count(df) == 0)
      realms[df] = r;
    else
      realms[df].merge(r);
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(from, p);
    ::decode(realms, p);
  }
    
  void encode_payload(uint64_t features) {
    ::encode(from, payload);
    ::encode(realms, payload);
  }
};

WRITE_CLASS_ENCODER(MCacheExpire::realm)

#endif
