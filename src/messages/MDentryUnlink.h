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


#ifndef __MDENTRYUNLINK_H
#define __MDENTRYUNLINK_H

class MDentryUnlink : public Message {
  dirfrag_t dirfrag;
  string dn;

 public:
  dirfrag_t get_dirfrag() { return dirfrag; }
  string& get_dn() { return dn; }

  CInodeDiscover *strayin;
  CDirDiscover *straydir;
  CDentryDiscover *straydn;

  MDentryUnlink() :
    Message(MSG_MDS_DENTRYUNLINK),
    strayin(0), straydir(0), straydn(0) { }
  MDentryUnlink(dirfrag_t df, string& n) :
    Message(MSG_MDS_DENTRYUNLINK),
    dirfrag(df),
    dn(n),
    strayin(0), straydir(0), straydn(0) { }
  ~MDentryUnlink() {
    delete strayin;
    delete straydir;
    delete straydn;
  }

  const char *get_type_name() { return "dentry_unlink";}
  void print(ostream& o) {
    o << "dentry_unlink(" << dirfrag << " " << dn << ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirfrag, p);
    ::decode(dn, p);

    bool isstray;
    ::decode(isstray, p);
    if (isstray) {
      strayin = new CInodeDiscover(p);
      straydir = new CDirDiscover(p);
      straydn = new CDentryDiscover(p);
    }
  }
  void encode_payload() {
    ::encode(dirfrag, payload);
    ::encode(dn, payload);

    bool isstray = strayin ? true:false;
    ::encode(isstray, payload);
    if (isstray) {
      strayin->encode(payload);
      straydir->encode(payload);
      straydn->encode(payload);
    }
  }
};

#endif
