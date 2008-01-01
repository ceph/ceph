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
    int off = 0;
    payload.copy(off, sizeof(dirfrag), (char*)&dirfrag);
    off += sizeof(dirfrag);
    ::_decode(dn, payload, off);

    bool isstray;
    payload.copy(off, sizeof(isstray), (char*)&isstray);
    off += sizeof(isstray);
    if (isstray) {
      strayin = new CInodeDiscover;
      strayin->_decode(payload, off);
      straydir = new CDirDiscover;
      straydir->_decode(payload, off);
      straydn = new CDentryDiscover;
      straydn->_decode(payload, off);
    }
  }
  void encode_payload() {
    payload.append((char*)&dirfrag,sizeof(dirfrag));
    ::_encode(dn, payload);

    bool isstray = strayin ? true:false;
    payload.append((char*)&isstray, sizeof(isstray));
    if (isstray) {
      strayin->_encode(payload);
      straydir->_encode(payload);
      straydn->_encode(payload);
    }
  }
};

#endif
