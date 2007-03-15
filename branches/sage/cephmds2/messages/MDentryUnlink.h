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


#ifndef __MDENTRYUNLINK_H
#define __MDENTRYUNLINK_H

class MDentryUnlink : public Message {
  dirfrag_t dirfrag;
  string dn;

 public:
  dirfrag_t get_dirfrag() { return dirfrag; }
  string& get_dn() { return dn; }

  MDentryUnlink() {}
  MDentryUnlink(dirfrag_t df, string& n) :
    Message(MSG_MDS_DENTRYUNLINK),
    dirfrag(df),
    dn(n) { }

  char *get_type_name() { return "dentry_unlink";}
  void print(ostream& o) {
    o << "dentry_unlink(" << dirfrag << " " << dn << ")";
  }
  
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(dirfrag), (char*)&dirfrag);
    off += sizeof(dirfrag);
    ::_decode(dn, payload, off);
  }
  void encode_payload() {
    payload.append((char*)&dirfrag,sizeof(dirfrag));
    ::_encode(dn, payload);
  }
};

#endif
