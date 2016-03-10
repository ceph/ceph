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


#ifndef CEPH_MDENTRYUNLINK_H
#define CEPH_MDENTRYUNLINK_H

class MDentryUnlink : public Message {
  dirfrag_t dirfrag;
  string dn;

 public:
  dirfrag_t get_dirfrag() { return dirfrag; }
  string& get_dn() { return dn; }

  bufferlist straybl;

  MDentryUnlink() :
    Message(MSG_MDS_DENTRYUNLINK) { }
  MDentryUnlink(dirfrag_t df, string& n) :
    Message(MSG_MDS_DENTRYUNLINK),
    dirfrag(df),
    dn(n) {}
private:
  ~MDentryUnlink() {}

public:
  const char *get_type_name() const { return "dentry_unlink";}
  void print(ostream& o) const {
    o << "dentry_unlink(" << dirfrag << " " << dn << ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirfrag, p);
    ::decode(dn, p);
    ::decode(straybl, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(dirfrag, payload);
    ::encode(dn, payload);
    ::encode(straybl, payload);
  }
};
REGISTER_MESSAGE(MDentryUnlink, MSG_MDS_DENTRYUNLINK);
#endif
