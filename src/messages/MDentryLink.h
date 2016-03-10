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


#ifndef CEPH_MDENTRYLINK_H
#define CEPH_MDENTRYLINK_H

class MDentryLink : public Message {
  dirfrag_t subtree;
  dirfrag_t dirfrag;
  string dn;
  bool is_primary;

 public:
  dirfrag_t get_subtree() { return subtree; }
  dirfrag_t get_dirfrag() { return dirfrag; }
  string& get_dn() { return dn; }
  bool get_is_primary() { return is_primary; }

  bufferlist bl;

  MDentryLink() :
    Message(MSG_MDS_DENTRYLINK) { }
  MDentryLink(dirfrag_t r, dirfrag_t df, string& n, bool p) :
    Message(MSG_MDS_DENTRYLINK),
    subtree(r),
    dirfrag(df),
    dn(n),
    is_primary(p) {}
private:
  ~MDentryLink() {}

public:
  const char *get_type_name() const { return "dentry_link";}
  void print(ostream& o) const {
    o << "dentry_link(" << dirfrag << " " << dn << ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(subtree, p);
    ::decode(dirfrag, p);
    ::decode(dn, p);
    ::decode(is_primary, p);
    ::decode(bl, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(subtree, payload);
    ::encode(dirfrag, payload);
    ::encode(dn, payload);
    ::encode(is_primary, payload);
    ::encode(bl, payload);
  }
};
REGISTER_MESSAGE(MDentryLink, MSG_MDS_DENTRYLINK);
#endif
