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

#include <string_view>

#include "msg/Message.h"

class MDentryLink : public MessageInstance<MDentryLink> {
public:
  friend factory;
private:
  dirfrag_t subtree;
  dirfrag_t dirfrag;
  string dn;
  bool is_primary = false;

 public:
  dirfrag_t get_subtree() const { return subtree; }
  dirfrag_t get_dirfrag() const { return dirfrag; }
  const string& get_dn() const { return dn; }
  bool get_is_primary() const { return is_primary; }

  bufferlist bl;

protected:
  MDentryLink() :
    MessageInstance(MSG_MDS_DENTRYLINK) { }
  MDentryLink(dirfrag_t r, dirfrag_t df, std::string_view n, bool p) :
    MessageInstance(MSG_MDS_DENTRYLINK),
    subtree(r),
    dirfrag(df),
    dn(n),
    is_primary(p) {}
  ~MDentryLink() override {}

public:
  std::string_view get_type_name() const override { return "dentry_link";}
  void print(ostream& o) const override {
    o << "dentry_link(" << dirfrag << " " << dn << ")";
  }
  
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(subtree, p);
    decode(dirfrag, p);
    decode(dn, p);
    decode(is_primary, p);
    decode(bl, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(subtree, payload);
    encode(dirfrag, payload);
    encode(dn, payload);
    encode(is_primary, payload);
    encode(bl, payload);
  }
};

#endif
