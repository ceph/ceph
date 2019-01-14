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

#include <string_view>

#include "msg/Message.h"

class MDentryUnlink : public MessageInstance<MDentryUnlink> {
public:
  friend factory;
private:

  dirfrag_t dirfrag;
  string dn;

 public:
  dirfrag_t get_dirfrag() const { return dirfrag; }
  const string& get_dn() const { return dn; }

  bufferlist straybl;
  bufferlist snapbl;

protected:
  MDentryUnlink() :
    MessageInstance(MSG_MDS_DENTRYUNLINK) { }
  MDentryUnlink(dirfrag_t df, std::string_view n) :
    MessageInstance(MSG_MDS_DENTRYUNLINK),
    dirfrag(df),
    dn(n) {}
  ~MDentryUnlink() override {}

public:
  std::string_view get_type_name() const override { return "dentry_unlink";}
  void print(ostream& o) const override {
    o << "dentry_unlink(" << dirfrag << " " << dn << ")";
  }
  
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(dirfrag, p);
    decode(dn, p);
    decode(straybl, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
    encode(dn, payload);
    encode(straybl, payload);
  }
};

#endif
