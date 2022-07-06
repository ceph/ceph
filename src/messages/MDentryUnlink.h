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

#include "messages/MMDSOp.h"

class MDentryUnlink final : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
  
  dirfrag_t dirfrag;
  std::string dn;

 public:
  dirfrag_t get_dirfrag() const { return dirfrag; }
  const std::string& get_dn() const { return dn; }

  ceph::buffer::list straybl;
  ceph::buffer::list snapbl;

protected:
  MDentryUnlink() :
    MMDSOp(MSG_MDS_DENTRYUNLINK, HEAD_VERSION, COMPAT_VERSION) { }
  MDentryUnlink(dirfrag_t df, std::string_view n) :
    MMDSOp(MSG_MDS_DENTRYUNLINK, HEAD_VERSION, COMPAT_VERSION),
    dirfrag(df),
    dn(n) {}
  ~MDentryUnlink() final {}

public:
  std::string_view get_type_name() const override { return "dentry_unlink";}
  void print(std::ostream& o) const override {
    o << "dentry_unlink(" << dirfrag << " " << dn << ")";
  }
  
  void decode_payload() override {
    using ceph::decode;
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
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
