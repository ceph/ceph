// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MDSOPENINOREPLY_H
#define CEPH_MDSOPENINOREPLY_H

#include "messages/MMDSOp.h"

class MMDSOpenInoReply : public MMDSOp {
public:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
  inodeno_t ino;
  std::vector<inode_backpointer_t> ancestors;
  mds_rank_t hint;
  int32_t error;

protected:
  MMDSOpenInoReply() : MMDSOp{MSG_MDS_OPENINOREPLY, HEAD_VERSION, COMPAT_VERSION}, error(0) {}
  MMDSOpenInoReply(ceph_tid_t t, inodeno_t i, mds_rank_t h=MDS_RANK_NONE, int e=0) :
    MMDSOp{MSG_MDS_OPENINOREPLY, HEAD_VERSION, COMPAT_VERSION}, ino(i), hint(h), error(e) {
    header.tid = t;
  }


public:
  std::string_view get_type_name() const override { return "openinoreply"; }
  void print(std::ostream &out) const override {
    out << "openinoreply(" << header.tid << " "
	<< ino << " " << hint << " " << ancestors << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(ino, payload);
    encode(ancestors, payload);
    encode(hint, payload);
    encode(error, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(ino, p);
    decode(ancestors, p);
    decode(hint, p);
    decode(error, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
