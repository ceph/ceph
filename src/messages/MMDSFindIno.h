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

#ifndef CEPH_MDSFINDINO_H
#define CEPH_MDSFINDINO_H

#include "include/filepath.h"
#include "messages/MMDSOp.h"

class MMDSFindIno final : public MMDSOp {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
public:
  ceph_tid_t tid {0};
  inodeno_t ino;

protected:
  MMDSFindIno() : MMDSOp{MSG_MDS_FINDINO, HEAD_VERSION, COMPAT_VERSION} {}
  MMDSFindIno(ceph_tid_t t, inodeno_t i) : MMDSOp{MSG_MDS_FINDINO, HEAD_VERSION, COMPAT_VERSION}, tid(t), ino(i) {}
  ~MMDSFindIno() final {}

public:
  std::string_view get_type_name() const override { return "findino"; }
  void print(std::ostream &out) const override {
    out << "findino(" << tid << " " << ino << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(tid, payload);
    encode(ino, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(tid, p);
    decode(ino, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
