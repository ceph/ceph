// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MMDSSNAPUPDATEREPLY_H
#define CEPH_MMDSSNAPUPDATEREPLY_H

#include "messages/MMDSOp.h"

class MMDSSnapUpdateReply final : public MMDSOp {
private:
  inodeno_t ino;

public:
  inodeno_t get_ino() const { return ino; }

protected:
  MMDSSnapUpdateReply() : MMDSOp{MSG_MDS_SNAPUPDATEREPLY} {}
  MMDSSnapUpdateReply(inodeno_t i, version_t tid) :
    MMDSOp{MSG_MDS_SNAPUPDATEREPLY}, ino(i) {
      set_tid(tid);
    }
  ~MMDSSnapUpdateReply() final {}

public:
  std::string_view get_type_name() const override { return "snap_update_reply"; }
  void print(std::ostream& o) const override {
    o << "snap_update_reply(" << ino << " table_tid " << get_tid() << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(ino, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(ino, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
