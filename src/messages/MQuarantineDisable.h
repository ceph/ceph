// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#ifndef CEPH_MQUARANTINEDISABLE_H
#define CEPH_MQUARANTINEDISABLE_H

#include "msg/Message.h"


class MQuarantineDisable final : public SafeMessage {
 public:
  std::string_view get_type_name() const override { return "quarantine_disable";}
  void print(std::ostream& out) const override {
    out << "quarantine_disable(" << oserrno << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(ino, p);
    decode(oserrno, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(ino, payload);
    encode(oserrno, payload);
  }

  inodeno_t ino;
  int oserrno = 0; // used to reset in->oserrno at client side to zero to allow ops

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  MQuarantineDisable() : 
    SafeMessage{CEPH_MSG_CLIENT_QUARANTINE_DISABLE, HEAD_VERSION, COMPAT_VERSION}
  { }
  ~MQuarantineDisable() final {}
};

#endif
