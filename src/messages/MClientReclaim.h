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


#ifndef CEPH_MCLIENTRECLAIM_H
#define CEPH_MCLIENTRECLAIM_H

#include "msg/Message.h"

class MClientReclaim: public MessageInstance<MClientReclaim> {
public:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
  static constexpr uint32_t FLAG_FINISH = 1U << 31;

  uint32_t get_flags() const { return flags; }
  std::string_view get_uuid() const { return uuid; }

  std::string_view get_type_name() const override { return "client_reclaim"; }
  void print(ostream& o) const override {
    std::ios_base::fmtflags f(o.flags());
    o << "client_reclaim(" << get_uuid() << " flags 0x" << std::hex << get_flags() << ")";
    o.flags(f);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(uuid, payload);
    encode(flags, payload);
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(uuid, p);
    decode(flags, p);
  }

protected:
  friend factory;
  MClientReclaim() :
    MessageInstance(CEPH_MSG_CLIENT_RECLAIM, HEAD_VERSION, COMPAT_VERSION) {}
  MClientReclaim(std::string_view _uuid, uint32_t _flags) :
    MessageInstance(CEPH_MSG_CLIENT_RECLAIM, HEAD_VERSION, COMPAT_VERSION),
    uuid(_uuid), flags(_flags) {}
private:
  ~MClientReclaim() override {}

  std::string uuid;
  uint32_t flags = 0;
};

#endif
