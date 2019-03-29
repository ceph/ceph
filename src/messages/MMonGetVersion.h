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

#ifndef CEPH_MMONGETVERSION_H
#define CEPH_MMONGETVERSION_H

#include "msg/Message.h"

#include "include/types.h"

/*
 * This message is sent to the monitors to verify that the client's
 * version of the map(s) is the latest available. For example, this
 * can be used to determine whether a pool actually does not exist, or
 * if it may have been created but the map was not received yet.
 */
class MMonGetVersion : public MessageInstance<MMonGetVersion> {
public:
  friend factory;

  MMonGetVersion() : MessageInstance(CEPH_MSG_MON_GET_VERSION) {}

  std::string_view get_type_name() const override {
    return "mon_get_version";
  }

  void print(std::ostream& o) const override {
    o << "mon_get_version(what=" << what << " handle=" << handle << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(handle, payload);
    encode(what, payload);
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    using ceph::decode;
    decode(handle, p);
    decode(what, p);
  }

  ceph_tid_t handle = 0;
  std::string what;

private:
  ~MMonGetVersion() override {}
};

#endif
