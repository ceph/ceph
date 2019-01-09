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

#ifndef CEPH_MMONGETVERSIONREPLY_H
#define CEPH_MMONGETVERSIONREPLY_H

#include "msg/Message.h"

#include "include/types.h"

/*
 * This message is sent from the monitors to clients in response to a
 * MMonGetVersion. The latest version of the requested thing is sent
 * back.
 */
class MMonGetVersionReply : public MessageInstance<MMonGetVersionReply> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 2;

public:
  MMonGetVersionReply() : MessageInstance(CEPH_MSG_MON_GET_VERSION_REPLY, HEAD_VERSION) { }

  std::string_view get_type_name() const override {
    return "mon_get_version_reply";
  }

  void print(ostream& o) const override {
    o << "mon_get_version_reply(handle=" << handle << " version=" << version << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(handle, payload);
    encode(version, payload);
    encode(oldest_version, payload);
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(handle, p);
    decode(version, p);
    if (header.version >= 2)
      decode(oldest_version, p);
  }

  ceph_tid_t handle = 0;
  version_t version = 0;
  version_t oldest_version = 0;

private:
  ~MMonGetVersionReply() override {}
};

#endif
