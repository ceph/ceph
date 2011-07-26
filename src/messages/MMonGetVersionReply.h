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
class MMonGetVersionReply : public Message {
public:
  MMonGetVersionReply() : Message(CEPH_MSG_MON_GET_VERSION_REPLY) {}

  const char *get_type_name() {
    return "mon_check_map_ack";
  }

  void print(ostream& o) {
    o << "mon_check_map_ack(handle=" << handle << " version=" << version << ")";
  }

  void encode_payload(CephContext *cct) {
    ::encode(handle, payload);
    ::encode(version, payload);
  }

  void decode_payload(CephContext *cct) {
    bufferlist::iterator p = payload.begin();
    ::decode(handle, p);
    ::decode(version, p);
  }

  tid_t handle;
  version_t version;

private:
  ~MMonGetVersionReply() {}
};

#endif
