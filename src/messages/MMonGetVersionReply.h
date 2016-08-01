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

  static const int HEAD_VERSION = 2;

public:
  MMonGetVersionReply() : Message(CEPH_MSG_MON_GET_VERSION_REPLY, HEAD_VERSION) { }

  const char *get_type_name() const {
    return "mon_get_version_reply";
  }

  void print(ostream& o) const {
    o << "mon_get_version_reply(handle=" << handle << " version=" << version << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(handle, payload);
    ::encode(version, payload);
    ::encode(oldest_version, payload);
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(handle, p);
    ::decode(version, p);
    if (header.version >= 2)
      ::decode(oldest_version, p);
  }

  ceph_tid_t handle;
  version_t version;
  version_t oldest_version;

private:
  ~MMonGetVersionReply() {}
};

#endif
