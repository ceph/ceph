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
class MMonGetVersion : public Message {
public:
  MMonGetVersion() : Message(CEPH_MSG_MON_GET_VERSION) {}

  const char *get_type_name() const {
    return "mon_get_version";
  }

  void print(ostream& o) const {
    o << "mon_get_version(what=" << what << " handle=" << handle << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(handle, payload);
    ::encode(what, payload);
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(handle, p);
    ::decode(what, p);
  }

  ceph_tid_t handle;
  string what;

private:
  ~MMonGetVersion() {}
};
REGISTER_MESSAGE(MMonGetVersion, CEPH_MSG_MON_GET_VERSION);
#endif
