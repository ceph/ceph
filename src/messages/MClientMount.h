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

#ifndef __MCLIENTMOUNT_H
#define __MCLIENTMOUNT_H

#include "messages/PaxosServiceMessage.h"

class MClientMount : public PaxosServiceMessage {
public:
  MClientMount() : PaxosServiceMessage(CEPH_MSG_CLIENT_MOUNT, VERSION_T) { }

  const char *get_type_name() { return "client_mount"; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
  }
  void encode_payload() {
    paxos_encode();
  }
};

#endif
