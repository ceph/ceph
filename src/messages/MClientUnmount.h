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

#ifndef __MCLIENTUNMOUNT_H
#define __MCLIENTUNMOUNT_H

#include "messages/PaxosServiceMessage.h"

class MClientUnmount : public PaxosServiceMessage {
public:
  MClientUnmount() : PaxosServiceMessage(CEPH_MSG_CLIENT_UNMOUNT, VERSION_T) { }
 
  const char *get_type_name() { return "client_unmount"; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
  }
  void encode_payload() {
    paxos_encode();
  }
};

#endif
