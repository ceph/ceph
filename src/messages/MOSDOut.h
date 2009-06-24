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



#ifndef __MOSDOUT_H
#define __MOSDOUT_H

#include "messages/PaxosServiceMessage.h"


class MOSDOut : public PaxosServiceMessage {
 public:
  epoch_t map_epoch;

  MOSDOut(epoch_t e) : PaxosServiceMessage(MSG_OSD_OUT, e), map_epoch(e) {
  }
  MOSDOut() {}

  void encode_payload() {
    paxos_encode();
    ::encode(map_epoch, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(map_epoch, p);
  }

  const char *get_type_name() { return "oout"; }
};

#endif
