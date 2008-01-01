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


#ifndef __MOSDPGREMOVE_H
#define __MOSDPGREMOVE_H

#include "msg/Message.h"


class MOSDPGRemove : public Message {
  epoch_t epoch;

 public:
  set<pg_t> pg_list;

  epoch_t get_epoch() { return epoch; }

  MOSDPGRemove() {}
  MOSDPGRemove(epoch_t e, set<pg_t>& l) :
    Message(MSG_OSD_PG_REMOVE) {
    this->epoch = e;
    pg_list = l;
  }
  
  const char *get_type_name() { return "PGrm"; }

  void encode_payload() {
    payload.append((char*)&epoch, sizeof(epoch));
    _encode(pg_list, payload);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    _decode(pg_list, payload, off);
  }

};

#endif
