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



#ifndef __MOSDALIVE_H
#define __MOSDALIVE_H

#include "msg/Message.h"

class MOSDAlive : public Message {
 public:
  epoch_t map_epoch;

  MOSDAlive(epoch_t e) : Message(MSG_OSD_ALIVE), map_epoch(e) { }
  MOSDAlive() {}

  void encode_payload() {
    ::encode(map_epoch, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(map_epoch, p);
  }

  const char *get_type_name() { return "osd_alive"; }
  void print(ostream &out) {
    out << "osd_alive(" << map_epoch << ")";
  }
  
};

#endif
