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


#ifndef __MOSDPGACTIVATESET_H
#define __MOSDPGACTIVATESET_H

#include "msg/Message.h"

class MOSDPGActivateSet : public Message {
  epoch_t epoch;

public:
  list<PG::Info> pg_info;

  epoch_t get_epoch() { return epoch; }

  MOSDPGActivateSet() {}
  MOSDPGActivateSet(version_t mv) :
    Message(MSG_OSD_PG_ACTIVATE_SET),
    epoch(mv) { }

  const char *get_type_name() { return "pg_activate_set"; }
  void print(ostream& out) {
    out << "pg_activate_set(" << pg_info.size() << " pgs e" << epoch << ")";
  }

  void encode_payload() {
    ::_encode(epoch, payload);
    ::_encode(pg_info, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(epoch, payload, off);
    ::_decode(pg_info, payload, off);
  }
};

#endif
