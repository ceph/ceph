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

#ifndef __MMONMAP_H
#define __MMONMAP_H

#include "msg/Message.h"

class MMonMap : public Message {
public:
  entity_addr_t addr;
  bufferlist monmapbl;

  MMonMap() : Message(CEPH_MSG_MON_MAP) { }
  MMonMap(entity_addr_t t, bufferlist &bl) : Message(CEPH_MSG_MON_MAP) { 
    addr = t;
    monmapbl.claim(bl);
  }

  const char *get_type_name() { return "mon_map"; }

  void encode_payload() { 
    ::encode(addr, payload);
    ::encode(monmapbl, payload);
  }
  void decode_payload() { 
    bufferlist::iterator p = payload.begin();
    ::decode(addr, p);
    ::decode(monmapbl, p);
  }
};

#endif
