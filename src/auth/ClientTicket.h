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

#ifndef __CEPH_AUTH_CLIENTTICKET
#define __CEPH_AUTH_CLIENTTICKET

#include "include/types.h"

struct ClientTicket {
  int client;
  entity_addr_t addr;
  utime_t created, expires;
  __u32 flags;

  void encode(bufferlist& bl) const {
    ::encode(client, bl);
    ::encode(addr, bl);
    ::encode(created, bl);
    ::encode(expires, bl);
    ::encode(flags, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(client, bl);
    ::decode(addr, bl);
    ::decode(created, bl);
    ::decode(expires, bl);
    ::decode(flags, bl);
  }

};
WRITE_CLASS_ENCODER(ClientTicket)

#endif
