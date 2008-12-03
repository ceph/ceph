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


#ifndef __MOSDPGSCRUB_H
#define __MOSDPGSCRUB_H

#include "msg/Message.h"

struct MOSDPGScrub : public Message {
  pg_t pgid;
  epoch_t epoch;
  bufferlist map;

  MOSDPGScrub() {}
  MOSDPGScrub(pg_t p, epoch_t e) :
    Message(MSG_OSD_PG_SCRUB),
    pgid(p), epoch(e) {}

  const char *get_type_name() { return "pg_scrub"; }
  void print(ostream& out) {
    out << "pg_scrub(" << pgid << " e" << epoch;
    if (map.length())
      out << " " << map.length() << " bytes";
    out << ")";
  }

  void encode_payload() {
    ::encode(pgid, payload);
    ::encode(epoch, payload);
    ::encode(map, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(pgid, p);
    ::decode(epoch, p);
    ::decode(map, p);
  }
};

#endif
