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


#ifndef __MOSDPGUPDATE_H
#define __MOSDPGUPDATE_H

#include "msg/Message.h"

class MOSDPGUpdate : public Message {
  version_t   map_version;
  pg_t        pgid;
  //pginfo_t    info;
  bool        complete;
  version_t   last_any_complete;

 public:
  version_t get_version() { return map_version; }
  pg_t get_pgid() { return pgid; }
  //pginfo_t& get_pginfo() { return info; }
  bool is_complete() { return complete; }
  version_t get_last_any_complete() { return last_any_complete; }

  MOSDPGUpdate() {}
  MOSDPGUpdate(version_t mv, pg_t pgid, bool complete, version_t last_any_complete) :
    Message(MSG_OSD_PG_UPDATE) {
    this->map_version = mv;
    this->pgid = pgid;
    this->complete = complete;
    this->last_any_complete = last_any_complete;
  }
  
  char *get_type_name() { return "PGUp"; }
  void print(ostream& out) {
    out << "pg_update(" << pgid << " e" << map_version;
    if (complete) out << " complete";
    out << " lac=" << last_any_complete;
    out << ")";
  }

  void encode_payload() {
    ::encode(map_version, payload);
    ::encode(pgid, payload);
    ::encode(complete, payload);
    ::encode(last_any_complete, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(map_version, p);
    ::decode(pgid, p);
    ::decode(complete, p);
    ::decode(last_any_complete, p);
  }
};

#endif
