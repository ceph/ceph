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


#ifndef __MOSDPGINFO_H
#define __MOSDPGINFO_H

#include "msg/Message.h"

class MOSDPGInfo : public Message {
  epoch_t epoch;

public:
  vector<PG::Info> pg_info;

  epoch_t get_epoch() { return epoch; }

  MOSDPGInfo() {}
  MOSDPGInfo(version_t mv) :
    Message(MSG_OSD_PG_INFO),
    epoch(mv) { }
private:
  ~MOSDPGInfo() {}

public:
  const char *get_type_name() { return "pg_info"; }
  void print(ostream& out) {
    out << "pg_info(" << pg_info.size() << " pgs e" << epoch << ")";
  }

  void encode_payload() {
    ::encode(epoch, payload);
    ::encode(pg_info, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    ::decode(pg_info, p);
  }
};

#endif
