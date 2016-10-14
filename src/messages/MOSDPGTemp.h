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



#ifndef CEPH_MOSDPGTEMP_H
#define CEPH_MOSDPGTEMP_H

#include "messages/PaxosServiceMessage.h"

class MOSDPGTemp : public PaxosServiceMessage {
 public:
  epoch_t map_epoch;
  map<pg_t, vector<int32_t> > pg_temp;

  MOSDPGTemp(epoch_t e) : PaxosServiceMessage(MSG_OSD_PGTEMP, e), map_epoch(e) { }
  MOSDPGTemp() : PaxosServiceMessage(MSG_OSD_PGTEMP, 0) {}
private:
  ~MOSDPGTemp() {}

public:
  void encode_payload(uint64_t features) {
    paxos_encode();
    ::encode(map_epoch, payload);
    ::encode(pg_temp, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(map_epoch, p);
    ::decode(pg_temp, p);
  }

  const char *get_type_name() const { return "osd_pgtemp"; }
  void print(ostream &out) const {
    out << "osd_pgtemp(e" << map_epoch << " " << pg_temp << " v" << version << ")";
  }
  
};

#endif
