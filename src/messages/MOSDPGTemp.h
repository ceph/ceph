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

class MOSDPGTemp : public MessageInstance<MOSDPGTemp, PaxosServiceMessage> {
public:
  friend factory;

  epoch_t map_epoch = 0;
  map<pg_t, vector<int32_t> > pg_temp;
  bool forced = false;

  MOSDPGTemp(epoch_t e)
    : MessageInstance(MSG_OSD_PGTEMP, e, HEAD_VERSION, COMPAT_VERSION),
      map_epoch(e)
  {}
  MOSDPGTemp()
    : MOSDPGTemp(0)
  {}
private:
  ~MOSDPGTemp() override {}

public:
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(map_epoch, payload);
    encode(pg_temp, payload);
    encode(forced, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(map_epoch, p);
    decode(pg_temp, p);
    if (header.version >= 2) {
      decode(forced, p);
    }
  }

  std::string_view get_type_name() const override { return "osd_pgtemp"; }
  void print(ostream &out) const override {
    out << "osd_pgtemp(e" << map_epoch << " " << pg_temp << " v" << version << ")";
  }
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;
};

#endif
