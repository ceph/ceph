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

#ifndef CEPH_MPGSTATSACK_H
#define CEPH_MPGSTATSACK_H

#include "osd/osd_types.h"

class MPGStatsAck : public MessageInstance<MPGStatsAck> {
public:
  friend factory;

  map<pg_t,pair<version_t,epoch_t> > pg_stat;
  
  MPGStatsAck() : MessageInstance(MSG_PGSTATSACK) {}

private:
  ~MPGStatsAck() override {}

public:
  std::string_view get_type_name() const override { return "pg_stats_ack"; }
  void print(ostream& out) const override {
    out << "pg_stats_ack(" << pg_stat.size() << " pgs tid " << get_tid() << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pg_stat, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(pg_stat, p);
  }
};

#endif
