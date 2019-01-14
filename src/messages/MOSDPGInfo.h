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


#ifndef CEPH_MOSDPGINFO_H
#define CEPH_MOSDPGINFO_H

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDPGInfo : public MessageInstance<MOSDPGInfo> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 5;
  static constexpr int COMPAT_VERSION = 5;

  epoch_t epoch = 0;

public:
  vector<pair<pg_notify_t,PastIntervals> > pg_list;

  epoch_t get_epoch() const { return epoch; }

  MOSDPGInfo()
    : MessageInstance(MSG_OSD_PG_INFO, HEAD_VERSION, COMPAT_VERSION) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
  MOSDPGInfo(version_t mv)
    : MessageInstance(MSG_OSD_PG_INFO, HEAD_VERSION, COMPAT_VERSION),
      epoch(mv) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
private:
  ~MOSDPGInfo() override {}

public:
  std::string_view get_type_name() const override { return "pg_info"; }
  void print(ostream& out) const override {
    out << "pg_info(";
    for (auto i = pg_list.begin();
         i != pg_list.end();
         ++i) {
      if (i != pg_list.begin())
	out << " ";
      out << i->first << "=" << i->second;
    }
    out << " epoch " << epoch
	<< ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(pg_list, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(pg_list, p);
  }
};

#endif
