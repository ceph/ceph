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

#ifndef CEPH_MOSDPGPEERNOTIFY_H
#define CEPH_MOSDPGPEERNOTIFY_H

#include "msg/Message.h"

#include "osd/osd_types.h"

/*
 * PGNotify - notify primary of my PGs and versions.
 */

class MOSDPGNotify : public MessageInstance<MOSDPGNotify> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 6;
  static constexpr int COMPAT_VERSION = 6;

  epoch_t epoch = 0;
  /// query_epoch is the epoch of the query being responded to, or
  /// the current epoch if this is not being sent in response to a
  /// query. This allows the recipient to disregard responses to old
  /// queries.
  using pg_list_t = std::vector<std::pair<pg_notify_t,PastIntervals>>;
  pg_list_t pg_list;   // pgid -> version

 public:
  version_t get_epoch() const { return epoch; }
  const pg_list_t& get_pg_list() const {
    return pg_list;
  }

  MOSDPGNotify()
    : MOSDPGNotify(0, {})
  {}
  MOSDPGNotify(epoch_t e, pg_list_t&& l)
    : MessageInstance(MSG_OSD_PG_NOTIFY, HEAD_VERSION, COMPAT_VERSION),
      epoch(e),
      pg_list(std::move(l)) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
private:
  ~MOSDPGNotify() override {}

public:  
  std::string_view get_type_name() const override { return "PGnot"; }

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
  void print(ostream& out) const override {
    out << "pg_notify(";
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
};

#endif
