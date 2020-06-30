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


#ifndef CEPH_MOSDPGQUERY_H
#define CEPH_MOSDPGQUERY_H

#include "common/hobject.h"
#include "msg/Message.h"

/*
 * PGQuery - query another OSD as to the contents of their PGs
 */

class MOSDPGQuery : public Message {
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 4;

  version_t epoch = 0;

 public:
  version_t get_epoch() const { return epoch; }
  using pg_list_t = std::map<spg_t, pg_query_t>;
  pg_list_t pg_list;

  MOSDPGQuery() : Message{MSG_OSD_PG_QUERY,
			  HEAD_VERSION,
			  COMPAT_VERSION} {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
  MOSDPGQuery(epoch_t e, pg_list_t&& ls) :
    Message{MSG_OSD_PG_QUERY,
	    HEAD_VERSION,
	    COMPAT_VERSION},
    epoch(e),
    pg_list(std::move(ls)) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
private:
  ~MOSDPGQuery() override {}

public:  
  std::string_view get_type_name() const override { return "pg_query"; }
  void print(std::ostream& out) const override {
    out << "pg_query(";
    for (auto p = pg_list.begin(); p != pg_list.end(); ++p) {
      if (p != pg_list.begin())
	out << ",";
      out << p->first;
    }
    out << " epoch " << epoch << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(pg_list, payload, features);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(pg_list, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
