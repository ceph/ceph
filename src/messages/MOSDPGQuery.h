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
  static const int HEAD_VERSION = 3;
  static const int COMPAT_VERSION = 1;
  version_t       epoch;

 public:
  version_t get_epoch() { return epoch; }
  map<spg_t, pg_query_t>  pg_list;

  MOSDPGQuery() : Message(MSG_OSD_PG_QUERY,
			  HEAD_VERSION,
			  COMPAT_VERSION) {}
  MOSDPGQuery(epoch_t e, map<spg_t,pg_query_t>& ls) :
    Message(MSG_OSD_PG_QUERY,
	    HEAD_VERSION,
	    COMPAT_VERSION),
    epoch(e) {
    pg_list.swap(ls);
  }
private:
  ~MOSDPGQuery() {}

public:  
  const char *get_type_name() const { return "pg_query"; }
  void print(ostream& out) const {
    out << "pg_query(";
    for (map<spg_t,pg_query_t>::const_iterator p = pg_list.begin();
	 p != pg_list.end(); ++p) {
      if (p != pg_list.begin())
	out << ",";
      out << p->first;
    }
    out << " epoch " << epoch << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(epoch, payload);
    vector<pair<pg_t, pg_query_t> > _pg_list;
    _pg_list.reserve(pg_list.size());
    vector<shard_id_t> _shard_list;
    _shard_list.reserve(pg_list.size());
    for (map<spg_t, pg_query_t>::iterator i = pg_list.begin();
	 i != pg_list.end();
	 ++i) {
      _pg_list.push_back(make_pair(i->first.pgid, i->second));
      _shard_list.push_back(i->first.shard);
    }
    ::encode(_pg_list, payload, features);
    ::encode(_shard_list, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    vector<pair<pg_t, pg_query_t> > _pg_list;
    ::decode(_pg_list, p);
    vector<shard_id_t> _shard_list(_pg_list.size(), shard_id_t::NO_SHARD);
    if (header.version >= 3) {
      _shard_list.clear();
      ::decode(_shard_list, p);
    }
    assert(_pg_list.size() == _shard_list.size());
    for (unsigned i = 0; i < _pg_list.size(); ++i) {
      pg_list.insert(
	make_pair(
	  spg_t(_pg_list[i].first, _shard_list[i]), _pg_list[i].second));
    }

    if (header.version < 2) {
      for (map<spg_t, pg_query_t>::iterator i = pg_list.begin();
	   i != pg_list.end();
	   ++i) {
	i->second.epoch_sent = epoch;
      }
    }
  }
};
REGISTER_MESSAGE(MOSDPGQuery, MSG_OSD_PG_QUERY);
#endif
