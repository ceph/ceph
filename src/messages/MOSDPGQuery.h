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

#include "msg/Message.h"

/*
 * PGQuery - query another OSD as to the contents of their PGs
 */

class MOSDPGQuery : public Message {
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;
  version_t       epoch;

 public:
  version_t get_epoch() { return epoch; }
  map<pg_t,pg_query_t>  pg_list;

  MOSDPGQuery() : Message(MSG_OSD_PG_QUERY,
			  HEAD_VERSION,
			  COMPAT_VERSION) {}
  MOSDPGQuery(epoch_t e, map<pg_t,pg_query_t>& ls) :
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
    for (map<pg_t,pg_query_t>::const_iterator p = pg_list.begin(); p != pg_list.end(); ++p) {
      if (p != pg_list.begin())
	out << ",";
      out << p->first;
    }
    out << " epoch " << epoch << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(epoch, payload);
    ::encode(pg_list, payload, features);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    ::decode(pg_list, p);

    if (header.version < 2) {
      for (map<pg_t, pg_query_t>::iterator i = pg_list.begin();
	   i != pg_list.end();
	   ++i) {
	i->second.epoch_sent = epoch;
      }
    }
  }
};

#endif
