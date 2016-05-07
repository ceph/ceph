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

class MOSDPGInfo : public Message {
  static const int HEAD_VERSION = 4;
  static const int COMPAT_VERSION = 1;

  epoch_t epoch;

public:
  vector<pair<pg_notify_t,pg_interval_map_t> > pg_list;

  epoch_t get_epoch() { return epoch; }

  MOSDPGInfo()
    : Message(MSG_OSD_PG_INFO, HEAD_VERSION, COMPAT_VERSION) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
  MOSDPGInfo(version_t mv)
    : Message(MSG_OSD_PG_INFO, HEAD_VERSION, COMPAT_VERSION),
      epoch(mv) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
private:
  ~MOSDPGInfo() {}

public:
  const char *get_type_name() const { return "pg_info"; }
  void print(ostream& out) const {
    out << "pg_info(" << pg_list.size() << " pgs e" << epoch << ":";

    for (vector<pair<pg_notify_t,pg_interval_map_t> >::const_iterator i = pg_list.begin();
         i != pg_list.end();
         ++i) {
      if (i != pg_list.begin())
	out << ",";
      out << i->first.info.pgid;
      if (i->second.size())
	out << "(" << i->second.size() << ")";
    }

    out << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(epoch, payload);

    // v1 was vector<pg_info_t>
    __u32 n = pg_list.size();
    ::encode(n, payload);
    for (vector<pair<pg_notify_t,pg_interval_map_t> >::iterator p = pg_list.begin();
	 p != pg_list.end();
	 p++)
      ::encode(p->first.info, payload);

    // v2 needs the pg_interval_map_t for each record
    for (vector<pair<pg_notify_t,pg_interval_map_t> >::iterator p = pg_list.begin();
	 p != pg_list.end();
	 p++)
      ::encode(p->second, payload);

    // v3 needs epoch_sent, query_epoch
    for (vector<pair<pg_notify_t,pg_interval_map_t> >::iterator p = pg_list.begin();
	 p != pg_list.end();
	 p++)
      ::encode(pair<epoch_t, epoch_t>(
		 p->first.epoch_sent, p->first.query_epoch), payload);

    // v4 needs from, to
    for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator p = pg_list.begin();
	 p != pg_list.end();
	 ++p) {
      ::encode(p->first.from, payload);
      ::encode(p->first.to, payload);
    }
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);

    // decode pg_info_t portion of the vector
    __u32 n;
    ::decode(n, p);
    pg_list.resize(n);
    for (unsigned i=0; i<n; i++) {
      ::decode(pg_list[i].first.info, p);
    }

    if (header.version >= 2) {
      // get the pg_interval_map_t portion
      for (unsigned i=0; i<n; i++) {
	::decode(pg_list[i].second, p);
      }
    }

    // v3 needs epoch_sent, query_epoch
    for (vector<pair<pg_notify_t,pg_interval_map_t> >::iterator i = pg_list.begin();
	 i != pg_list.end();
	 i++) {
      if (header.version >= 3) {
	pair<epoch_t, epoch_t> dec;
	::decode(dec, p);
	i->first.epoch_sent = dec.first;
	i->first.query_epoch = dec.second;
      } else {
	i->first.epoch_sent = epoch;
	i->first.query_epoch = epoch;
      }
    }

    // v4 needs from and to
    if (header.version >= 4) {
      for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator i = pg_list.begin();
	   i != pg_list.end();
	   i++) {
	::decode(i->first.from, p);
	::decode(i->first.to, p);
      }
    }
  }
};

#endif
