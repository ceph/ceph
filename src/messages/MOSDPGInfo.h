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
  static const int HEAD_VERSION = 5;
  static const int COMPAT_VERSION = 1;

  epoch_t epoch;

public:
  vector<pair<pg_notify_t,PastIntervals> > pg_list;

  epoch_t get_epoch() const { return epoch; }

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
  ~MOSDPGInfo() override {}

public:
  const char *get_type_name() const override { return "pg_info"; }
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
    if (!HAVE_FEATURE(features, SERVER_LUMINOUS)) {
      header.version = 4;

      // for kraken+jewel only
      ::encode(epoch, payload);

      // v1 was vector<pg_info_t>
      __u32 n = pg_list.size();
      ::encode(n, payload);
      for (auto p = pg_list.begin();
	   p != pg_list.end();
	   p++)
	::encode(p->first.info, payload);

      // v2 needs the PastIntervals for each record
      for (auto p = pg_list.begin();
	   p != pg_list.end();
	   p++) {
	p->second.encode_classic(payload);
      }

      // v3 needs epoch_sent, query_epoch
      for (auto p = pg_list.begin();
	   p != pg_list.end();
	   p++)
	::encode(pair<epoch_t, epoch_t>(
		   p->first.epoch_sent, p->first.query_epoch), payload);

      // v4 needs from, to
      for (auto p = pg_list.begin();
	   p != pg_list.end();
	   ++p) {
	::encode(p->first.from, payload);
	::encode(p->first.to, payload);
      }
      return;
    }
    ::encode(epoch, payload);
    ::encode(pg_list, payload);
  }
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    if (header.version < 5) {
      ::decode(epoch, p);

      // decode pg_info_t portion of the vector
      __u32 n;
      ::decode(n, p);
      pg_list.resize(n);
      for (unsigned i=0; i<n; i++) {
	::decode(pg_list[i].first.info, p);
      }

      if (header.version >= 2) {
	// get the PastIntervals portion
	for (unsigned i=0; i<n; i++) {
	  if (header.version >= 5) {
	    ::decode(pg_list[i].second, p);
	  } else {
	    pg_list[i].second.decode_classic(p);
	  }
	}
      }

      // v3 needs epoch_sent, query_epoch
      for (auto i = pg_list.begin();
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
	for (auto i = pg_list.begin();
	     i != pg_list.end();
	     i++) {
	  ::decode(i->first.from, p);
	  ::decode(i->first.to, p);
	}
      }
      return;
    }
    ::decode(epoch, p);
    ::decode(pg_list, p);
  }
};

#endif
