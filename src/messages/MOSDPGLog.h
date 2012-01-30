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


#ifndef CEPH_MOSDPGLOG_H
#define CEPH_MOSDPGLOG_H

#include "msg/Message.h"

class MOSDPGLog : public Message {
  epoch_t epoch;
  /// query_epoch is the epoch of the query being responded to, or
  /// the current epoch if this is not being sent in response to a
  /// query. This allows the recipient to disregard responses to old
  /// queries.
  epoch_t query_epoch;

public:
  PG::Info info;
  PG::Log log;
  PG::Missing missing;

  epoch_t get_epoch() { return epoch; }
  pg_t get_pgid() { return info.pgid; }
  epoch_t get_query_epoch() { return query_epoch; }

  MOSDPGLog() {}
  MOSDPGLog(version_t mv, PG::Info& i) :
    Message(MSG_OSD_PG_LOG),
    epoch(mv), query_epoch(mv), info(i)  { }
  MOSDPGLog(version_t mv, PG::Info& i, epoch_t query_epoch) :
    Message(MSG_OSD_PG_LOG),
    epoch(mv), query_epoch(query_epoch), info(i)  { }
private:
  ~MOSDPGLog() {}

public:
  const char *get_type_name() { return "PGlog"; }
  void print(ostream& out) {
    out << "pg_log(" << info.pgid << " epoch " << epoch
	<< " query_epoch " << query_epoch << ")";
  }

  void encode_payload(uint64_t features) {
    header.version = 2;
    ::encode(epoch, payload);
    ::encode(info, payload);
    ::encode(log, payload);
    ::encode(missing, payload);
    ::encode(query_epoch, payload);
  }
  void decode_payload(CephContext *cct) {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    ::decode(info, p);
    ::decode(log, p);
    ::decode(missing, p);
    if (header.version > 1) {
      ::decode(query_epoch, p);
    }
  }
};

#endif
