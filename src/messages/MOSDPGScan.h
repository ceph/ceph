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

#ifndef CEPH_MOSDPGSCAN_H
#define CEPH_MOSDPGSCAN_H

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDPGScan : public Message {
public:
  enum {
    OP_SCAN_GET_DIGEST = 1,      // just objects and versions
    OP_SCAN_DIGEST = 2,          // result
  };
  const char *get_op_name(int o) {
    switch (o) {
    case OP_SCAN_GET_DIGEST: return "get_digest";
    case OP_SCAN_DIGEST: return "digest";
    default: return "???";
    }
  }

  __u32 op;
  epoch_t map_epoch, query_epoch;
  pg_t pgid;
  hobject_t begin, end;

  virtual void decode_payload(CephContext *cct) {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(map_epoch, p);
    ::decode(query_epoch, p);
    ::decode(pgid, p);
    ::decode(begin, p);
    ::decode(end, p);
  }

  virtual void encode_payload(CephContext *cct) {
    ::encode(op, payload);
    ::encode(map_epoch, payload);
    ::encode(query_epoch, payload);
    ::encode(pgid, payload);
    ::encode(begin, payload);
    ::encode(end, payload);
  }

  MOSDPGScan() : Message(MSG_OSD_PG_SCAN) {}
  MOSDPGScan(__u32 o, epoch_t e, epoch_t qe, pg_t p, hobject_t be, hobject_t en)
    : Message(MSG_OSD_PG_SCAN),
      op(o),
      map_epoch(e), query_epoch(e),
      pgid(p),
      begin(be), end(en) {
  }
private:
  ~MOSDPGScan() {}

public:
  const char *get_type_name() { return "pg_scan"; }
  void print(ostream& out) {
    out << "pg_scan(" << get_op_name(op)
	<< " " << pgid
	<< " " << begin << "-" << end
	<< " e " << map_epoch << "/" << query_epoch
	<< ")";
  }
};

#endif
