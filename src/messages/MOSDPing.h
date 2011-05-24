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

#ifndef CEPH_MOSDPING_H
#define CEPH_MOSDPING_H

#include "common/Clock.h"

#include "msg/Message.h"
#include "osd/osd_types.h"


class MOSDPing : public Message {
 public:
  enum {
    HEARTBEAT = 0,
    REQUEST_HEARTBEAT = 1,
    YOU_DIED = 2,
  };
  const char *get_op_name(int op) {
    switch (op) {
    case HEARTBEAT: return "heartbeat";
    case REQUEST_HEARTBEAT: return "request_heartbeat";
    case YOU_DIED: return "you_died";
    default: return "???";
    }
  }

  ceph_fsid_t fsid;
  epoch_t map_epoch, peer_as_of_epoch;
  __u8 op;
  osd_peer_stat_t peer_stat;

  MOSDPing(const ceph_fsid_t& f, epoch_t e, epoch_t pe, osd_peer_stat_t& ps, __u8 o=HEARTBEAT) : 
    Message(MSG_OSD_PING), fsid(f), map_epoch(e), peer_as_of_epoch(pe), op(o), peer_stat(ps) { }
  MOSDPing() {}
private:
  ~MOSDPing() {}

public:
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(map_epoch, p);
    ::decode(peer_as_of_epoch, p);
    ::decode(op, p);
    ::decode(peer_stat, p);
  }
  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(map_epoch, payload);
    ::encode(peer_as_of_epoch, payload);
    ::encode(op, payload);
    ::encode(peer_stat, payload);
  }

  const char *get_type_name() { return "osd_ping"; }
  void print(ostream& out) {
    out << "osd_ping(e" << map_epoch << " as_of " << peer_as_of_epoch
	<< " " << get_op_name(op) << ")";
  }
};

#endif
