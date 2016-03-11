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

  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;

 public:
  enum {
    HEARTBEAT = 0,
    START_HEARTBEAT = 1,
    YOU_DIED = 2,
    STOP_HEARTBEAT = 3,
    PING = 4,
    PING_REPLY = 5,
  };
  const char *get_op_name(int op) const {
    switch (op) {
    case HEARTBEAT: return "heartbeat";
    case START_HEARTBEAT: return "start_heartbeat";
    case STOP_HEARTBEAT: return "stop_heartbeat";
    case YOU_DIED: return "you_died";
    case PING: return "ping";
    case PING_REPLY: return "ping_reply";
    default: return "???";
    }
  }

  uuid_d fsid;
  epoch_t map_epoch, peer_as_of_epoch;
  __u8 op;
  osd_peer_stat_t peer_stat;
  utime_t stamp;

  MOSDPing(const uuid_d& f, epoch_t e, __u8 o, utime_t s)
    : Message(MSG_OSD_PING, HEAD_VERSION, COMPAT_VERSION),
      fsid(f), map_epoch(e), peer_as_of_epoch(0), op(o), stamp(s)
  { }
  MOSDPing()
    : Message(MSG_OSD_PING, HEAD_VERSION, COMPAT_VERSION)
  {}
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
    if (header.version >= 2)
      ::decode(stamp, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(fsid, payload);
    ::encode(map_epoch, payload);
    ::encode(peer_as_of_epoch, payload);
    ::encode(op, payload);
    ::encode(peer_stat, payload);
    ::encode(stamp, payload);
  }

  const char *get_type_name() const { return "osd_ping"; }
  void print(ostream& out) const {
    out << "osd_ping(" << get_op_name(op)
	<< " e" << map_epoch
      //<< " as_of " << peer_as_of_epoch
	<< " stamp " << stamp
	<< ")";
  }
};
REGISTER_MESSAGE(MOSDPing, MSG_OSD_PING);
#endif
