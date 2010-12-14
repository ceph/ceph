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


#ifndef CEPH_MWATCHNOTIFY_H
#define CEPH_MWATCHNOTIFY_H

#include "msg/Message.h"


class MWatchNotify : public Message {
 public:
  uint64_t cookie;
  uint64_t ver;
  uint64_t notify_id;
  uint8_t opcode;

  MWatchNotify() : Message(CEPH_MSG_WATCH_NOTIFY) { }
  MWatchNotify(uint64_t c, uint64_t v, uint64_t i, uint8_t o) : Message(CEPH_MSG_WATCH_NOTIFY),
					cookie(c), ver(v), notify_id(i), opcode(o) { }
private:
  ~MWatchNotify() {}

public:
  void decode_payload() {
    uint8_t msg_ver;
    bufferlist::iterator p = payload.begin();
    ::decode(msg_ver, p);
    ::decode(opcode, p);
    ::decode(cookie, p);
    ::decode(ver, p);
    ::decode(notify_id, p);
  }
  void encode_payload() {
    uint8_t msg_ver = 0;
    ::encode(msg_ver, payload);
    ::encode(opcode, payload);
    ::encode(cookie, payload);
    ::encode(ver, payload);
    ::encode(notify_id, payload);
  }

  const char *get_type_name() { return "watch-notify"; }
  void print(ostream& out) {
    out << "watch-notify(c=" << cookie << " v=" << ver << " i=" << notify_id << " opcode=" << (int)opcode << ")";
  }
};

#endif
