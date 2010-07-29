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

#ifndef CEPH_MOSDPGPEERNOTIFY_H
#define CEPH_MOSDPGPEERNOTIFY_H

#include "msg/Message.h"

#include "osd/PG.h"

/*
 * PGNotify - notify primary of my PGs and versions.
 */

class MOSDPGNotify : public Message {
  epoch_t      epoch;
  vector<PG::Info> pg_list;   // pgid -> version

 public:
  version_t get_epoch() { return epoch; }
  vector<PG::Info>& get_pg_list() { return pg_list; }

  MOSDPGNotify() {}
  MOSDPGNotify(epoch_t e, vector<PG::Info>& l) :
    Message(MSG_OSD_PG_NOTIFY) {
    this->epoch = e;
    pg_list.swap(l);
  }
private:
  ~MOSDPGNotify() {}

public:  
  const char *get_type_name() { return "PGnot"; }

  void encode_payload() {
    ::encode(epoch, payload);
    ::encode(pg_list, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    ::decode(pg_list, p);
  }
};

#endif
