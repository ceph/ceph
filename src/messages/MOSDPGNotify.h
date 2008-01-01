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

#ifndef __MOSDPGPEERNOTIFY_H
#define __MOSDPGPEERNOTIFY_H

#include "msg/Message.h"

#include "osd/PG.h"

/*
 * PGNotify - notify primary of my PGs and versions.
 */

class MOSDPGNotify : public Message {
  epoch_t      epoch;
  list<PG::Info> pg_list;   // pgid -> version

 public:
  version_t get_epoch() { return epoch; }
  list<PG::Info>& get_pg_list() { return pg_list; }

  MOSDPGNotify() {}
  MOSDPGNotify(epoch_t e, list<PG::Info>& l) :
    Message(MSG_OSD_PG_NOTIFY) {
    this->epoch = e;
    pg_list.splice(pg_list.begin(),l);
  }
  
  const char *get_type_name() { return "PGnot"; }

  void encode_payload() {
    payload.append((char*)&epoch, sizeof(epoch));
    _encode(pg_list, payload);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    _decode(pg_list, payload, off);
  }
};

#endif
