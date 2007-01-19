// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#ifndef __MOSDPGLOG_H
#define __MOSDPGLOG_H

#include "msg/Message.h"

class MOSDPGLog : public Message {
  epoch_t epoch;
  pg_t    pgid;

public:
  PG::Info info;
  PG::Log log;
  PG::Missing missing;

  epoch_t get_epoch() { return epoch; }
  pg_t get_pgid() { return pgid; }

  MOSDPGLog() {}
  MOSDPGLog(version_t mv, pg_t pgid) :
    Message(MSG_OSD_PG_LOG) {
    this->epoch = mv;
    this->pgid = pgid;
  }

  char *get_type_name() { return "PGlog"; }

  void encode_payload() {
    payload.append((char*)&epoch, sizeof(epoch));
    payload.append((char*)&pgid, sizeof(pgid));
    payload.append((char*)&info, sizeof(info));
    log._encode(payload);
    missing._encode(payload);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    payload.copy(off, sizeof(pgid), (char*)&pgid);
    off += sizeof(pgid);
    payload.copy(off, sizeof(info), (char*)&info);
    off += sizeof(info);
    log._decode(payload, off);
    missing._decode(payload, off);
  }
};

#endif
