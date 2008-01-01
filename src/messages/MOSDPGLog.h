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


#ifndef __MOSDPGLOG_H
#define __MOSDPGLOG_H

#include "msg/Message.h"

class MOSDPGLog : public Message {
  epoch_t epoch;

public:
  PG::Info info;
  PG::Log log;
  PG::Missing missing;

  epoch_t get_epoch() { return epoch; }
  pg_t get_pgid() { return info.pgid; }

  MOSDPGLog() {}
  MOSDPGLog(version_t mv, PG::Info& i) :
    Message(MSG_OSD_PG_LOG),
    epoch(mv), info(i) { }

  const char *get_type_name() { return "PGlog"; }
  void print(ostream& out) {
    out << "pg_log(" << info.pgid << " e" << epoch << ")";
  }

  void encode_payload() {
    payload.append((char*)&epoch, sizeof(epoch));
    payload.append((char*)&info, sizeof(info));
    log._encode(payload);
    missing._encode(payload);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    payload.copy(off, sizeof(info), (char*)&info);
    off += sizeof(info);
    log._decode(payload, off);
    missing._decode(payload, off);
  }
};

#endif
