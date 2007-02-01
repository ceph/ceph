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


#ifndef __MOSDPGQUERYREPLY_H
#define __MOSDPGQUERYREPLY_H

#include "msg/Message.h"

class MOSDPGSummary : public Message {
  epoch_t epoch;
  pg_t pgid;

public:
  PG::PGInfo info;
  bufferlist    sumbl;

  epoch_t get_epoch() { return epoch; }

  MOSDPGSummary() {}
  MOSDPGSummary(version_t mv, pg_t pgid, PG::PGSummary &summary) :
    Message(MSG_OSD_PG_SUMMARY) {
    this->epoch = mv;
    this->pgid = pgid;
    summary._encode(sumbl);
  }

  pg_t get_pgid() { return pgid; }
  bufferlist& get_summary_bl() {
    return sumbl;
  }
  
  char *get_type_name() { return "PGsum"; }

  void encode_payload() {
    payload.append((char*)&epoch, sizeof(epoch));
    payload.append((char*)&pgid, sizeof(pgid));
    payload.append((char*)&info, sizeof(info));
    payload.claim_append(sumbl);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    payload.copy(off, sizeof(pgid), (char*)&pgid);
    off += sizeof(pgid);
    payload.copy(off, sizeof(info), (char*)&info);
    off += sizeof(info);

    payload.splice(0, off);
    sumbl.claim(payload);
  }
};

#endif
