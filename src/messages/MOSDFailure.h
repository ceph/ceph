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


#ifndef __MOSDFAILURE_H
#define __MOSDFAILURE_H

#include "messages/PaxosServiceMessage.h"


class MOSDFailure : public PaxosServiceMessage {
 public:
  ceph_fsid_t fsid;
  entity_inst_t failed;
  epoch_t       epoch;

  MOSDFailure() : PaxosServiceMessage(MSG_OSD_FAILURE, 0) {}
  MOSDFailure(const ceph_fsid_t &fs, entity_inst_t f, epoch_t e) : 
    PaxosServiceMessage(MSG_OSD_FAILURE, e),
    fsid(fs), failed(f), epoch(e) {}
private:
  ~MOSDFailure() {}

public: 
  entity_inst_t get_failed() { return failed; }
  epoch_t get_epoch() { return epoch; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(failed, p);
    ::decode(epoch, p);
  }
  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(failed, payload);
    ::encode(epoch, payload);
  }

  const char *get_type_name() { return "osd_failure"; }
  void print(ostream& out) {
    out << "osd_failure(" << failed << " e" << epoch << " v" << version << ")";
  }
};

#endif
