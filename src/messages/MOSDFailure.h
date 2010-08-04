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


#ifndef CEPH_MOSDFAILURE_H
#define CEPH_MOSDFAILURE_H

#include "messages/PaxosServiceMessage.h"


class MOSDFailure : public PaxosServiceMessage {
 public:
  ceph_fsid_t fsid;
  entity_inst_t target_osd;
  __u8 is_failed;
  epoch_t       epoch;

  MOSDFailure() : PaxosServiceMessage(MSG_OSD_FAILURE, 0) {}
  MOSDFailure(const ceph_fsid_t &fs, entity_inst_t f, epoch_t e) : 
    PaxosServiceMessage(MSG_OSD_FAILURE, e),
    fsid(fs), target_osd(f), is_failed(true), epoch(e) {}
private:
  ~MOSDFailure() {}

public: 
  entity_inst_t get_target() { return target_osd; }
  bool if_osd_failed() { return is_failed; }
  epoch_t get_epoch() { return epoch; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(target_osd, p);
    ::decode(epoch, p);
    if (header.version >=2)
      ::decode(is_failed, p);
    else is_failed = true;
  }
  void encode_payload() {
    header.version = 2;
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(target_osd, payload);
    ::encode(epoch, payload);
    ::encode(is_failed, payload);
  }

  const char *get_type_name() { return "osd_failure"; }
  void print(ostream& out) {
    out << "osd_failure(" << target_osd << " e" << epoch << " v" << version << ")";
  }
};

#endif
