// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MOSDMARKMEDOWN_H
#define CEPH_MOSDMARKMEDOWN_H

#include "messages/PaxosServiceMessage.h"

class MOSDMarkMeDown : public PaxosServiceMessage {

  static const int COMPAT_VERSION = 1;
  static const int HEAD_VERSION = 2;

 public:
  uuid_d fsid;
  entity_inst_t target_osd;
  epoch_t epoch;
  bool request_ack;          // ack requested

  MOSDMarkMeDown()
    : PaxosServiceMessage(MSG_OSD_MARK_ME_DOWN, 0,
			  HEAD_VERSION, COMPAT_VERSION) { }
  MOSDMarkMeDown(const uuid_d &fs, const entity_inst_t& f,
		 epoch_t e, bool request_ack)
    : PaxosServiceMessage(MSG_OSD_MARK_ME_DOWN, e,
			  HEAD_VERSION, COMPAT_VERSION),
      fsid(fs), target_osd(f), epoch(e), request_ack(request_ack) {}
 private:
  ~MOSDMarkMeDown() {}

public: 
  entity_inst_t get_target() { return target_osd; }
  epoch_t get_epoch() { return epoch; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(target_osd, p);
    ::decode(epoch, p);
    ::decode(request_ack, p);
    if (header.version < 2)
      request_ack = true;    // assume true for older clients
  }
  void encode_payload(uint64_t features) {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(target_osd, payload);
    ::encode(epoch, payload);
    ::encode(request_ack, payload);
  }

  const char *get_type_name() const { return "MOSDMarkMeDown"; }
  void print(ostream& out) const {
    out << "MOSDMarkMeDown("
	<< "request_ack=" << request_ack
	<< ", target_osd=" << target_osd
	<< ", fsid=" << fsid
	<< ")";
  }
};

#endif
