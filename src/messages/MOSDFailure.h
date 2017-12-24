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

  static const int HEAD_VERSION = 3;
  static const int COMPAT_VERSION = 3;

 public:
  enum {
    FLAG_ALIVE = 0,      // use this on its own to mark as "I'm still alive"
    FLAG_FAILED = 1,     // if set, failure; if not, recovery
    FLAG_IMMEDIATE = 2,  // known failure, not a timeout
  };
  
  uuid_d fsid;
  entity_inst_t target_osd;
  __u8 flags = 0;
  epoch_t       epoch = 0;
  int32_t failed_for = 0;  // known to be failed since at least this long

  MOSDFailure() : PaxosServiceMessage(MSG_OSD_FAILURE, 0, HEAD_VERSION) { }
  MOSDFailure(const uuid_d &fs, const entity_inst_t& f, int duration, epoch_t e)
    : PaxosServiceMessage(MSG_OSD_FAILURE, e, HEAD_VERSION, COMPAT_VERSION),
      fsid(fs), target_osd(f),
      flags(FLAG_FAILED),
      epoch(e), failed_for(duration) { }
  MOSDFailure(const uuid_d &fs, const entity_inst_t& f, int duration, 
              epoch_t e, __u8 extra_flags)
    : PaxosServiceMessage(MSG_OSD_FAILURE, e, HEAD_VERSION, COMPAT_VERSION),
      fsid(fs), target_osd(f),
      flags(extra_flags),
      epoch(e), failed_for(duration) { }
private:
  ~MOSDFailure() override {}

public: 
  entity_inst_t get_target() { return target_osd; }
  bool if_osd_failed() const { 
    return flags & FLAG_FAILED; 
  }
  bool is_immediate() const { 
    return flags & FLAG_IMMEDIATE; 
  }
  epoch_t get_epoch() const { return epoch; }

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    decode(fsid, p);
    decode(target_osd, p);
    decode(epoch, p);
    decode(flags, p);
    decode(failed_for, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(target_osd, payload, features);
    encode(epoch, payload);
    encode(flags, payload);
    encode(failed_for, payload);
  }

  const char *get_type_name() const override { return "osd_failure"; }
  void print(ostream& out) const override {
    out << "osd_failure("
	<< (if_osd_failed() ? "failed " : "recovered ")
	<< (is_immediate() ? "immediate " : "timeout ")
	<< target_osd << " for " << failed_for << "sec e" << epoch
	<< " v" << version << ")";
  }
};

#endif
