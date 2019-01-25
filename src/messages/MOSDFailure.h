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


class MOSDFailure : public MessageInstance<MOSDFailure, PaxosServiceMessage> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 4;

 public:
  enum {
    FLAG_ALIVE = 0,      // use this on its own to mark as "I'm still alive"
    FLAG_FAILED = 1,     // if set, failure; if not, recovery
    FLAG_IMMEDIATE = 2,  // known failure, not a timeout
  };
  
  uuid_d fsid;
  int32_t target_osd;
  entity_addrvec_t target_addrs;
  __u8 flags = 0;
  epoch_t epoch = 0;
  int32_t failed_for = 0;  // known to be failed since at least this long

  MOSDFailure() : MessageInstance(MSG_OSD_FAILURE, 0, HEAD_VERSION) { }
  MOSDFailure(const uuid_d &fs, int osd, const entity_addrvec_t& av,
	      int duration, epoch_t e)
    : MessageInstance(MSG_OSD_FAILURE, e, HEAD_VERSION, COMPAT_VERSION),
      fsid(fs),
      target_osd(osd),
      target_addrs(av),
      flags(FLAG_FAILED),
      epoch(e), failed_for(duration) { }
  MOSDFailure(const uuid_d &fs, int osd, const entity_addrvec_t& av,
	      int duration,
              epoch_t e, __u8 extra_flags)
    : MessageInstance(MSG_OSD_FAILURE, e, HEAD_VERSION, COMPAT_VERSION),
      fsid(fs),
      target_osd(osd),
      target_addrs(av),
      flags(extra_flags),
      epoch(e), failed_for(duration) { }
private:
  ~MOSDFailure() override {}

public:
  int get_target_osd() { return target_osd; }
  const entity_addrvec_t& get_target_addrs() { return target_addrs; }
  bool if_osd_failed() const { 
    return flags & FLAG_FAILED; 
  }
  bool is_immediate() const { 
    return flags & FLAG_IMMEDIATE; 
  }
  epoch_t get_epoch() const { return epoch; }

  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    if (header.version < 4) {
      entity_inst_t i;
      decode(i, p);
      target_osd = i.name.num();
      target_addrs.v.push_back(i.addr);
    } else {
      decode(target_osd, p);
      decode(target_addrs, p);
    }
    decode(epoch, p);
    decode(flags, p);
    decode(failed_for, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      header.version = 3;
      header.compat_version = 3;
      encode(fsid, payload);
      encode(entity_inst_t(entity_name_t::OSD(target_osd),
			   target_addrs.legacy_addr()), payload, features);
      encode(epoch, payload);
      encode(flags, payload);
      encode(failed_for, payload);
      return;
    }
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    encode(fsid, payload);
    encode(target_osd, payload, features);
    encode(target_addrs, payload, features);
    encode(epoch, payload);
    encode(flags, payload);
    encode(failed_for, payload);
  }

  std::string_view get_type_name() const override { return "osd_failure"; }
  void print(ostream& out) const override {
    out << "osd_failure("
	<< (if_osd_failed() ? "failed " : "recovered ")
	<< (is_immediate() ? "immediate " : "timeout ")
	<< "osd." << target_osd << " " << target_addrs
	<< " for " << failed_for << "sec e" << epoch
	<< " v" << version << ")";
  }
};

#endif
