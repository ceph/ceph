// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Greg Farnum/Red Hat <gfarnum@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MMONMGRREPORT_H
#define CEPH_MMONMGRREPORT_H

#include "messages/PaxosServiceMessage.h"
#include "include/types.h"
#include "include/health.h"
#include "mon/health_check.h"
#include "mon/PGMap.h"

class MMonMgrReport final : public PaxosServiceMessage {
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 1;

public:
  // PGMapDigest is in data payload
  health_check_map_t health_checks;
  ceph::buffer::list service_map_bl;  // encoded ServiceMap
  std::map<std::string,ProgressEvent> progress_events;
  uint64_t gid = 0;

  MMonMgrReport()
    : PaxosServiceMessage{MSG_MON_MGR_REPORT, 0, HEAD_VERSION, COMPAT_VERSION}
  {}
private:
  ~MMonMgrReport() final {}

public:
  std::string_view get_type_name() const override { return "monmgrreport"; }

  void print(std::ostream& out) const override {
    out << get_type_name() << "(gid " << gid
	<< ", " << health_checks.checks.size() << " checks, "
	<< progress_events.size() << " progress events)";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(health_checks, payload);
    encode(service_map_bl, payload);
    encode(progress_events, payload);
    encode(gid, payload);

    if (!HAVE_FEATURE(features, SERVER_NAUTILUS) ||
	!HAVE_FEATURE(features, SERVER_MIMIC)) {
      // PGMapDigest had a backwards-incompatible change between
      // luminous and mimic, and conditionally encodes based on
      // provided features, so reencode the one in our data payload.
      // The mgr isn't able to do this at the time the encoded
      // PGMapDigest is constructed because we don't know which mon we
      // will target.  Note that this only triggers if the user
      // upgrades ceph-mgr before ceph-mon (tsk tsk).
      PGMapDigest digest;
      auto p = data.cbegin();
      decode(digest, p);
      ceph::buffer::list bl;
      encode(digest, bl, features);
      set_data(bl);
    }
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(health_checks, p);
    decode(service_map_bl, p);
    if (header.version >= 2) {
      decode(progress_events, p);
    }
    if (header.version >= 3) {
      decode(gid, p);
    }
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
