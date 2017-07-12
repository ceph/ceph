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

class MMonMgrReport : public PaxosServiceMessage {

  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

public:
  // PGMapDigest is in data payload
  health_check_map_t health_checks;
  bufferlist service_map_bl;  // encoded ServiceMap

  MMonMgrReport()
    : PaxosServiceMessage(MSG_MON_MGR_REPORT, 0, HEAD_VERSION, COMPAT_VERSION)
  {}
private:
  ~MMonMgrReport() override {}

public:
  const char *get_type_name() const override { return "monmgrreport"; }

  void print(ostream& out) const override {
    out << get_type_name() << "(" << health_checks.checks.size() << " checks)";
  }

  void encode_payload(uint64_t features) override {
    paxos_encode();
    ::encode(health_checks, payload);
    ::encode(service_map_bl, payload);
  }
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(health_checks, p);
    ::decode(service_map_bl, p);
  }
};

#endif
