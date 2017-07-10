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

// health_status_t
static inline void encode(health_status_t hs, bufferlist& bl) {
  uint8_t v = hs;
  ::encode(v, bl);
}
static inline void decode(health_status_t& hs, bufferlist::iterator& p) {
  uint8_t v;
  ::decode(v, p);
  hs = health_status_t(v);
}

class MMonMgrReport : public PaxosServiceMessage {

  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

public:
  // PGMapDigest is in data payload
  list<pair<health_status_t,std::string>> health_summary, health_detail;
  bufferlist service_map_bl;  // encoded ServiceMap

  MMonMgrReport()
    : PaxosServiceMessage(MSG_MON_MGR_REPORT, 0, HEAD_VERSION, COMPAT_VERSION)
  {}
private:
  ~MMonMgrReport() override {}

public:
  const char *get_type_name() const override { return "monmgrreport"; }

  void print(ostream& out) const override {
    out << get_type_name();
  }

  void encode_payload(uint64_t features) override {
    paxos_encode();
    ::encode(health_summary, payload);
    ::encode(health_detail, payload);
    ::encode(service_map_bl, payload);
  }
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(health_summary, p);
    ::decode(health_detail, p);
    ::decode(service_map_bl, p);
  }
};

#endif
