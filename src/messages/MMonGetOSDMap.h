// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MMONGETOSDMAP_H
#define CEPH_MMONGETOSDMAP_H

#include "msg/Message.h"

#include "include/types.h"

class MMonGetOSDMap : public PaxosServiceMessage {
  epoch_t full_first, full_last;
  epoch_t inc_first, inc_last;

public:
  MMonGetOSDMap()
    : PaxosServiceMessage(CEPH_MSG_MON_GET_OSDMAP, 0),
      full_first(0),
      full_last(0),
      inc_first(0),
      inc_last(0) { }
private:
  ~MMonGetOSDMap() {}

public:
  void request_full(epoch_t first, epoch_t last) {
    assert(last >= first);
    full_first = first;
    full_last = last;
  }
  void request_inc(epoch_t first, epoch_t last) {
    assert(last >= first);
    inc_first = first;
    inc_last = last;
  }
  epoch_t get_full_first() const {
    return full_first;
  }
  epoch_t get_full_last() const {
    return full_last;
  }
  epoch_t get_inc_first() const {
    return inc_first;
  }
  epoch_t get_inc_last() const {
    return inc_last;
  }

  const char *get_type_name() const { return "mon_get_osdmap"; }
  void print(ostream& out) const {
    out << "mon_get_osdmap(";
    if (full_first && full_last)
      out << "full " << full_first << "-" << full_last;
    if (inc_first && inc_last)
      out << " inc" << inc_first << "-" << inc_last;
    out << ")";
  }

  void encode_payload(uint64_t features) {
    paxos_encode();
    ::encode(full_first, payload);
    ::encode(full_last, payload);
    ::encode(inc_first, payload);
    ::encode(inc_last, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(full_first, p);
    ::decode(full_last, p);
    ::decode(inc_first, p);
    ::decode(inc_last, p);
  }
};
REGISTER_MESSAGE(MMonGetOSDMap, CEPH_MSG_MON_GET_OSDMAP);
#endif
