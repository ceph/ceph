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

#ifndef CEPH_MREMOVESNAPS_H
#define CEPH_MREMOVESNAPS_H

#include "messages/PaxosServiceMessage.h"

class MRemoveSnaps : public PaxosServiceMessage {
public:
  std::map<int32_t, std::vector<snapid_t>> snaps;

protected:
  MRemoveSnaps() :
    PaxosServiceMessage{MSG_REMOVE_SNAPS, 0} { }
  MRemoveSnaps(std::map<int, std::vector<snapid_t>>& s) : 
    PaxosServiceMessage{MSG_REMOVE_SNAPS, 0} {
    snaps.swap(s);
  }
  ~MRemoveSnaps() override {}

public:
  std::string_view get_type_name() const override { return "remove_snaps"; }
  void print(std::ostream& out) const override {
    out << "remove_snaps(" << snaps << " v" << version << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(snaps, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(snaps, p);
    ceph_assert(p.end());
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
