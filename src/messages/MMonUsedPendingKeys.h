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

#pragma once

#include "messages/PaxosServiceMessage.h"

class MMonUsedPendingKeys final : public PaxosServiceMessage {
public:
  std::map<EntityName,CryptoKey> used_pending_keys;
  
  MMonUsedPendingKeys() : PaxosServiceMessage{MSG_MON_USED_PENDING_KEYS, 0}
  {}
private:
  ~MMonUsedPendingKeys() final {}

public:
  std::string_view get_type_name() const override { return "used_pending_keys"; }
  void print(std::ostream& out) const override {
    out << "used_pending_keys(" << used_pending_keys.size() << " keys)";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(used_pending_keys, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(used_pending_keys, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
