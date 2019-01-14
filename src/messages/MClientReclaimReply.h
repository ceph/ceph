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


#ifndef CEPH_MCLIENTRECLAIMREPLY_H
#define CEPH_MCLIENTRECLAIMREPLY_H

#include "msg/Message.h"

class MClientReclaimReply: public MessageInstance<MClientReclaimReply> {
public:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  int32_t get_result() const { return result; }
  void set_result(int r) { result = r; }
  epoch_t get_epoch() const { return epoch; }
  void set_epoch(epoch_t e) { epoch = e; }
  const entity_addrvec_t& get_addrs() const { return addrs; }
  void set_addrs(const entity_addrvec_t& _addrs)  { addrs = _addrs; }

  std::string_view get_type_name() const override { return "client_reclaim_reply"; }
  void print(ostream& o) const override {
    o << "client_reclaim_reply(" << result << " e " << epoch << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(result, payload);
    encode(epoch, payload);
    encode(addrs, payload, features);
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(result, p);
    decode(epoch, p);
    decode(addrs, p);
  }

protected:
  friend factory;
  MClientReclaimReply() :
    MessageInstance(CEPH_MSG_CLIENT_RECLAIM_REPLY, HEAD_VERSION, COMPAT_VERSION) {}
  MClientReclaimReply(int r, epoch_t e=0) :
    MessageInstance(CEPH_MSG_CLIENT_RECLAIM_REPLY, HEAD_VERSION, COMPAT_VERSION),
    result(r), epoch(e) {}

private:
  ~MClientReclaimReply() override {}

  int32_t result;
  epoch_t epoch;
  entity_addrvec_t addrs;
};

#endif
