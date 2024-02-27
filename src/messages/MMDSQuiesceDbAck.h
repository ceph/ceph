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

#include "messages/MMDSOp.h"
#include "mds/QuiesceDbEncoding.h"

class MMDSQuiesceDbAck final : public MMDSOp {
protected:
  MMDSQuiesceDbAck() : MMDSOp{MSG_MDS_QUIESCE_DB_ACK} {}
  ~MMDSQuiesceDbAck() final {}

public:
  std::string_view get_type_name() const override { return "mds_quiesce_db_ack"; }
  void print(std::ostream& o) const override {

  }

  void encode_payload(uint64_t features) override
  {
    // noop to prevent unnecessary overheads
  }

  void encode_payload_from(mds_gid_t const&gid, QuiesceMap const&diff_map)
  {
    using ceph::encode;

    ceph_assert(gid != MDS_GID_NONE);

    ENCODE_START(1, 1, payload);
    encode(gid, payload);
    encode(diff_map, payload);
    ENCODE_FINISH(payload);
  }

  void decode_payload() override {
    // noop to prevent unnecessary overheads
  }

  void decode_payload_into(mds_gid_t &gid, QuiesceMap &diff_map) const
  {
    using ceph::decode;
    auto p = payload.cbegin();
    DECODE_START(1, p);
    decode(gid, p);
    decode(diff_map, p);
    DECODE_FINISH(p);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};
