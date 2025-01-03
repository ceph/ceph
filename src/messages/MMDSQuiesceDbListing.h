// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM, Red Hat
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

class MMDSQuiesceDbListing final : public MMDSOp {
protected:
  MMDSQuiesceDbListing() : MMDSOp{MSG_MDS_QUIESCE_DB_LISTING} {}
  MMDSQuiesceDbListing(auto&& _pl)
    : MMDSOp{MSG_MDS_QUIESCE_DB_LISTING}
    , peer_listing(std::forward<decltype(_pl)>(_pl))
    {}
  ~MMDSQuiesceDbListing() final {}

public:
  std::string_view get_type_name() const override { return "mds_quiesce_db_listing"; }
  void print(std::ostream& o) const override {
    o << get_type_name();
  }

  void encode_payload(uint64_t features) override { 
    ::encode(peer_listing, payload);
  }

  void decode_payload() override {
    // noop to prevent unnecessary overheads
  }

  void decode_payload_into(QuiesceDbPeerListing &pl) const
  {
    auto p = payload.cbegin();
    ::decode(pl, p);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);

  QuiesceDbPeerListing peer_listing;
};
