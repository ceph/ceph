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


#include "MOSDFastDispatchOp.h"

class MOSDPGPCT final : public MOSDFastDispatchOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  /// epoch at which the message was sent
  epoch_t map_epoch = 0;

  /// start epoch of the interval in which the message was sent
  epoch_t min_epoch = 0;

  /// target pg
  spg_t pgid;

  /**
   * pg_committed_to
   *
   * Propagates PeeringState::pg_committed_to to replicas as with
   * MOSDRepOp, ECSubWrite, MOSDPGPCT.
   */
  eversion_t pg_committed_to;

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDPGPCT()
    : MOSDFastDispatchOp{MSG_OSD_PG_PCT, HEAD_VERSION,
			 COMPAT_VERSION} {}
  MOSDPGPCT(
    spg_t pgid,
    epoch_t epoch,
    epoch_t min_epoch,
    eversion_t pg_committed_to)
    : MOSDFastDispatchOp{MSG_OSD_PG_PCT, HEAD_VERSION,
			 COMPAT_VERSION},
      map_epoch(epoch),
      min_epoch(min_epoch),
      pgid(pgid),
      pg_committed_to(pg_committed_to)
  {}

private:
  ~MOSDPGPCT() final {}

public:
  std::string_view get_type_name() const override { return "PGPCT"; }
  void print(std::ostream& out) const override {
    out << "pg_pct(" << pgid << " epoch " << map_epoch
	<< "/" << min_epoch
	<< " pg_committed_to " << pg_committed_to
	<< ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(map_epoch, payload);
    encode(min_epoch, payload);
    encode(pgid, payload);
    encode(pg_committed_to, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(map_epoch, p);
    decode(min_epoch, p);
    decode(pgid, p);
    decode(pg_committed_to, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
