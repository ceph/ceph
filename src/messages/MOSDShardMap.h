// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <map>

#include "msg/Message.h"
#include "osd/osd_types.h"

/**
 * MOSDShardMap
 *
 * Sent by a Crimson OSD to a client to advertise the current mapping of
 * spg_t to the reactor core that owns each PG.  Clients cache this table
 * per-OSD (keyed by bind_epoch) and include the looked-up core id as
 * shard_hint in subsequent MOSDOp messages, allowing the OSD to skip
 * the get_pg_mapping pipeline stage on the hot path.
 *
 * The table is invalidated whenever bind_epoch advances; the OSD detects
 * this via OSDConnectionPriv::last_shard_map_sent and re-sends automatically.
 *
 * Classic (non-Crimson) OSDs never send this message and clients should
 * treat its absence as meaning shard_hint is always NULL_CORE (0xffffffff).
 */
class MOSDShardMap final : public Message {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  /// OSD instance this table belongs to.
  int whoami = -1;

  /// OSD map epoch at which this table was generated.  Clients must
  /// discard cached entries whose bind_epoch is older than this value.
  epoch_t bind_epoch = 0;

  /// Maps each active spg_t to the reactor core id that owns it.
  /// core_id_t is seastar::shard_id (uint32_t); the NULL_CORE sentinel
  /// (std::numeric_limits<uint32_t>::max()) means "not yet placed".
  std::map<spg_t, uint32_t> pg_to_core;

  MOSDShardMap()
    : Message{MSG_OSD_SHARD_MAP, HEAD_VERSION, COMPAT_VERSION} {}

  MOSDShardMap(int whoami_, epoch_t bind_epoch_,
               std::map<spg_t, uint32_t> pg_to_core_)
    : Message{MSG_OSD_SHARD_MAP, HEAD_VERSION, COMPAT_VERSION},
      whoami(whoami_),
      bind_epoch(bind_epoch_),
      pg_to_core(std::move(pg_to_core_)) {}

private:
  ~MOSDShardMap() final = default;

public:
  std::string_view get_type_name() const override { return "osd_shard_map"; }

  void print(std::ostream& out) const override {
    out << "osd_shard_map(osd." << whoami
        << " e" << bind_epoch
        << " pgs=" << pg_to_core.size() << ")";
  }

  void encode_payload(uint64_t /*features*/) override {
    using ceph::encode;
    encode(whoami, payload);
    encode(bind_epoch, payload);
    encode(pg_to_core, payload);
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = std::cbegin(payload);
    decode(whoami, p);
    decode(bind_epoch, p);
    decode(pg_to_core, p);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
