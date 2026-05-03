// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <memory>
#include <set>

#include "include/expected.hpp"
#include "osd/scrubber/scrub_backend.h"
#include "osd/scrubber/pg_scrubber.h"
#include "osd/PGBackend.h"
#include "osd/PeeringState.h"
#include "osd/osd_types.h"
#include "test/osd/scrubber_generators.h"
#include "global/global_context.h"

// Type alias for compatibility with existing code
using MockLoggerSinkSet = ScrubGenerator::MockLog;

/**
 * MockScrubBeListener - Mock implementation of ScrubBeListener for testing
 *
 * Provides minimal implementation of the ScrubBeListener interface for
 * testing scrub backend functionality.
 */
class MockScrubBeListener : public ScrubBeListener {
 public:
  MockScrubBeListener()
      : pg_whoami(0, shard_id_t::NO_SHARD),
        primary_shard(0, shard_id_t::NO_SHARD) {}

  MockScrubBeListener(pg_shard_t whoami, pg_shard_t primary)
      : pg_whoami(whoami), primary_shard(primary) {}

  std::ostream& gen_prefix(std::ostream& out) const override {
    return out << "scrub: ";
  }
  CephContext* get_pg_cct() const override { return g_ceph_context; }
  LoggerSinkSet& get_logger() const override { return logger; }
  bool is_primary() const override { return pg_whoami == primary_shard; }
  spg_t get_pgid() const override { return pgid; }
  const OSDMapRef& get_osdmap() const override { return osdmap; }
  void add_to_stats(const object_stat_sum_t&) override {}
  void submit_digest_fixes(const digests_fixes_t&) override {}

  pg_shard_t pg_whoami;       // This shard's identity
  pg_shard_t primary_shard;   // The primary shard's identity
  spg_t pgid;
  OSDMapRef osdmap;
  mutable MockLoggerSinkSet logger;
};

/**
 * MockPgScrubBeListener - Mock implementation of Scrub::PgScrubBeListener for testing
 *
 * Provides EC-aware implementation of the PgScrubBeListener interface for
 * testing scrub backend functionality with erasure-coded pools.
 *
 * Key features:
 * - Delegates EC operations (encode/decode) to real ECBackend for correctness
 * - Implements logical_to_ondisk_size() for proper EC shard size calculation
 * - Delegates get_is_nonprimary_shard() to handle multi-zone EC pools correctly
 */
class MockPgScrubBeListener : public Scrub::PgScrubBeListener {
 public:
  MockPgScrubBeListener(PGBackend* be = nullptr) : backend(be) {}

  const PGPool& get_pgpool() const override { return *pool; }

  pg_shard_t get_primary() const override {
    return pg_shard_t(primary, shard_id_t(primary));
  }

  void force_object_missing(ScrubberPasskey,
                            const std::set<pg_shard_t>& peer,
                            const hobject_t& oid,
                            eversion_t version) override {}

  const pg_info_t& get_pg_info(ScrubberPasskey) const override { return info; }

  uint64_t logical_to_ondisk_size(uint64_t logical_size, shard_id_t shard_id,
                                  bool object_is_legacy_ec) const override {
    return backend->be_get_ondisk_size(logical_size, shard_id_t(shard_id),
                                       object_is_legacy_ec);
  }

  bool ec_can_decode(const shard_id_set& available_shards) const override {
    return backend->ec_can_decode(available_shards);
  }

  shard_id_map<bufferlist> ec_encode_acting_set(const bufferlist& bl) const override {
    return backend->ec_encode_acting_set(bl);
  }

  shard_id_map<bufferlist> ec_decode_acting_set(
      const shard_id_map<bufferlist>& shard_map,
      int chunk_size) const override {
    return backend->ec_decode_acting_set(shard_map, chunk_size);
  }

  bool get_ec_supports_crc_encode_decode() const override {
    return backend->get_ec_supports_crc_encode_decode();
  }

  ECUtil::stripe_info_t get_ec_sinfo() const override {
    return backend->ec_get_sinfo();
  }

  bool is_waiting_for_unreadable_object() const override { return false; }

  bool get_is_nonprimary_shard(const pg_shard_t& shard) const override {
    return backend->get_is_nonprimary_shard(shard.shard);
  }

  bool get_is_hinfo_required() const override {
    return backend->get_is_hinfo_required();
  }

  bool get_is_ec_optimized() const override {
    return backend->get_is_ec_optimized();
  }

  std::shared_ptr<PGPool> pool;
  int primary;
  pg_info_t info;
  PGBackend* backend;
};

/**
 * MockSnapMapReader - Mock implementation of Scrub::SnapMapReaderI for testing
 *
 * Provides minimal implementation that returns empty snapshot sets,
 * suitable for testing scrub functionality without snapshot complexity.
 */
class MockSnapMapReader : public Scrub::SnapMapReaderI {
 public:
  tl::expected<std::set<snapid_t>, result_t> get_snaps(
      const hobject_t&) const override {
    return std::set<snapid_t>{};
  }
  tl::expected<std::set<snapid_t>, result_t> get_snaps_check_consistency(
      const hobject_t&) const override {
    return std::set<snapid_t>{};
  }
};

// Test helper to access ScrubBackend internals
class TestScrubBackend : public ScrubBackend {
public:
  TestScrubBackend(ScrubBeListener& scrubber,
                   Scrub::PgScrubBeListener& pg,
                   pg_shard_t i_am,
                   bool repair,
                   scrub_level_t shallow_or_deep,
                   const std::set<pg_shard_t>& acting)
      : ScrubBackend(scrubber, pg, i_am, repair, shallow_or_deep, acting)
  {}

  void insert_faked_smap(pg_shard_t shard, const ScrubMap& smap) {
    ceph_assert(this_chunk.has_value());
    this_chunk->received_maps[shard] = smap;
  }

  // Factory method to create MockScrubBeListener with proper initialization
  static std::unique_ptr<MockScrubBeListener> create_scrub_listener(
      const spg_t& pgid,
      const OSDMapRef& osdmap)
  {
    auto listener = std::make_unique<MockScrubBeListener>();
    listener->pgid = pgid;
    listener->osdmap = osdmap;
    return listener;
  }

  // Factory method to create MockSnapMapReader
  static std::unique_ptr<MockSnapMapReader> create_snap_reader()
  {
    return std::make_unique<MockSnapMapReader>();
  }
};

// Made with Bob
