// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "common/dout.h"
#include "ECUtil.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "os/Transaction.h"
#include "PGTransaction.h"

namespace ECTransaction {
class WritePlanObj {
 public:
  const hobject_t hoid;
  std::optional<ECUtil::shard_extent_set_t> to_read;
  ECUtil::shard_extent_set_t will_write;
  const ECUtil::HashInfoRef hinfo;
  const ECUtil::HashInfoRef shinfo;
  const uint64_t orig_size;
  const uint64_t projected_size;
  bool invalidates_cache;
  bool do_parity_delta_write = false;

  WritePlanObj(
      const hobject_t &hoid,
      const PGTransaction::ObjectOperation &op,
      const ECUtil::stripe_info_t &sinfo,
      const shard_id_set readable_shards,
      const shard_id_set writable_shards,
      const bool object_in_cache,
      uint64_t orig_size,
      const std::optional<object_info_t> &oi,
      const std::optional<object_info_t> &soi,
      const ECUtil::HashInfoRef &&hinfo,
      const ECUtil::HashInfoRef &&shinfo,
      const unsigned pdw_write_mode);

  void print(std::ostream &os) const {
    os << "{hoid: " << hoid
       << " to_read: " << to_read
       << " will_write: " << will_write
       << " hinfo: " << hinfo
       << " shinfo: " << shinfo
       << " orig_size: " << orig_size
       << " projected_size: " << projected_size
       << " invalidates_cache: " << invalidates_cache
       << " do_pdw: " << do_parity_delta_write
       << "}";
  }
};

struct WritePlan {
  bool want_read;
  std::list<WritePlanObj> plans;

  void print(std::ostream &os) const {
    os << " plans: [";
    bool first = true;
    for (auto && p : plans) {
      if (first) {
        first = false;
      } else {
        os << ", ";
      }
      os << "{" << p << "}";
    }
   os << "]";
  }
};

class Generate {
  PGTransaction &t;
  const ErasureCodeInterfaceRef &ec_impl;
  const pg_t &pgid;
  const ECUtil::stripe_info_t &sinfo;
  shard_id_map<ceph::os::Transaction> &transactions;
  DoutPrefixProvider *dpp;
  const OSDMapRef &osdmap;
  pg_log_entry_t *entry;
  const hobject_t &oid;
  PGTransaction::ObjectOperation& op;
  ObjectContextRef obc;
  std::map<std::string, std::optional<bufferlist>> xattr_rollback;
  const WritePlanObj &plan;
  std::optional<ECUtil::shard_extent_map_t> read_sem;
  ECUtil::shard_extent_map_t to_write;
  std::vector<std::pair<uint64_t, uint64_t>> rollback_extents;
  std::vector<shard_id_set> rollback_shards;
  uint32_t fadvise_flags = 0;
  bool written_shards_final{false};

  void all_shards_written();
  void shard_written(const shard_id_t shard);
  void shards_written(const shard_id_set &shards);
  void delete_first();
  void zero_truncate_to_delete();
  void process_init();
  void encode_and_write();
  void truncate();
  void overlay_writes();
  void appends_and_clone_ranges();
  void written_and_present_shards();
  void attr_updates();
  void handle_deletes();

 public:
  Generate(PGTransaction &t,
    ErasureCodeInterfaceRef &ec_impl, pg_t &pgid,
    const ECUtil::stripe_info_t &sinfo,
    const std::map<hobject_t, ECUtil::shard_extent_map_t> &partial_extents,
    std::map<hobject_t, ECUtil::shard_extent_map_t> *written_map,
    shard_id_map<ceph::os::Transaction> &transactions,
    const OSDMapRef &osdmap,
    const hobject_t &oid, PGTransaction::ObjectOperation &op,
    WritePlanObj &plan,
    DoutPrefixProvider *dpp,
    pg_log_entry_t *entry);
};

void generate_transactions(
    PGTransaction *_t,
    WritePlan &plan,
    ceph::ErasureCodeInterfaceRef &ec_impl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    const std::map<hobject_t, ECUtil::shard_extent_map_t> &partial_extents,
    std::vector<pg_log_entry_t> &entries,
    std::map<hobject_t, ECUtil::shard_extent_map_t> *written_map,
    shard_id_map<ceph::os::Transaction> *transactions,
    std::set<hobject_t> *temp_added,
    std::set<hobject_t> *temp_removed,
    DoutPrefixProvider *dpp,
    const OSDMapRef &osdmap
  );
}
