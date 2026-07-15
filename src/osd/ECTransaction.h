// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "common/ceph_releases.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "os/Transaction.h"
#include "OSDMap.h"
#include "PGTransaction.h"
#include "osd/ECOmapJournal.h"

class PGLog;

namespace ECTransaction {
class WritePlanObj {
 public:
  const hobject_t hoid;
  std::optional<ECUtil::shard_extent_set_t> to_read;
  ECUtil::shard_extent_set_t will_write;
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
      unsigned pdw_write_mode);

  void print(std::ostream &os) const {
    os << "{hoid: " << hoid
       << " to_read: " << to_read
       << " will_write: " << will_write
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

/**
 * Decode and accumulate omap updates from encoded operation list.
 * Handles Insert, Remove, and RemoveRange operations.
 */
void accumulate_omap_updates(
  bool clear_omap,
  const std::optional<ceph::buffer::list>& header,
  const std::vector<std::pair<OmapUpdateType, ceph::buffer::list>>& updates,
  std::optional<ceph::buffer::list>& out_header,
  std::map<std::string, std::optional<ceph::buffer::list>>& key_updates,
  std::list<std::pair<std::string, std::optional<std::string>>>& removed_ranges);

/**
 * Apply accumulated omap updates to primary-capable shard transactions.
 */
void apply_omap_to_transactions(
  shard_id_map<ceph::os::Transaction>& transactions,
  const pg_t& pgid,
  const hobject_t& target_oid,
  const ECUtil::stripe_info_t& sinfo,
  bool clear_omap,
  const std::optional<ceph::buffer::list>& header,
  const std::map<std::string, std::optional<ceph::buffer::list>>& key_updates,
  const std::list<std::pair<std::string, std::optional<std::string>>>& removed_ranges,
  const DoutPrefixProvider* dpp);


/**
 * OmapCloneVisitor - Visitor to extract and apply omap updates to clone transactions
 *
 * This visitor implements ObjectModDesc::Visitor to traverse PG log entries and
 * accumulate omap updates (key-value pairs, range removals, header changes) that
 * need to be applied to a cloned object. It ensures that incomplete omap updates
 * from the PG log are properly transferred to the clone.
 */
class OmapCloneVisitor : public ObjectModDesc::Visitor {
private:
  shard_id_map<ceph::os::Transaction> &transactions;
  const pg_t &pgid;
  const hobject_t &source_oid;
  const hobject_t &dest_oid;
  const ECUtil::stripe_info_t &sinfo;
  ECOmapJournal &ec_omap_journal;
  const DoutPrefixProvider *dpp;
  
  // Accumulated omap state
  bool has_clear_omap = false;
  std::optional<ceph::buffer::list> omap_header;
  std::map<std::string, std::optional<ceph::buffer::list>> omap_updates;
  std::list<std::pair<std::string, std::optional<std::string>>> removed_ranges;

public:
  OmapCloneVisitor(
    shard_id_map<ceph::os::Transaction> &txns,
    const pg_t &pg,
    const hobject_t &src,
    const hobject_t &dst,
    const ECUtil::stripe_info_t &stripe_info,
    ECOmapJournal &journal,
    const DoutPrefixProvider *dpp)
    : transactions(txns), pgid(pg), source_oid(src), dest_oid(dst),
      sinfo(stripe_info), ec_omap_journal(journal), dpp(dpp) {}

  /**
   * Called by ObjectModDesc::visit() when an ec_omap modification is encountered
   * Accumulates omap updates from the PG log entry
   */
  void ec_omap(
    bool clear_omap,
    std::optional<ceph::buffer::list> header,
    std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> &updates) override;
  
  /**
   * Apply accumulated omap updates to the clone transaction
   * This should be called after visiting all relevant log entries
   */
  void apply_to_clone();
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
  ECOmapJournal &ec_omap_journal;
  const PGLog &pg_log;

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
  void written_shards();
  void attr_updates();
  std::vector<const pg_log_entry_t*> get_incomplete_ec_omap_log_entries(
    const hobject_t &hoid,
    eversion_t can_rollback_to);
  /**
   * Apply omap updates directly to transactions without journaling.
   * Should only be called when entry is null (temporary/non-journaled operations).
   * For journaled operations, use entry->mod_desc.ec_omap() instead.
   */
  void apply_omap_updates_without_journal();

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
    pg_log_entry_t *entry,
    bool &first_write_in_interval,
    ECOmapJournal &ec_omap_journal,
    const PGLog &pg_log);
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
    const OSDMapRef &osdmap,
    bool &first_write_in_interval,
    ECOmapJournal &ec_omap_journal,
    const PGLog &pg_log
  );
}
