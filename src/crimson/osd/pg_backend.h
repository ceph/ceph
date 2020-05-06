// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <memory>
#include <string>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <boost/container/flat_set.hpp>

#include "crimson/os/futurized_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/acked_peers.h"
#include "crimson/osd/pg.h"
#include "crimson/common/shared_lru.h"
#include "osd/osd_types.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/osdop_params.h"

struct hobject_t;
class MOSDRepOpReply;

namespace ceph::os {
  class Transaction;
}

namespace crimson::osd {
  class ShardServices;
}

class PGBackend
{
protected:
  using CollectionRef = crimson::os::CollectionRef;
  using ec_profile_t = std::map<std::string, std::string>;
  // low-level read errorator
  using ll_read_errorator = crimson::os::FuturizedStore::read_errorator;

public:
  using load_metadata_ertr = crimson::errorator<
    crimson::ct_error::object_corrupted>;
  PGBackend(shard_id_t shard, CollectionRef coll, crimson::os::FuturizedStore* store);
  virtual ~PGBackend() = default;
  static std::unique_ptr<PGBackend> create(pg_t pgid,
					   const pg_shard_t pg_shard,
					   const pg_pool_t& pool,
					   crimson::os::CollectionRef coll,
					   crimson::osd::ShardServices& shard_services,
					   const ec_profile_t& ec_profile);
  using attrs_t =
    std::map<std::string, ceph::bufferptr, std::less<>>;
  using read_errorator = ll_read_errorator::extend<
    crimson::ct_error::object_corrupted>;
  read_errorator::future<ceph::bufferlist> read(
    const object_info_t& oi,
    uint64_t off,
    uint64_t len,
    size_t truncate_size,
    uint32_t truncate_seq,
    uint32_t flags);

  using stat_errorator = crimson::errorator<crimson::ct_error::enoent>;
  stat_errorator::future<> stat(
    const ObjectState& os,
    OSDOp& osd_op);

  seastar::future<> create(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  seastar::future<> remove(
    ObjectState& os,
    ceph::os::Transaction& txn);
  seastar::future<> write(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);
  seastar::future<> writefull(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);
  seastar::future<crimson::osd::acked_peers_t> mutate_object(
    std::set<pg_shard_t> pg_shards,
    crimson::osd::ObjectContextRef &&obc,
    ceph::os::Transaction&& txn,
    const osd_op_params_t& osd_op_p,
    epoch_t min_epoch,
    epoch_t map_epoch,
    std::vector<pg_log_entry_t>&& log_entries);
  seastar::future<std::tuple<std::vector<hobject_t>, hobject_t>> list_objects(
    const hobject_t& start,
    uint64_t limit) const;
  seastar::future<> setxattr(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  using get_attr_errorator = crimson::os::FuturizedStore::get_attr_errorator;
  get_attr_errorator::future<> getxattr(
    const ObjectState& os,
    OSDOp& osd_op) const;
  get_attr_errorator::future<ceph::bufferptr> getxattr(
    const hobject_t& soid,
    std::string_view key) const;
  seastar::future<struct stat> stat(
    CollectionRef c,
    const ghobject_t& oid) const;
  seastar::future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t off,
    uint64_t len);

  // OMAP
  seastar::future<> omap_get_keys(
    const ObjectState& os,
    OSDOp& osd_op) const;
  seastar::future<> omap_get_vals(
    const ObjectState& os,
    OSDOp& osd_op) const;
  seastar::future<> omap_get_vals_by_keys(
    const ObjectState& os,
    OSDOp& osd_op) const;
  seastar::future<> omap_set_vals(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);
  seastar::future<ceph::bufferlist> omap_get_header(
    crimson::os::CollectionRef& c,
    const ghobject_t& oid);

  virtual void got_rep_op_reply(const MOSDRepOpReply&) {}
protected:
  const shard_id_t shard;
  CollectionRef coll;
  crimson::os::FuturizedStore* store;
public:
  struct loaded_object_md_t {
    ObjectState os;
    std::optional<SnapSet> ss;
    using ref = std::unique_ptr<loaded_object_md_t>;
  };
  load_metadata_ertr::future<loaded_object_md_t::ref> load_metadata(
    const hobject_t &oid);

private:
  virtual ll_read_errorator::future<ceph::bufferlist> _read(
    const hobject_t& hoid,
    size_t offset,
    size_t length,
    uint32_t flags) = 0;

  bool maybe_create_new_object(ObjectState& os, ceph::os::Transaction& txn);
  virtual seastar::future<crimson::osd::acked_peers_t>
  _submit_transaction(std::set<pg_shard_t>&& pg_shards,
		      const hobject_t& hoid,
		      ceph::os::Transaction&& txn,
		      const osd_op_params_t& osd_op_p,
		      epoch_t min_epoch, epoch_t max_epoch,
		      std::vector<pg_log_entry_t>&& log_entries) = 0;
  friend class ReplicatedRecoveryBackend;
};
