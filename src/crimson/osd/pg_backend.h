// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <memory>
#include <string>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <boost/container/flat_set.hpp>

#include "include/rados.h"

#include "crimson/os/futurized_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/acked_peers.h"
#include "crimson/common/shared_lru.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "os/Transaction.h"
#include "osd/osd_types.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/osdop_params.h"

struct hobject_t;

namespace ceph::os {
  class Transaction;
}

namespace crimson::osd {
  class ShardServices;
  class PG;
}

class PGBackend
{
protected:
  using CollectionRef = crimson::os::CollectionRef;
  using ec_profile_t = std::map<std::string, std::string>;
  // low-level read errorator
  using ll_read_errorator = crimson::os::FuturizedStore::read_errorator;
  using ll_read_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      ll_read_errorator>;

public:
  using load_metadata_ertr = crimson::errorator<
    crimson::ct_error::object_corrupted>;
  using load_metadata_iertr =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      load_metadata_ertr>;
  using interruptor =
    ::crimson::interruptible::interruptor<
      ::crimson::osd::IOInterruptCondition>;
  template <typename T = void>
  using interruptible_future =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, T>;
  using rep_op_fut_t =
    std::tuple<interruptible_future<>,
	       interruptible_future<crimson::osd::acked_peers_t>>;
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
  using read_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      read_errorator>;
  read_ierrorator::future<> read(
    const ObjectState& os,
    OSDOp& osd_op);
  read_ierrorator::future<> sparse_read(
    const ObjectState& os,
    OSDOp& osd_op);
  using checksum_errorator = ll_read_errorator::extend<
    crimson::ct_error::object_corrupted,
    crimson::ct_error::invarg>;
  using checksum_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      checksum_errorator>;
  checksum_ierrorator::future<> checksum(
    const ObjectState& os,
    OSDOp& osd_op);
  using cmp_ext_errorator = ll_read_errorator::extend<
    crimson::ct_error::invarg>;
  using cmp_ext_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      cmp_ext_errorator>;
  cmp_ext_ierrorator::future<> cmp_ext(
    const ObjectState& os,
    OSDOp& osd_op);
  using stat_errorator = crimson::errorator<crimson::ct_error::enoent>;
  using stat_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      stat_errorator>;
  stat_ierrorator::future<> stat(
    const ObjectState& os,
    OSDOp& osd_op);

  // TODO: switch the entire write family to errorator.
  using write_ertr = crimson::errorator<
    crimson::ct_error::file_too_large>;
  using write_iertr =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      write_ertr>;
  interruptible_future<> create(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  interruptible_future<> remove(
    ObjectState& os,
    ceph::os::Transaction& txn);
  interruptible_future<> write(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);
  interruptible_future<> write_same(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);
  interruptible_future<> writefull(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);
  using append_errorator = crimson::errorator<
    crimson::ct_error::invarg>;
  using append_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      append_errorator>;
  append_ierrorator::future<> append(
    ObjectState& os,
    OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);
  write_iertr::future<> truncate(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);
  write_iertr::future<> zero(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);
  rep_op_fut_t mutate_object(
    std::set<pg_shard_t> pg_shards,
    crimson::osd::ObjectContextRef &&obc,
    ceph::os::Transaction&& txn,
    osd_op_params_t&& osd_op_p,
    epoch_t min_epoch,
    epoch_t map_epoch,
    std::vector<pg_log_entry_t>&& log_entries);
  interruptible_future<std::tuple<std::vector<hobject_t>, hobject_t>> list_objects(
    const hobject_t& start,
    uint64_t limit) const;
  interruptible_future<> setxattr(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  using get_attr_errorator = crimson::os::FuturizedStore::get_attr_errorator;
  using get_attr_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      get_attr_errorator>;
  get_attr_ierrorator::future<> getxattr(
    const ObjectState& os,
    OSDOp& osd_op) const;
  get_attr_ierrorator::future<ceph::bufferlist> getxattr(
    const hobject_t& soid,
    std::string_view key) const;
  get_attr_ierrorator::future<> get_xattrs(
    const ObjectState& os,
    OSDOp& osd_op) const;
  using cmp_xattr_errorator = ::crimson::os::FuturizedStore::get_attr_errorator;
  using cmp_xattr_ierrorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      cmp_xattr_errorator>;
  cmp_xattr_ierrorator::future<> cmp_xattr(
    const ObjectState& os,
    OSDOp& osd_op) const;
  using rm_xattr_ertr = crimson::errorator<crimson::ct_error::enoent>;
  using rm_xattr_iertr =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      rm_xattr_ertr>;
  rm_xattr_iertr::future<> rm_xattr(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  interruptible_future<struct stat> stat(
    CollectionRef c,
    const ghobject_t& oid) const;
  interruptible_future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t off,
    uint64_t len);

  // OMAP
  ll_read_ierrorator::future<> omap_get_keys(
    const ObjectState& os,
    OSDOp& osd_op) const;
  ll_read_ierrorator::future<> omap_get_vals(
    const ObjectState& os,
    OSDOp& osd_op) const;
  ll_read_ierrorator::future<> omap_get_vals_by_keys(
    const ObjectState& os,
    OSDOp& osd_op) const;
  interruptible_future<> omap_set_vals(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);
  ll_read_ierrorator::future<ceph::bufferlist> omap_get_header(
    const crimson::os::CollectionRef& c,
    const ghobject_t& oid) const;
  ll_read_ierrorator::future<> omap_get_header(
    const ObjectState& os,
    OSDOp& osd_op) const;
  interruptible_future<> omap_set_header(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  interruptible_future<> omap_remove_range(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  using omap_clear_ertr = crimson::errorator<crimson::ct_error::enoent>;
  using omap_clear_iertr =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      omap_clear_ertr>;
  omap_clear_iertr::future<> omap_clear(
    ObjectState& os,
    OSDOp& osd_op,
    ceph::os::Transaction& trans,
    osd_op_params_t& osd_op_params);

  virtual void got_rep_op_reply(const MOSDRepOpReply&) {}
  virtual seastar::future<> stop() = 0;
  struct peering_info_t {
    bool is_primary;
  };
  virtual void on_actingset_changed(peering_info_t pi) = 0;
  virtual void on_activate_complete();
protected:
  const shard_id_t shard;
  CollectionRef coll;
  crimson::os::FuturizedStore* store;
  bool stopping = false;
  std::optional<peering_info_t> peering;
  virtual seastar::future<> request_committed(
    const osd_reqid_t& reqid,
    const eversion_t& at_version) = 0;
public:
  struct loaded_object_md_t {
    ObjectState os;
    std::optional<SnapSet> ss;
    using ref = std::unique_ptr<loaded_object_md_t>;
  };
  load_metadata_iertr::future<loaded_object_md_t::ref>
  load_metadata(
    const hobject_t &oid);

private:
  virtual ll_read_ierrorator::future<ceph::bufferlist> _read(
    const hobject_t& hoid,
    size_t offset,
    size_t length,
    uint32_t flags) = 0;

  bool maybe_create_new_object(ObjectState& os, ceph::os::Transaction& txn);
  virtual rep_op_fut_t
  _submit_transaction(std::set<pg_shard_t>&& pg_shards,
		      const hobject_t& hoid,
		      ceph::os::Transaction&& txn,
		      osd_op_params_t&& osd_op_p,
		      epoch_t min_epoch, epoch_t max_epoch,
		      std::vector<pg_log_entry_t>&& log_entries) = 0;
  friend class ReplicatedRecoveryBackend;
  friend class ::crimson::osd::PG;
};
