// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/interruptible_future.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "crimson/osd/recovery_backend.h"
#include "crimson/osd/object_metadata_helper.h"

#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGRecoveryDeleteReply.h"
#include "os/ObjectStore.h"

class ReplicatedRecoveryBackend : public RecoveryBackend {
public:
  ReplicatedRecoveryBackend(crimson::osd::PG& pg,
			    crimson::osd::ShardServices& shard_services,
			    crimson::os::CollectionRef coll,
			    PGBackend* backend)
    : RecoveryBackend(pg, shard_services, coll, backend)
  {}
  interruptible_future<> handle_recovery_op(
    Ref<MOSDFastDispatchOp> m,
    crimson::net::ConnectionXcoreRef conn) final;

  interruptible_future<> recover_object(
    const hobject_t& soid,
    eversion_t need) final;
  interruptible_future<> recover_delete(
    const hobject_t& soid,
    eversion_t need) final;
  interruptible_future<> push_delete(
    const hobject_t& soid,
    eversion_t need) final;
protected:
  interruptible_future<> handle_pull(
    Ref<MOSDPGPull> m);
  interruptible_future<> handle_pull_response(
    Ref<MOSDPGPush> m);
  interruptible_future<> handle_push(
    Ref<MOSDPGPush> m);
  interruptible_future<> handle_push_reply(
    Ref<MOSDPGPushReply> m);
  interruptible_future<> handle_recovery_delete(
    Ref<MOSDPGRecoveryDelete> m);
  interruptible_future<> handle_recovery_delete_reply(
    Ref<MOSDPGRecoveryDeleteReply> m);
  interruptible_future<PushOp> prep_push_to_replica(
    const hobject_t& soid,
    eversion_t need,
    pg_shard_t pg_shard);
  interruptible_future<PushOp> prep_push(
    const hobject_t& soid,
    eversion_t need,
    pg_shard_t pg_shard,
    const crimson::osd::subsets_t& subsets,
    const SnapSet push_info_ss);
  void prepare_pull(
    const crimson::osd::ObjectContextRef &head_obc,
    PullOp& pull_op,
    pull_info_t& pull_info,
    const hobject_t& soid,
    eversion_t need);
  ObjectRecoveryInfo set_recovery_info(
    const hobject_t& soid,
    const crimson::osd::SnapSetContextRef ssc);
  std::vector<pg_shard_t> get_shards_to_push(
    const hobject_t& soid) const;
  interruptible_future<PushOp> build_push_op(
    const ObjectRecoveryInfo& recovery_info,
    const ObjectRecoveryProgress& progress,
    object_stat_sum_t* stat);
  /// @returns true if this push op is the last push op for
  ///          recovery @c pop.soid
  interruptible_future<bool> _handle_pull_response(
    pg_shard_t from,
    PushOp& push_op,
    PullOp* response,
    ceph::os::Transaction* t);
  void recalc_subsets(
    ObjectRecoveryInfo& recovery_info,
    crimson::osd::SnapSetContextRef ssc);
  std::pair<interval_set<uint64_t>, ceph::bufferlist> trim_pushed_data(
    const interval_set<uint64_t> &copy_subset,
    const interval_set<uint64_t> &intervals_received,
    ceph::bufferlist data_received);
  interruptible_future<> submit_push_data(
    const ObjectRecoveryInfo &recovery_info,
    bool first,
    bool complete,
    bool clear_omap,
    interval_set<uint64_t>&& data_zeros,
    interval_set<uint64_t>&& intervals_included,
    ceph::bufferlist&& data_included,
    ceph::bufferlist&& omap_header,
    const std::map<std::string, bufferlist, std::less<>> &attrs,
    std::map<std::string, bufferlist>&& omap_entries,
    ceph::os::Transaction *t);
  void submit_push_complete(
    const ObjectRecoveryInfo &recovery_info,
    ObjectStore::Transaction *t);
  interruptible_future<> _handle_push(
    pg_shard_t from,
    PushOp& push_op,
    PushReplyOp *response,
    ceph::os::Transaction *t);
  interruptible_future<std::optional<PushOp>> _handle_push_reply(
    pg_shard_t peer,
    const PushReplyOp &op);
  interruptible_future<> on_local_recover_persist(
    const hobject_t& soid,
    const ObjectRecoveryInfo& _recovery_info,
    bool is_delete,
    epoch_t epoch_to_freeze);
  interruptible_future<> local_recover_delete(
    const hobject_t& soid,
    eversion_t need,
    epoch_t epoch_frozen);
  seastar::future<> on_stop() final {
    return seastar::now();
  }
private:
  /// pull missing object from peer
  interruptible_future<> maybe_pull_missing_obj(
    const hobject_t& soid,
    eversion_t need);

  /// load object context for recovery if it is not ready yet
  using load_obc_ertr = crimson::errorator<
    crimson::ct_error::object_corrupted>;
  using load_obc_iertr =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      load_obc_ertr>;

  interruptible_future<> maybe_push_shards(
    const crimson::osd::ObjectContextRef &head_obc,
    const hobject_t& soid,
    eversion_t need);

  /// read the data attached to given object. the size of them is supposed to
  /// be relatively small.
  ///
  /// @return @c oi.version
  interruptible_future<eversion_t> read_metadata_for_push_op(
    const hobject_t& oid,
    const ObjectRecoveryProgress& progress,
    ObjectRecoveryProgress& new_progress,
    eversion_t ver,
    PushOp* push_op);
  /// read the remaining extents of object to be recovered and fill push_op
  /// with them
  ///
  /// @param oid object being recovered
  /// @param copy_subset extents we want
  /// @param offset the offset in object from where we should read
  /// @return the new offset
  interruptible_future<uint64_t> read_object_for_push_op(
    const hobject_t& oid,
    const interval_set<uint64_t>& copy_subset,
    uint64_t offset,
    uint64_t max_len,
    PushOp* push_op);
  interruptible_future<> read_omap_for_push_op(
    const hobject_t& oid,
    const ObjectRecoveryProgress& progress,
    ObjectRecoveryProgress& new_progress,
    uint64_t& max_len,
    PushOp* push_op);
  interruptible_future<hobject_t> prep_push_target(
    const ObjectRecoveryInfo &recovery_info,
    bool first,
    bool complete,
    bool clear_omap,
    ObjectStore::Transaction* t,
    const std::map<std::string, bufferlist, std::less<>> &attrs,
    bufferlist&& omap_header);
  using interruptor = crimson::interruptible::interruptor<
    crimson::osd::IOInterruptCondition>;
};
