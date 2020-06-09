// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/recovery_backend.h"

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
    : RecoveryBackend(pg, shard_services, coll, backend) {}
  seastar::future<> handle_recovery_op(
    Ref<MOSDFastDispatchOp> m) final;

  seastar::future<> recover_object(
    const hobject_t& soid,
    eversion_t need) final;
  seastar::future<> recover_delete(
    const hobject_t& soid,
    eversion_t need) final;
  seastar::future<> push_delete(
    const hobject_t& soid,
    eversion_t need) final;
protected:
  seastar::future<> handle_pull(
    Ref<MOSDPGPull> m);
  seastar::future<> handle_pull_response(
    Ref<MOSDPGPush> m);
  seastar::future<> handle_push(
    Ref<MOSDPGPush> m);
  seastar::future<> handle_push_reply(
    Ref<MOSDPGPushReply> m);
  seastar::future<> handle_recovery_delete(
    Ref<MOSDPGRecoveryDelete> m);
  seastar::future<> handle_recovery_delete_reply(
    Ref<MOSDPGRecoveryDeleteReply> m);
  seastar::future<> prep_push(
    const hobject_t& soid,
    eversion_t need,
    std::map<pg_shard_t, PushOp>* pops,
    const std::list<std::map<pg_shard_t, pg_missing_t>::const_iterator>& shards);
  void prepare_pull(
    PullOp& po,
    PullInfo& pi,
    const hobject_t& soid,
    eversion_t need);
  std::list<std::map<pg_shard_t, pg_missing_t>::const_iterator> get_shards_to_push(
    const hobject_t& soid);
  seastar::future<ObjectRecoveryProgress> build_push_op(
    const ObjectRecoveryInfo& recovery_info,
    const ObjectRecoveryProgress& progress,
    object_stat_sum_t* stat,
    PushOp* pop);
  seastar::future<bool> _handle_pull_response(
    pg_shard_t from,
    PushOp& pop,
    PullOp* response,
    ceph::os::Transaction* t);
  void trim_pushed_data(
    const interval_set<uint64_t> &copy_subset,
    const interval_set<uint64_t> &intervals_received,
    ceph::bufferlist data_received,
    interval_set<uint64_t> *intervals_usable,
    bufferlist *data_usable);
  seastar::future<> submit_push_data(
    const ObjectRecoveryInfo &recovery_info,
    bool first,
    bool complete,
    bool clear_omap,
    interval_set<uint64_t> &data_zeros,
    const interval_set<uint64_t> &intervals_included,
    ceph::bufferlist data_included,
    ceph::bufferlist omap_header,
    const std::map<string, bufferlist> &attrs,
    const std::map<string, bufferlist> &omap_entries,
    ceph::os::Transaction *t);
  void submit_push_complete(
    const ObjectRecoveryInfo &recovery_info,
    ObjectStore::Transaction *t);
  seastar::future<> _handle_push(
    pg_shard_t from,
    const PushOp &pop,
    PushReplyOp *response,
    ceph::os::Transaction *t);
  seastar::future<bool> _handle_push_reply(
    pg_shard_t peer,
    const PushReplyOp &op,
    PushOp *reply);
  seastar::future<> on_local_recover_persist(
    const hobject_t& soid,
    const ObjectRecoveryInfo& _recovery_info,
    bool is_delete,
    epoch_t epoch_to_freeze);
  seastar::future<> local_recover_delete(
    const hobject_t& soid,
    eversion_t need,
    epoch_t epoch_frozen);
  seastar::future<> on_stop() final {
    return seastar::now();
  }
};
