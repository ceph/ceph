// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/future.hh>
#include <seastar/core/do_with.hh>

#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"
#include "osd/osd_types_fmt.h"
#include "replicated_recovery_backend.h"
#include "msg/Message.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

using std::less;
using std::map;
using std::string;

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::recover_object(
  const hobject_t& soid,
  eversion_t need)
{
  logger().debug("{}: {}, {}", __func__, soid, need);
  // always add_recovering(soid) before recover_object(soid)
  assert(is_recovering(soid));
  // start tracking the recovery of soid
  return maybe_pull_missing_obj(soid, need).then_interruptible([this, soid, need] {
    logger().debug("recover_object: loading obc: {}", soid);
    return pg.obc_loader.with_obc<RWState::RWREAD>(soid,
      [this, soid, need](auto head, auto obc) {
      logger().debug("recover_object: loaded obc: {}", obc->obs.oi.soid);
      auto& recovery_waiter = get_recovering(soid);
      recovery_waiter.obc = obc;
      recovery_waiter.obc->wait_recovery_read();
      return maybe_push_shards(head, soid, need);
    }).handle_error_interruptible(
      crimson::osd::PG::load_obc_ertr::all_same_way([soid](auto& code) {
      // TODO: may need eio handling?
      logger().error("recover_object saw error code {}, ignoring object {}",
                     code, soid);
    }));
  });
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::maybe_push_shards(
  const crimson::osd::ObjectContextRef &head_obc,
  const hobject_t& soid,
  eversion_t need)
{
  return seastar::do_with(
    get_shards_to_push(soid),
    [this, need, soid, head_obc](auto &shards) {
    return interruptor::parallel_for_each(
      shards,
      [this, need, soid, head_obc](auto shard) {
      return prep_push_to_replica(soid, need, shard
      ).then_interruptible([this, soid, shard](auto push) {
        auto msg = crimson::make_message<MOSDPGPush>();
        msg->from = pg.get_pg_whoami();
        msg->pgid = pg.get_pgid();
        msg->map_epoch = pg.get_osdmap_epoch();
        msg->min_epoch = pg.get_last_peering_reset();
        msg->pushes.push_back(std::move(push));
        msg->set_priority(pg.get_recovery_op_priority());
        return interruptor::make_interruptible(
            shard_services.send_to_osd(shard.osd,
                                       std::move(msg),
                                       pg.get_osdmap_epoch()))
        .then_interruptible(
          [this, soid, shard] {
          return get_recovering(soid).wait_for_pushes(shard);
        });
      });
    });
  }).then_interruptible([this, soid] {
    auto &recovery = get_recovering(soid);
    if (auto push_info = recovery.pushing.begin();
        push_info != recovery.pushing.end()) {
      pg.get_recovery_handler()->on_global_recover(soid,
                                                   push_info->second.stat,
                                                   false);
    } else if (recovery.pull_info) {
      // no push happened (empty get_shards_to_push()) but pull actually did
      pg.get_recovery_handler()->on_global_recover(soid,
                                                   recovery.pull_info->stat,
                                                   false);
    } else {
      // no pulls, no pushes
    }
    return seastar::make_ready_future<>();
  }).handle_exception_interruptible([this, soid](auto e) {
    auto &recovery = get_recovering(soid);
    if (recovery.obc) {
      recovery.obc->drop_recovery_read();
    }
    recovering.erase(soid);
    return seastar::make_exception_future<>(e);
  });
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::maybe_pull_missing_obj(
  const hobject_t& soid,
  eversion_t need)
{
  logger().debug("{}: {}, {}", __func__, soid, need);
  pg_missing_tracker_t local_missing = pg.get_local_missing();
  if (!local_missing.is_missing(soid)) {
    // object is not missing, don't pull
    return seastar::make_ready_future<>();
  }
  return pg.obc_loader.with_obc<RWState::RWREAD>(soid.get_head(),
    [this, soid, need](auto head, auto) {
    PullOp pull_op;
    auto& recovery_waiter = get_recovering(soid);
    recovery_waiter.pull_info =
      std::make_optional<RecoveryBackend::pull_info_t>();
    auto& pull_info = *recovery_waiter.pull_info;
    prepare_pull(head, pull_op, pull_info, soid, need);
    auto msg = crimson::make_message<MOSDPGPull>();
    msg->from = pg.get_pg_whoami();
    msg->set_priority(pg.get_recovery_op_priority());
    msg->pgid = pg.get_pgid();
    msg->map_epoch = pg.get_osdmap_epoch();
    msg->min_epoch = pg.get_last_peering_reset();
    msg->set_pulls({std::move(pull_op)});
    return shard_services.send_to_osd(
      pull_info.from.osd,
      std::move(msg),
      pg.get_osdmap_epoch());
  }).si_then([this, soid] {
    auto& recovery_waiter = get_recovering(soid);
    return recovery_waiter.wait_for_pull();
  }).handle_error_interruptible(
    crimson::ct_error::assert_all("unexpected error")
  );
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::push_delete(
  const hobject_t& soid,
  eversion_t need)
{
  logger().debug("{}: {}, {}", __func__, soid, need);
  epoch_t min_epoch = pg.get_last_peering_reset();

  assert(pg.get_acting_recovery_backfill().size() > 0);
  return interruptor::parallel_for_each(pg.get_acting_recovery_backfill(),
    [this, soid, need, min_epoch](pg_shard_t shard)
    -> interruptible_future<> {
    if (shard == pg.get_pg_whoami())
      return seastar::make_ready_future<>();
    auto iter = pg.get_shard_missing().find(shard);
    if (iter == pg.get_shard_missing().end())
      return seastar::make_ready_future<>();
    if (iter->second.is_missing(soid)) {
      logger().debug("push_delete: will remove {} from {}", soid, shard);
      pg.begin_peer_recover(shard, soid);
      spg_t target_pg(pg.get_info().pgid.pgid, shard.shard);
      auto msg = crimson::make_message<MOSDPGRecoveryDelete>(
	  pg.get_pg_whoami(), target_pg, pg.get_osdmap_epoch(), min_epoch);
      msg->set_priority(pg.get_recovery_op_priority());
      msg->objects.push_back(std::make_pair(soid, need));
      return interruptor::make_interruptible(
	  shard_services.send_to_osd(shard.osd, std::move(msg),
				     pg.get_osdmap_epoch())).then_interruptible(
	[this, soid, shard] {
	return get_recovering(soid).wait_for_pushes(shard);
      });
    }
    return seastar::make_ready_future<>();
  });
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::handle_recovery_delete(
  Ref<MOSDPGRecoveryDelete> m)
{
  logger().debug("{}: {}", __func__, *m);

  auto& p = m->objects.front(); //TODO: only one delete per message for now.
  return local_recover_delete(p.first, p.second, pg.get_osdmap_epoch())
  .then_interruptible(
    [this, m] {
    auto reply = crimson::make_message<MOSDPGRecoveryDeleteReply>();
    reply->from = pg.get_pg_whoami();
    reply->set_priority(m->get_priority());
    reply->pgid = spg_t(pg.get_info().pgid.pgid, m->from.shard);
    reply->map_epoch = m->map_epoch;
    reply->min_epoch = m->min_epoch;
    reply->objects = m->objects;
    return shard_services.send_to_osd(m->from.osd, std::move(reply), pg.get_osdmap_epoch());
  });
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::on_local_recover_persist(
  const hobject_t& soid,
  const ObjectRecoveryInfo& _recovery_info,
  bool is_delete,
  epoch_t epoch_frozen)
{
  logger().debug("{}", __func__);
  return seastar::do_with(
    ceph::os::Transaction(),
    [this, soid, &_recovery_info, is_delete, epoch_frozen](auto &t) {
    return pg.get_recovery_handler()->on_local_recover(
      soid, _recovery_info, is_delete, t
    ).then_interruptible([this, &t] {
      logger().debug("ReplicatedRecoveryBackend::{}: do_transaction...", __func__);
      return shard_services.get_store().do_transaction(coll, std::move(t));
    }).then_interruptible(
      [this, epoch_frozen, last_complete = pg.get_info().last_complete] {
      pg.get_recovery_handler()->_committed_pushed_object(epoch_frozen, last_complete);
      return seastar::make_ready_future<>();
    });
  });
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::local_recover_delete(
  const hobject_t& soid,
  eversion_t need,
  epoch_t epoch_to_freeze)
{
  logger().debug("{}: {}, {}", __func__, soid, need);
  return backend->load_metadata(soid).safe_then_interruptible([this]
    (auto lomt) -> interruptible_future<> {
    if (lomt->os.exists) {
      return seastar::do_with(ceph::os::Transaction(),
	[this, lomt = std::move(lomt)](auto& txn) {
	return backend->remove(lomt->os, txn).then_interruptible(
	  [this, &txn]() mutable {
	  logger().debug("ReplicatedRecoveryBackend::local_recover_delete: do_transaction...");
	  return shard_services.get_store().do_transaction(coll,
							   std::move(txn));
	});
      });
    }
    return seastar::make_ready_future<>();
  }).safe_then_interruptible([this, soid, epoch_to_freeze, need] {
    return seastar::do_with(
      ObjectRecoveryInfo(),
      [soid, need, this, epoch_to_freeze](auto &recovery_info) {
      recovery_info.soid = soid;
      recovery_info.version = need;
      return on_local_recover_persist(soid, recovery_info,
                                      true, epoch_to_freeze);
    });
  }, PGBackend::load_metadata_ertr::all_same_way(
      [this, soid, epoch_to_freeze, need] (auto e) {
      return seastar::do_with(
        ObjectRecoveryInfo(),
        [soid, need, this, epoch_to_freeze](auto &recovery_info) {
        recovery_info.soid = soid;
        recovery_info.version = need;
        return on_local_recover_persist(soid, recovery_info,
                                        true, epoch_to_freeze);
      });
    })
  );
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::recover_delete(
  const hobject_t &soid, eversion_t need)
{
  logger().debug("{}: {}, {}", __func__, soid, need);

  epoch_t cur_epoch = pg.get_osdmap_epoch();
  return seastar::do_with(object_stat_sum_t(),
    [this, soid, need, cur_epoch](auto& stat_diff) {
    return local_recover_delete(soid, need, cur_epoch).then_interruptible(
      [this, &stat_diff, cur_epoch, soid, need]()
      -> interruptible_future<> {
      if (!pg.has_reset_since(cur_epoch)) {
	bool object_missing = false;
	for (const auto& shard : pg.get_acting_recovery_backfill()) {
	  if (shard == pg.get_pg_whoami())
	    continue;
	  if (pg.get_shard_missing(shard)->is_missing(soid)) {
	    logger().debug("recover_delete: soid {} needs to deleted from replca {}",
			   soid, shard);
	    object_missing = true;
	    break;
	  }
	}

	if (!object_missing) {
	  stat_diff.num_objects_recovered = 1;
	  return seastar::make_ready_future<>();
	} else {
	  return push_delete(soid, need);
	}
      }
      return seastar::make_ready_future<>();
    }).then_interruptible([this, soid, &stat_diff] {
      pg.get_recovery_handler()->on_global_recover(soid, stat_diff, true);
      return seastar::make_ready_future<>();
    });
  });
}

RecoveryBackend::interruptible_future<PushOp>
ReplicatedRecoveryBackend::prep_push_to_replica(
  const hobject_t& soid,
  eversion_t need,
  pg_shard_t pg_shard)
{
  logger().debug("{}: {}, {}", __func__, soid, need);

  auto& recovery_waiter = get_recovering(soid);
  auto& obc = recovery_waiter.obc;
  SnapSet push_info_ss; // only populated if soid is_snap()
  crimson::osd::subsets_t subsets;
  const auto& missing =
    pg.get_shard_missing().find(pg_shard)->second;

  // are we doing a clone on the replica?
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    // try to base push off of clones that succeed/preceed poid
    // we need the head (and current SnapSet) locally to do that.
    if (pg.get_local_missing().is_missing(head)) {
      logger().debug("{} missing head {}, pushing raw clone",
                     __func__, head);
      if (obc->obs.oi.size) {
        subsets.data_subset.insert(0, obc->obs.oi.size);
      }
      return prep_push(soid,
                       need,
                       pg_shard,
                       subsets,
                       push_info_ss);
    }
    auto ssc = obc->ssc;
    ceph_assert(ssc);
    push_info_ss = ssc->snapset;
    logger().debug("push_to_replica snapset is {}",
                   ssc->snapset);

    subsets = crimson::osd::calc_clone_subsets(
      ssc->snapset, soid,
      missing,
      // get_peer_info() asserts `peer_info` existence.
      pg.get_peering_state().get_peer_info(
        pg_shard).last_backfill);
  } else if (soid.snap == CEPH_NOSNAP) {
    // pushing head or unversioned object.
    // base this on partially on replica's clones?
    auto ssc = obc->ssc;
    ceph_assert(ssc);
    logger().debug("push_to_replica snapset is {}",
                   ssc->snapset);
    subsets = crimson::osd::calc_head_subsets(
      obc->obs.oi.size,
      ssc->snapset, soid,
      missing,
      pg.get_peering_state().get_peer_info(
        pg_shard).last_backfill);
  }
  return prep_push(soid,
                   need,
                   pg_shard,
                   subsets,
                   push_info_ss);
}

RecoveryBackend::interruptible_future<PushOp>
ReplicatedRecoveryBackend::prep_push(
  const hobject_t& soid,
  eversion_t need,
  pg_shard_t pg_shard,
  const crimson::osd::subsets_t& subsets,
  const SnapSet push_info_ss)
{
  logger().debug("{}: {}, {}", __func__, soid, need);
  auto& recovery_waiter = get_recovering(soid);
  auto& obc = recovery_waiter.obc;

  auto& push_info = recovery_waiter.pushing[pg_shard];
  pg.begin_peer_recover(pg_shard, soid);
  const auto pmissing_iter = pg.get_shard_missing().find(pg_shard);
  const auto missing_iter = pmissing_iter->second.get_items().find(soid);
  assert(missing_iter != pmissing_iter->second.get_items().end());

  push_info.obc = obc;
  push_info.recovery_info.size = obc->obs.oi.size;
  push_info.recovery_info.copy_subset = subsets.data_subset;
  push_info.recovery_info.clone_subset = subsets.clone_subsets;
  push_info.recovery_info.ss = push_info_ss;
  push_info.recovery_info.soid = soid;
  push_info.recovery_info.oi = obc->obs.oi;
  push_info.recovery_info.version = obc->obs.oi.version;
  push_info.recovery_info.object_exist =
    missing_iter->second.clean_regions.object_is_exist();
  push_info.recovery_progress.omap_complete =
    !missing_iter->second.clean_regions.omap_is_dirty();

  return build_push_op(push_info.recovery_info,
                       push_info.recovery_progress,
                       &push_info.stat).then_interruptible(
    [this, soid, pg_shard](auto push_op) {
    auto& recovery_waiter = get_recovering(soid);
    auto& push_info = recovery_waiter.pushing[pg_shard];
    push_info.recovery_progress = push_op.after_progress;
    return push_op;
  });

}

void ReplicatedRecoveryBackend::prepare_pull(
  const crimson::osd::ObjectContextRef &head_obc,
  PullOp& pull_op,
  pull_info_t& pull_info,
  const hobject_t& soid,
  eversion_t need) {
  logger().debug("{}: {}, {}", __func__, soid, need);

  pg_missing_tracker_t local_missing = pg.get_local_missing();
  const auto missing_iter = local_missing.get_items().find(soid);
  auto m = pg.get_missing_loc_shards();
  pg_shard_t fromshard = *(m[soid].begin());
  const auto& last_backfill =
    pg.get_peering_state().get_peer_info(fromshard).last_backfill;

  pull_op.recovery_info =
    set_recovery_info(soid, head_obc->ssc, last_backfill);
  pull_op.soid = soid;
  pull_op.recovery_progress.data_complete = false;
  pull_op.recovery_progress.omap_complete =
    !missing_iter->second.clean_regions.omap_is_dirty();
  pull_op.recovery_progress.data_recovered_to = 0;
  pull_op.recovery_progress.first = true;

  pull_info.from = fromshard;
  pull_info.soid = soid;
  pull_info.head_ctx = head_obc;
  pull_info.recovery_info = pull_op.recovery_info;
  pull_info.recovery_progress = pull_op.recovery_progress;
}

ObjectRecoveryInfo ReplicatedRecoveryBackend::set_recovery_info(
  const hobject_t& soid,
  const crimson::osd::SnapSetContextRef ssc,
  const hobject_t& last_backfill)
{
  pg_missing_tracker_t local_missing = pg.get_local_missing();
  const auto missing_iter = local_missing.get_items().find(soid);
  ObjectRecoveryInfo recovery_info;
  if (soid.is_snap()) {
    assert(!local_missing.is_missing(soid.get_head()));
    assert(ssc);
    recovery_info.ss = ssc->snapset;
    auto subsets = crimson::osd::calc_clone_subsets(
      ssc->snapset, soid, local_missing, last_backfill);
    crimson::osd::set_subsets(subsets, recovery_info);
    logger().debug("{}: pulling {}", __func__, recovery_info);
    ceph_assert(ssc->snapset.clone_size.count(soid.snap));
    recovery_info.size = ssc->snapset.clone_size[soid.snap];
  } else {
    // pulling head or unversioned object.
    // always pull the whole thing.
    recovery_info.copy_subset.insert(0, (uint64_t) -1);
    recovery_info.copy_subset.intersection_of(
      missing_iter->second.clean_regions.get_dirty_regions());
    recovery_info.size = ((uint64_t) -1);
  }
  recovery_info.object_exist =
    missing_iter->second.clean_regions.object_is_exist();
  recovery_info.soid = soid;
  return recovery_info;
}

RecoveryBackend::interruptible_future<PushOp>
ReplicatedRecoveryBackend::build_push_op(
    const ObjectRecoveryInfo& recovery_info,
    const ObjectRecoveryProgress& progress,
    object_stat_sum_t* stat)
{
  logger().debug("{} {} @{}",
		 __func__, recovery_info.soid, recovery_info.version);
  return seastar::do_with(ObjectRecoveryProgress(progress),
			  uint64_t(crimson::common::local_conf()
			    ->osd_recovery_max_chunk),
			  recovery_info.version,
			  PushOp(),
    [this, &recovery_info, &progress, stat]
    (auto& new_progress, auto& available, auto& v, auto& push_op) {
    return read_metadata_for_push_op(recovery_info.soid,
                                     progress, new_progress,
                                     v, &push_op
    ).then_interruptible([&](eversion_t local_ver) mutable {
      // If requestor didn't know the version, use ours
      if (v == eversion_t()) {
        v = local_ver;
      } else if (v != local_ver) {
        logger().error("build_push_op: {} push {} v{} failed because local copy is {}",
                       pg.get_pgid(), recovery_info.soid, recovery_info.version, local_ver);
        // TODO: bail out
      }
      return read_omap_for_push_op(recovery_info.soid,
                                   progress,
                                   new_progress,
                                   available, &push_op);
    }).then_interruptible([this, &recovery_info, &progress,
                           &available, &push_op]() mutable {
      logger().debug("build_push_op: available: {}, copy_subset: {}",
		     available, recovery_info.copy_subset);
      return read_object_for_push_op(recovery_info.soid,
				     recovery_info.copy_subset,
				     progress.data_recovered_to,
				     available, &push_op);
    }).then_interruptible([&recovery_info, &v, &progress,
                           &new_progress, stat, &push_op]
            (uint64_t recovered_to) mutable {
      new_progress.data_recovered_to = recovered_to;
      if (new_progress.is_complete(recovery_info)) {
	new_progress.data_complete = true;
	if (stat)
	  stat->num_objects_recovered++;
      } else if (progress.first && progress.omap_complete) {
      // If omap is not changed, we need recovery omap
      // when recovery cannot be completed once
	new_progress.omap_complete = false;
      }
      if (stat) {
	stat->num_keys_recovered += push_op.omap_entries.size();
	stat->num_bytes_recovered += push_op.data.length();
      }
      push_op.version = v;
      push_op.soid = recovery_info.soid;
      push_op.recovery_info = recovery_info;
      push_op.after_progress = new_progress;
      push_op.before_progress = progress;
      logger().debug("build_push_op: push_op version:"
                     " {}, push_op data length: {}",
		     push_op.version, push_op.data.length());
      return seastar::make_ready_future<PushOp>(std::move(push_op));
    });
  });
}

RecoveryBackend::interruptible_future<eversion_t>
ReplicatedRecoveryBackend::read_metadata_for_push_op(
    const hobject_t& oid,
    const ObjectRecoveryProgress& progress,
    ObjectRecoveryProgress& new_progress,
    eversion_t ver,
    PushOp* push_op)
{
  logger().debug("{}, {}", __func__, oid);
  if (!progress.first) {
    return seastar::make_ready_future<eversion_t>(ver);
  }
  return interruptor::make_interruptible(interruptor::when_all_succeed(
      backend->omap_get_header(coll, ghobject_t(oid)).handle_error_interruptible<false>(
	crimson::os::FuturizedStore::Shard::read_errorator::all_same_way(
	  [oid] (const std::error_code& e) {
	  logger().debug("read_metadata_for_push_op, error {} when getting omap header: {}", e, oid);
	  return seastar::make_ready_future<bufferlist>();
	})),
      interruptor::make_interruptible(store->get_attrs(coll, ghobject_t(oid)))
      .handle_error_interruptible<false>(
	crimson::os::FuturizedStore::Shard::get_attrs_ertr::all_same_way(
	  [oid] (const std::error_code& e) {
	  logger().debug("read_metadata_for_push_op, error {} when getting attrs: {}", e, oid);
	  return seastar::make_ready_future<crimson::os::FuturizedStore::Shard::attrs_t>();
	}))
  )).then_unpack_interruptible([&new_progress, push_op](auto bl, auto attrs) {
    if (bl.length() == 0) {
      logger().warn("read_metadata_for_push_op: fail to read omap header");
    } else if (attrs.empty()) {
      logger().error("read_metadata_for_push_op: fail to read attrs");
      return eversion_t{};
    }
    push_op->omap_header.claim_append(std::move(bl));
    for (auto&& [key, val] : attrs) {
      push_op->attrset.emplace(std::move(key), std::move(val));
    }
    logger().debug("read_metadata_for_push_op: {}", push_op->attrset[OI_ATTR]);
    object_info_t oi;
    oi.decode_no_oid(push_op->attrset[OI_ATTR]);
    new_progress.first = false;
    return oi.version;
  });
}

RecoveryBackend::interruptible_future<uint64_t>
ReplicatedRecoveryBackend::read_object_for_push_op(
    const hobject_t& oid,
    const interval_set<uint64_t>& copy_subset,
    uint64_t offset,
    uint64_t max_len,
    PushOp* push_op)
{
  if (max_len == 0 || copy_subset.empty()) {
    push_op->data_included.clear();
    return seastar::make_ready_future<uint64_t>(offset);
  }
  // 1. get the extents in the interested range
  return interruptor::make_interruptible(backend->fiemap(coll, ghobject_t{oid},
    0, copy_subset.range_end())).safe_then_interruptible(
    [=, this](auto&& fiemap_included) mutable {
    interval_set<uint64_t> extents;
    try {
      extents.intersection_of(copy_subset, std::move(fiemap_included));
    } catch (std::exception &) {
      // if fiemap() fails, we will read nothing, as the intersection of
      // copy_subset and an empty interval_set would be empty anyway
      extents.clear();
    }
    // 2. we can read up to "max_len" bytes from "offset", so truncate the
    //    extents down to this quota. no need to return the number of consumed
    //    bytes, as this is the last consumer of this quota
    push_op->data_included.span_of(extents, offset, max_len);
    // 3. read the truncated extents
    // TODO: check if the returned extents are pruned
    return interruptor::make_interruptible(store->readv(coll, ghobject_t{oid},
      push_op->data_included, 0));
  }).safe_then_interruptible([push_op, range_end=copy_subset.range_end()](auto &&bl) {
    push_op->data.claim_append(std::move(bl));
    uint64_t recovered_to = 0;
    if (push_op->data_included.empty()) {
      // zero filled section, skip to end!
      recovered_to = range_end;
    } else {
      // note down the progress, we will start from there next time
      recovered_to = push_op->data_included.range_end();
    }
    return seastar::make_ready_future<uint64_t>(recovered_to);
  }, PGBackend::read_errorator::all_same_way([](auto e) {
    logger().debug("build_push_op: read exception");
    return seastar::make_exception_future<uint64_t>(e);
  }));
}

static std::optional<std::string> nullopt_if_empty(const std::string& s)
{
  return s.empty() ? std::nullopt : std::make_optional(s);
}

static bool is_too_many_entries_per_chunk(const PushOp* push_op)
{
  const uint64_t entries_per_chunk =
    crimson::common::local_conf()->osd_recovery_max_omap_entries_per_chunk;
  if (!entries_per_chunk) {
    // the limit is disabled
    return false;
  }
  return push_op->omap_entries.size() >= entries_per_chunk;
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::read_omap_for_push_op(
    const hobject_t& oid,
    const ObjectRecoveryProgress& progress,
    ObjectRecoveryProgress& new_progress,
    uint64_t& max_len,
    PushOp* push_op)
{
  if (progress.omap_complete) {
    return seastar::make_ready_future<>();
  }
  return seastar::repeat([&new_progress, &max_len, push_op, &oid, this] {
    return shard_services.get_store().omap_get_values(
      coll, ghobject_t{oid}, nullopt_if_empty(new_progress.omap_recovered_to)
    ).safe_then([&new_progress, &max_len, push_op](const auto& ret) {
      const auto& [done, kvs] = ret;
      bool stop = done;
      // assuming "values.empty() only if done" holds here!
      for (const auto& [key, value] : kvs) {
        if (is_too_many_entries_per_chunk(push_op)) {
          stop = true;
          break;
        }
        if (const uint64_t entry_size = key.size() + value.length();
            entry_size > max_len) {
          stop = true;
          break;
        } else {
          max_len -= std::min(max_len, entry_size);
        }
        push_op->omap_entries.emplace(key, value);
      }
      if (!push_op->omap_entries.empty()) {
        // we iterate in order
        new_progress.omap_recovered_to = std::rbegin(push_op->omap_entries)->first;
      }
      if (done) {
        new_progress.omap_complete = true;
      }
      return seastar::make_ready_future<seastar::stop_iteration>(
        stop ? seastar::stop_iteration::yes : seastar::stop_iteration::no
      );
    }, crimson::os::FuturizedStore::Shard::read_errorator::assert_all{});
  });
}

std::vector<pg_shard_t>
ReplicatedRecoveryBackend::get_shards_to_push(const hobject_t& soid) const
{
  std::vector<pg_shard_t> shards;
  assert(pg.get_acting_recovery_backfill().size() > 0);
  for (const auto& peer : pg.get_acting_recovery_backfill()) {
    if (peer == pg.get_pg_whoami())
      continue;
    auto shard_missing =
      pg.get_shard_missing().find(peer);
    assert(shard_missing != pg.get_shard_missing().end());
    if (shard_missing->second.is_missing(soid)) {
      shards.push_back(shard_missing->first);
    }
  }
  return shards;
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::handle_pull(Ref<MOSDPGPull> m)
{
  logger().debug("{}: {}", __func__, *m);
  if (pg.can_discard_replica_op(*m)) {
    logger().debug("{}: discarding {}", __func__, *m);
    return seastar::now();
  }
  return seastar::do_with(m->take_pulls(), [this, from=m->from](auto& pulls) {
    return interruptor::parallel_for_each(pulls,
                                      [this, from](auto& pull_op) {
      const hobject_t& soid = pull_op.soid;
      logger().debug("handle_pull: {}", soid);
      return backend->stat(coll, ghobject_t(soid)).then_interruptible(
        [this, &pull_op](auto st) {
        ObjectRecoveryInfo &recovery_info = pull_op.recovery_info;
        ObjectRecoveryProgress &progress = pull_op.recovery_progress;
        if (progress.first && recovery_info.size == ((uint64_t) -1)) {
          // Adjust size and copy_subset
          recovery_info.size = st.st_size;
          if (st.st_size) {
            interval_set<uint64_t> object_range;
            object_range.insert(0, st.st_size);
            recovery_info.copy_subset.intersection_of(object_range);
          } else {
            recovery_info.copy_subset.clear();
          }
          assert(recovery_info.clone_subset.empty());
        }
        return build_push_op(recovery_info, progress, 0);
      }).then_interruptible([this, from](auto push_op) {
        auto msg = crimson::make_message<MOSDPGPush>();
        msg->from = pg.get_pg_whoami();
        msg->pgid = pg.get_pgid();
        msg->map_epoch = pg.get_osdmap_epoch();
        msg->min_epoch = pg.get_last_peering_reset();
        msg->set_priority(pg.get_recovery_op_priority());
        msg->pushes.push_back(std::move(push_op));
        return shard_services.send_to_osd(from.osd, std::move(msg),
                                          pg.get_osdmap_epoch());
      });
    });
  });
}

RecoveryBackend::interruptible_future<bool>
ReplicatedRecoveryBackend::_handle_pull_response(
  pg_shard_t from,
  PushOp& push_op,
  PullOp* response,
  ceph::os::Transaction* t)
{
  logger().debug("handle_pull_response {} {} data.size() is {} data_included: {}",
      push_op.recovery_info, push_op.after_progress,
      push_op.data.length(), push_op.data_included);

  const hobject_t &hoid = push_op.soid;
  auto& recovery_waiter = get_recovering(hoid);
  auto& pull_info = *recovery_waiter.pull_info;
  if (pull_info.recovery_info.size == (uint64_t(-1))) {
    pull_info.recovery_info.size = push_op.recovery_info.size;
    pull_info.recovery_info.copy_subset.intersection_of(
	push_op.recovery_info.copy_subset);
  }

  // If primary doesn't have object info and didn't know version
  if (pull_info.recovery_info.version == eversion_t())
    pull_info.recovery_info.version = push_op.version;

  auto prepare_waiter = interruptor::make_interruptible(
      seastar::make_ready_future<>());
  if (pull_info.recovery_progress.first) {
    prepare_waiter = pg.obc_loader.with_obc<RWState::RWNONE>(
      pull_info.recovery_info.soid,
      [this, &pull_info, &recovery_waiter, &push_op](auto, auto obc) {
        pull_info.obc = obc;
        recovery_waiter.obc = obc;
        obc->obs.oi.decode_no_oid(push_op.attrset.at(OI_ATTR),
                                  push_op.soid);
        auto ss_attr_iter = push_op.attrset.find(SS_ATTR);
        if (ss_attr_iter != push_op.attrset.end()) {
          if (!obc->ssc) {
            obc->ssc = new crimson::osd::SnapSetContext(
              push_op.soid.get_snapdir());
          }
          try {
            obc->ssc->snapset = SnapSet(ss_attr_iter->second);
            obc->ssc->exists = true;
          } catch (const buffer::error&) {
            logger().warn("unable to decode SnapSet");
            throw crimson::osd::invalid_argument();
          }
          assert(!pull_info.obc->ssc->exists ||
                 obc->ssc->snapset.seq == pull_info.obc->ssc->snapset.seq);
        }
        pull_info.recovery_info.oi = obc->obs.oi;
        if (pull_info.recovery_info.soid.snap &&
            pull_info.recovery_info.soid.snap < CEPH_NOSNAP) {
            recalc_subsets(pull_info.recovery_info,
                           pull_info.obc->ssc);
        }
        return crimson::osd::PG::load_obc_ertr::now();
      }).handle_error_interruptible(crimson::ct_error::assert_all{});
  };
  return prepare_waiter.then_interruptible(
    [this, &pull_info, &push_op, t, response]() mutable {
    const bool first = pull_info.recovery_progress.first;
    pull_info.recovery_progress = push_op.after_progress;
    logger().debug("new recovery_info {}, new progress {}",
		   pull_info.recovery_info, pull_info.recovery_progress);
    interval_set<uint64_t> data_zeros;
    {
      uint64_t offset = push_op.before_progress.data_recovered_to;
      uint64_t length = (push_op.after_progress.data_recovered_to -
			 push_op.before_progress.data_recovered_to);
      if (length) {
        data_zeros.insert(offset, length);
      }
    }
    auto [usable_intervals, data] =
      trim_pushed_data(pull_info.recovery_info.copy_subset,
                       push_op.data_included, push_op.data);
    bool complete = pull_info.is_complete();
    bool clear_omap = !push_op.before_progress.omap_complete;
    return submit_push_data(pull_info.recovery_info,
                            first, complete, clear_omap,
                            std::move(data_zeros), std::move(usable_intervals),
                            std::move(data), std::move(push_op.omap_header),
                            push_op.attrset, std::move(push_op.omap_entries), t)
    .then_interruptible(
      [this, response, &pull_info, &push_op, complete,
        t, bytes_recovered=data.length()]()
      -> RecoveryBackend::interruptible_future<bool> {
      pull_info.stat.num_keys_recovered += push_op.omap_entries.size();
      pull_info.stat.num_bytes_recovered += bytes_recovered;

      if (complete) {
	pull_info.stat.num_objects_recovered++;
	return pg.get_recovery_handler()->on_local_recover(
	    push_op.soid, get_recovering(push_op.soid).pull_info->recovery_info,
	    false, *t
        ).then_interruptible([] {
          return true;
        });
      } else {
        response->soid = push_op.soid;
        response->recovery_info = pull_info.recovery_info;
        response->recovery_progress = pull_info.recovery_progress;
        return seastar::make_ready_future<bool>(false);
      }
    });
  });
}

void ReplicatedRecoveryBackend::recalc_subsets(
    ObjectRecoveryInfo& recovery_info,
    crimson::osd::SnapSetContextRef ssc)
{
  assert(ssc);
  auto subsets = crimson::osd::calc_clone_subsets(
    ssc->snapset, recovery_info.soid, pg.get_local_missing(),
    pg.get_info().last_backfill);
  crimson::osd::set_subsets(subsets, recovery_info);
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::handle_pull_response(
  Ref<MOSDPGPush> m)
{
  if (pg.can_discard_replica_op(*m)) {
    logger().debug("{}: discarding {}", __func__, *m);
    return seastar::now();
  }
  const PushOp& push_op = m->pushes[0]; //TODO: only one push per message for now.
  if (push_op.version == eversion_t()) {
    // replica doesn't have it!
    pg.get_recovery_handler()->on_failed_recover({ m->from }, push_op.soid,
	get_recovering(push_op.soid).pull_info->recovery_info.version);
    return seastar::make_exception_future<>(
	std::runtime_error(fmt::format(
	    "Error on pushing side {} when pulling obj {}",
	    m->from, push_op.soid)));
  }

  logger().debug("{}: {}", __func__, *m);
  return seastar::do_with(PullOp(), [this, m](auto& response) {
    return seastar::do_with(ceph::os::Transaction(), m.get(),
      [this, &response](auto& t, auto& m) {
      pg_shard_t from = m->from;
      PushOp& push_op = m->pushes[0]; // only one push per message for now
      return _handle_pull_response(from, push_op, &response, &t
      ).then_interruptible(
	[this, &t](bool complete) {
	epoch_t epoch_frozen = pg.get_osdmap_epoch();
	logger().debug("ReplicatedRecoveryBackend::handle_pull_response: do_transaction...");
	return shard_services.get_store().do_transaction(coll, std::move(t))
	  .then([this, epoch_frozen, complete,
	  last_complete = pg.get_info().last_complete] {
	  pg.get_recovery_handler()->_committed_pushed_object(epoch_frozen, last_complete);
	  return seastar::make_ready_future<bool>(complete);
	});
      });
    }).then_interruptible([this, m, &response](bool complete) {
      if (complete) {
	auto& push_op = m->pushes[0];
	get_recovering(push_op.soid).set_pulled();
	return seastar::make_ready_future<>();
      } else {
	auto reply = crimson::make_message<MOSDPGPull>();
	reply->from = pg.get_pg_whoami();
	reply->set_priority(m->get_priority());
	reply->pgid = pg.get_info().pgid;
	reply->map_epoch = m->map_epoch;
	reply->min_epoch = m->min_epoch;
	reply->set_pulls({std::move(response)});
	return shard_services.send_to_osd(m->from.osd, std::move(reply), pg.get_osdmap_epoch());
      }
    });
  });
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::_handle_push(
  pg_shard_t from,
  PushOp &push_op,
  PushReplyOp *response,
  ceph::os::Transaction *t)
{
  logger().debug("{}", __func__);

  bool first = push_op.before_progress.first;
  interval_set<uint64_t> data_zeros;
  {
    uint64_t offset = push_op.before_progress.data_recovered_to;
    uint64_t length = (push_op.after_progress.data_recovered_to -
                       push_op.before_progress.data_recovered_to);
    if (length) {
      data_zeros.insert(offset, length);
    }
  }
  bool complete = (push_op.after_progress.data_complete &&
		   push_op.after_progress.omap_complete);
  bool clear_omap = !push_op.before_progress.omap_complete;
  response->soid = push_op.recovery_info.soid;

  return submit_push_data(push_op.recovery_info, first, complete, clear_omap,
                          std::move(data_zeros),
                          std::move(push_op.data_included),
                          std::move(push_op.data),
                          std::move(push_op.omap_header),
                          push_op.attrset, 
                          std::move(push_op.omap_entries), t)
  .then_interruptible(
    [this, complete, &push_op, t] {
    if (complete) {
      return pg.get_recovery_handler()->on_local_recover(
        push_op.recovery_info.soid, push_op.recovery_info,
        false, *t);
    }
    return RecoveryBackend::interruptor::now();
  });
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::handle_push(
  Ref<MOSDPGPush> m)
{
  if (pg.can_discard_replica_op(*m)) {
    logger().debug("{}: discarding {}", __func__, *m);
    return seastar::now();
  }
  if (pg.is_primary()) {
    return handle_pull_response(m);
  }

  logger().debug("{}: {}", __func__, *m);
  return seastar::do_with(PushReplyOp(), [this, m](auto& response) {
    PushOp& push_op = m->pushes[0]; // TODO: only one push per message for now
    return seastar::do_with(ceph::os::Transaction(),
      [this, m, &push_op, &response](auto& t) {
      return _handle_push(m->from, push_op, &response, &t).then_interruptible(
	[this, &t] {
	epoch_t epoch_frozen = pg.get_osdmap_epoch();
	logger().debug("ReplicatedRecoveryBackend::handle_push: do_transaction...");
	return interruptor::make_interruptible(
	    shard_services.get_store().do_transaction(coll, std::move(t))).then_interruptible(
	  [this, epoch_frozen, last_complete = pg.get_info().last_complete] {
	  //TODO: this should be grouped with pg.on_local_recover somehow.
	  pg.get_recovery_handler()->_committed_pushed_object(epoch_frozen, last_complete);
	});
      });
    }).then_interruptible([this, m, &response]() mutable {
      auto reply = crimson::make_message<MOSDPGPushReply>();
      reply->from = pg.get_pg_whoami();
      reply->set_priority(m->get_priority());
      reply->pgid = pg.get_info().pgid;
      reply->map_epoch = m->map_epoch;
      reply->min_epoch = m->min_epoch;
      std::vector<PushReplyOp> replies = { std::move(response) };
      reply->replies.swap(replies);
      return shard_services.send_to_osd(m->from.osd,
	  std::move(reply), pg.get_osdmap_epoch());
    });
  });
}

RecoveryBackend::interruptible_future<std::optional<PushOp>>
ReplicatedRecoveryBackend::_handle_push_reply(
  pg_shard_t peer,
  const PushReplyOp &op)
{
  const hobject_t& soid = op.soid;
  logger().debug("{}, soid {}, from {}", __func__, soid, peer);
  auto recovering_iter = recovering.find(soid);
  if (recovering_iter == recovering.end()
      || !recovering_iter->second->pushing.count(peer)) {
    logger().debug("huh, i wasn't pushing {} to osd.{}", soid, peer);
    return seastar::make_ready_future<std::optional<PushOp>>();
  } else {
    auto& push_info = recovering_iter->second->pushing[peer];
    bool error = push_info.recovery_progress.error;
    if (!push_info.recovery_progress.data_complete && !error) {
      return build_push_op(push_info.recovery_info, push_info.recovery_progress,
			   &push_info.stat
      ).then_interruptible([&push_info] (auto push_op) {
        push_info.recovery_progress = push_op.after_progress;
	return seastar::make_ready_future<std::optional<PushOp>>(
          std::move(push_op));
      }).handle_exception_interruptible(
        [recovering_iter, &push_info, peer] (auto e) {
        push_info.recovery_progress.error = true;
        recovering_iter->second->set_push_failed(peer, e);
        return seastar::make_ready_future<std::optional<PushOp>>();
      });
    }
    if (!error) {
      pg.get_recovery_handler()->on_peer_recover(peer,
                                                 soid,
                                                 push_info.recovery_info);
    }
    recovering_iter->second->set_pushed(peer);
    return seastar::make_ready_future<std::optional<PushOp>>();
  }
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::handle_push_reply(
  Ref<MOSDPGPushReply> m)
{
  logger().debug("{}: {}", __func__, *m);
  auto from = m->from;
  auto& push_reply = m->replies[0]; //TODO: only one reply per message

  return _handle_push_reply(from, push_reply).then_interruptible(
    [this, from](std::optional<PushOp> push_op) {
    if (push_op) {
      auto msg = crimson::make_message<MOSDPGPush>();
      msg->from = pg.get_pg_whoami();
      msg->pgid = pg.get_pgid();
      msg->map_epoch = pg.get_osdmap_epoch();
      msg->min_epoch = pg.get_last_peering_reset();
      msg->set_priority(pg.get_recovery_op_priority());
      msg->pushes.push_back(std::move(*push_op));
      return shard_services.send_to_osd(from.osd,
                                        std::move(msg),
                                        pg.get_osdmap_epoch());
    } else {
      return seastar::make_ready_future<>();
    }
  });
}

std::pair<interval_set<uint64_t>,
	  bufferlist>
ReplicatedRecoveryBackend::trim_pushed_data(
  const interval_set<uint64_t> &copy_subset,
  const interval_set<uint64_t> &intervals_received,
  ceph::bufferlist data_received)
{
  logger().debug("{}", __func__);
  // what i have is only a subset of what i want
  if (intervals_received.subset_of(copy_subset)) {
    return {intervals_received, data_received};
  }
  // only collect the extents included by copy_subset and intervals_received
  interval_set<uint64_t> intervals_usable;
  bufferlist data_usable;
  intervals_usable.intersection_of(copy_subset, intervals_received);
  uint64_t have_off = 0;
  for (auto [have_start, have_len] : intervals_received) {
    interval_set<uint64_t> want;
    want.insert(have_start, have_len);
    want.intersection_of(copy_subset);
    for (auto [want_start, want_len] : want) {
      bufferlist sub;
      uint64_t data_off = have_off + (want_start - have_start);
      sub.substr_of(data_received, data_off, want_len);
      data_usable.claim_append(sub);
    }
    have_off += have_len;
  }
  return {intervals_usable, data_usable};
}

RecoveryBackend::interruptible_future<hobject_t>
ReplicatedRecoveryBackend::prep_push_target(
  const ObjectRecoveryInfo& recovery_info,
  bool first,
  bool complete,
  bool clear_omap,
  ObjectStore::Transaction* t,
  const map<string, bufferlist, less<>>& attrs,
  bufferlist&& omap_header)
{
  if (!first) {
    return seastar::make_ready_future<hobject_t>(
      get_temp_recovery_object(recovery_info.soid,
                               recovery_info.version));
  }

  ghobject_t target_oid;
  if (complete) {
    // overwrite the original object
    target_oid = ghobject_t(recovery_info.soid);
  } else {
    target_oid = ghobject_t(get_temp_recovery_object(recovery_info.soid,
                                                     recovery_info.version));
    logger().debug("{}: Adding oid {} in the temp collection",
                   __func__, target_oid);
    add_temp_obj(target_oid.hobj);
  }
  // create a new object
  if (!complete || !recovery_info.object_exist) {
    t->remove(coll->get_cid(), target_oid);
    t->touch(coll->get_cid(), target_oid);
    object_info_t oi;
    oi.decode_no_oid(attrs.at(OI_ATTR));
    t->set_alloc_hint(coll->get_cid(), target_oid,
                      oi.expected_object_size,
                      oi.expected_write_size,
                      oi.alloc_hint_flags);
  }
  if (complete) {
    // remove xattr and update later if overwrite on original object
    t->rmattrs(coll->get_cid(), target_oid);
    // if need update omap, clear the previous content first
    if (clear_omap) {
      t->omap_clear(coll->get_cid(), target_oid);
    }
  }
  t->truncate(coll->get_cid(), target_oid, recovery_info.size);
  if (omap_header.length()) {
    t->omap_setheader(coll->get_cid(), target_oid, omap_header);
  }
  if (complete || !recovery_info.object_exist) {
    return seastar::make_ready_future<hobject_t>(target_oid.hobj);
  }
  // clone overlap content in local object if using a new object
  return interruptor::make_interruptible(store->stat(coll, ghobject_t(recovery_info.soid)))
  .then_interruptible(
    [this, &recovery_info, t, target_oid] (auto st) {
    // TODO: pg num bytes counting
    uint64_t local_size = std::min(recovery_info.size, (uint64_t)st.st_size);
    interval_set<uint64_t> local_intervals_included, local_intervals_excluded;
    if (local_size) {
      local_intervals_included.insert(0, local_size);
      local_intervals_excluded.intersection_of(local_intervals_included, recovery_info.copy_subset);
      local_intervals_included.subtract(local_intervals_excluded);
    }
    for (auto [off, len] : local_intervals_included) {
      logger().debug(" clone_range {} {}~{}",
                     recovery_info.soid, off, len);
      t->clone_range(coll->get_cid(), ghobject_t(recovery_info.soid),
                     target_oid, off, len, off);
    }
    return seastar::make_ready_future<hobject_t>(target_oid.hobj);
  });
}
RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::submit_push_data(
  const ObjectRecoveryInfo &recovery_info,
  bool first,
  bool complete,
  bool clear_omap,
  interval_set<uint64_t>&& data_zeros,
  interval_set<uint64_t>&& intervals_included,
  bufferlist&& data_included,
  bufferlist&& omap_header,
  const map<string, bufferlist, less<>> &attrs,
  map<string, bufferlist>&& omap_entries,
  ObjectStore::Transaction *t)
{
  logger().debug("{}", __func__);
  return prep_push_target(recovery_info, first, complete,
                          clear_omap, t, attrs,
                          std::move(omap_header)).then_interruptible(
    [this,
     &recovery_info, t,
     first, complete,
     data_zeros=std::move(data_zeros),
     intervals_included=std::move(intervals_included),
     data_included=std::move(data_included),
     omap_entries=std::move(omap_entries),
     &attrs](auto target_oid) mutable {

    uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL;
    // Punch zeros for data, if fiemap indicates nothing but it is marked dirty
    if (!data_zeros.empty()) {
      data_zeros.intersection_of(recovery_info.copy_subset);
      assert(intervals_included.subset_of(data_zeros));
      data_zeros.subtract(intervals_included);

      logger().debug("submit_push_data recovering object {} copy_subset: {} "
	  "intervals_included: {} data_zeros: {}",
	  recovery_info.soid, recovery_info.copy_subset,
	  intervals_included, data_zeros);

      for (auto [start, len] : data_zeros) {
        t->zero(coll->get_cid(), ghobject_t(target_oid), start, len);
      }
    }
    uint64_t off = 0;
    for (auto [start, len] : intervals_included) {
      bufferlist bit;
      bit.substr_of(data_included, off, len);
      t->write(coll->get_cid(), ghobject_t(target_oid),
	       start, len, bit, fadvise_flags);
      off += len;
    }

    if (!omap_entries.empty())
      t->omap_setkeys(coll->get_cid(), ghobject_t(target_oid), omap_entries);
    if (!attrs.empty())
      t->setattrs(coll->get_cid(), ghobject_t(target_oid), attrs);

    if (complete) {
      if (!first) {
	logger().debug("submit_push_data: Removing oid {} from the temp collection",
	  target_oid);
	clear_temp_obj(target_oid);
	t->remove(coll->get_cid(), ghobject_t(recovery_info.soid));
	t->collection_move_rename(coll->get_cid(), ghobject_t(target_oid),
				  coll->get_cid(), ghobject_t(recovery_info.soid));
      }
      submit_push_complete(recovery_info, t);
    }
    logger().debug("submit_push_data: done");
    return seastar::make_ready_future<>();
  });
}

void ReplicatedRecoveryBackend::submit_push_complete(
  const ObjectRecoveryInfo &recovery_info,
  ObjectStore::Transaction *t)
{
  for (const auto& [oid, extents] : recovery_info.clone_subset) {
    for (const auto& [off, len] : extents) {
      logger().debug(" clone_range {} {}~{}", oid, off, len);
      t->clone_range(coll->get_cid(), ghobject_t(oid), ghobject_t(recovery_info.soid),
                     off, len, off);
    }
  }
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::handle_recovery_delete_reply(
  Ref<MOSDPGRecoveryDeleteReply> m)
{
  auto& p = m->objects.front();
  hobject_t soid = p.first;
  ObjectRecoveryInfo recovery_info;
  recovery_info.version = p.second;
  pg.get_recovery_handler()->on_peer_recover(m->from, soid, recovery_info);
  get_recovering(soid).set_pushed(m->from);
  return seastar::now();
}

RecoveryBackend::interruptible_future<>
ReplicatedRecoveryBackend::handle_recovery_op(
  Ref<MOSDFastDispatchOp> m,
  crimson::net::ConnectionXcoreRef conn)
{
  switch (m->get_header().type) {
  case MSG_OSD_PG_PULL:
    return handle_pull(boost::static_pointer_cast<MOSDPGPull>(m));
  case MSG_OSD_PG_PUSH:
    return handle_push(boost::static_pointer_cast<MOSDPGPush>(m));
  case MSG_OSD_PG_PUSH_REPLY:
    return handle_push_reply(
	boost::static_pointer_cast<MOSDPGPushReply>(m));
  case MSG_OSD_PG_RECOVERY_DELETE:
    return handle_recovery_delete(
	boost::static_pointer_cast<MOSDPGRecoveryDelete>(m));
  case MSG_OSD_PG_RECOVERY_DELETE_REPLY:
    return handle_recovery_delete_reply(
	boost::static_pointer_cast<MOSDPGRecoveryDeleteReply>(m));
  default:
    // delegate to parent class for handling backend-agnostic recovery ops.
    return RecoveryBackend::handle_recovery_op(std::move(m), conn);
  }
}

