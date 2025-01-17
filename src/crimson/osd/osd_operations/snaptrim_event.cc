// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/coroutine.h"
#include "crimson/osd/osd_operations/snaptrim_event.h"
#include "crimson/osd/ops_executer.h"
#include "crimson/osd/pg.h"
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson {
  template <>
  struct EventBackendRegistry<osd::SnapTrimEvent> {
    static std::tuple<> get_backends() {
      return {};
    }
  };

  template <>
  struct EventBackendRegistry<osd::SnapTrimObjSubEvent> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
}

namespace crimson::osd {

PG::interruptible_future<>
PG::BackgroundProcessLock::lock_with_op(SnapTrimEvent &st_event) noexcept
{
  return st_event.enter_stage<interruptor>(wait
  ).then_interruptible([this] {
    return mutex.lock();
  });
}

void SnapTrimEvent::print(std::ostream &lhs) const
{
  lhs << "SnapTrimEvent("
      << "pgid=" << pg->get_pgid()
      << " snapid=" << snapid
      << " needs_pause=" << needs_pause
      << ")";
}

void SnapTrimEvent::dump_detail(Formatter *f) const
{
  f->open_object_section("SnapTrimEvent");
  f->dump_stream("pgid") << pg->get_pgid();
  f->close_section();
}

CommonPGPipeline& SnapTrimEvent::client_pp()
{
  return pg->request_pg_pipeline;
}

SnapTrimEvent::snap_trim_event_ret_t
SnapTrimEvent::start()
{
  ceph_assert(pg->is_active_clean());

  /* TODO: add a way to expose progress via the optracker without misusing
   * pipeline stages. https://tracker.ceph.com/issues/66473 */
  ShardServices &shard_services = pg->get_shard_services();
  {
    co_await pg->background_process_lock.lock_with_op(*this);
    auto unlocker = seastar::defer([this] {
      pg->background_process_lock.unlock();
    });

    auto to_trim_fut = interruptor::async([this] {
      using crimson::common::local_conf;
      const auto max =
	local_conf().get_val<uint64_t>("osd_pg_max_concurrent_snap_trims");
      // we need to look for at least 1 snaptrim, otherwise we'll misinterpret
      // the nullopt below and erase snapid.
      auto to_trim = snap_mapper.get_next_objects_to_trim(
	snapid,
	max);
      if (!to_trim.has_value()) {
	return std::vector<hobject_t>{};
      }
      logger().debug("{}: async almost done line {}", *this, __LINE__);
      return std::move(*to_trim);
    });
    auto to_trim = co_await std::move(to_trim_fut);

    if (to_trim.empty()) {
      // the legit ENOENT -> done
      logger().debug("{}: to_trim is empty! Stopping iteration", *this);
      co_return seastar::stop_iteration::yes;
    }
    for (const auto& object : to_trim) {
      logger().debug("{}: trimming {}", *this, object);
      subop_blocker.emplace_back(
	shard_services.start_operation_may_interrupt<
	interruptor, SnapTrimObjSubEvent>(
	  pg,
	  object,
	  snapid));
    }

    logger().debug("{}: awaiting completion", *this);
    co_await subop_blocker.interruptible_wait_completion();
  }

  if (needs_pause) {
    using crimson::common::local_conf;
    const auto time_to_sleep =
      local_conf().template get_val<double>("osd_snap_trim_sleep");
    logger().debug("{}: time_to_sleep {}", *this, time_to_sleep);
    // TODO: this logic should be more sophisticated and distinguish
    // between SSDs, HDDs and the hybrid case
    co_await interruptor::make_interruptible(
      seastar::sleep(
	std::chrono::milliseconds(std::lround(time_to_sleep * 1000))));
  }

  logger().debug("{}: all completed", *this);
  co_return seastar::stop_iteration::no;
}


CommonPGPipeline& SnapTrimObjSubEvent::client_pp()
{
  return pg->request_pg_pipeline;
}

SnapTrimObjSubEvent::snap_trim_obj_subevent_ret_t
SnapTrimObjSubEvent::remove_clone(
  ObjectContextRef obc,
  ObjectContextRef head_obc,
  ceph::os::Transaction& txn
) {
  const auto p = std::find(
    head_obc->ssc->snapset.clones.begin(),
    head_obc->ssc->snapset.clones.end(),
    coid.snap);
  if (p == head_obc->ssc->snapset.clones.end()) {
    logger().error("{}: Snap {} not in clones",
                   *this, coid.snap);
    return crimson::ct_error::enoent::make();
  }
  assert(p != head_obc->ssc->snapset.clones.end());
  snapid_t last = coid.snap;
  delta_stats.num_bytes -= head_obc->ssc->snapset.get_clone_bytes(last);

  if (p != head_obc->ssc->snapset.clones.begin()) {
    // not the oldest... merge overlap into next older clone
    std::vector<snapid_t>::iterator n = p - 1;
    hobject_t prev_coid = coid;
    prev_coid.snap = *n;

    // does the classical OSD really need is_present_clone(prev_coid)?
    delta_stats.num_bytes -= head_obc->ssc->snapset.get_clone_bytes(*n);
    head_obc->ssc->snapset.clone_overlap[*n].intersection_of(
      head_obc->ssc->snapset.clone_overlap[*p]);
    delta_stats.num_bytes += head_obc->ssc->snapset.get_clone_bytes(*n);
  }
  delta_stats.num_objects--;
  if (obc->obs.oi.is_dirty()) {
    delta_stats.num_objects_dirty--;
  }
  if (obc->obs.oi.is_omap()) {
    delta_stats.num_objects_omap--;
  }
  if (obc->obs.oi.is_whiteout()) {
    logger().debug("{}: trimming whiteout on {}",
                   *this, coid);
    delta_stats.num_whiteouts--;
  }
  delta_stats.num_object_clones--;

  obc->obs.exists = false;
  head_obc->ssc->snapset.clones.erase(p);
  head_obc->ssc->snapset.clone_overlap.erase(last);
  head_obc->ssc->snapset.clone_size.erase(last);
  head_obc->ssc->snapset.clone_snaps.erase(last);

  add_log_entry(
    pg_log_entry_t::DELETE,
    coid,
    obc->obs.oi.version,
    0,
    osd_reqid_t(),
    obc->obs.oi.mtime, // will be replaced in `apply_to()`
    0);
  txn.remove(
    pg->get_collection_ref()->get_cid(),
    ghobject_t{coid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD});
  obc->obs.oi = object_info_t(coid);
  return interruptor::now();
}

void SnapTrimObjSubEvent::remove_head_whiteout(
  ObjectContextRef obc,
  ObjectContextRef head_obc,
  ceph::os::Transaction& txn
) {
  // NOTE: this arguably constitutes minor interference with the
  // tiering agent if this is a cache tier since a snap trim event
  // is effectively evicting a whiteout we might otherwise want to
  // keep around.
  const auto head_oid = coid.get_head();
  logger().info("{}: {} removing {}",
                *this, coid, head_oid);
  add_log_entry(
    pg_log_entry_t::DELETE,
    head_oid,
    head_obc->obs.oi.version,
    0,
    osd_reqid_t(),
    obc->obs.oi.mtime, // will be replaced in `apply_to()`
    0);
  logger().info("{}: remove snap head", *this);
  object_info_t& oi = head_obc->obs.oi;
  delta_stats.num_objects--;
  if (oi.is_dirty()) {
    delta_stats.num_objects_dirty--;
  }
  if (oi.is_omap()) {
    delta_stats.num_objects_omap--;
  }
  if (oi.is_whiteout()) {
    logger().debug("{}: trimming whiteout on {}",
                   *this, oi.soid);
    delta_stats.num_whiteouts--;
  }
  head_obc->obs.exists = false;
  head_obc->obs.oi = object_info_t(head_oid);
  txn.remove(pg->get_collection_ref()->get_cid(),
             ghobject_t{head_oid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD});
}

SnapTrimObjSubEvent::interruptible_future<>
SnapTrimObjSubEvent::adjust_snaps(
  ObjectContextRef obc,
  ObjectContextRef head_obc,
  const std::set<snapid_t>& new_snaps,
  ceph::os::Transaction& txn
) {
  head_obc->ssc->snapset.clone_snaps[coid.snap] =
    std::vector<snapid_t>(new_snaps.rbegin(), new_snaps.rend());

  // we still do a 'modify' event on this object just to trigger a
  // snapmapper.update ... :(
  obc->obs.oi.prior_version = obc->obs.oi.version;
  obc->obs.oi.version = osd_op_p.at_version;
  ceph::bufferlist bl;
  obc->obs.oi.encode_no_oid(
    bl,
    pg->get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
  txn.setattr(
    pg->get_collection_ref()->get_cid(),
    ghobject_t{coid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD},
    OI_ATTR,
    bl);
  auto &loge = add_log_entry(
    pg_log_entry_t::MODIFY,
    coid,
    obc->obs.oi.prior_version,
    0,
    osd_reqid_t(),
    obc->obs.oi.mtime,
    0);
  bufferlist snapsbl;
  encode(new_snaps, snapsbl);
  loge.snaps.swap(snapsbl);
  return interruptor::now();
}

void SnapTrimObjSubEvent::update_head(
  ObjectContextRef obc,
  ObjectContextRef head_obc,
  ceph::os::Transaction& txn
) {
  const auto head_oid = coid.get_head();
  logger().info("{}: writing updated snapset on {}, snapset is {}",
                *this, head_oid, head_obc->ssc->snapset);
  add_log_entry(
    pg_log_entry_t::MODIFY,
    head_oid,
    head_obc->obs.oi.version,
    0,
    osd_reqid_t(),
    obc->obs.oi.mtime,
    0);

  head_obc->obs.oi.prior_version = head_obc->obs.oi.version;
  head_obc->obs.oi.version = osd_op_p.at_version;

  std::map<std::string, ceph::bufferlist, std::less<>> attrs;
  ceph::bufferlist bl;
  encode(head_obc->ssc->snapset, bl);
  attrs[SS_ATTR] = std::move(bl);

  bl.clear();
  head_obc->obs.oi.encode_no_oid(bl,
    pg->get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
  attrs[OI_ATTR] = std::move(bl);
  txn.setattrs(
    pg->get_collection_ref()->get_cid(),
    ghobject_t{head_oid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD},
    attrs);
}

SnapTrimObjSubEvent::remove_or_update_iertr::future<ceph::os::Transaction>
SnapTrimObjSubEvent::remove_or_update(
  ObjectContextRef obc,
  ObjectContextRef head_obc)
{
  auto citer = head_obc->ssc->snapset.clone_snaps.find(coid.snap);
  if (citer == head_obc->ssc->snapset.clone_snaps.end()) {
    logger().error("{}: No clone_snaps in snapset {} for object {}",
                   *this, head_obc->ssc->snapset, coid);
    return crimson::ct_error::enoent::make();
  }
  const auto& old_snaps = citer->second;
  if (old_snaps.empty()) {
    logger().error("{}: no object info snaps for object {}",
                   *this, coid);
    return crimson::ct_error::enoent::make();
  }
  if (head_obc->ssc->snapset.seq == 0) {
    logger().error("{}: no snapset.seq for object {}",
                   *this, coid);
    return crimson::ct_error::enoent::make();
  }
  const OSDMapRef& osdmap = pg->get_osdmap();
  std::set<snapid_t> new_snaps;
  for (const auto& old_snap : old_snaps) {
    if (!osdmap->in_removed_snaps_queue(pg->get_info().pgid.pgid.pool(),
                                        old_snap)
        && old_snap != snap_to_trim) {
      new_snaps.insert(old_snap);
    }
  }

  return seastar::do_with(ceph::os::Transaction{}, [=, this](auto &txn) {
    osd_op_p.at_version = pg->get_next_version();
    auto ret = remove_or_update_iertr::now();
    if (new_snaps.empty()) {
      // remove clone from snapset
      logger().info("{}: {} snaps {} -> {} ... deleting",
		    *this, coid, old_snaps, new_snaps);
      ret = remove_clone(obc, head_obc, txn);
    } else {
      // save adjusted snaps for this object
      logger().info("{}: {} snaps {} -> {}",
		    *this, coid, old_snaps, new_snaps);
      ret = adjust_snaps(obc, head_obc, new_snaps, txn);
    }
    return std::move(ret).si_then(
      [&txn, obc, head_obc=std::move(head_obc), this]() mutable {
      // save head snapset
      logger().debug("{}: {} new snapset {} on {}",
		     *this, coid, head_obc->ssc->snapset, head_obc->obs.oi);
      osd_op_p.at_version.version++;
      if (head_obc->ssc->snapset.clones.empty() && head_obc->obs.oi.is_whiteout()) {
	remove_head_whiteout(obc, head_obc, txn);
      } else {
	update_head(obc, head_obc, txn);
      }
      // Stats reporting - Set number of objects trimmed
      if (delta_stats.num_objects < 0) {
        int64_t num_objects_trimmed = std::abs(delta_stats.num_objects);
        pg->get_peering_state().update_stats_wo_resched(
          [num_objects_trimmed](auto &history, auto &stats) {
          stats.objects_trimmed += num_objects_trimmed;
        });
      }
      pg->apply_stats(coid, delta_stats);
    }).si_then(
      [&txn] () mutable {
      return std::move(txn);
    });
  });
}

SnapTrimObjSubEvent::snap_trim_obj_subevent_ret_t
SnapTrimObjSubEvent::start()
{
  ceph_assert(pg->is_active_clean());

  auto exit_handle = seastar::defer([this] {
    logger().debug("{}: exit", *this);
    handle.exit();
  });

  co_await enter_stage<interruptor>(
    client_pp().check_already_complete_get_obc);

  logger().debug("{}: getting obc for {}", *this, coid);
  // end of commonality
  // lock both clone's and head's obcs
  co_await pg->obc_loader.with_obc<RWState::RWWRITE>(
    coid,
    std::bind(&SnapTrimObjSubEvent::process_and_submit,
              this, std::placeholders::_1, std::placeholders::_2),
    false
  ).handle_error_interruptible(
    remove_or_update_iertr::pass_further{},
    crimson::ct_error::assert_all{"unexpected error in SnapTrimObjSubEvent"}
  );

  logger().debug("{}: completed", *this);
  co_await interruptor::make_interruptible(handle.complete());
}

ObjectContextLoader::load_obc_iertr::future<>
SnapTrimObjSubEvent::process_and_submit(ObjectContextRef head_obc,
                                        ObjectContextRef clone_obc) {
  logger().debug("{}: got clone_obc={}", *this, clone_obc->get_oid());

  co_await enter_stage<interruptor>(client_pp().process);

  logger().debug("{}: processing clone_obc={}", *this, clone_obc->get_oid());

  auto txn = co_await remove_or_update(clone_obc, head_obc);

  auto [submitted, all_completed] = co_await pg->submit_transaction(
	  std::move(clone_obc),
	  std::move(txn),
	  std::move(osd_op_p),
	  std::move(log_entries)
  );

  co_await std::move(submitted);

  co_await enter_stage<interruptor>(client_pp().wait_repop);

  co_await std::move(all_completed);

  co_return;
}

void SnapTrimObjSubEvent::print(std::ostream &lhs) const
{
  lhs << "SnapTrimObjSubEvent("
      << "coid=" << coid
      << " snapid=" << snap_to_trim
      << ")";
}

void SnapTrimObjSubEvent::dump_detail(Formatter *f) const
{
  f->open_object_section("SnapTrimObjSubEvent");
  f->dump_stream("coid") << coid;
  f->close_section();
}

} // namespace crimson::osd
