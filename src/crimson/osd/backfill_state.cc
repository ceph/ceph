// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <boost/type_index.hpp>
#include <fmt/ranges.h>
#include "common/hobject.h"
#include "crimson/osd/backfill_state.h"
#include "osd/osd_types_fmt.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

BackfillState::BackfillState(
  BackfillState::BackfillListener& backfill_listener,
  std::unique_ptr<BackfillState::PeeringFacade> peering_state,
  std::unique_ptr<BackfillState::PGFacade> pg)
  : backfill_machine(*this,
                     backfill_listener,
                     std::move(peering_state),
                     std::move(pg)),
    progress_tracker(
      std::make_unique<BackfillState::ProgressTracker>(backfill_machine))
{
  logger().debug("{}:{}", __func__, __LINE__);
  backfill_machine.initiate();
}

template <class S>
BackfillState::StateHelper<S>::StateHelper()
{
  logger().debug("enter {}",
		 boost::typeindex::type_id<S>().pretty_name());
}

template <class S>
BackfillState::StateHelper<S>::~StateHelper()
{
  logger().debug("exit {}",
		 boost::typeindex::type_id<S>().pretty_name());
}

BackfillState::~BackfillState() = default;

BackfillState::BackfillMachine::BackfillMachine(
  BackfillState& backfill_state,
  BackfillState::BackfillListener& backfill_listener,
  std::unique_ptr<BackfillState::PeeringFacade> peering_state,
  std::unique_ptr<BackfillState::PGFacade> pg)
  : backfill_state(backfill_state),
    backfill_listener(backfill_listener),
    peering_state(std::move(peering_state)),
    pg(std::move(pg))
{}

BackfillState::BackfillMachine::~BackfillMachine() = default;

BackfillState::Initial::Initial(my_context ctx)
  : my_base(ctx)
{
  backfill_state().last_backfill_started = peering_state().earliest_backfill();
  logger().debug("{}: bft={} from {}",
                 __func__, peering_state().get_backfill_targets(),
                 backfill_state().last_backfill_started);
  for (const auto& bt : peering_state().get_backfill_targets()) {
    logger().debug("{}: target shard {} from {}",
                   __func__, bt, peering_state().get_peer_last_backfill(bt));
  }
  ceph_assert(peering_state().get_backfill_targets().size());
  ceph_assert(!backfill_state().last_backfill_started.is_max());
}

boost::statechart::result
BackfillState::Initial::react(const BackfillState::Triggered& evt)
{
  logger().debug("{}: backfill triggered", __func__);
  ceph_assert(backfill_state().last_backfill_started == \
              peering_state().earliest_backfill());
  ceph_assert(peering_state().is_backfilling());
  // initialize BackfillIntervals
  for (const auto& bt : peering_state().get_backfill_targets()) {
    backfill_state().peer_backfill_info[bt].reset(
      peering_state().get_peer_last_backfill(bt));
  }
  backfill_state().backfill_info.reset(backfill_state().last_backfill_started);
  if (Enqueuing::all_enqueued(peering_state(),
                              backfill_state().backfill_info,
                              backfill_state().peer_backfill_info)) {
    logger().debug("{}: switching to Done state", __func__);
    return transit<BackfillState::Done>();
  } else {
    logger().debug("{}: switching to Enqueuing state", __func__);
    return transit<BackfillState::Enqueuing>();
  }
}


// -- Enqueuing
void BackfillState::Enqueuing::maybe_update_range()
{
  if (auto& primary_bi = backfill_state().backfill_info;
      primary_bi.version >= pg().get_projected_last_update()) {
    logger().info("{}: bi is current", __func__);
    ceph_assert(primary_bi.version == pg().get_projected_last_update());
  } else if (primary_bi.version >= peering_state().get_log_tail()) {
#if 0
    if (peering_state().get_pg_log().get_log().empty() &&
        pg().get_projected_log().empty()) {
      /* Because we don't move log_tail on split, the log might be
       * empty even if log_tail != last_update.  However, the only
       * way to get here with an empty log is if log_tail is actually
       * eversion_t(), because otherwise the entry which changed
       * last_update since the last scan would have to be present.
       */
      ceph_assert(primary_bi.version == eversion_t());
      return;
    }
#endif
    logger().debug("{}: bi is old, ({}) can be updated with log to {}",
                   __func__,
                   primary_bi.version,
                   pg().get_projected_last_update());
    logger().debug("{}: scanning pg log first", __func__);
    peering_state().scan_log_after(primary_bi.version,
      [&](const pg_log_entry_t& e) {
        logger().debug("maybe_update_range(lambda): updating from version {}",
                       e.version);
        if (e.soid >= primary_bi.begin && e.soid <  primary_bi.end) {
	  if (e.is_update()) {
	    logger().debug("maybe_update_range(lambda): {} updated to ver {}",
                           e.soid, e.version);
            primary_bi.objects.erase(e.soid);
            primary_bi.objects.insert(std::make_pair(e.soid,
                                                             e.version));
	  } else if (e.is_delete()) {
            logger().debug("maybe_update_range(lambda): {} removed",
                           e.soid);
            primary_bi.objects.erase(e.soid);
          }
        }
      });
    primary_bi.version = pg().get_projected_last_update();
  } else {
    ceph_abort_msg(
      "scan_range should have raised primary_bi.version past log_tail");
  }
}

void BackfillState::Enqueuing::trim_backfill_infos()
{
  for (const auto& bt : peering_state().get_backfill_targets()) {
    backfill_state().peer_backfill_info[bt].trim_to(
      std::max(peering_state().get_peer_last_backfill(bt),
               backfill_state().last_backfill_started));
  }
  backfill_state().backfill_info.trim_to(
    backfill_state().last_backfill_started);
}

/* static */ bool BackfillState::Enqueuing::all_enqueued(
  const PeeringFacade& peering_state,
  const BackfillInterval& backfill_info,
  const std::map<pg_shard_t, BackfillInterval>& peer_backfill_info)
{
  const bool all_local_enqueued = \
    backfill_info.extends_to_end() && backfill_info.empty();
  const bool all_peer_enqueued = std::all_of(
    std::begin(peer_backfill_info),
    std::end(peer_backfill_info),
    [] (const auto& kv) {
      [[maybe_unused]] const auto& [ shard, peer_backfill_info ] = kv;
      return peer_backfill_info.extends_to_end() && peer_backfill_info.empty();
    });
  return all_local_enqueued && all_peer_enqueued;
}

hobject_t BackfillState::Enqueuing::earliest_peer_backfill(
  const std::map<pg_shard_t, BackfillInterval>& peer_backfill_info) const
{
  hobject_t e = hobject_t::get_max();
  for (const pg_shard_t& bt : peering_state().get_backfill_targets()) {
    const auto iter = peer_backfill_info.find(bt);
    ceph_assert(iter != peer_backfill_info.end());
    e = std::min(e, iter->second.begin);
  }
  return e;
}

bool BackfillState::Enqueuing::should_rescan_replicas(
  const std::map<pg_shard_t, BackfillInterval>& peer_backfill_info,
  const BackfillInterval& backfill_info) const
{
  const auto& targets = peering_state().get_backfill_targets();
  return std::any_of(std::begin(targets), std::end(targets),
    [&] (const auto& bt) {
      return ReplicasScanning::replica_needs_scan(peer_backfill_info.at(bt),
                                                  backfill_info);
    });
}

bool BackfillState::Enqueuing::should_rescan_primary(
  const std::map<pg_shard_t, BackfillInterval>& peer_backfill_info,
  const BackfillInterval& backfill_info) const
{
  return backfill_info.begin <= earliest_peer_backfill(peer_backfill_info) &&
	 !backfill_info.extends_to_end();
}

void BackfillState::Enqueuing::trim_backfilled_object_from_intervals(
  BackfillState::Enqueuing::result_t&& result,
  hobject_t& last_backfill_started,
  std::map<pg_shard_t, BackfillInterval>& peer_backfill_info)
{
  std::for_each(std::begin(result.pbi_targets), std::end(result.pbi_targets),
    [&peer_backfill_info] (const auto& bt) {
      peer_backfill_info.at(bt).pop_front();
    });
  last_backfill_started = std::move(result.new_last_backfill_started);
}

BackfillState::Enqueuing::result_t
BackfillState::Enqueuing::remove_on_peers(const hobject_t& check)
{
  // set `new_last_backfill_started` to `check`
  result_t result { {}, check };
  for (const auto& bt : peering_state().get_backfill_targets()) {
    const auto& pbi = backfill_state().peer_backfill_info.at(bt);
    if (pbi.begin == check) {
      result.pbi_targets.insert(bt);
      const auto& version = pbi.objects.begin()->second;
      backfill_state().progress_tracker->enqueue_drop(pbi.begin);
      backfill_listener().enqueue_drop(bt, pbi.begin, version);
    }
  }
  logger().debug("{}: BACKFILL removing {} from peers {}",
                 __func__, check, result.pbi_targets);
  ceph_assert(!result.pbi_targets.empty());
  return result;
}

BackfillState::Enqueuing::result_t
BackfillState::Enqueuing::update_on_peers(const hobject_t& check)
{
  logger().debug("{}: check={}", __func__, check);
  const auto& primary_bi = backfill_state().backfill_info;
  result_t result { {}, primary_bi.begin };

  for (const auto& bt : peering_state().get_backfill_targets()) {
    const auto& peer_bi = backfill_state().peer_backfill_info.at(bt);

    // Find all check peers that have the wrong version
    if (const eversion_t& obj_v = primary_bi.objects.begin()->second;
        check == primary_bi.begin && check == peer_bi.begin) {
      if(peer_bi.objects.begin()->second != obj_v &&
          backfill_state().progress_tracker->enqueue_push(primary_bi.begin)) {
        backfill_listener().enqueue_push(primary_bi.begin, obj_v);
      } else {
        // it's fine, keep it! OR already recovering
      }
      result.pbi_targets.insert(bt);
    } else {
      // Only include peers that we've caught up to their backfill line
      // otherwise, they only appear to be missing this object
      // because their peer_bi.begin > backfill_info.begin.
      if (primary_bi.begin > peering_state().get_peer_last_backfill(bt) &&
          backfill_state().progress_tracker->enqueue_push(primary_bi.begin)) {
        backfill_listener().enqueue_push(primary_bi.begin, obj_v);
      }
    }
  }
  return result;
}

bool BackfillState::Enqueuing::Enqueuing::all_emptied(
  const BackfillInterval& local_backfill_info,
  const std::map<pg_shard_t, BackfillInterval>& peer_backfill_info) const
{
  const auto& targets = peering_state().get_backfill_targets();
  const auto replicas_emptied =
    std::all_of(std::begin(targets), std::end(targets),
      [&] (const auto& bt) {
        return peer_backfill_info.at(bt).empty();
      });
  return local_backfill_info.empty() && replicas_emptied;
}

BackfillState::Enqueuing::Enqueuing(my_context ctx)
  : my_base(ctx)
{
  auto& primary_bi = backfill_state().backfill_info;

  // update our local interval to cope with recent changes
  primary_bi.begin = backfill_state().last_backfill_started;
  if (primary_bi.version < peering_state().get_log_tail()) {
    // it might be that the OSD is so flooded with modifying operations
    // that backfill will be spinning here over and over. For the sake
    // of performance and complexity we don't synchronize with entire PG.
    // similar can happen in classical OSD.
    logger().warn("{}: bi is old, rescanning of local backfill_info",
                  __func__);
    post_event(RequestPrimaryScanning{});
    return;
  } else {
    maybe_update_range();
  }
  trim_backfill_infos();

  while (!all_emptied(primary_bi, backfill_state().peer_backfill_info)) {
    if (!backfill_listener().budget_available()) {
      post_event(RequestWaiting{});
      return;
    } else if (should_rescan_replicas(backfill_state().peer_backfill_info,
                                      primary_bi)) {
      // Count simultaneous scans as a single op and let those complete
      post_event(RequestReplicasScanning{});
      return;
    }
    // Get object within set of peers to operate on and the set of targets
    // for which that object applies.
    if (const hobject_t check = \
          earliest_peer_backfill(backfill_state().peer_backfill_info);
        check < primary_bi.begin) {
      // Don't increment ops here because deletions
      // are cheap and not replied to unlike real recovery_ops,
      // and we can't increment ops without requeueing ourself
      // for recovery.
      auto result = remove_on_peers(check);
      trim_backfilled_object_from_intervals(std::move(result),
					    backfill_state().last_backfill_started,
					    backfill_state().peer_backfill_info);
    } else {
      auto result = update_on_peers(check);
      trim_backfilled_object_from_intervals(std::move(result),
					    backfill_state().last_backfill_started,
					    backfill_state().peer_backfill_info);
      primary_bi.pop_front();
    }
    backfill_listener().maybe_flush();
  }

  if (should_rescan_primary(backfill_state().peer_backfill_info,
                            primary_bi)) {
    // need to grab one another chunk of the object namespace and restart
    // the queueing.
    logger().debug("{}: reached end for current local chunk",
                   __func__);
    post_event(RequestPrimaryScanning{});
  } else if (backfill_state().progress_tracker->tracked_objects_completed()) {
    post_event(RequestDone{});
  } else {
    logger().debug("{}: reached end for both local and all peers "
                   "but still has in-flight operations", __func__);
    post_event(RequestWaiting{});
  }
}

// -- PrimaryScanning
BackfillState::PrimaryScanning::PrimaryScanning(my_context ctx)
  : my_base(ctx)
{
  backfill_state().backfill_info.version = peering_state().get_last_update();
  backfill_listener().request_primary_scan(
    backfill_state().backfill_info.begin);
}

boost::statechart::result
BackfillState::PrimaryScanning::react(PrimaryScanned evt)
{
  logger().debug("{}", __func__);
  backfill_state().backfill_info = std::move(evt.result);
  return transit<Enqueuing>();
}

boost::statechart::result
BackfillState::PrimaryScanning::react(ObjectPushed evt)
{
  logger().debug("PrimaryScanning::react() on ObjectPushed; evt.object={}",
                 evt.object);
  backfill_state().progress_tracker->complete_to(evt.object, evt.stat);
  return discard_event();
}

// -- ReplicasScanning
bool BackfillState::ReplicasScanning::replica_needs_scan(
  const BackfillInterval& replica_backfill_info,
  const BackfillInterval& local_backfill_info)
{
  return replica_backfill_info.empty() && \
         replica_backfill_info.begin <= local_backfill_info.begin && \
         !replica_backfill_info.extends_to_end();
}

BackfillState::ReplicasScanning::ReplicasScanning(my_context ctx)
  : my_base(ctx)
{
  for (const auto& bt : peering_state().get_backfill_targets()) {
    if (const auto& pbi = backfill_state().peer_backfill_info.at(bt);
        replica_needs_scan(pbi, backfill_state().backfill_info)) {
      logger().debug("{}: scanning peer osd.{} from {}",
                     __func__, bt, pbi.end);
      backfill_listener().request_replica_scan(bt, pbi.end, hobject_t{});

      ceph_assert(waiting_on_backfill.find(bt) == \
                  waiting_on_backfill.end());
      waiting_on_backfill.insert(bt);
    }
  }
  ceph_assert(!waiting_on_backfill.empty());
  // TODO: start_recovery_op(hobject_t::get_max()); // XXX: was pbi.end
}

#if 0
BackfillState::ReplicasScanning::~ReplicasScanning()
{
  // TODO: finish_recovery_op(hobject_t::get_max());
}
#endif

boost::statechart::result
BackfillState::ReplicasScanning::react(ReplicaScanned evt)
{
  logger().debug("{}: got scan result from osd={}, result={}",
                 __func__, evt.from, evt.result);
  // TODO: maybe we'll be able to move waiting_on_backfill from
  // the machine to the state.
  ceph_assert(peering_state().is_backfill_target(evt.from));
  if (waiting_on_backfill.erase(evt.from)) {
    backfill_state().peer_backfill_info[evt.from] = std::move(evt.result);
    if (waiting_on_backfill.empty()) {
      ceph_assert(backfill_state().peer_backfill_info.size() == \
                  peering_state().get_backfill_targets().size());
      return transit<Enqueuing>();
    }
  } else {
    // we canceled backfill for a while due to a too full, and this
    // is an extra response from a non-too-full peer
    logger().debug("{}: canceled backfill (too full?)", __func__);
  }
  return discard_event();
}

boost::statechart::result
BackfillState::ReplicasScanning::react(ObjectPushed evt)
{
  logger().debug("ReplicasScanning::react() on ObjectPushed; evt.object={}",
                 evt.object);
  backfill_state().progress_tracker->complete_to(evt.object, evt.stat);
  return discard_event();
}


// -- Waiting
BackfillState::Waiting::Waiting(my_context ctx)
  : my_base(ctx)
{
}

boost::statechart::result
BackfillState::Waiting::react(ObjectPushed evt)
{
  logger().debug("Waiting::react() on ObjectPushed; evt.object={}",
                 evt.object);
  backfill_state().progress_tracker->complete_to(evt.object, evt.stat);
  if (!Enqueuing::all_enqueued(peering_state(),
                               backfill_state().backfill_info,
                               backfill_state().peer_backfill_info)) {
    return transit<Enqueuing>();
  } else if (backfill_state().progress_tracker->tracked_objects_completed()) {
    return transit<Done>();
  } else {
    // we still have something to wait on
    logger().debug("Waiting::react() on ObjectPushed; still waiting");
    return discard_event();
  }
}

// -- Done
BackfillState::Done::Done(my_context ctx)
  : my_base(ctx)
{
  logger().info("{}: backfill is done", __func__);
  backfill_listener().backfilled();
}

// -- Crashed
BackfillState::Crashed::Crashed()
{
  ceph_abort_msg("{}: this should not happen");
}

// ProgressTracker is an intermediary between the BackfillListener and
// BackfillMachine + its states. All requests to push or drop an object
// are directed through it. The same happens with notifications about
// completing given operations which are generated by BackfillListener
// and dispatched as i.e. ObjectPushed events.
// This allows ProgressTacker to track the list of in-flight operations
// which is essential to make the decision whether the entire machine
// should switch from Waiting to Done keep in Waiting.
// ProgressTracker also coordinates .last_backfill_started and stats
// updates.
bool BackfillState::ProgressTracker::tracked_objects_completed() const
{
  return registry.empty();
}

bool BackfillState::ProgressTracker::enqueue_push(const hobject_t& obj)
{
  [[maybe_unused]] const auto [it, first_seen] = registry.try_emplace(
    obj, registry_item_t{op_stage_t::enqueued_push, std::nullopt});
  return first_seen;
}

void BackfillState::ProgressTracker::enqueue_drop(const hobject_t& obj)
{
  registry.try_emplace(
    obj, registry_item_t{op_stage_t::enqueued_drop, pg_stat_t{}});
}

void BackfillState::ProgressTracker::complete_to(
  const hobject_t& obj,
  const pg_stat_t& stats)
{
  logger().debug("{}: obj={}",
                 __func__, obj);
  if (auto completion_iter = registry.find(obj);
      completion_iter != std::end(registry)) {
    completion_iter->second = \
      registry_item_t{ op_stage_t::completed_push, stats };
  } else {
    ceph_abort_msg("completing untracked object shall not happen");
  }
  for (auto it = std::begin(registry);
       it != std::end(registry) &&
         it->second.stage != op_stage_t::enqueued_push;
       it = registry.erase(it)) {
    auto& [soid, item] = *it;
    assert(item.stats);
    peering_state().update_complete_backfill_object_stats(
      soid,
      *item.stats);
  }
  if (Enqueuing::all_enqueued(peering_state(),
                              backfill_state().backfill_info,
                              backfill_state().peer_backfill_info) &&
      tracked_objects_completed()) {
    backfill_state().last_backfill_started = hobject_t::get_max();
    backfill_listener().update_peers_last_backfill(hobject_t::get_max());
  } else {
    backfill_listener().update_peers_last_backfill(obj);
  }
}

} // namespace crimson::osd
