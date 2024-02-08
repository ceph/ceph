// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PGPeeringEvent.h"
#include "common/ceph_releases.h"
#include "common/dout.h"
#include "PeeringState.h"

#include "messages/MOSDPGRemove.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"
#include "messages/MOSDScrubReserve.h"
#include "messages/MOSDPGInfo2.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGNotify2.h"
#include "messages/MOSDPGQuery2.h"
#include "messages/MOSDPGLease.h"
#include "messages/MOSDPGLeaseAck.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd

using std::dec;
using std::hex;
using std::make_pair;
using std::map;
using std::ostream;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

using ceph::Formatter;
using ceph::make_message;

BufferedRecoveryMessages::BufferedRecoveryMessages(PeeringCtx &ctx)
  // steal messages from ctx
  : message_map{std::move(ctx.message_map)}
{}

void BufferedRecoveryMessages::send_notify(int to, const pg_notify_t &n)
{
  spg_t pgid(n.info.pgid.pgid, n.to);
  send_osd_message(to, TOPNSPC::make_message<MOSDPGNotify2>(pgid, n));
}

void BufferedRecoveryMessages::send_query(
  int to,
  spg_t to_spgid,
  const pg_query_t &q)
{
  send_osd_message(to, TOPNSPC::make_message<MOSDPGQuery2>(to_spgid, q));
}

void BufferedRecoveryMessages::send_info(
  int to,
  spg_t to_spgid,
  epoch_t min_epoch,
  epoch_t cur_epoch,
  const pg_info_t &info,
  std::optional<pg_lease_t> lease,
  std::optional<pg_lease_ack_t> lease_ack)
{
  send_osd_message(
    to,
    TOPNSPC::make_message<MOSDPGInfo2>(
      to_spgid,
      info,
      cur_epoch,
      min_epoch,
      lease,
      lease_ack)
  );
}

void PGPool::update(OSDMapRef map)
{
  const pg_pool_t *pi = map->get_pg_pool(id);
  if (!pi) {
    return; // pool has been deleted
  }
  info = *pi;
  name = map->get_pool_name(id);

  bool updated = false;
  if ((map->get_epoch() != cached_epoch + 1) ||
      (pi->get_snap_epoch() == map->get_epoch())) {
    updated = true;
  }

  if (info.is_pool_snaps_mode() && updated) {
    snapc = pi->get_snap_context();
  }
  cached_epoch = map->get_epoch();
}

/*-------------Peering State Helpers----------------*/
#undef dout_prefix
#define dout_prefix (dpp->gen_prefix(*_dout)) \
        << "PeeringState::" << __func__ << " "
#undef psdout
#define psdout(x) ldout(cct, x)

PeeringState::PeeringState(
  CephContext *cct,
  pg_shard_t pg_whoami,
  spg_t spgid,
  const PGPool &_pool,
  OSDMapRef curmap,
  DoutPrefixProvider *dpp,
  PeeringListener *pl)
  : state_history(*pl),
    cct(cct),
    spgid(spgid),
    dpp(dpp),
    pl(pl),
    orig_ctx(0),
    osdmap_ref(curmap),
    pool(_pool),
    pg_whoami(pg_whoami),
    info(spgid),
    pg_log(cct),
    last_require_osd_release(curmap->require_osd_release),
    missing_loc(spgid, this, dpp, cct),
    machine(this, cct, spgid, dpp, pl, &state_history)
{
  machine.initiate();
}

void PeeringState::start_handle(PeeringCtx *new_ctx) {
  ceph_assert(!rctx);
  ceph_assert(!orig_ctx);
  orig_ctx = new_ctx;
  if (new_ctx) {
    if (messages_pending_flush) {
      rctx.emplace(*messages_pending_flush, *new_ctx);
    } else {
      rctx.emplace(*new_ctx);
    }
    rctx->start_time = ceph_clock_now();
  }
}

void PeeringState::begin_block_outgoing() {
  ceph_assert(!messages_pending_flush);
  ceph_assert(orig_ctx);
  ceph_assert(rctx);
  messages_pending_flush.emplace();
  rctx.emplace(*messages_pending_flush, *orig_ctx);
}

void PeeringState::clear_blocked_outgoing() {
  ceph_assert(orig_ctx);
  ceph_assert(rctx);
  messages_pending_flush = std::optional<BufferedRecoveryMessages>();
}

void PeeringState::end_block_outgoing() {
  ceph_assert(messages_pending_flush);
  ceph_assert(orig_ctx);
  ceph_assert(rctx);

  orig_ctx->accept_buffered_messages(*messages_pending_flush);
  rctx.emplace(*orig_ctx);
  messages_pending_flush = std::optional<BufferedRecoveryMessages>();
}

void PeeringState::end_handle() {
  if (rctx) {
    utime_t dur = ceph_clock_now() - rctx->start_time;
    machine.event_time += dur;
  }

  machine.event_count++;
  rctx = std::nullopt;
  orig_ctx = NULL;
}

void PeeringState::check_recovery_sources(const OSDMapRef& osdmap)
{
  /*
   * check that any peers we are planning to (or currently) pulling
   * objects from are dealt with.
   */
  missing_loc.check_recovery_sources(osdmap);
  pl->check_recovery_sources(osdmap);

  for (auto i = peer_log_requested.begin(); i != peer_log_requested.end();) {
    if (!osdmap->is_up(i->osd)) {
      psdout(10) << "peer_log_requested removing " << *i << dendl;
      peer_log_requested.erase(i++);
    } else {
      ++i;
    }
  }

  for (auto i = peer_missing_requested.begin();
       i != peer_missing_requested.end();) {
    if (!osdmap->is_up(i->osd)) {
      psdout(10) << "peer_missing_requested removing " << *i << dendl;
      peer_missing_requested.erase(i++);
    } else {
      ++i;
    }
  }
}

void PeeringState::update_history(const pg_history_t& new_history)
{
  auto mnow = pl->get_mnow();
  info.history.refresh_prior_readable_until_ub(mnow, prior_readable_until_ub);
  if (info.history.merge(new_history)) {
    psdout(20) << "advanced history from " << new_history << dendl;
    dirty_info = true;
    if (info.history.last_epoch_clean >= info.history.same_interval_since) {
      psdout(20) << "clearing past_intervals" << dendl;
      past_intervals.clear();
      dirty_big_info = true;
    }
    prior_readable_until_ub = info.history.get_prior_readable_until_ub(mnow);
    if (prior_readable_until_ub != ceph::signedspan::zero()) {
      dout(20) << "prior_readable_until_ub " << prior_readable_until_ub
	       << " (mnow " << mnow << " + "
	       << info.history.prior_readable_until_ub << ")" << dendl;
    }
  }
}

hobject_t PeeringState::earliest_backfill() const
{
  hobject_t e = hobject_t::get_max();
  for (const pg_shard_t& bt : get_backfill_targets()) {
    const pg_info_t &pi = get_peer_info(bt);
    e = std::min(pi.last_backfill, e);
  }
  return e;
}

void PeeringState::purge_strays()
{
  if (is_premerge()) {
    psdout(10) << "purge_strays " << stray_set << " but premerge, doing nothing"
	       << dendl;
    return;
  }
  if (cct->_conf.get_val<bool>("osd_debug_no_purge_strays")) {
    return;
  }
  psdout(10) << "purge_strays " << stray_set << dendl;

  bool removed = false;
  for (auto p = stray_set.begin(); p != stray_set.end(); ++p) {
    ceph_assert(!is_acting_recovery_backfill(*p));
    if (get_osdmap()->is_up(p->osd)) {
      psdout(10) << "sending PGRemove to osd." << *p << dendl;
      vector<spg_t> to_remove;
      to_remove.push_back(spg_t(info.pgid.pgid, p->shard));
      auto m = TOPNSPC::make_message<MOSDPGRemove>(
	get_osdmap_epoch(),
	to_remove);
      pl->send_cluster_message(p->osd, std::move(m), get_osdmap_epoch());
    } else {
      psdout(10) << "not sending PGRemove to down osd." << *p << dendl;
    }
    peer_missing.erase(*p);
    peer_info.erase(*p);
    missing_loc.remove_stray_recovery_sources(*p);
    peer_purged.insert(*p);
    removed = true;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  stray_set.clear();

  // clear _requested maps; we may have to peer() again if we discover
  // (more) stray content
  peer_log_requested.clear();
  peer_missing_requested.clear();
}

void PeeringState::query_unfound(Formatter *f, string state)
{
  psdout(20) << "Enter PeeringState common QueryUnfound" << dendl;
  {
    f->dump_string("state", state);
    f->dump_bool("available_might_have_unfound", true);
    f->open_array_section("might_have_unfound");
    for (auto p = might_have_unfound.begin();
	 p != might_have_unfound.end();
	 ++p) {
      if (peer_missing.count(*p)) {
	; // Ignore already probed OSDs
      } else {
        f->open_object_section("osd");
        f->dump_stream("osd") << *p;
	if (peer_missing_requested.count(*p)) {
	  f->dump_string("status", "querying");
        } else if (!get_osdmap()->is_up(p->osd)) {
	  f->dump_string("status", "osd is down");
        } else {
	  f->dump_string("status", "not queried");
        }
        f->close_section();
      }
    }
    f->close_section();
  }
  psdout(20) << "Exit PeeringState common QueryUnfound" << dendl;
  return;
}

bool PeeringState::proc_replica_info(
  pg_shard_t from, const pg_info_t &oinfo, epoch_t send_epoch)
{
  auto p = peer_info.find(from);
  if (p != peer_info.end() && p->second.last_update == oinfo.last_update) {
    psdout(10) << " got dup osd." << from << " info "
	       << oinfo << ", identical to ours" << dendl;
    return false;
  }

  if (!get_osdmap()->has_been_up_since(from.osd, send_epoch)) {
    psdout(10) << " got info " << oinfo << " from down osd." << from
	     << " discarding" << dendl;
    return false;
  }

  psdout(10) << " got osd." << from << " " << oinfo << dendl;
  ceph_assert(is_primary());
  peer_info[from] = oinfo;
  might_have_unfound.insert(from);

  update_history(oinfo.history);

  // stray?
  if (!is_up(from) && !is_acting(from)) {
    psdout(10) << " osd." << from << " has stray content: " << oinfo << dendl;
    stray_set.insert(from);
    if (is_clean()) {
      purge_strays();
    }
  }

  // was this a new info?  if so, update peers!
  if (p == peer_info.end())
    update_heartbeat_peers();

  return true;
}


void PeeringState::remove_down_peer_info(const OSDMapRef &osdmap)
{
  // Remove any downed osds from peer_info
  bool removed = false;
  auto p = peer_info.begin();
  while (p != peer_info.end()) {
    if (!osdmap->is_up(p->first.osd)) {
      psdout(10) << " dropping down osd." << p->first << " info " << p->second << dendl;
      peer_missing.erase(p->first);
      peer_log_requested.erase(p->first);
      peer_missing_requested.erase(p->first);
      peer_info.erase(p++);
      removed = true;
    } else
      ++p;
  }

  // Remove any downed osds from peer_purged so we can re-purge if necessary
  auto it = peer_purged.begin();
  while (it != peer_purged.end()) {
    if (!osdmap->is_up(it->osd)) {
      psdout(10) << " dropping down osd." << *it << " from peer_purged" << dendl;
      peer_purged.erase(it++);
    } else {
      ++it;
    }
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  check_recovery_sources(osdmap);
}

void PeeringState::update_heartbeat_peers()
{
  if (!is_primary())
    return;

  set<int> new_peers;
  for (unsigned i=0; i<acting.size(); i++) {
    if (acting[i] != CRUSH_ITEM_NONE)
      new_peers.insert(acting[i]);
  }
  for (unsigned i=0; i<up.size(); i++) {
    if (up[i] != CRUSH_ITEM_NONE)
      new_peers.insert(up[i]);
  }
  for (auto p = peer_info.begin(); p != peer_info.end(); ++p) {
    new_peers.insert(p->first.osd);
  }
  pl->update_heartbeat_peers(std::move(new_peers));
}

void PeeringState::write_if_dirty(ObjectStore::Transaction& t)
{
  pl->prepare_write(
    info,
    last_written_info,
    past_intervals,
    pg_log,
    dirty_info,
    dirty_big_info,
    last_persisted_osdmap < get_osdmap_epoch(),
    t);
  if (dirty_info || dirty_big_info) {
    last_persisted_osdmap = get_osdmap_epoch();
    last_written_info = info;
    dirty_info = false;
    dirty_big_info = false;
  }
}

void PeeringState::advance_map(
  OSDMapRef osdmap, OSDMapRef lastmap,
  vector<int>& newup, int up_primary,
  vector<int>& newacting, int acting_primary,
  PeeringCtx &rctx)
{
  ceph_assert(lastmap == osdmap_ref);
  psdout(10) << "handle_advance_map "
	    << newup << "/" << newacting
	    << " -- " << up_primary << "/" << acting_primary
	    << dendl;

  update_osdmap_ref(osdmap);
  pool.update(osdmap);

  AdvMap evt(
    osdmap, lastmap, newup, up_primary,
    newacting, acting_primary);
  handle_event(evt, &rctx);
  if (pool.info.last_change == osdmap_ref->get_epoch()) {
    pl->on_pool_change();
  }
  readable_interval = pool.get_readable_interval(cct->_conf);
  last_require_osd_release = osdmap->require_osd_release;
}

void PeeringState::activate_map(PeeringCtx &rctx)
{
  psdout(10) << dendl;
  ActMap evt;
  handle_event(evt, &rctx);
  if (osdmap_ref->get_epoch() - last_persisted_osdmap >
    cct->_conf->osd_pg_epoch_persisted_max_stale) {
    psdout(20) << ": Dirtying info: last_persisted is "
	      << last_persisted_osdmap
	      << " while current is " << osdmap_ref->get_epoch() << dendl;
    dirty_info = true;
  } else {
    psdout(20) << ": Not dirtying info: last_persisted is "
	      << last_persisted_osdmap
	      << " while current is " << osdmap_ref->get_epoch() << dendl;
  }
  write_if_dirty(rctx.transaction);

  if (get_osdmap()->check_new_blocklist_entries()) {
    pl->check_blocklisted_watchers();
  }
}

void PeeringState::set_last_peering_reset()
{
  psdout(20) << "set_last_peering_reset " << get_osdmap_epoch() << dendl;
  if (last_peering_reset != get_osdmap_epoch()) {
    last_peering_reset = get_osdmap_epoch();
    psdout(10) << "Clearing blocked outgoing recovery messages" << dendl;
    clear_blocked_outgoing();
    if (!pl->try_flush_or_schedule_async()) {
      psdout(10) << "Beginning to block outgoing recovery messages" << dendl;
      begin_block_outgoing();
    } else {
      psdout(10) << "Not blocking outgoing recovery messages" << dendl;
    }
  }
}

void PeeringState::complete_flush()
{
  flushes_in_progress--;
  if (flushes_in_progress == 0) {
    pl->on_flushed();
  }
}

void PeeringState::check_full_transition(OSDMapRef lastmap, OSDMapRef osdmap)
{
  const pg_pool_t *pi = osdmap->get_pg_pool(info.pgid.pool());
  if (!pi) {
    return; // pool deleted
  }
  bool changed = false;
  if (pi->has_flag(pg_pool_t::FLAG_FULL)) {
    const pg_pool_t *opi = lastmap->get_pg_pool(info.pgid.pool());
    if (!opi || !opi->has_flag(pg_pool_t::FLAG_FULL)) {
      psdout(10) << " pool was marked full in " << osdmap->get_epoch() << dendl;
      changed = true;
    }
  }
  if (changed) {
    info.history.last_epoch_marked_full = osdmap->get_epoch();
    dirty_info = true;
  }
}

bool PeeringState::should_restart_peering(
  int newupprimary,
  int newactingprimary,
  const vector<int>& newup,
  const vector<int>& newacting,
  OSDMapRef lastmap,
  OSDMapRef osdmap)
{
  if (PastIntervals::is_new_interval(
	primary.osd,
	newactingprimary,
	acting,
	newacting,
	up_primary.osd,
	newupprimary,
	up,
	newup,
	osdmap.get(),
	lastmap.get(),
	info.pgid.pgid)) {
    psdout(20) << "new interval newup " << newup
	       << " newacting " << newacting << dendl;
    return true;
  }
  if (!lastmap->is_up(pg_whoami.osd) && osdmap->is_up(pg_whoami.osd)) {
    psdout(10) << "osd transitioned from down -> up"
	       << dendl;
    return true;
  }
  return false;
}

/* Called before initializing peering during advance_map */
void PeeringState::start_peering_interval(
  const OSDMapRef lastmap,
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  ObjectStore::Transaction &t)
{
  const OSDMapRef osdmap = get_osdmap();

  set_last_peering_reset();

  vector<int> oldacting, oldup;
  int oldrole = get_role();

  if (is_primary()) {
    pl->clear_ready_to_merge();
  }


  pg_shard_t old_acting_primary = get_primary();
  pg_shard_t old_up_primary = up_primary;
  bool was_old_primary = is_primary();
  bool was_old_nonprimary = is_nonprimary();

  acting.swap(oldacting);
  up.swap(oldup);
  init_primary_up_acting(
    newup,
    newacting,
    new_up_primary,
    new_acting_primary);

  if (info.stats.up != up ||
      info.stats.acting != acting ||
      info.stats.up_primary != new_up_primary ||
      info.stats.acting_primary != new_acting_primary) {
    info.stats.up = up;
    info.stats.up_primary = new_up_primary;
    info.stats.acting = acting;
    info.stats.acting_primary = new_acting_primary;
    info.stats.mapping_epoch = osdmap->get_epoch();
  }

  pl->clear_publish_stats();

  // This will now be remapped during a backfill in cases
  // that it would not have been before.
  if (up != acting)
    state_set(PG_STATE_REMAPPED);
  else
    state_clear(PG_STATE_REMAPPED);

  int role = osdmap->calc_pg_role(pg_whoami, acting);
  set_role(role);

  // did acting, up, primary|acker change?
  if (!lastmap) {
    psdout(10) << " no lastmap" << dendl;
    dirty_info = true;
    dirty_big_info = true;
    info.history.same_interval_since = osdmap->get_epoch();
  } else {
    std::stringstream debug;
    ceph_assert(info.history.same_interval_since != 0);
    bool new_interval = PastIntervals::check_new_interval(
      old_acting_primary.osd,
      new_acting_primary,
      oldacting, newacting,
      old_up_primary.osd,
      new_up_primary,
      oldup, newup,
      info.history.same_interval_since,
      info.history.last_epoch_clean,
      osdmap.get(),
      lastmap.get(),
      info.pgid.pgid,
      missing_loc.get_recoverable_predicate(),
      &past_intervals,
      &debug);
    psdout(10) << ": check_new_interval output: "
	       << debug.str() << dendl;
    if (new_interval) {
      if (osdmap->get_epoch() == pl->cluster_osdmap_trim_lower_bound() &&
	  info.history.last_epoch_clean < osdmap->get_epoch()) {
	psdout(10) << " map gap, clearing past_intervals and faking" << dendl;
	// our information is incomplete and useless; someone else was clean
	// after everything we know if osdmaps were trimmed.
	past_intervals.clear();
      } else {
	psdout(10) << " noting past " << past_intervals << dendl;
      }
      dirty_info = true;
      dirty_big_info = true;
      info.history.same_interval_since = osdmap->get_epoch();
      if (osdmap->have_pg_pool(info.pgid.pgid.pool()) &&
	  info.pgid.pgid.is_split(lastmap->get_pg_num(info.pgid.pgid.pool()),
				  osdmap->get_pg_num(info.pgid.pgid.pool()),
				  nullptr)) {
	info.history.last_epoch_split = osdmap->get_epoch();
      }
    }
  }

  if (old_up_primary != up_primary ||
      oldup != up) {
    info.history.same_up_since = osdmap->get_epoch();
  }
  // this comparison includes primary rank via pg_shard_t
  if (old_acting_primary != get_primary()) {
    info.history.same_primary_since = osdmap->get_epoch();
  }

  on_new_interval();

  psdout(1) << "up " << oldup << " -> " << up
	    << ", acting " << oldacting << " -> " << acting
	    << ", acting_primary " << old_acting_primary << " -> "
	    << new_acting_primary
	    << ", up_primary " << old_up_primary << " -> " << new_up_primary
	    << ", role " << oldrole << " -> " << role
	    << ", features acting " << acting_features
	    << " upacting " << upacting_features
	    << dendl;

  // deactivate.
  state_clear(PG_STATE_ACTIVE);
  state_clear(PG_STATE_PEERED);
  state_clear(PG_STATE_PREMERGE);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_RECOVERY_WAIT);
  state_clear(PG_STATE_RECOVERY_TOOFULL);
  state_clear(PG_STATE_RECOVERING);

  peer_purged.clear();
  acting_recovery_backfill.clear();

  // reset primary/replica state?
  if (was_old_primary || is_primary()) {
    pl->clear_want_pg_temp();
  } else if (was_old_nonprimary || is_nonprimary()) {
    pl->clear_want_pg_temp();
  }
  clear_primary_state();

  pl->on_change(t);

  ceph_assert(!deleting);

  // should we tell the primary we are here?
  send_notify = !is_primary();

  if (role != oldrole ||
      was_old_primary != is_primary()) {
    // did primary change?
    if (was_old_primary != is_primary()) {
      state_clear(PG_STATE_CLEAN);
    }

    pl->on_role_change();
  } else {
    // no role change.
    // did primary change?
    if (get_primary() != old_acting_primary) {
      psdout(10) << oldacting << " -> " << acting
	       << ", acting primary "
	       << old_acting_primary << " -> " << get_primary()
	       << dendl;
    } else {
      // primary is the same.
      if (is_primary()) {
	// i am (still) primary. but my replica set changed.
	state_clear(PG_STATE_CLEAN);

	psdout(10) << oldacting << " -> " << acting
		 << ", replicas changed" << dendl;
      }
    }
  }

  if (acting.empty() && !up.empty() && up_primary == pg_whoami) {
    psdout(10) << " acting empty, but i am up[0], clearing pg_temp" << dendl;
    pl->queue_want_pg_temp(acting);
  }
}

void PeeringState::on_new_interval()
{
  dout(20) << dendl;
  const OSDMapRef osdmap = get_osdmap();

  // initialize features
  acting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  upacting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  for (auto p = acting.begin(); p != acting.end(); ++p) {
    if (*p == CRUSH_ITEM_NONE)
      continue;
    uint64_t f = osdmap->get_xinfo(*p).features;
    acting_features &= f;
    upacting_features &= f;
  }
  for (auto p = up.begin(); p != up.end(); ++p) {
    if (*p == CRUSH_ITEM_NONE)
      continue;
    upacting_features &= osdmap->get_xinfo(*p).features;
  }
  psdout(20) << "upacting_features 0x" << std::hex
	     << upacting_features << std::dec
	     << " from " << acting << "+" << up << dendl;

  psdout(20) << "checking missing set deletes flag. missing = "
	     << get_pg_log().get_missing() << dendl;

  if (!pg_log.get_missing().may_include_deletes &&
      !perform_deletes_during_peering()) {
    pl->rebuild_missing_set_with_deletes(pg_log);
  }
  ceph_assert(
    pg_log.get_missing().may_include_deletes ==
    !perform_deletes_during_peering());

  init_hb_stamps();

  // update lease bounds for a new interval
  auto mnow = pl->get_mnow();
  prior_readable_until_ub = std::max(prior_readable_until_ub,
				     readable_until_ub);
  prior_readable_until_ub = info.history.refresh_prior_readable_until_ub(
    mnow, prior_readable_until_ub);
  psdout(10) << "prior_readable_until_ub "
	     << prior_readable_until_ub << " (mnow " << mnow << " + "
	     << info.history.prior_readable_until_ub << ")" << dendl;
  prior_readable_down_osds.clear(); // we populate this when we build the priorset

  readable_until =
    readable_until_ub =
    readable_until_ub_sent =
    readable_until_ub_from_primary = ceph::signedspan::zero();

  acting_readable_until_ub.clear();
  if (is_primary()) {
    acting_readable_until_ub.resize(acting.size(), ceph::signedspan::zero());
  }

  pl->on_new_interval();
}

void PeeringState::init_primary_up_acting(
  const vector<int> &newup,
  const vector<int> &newacting,
  int new_up_primary,
  int new_acting_primary)
{
  actingset.clear();
  acting = newacting;
  for (uint8_t i = 0; i < acting.size(); ++i) {
    if (acting[i] != CRUSH_ITEM_NONE)
      actingset.insert(
	pg_shard_t(
	  acting[i],
	  pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }
  upset.clear();
  up = newup;
  for (uint8_t i = 0; i < up.size(); ++i) {
    if (up[i] != CRUSH_ITEM_NONE)
      upset.insert(
	pg_shard_t(
	  up[i],
	  pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }
  if (!pool.info.is_erasure()) {
    // replicated
    up_primary = pg_shard_t(new_up_primary, shard_id_t::NO_SHARD);
    primary = pg_shard_t(new_acting_primary, shard_id_t::NO_SHARD);
  } else {
    // erasure
    up_primary = pg_shard_t();
    primary = pg_shard_t();
    for (uint8_t i = 0; i < up.size(); ++i) {
      if (up[i] == new_up_primary) {
	up_primary = pg_shard_t(up[i], shard_id_t(i));
	break;
      }
    }
    for (uint8_t i = 0; i < acting.size(); ++i) {
      if (acting[i] == new_acting_primary) {
	primary = pg_shard_t(acting[i], shard_id_t(i));
	break;
      }
    }
    ceph_assert(up_primary.osd == new_up_primary);
    ceph_assert(primary.osd == new_acting_primary);
  }
}

void PeeringState::init_hb_stamps()
{
  if (is_primary()) {
    // we care about all other osds in the acting set
    hb_stamps.resize(acting.size() - 1);
    unsigned i = 0;
    for (auto p : acting) {
      if (p == CRUSH_ITEM_NONE || p == get_primary().osd) {
	continue;
      }
      hb_stamps[i++] = pl->get_hb_stamps(p);
    }
    hb_stamps.resize(i);
  } else if (is_nonprimary()) {
    // we care about just the primary
    hb_stamps.resize(1);
    hb_stamps[0] = pl->get_hb_stamps(get_primary().osd);
  } else {
    hb_stamps.clear();
  }
  dout(10) << "now " << hb_stamps << dendl;
}


void PeeringState::clear_recovery_state()
{
  async_recovery_targets.clear();
  backfill_targets.clear();
}

void PeeringState::clear_primary_state()
{
  psdout(10) << "clear_primary_state" << dendl;

  // clear peering state
  stray_set.clear();
  peer_log_requested.clear();
  peer_missing_requested.clear();
  peer_info.clear();
  peer_bytes.clear();
  peer_missing.clear();
  peer_last_complete_ondisk.clear();
  peer_activated.clear();
  min_last_complete_ondisk = eversion_t();
  pg_trim_to = eversion_t();
  might_have_unfound.clear();
  need_up_thru = false;
  missing_loc.clear();
  pg_log.reset_recovery_pointers();

  clear_recovery_state();

  last_update_ondisk = eversion_t();
  missing_loc.clear();
  pl->clear_primary_state();
}

/// return [start,end) bounds for required past_intervals
static pair<epoch_t, epoch_t> get_required_past_interval_bounds(
  const pg_info_t &info,
  epoch_t oldest_map) {
  epoch_t start = std::max(
    info.history.last_epoch_clean ? info.history.last_epoch_clean :
    info.history.epoch_pool_created,
    oldest_map);
  epoch_t end = std::max(
    info.history.same_interval_since,
    info.history.epoch_pool_created);
  return make_pair(start, end);
}


void PeeringState::check_past_interval_bounds() const
{
  // cluster_osdmap_trim_lower_bound gives us a bound on needed
  // intervals, see doc/dev/osd_internals/past_intervals.rst
  auto oldest_epoch = pl->cluster_osdmap_trim_lower_bound();
  auto rpib = get_required_past_interval_bounds(
    info,
    oldest_epoch);
  if (rpib.first >= rpib.second) {
    // do not warn if the start bound is dictated by oldest_map; the
    // past intervals are presumably appropriate given the pg info.
    if (!past_intervals.empty() &&
	rpib.first > oldest_epoch) {
      pl->get_clog_error() << info.pgid << " required past_interval bounds are"
			     << " empty [" << rpib << ") but past_intervals is not: "
			     << past_intervals;
      derr << info.pgid << " required past_interval bounds are"
	   << " empty [" << rpib << ") but past_intervals is not: "
	   << past_intervals << dendl;
    }
  } else {
    if (past_intervals.empty()) {
      pl->get_clog_error() << info.pgid << " required past_interval bounds are"
			     << " not empty [" << rpib << ") but past_intervals "
			     << past_intervals << " is empty";
      derr << info.pgid << " required past_interval bounds are"
	   << " not empty [" << rpib << ") but past_intervals "
	   << past_intervals << " is empty" << dendl;
      ceph_assert(!past_intervals.empty());
    }

    auto apib = past_intervals.get_bounds();
    if (apib.first > rpib.first) {
      pl->get_clog_error() << info.pgid << " past_intervals [" << apib
			     << ") start interval does not contain the required"
			     << " bound [" << rpib << ") start";
      derr << info.pgid << " past_intervals [" << apib
	   << ") start interval does not contain the required"
	   << " bound [" << rpib << ") start" << dendl;
      ceph_abort_msg("past_interval start interval mismatch");
    }
    if (apib.second != rpib.second) {
      pl->get_clog_error() << info.pgid << " past_interal bound [" << apib
			     << ") end does not match required [" << rpib
			     << ") end";
      derr << info.pgid << " past_interal bound [" << apib
	   << ") end does not match required [" << rpib
	   << ") end" << dendl;
      ceph_abort_msg("past_interval end mismatch");
    }
  }
}

int PeeringState::clamp_recovery_priority(int priority, int pool_recovery_priority, int max)
{
  static_assert(OSD_RECOVERY_PRIORITY_MIN < OSD_RECOVERY_PRIORITY_MAX, "Invalid priority range");
  static_assert(OSD_RECOVERY_PRIORITY_MIN >= 0, "Priority range must match unsigned type");

  ceph_assert(max <= OSD_RECOVERY_PRIORITY_MAX);

  // User can't set this too high anymore, but might be a legacy value
  if (pool_recovery_priority > OSD_POOL_PRIORITY_MAX)
    pool_recovery_priority = OSD_POOL_PRIORITY_MAX;
  if (pool_recovery_priority < OSD_POOL_PRIORITY_MIN)
    pool_recovery_priority = OSD_POOL_PRIORITY_MIN;
  // Shift range from min to max to 0 to max - min
  pool_recovery_priority += (0 - OSD_POOL_PRIORITY_MIN);
  ceph_assert(pool_recovery_priority >= 0 && pool_recovery_priority <= (OSD_POOL_PRIORITY_MAX - OSD_POOL_PRIORITY_MIN));

  priority += pool_recovery_priority;

  // Clamp to valid range
  return std::clamp<int>(priority, OSD_RECOVERY_PRIORITY_MIN, max);
}

unsigned PeeringState::get_recovery_priority()
{
  // a higher value -> a higher priority
  int ret = OSD_RECOVERY_PRIORITY_BASE;
  int base = ret;

  if (state & PG_STATE_FORCED_RECOVERY) {
    ret = OSD_RECOVERY_PRIORITY_FORCED;
  } else {
    // XXX: This priority boost isn't so much about inactive, but about data-at-risk
    if (is_degraded() && info.stats.avail_no_missing.size() < pool.info.min_size) {
      base = OSD_RECOVERY_INACTIVE_PRIORITY_BASE;
      // inactive: no. of replicas < min_size, highest priority since it blocks IO
      ret = base + (pool.info.min_size - info.stats.avail_no_missing.size());
    }

    int64_t pool_recovery_priority = 0;
    pool.info.opts.get(pool_opts_t::RECOVERY_PRIORITY, &pool_recovery_priority);

    ret = clamp_recovery_priority(ret, pool_recovery_priority, max_prio_map[base]);
  }
  psdout(20) << "recovery priority is " << ret << dendl;
  return static_cast<unsigned>(ret);
}

unsigned PeeringState::get_backfill_priority()
{
  // a higher value -> a higher priority
  int ret = OSD_BACKFILL_PRIORITY_BASE;
  int base = ret;

  if (state & PG_STATE_FORCED_BACKFILL) {
    ret = OSD_BACKFILL_PRIORITY_FORCED;
  } else {
    if (actingset.size() < pool.info.min_size) {
      base = OSD_BACKFILL_INACTIVE_PRIORITY_BASE;
      // inactive: no. of replicas < min_size, highest priority since it blocks IO
      ret = base + (pool.info.min_size - actingset.size());

    } else if (is_undersized()) {
      // undersized: OSD_BACKFILL_DEGRADED_PRIORITY_BASE + num missing replicas
      ceph_assert(pool.info.size > actingset.size());
      base = OSD_BACKFILL_DEGRADED_PRIORITY_BASE;
      ret = base + (pool.info.size - actingset.size());

    } else if (is_degraded()) {
      // degraded: baseline degraded
      base = ret = OSD_BACKFILL_DEGRADED_PRIORITY_BASE;
    }

    // Adjust with pool's recovery priority
    int64_t pool_recovery_priority = 0;
    pool.info.opts.get(pool_opts_t::RECOVERY_PRIORITY, &pool_recovery_priority);

    ret = clamp_recovery_priority(ret, pool_recovery_priority, max_prio_map[base]);
  }

  psdout(20) << "backfill priority is " << ret << dendl;
  return static_cast<unsigned>(ret);
}

unsigned PeeringState::get_delete_priority()
{
  auto state = get_osdmap()->get_state(pg_whoami.osd);
  if (state & (CEPH_OSD_BACKFILLFULL |
               CEPH_OSD_FULL)) {
    return OSD_DELETE_PRIORITY_FULL;
  } else if (state & CEPH_OSD_NEARFULL) {
    return OSD_DELETE_PRIORITY_FULLISH;
  } else {
    return OSD_DELETE_PRIORITY_NORMAL;
  }
}

bool PeeringState::set_force_recovery(bool b)
{
  bool did = false;
  if (b) {
    if (!(state & PG_STATE_FORCED_RECOVERY) &&
	(state & (PG_STATE_DEGRADED |
		  PG_STATE_RECOVERY_WAIT |
		  PG_STATE_RECOVERING))) {
      psdout(20) << "set" << dendl;
      state_set(PG_STATE_FORCED_RECOVERY);
      pl->publish_stats_to_osd();
      did = true;
    }
  } else if (state & PG_STATE_FORCED_RECOVERY) {
    psdout(20) << "clear" << dendl;
    state_clear(PG_STATE_FORCED_RECOVERY);
    pl->publish_stats_to_osd();
    did = true;
  }
  if (did) {
    psdout(20) << "state " << get_current_state()
	     << dendl;
    pl->update_local_background_io_priority(get_recovery_priority());
  }
  return did;
}

bool PeeringState::set_force_backfill(bool b)
{
  bool did = false;
  if (b) {
    if (!(state & PG_STATE_FORCED_BACKFILL) &&
	(state & (PG_STATE_DEGRADED |
		  PG_STATE_BACKFILL_WAIT |
		  PG_STATE_BACKFILLING))) {
      psdout(10) << "set" << dendl;
      state_set(PG_STATE_FORCED_BACKFILL);
      pl->publish_stats_to_osd();
      did = true;
    }
  } else if (state & PG_STATE_FORCED_BACKFILL) {
    psdout(10) << "clear" << dendl;
    state_clear(PG_STATE_FORCED_BACKFILL);
    pl->publish_stats_to_osd();
    did = true;
  }
  if (did) {
    psdout(20) << "state " << get_current_state()
	     << dendl;
    pl->update_local_background_io_priority(get_backfill_priority());
  }
  return did;
}

void PeeringState::schedule_renew_lease()
{
  pl->schedule_renew_lease(
    last_peering_reset,
    readable_interval / 2);
}

void PeeringState::send_lease()
{
  epoch_t epoch = pl->get_osdmap_epoch();
  for (auto peer : actingset) {
    if (peer == pg_whoami) {
      continue;
    }
    pl->send_cluster_message(
      peer.osd,
      TOPNSPC::make_message<MOSDPGLease>(epoch,
		      spg_t(spgid.pgid, peer.shard),
		      get_lease()),
      epoch);
  }
}

void PeeringState::proc_lease(const pg_lease_t& l)
{
  assert(HAVE_FEATURE(upacting_features, SERVER_OCTOPUS));
  if (!is_nonprimary()) {
    psdout(20) << "no-op, !nonprimary" << dendl;
    return;
  }
  psdout(10) << l << dendl;
  if (l.readable_until_ub > readable_until_ub_from_primary) {
    readable_until_ub_from_primary = l.readable_until_ub;
  }

  ceph::signedspan ru = ceph::signedspan::zero();
  if (l.readable_until != ceph::signedspan::zero() &&
      hb_stamps[0]->peer_clock_delta_ub) {
    ru = l.readable_until - *hb_stamps[0]->peer_clock_delta_ub;
    psdout(20) << " peer_clock_delta_ub " << *hb_stamps[0]->peer_clock_delta_ub
	       << " -> ru " << ru << dendl;
  }
  if (ru > readable_until) {
    readable_until = ru;
    psdout(20) << "readable_until now " << readable_until << dendl;
    // NOTE: if we ever decide to block/queue ops on the replica,
    // we'll need to wake them up here.
  }

  ceph::signedspan ruub;
  if (hb_stamps[0]->peer_clock_delta_lb) {
    ruub = l.readable_until_ub - *hb_stamps[0]->peer_clock_delta_lb;
    psdout(20) << " peer_clock_delta_lb " << *hb_stamps[0]->peer_clock_delta_lb
	       << " -> ruub " << ruub << dendl;
  } else {
    ruub = pl->get_mnow() + l.interval;
    psdout(20) << " no peer_clock_delta_lb -> ruub " << ruub << dendl;
  }
  if (ruub > readable_until_ub) {
    readable_until_ub = ruub;
    psdout(20) << "readable_until_ub now " << readable_until_ub
	       << dendl;
  }
}

void PeeringState::proc_lease_ack(int from, const pg_lease_ack_t& a)
{
  assert(HAVE_FEATURE(upacting_features, SERVER_OCTOPUS));
  auto now = pl->get_mnow();
  bool was_min = false;
  for (unsigned i = 0; i < acting.size(); ++i) {
    if (from == acting[i]) {
      // the lease_ack value is based on the primary's clock
      if (a.readable_until_ub > acting_readable_until_ub[i]) {
	if (acting_readable_until_ub[i] == readable_until) {
	  was_min = true;
	}
	acting_readable_until_ub[i] = a.readable_until_ub;
	break;
      }
    }
  }
  if (was_min) {
    auto old_ru = readable_until;
    recalc_readable_until();
    if (now >= old_ru) {
      pl->recheck_readable();
    }
  }
}

void PeeringState::proc_renew_lease()
{
  assert(HAVE_FEATURE(upacting_features, SERVER_OCTOPUS));
  renew_lease(pl->get_mnow());
  if (actingset.size() > 1) {
    send_lease();
  } else {
    pl->recheck_readable();
  }
  schedule_renew_lease();
}

void PeeringState::recalc_readable_until()
{
  assert(is_primary());
  ceph::signedspan min = readable_until_ub_sent;
  for (unsigned i = 0; i < acting.size(); ++i) {
    if (acting[i] == pg_whoami.osd || acting[i] == CRUSH_ITEM_NONE) {
      continue;
    }
    dout(20) << "peer osd." << acting[i]
	     << " ruub " << acting_readable_until_ub[i] << dendl;
    if (acting_readable_until_ub[i] < min) {
      min = acting_readable_until_ub[i];
    }
  }
  readable_until = min;
  readable_until_ub = min;
  dout(20) << "readable_until[_ub] " << readable_until
	   << " (sent " << readable_until_ub_sent << ")" << dendl;
}

bool PeeringState::check_prior_readable_down_osds(const OSDMapRef& map)
{
  assert(HAVE_FEATURE(upacting_features, SERVER_OCTOPUS));
  bool changed = false;
  auto p = prior_readable_down_osds.begin();
  while (p != prior_readable_down_osds.end()) {
    if (map->is_dead(*p)) {
      dout(10) << "prior_readable_down_osds osd." << *p
	       << " is dead as of epoch " << map->get_epoch()
	       << dendl;
      p = prior_readable_down_osds.erase(p);
      changed = true;
    } else {
      ++p;
    }
  }
  if (changed && prior_readable_down_osds.empty()) {
    psdout(10) << " empty prior_readable_down_osds, clearing ub" << dendl;
    clear_prior_readable_until_ub();
    return true;
  }
  return false;
}

bool PeeringState::adjust_need_up_thru(const OSDMapRef osdmap)
{
  epoch_t up_thru = osdmap->get_up_thru(pg_whoami.osd);
  if (need_up_thru &&
      up_thru >= info.history.same_interval_since) {
    psdout(10) << "adjust_need_up_thru now "
	       << up_thru << ", need_up_thru now false" << dendl;
    need_up_thru = false;
    return true;
  }
  return false;
}

PastIntervals::PriorSet PeeringState::build_prior()
{
  if (1) {
    // sanity check
    for (auto it = peer_info.begin(); it != peer_info.end(); ++it) {
      ceph_assert(info.history.last_epoch_started >=
		  it->second.history.last_epoch_started);
    }
  }

  const OSDMap &osdmap = *get_osdmap();
  PastIntervals::PriorSet prior = past_intervals.get_prior_set(
    pool.info.is_erasure(),
    info.history.last_epoch_started,
    &missing_loc.get_recoverable_predicate(),
    [&](epoch_t start, int osd, epoch_t *lost_at) {
      const osd_info_t *pinfo = 0;
      if (osdmap.exists(osd)) {
	pinfo = &osdmap.get_info(osd);
	if (lost_at)
	  *lost_at = pinfo->lost_at;
      }

      if (osdmap.is_up(osd)) {
	return PastIntervals::UP;
      } else if (!pinfo) {
	return PastIntervals::DNE;
      } else if (pinfo->lost_at > start) {
	return PastIntervals::LOST;
      } else {
	return PastIntervals::DOWN;
      }
    },
    up,
    acting,
    dpp);

  if (prior.pg_down) {
    state_set(PG_STATE_DOWN);
  }

  if (get_osdmap()->get_up_thru(pg_whoami.osd) <
      info.history.same_interval_since) {
    psdout(10) << "up_thru " << get_osdmap()->get_up_thru(pg_whoami.osd)
	       << " < same_since " << info.history.same_interval_since
	       << ", must notify monitor" << dendl;
    need_up_thru = true;
  } else {
    psdout(10) << "up_thru " << get_osdmap()->get_up_thru(pg_whoami.osd)
	       << " >= same_since " << info.history.same_interval_since
	       << ", all is well" << dendl;
    need_up_thru = false;
  }
  pl->set_probe_targets(prior.probe);
  return prior;
}

bool PeeringState::needs_recovery() const
{
  ceph_assert(is_primary());

  auto &missing = pg_log.get_missing();

  if (missing.num_missing()) {
    psdout(10) << "primary has " << missing.num_missing()
	       << " missing" << dendl;
    return true;
  }

  ceph_assert(!acting_recovery_backfill.empty());
  for (const pg_shard_t& peer : acting_recovery_backfill) {
    if (peer == get_primary()) {
      continue;
    }
    auto pm = peer_missing.find(peer);
    if (pm == peer_missing.end()) {
      psdout(10) << "osd." << peer << " doesn't have missing set"
		 << dendl;
      continue;
    }
    if (pm->second.num_missing()) {
      psdout(10) << "osd." << peer << " has "
		 << pm->second.num_missing() << " missing" << dendl;
      return true;
    }
  }

  psdout(10) << "is recovered" << dendl;
  return false;
}

bool PeeringState::needs_backfill() const
{
  ceph_assert(is_primary());

  // We can assume that only possible osds that need backfill
  // are on the backfill_targets vector nodes.
  for (const pg_shard_t& peer : backfill_targets) {
    auto pi = peer_info.find(peer);
    ceph_assert(pi != peer_info.end());
    if (!pi->second.last_backfill.is_max()) {
      psdout(10) << "osd." << peer
		 << " has last_backfill " << pi->second.last_backfill << dendl;
      return true;
    }
  }

  psdout(10) << "does not need backfill" << dendl;
  return false;
}

/**
* Returns whether a particular object can be safely read on this replica
*/
bool PeeringState::can_serve_replica_read(const hobject_t &hoid)
{
  ceph_assert(!is_primary());
  eversion_t min_last_complete_ondisk = get_min_last_complete_ondisk();
  if (!pg_log.get_log().has_write_since(
      hoid, min_last_complete_ondisk)) {
    psdout(20) << "can be safely read on this replica" << dendl;
    return true;
  } else {
    psdout(20) << "can't read object on this replica" << dendl;
    return false;
  }
}

/*
 * Returns true unless there is a non-lost OSD in might_have_unfound.
 */
bool PeeringState::all_unfound_are_queried_or_lost(
  const OSDMapRef osdmap) const
{
  ceph_assert(is_primary());

  auto peer = might_have_unfound.begin();
  auto mend = might_have_unfound.end();
  for (; peer != mend; ++peer) {
    if (peer_missing.count(*peer))
      continue;
    auto iter = peer_info.find(*peer);
    if (iter != peer_info.end() &&
        (iter->second.is_empty() || iter->second.dne()))
      continue;
    if (!osdmap->exists(peer->osd))
      continue;
    const osd_info_t &osd_info(osdmap->get_info(peer->osd));
    if (osd_info.lost_at <= osd_info.up_from) {
      // If there is even one OSD in might_have_unfound that isn't lost, we
      // still might retrieve our unfound.
      return false;
    }
  }
  psdout(10) << "all_unfound_are_queried_or_lost all of might_have_unfound "
	     << might_have_unfound
	     << " have been queried or are marked lost" << dendl;
  return true;
}


void PeeringState::reject_reservation()
{
  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    primary.osd,
    TOPNSPC::make_message<MBackfillReserve>(
      MBackfillReserve::REJECT_TOOFULL,
      spg_t(info.pgid.pgid, primary.shard),
      get_osdmap_epoch()),
    get_osdmap_epoch());
}

/**
 * find_best_info
 *
 * Returns an iterator to the best info in infos sorted by:
 *  1) Prefer newer last_update
 *  2) Prefer longer tail if it brings another info into contiguity
 *  3) Prefer current primary
 */
map<pg_shard_t, pg_info_t>::const_iterator PeeringState::find_best_info(
  const map<pg_shard_t, pg_info_t> &infos,
  bool restrict_to_up_acting,
  bool *history_les_bound) const
{
  ceph_assert(history_les_bound);
  /* See doc/dev/osd_internals/last_epoch_started.rst before attempting
   * to make changes to this process.  Also, make sure to update it
   * when you find bugs! */
  epoch_t max_last_epoch_started_found = 0;
  for (auto i = infos.begin(); i != infos.end(); ++i) {
    if (!cct->_conf->osd_find_best_info_ignore_history_les &&
	max_last_epoch_started_found < i->second.history.last_epoch_started) {
      *history_les_bound = true;
      max_last_epoch_started_found = i->second.history.last_epoch_started;
    }
    if (!i->second.is_incomplete() &&
	max_last_epoch_started_found < i->second.last_epoch_started) {
      *history_les_bound = false;
      max_last_epoch_started_found = i->second.last_epoch_started;
    }
  }
  eversion_t min_last_update_acceptable = eversion_t::max();
  for (auto i = infos.begin(); i != infos.end(); ++i) {
    if (max_last_epoch_started_found <= i->second.last_epoch_started) {
      if (min_last_update_acceptable > i->second.last_update)
	min_last_update_acceptable = i->second.last_update;
    }
  }
  if (min_last_update_acceptable == eversion_t::max())
    return infos.end();

  auto best = infos.end();
  // find osd with newest last_update (oldest for ec_pool).
  // if there are multiples, prefer
  //  - a longer tail, if it brings another peer into log contiguity
  //  - the current primary
  for (auto p = infos.begin(); p != infos.end(); ++p) {
    if (restrict_to_up_acting && !is_up(p->first) &&
	!is_acting(p->first))
      continue;
    // Only consider peers with last_update >= min_last_update_acceptable
    if (p->second.last_update < min_last_update_acceptable)
      continue;
    // Disqualify anyone with a too old last_epoch_started
    if (p->second.last_epoch_started < max_last_epoch_started_found)
      continue;
    // Disqualify anyone who is incomplete (not fully backfilled)
    if (p->second.is_incomplete())
      continue;
    if (best == infos.end()) {
      best = p;
      continue;
    }
    // Prefer newer last_update
    if (pool.info.require_rollback()) {
      if (p->second.last_update > best->second.last_update)
	continue;
      if (p->second.last_update < best->second.last_update) {
	best = p;
	continue;
      }
    } else {
      if (p->second.last_update < best->second.last_update)
	continue;
      if (p->second.last_update > best->second.last_update) {
	best = p;
	continue;
      }
    }

    // Prefer longer tail
    if (p->second.log_tail > best->second.log_tail) {
      continue;
    } else if (p->second.log_tail < best->second.log_tail) {
      best = p;
      continue;
    }

    if (!p->second.has_missing() && best->second.has_missing()) {
      psdout(10) << "prefer osd." << p->first
               << " because it is complete while best has missing"
               << dendl;
      best = p;
      continue;
    } else if (p->second.has_missing() && !best->second.has_missing()) {
      psdout(10) << "skipping osd." << p->first
               << " because it has missing while best is complete"
               << dendl;
      continue;
    } else {
      // both are complete or have missing
      // fall through
    }

    // prefer current primary (usually the caller), all things being equal
    if (p->first == pg_whoami) {
      psdout(10) << "calc_acting prefer osd." << p->first
		 << " because it is current primary" << dendl;
      best = p;
      continue;
    }
  }
  return best;
}

void PeeringState::calc_ec_acting(
  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
  unsigned size,
  const vector<int> &acting,
  const vector<int> &up,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool restrict_to_up_acting,
  vector<int> *_want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  ostream &ss)
{
  vector<int> want(size, CRUSH_ITEM_NONE);
  map<shard_id_t, set<pg_shard_t> > all_info_by_shard;
  for (auto i = all_info.begin();
       i != all_info.end();
       ++i) {
    all_info_by_shard[i->first.shard].insert(i->first);
  }
  for (uint8_t i = 0; i < want.size(); ++i) {
    ss << "For position " << (unsigned)i << ": ";
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(up[i], shard_id_t(i)))->second.is_incomplete() &&
	all_info.find(pg_shard_t(up[i], shard_id_t(i)))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting up[i]: " << pg_shard_t(up[i], shard_id_t(i)) << std::endl;
      want[i] = up[i];
      continue;
    }
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE) {
      ss << " backfilling up[i]: " << pg_shard_t(up[i], shard_id_t(i))
	 << " and ";
      backfill->insert(pg_shard_t(up[i], shard_id_t(i)));
    }

    if (acting.size() > (unsigned)i && acting[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(acting[i], shard_id_t(i)))->second.is_incomplete() &&
	all_info.find(pg_shard_t(acting[i], shard_id_t(i)))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting acting[i]: " << pg_shard_t(acting[i], shard_id_t(i)) << std::endl;
      want[i] = acting[i];
    } else if (!restrict_to_up_acting) {
      for (auto j = all_info_by_shard[shard_id_t(i)].begin();
	   j != all_info_by_shard[shard_id_t(i)].end();
	   ++j) {
	ceph_assert(j->shard == i);
	if (!all_info.find(*j)->second.is_incomplete() &&
	    all_info.find(*j)->second.last_update >=
	    auth_log_shard->second.log_tail) {
	  ss << " selecting stray: " << *j << std::endl;
	  want[i] = j->osd;
	  break;
	}
      }
      if (want[i] == CRUSH_ITEM_NONE)
	ss << " failed to fill position " << (int)i << std::endl;
    }
  }

  for (uint8_t i = 0; i < want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE) {
      acting_backfill->insert(pg_shard_t(want[i], shard_id_t(i)));
    }
  }
  acting_backfill->insert(backfill->begin(), backfill->end());
  _want->swap(want);
}

std::pair<map<pg_shard_t, pg_info_t>::const_iterator, eversion_t>
PeeringState::select_replicated_primary(
  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
  uint64_t force_auth_primary_missing_objects,
  const std::vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  const OSDMapRef osdmap,
  ostream &ss)
{
  pg_shard_t auth_log_shard_id = auth_log_shard->first;

  ss << __func__ << " newest update on osd." << auth_log_shard_id
     << " with " << auth_log_shard->second << std::endl;

  // select primary
  auto primary = all_info.find(up_primary);
  if (up.size() &&
      !primary->second.is_incomplete() &&
      primary->second.last_update >=
        auth_log_shard->second.log_tail) {
    assert(HAVE_FEATURE(osdmap->get_up_osd_features(), SERVER_NAUTILUS));
    auto approx_missing_objects =
      primary->second.stats.stats.sum.num_objects_missing;
    auto auth_version = auth_log_shard->second.last_update.version;
    auto primary_version = primary->second.last_update.version;
    if (auth_version > primary_version) {
      approx_missing_objects += auth_version - primary_version;
    } else {
      approx_missing_objects += primary_version - auth_version;
    }
    if ((uint64_t)approx_missing_objects >
        force_auth_primary_missing_objects) {
      primary = auth_log_shard;
      ss << "up_primary: " << up_primary << ") has approximate "
         << approx_missing_objects
         << "(>" << force_auth_primary_missing_objects <<") "
         << "missing objects, osd." << auth_log_shard_id
         << " selected as primary instead"
         << std::endl;
    } else {
      ss << "up_primary: " << up_primary << ") selected as primary"
         << std::endl;
    }
  } else {
    ceph_assert(!auth_log_shard->second.is_incomplete());
    ss << "up[0] needs backfill, osd." << auth_log_shard_id
       << " selected as primary instead" << std::endl;
    primary = auth_log_shard;
  }

  ss << __func__ << " primary is osd." << primary->first
     << " with " << primary->second << std::endl;

  /* We include auth_log_shard->second.log_tail because in GetLog,
   * we will request logs back to the min last_update over our
   * acting_backfill set, which will result in our log being extended
   * as far backwards as necessary to pick up any peers which can
   * be log recovered by auth_log_shard's log */
  eversion_t oldest_auth_log_entry =
    std::min(primary->second.log_tail, auth_log_shard->second.log_tail);

  return std::make_pair(primary, oldest_auth_log_entry);
}


/**
 * calculate the desired acting set.
 *
 * Choose an appropriate acting set.  Prefer up[0], unless it is
 * incomplete, or another osd has a longer tail that allows us to
 * bring other up nodes up to date.
 */
void PeeringState::calc_replicated_acting(
  map<pg_shard_t, pg_info_t>::const_iterator primary,
  eversion_t oldest_auth_log_entry,
  unsigned size,
  const vector<int> &acting,
  const vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool restrict_to_up_acting,
  vector<int> *want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  const OSDMapRef osdmap,
  const PGPool& pool,
  ostream &ss)
{
  ss << __func__ << (restrict_to_up_acting ? " restrict_to_up_acting" : "")
     << std::endl;

  want->push_back(primary->first.osd);
  acting_backfill->insert(primary->first);

  // select replicas that have log contiguity with primary.
  // prefer up, then acting, then any peer_info osds
  for (auto i : up) {
    pg_shard_t up_cand = pg_shard_t(i, shard_id_t::NO_SHARD);
    if (up_cand == primary->first)
      continue;
    const pg_info_t &cur_info = all_info.find(up_cand)->second;
    if (cur_info.is_incomplete() ||
        cur_info.last_update < oldest_auth_log_entry) {
      ss << " shard " << up_cand << " (up) backfill " << cur_info << std::endl;
      backfill->insert(up_cand);
      acting_backfill->insert(up_cand);
    } else {
      want->push_back(i);
      acting_backfill->insert(up_cand);
      ss << " osd." << i << " (up) accepted " << cur_info << std::endl;
    }
  }

  if (want->size() >= size) {
    return;
  }

  std::vector<std::pair<eversion_t, int>> candidate_by_last_update;
  candidate_by_last_update.reserve(acting.size());
  // This no longer has backfill OSDs, but they are covered above.
  for (auto i : acting) {
    pg_shard_t acting_cand(i, shard_id_t::NO_SHARD);
    // skip up osds we already considered above
    if (acting_cand == primary->first)
      continue;
    auto up_it = find(up.begin(), up.end(), i);
    if (up_it != up.end())
      continue;

    const pg_info_t &cur_info = all_info.find(acting_cand)->second;
    if (cur_info.is_incomplete() ||
	cur_info.last_update < oldest_auth_log_entry) {
      ss << " shard " << acting_cand << " (acting) REJECTED "
	 << cur_info << std::endl;
    } else {
      candidate_by_last_update.emplace_back(cur_info.last_update, i);
    }
  }

  auto sort_by_eversion =[](const std::pair<eversion_t, int> &lhs,
                            const std::pair<eversion_t, int> &rhs) {
    return lhs.first > rhs.first;
  };
  // sort by last_update, in descending order.
  std::sort(candidate_by_last_update.begin(),
            candidate_by_last_update.end(), sort_by_eversion);
  for (auto &p: candidate_by_last_update) {
    ceph_assert(want->size() < size);
    want->push_back(p.second);
    pg_shard_t s = pg_shard_t(p.second, shard_id_t::NO_SHARD);
    acting_backfill->insert(s);
    ss << " shard " << s << " (acting) accepted "
       << all_info.find(s)->second << std::endl;
    if (want->size() >= size) {
      return;
    }
  }

  if (restrict_to_up_acting) {
    return;
  }
  candidate_by_last_update.clear();
  candidate_by_last_update.reserve(all_info.size()); // overestimate but fine
  // continue to search stray to find more suitable peers
  for (auto &i : all_info) {
    // skip up osds we already considered above
    if (i.first == primary->first)
      continue;
    auto up_it = find(up.begin(), up.end(), i.first.osd);
    if (up_it != up.end())
      continue;
    auto acting_it = find(
      acting.begin(), acting.end(), i.first.osd);
    if (acting_it != acting.end())
      continue;

    if (i.second.is_incomplete() ||
	i.second.last_update < oldest_auth_log_entry) {
      ss << " shard " << i.first << " (stray) REJECTED " << i.second
         << std::endl;
    } else {
      candidate_by_last_update.emplace_back(
        i.second.last_update, i.first.osd);
    }
  }

  if (candidate_by_last_update.empty()) {
    // save us some effort
    return;
  }

  // sort by last_update, in descending order.
  std::sort(candidate_by_last_update.begin(),
            candidate_by_last_update.end(), sort_by_eversion);

  for (auto &p: candidate_by_last_update) {
    ceph_assert(want->size() < size);
    want->push_back(p.second);
    pg_shard_t s = pg_shard_t(p.second, shard_id_t::NO_SHARD);
    acting_backfill->insert(s);
    ss << " shard " << s << " (stray) accepted "
       << all_info.find(s)->second << std::endl;
    if (want->size() >= size) {
      return;
    }
  }
}

// Defines osd preference order: acting set, then larger last_update
using osd_ord_t = std::tuple<bool, eversion_t>; // <acting, last_update>
using osd_id_t = int;

class bucket_candidates_t {
  std::deque<std::pair<osd_ord_t, osd_id_t>> osds;
  int selected = 0;

public:
  void add_osd(osd_ord_t ord, osd_id_t osd) {
    // osds will be added in smallest to largest order
    assert(osds.empty() || osds.back().first <= ord);
    osds.push_back(std::make_pair(ord, osd));
  }
  osd_id_t pop_osd() {
    ceph_assert(!is_empty());
    auto ret = osds.front();
    osds.pop_front();
    return ret.second;
  }

  void inc_selected() { selected++; }
  unsigned get_num_selected() const { return selected; }

  osd_ord_t get_ord() const {
    return osds.empty() ? std::make_tuple(false, eversion_t())
      : osds.front().first;
  }

  bool is_empty() const { return osds.empty(); }

  bool operator<(const bucket_candidates_t &rhs) const {
    return std::make_tuple(-selected, get_ord()) <
      std::make_tuple(-rhs.selected, rhs.get_ord());
  }

  friend std::ostream &operator<<(std::ostream &, const bucket_candidates_t &);
};

std::ostream &operator<<(std::ostream &lhs, const bucket_candidates_t &cand)
{
  return lhs << "candidates[" << cand.osds << "]";
}

class bucket_heap_t {
  using elem_t = std::reference_wrapper<bucket_candidates_t>;
  std::vector<elem_t> heap;

  // Max heap -- should emit buckets in order of preference
  struct comp {
    bool operator()(const elem_t &lhs, const elem_t &rhs) {
      return lhs.get() < rhs.get();
    }
  };
public:
  void push_if_nonempty(elem_t e) {
    if (!e.get().is_empty()) {
      heap.push_back(e);
      std::push_heap(heap.begin(), heap.end(), comp());
    }
  }
  elem_t pop() {
    std::pop_heap(heap.begin(), heap.end(), comp());
    auto ret = heap.back();
    heap.pop_back();
    return ret;
  }

  bool is_empty() const { return heap.empty(); }
};

/**
 * calc_replicated_acting_stretch
 *
 * Choose an acting set using as much of the up set as possible; filling 
 * in the remaining slots so as to maximize the number of crush buckets at
 * level pool.info.peering_crush_bucket_barrier represented.
 *
 * Stretch clusters are a bit special: while they have a "size" the
 * same way as normal pools, if we happen to lose a data center
 * (we call it a "stretch bucket", but really it'll be a data center or
 * a cloud availability zone), we don't actually want to shove
 * 2 DC's worth of replication into a single site -- it won't fit!
 * So we locally calculate a bucket_max, based
 * on the targeted number of stretch buckets for the pool and
 * its size. Then we won't pull more than bucket_max from any
 * given ancestor even if it leaves us undersized.

 * There are two distinct phases: (commented below)
 */
void PeeringState::calc_replicated_acting_stretch(
  map<pg_shard_t, pg_info_t>::const_iterator primary,
  eversion_t oldest_auth_log_entry,
  unsigned size,
  const vector<int> &acting,
  const vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool restrict_to_up_acting,
  vector<int> *want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  const OSDMapRef osdmap,
  const PGPool& pool,
  ostream &ss)
{
  ceph_assert(want);
  ceph_assert(acting_backfill);
  ceph_assert(backfill);
  ss << __func__ << (restrict_to_up_acting ? " restrict_to_up_acting" : "")
     << std::endl;

  auto used = [want](int osd) {
    return std::find(want->begin(), want->end(), osd) != want->end();
  };

  auto usable_info = [&](const auto &cur_info) mutable {
    return !(cur_info.is_incomplete() ||
	     cur_info.last_update < oldest_auth_log_entry);
  };

  auto osd_info = [&](int osd) mutable -> const pg_info_t & {
    pg_shard_t cand = pg_shard_t(osd, shard_id_t::NO_SHARD);
    const pg_info_t &cur_info = all_info.find(cand)->second;
    return cur_info;
  };

  auto usable_osd = [&](int osd) mutable {
    return usable_info(osd_info(osd));
  };

  std::map<int, bucket_candidates_t> ancestors;
  auto get_ancestor = [&](int osd) mutable {
    int ancestor = osdmap->crush->get_parent_of_type(
      osd,
      pool.info.peering_crush_bucket_barrier,
      pool.info.crush_rule);
    return &ancestors[ancestor];
  };

  unsigned bucket_max = pool.info.size / pool.info.peering_crush_bucket_target;
  if (bucket_max * pool.info.peering_crush_bucket_target < pool.info.size) {
    ++bucket_max; 
  }

  /* 1) Select all usable osds from the up set as well as the primary
   *
   * We also stash any unusable osds from up into backfill.
   */
  auto add_required = [&](int osd) {
    if (!used(osd)) {
      want->push_back(osd);
      acting_backfill->insert(
	pg_shard_t(osd, shard_id_t::NO_SHARD));
      get_ancestor(osd)->inc_selected();
    }
  };
  add_required(primary->first.osd);
  ss << " osd " << primary->first.osd << " primary accepted "
     << osd_info(primary->first.osd) << std::endl;
  for (auto upcand: up) {
    auto upshard = pg_shard_t(upcand, shard_id_t::NO_SHARD);
    auto &curinfo = osd_info(upcand);
    if (usable_osd(upcand)) {
      ss << " osd " << upcand << " (up) accepted " << curinfo << std::endl;
      add_required(upcand);
    } else {
      ss << " osd " << upcand << " (up) backfill " << curinfo << std::endl;
      backfill->insert(upshard);
      acting_backfill->insert(upshard);
    }
  }

  if (want->size() >= pool.info.size) { // non-failed CRUSH mappings are valid
    ss << " up set sufficient" << std::endl;
    return;
  }
  ss << " up set insufficient, considering remaining osds" << std::endl;

  /* 2) Fill out remaining slots from usable osds in all_info
   *    while maximizing the number of ancestor nodes at the
   *    barrier_id crush level.
   */
  {
    std::vector<std::pair<osd_ord_t, osd_id_t>> candidates;
    /* To do this, we first filter the set of usable osd into an ordered
     * list of usable osds
     */
    auto get_osd_ord = [&](bool is_acting, const pg_info_t &info) -> osd_ord_t {
      return std::make_tuple(
	!is_acting /* acting should sort first */,
	info.last_update);
    };
    for (auto &cand : acting) {
      auto &cand_info = osd_info(cand);
      if (!used(cand) && usable_info(cand_info)) {
	ss << " acting candidate " << cand << " " << cand_info << std::endl;
	candidates.push_back(std::make_pair(get_osd_ord(true, cand_info), cand));
      }
    }
    if (!restrict_to_up_acting) {
      for (auto &[cand, info] : all_info) {
	if (!used(cand.osd) && usable_info(info) &&
	    (std::find(acting.begin(), acting.end(), cand.osd)
	     == acting.end())) {
	  ss << " other candidate " << cand << " " << info << std::endl;
	  candidates.push_back(
	    std::make_pair(get_osd_ord(false, info), cand.osd));
	}
      }
    }
    std::sort(candidates.begin(), candidates.end());

    // We then filter these candidates by ancestor
    std::for_each(candidates.begin(), candidates.end(), [&](auto cand) {
      get_ancestor(cand.second)->add_osd(cand.first, cand.second);
    });
  }

  auto pop_ancestor = [&](auto &ancestor) {
    ceph_assert(!ancestor.is_empty());
    auto osd = ancestor.pop_osd();

    ss << " accepting candidate " << osd << std::endl;

    ceph_assert(!used(osd));
    ceph_assert(usable_osd(osd));

    want->push_back(osd);
    acting_backfill->insert(
      pg_shard_t(osd, shard_id_t::NO_SHARD));
    ancestor.inc_selected();
  };

  /* Next, we use the ancestors map to grab a descendant of the
   * peering_crush_mandatory_member if not already represented.
   *
   * TODO: using 0 here to match other users.  Prior to merge, I
   * expect that this and other users should instead check against
   * CRUSH_ITEM_NONE.
   */
  if (pool.info.peering_crush_mandatory_member != CRUSH_ITEM_NONE) {
    auto aiter = ancestors.find(pool.info.peering_crush_mandatory_member);
    if (aiter != ancestors.end() &&
	!aiter->second.get_num_selected()) {
      ss << " adding required ancestor " << aiter->first << std::endl;
      ceph_assert(!aiter->second.is_empty()); // wouldn't exist otherwise
      pop_ancestor(aiter->second);
    }
  }

  /* We then place the ancestors in a heap ordered by fewest selected
   * and then by the ordering token of the next osd */
  bucket_heap_t aheap;
  std::for_each(ancestors.begin(), ancestors.end(), [&](auto &anc) {
    aheap.push_if_nonempty(anc.second);
  });

  /* and pull from this heap until it's empty or we have enough.
   * "We have enough" is a sufficient check here for
   * stretch_set_can_peer() because our heap sorting always
   * pulls from ancestors with the least number of included OSDs,
   * so if it is possible to satisfy the bucket_count constraints we
   * will do so.
   */
  while (!aheap.is_empty() && want->size() < pool.info.size) {
    auto next = aheap.pop();
    pop_ancestor(next.get());
    if (next.get().get_num_selected() < bucket_max) {
      aheap.push_if_nonempty(next);
    }
  }

  /* The end result is that we should have as many buckets covered as
   * possible while respecting up, the primary selection,
   * the pool size (given bucket count constraints),
   * and the mandatory member.
   */
}


bool PeeringState::recoverable(const vector<int> &want) const
{
  unsigned num_want_acting = 0;
  set<pg_shard_t> have;
  for (int i = 0; i < (int)want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE) {
      ++num_want_acting;
      have.insert(
        pg_shard_t(
          want[i],
          pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
  }

  if (num_want_acting < pool.info.min_size) {
    if (!cct->_conf.get_val<bool>("osd_allow_recovery_below_min_size")) {
      psdout(10) << "failed, recovery below min size not enabled" << dendl;
      return false;
    }
  }
  if (missing_loc.get_recoverable_predicate()(have)) {
    return true;
  } else {
    psdout(10) << "failed, not recoverable " << dendl;
    return false;
  }
}

void PeeringState::choose_async_recovery_ec(
  const map<pg_shard_t, pg_info_t> &all_info,
  const pg_info_t &auth_info,
  vector<int> *want,
  set<pg_shard_t> *async_recovery,
  const OSDMapRef osdmap) const
{
  set<pair<int, pg_shard_t> > candidates_by_cost;
  unsigned want_acting_size = 0;
  for (uint8_t i = 0; i < want->size(); ++i) {
    if ((*want)[i] == CRUSH_ITEM_NONE)
      continue;
    ++want_acting_size;

    // Considering log entries to recover is accurate enough for
    // now. We could use minimum_to_decode_with_cost() later if
    // necessary.
    pg_shard_t shard_i((*want)[i], shard_id_t(i));
    // do not include strays
    if (stray_set.find(shard_i) != stray_set.end())
      continue;
    // Do not include an osd that is not up, since choosing it as
    // an async_recovery_target will move it out of the acting set.
    // This results in it being identified as a stray during peering,
    // because it is no longer in the up or acting set.
    if (!is_up(shard_i))
      continue;
    auto shard_info = all_info.find(shard_i)->second;
    // for ec pools we rollback all entries past the authoritative
    // last_update *before* activation. This is relatively inexpensive
    // compared to recovery, since it is purely local, so treat shards
    // past the authoritative last_update the same as those equal to it.
    version_t auth_version = auth_info.last_update.version;
    version_t candidate_version = shard_info.last_update.version;
    assert(HAVE_FEATURE(osdmap->get_up_osd_features(), SERVER_NAUTILUS));
    auto approx_missing_objects =
      shard_info.stats.stats.sum.num_objects_missing;
    if (auth_version > candidate_version) {
      approx_missing_objects += auth_version - candidate_version;
    }
    if (static_cast<uint64_t>(approx_missing_objects) >
       cct->_conf.get_val<uint64_t>("osd_async_recovery_min_cost")) {
      candidates_by_cost.emplace(approx_missing_objects, shard_i);
    }
  }
  ceph_assert(candidates_by_cost.size() <= want_acting_size);

  psdout(20) << "candidates by cost are: " << candidates_by_cost
	     << dendl;

  // take out as many osds as we can for async recovery, in order of cost
  for (auto rit = candidates_by_cost.rbegin();
       rit != candidates_by_cost.rend(); ++rit) {
    pg_shard_t cur_shard = rit->second;
    vector<int> candidate_want(*want);
    candidate_want[cur_shard.shard.id] = CRUSH_ITEM_NONE;
    ceph_assert(want_acting_size > 0);
    --want_acting_size;
    if ((want_acting_size >= pool.info.min_size) &&
	recoverable(candidate_want)) {
      want->swap(candidate_want);
      async_recovery->insert(cur_shard);
    }
  }
  psdout(20) << "result want=" << *want
	     << " async_recovery=" << *async_recovery << dendl;
}

void PeeringState::choose_async_recovery_replicated(
  const map<pg_shard_t, pg_info_t> &all_info,
  const pg_info_t &auth_info,
  vector<int> *want,
  set<pg_shard_t> *async_recovery,
  const OSDMapRef osdmap) const
{
  set<pair<int, pg_shard_t> > candidates_by_cost;
  for (auto osd_num : *want) {
    pg_shard_t shard_i(osd_num, shard_id_t::NO_SHARD);
    // do not include strays
    if (stray_set.find(shard_i) != stray_set.end())
      continue;
    // Do not include an osd that is not up, since choosing it as
    // an async_recovery_target will move it out of the acting set.
    // This results in it being identified as a stray during peering,
    // because it is no longer in the up or acting set.
    if (!is_up(shard_i))
      continue;
    auto shard_info = all_info.find(shard_i)->second;
    // use the approximate magnitude of the difference in length of
    // logs plus historical missing objects as the cost of recovery
    version_t auth_version = auth_info.last_update.version;
    version_t candidate_version = shard_info.last_update.version;
    assert(HAVE_FEATURE(osdmap->get_up_osd_features(), SERVER_NAUTILUS));
    auto approx_missing_objects =
      shard_info.stats.stats.sum.num_objects_missing;
    if (auth_version > candidate_version) {
      approx_missing_objects += auth_version - candidate_version;
    } else {
      approx_missing_objects += candidate_version - auth_version;
    }
    if (static_cast<uint64_t>(approx_missing_objects)  >
       cct->_conf.get_val<uint64_t>("osd_async_recovery_min_cost")) {
      candidates_by_cost.emplace(approx_missing_objects, shard_i);
    }
  }

  psdout(20) << "candidates by cost are: " << candidates_by_cost
	     << dendl;
  // take out as many osds as we can for async recovery, in order of cost
  for (auto rit = candidates_by_cost.rbegin();
       rit != candidates_by_cost.rend(); ++rit) {
    if (want->size() <= pool.info.min_size) {
      break;
    }
    pg_shard_t cur_shard = rit->second;
    vector<int> candidate_want(*want);
    for (auto it = candidate_want.begin(); it != candidate_want.end(); ++it) {
      if (*it == cur_shard.osd) {
	candidate_want.erase(it);
	if (pool.info.stretch_set_can_peer(candidate_want, *osdmap, NULL)) {
	  // if we're in stretch mode, we can only remove the osd if it doesn't
	  // break peering limits.
	  want->swap(candidate_want);
	  async_recovery->insert(cur_shard);
	}
	break;
      }
    }
  }

  psdout(20) << "result want=" << *want
	     << " async_recovery=" << *async_recovery << dendl;
}

/**
 * choose acting
 *
 * calculate the desired acting, and request a change with the monitor
 * if it differs from the current acting.
 *
 * if restrict_to_up_acting=true, we filter out anything that's not in
 * up/acting.  in order to lift this restriction, we need to
 *  1) check whether it's worth switching the acting set any time we get
 *     a new pg info (not just here, when recovery finishes)
 *  2) check whether anything in want_acting went down on each new map
 *     (and, if so, calculate a new want_acting)
 *  3) remove the assertion in PG::PeeringState::Active::react(const AdvMap)
 * TODO!
 */
bool PeeringState::choose_acting(pg_shard_t &auth_log_shard_id,
				 bool restrict_to_up_acting,
				 bool *history_les_bound,
				 bool request_pg_temp_change_only)
{
  map<pg_shard_t, pg_info_t> all_info(peer_info.begin(), peer_info.end());
  all_info[pg_whoami] = info;

  if (cct->_conf->subsys.should_gather<dout_subsys, 10>()) {
    for (auto p = all_info.begin(); p != all_info.end(); ++p) {
      psdout(10) << "all_info osd." << p->first << " "
		 << p->second << dendl;
    }
  }

  auto auth_log_shard = find_best_info(all_info, restrict_to_up_acting,
				       history_les_bound);

  if (auth_log_shard == all_info.end()) {
    if (up != acting) {
      psdout(10) << "no suitable info found (incomplete backfills?),"
		 << " reverting to up" << dendl;
      want_acting = up;
      vector<int> empty;
      pl->queue_want_pg_temp(empty);
    } else {
      psdout(10) << "failed" << dendl;
      ceph_assert(want_acting.empty());
    }
    return false;
  }

  ceph_assert(!auth_log_shard->second.is_incomplete());
  auth_log_shard_id = auth_log_shard->first;

  set<pg_shard_t> want_backfill, want_acting_backfill;
  vector<int> want;
  stringstream ss;
  if (pool.info.is_replicated()) {
    auto [primary_shard, oldest_log] = select_replicated_primary(
      auth_log_shard,
      cct->_conf.get_val<uint64_t>(
	"osd_force_auth_primary_missing_objects"),
      up,
      up_primary,
      all_info,
      get_osdmap(),
      ss);
    if (pool.info.is_stretch_pool()) {
      calc_replicated_acting_stretch(
	primary_shard,
	oldest_log,
	get_osdmap()->get_pg_size(info.pgid.pgid),
	acting,
	up,
	up_primary,
	all_info,
	restrict_to_up_acting,
	&want,
	&want_backfill,
	&want_acting_backfill,
	get_osdmap(),
	pool,
	ss);
    } else {
      calc_replicated_acting(
	primary_shard,
	oldest_log,
	get_osdmap()->get_pg_size(info.pgid.pgid),
	acting,
	up,
	up_primary,
	all_info,
	restrict_to_up_acting,
	&want,
	&want_backfill,
	&want_acting_backfill,
	get_osdmap(),
	pool,
	ss);
    }
  } else {
    calc_ec_acting(
      auth_log_shard,
      get_osdmap()->get_pg_size(info.pgid.pgid),
      acting,
      up,
      all_info,
      restrict_to_up_acting,
      &want,
      &want_backfill,
      &want_acting_backfill,
      ss);
  }
  psdout(10) << ss.str() << dendl;

  if (!recoverable(want)) {
    want_acting.clear();
    return false;
  }

  set<pg_shard_t> want_async_recovery;
  if (HAVE_FEATURE(get_osdmap()->get_up_osd_features(), SERVER_MIMIC)) {
    if (pool.info.is_erasure()) {
      choose_async_recovery_ec(
	all_info, auth_log_shard->second, &want, &want_async_recovery,
	get_osdmap());
    } else {
      choose_async_recovery_replicated(
	all_info, auth_log_shard->second, &want, &want_async_recovery,
	get_osdmap());
    }
  }
  while (want.size() > pool.info.size) {
    // async recovery should have taken out as many osds as it can.
    // if not, then always evict the last peer
    // (will get synchronously recovered later)
    psdout(10) << "evicting osd." << want.back()
               << " from oversized want " << want << dendl;
    want.pop_back();
  }
  if (want != acting) {
    psdout(10) << "want " << want << " != acting " << acting
	       << ", requesting pg_temp change" << dendl;
    want_acting = want;

    if (!cct->_conf->osd_debug_no_acting_change) {
      if (want_acting == up) {
	// There can't be any pending backfill if
	// want is the same as crush map up OSDs.
	ceph_assert(want_backfill.empty());
	vector<int> empty;
	pl->queue_want_pg_temp(empty);
      } else
	pl->queue_want_pg_temp(want);
    }
    return false;
  }

  if (request_pg_temp_change_only)
    return true;
  want_acting.clear();
  acting_recovery_backfill = want_acting_backfill;
  psdout(10) << "acting_recovery_backfill is "
	     << acting_recovery_backfill << dendl;
  ceph_assert(
    backfill_targets.empty() ||
    backfill_targets == want_backfill);
  if (backfill_targets.empty()) {
    // Caller is GetInfo
    backfill_targets = want_backfill;
  }
  // Adding !needs_recovery() to let the async_recovery_targets reset after recovery is complete
  ceph_assert(
    async_recovery_targets.empty() ||
    async_recovery_targets == want_async_recovery ||
    !needs_recovery());
  if (async_recovery_targets.empty() || !needs_recovery()) {
    async_recovery_targets = want_async_recovery;
  }
  // Will not change if already set because up would have had to change
  // Verify that nothing in backfill is in stray_set
  for (auto i = want_backfill.begin(); i != want_backfill.end(); ++i) {
    ceph_assert(stray_set.find(*i) == stray_set.end());
  }
  psdout(10) << "choose_acting want=" << want << " backfill_targets="
           << want_backfill << " async_recovery_targets="
           << async_recovery_targets << dendl;
  return true;
}

void PeeringState::log_weirdness()
{
  if (pg_log.get_tail() != info.log_tail)
    pl->get_clog_error() << info.pgid
			   << " info mismatch, log.tail " << pg_log.get_tail()
			   << " != info.log_tail " << info.log_tail;
  if (pg_log.get_head() != info.last_update)
    pl->get_clog_error() << info.pgid
			   << " info mismatch, log.head " << pg_log.get_head()
			   << " != info.last_update " << info.last_update;

  if (!pg_log.get_log().empty()) {
    // sloppy check
    if ((pg_log.get_log().log.begin()->version <= pg_log.get_tail()))
      pl->get_clog_error() << info.pgid
			     << " log bound mismatch, info (tail,head] ("
			     << pg_log.get_tail() << ","
			     << pg_log.get_head() << "]"
			     << " actual ["
			     << pg_log.get_log().log.begin()->version << ","
			     << pg_log.get_log().log.rbegin()->version << "]";
  }

  if (pg_log.get_log().caller_ops.size() > pg_log.get_log().log.size()) {
    pl->get_clog_error() << info.pgid
			   << " caller_ops.size "
			   << pg_log.get_log().caller_ops.size()
			   << " > log size " << pg_log.get_log().log.size();
  }
}

/*
 * Process information from a replica to determine if it could have any
 * objects that i need.
 *
 * TODO: if the missing set becomes very large, this could get expensive.
 * Instead, we probably want to just iterate over our unfound set.
 */
bool PeeringState::search_for_missing(
  const pg_info_t &oinfo, const pg_missing_t &omissing,
  pg_shard_t from,
  PeeringCtxWrapper &ctx)
{
  uint64_t num_unfound_before = missing_loc.num_unfound();
  bool found_missing = missing_loc.add_source_info(
    from, oinfo, omissing, ctx.handle);
  if (found_missing && num_unfound_before != missing_loc.num_unfound())
    pl->publish_stats_to_osd();
  // avoid doing this if the peer is empty.  This is abit of paranoia
  // to avoid doing something rash if add_source_info() above
  // incorrectly decided we found something new. (if the peer has
  // last_update=0'0 that's impossible.)
  if (found_missing &&
      oinfo.last_update != eversion_t()) {
    pg_info_t tinfo(oinfo);
    tinfo.pgid.shard = pg_whoami.shard;
    ctx.send_info(
      from.osd,
      spg_t(info.pgid.pgid, from.shard),
      get_osdmap_epoch(),  // fixme: use lower epoch?
      get_osdmap_epoch(),
      tinfo);
  }
  return found_missing;
}

bool PeeringState::discover_all_missing(
  BufferedRecoveryMessages &rctx)
{
  auto &missing = pg_log.get_missing();
  uint64_t unfound = get_num_unfound();
  bool any = false;  // did we start any queries

  psdout(10) << missing.num_missing() << " missing, "
             << unfound << " unfound"
             << dendl;

  auto m = might_have_unfound.begin();
  auto mend = might_have_unfound.end();
  for (; m != mend; ++m) {
    pg_shard_t peer(*m);

    if (!get_osdmap()->is_up(peer.osd)) {
      psdout(20) << "skipping down osd." << peer << dendl;
      continue;
    }

    if (peer_purged.count(peer)) {
      psdout(20) << "skipping purged osd." << peer << dendl;
      continue;
    }

    auto iter = peer_info.find(peer);
    if (iter != peer_info.end() &&
        (iter->second.is_empty() || iter->second.dne())) {
      // ignore empty peers
      continue;
    }

    // If we've requested any of this stuff, the pg_missing_t information
    // should be on its way.
    // TODO: coalsce requested_* into a single data structure
    if (peer_missing.find(peer) != peer_missing.end()) {
      psdout(20) << ": osd." << peer
		 << ": we already have pg_missing_t" << dendl;
      continue;
    }
    if (peer_log_requested.find(peer) != peer_log_requested.end()) {
      psdout(20) << ": osd." << peer
		 << ": in peer_log_requested" << dendl;
      continue;
    }
    if (peer_missing_requested.find(peer) != peer_missing_requested.end()) {
      psdout(20) << ": osd." << peer
		 << ": in peer_missing_requested" << dendl;
      continue;
    }

    // Request missing
    psdout(10) << ": osd." << peer << ": requesting pg_missing_t"
	       << dendl;
    peer_missing_requested.insert(peer);
    rctx.send_query(
      peer.osd,
      spg_t(info.pgid.pgid, peer.shard),
      pg_query_t(
	pg_query_t::FULLLOG,
	peer.shard, pg_whoami.shard,
	info.history, get_osdmap_epoch()));
    any = true;
  }
  return any;
}

/* Build the might_have_unfound set.
 *
 * This is used by the primary OSD during recovery.
 *
 * This set tracks the OSDs which might have unfound objects that the primary
 * OSD needs. As we receive pg_missing_t from each OSD in might_have_unfound, we
 * will remove the OSD from the set.
 */
void PeeringState::build_might_have_unfound()
{
  ceph_assert(might_have_unfound.empty());
  ceph_assert(is_primary());

  psdout(10) << dendl;

  check_past_interval_bounds();

  might_have_unfound = past_intervals.get_might_have_unfound(
    pg_whoami,
    pool.info.is_erasure());

  // include any (stray) peers
  for (auto p = peer_info.begin(); p != peer_info.end(); ++p)
    might_have_unfound.insert(p->first);

  psdout(15) << ": built " << might_have_unfound << dendl;
}

void PeeringState::activate(
  ObjectStore::Transaction& t,
  epoch_t activation_epoch,
  PeeringCtxWrapper &ctx)
{
  ceph_assert(!is_peered());

  // twiddle pg state
  state_clear(PG_STATE_DOWN);

  send_notify = false;

  if (is_primary()) {
    // only update primary last_epoch_started if we will go active
    if (acting_set_writeable()) {
      ceph_assert(cct->_conf->osd_find_best_info_ignore_history_les ||
	     info.last_epoch_started <= activation_epoch);
      info.last_epoch_started = activation_epoch;
      info.last_interval_started = info.history.same_interval_since;
    }
  } else if (is_acting(pg_whoami)) {
    /* update last_epoch_started on acting replica to whatever the primary sent
     * unless it's smaller (could happen if we are going peered rather than
     * active, see doc/dev/osd_internals/last_epoch_started.rst) */
    if (info.last_epoch_started < activation_epoch) {
      info.last_epoch_started = activation_epoch;
      info.last_interval_started = info.history.same_interval_since;
    }
  }

  auto &missing = pg_log.get_missing();

  min_last_complete_ondisk = eversion_t(0,0);  // we don't know (yet)!
  if (is_primary()) {
    last_update_ondisk = info.last_update;
  }
  last_update_applied = info.last_update;
  last_rollback_info_trimmed_to_applied = pg_log.get_can_rollback_to();

  need_up_thru = false;

  // write pg info, log
  dirty_info = true;
  dirty_big_info = true; // maybe

  pl->schedule_event_on_commit(
    t,
    std::make_shared<PGPeeringEvent>(
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      ActivateCommitted(
	get_osdmap_epoch(),
	activation_epoch)));

  // init complete pointer
  if (missing.num_missing() == 0) {
    psdout(10) << "activate - no missing, moving last_complete " << info.last_complete
	     << " -> " << info.last_update << dendl;
    info.last_complete = info.last_update;
    info.stats.stats.sum.num_objects_missing = 0;
    pg_log.reset_recovery_pointers();
  } else {
    psdout(10) << "activate - not complete, " << missing << dendl;
    info.stats.stats.sum.num_objects_missing = missing.num_missing();
    pg_log.activate_not_complete(info);
  }

  log_weirdness();

  if (is_primary()) {
    // initialize snap_trimq
    interval_set<snapid_t> to_trim;
    auto& removed_snaps_queue = get_osdmap()->get_removed_snaps_queue();
    auto p = removed_snaps_queue.find(info.pgid.pgid.pool());
    if (p != removed_snaps_queue.end()) {
      dout(20) << "activate - purged_snaps " << info.purged_snaps
	       << " removed_snaps " << p->second
	       << dendl;
      for (auto q : p->second) {
	to_trim.insert(q.first, q.second);
      }
    }
    interval_set<snapid_t> purged;
    purged.intersection_of(to_trim, info.purged_snaps);
    to_trim.subtract(purged);

    assert(HAVE_FEATURE(upacting_features, SERVER_OCTOPUS));
    renew_lease(pl->get_mnow());
    // do not schedule until we are actually activated

    // adjust purged_snaps: PG may have been inactive while snaps were pruned
    // from the removed_snaps_queue in the osdmap.  update local purged_snaps
    // reflect only those snaps that we thought were pruned and were still in
    // the queue.
    info.purged_snaps.swap(purged);

    // start up replicas
    if (prior_readable_down_osds.empty()) {
      dout(10) << "no prior_readable_down_osds to wait on, clearing ub"
	       << dendl;
      clear_prior_readable_until_ub();
    }
    info.history.refresh_prior_readable_until_ub(pl->get_mnow(),
						 prior_readable_until_ub);

    ceph_assert(!acting_recovery_backfill.empty());
    for (auto i = acting_recovery_backfill.begin();
	 i != acting_recovery_backfill.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      pg_shard_t peer = *i;
      auto pi_it = peer_info.find(peer);
      ceph_assert(pi_it != peer_info.end());
      pg_info_t& pi = pi_it->second;

      psdout(10) << "activate peer osd." << peer << " " << pi << dendl;

      #if defined(WITH_SEASTAR)
      MURef<MOSDPGLog> m;
      #else
      MRef<MOSDPGLog> m;
      #endif
      auto pm_it = peer_missing.find(peer);
      ceph_assert(pm_it != peer_missing.end());
      pg_missing_t& pm = pm_it->second;

      bool needs_past_intervals = pi.dne();

      // Save num_bytes for backfill reservation request, can't be negative
      peer_bytes[peer] = std::max<int64_t>(0, pi.stats.stats.sum.num_bytes);

      if (pi.last_update == info.last_update) {
        // empty log
	if (!pi.last_backfill.is_max())
	  pl->get_clog_info() << info.pgid << " continuing backfill to osd."
				<< peer
				<< " from (" << pi.log_tail << "," << pi.last_update
				<< "] " << pi.last_backfill
				<< " to " << info.last_update;
	if (!pi.is_empty()) {
	  psdout(10) << "activate peer osd." << peer
		     << " is up to date, queueing in pending_activators" << dendl;
	  ctx.send_info(
	    peer.osd,
	    spg_t(info.pgid.pgid, peer.shard),
	    get_osdmap_epoch(), // fixme: use lower epoch?
	    get_osdmap_epoch(),
	    info,
	    get_lease());
	} else {
	  psdout(10) << "activate peer osd." << peer
		     << " is up to date, but sending pg_log anyway" << dendl;
	  m = TOPNSPC::make_message<MOSDPGLog>(
	    i->shard, pg_whoami.shard,
	    get_osdmap_epoch(), info,
	    last_peering_reset);
	}
      } else if (
	pg_log.get_tail() > pi.last_update ||
	pi.last_backfill == hobject_t() ||
	(backfill_targets.count(*i) && pi.last_backfill.is_max())) {
	/* ^ This last case covers a situation where a replica is not contiguous
	 * with the auth_log, but is contiguous with this replica.  Reshuffling
	 * the active set to handle this would be tricky, so instead we just go
	 * ahead and backfill it anyway.  This is probably preferrable in any
	 * case since the replica in question would have to be significantly
	 * behind.
	 */
	// backfill
	pl->get_clog_debug() << info.pgid << " starting backfill to osd." << peer
			       << " from (" << pi.log_tail << "," << pi.last_update
			       << "] " << pi.last_backfill
			       << " to " << info.last_update;

	pi.last_update = info.last_update;
	pi.last_complete = info.last_update;
	pi.set_last_backfill(hobject_t());
	pi.last_epoch_started = info.last_epoch_started;
	pi.last_interval_started = info.last_interval_started;
	pi.history = info.history;
	pi.hit_set = info.hit_set;
        pi.stats.stats.clear();
        pi.stats.stats.sum.num_bytes = peer_bytes[peer];

	// initialize peer with our purged_snaps.
	pi.purged_snaps = info.purged_snaps;

	m = TOPNSPC::make_message<MOSDPGLog>(
	  i->shard, pg_whoami.shard,
	  get_osdmap_epoch(), pi,
	  last_peering_reset /* epoch to create pg at */);

	// send some recent log, so that op dup detection works well.
	m->log.copy_up_to(cct, pg_log.get_log(),
			  cct->_conf->osd_max_pg_log_entries);
	m->info.log_tail = m->log.tail;
	pi.log_tail = m->log.tail;  // sigh...

	pm.clear();
      } else {
	// catch up
	ceph_assert(pg_log.get_tail() <= pi.last_update);
	m = TOPNSPC::make_message<MOSDPGLog>(
	  i->shard, pg_whoami.shard,
	  get_osdmap_epoch(), info,
	  last_peering_reset /* epoch to create pg at */);
	// send new stuff to append to replicas log
	m->log.copy_after(cct, pg_log.get_log(), pi.last_update);
      }

      // share past_intervals if we are creating the pg on the replica
      // based on whether our info for that peer was dne() *before*
      // updating pi.history in the backfill block above.
      if (m && needs_past_intervals)
	m->past_intervals = past_intervals;

      // update local version of peer's missing list!
      if (m && pi.last_backfill != hobject_t()) {
        for (auto p = m->log.log.begin(); p != m->log.log.end(); ++p) {
	  if (p->soid <= pi.last_backfill &&
	      !p->is_error()) {
	    if (perform_deletes_during_peering() && p->is_delete()) {
	      pm.rm(p->soid, p->version);
	    } else {
	      pm.add_next_event(*p);
	    }
	  }
	}
      }

      if (m) {
	dout(10) << "activate peer osd." << peer << " sending " << m->log
		 << dendl;
	m->lease = get_lease();
	pl->send_cluster_message(peer.osd, std::move(m), get_osdmap_epoch());
      }

      // peer now has
      pi.last_update = info.last_update;

      // update our missing
      if (pm.num_missing() == 0) {
	pi.last_complete = pi.last_update;
        psdout(10) << "activate peer osd." << peer << " " << pi
		   << " uptodate" << dendl;
      } else {
        psdout(10) << "activate peer osd." << peer << " " << pi
		   << " missing " << pm << dendl;
      }
    }

    // Set up missing_loc
    set<pg_shard_t> complete_shards;
    for (auto i = acting_recovery_backfill.begin();
	 i != acting_recovery_backfill.end();
	 ++i) {
      psdout(20) << "setting up missing_loc from shard " << *i
		 << " " << dendl;
      if (*i == get_primary()) {
	missing_loc.add_active_missing(missing);
        if (!missing.have_missing())
          complete_shards.insert(*i);
      } else {
	auto peer_missing_entry = peer_missing.find(*i);
	ceph_assert(peer_missing_entry != peer_missing.end());
	missing_loc.add_active_missing(peer_missing_entry->second);
        if (!peer_missing_entry->second.have_missing() &&
	    peer_info[*i].last_backfill.is_max())
	  complete_shards.insert(*i);
      }
    }

    // If necessary, create might_have_unfound to help us find our unfound objects.
    // NOTE: It's important that we build might_have_unfound before trimming the
    // past intervals.
    might_have_unfound.clear();
    if (needs_recovery()) {
      // If only one shard has missing, we do a trick to add all others as recovery
      // source, this is considered safe since the PGLogs have been merged locally,
      // and covers vast majority of the use cases, like one OSD/host is down for
      // a while for hardware repairing
      if (complete_shards.size() + 1 == acting_recovery_backfill.size()) {
        missing_loc.add_batch_sources_info(complete_shards, ctx.handle);
      } else {
        missing_loc.add_source_info(pg_whoami, info, pg_log.get_missing(),
				    ctx.handle);
        for (auto i = acting_recovery_backfill.begin();
	     i != acting_recovery_backfill.end();
	     ++i) {
	  if (*i == pg_whoami) continue;
	  psdout(10) << ": adding " << *i << " as a source" << dendl;
	  auto pi_it = peer_info.find(*i);
	  ceph_assert(pi_it != peer_info.end());
	  auto pm_it = peer_missing.find(*i);
	  ceph_assert(pm_it != peer_missing.end());
	  missing_loc.add_source_info(
	    *i,
	    pi_it->second,
	    pm_it->second,
            ctx.handle);
        }
      }
      for (auto i = peer_missing.begin(); i != peer_missing.end(); ++i) {
	if (is_acting_recovery_backfill(i->first))
	  continue;
	auto pi_it = peer_info.find(i->first);
	ceph_assert(pi_it != peer_info.end());
	search_for_missing(
	  pi_it->second,
	  i->second,
	  i->first,
	  ctx);
      }

      build_might_have_unfound();

      // Always call now so update_calc_stats() will be accurate
      discover_all_missing(ctx.msgs);

    }

    // num_objects_degraded if calculated should reflect this too, unless no
    // missing and we are about to go clean.
    if (get_osdmap()->get_pg_size(info.pgid.pgid) > actingset.size()) {
      state_set(PG_STATE_UNDERSIZED);
    }

    state_set(PG_STATE_ACTIVATING);
    pl->on_activate(std::move(to_trim));
  } else {
    pl->on_replica_activate();
  }
  if (acting_set_writeable()) {
    PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
    pg_log.roll_forward(rollbacker.get());
  }
}

void PeeringState::share_pg_info()
{
  psdout(10) << "share_pg_info" << dendl;

  info.history.refresh_prior_readable_until_ub(pl->get_mnow(),
					       prior_readable_until_ub);

  // share new pg_info_t with replicas
  ceph_assert(!acting_recovery_backfill.empty());
  for (auto pg_shard : acting_recovery_backfill) {
    if (pg_shard == pg_whoami) continue;
    if (auto peer = peer_info.find(pg_shard); peer != peer_info.end()) {
      peer->second.last_epoch_started = info.last_epoch_started;
      peer->second.last_interval_started = info.last_interval_started;
      peer->second.history.merge(info.history);
    }
    auto m = TOPNSPC::make_message<MOSDPGInfo2>(spg_t{info.pgid.pgid, pg_shard.shard},
			  info,
			  get_osdmap_epoch(),
			  get_osdmap_epoch(),
			  std::optional<pg_lease_t>{get_lease()},
			  std::nullopt);
    pl->send_cluster_message(pg_shard.osd, std::move(m), get_osdmap_epoch());
  }
}

void PeeringState::merge_log(
  ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t&& olog,
  pg_shard_t from)
{
  PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
  pg_log.merge_log(
    oinfo, std::move(olog), from, info, rollbacker.get(),
    dirty_info, dirty_big_info);
}

void PeeringState::rewind_divergent_log(
  ObjectStore::Transaction& t, eversion_t newhead)
{
  PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
  pg_log.rewind_divergent_log(
    newhead, info, rollbacker.get(), dirty_info, dirty_big_info);
}


void PeeringState::proc_primary_info(
  ObjectStore::Transaction &t, const pg_info_t &oinfo)
{
  ceph_assert(!is_primary());

  update_history(oinfo.history);
  if (!info.stats.stats_invalid && info.stats.stats.sum.num_scrub_errors) {
    info.stats.stats.sum.num_scrub_errors = 0;
    info.stats.stats.sum.num_shallow_scrub_errors = 0;
    info.stats.stats.sum.num_deep_scrub_errors = 0;
    dirty_info = true;
  }

  if (!(info.purged_snaps == oinfo.purged_snaps)) {
    psdout(10) << "updating purged_snaps to "
	       << oinfo.purged_snaps
	       << dendl;
    info.purged_snaps = oinfo.purged_snaps;
    dirty_info = true;
    dirty_big_info = true;
  }
}

void PeeringState::proc_master_log(
  ObjectStore::Transaction& t, pg_info_t &oinfo,
  pg_log_t&& olog, pg_missing_t&& omissing, pg_shard_t from)
{
  psdout(10) << "proc_master_log for osd." << from << ": "
	     << olog << " " << omissing << dendl;
  ceph_assert(!is_peered() && is_primary());

  // merge log into our own log to build master log.  no need to
  // make any adjustments to their missing map; we are taking their
  // log to be authoritative (i.e., their entries are by definitely
  // non-divergent).
  merge_log(t, oinfo, std::move(olog), from);
  peer_info[from] = oinfo;
  psdout(10) << " peer osd." << from << " now " << oinfo
	     << " " << omissing << dendl;
  might_have_unfound.insert(from);

  // See doc/dev/osd_internals/last_epoch_started
  if (oinfo.last_epoch_started > info.last_epoch_started) {
    info.last_epoch_started = oinfo.last_epoch_started;
    dirty_info = true;
  }
  if (oinfo.last_interval_started > info.last_interval_started) {
    info.last_interval_started = oinfo.last_interval_started;
    dirty_info = true;
  }
  update_history(oinfo.history);
  ceph_assert(cct->_conf->osd_find_best_info_ignore_history_les ||
	 info.last_epoch_started >= info.history.last_epoch_started);

  peer_missing[from].claim(std::move(omissing));
}

void PeeringState::proc_replica_log(
  pg_info_t &oinfo,
  const pg_log_t &olog,
  pg_missing_t&& omissing,
  pg_shard_t from)
{
  psdout(10) << "proc_replica_log for osd." << from << ": "
	     << oinfo << " " << olog << " " << omissing << dendl;

  pg_log.proc_replica_log(oinfo, olog, omissing, from);

  peer_info[from] = oinfo;
  psdout(10) << " peer osd." << from << " now "
	     << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  for (auto i = omissing.get_items().begin();
       i != omissing.get_items().end();
       ++i) {
    psdout(20) << " after missing " << i->first
	       << " need " << i->second.need
	       << " have " << i->second.have << dendl;
  }
  peer_missing[from].claim(std::move(omissing));
}

/**
* Update min_last_complete_ondisk to the minimum
* last_complete_ondisk version informed by each peer.
*/
void PeeringState::calc_min_last_complete_ondisk() {
  ceph_assert(!acting_recovery_backfill.empty());
  eversion_t min = last_complete_ondisk;
  for (const auto& pg_shard : acting_recovery_backfill) {
    if (pg_shard == get_primary()) {
      continue;
    }
    if (peer_last_complete_ondisk.count(pg_shard) == 0) {
      psdout(20) <<  "no complete info on: "
                 << pg_shard << dendl;
      return;
    }
    if (peer_last_complete_ondisk[pg_shard] < min) {
      min = peer_last_complete_ondisk[pg_shard];
    }
  }
  if (min != min_last_complete_ondisk) {
    psdout(20) << "last_complete_ondisk is "
               << "updated to: " << min
               << " from: " << min_last_complete_ondisk
               << dendl;
    min_last_complete_ondisk = min;
  }
}

void PeeringState::fulfill_info(
  pg_shard_t from, const pg_query_t &query,
  pair<pg_shard_t, pg_info_t> &notify_info)
{
  ceph_assert(from == primary);
  ceph_assert(query.type == pg_query_t::INFO);

  // info
  psdout(10) << "sending info" << dendl;
  notify_info = make_pair(from, info);
}

void PeeringState::fulfill_log(
  pg_shard_t from, const pg_query_t &query, epoch_t query_epoch)
{
  psdout(10) << "log request from " << from << dendl;
  ceph_assert(from == primary);
  ceph_assert(query.type != pg_query_t::INFO);

  auto mlog = TOPNSPC::make_message<MOSDPGLog>(
    from.shard, pg_whoami.shard,
    get_osdmap_epoch(),
    info, query_epoch);
  mlog->missing = pg_log.get_missing();

  // primary -> other, when building master log
  if (query.type == pg_query_t::LOG) {
    psdout(10) << " sending info+missing+log since " << query.since
	       << dendl;
    if (query.since != eversion_t() && query.since < pg_log.get_tail()) {
      pl->get_clog_error() << info.pgid << " got broken pg_query_t::LOG since "
			     << query.since
			     << " when my log.tail is " << pg_log.get_tail()
			     << ", sending full log instead";
      mlog->log = pg_log.get_log();           // primary should not have requested this!!
    } else
      mlog->log.copy_after(cct, pg_log.get_log(), query.since);
  }
  else if (query.type == pg_query_t::FULLLOG) {
    psdout(10) << " sending info+missing+full log" << dendl;
    mlog->log = pg_log.get_log();
  }

  psdout(10) << " sending " << mlog->log << " " << mlog->missing << dendl;

  pl->send_cluster_message(from.osd, std::move(mlog), get_osdmap_epoch(), true);
}

void PeeringState::fulfill_query(const MQuery& query, PeeringCtxWrapper &rctx)
{
  if (query.query.type == pg_query_t::INFO) {
    pair<pg_shard_t, pg_info_t> notify_info;
    // note this refreshes our prior_readable_until_ub value
    update_history(query.query.history);
    fulfill_info(query.from, query.query, notify_info);
    rctx.send_notify(
      notify_info.first.osd,
      pg_notify_t(
	notify_info.first.shard, pg_whoami.shard,
	query.query_epoch,
	get_osdmap_epoch(),
	notify_info.second,
	past_intervals));
  } else {
    update_history(query.query.history);
    fulfill_log(query.from, query.query, query.query_epoch);
  }
}

void PeeringState::try_mark_clean()
{
  if (actingset.size() == get_osdmap()->get_pg_size(info.pgid.pgid)) {
    state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
    state_set(PG_STATE_CLEAN);
    info.history.last_epoch_clean = get_osdmap_epoch();
    info.history.last_interval_clean = info.history.same_interval_since;
    past_intervals.clear();
    dirty_big_info = true;
    dirty_info = true;
  }

  if (!is_active() && is_peered()) {
    if (is_clean()) {
      bool target;
      if (pool.info.is_pending_merge(info.pgid.pgid, &target)) {
	if (target) {
	  psdout(10) << "ready to merge (target)" << dendl;
	  pl->set_ready_to_merge_target(
	    info.last_update,
	    info.history.last_epoch_started,
	    info.history.last_epoch_clean);
	} else {
	  psdout(10) << "ready to merge (source)" << dendl;
	  pl->set_ready_to_merge_source(info.last_update);
	}
      }
    } else {
      psdout(10) << "not clean, not ready to merge" << dendl;
      // we should have notified OSD in Active state entry point
    }
  }

  state_clear(PG_STATE_FORCED_RECOVERY | PG_STATE_FORCED_BACKFILL);

  share_pg_info();
  pl->publish_stats_to_osd();
  clear_recovery_state();
}

void PeeringState::split_into(
  pg_t child_pgid, PeeringState *child, unsigned split_bits)
{
  child->update_osdmap_ref(get_osdmap());
  child->pool = pool;

  // Log
  pg_log.split_into(child_pgid, split_bits, &(child->pg_log));
  child->info.last_complete = info.last_complete;

  info.last_update = pg_log.get_head();
  child->info.last_update = child->pg_log.get_head();

  child->info.last_user_version = info.last_user_version;

  info.log_tail = pg_log.get_tail();
  child->info.log_tail = child->pg_log.get_tail();

  // reset last_complete, we might have modified pg_log & missing above
  pg_log.reset_complete_to(&info);
  child->pg_log.reset_complete_to(&child->info);

  // Info
  child->info.history = info.history;
  child->info.history.epoch_created = get_osdmap_epoch();
  child->info.purged_snaps = info.purged_snaps;

  if (info.last_backfill.is_max()) {
    child->info.set_last_backfill(hobject_t::get_max());
  } else {
    // restart backfill on parent and child to be safe.  we could
    // probably do better in the bitwise sort case, but it's more
    // fragile (there may be special work to do on backfill completion
    // in the future).
    info.set_last_backfill(hobject_t());
    child->info.set_last_backfill(hobject_t());
    // restarting backfill implies that the missing set is empty,
    // since it is only used for objects prior to last_backfill
    pg_log.reset_backfill();
    child->pg_log.reset_backfill();
  }

  child->info.stats = info.stats;
  child->info.stats.parent_split_bits = split_bits;
  info.stats.stats_invalid = true;
  child->info.stats.stats_invalid = true;
  child->info.stats.objects_trimmed = 0;
  child->info.stats.snaptrim_duration = 0.0;
  child->info.last_epoch_started = info.last_epoch_started;
  child->info.last_interval_started = info.last_interval_started;

  // There can't be recovery/backfill going on now
  int primary, up_primary;
  vector<int> newup, newacting;
  get_osdmap()->pg_to_up_acting_osds(
    child->info.pgid.pgid, &newup, &up_primary, &newacting, &primary);
  child->init_primary_up_acting(
    newup,
    newacting,
    up_primary,
    primary);
  child->role = OSDMap::calc_pg_role(pg_whoami, child->acting);

  // this comparison includes primary rank via pg_shard_t
  if (get_primary() != child->get_primary())
    child->info.history.same_primary_since = get_osdmap_epoch();

  child->info.stats.up = newup;
  child->info.stats.up_primary = up_primary;
  child->info.stats.acting = newacting;
  child->info.stats.acting_primary = primary;
  child->info.stats.mapping_epoch = get_osdmap_epoch();

  // History
  child->past_intervals = past_intervals;

  child->on_new_interval();

  child->send_notify = !child->is_primary();

  child->dirty_info = true;
  child->dirty_big_info = true;
  dirty_info = true;
  dirty_big_info = true;
}

void PeeringState::merge_from(
  map<spg_t,PeeringState *>& sources,
  PeeringCtx &rctx,
  unsigned split_bits,
  const pg_merge_meta_t& last_pg_merge_meta)
{
  bool incomplete = false;
  if (info.last_complete != info.last_update ||
      info.is_incomplete() ||
      info.dne()) {
    psdout(10) << "target incomplete" << dendl;
    incomplete = true;
  }
  if (last_pg_merge_meta.source_pgid != pg_t()) {
    if (info.pgid.pgid != last_pg_merge_meta.source_pgid.get_parent()) {
      psdout(10) << "target doesn't match expected parent "
		 << last_pg_merge_meta.source_pgid.get_parent()
		 << " of source_pgid " << last_pg_merge_meta.source_pgid
		 << dendl;
      incomplete = true;
    }
    if (info.last_update != last_pg_merge_meta.target_version) {
      psdout(10) << "target version doesn't match expected "
	       << last_pg_merge_meta.target_version << dendl;
      incomplete = true;
    }
  }

  PGLog::LogEntryHandlerRef handler{pl->get_log_handler(rctx.transaction)};
  pg_log.roll_forward(handler.get());

  info.last_complete = info.last_update;  // to fake out trim()
  pg_log.reset_recovery_pointers();
  pg_log.trim(info.last_update, info);

  vector<PGLog*> log_from;
  for (auto& i : sources) {
    auto& source = i.second;
    if (!source) {
      psdout(10) << "source " << i.first << " missing" << dendl;
      incomplete = true;
      continue;
    }
    if (source->info.last_complete != source->info.last_update ||
	source->info.is_incomplete() ||
	source->info.dne()) {
      psdout(10) << "source " << source->pg_whoami
		 << " incomplete"
		 << dendl;
      incomplete = true;
    }
    if (last_pg_merge_meta.source_pgid != pg_t()) {
      if (source->info.pgid.pgid != last_pg_merge_meta.source_pgid) {
	dout(10) << "source " << source->info.pgid.pgid
		 << " doesn't match expected source pgid "
		 << last_pg_merge_meta.source_pgid << dendl;
	incomplete = true;
      }
      if (source->info.last_update != last_pg_merge_meta.source_version) {
	dout(10) << "source version doesn't match expected "
		 << last_pg_merge_meta.target_version << dendl;
	incomplete = true;
      }
    }

    // prepare log
    PGLog::LogEntryHandlerRef handler{
      source->pl->get_log_handler(rctx.transaction)};
    source->pg_log.roll_forward(handler.get());
    source->info.last_complete = source->info.last_update;  // to fake out trim()
    source->pg_log.reset_recovery_pointers();
    source->pg_log.trim(source->info.last_update, source->info);
    log_from.push_back(&source->pg_log);

    // combine stats
    info.stats.add(source->info.stats);

    // pull up last_update
    info.last_update = std::max(info.last_update, source->info.last_update);

    // adopt source's PastIntervals if target has none.  we can do this since
    // pgp_num has been reduced prior to the merge, so the OSD mappings for
    // the PGs are identical.
    if (past_intervals.empty() && !source->past_intervals.empty()) {
      psdout(10) << "taking source's past_intervals" << dendl;
      past_intervals = source->past_intervals;
    }
  }

  info.last_complete = info.last_update;
  info.log_tail = info.last_update;
  if (incomplete) {
    info.last_backfill = hobject_t();
  }

  // merge logs
  pg_log.merge_from(log_from, info.last_update);

  // make sure we have a meaningful last_epoch_started/clean (if we were a
  // placeholder)
  if (info.history.epoch_created == 0) {
    // start with (a) source's history, since these PGs *should* have been
    // remapped in concert with each other...
    info.history = sources.begin()->second->info.history;

    // we use the last_epoch_{started,clean} we got from
    // the caller, which are the epochs that were reported by the PGs were
    // found to be ready for merge.
    info.history.last_epoch_clean = last_pg_merge_meta.last_epoch_clean;
    info.history.last_epoch_started = last_pg_merge_meta.last_epoch_started;
    info.last_epoch_started = last_pg_merge_meta.last_epoch_started;
    psdout(10) << "set les/c to "
	       << last_pg_merge_meta.last_epoch_started << "/"
	       << last_pg_merge_meta.last_epoch_clean
	       << " from pool last_dec_*, source pg history was "
	       << sources.begin()->second->info.history
	       << dendl;

    // above we have pulled down source's history and we need to check
    // history.epoch_created again to confirm that source is not a placeholder
    // too. (peering requires a sane history.same_interval_since value for any
    // non-newly created pg and below here we know we are basically iterating
    // back a series of past maps to fake a merge process, hence we need to
    // fix history.same_interval_since first so that start_peering_interval()
    // will not complain)
    if (info.history.epoch_created == 0) {
      dout(10) << "both merge target and source are placeholders,"
               << " set sis to lec " << info.history.last_epoch_clean
               << dendl;
      info.history.same_interval_since = info.history.last_epoch_clean;
    }

    // if the past_intervals start is later than last_epoch_clean, it
    // implies the source repeered again but the target didn't, or
    // that the source became clean in a later epoch than the target.
    // avoid the discrepancy but adjusting the interval start
    // backwards to match so that check_past_interval_bounds() will
    // not complain.
    auto pib = past_intervals.get_bounds();
    if (info.history.last_epoch_clean < pib.first) {
      psdout(10) << "last_epoch_clean "
		 << info.history.last_epoch_clean << " < past_interval start "
		 << pib.first << ", adjusting start backwards" << dendl;
      past_intervals.adjust_start_backwards(info.history.last_epoch_clean);
    }

    // Similarly, if the same_interval_since value is later than
    // last_epoch_clean, the next interval change will result in a
    // past_interval start that is later than last_epoch_clean.  This
    // can happen if we use the pg_history values from the merge
    // source.  Adjust the same_interval_since value backwards if that
    // happens.  (We trust the les and lec values more because they came from
    // the real target, whereas the history value we stole from the source.)
    if (info.history.last_epoch_started < info.history.same_interval_since) {
      psdout(10) << "last_epoch_started "
		 << info.history.last_epoch_started << " < same_interval_since "
		 << info.history.same_interval_since
		 << ", adjusting pg_history backwards" << dendl;
      info.history.same_interval_since = info.history.last_epoch_clean;
      // make sure same_{up,primary}_since are <= same_interval_since
      info.history.same_up_since = std::min(
	info.history.same_up_since, info.history.same_interval_since);
      info.history.same_primary_since = std::min(
	info.history.same_primary_since, info.history.same_interval_since);
    }
  }

  dirty_info = true;
  dirty_big_info = true;
}

void PeeringState::start_split_stats(
  const set<spg_t>& childpgs, vector<object_stat_sum_t> *out)
{
  out->resize(childpgs.size() + 1);
  info.stats.stats.sum.split(*out);
}

void PeeringState::finish_split_stats(
  const object_stat_sum_t& stats, ObjectStore::Transaction &t)
{
  info.stats.stats.sum = stats;
  write_if_dirty(t);
}

void PeeringState::update_blocked_by()
{
  // set a max on the number of blocking peers we report. if we go
  // over, report a random subset.  keep the result sorted.
  unsigned keep = std::min<unsigned>(
    blocked_by.size(), cct->_conf->osd_max_pg_blocked_by);
  unsigned skip = blocked_by.size() - keep;
  info.stats.blocked_by.clear();
  info.stats.blocked_by.resize(keep);
  unsigned pos = 0;
  for (auto p = blocked_by.begin(); p != blocked_by.end() && keep > 0; ++p) {
    if (skip > 0 && (rand() % (skip + keep) < skip)) {
      --skip;
    } else {
      info.stats.blocked_by[pos++] = *p;
      --keep;
    }
  }
}

static bool find_shard(const set<pg_shard_t> & pgs, shard_id_t shard)
{
    for (auto&p : pgs)
      if (p.shard == shard)
        return true;
    return false;
}

static pg_shard_t get_another_shard(const set<pg_shard_t> & pgs, pg_shard_t skip, shard_id_t shard)
{
    for (auto&p : pgs) {
      if (p == skip)
        continue;
      if (p.shard == shard)
        return p;
    }
    return pg_shard_t();
}

void PeeringState::update_calc_stats()
{
  info.stats.version = info.last_update;
  info.stats.created = info.history.epoch_created;
  info.stats.last_scrub = info.history.last_scrub;
  info.stats.last_scrub_stamp = info.history.last_scrub_stamp;
  info.stats.last_deep_scrub = info.history.last_deep_scrub;
  info.stats.last_deep_scrub_stamp = info.history.last_deep_scrub_stamp;
  info.stats.last_clean_scrub_stamp = info.history.last_clean_scrub_stamp;
  info.stats.last_epoch_clean = info.history.last_epoch_clean;

  info.stats.log_size = pg_log.get_head().version - pg_log.get_tail().version;
  info.stats.log_dups_size = pg_log.get_log().dups.size();
  info.stats.ondisk_log_size = info.stats.log_size;
  info.stats.log_start = pg_log.get_tail();
  info.stats.ondisk_log_start = pg_log.get_tail();
  info.stats.snaptrimq_len = pl->get_snap_trimq_size();

  unsigned num_shards = get_osdmap()->get_pg_size(info.pgid.pgid);

  // In rare case that upset is too large (usually transient), use as target
  // for calculations below.
  unsigned target = std::max(num_shards, (unsigned)upset.size());
  // For undersized actingset may be larger with OSDs out
  unsigned nrep = std::max(actingset.size(), upset.size());
  // calc num_object_copies
  info.stats.stats.calc_copies(std::max(target, nrep));
  info.stats.stats.sum.num_objects_degraded = 0;
  info.stats.stats.sum.num_objects_unfound = 0;
  info.stats.stats.sum.num_objects_misplaced = 0;
  info.stats.avail_no_missing.clear();
  info.stats.object_location_counts.clear();

  // We should never hit this condition, but if end up hitting it,
  // make sure to update num_objects and set PG_STATE_INCONSISTENT.
  if (info.stats.stats.sum.num_objects < 0) {
    psdout(0) << "negative num_objects = "
              << info.stats.stats.sum.num_objects << " setting it to 0 "
              << dendl;
    info.stats.stats.sum.num_objects = 0;
    state_set(PG_STATE_INCONSISTENT);
  }

  if ((is_remapped() || is_undersized() || !is_clean()) &&
      (is_peered()|| is_activating())) {
    psdout(20) << "actingset " << actingset << " upset "
	       << upset << " acting_recovery_backfill " << acting_recovery_backfill << dendl;

    ceph_assert(!acting_recovery_backfill.empty());

    bool estimate = false;

    // NOTE: we only generate degraded, misplaced and unfound
    // values for the summation, not individual stat categories.
    int64_t num_objects = info.stats.stats.sum.num_objects;

    // Objects missing from up nodes, sorted by # objects.
    boost::container::flat_set<pair<int64_t,pg_shard_t>> missing_target_objects;
    // Objects missing from nodes not in up, sort by # objects
    boost::container::flat_set<pair<int64_t,pg_shard_t>> acting_source_objects;

    // Fill missing_target_objects/acting_source_objects

    {
      int64_t missing;

      // Primary first
      missing = pg_log.get_missing().num_missing();
      ceph_assert(acting_recovery_backfill.count(pg_whoami));
      if (upset.count(pg_whoami)) {
        missing_target_objects.emplace(missing, pg_whoami);
      } else {
        acting_source_objects.emplace(missing, pg_whoami);
      }
      info.stats.stats.sum.num_objects_missing_on_primary = missing;
      if (missing == 0)
        info.stats.avail_no_missing.push_back(pg_whoami);
      psdout(20) << "shard " << pg_whoami
		 << " primary objects " << num_objects
		 << " missing " << missing
		 << dendl;
    }

    // All other peers
    for (auto& peer : peer_info) {
      // Primary should not be in the peer_info, skip if it is.
      if (peer.first == pg_whoami) continue;
      int64_t missing = 0;
      int64_t peer_num_objects =
        std::max((int64_t)0, peer.second.stats.stats.sum.num_objects);
      // Backfill targets always track num_objects accurately
      // all other peers track missing accurately.
      if (is_backfill_target(peer.first)) {
        missing = std::max((int64_t)0, num_objects - peer_num_objects);
      } else {
	auto pm_it = peer_missing.find(peer.first);
        if (pm_it != peer_missing.end()) {
          missing = pm_it->second.num_missing();
        } else {
          psdout(20) << "no peer_missing found for "
		     << peer.first << dendl;
          if (is_recovering()) {
            estimate = true;
          }
          missing = std::max((int64_t)0, num_objects - peer_num_objects);
        }
      }
      if (upset.count(peer.first)) {
	missing_target_objects.emplace(missing, peer.first);
      } else if (actingset.count(peer.first)) {
	acting_source_objects.emplace(missing, peer.first);
      }
      peer.second.stats.stats.sum.num_objects_missing = missing;
      if (missing == 0)
        info.stats.avail_no_missing.push_back(peer.first);
      psdout(20) << "shard " << peer.first
		 << " objects " << peer_num_objects
		 << " missing " << missing
		 << dendl;
    }

    // Compute object_location_counts
    for (auto& ml: missing_loc.get_missing_locs()) {
      info.stats.object_location_counts[ml.second]++;
      psdout(30) << ml.first << " object_location_counts["
		 << ml.second << "]=" << info.stats.object_location_counts[ml.second]
		 << dendl;
    }
    int64_t not_missing = num_objects - missing_loc.get_missing_locs().size();
    if (not_missing) {
	// During recovery we know upset == actingset and is being populated
	// During backfill we know that all non-missing objects are in the actingset
        info.stats.object_location_counts[actingset] = not_missing;
    }
    psdout(30) << "object_location_counts["
	       << upset << "]=" << info.stats.object_location_counts[upset]
	       << dendl;
    psdout(20) << "object_location_counts "
	       << info.stats.object_location_counts << dendl;

    // A misplaced object is not stored on the correct OSD
    int64_t misplaced = 0;
    // a degraded objects has fewer replicas or EC shards than the pool specifies.
    int64_t degraded = 0;

    if (is_recovering()) {
      for (auto& sml: missing_loc.get_missing_by_count()) {
        for (auto& ml: sml.second) {
          int missing_shards;
          if (sml.first == shard_id_t::NO_SHARD) {
            psdout(20) << "ml " << ml.second
		       << " upset size " << upset.size()
		       << " up " << ml.first.up << dendl;
            missing_shards = (int)upset.size() - ml.first.up;
          } else {
	    // Handle shards not even in upset below
            if (!find_shard(upset, sml.first))
	      continue;
	    missing_shards = std::max(0, 1 - ml.first.up);
            psdout(20) << "shard " << sml.first
		       << " ml " << ml.second
		       << " missing shards " << missing_shards << dendl;
          }
          int odegraded = ml.second * missing_shards;
          // Copies on other osds but limited to the possible degraded
          int more_osds = std::min(missing_shards, ml.first.other);
          int omisplaced = ml.second * more_osds;
          ceph_assert(omisplaced <= odegraded);
          odegraded -= omisplaced;

          misplaced += omisplaced;
          degraded += odegraded;
        }
      }

      psdout(20) << "missing based degraded "
		 << degraded << dendl;
      psdout(20) << "missing based misplaced "
		 << misplaced << dendl;

      // Handle undersized case
      if (pool.info.is_replicated()) {
        // Add degraded for missing targets (num_objects missing)
        ceph_assert(target >= upset.size());
        unsigned needed = target - upset.size();
        degraded += num_objects * needed;
      } else {
        for (unsigned i = 0 ; i < num_shards; ++i) {
          shard_id_t shard(i);

          if (!find_shard(upset, shard)) {
            pg_shard_t pgs = get_another_shard(actingset, pg_shard_t(), shard);

            if (pgs != pg_shard_t()) {
              int64_t missing;

              if (pgs == pg_whoami)
                missing = info.stats.stats.sum.num_objects_missing_on_primary;
              else
                missing = peer_info[pgs].stats.stats.sum.num_objects_missing;

              degraded += missing;
              misplaced += std::max((int64_t)0, num_objects - missing);
            } else {
              // No shard anywhere
              degraded += num_objects;
            }
          }
        }
      }
      goto out;
    }

    // Handle undersized case
    if (pool.info.is_replicated()) {
      // Add to missing_target_objects
      ceph_assert(target >= missing_target_objects.size());
      unsigned needed = target - missing_target_objects.size();
      if (needed)
        missing_target_objects.emplace(num_objects * needed, pg_shard_t(pg_shard_t::NO_OSD));
    } else {
      for (unsigned i = 0 ; i < num_shards; ++i) {
        shard_id_t shard(i);
	bool found = false;
	for (const auto& t : missing_target_objects) {
	  if (std::get<1>(t).shard == shard) {
	    found = true;
	    break;
	  }
	}
	if (!found)
	  missing_target_objects.emplace(num_objects, pg_shard_t(pg_shard_t::NO_OSD,shard));
      }
    }

    for (const auto& item : missing_target_objects)
      psdout(20) << "missing shard " << std::get<1>(item)
		 << " missing= " << std::get<0>(item) << dendl;
    for (const auto& item : acting_source_objects)
      psdout(20) << "acting shard " << std::get<1>(item)
		 << " missing= " << std::get<0>(item) << dendl;

    // Handle all objects not in missing for remapped
    // or backfill
    for (auto m = missing_target_objects.rbegin();
        m != missing_target_objects.rend(); ++m) {

      int64_t extra_missing = -1;

      if (pool.info.is_replicated()) {
	if (!acting_source_objects.empty()) {
	  auto extra_copy = acting_source_objects.begin();
	  extra_missing = std::get<0>(*extra_copy);
          acting_source_objects.erase(extra_copy);
	}
      } else {	// Erasure coded
	// Use corresponding shard
	for (const auto& a : acting_source_objects) {
	  if (std::get<1>(a).shard == std::get<1>(*m).shard) {
	    extra_missing = std::get<0>(a);
	    acting_source_objects.erase(a);
	    break;
	  }
	}
      }

      if (extra_missing >= 0 && std::get<0>(*m) >= extra_missing) {
	// We don't know which of the objects on the target
	// are part of extra_missing so assume are all degraded.
	misplaced += std::get<0>(*m) - extra_missing;
	degraded += extra_missing;
      } else {
	// 1. extra_missing == -1, more targets than sources so degraded
	// 2. extra_missing > std::get<0>(m), so that we know that some extra_missing
	//    previously degraded are now present on the target.
	degraded += std::get<0>(*m);
      }
    }
    // If there are still acting that haven't been accounted for
    // then they are misplaced
    for (const auto& a : acting_source_objects) {
      int64_t extra_misplaced = std::max((int64_t)0, num_objects - std::get<0>(a));
      psdout(20) << "extra acting misplaced " << extra_misplaced
		 << dendl;
      misplaced += extra_misplaced;
    }
out:
    // NOTE: Tests use these messages to verify this code
    psdout(20) << "degraded " << degraded
	       << (estimate ? " (est)": "") << dendl;
    psdout(20) << "misplaced " << misplaced
	       << (estimate ? " (est)": "")<< dendl;

    info.stats.stats.sum.num_objects_degraded = degraded;
    info.stats.stats.sum.num_objects_unfound = get_num_unfound();
    info.stats.stats.sum.num_objects_misplaced = misplaced;
  }
}

std::optional<pg_stat_t> PeeringState::prepare_stats_for_publish(
  const std::optional<pg_stat_t> &pg_stats_publish,
  const object_stat_collection_t &unstable_stats)
{
  if (info.stats.stats.sum.num_scrub_errors) {
    psdout(10) << "inconsistent due to " <<
      info.stats.stats.sum.num_scrub_errors << " scrub errors" << dendl;
    state_set(PG_STATE_INCONSISTENT);
  } else {
    state_clear(PG_STATE_INCONSISTENT);
    state_clear(PG_STATE_FAILED_REPAIR);
  }

  utime_t now = ceph_clock_now();
  if (info.stats.state != state) {
    info.stats.last_change = now;
    // Optimistic estimation, if we just find out an inactive PG,
    // assume it is active till now.
    if (!(state & PG_STATE_ACTIVE) &&
	(info.stats.state & PG_STATE_ACTIVE))
      info.stats.last_active = now;

    if ((state & PG_STATE_ACTIVE) &&
	!(info.stats.state & PG_STATE_ACTIVE))
      info.stats.last_became_active = now;
    if ((state & (PG_STATE_ACTIVE|PG_STATE_PEERED)) &&
	!(info.stats.state & (PG_STATE_ACTIVE|PG_STATE_PEERED)))
      info.stats.last_became_peered = now;
    info.stats.state = state;
  }

  update_calc_stats();
  if (info.stats.stats.sum.num_objects_degraded) {
    state_set(PG_STATE_DEGRADED);
  } else {
    state_clear(PG_STATE_DEGRADED);
  }
  update_blocked_by();

  pg_stat_t pre_publish = info.stats;
  pre_publish.stats.add(unstable_stats);

  // share (some of) our purged_snaps via the pg_stats. limit # of intervals
  // because we don't want to make the pg_stat_t structures too expensive.
  unsigned max = cct->_conf->osd_max_snap_prune_intervals_per_epoch;
  unsigned num = 0;
  auto i = info.purged_snaps.begin();
  while (num < max && i != info.purged_snaps.end()) {
    pre_publish.purged_snaps.insert(i.get_start(), i.get_len());
    ++num;
    ++i;
  }
  psdout(20) << "reporting purged_snaps "
	     << pre_publish.purged_snaps << dendl;

  // when there is no change in osdmap,
  // update info.stats.reported_epoch by the number of time seconds.
  utime_t cutoff_time = now;
  cutoff_time -= cct->_conf->osd_pg_stat_report_interval_max_seconds;
  bool is_time_expired = cutoff_time > info.stats.last_fresh ? true : false;

  // 500 epoch osdmaps are also the minimum number of osdmaps that mon must retain.
  // if info.stats.reported_epoch less than current osdmap epoch exceeds 500 osdmaps,
  // it can be considered that the one reported by pgid is too old and needs to be updated.
  // to facilitate mon trim osdmaps
  epoch_t cutoff_epoch = info.stats.reported_epoch;
  cutoff_epoch += cct->_conf->osd_pg_stat_report_interval_max_epochs;
  bool is_epoch_behind = cutoff_epoch < get_osdmap_epoch() ? true : false;

  if (pg_stats_publish && pre_publish == *pg_stats_publish &&
      (!is_epoch_behind && !is_time_expired)) {
    psdout(15) << "publish_stats_to_osd " << pg_stats_publish->reported_epoch
	       << ": no change since " << info.stats.last_fresh << dendl;
    return std::nullopt;
  } else {
    // update our stat summary and timestamps
    info.stats.reported_epoch = get_osdmap_epoch();
    ++info.stats.reported_seq;

    info.stats.last_fresh = now;

    if (info.stats.state & PG_STATE_CLEAN)
      info.stats.last_clean = now;
    if (info.stats.state & PG_STATE_ACTIVE)
      info.stats.last_active = now;
    if (info.stats.state & (PG_STATE_ACTIVE|PG_STATE_PEERED))
      info.stats.last_peered = now;
    info.stats.last_unstale = now;
    if ((info.stats.state & PG_STATE_DEGRADED) == 0)
      info.stats.last_undegraded = now;
    if ((info.stats.state & PG_STATE_UNDERSIZED) == 0)
      info.stats.last_fullsized = now;

    psdout(15) << "publish_stats_to_osd " << pre_publish.reported_epoch
	       << ":" << pre_publish.reported_seq << dendl;
    return std::make_optional(std::move(pre_publish));
  }
}

void PeeringState::init(
  int role,
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  const pg_history_t& history,
  const PastIntervals& pi,
  ObjectStore::Transaction &t)
{
  psdout(10) << "init role " << role << " up "
	     << newup << " acting " << newacting
	     << " history " << history
	     << " past_intervals " << pi
	     << dendl;

  set_role(role);
  init_primary_up_acting(
    newup,
    newacting,
    new_up_primary,
    new_acting_primary);

  info.history = history;
  past_intervals = pi;

  info.stats.up = up;
  info.stats.up_primary = new_up_primary;
  info.stats.acting = acting;
  info.stats.acting_primary = new_acting_primary;
  info.stats.mapping_epoch = info.history.same_interval_since;

  if (!perform_deletes_during_peering()) {
    pg_log.set_missing_may_contain_deletes();
  }

  on_new_interval();

  dirty_info = true;
  dirty_big_info = true;
  write_if_dirty(t);
}

void PeeringState::dump_peering_state(Formatter *f)
{
  f->dump_string("state", get_pg_state_string());
  f->dump_unsigned("epoch", get_osdmap_epoch());
  f->open_array_section("up");
  for (auto p = up.begin(); p != up.end(); ++p)
    f->dump_unsigned("osd", *p);
  f->close_section();
  f->open_array_section("acting");
  for (auto p = acting.begin(); p != acting.end(); ++p)
    f->dump_unsigned("osd", *p);
  f->close_section();
  if (!backfill_targets.empty()) {
    f->open_array_section("backfill_targets");
    for (auto p = backfill_targets.begin(); p != backfill_targets.end(); ++p)
      f->dump_stream("shard") << *p;
    f->close_section();
  }
  if (!async_recovery_targets.empty()) {
    f->open_array_section("async_recovery_targets");
    for (auto p = async_recovery_targets.begin();
	 p != async_recovery_targets.end();
	 ++p)
      f->dump_stream("shard") << *p;
    f->close_section();
  }
  if (!acting_recovery_backfill.empty()) {
    f->open_array_section("acting_recovery_backfill");
    for (auto p = acting_recovery_backfill.begin();
	 p != acting_recovery_backfill.end();
	 ++p)
      f->dump_stream("shard") << *p;
    f->close_section();
  }
  f->open_object_section("info");
  update_calc_stats();
  info.dump(f);
  f->close_section();

  f->open_array_section("peer_info");
  for (auto p = peer_info.begin(); p != peer_info.end(); ++p) {
    f->open_object_section("info");
    f->dump_stream("peer") << p->first;
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PeeringState::update_stats(
  std::function<bool(pg_history_t &, pg_stat_t &)> f,
  ObjectStore::Transaction *t) {
  if (f(info.history, info.stats)) {
    pl->publish_stats_to_osd();
  }

  if (t) {
    dirty_info = true;
    write_if_dirty(*t);
  }
}

void PeeringState::update_stats_wo_resched(
  std::function<void(pg_history_t &, pg_stat_t &)> f)
{
  f(info.history, info.stats);
}

bool PeeringState::append_log_entries_update_missing(
  const mempool::osd_pglog::list<pg_log_entry_t> &entries,
  ObjectStore::Transaction &t, std::optional<eversion_t> trim_to,
  std::optional<eversion_t> roll_forward_to)
{
  ceph_assert(!entries.empty());
  ceph_assert(entries.begin()->version > info.last_update);

  PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
  bool invalidate_stats =
    pg_log.append_new_log_entries(
      info.last_backfill,
      entries,
      rollbacker.get());

  if (roll_forward_to && entries.rbegin()->soid > info.last_backfill) {
    pg_log.roll_forward(rollbacker.get());
  }
  if (roll_forward_to && *roll_forward_to > pg_log.get_can_rollback_to()) {
    pg_log.roll_forward_to(*roll_forward_to, rollbacker.get());
    last_rollback_info_trimmed_to_applied = *roll_forward_to;
  }

  info.last_update = pg_log.get_head();

  if (pg_log.get_missing().num_missing() == 0) {
    // advance last_complete since nothing else is missing!
    info.last_complete = info.last_update;
  }
  info.stats.stats_invalid = info.stats.stats_invalid || invalidate_stats;

  psdout(20) << "trim_to bool = " << bool(trim_to)
	     << " trim_to = " << (trim_to ? *trim_to : eversion_t()) << dendl;
  if (trim_to)
    pg_log.trim(*trim_to, info);
  dirty_info = true;
  write_if_dirty(t);
  return invalidate_stats;
}

void PeeringState::merge_new_log_entries(
  const mempool::osd_pglog::list<pg_log_entry_t> &entries,
  ObjectStore::Transaction &t,
  std::optional<eversion_t> trim_to,
  std::optional<eversion_t> roll_forward_to)
{
  psdout(10) << entries << dendl;
  ceph_assert(is_primary());

  bool rebuild_missing = append_log_entries_update_missing(entries, t, trim_to, roll_forward_to);
  for (auto i = acting_recovery_backfill.begin();
       i != acting_recovery_backfill.end();
       ++i) {
    pg_shard_t peer(*i);
    if (peer == pg_whoami) continue;
    auto pm_it = peer_missing.find(peer);
    ceph_assert(pm_it != peer_missing.end());
    auto pi_it = peer_info.find(peer);
    ceph_assert(pi_it != peer_info.end());
    pg_missing_t& pmissing(pm_it->second);
    psdout(20) << "peer_missing for " << peer
	       << " = " << pmissing << dendl;
    pg_info_t& pinfo = pi_it->second;
    bool invalidate_stats = PGLog::append_log_entries_update_missing(
      pinfo.last_backfill,
      entries,
      true,
      NULL,
      pmissing,
      NULL,
      dpp);
    pinfo.last_update = info.last_update;
    pinfo.stats.stats_invalid = pinfo.stats.stats_invalid || invalidate_stats;
    rebuild_missing = rebuild_missing || invalidate_stats;
  }

  if (!rebuild_missing) {
    return;
  }

  for (auto &&i: entries) {
    missing_loc.rebuild(
      i.soid,
      pg_whoami,
      acting_recovery_backfill,
      info,
      pg_log.get_missing(),
      peer_missing,
      peer_info);
  }
}

void PeeringState::add_log_entry(const pg_log_entry_t& e, bool applied)
{
  // raise last_complete only if we were previously up to date
  if (info.last_complete == info.last_update)
    info.last_complete = e.version;

  // raise last_update.
  ceph_assert(e.version > info.last_update);
  info.last_update = e.version;

  // raise user_version, if it increased (it may have not get bumped
  // by all logged updates)
  if (e.user_version > info.last_user_version)
    info.last_user_version = e.user_version;

  // log mutation
  pg_log.add(e, applied);
  psdout(10) << "add_log_entry " << e << dendl;
}


void PeeringState::append_log(
  vector<pg_log_entry_t>&& logv,
  eversion_t trim_to,
  eversion_t roll_forward_to,
  eversion_t mlcod,
  ObjectStore::Transaction &t,
  bool transaction_applied,
  bool async)
{
  /* The primary has sent an info updating the history, but it may not
   * have arrived yet.  We want to make sure that we cannot remember this
   * write without remembering that it happened in an interval which went
   * active in epoch history.last_epoch_started.
   */
  if (info.last_epoch_started != info.history.last_epoch_started) {
    info.history.last_epoch_started = info.last_epoch_started;
  }
  if (info.last_interval_started != info.history.last_interval_started) {
    info.history.last_interval_started = info.last_interval_started;
  }
  psdout(10) << "append_log " << pg_log.get_log() << " " << logv << dendl;

  PGLog::LogEntryHandlerRef handler{pl->get_log_handler(t)};
  if (!transaction_applied) {
     /* We must be a backfill or async recovery peer, so it's ok if we apply
      * out-of-turn since we won't be considered when
      * determining a min possible last_update.
      *
      * We skip_rollforward() here, which advances the crt, without
      * doing an actual rollforward. This avoids cleaning up entries
      * from the backend and we do not end up in a situation, where the
      * object is deleted before we can _merge_object_divergent_entries().
      */
    pg_log.skip_rollforward();
  }

  for (auto p = logv.begin(); p != logv.end(); ++p) {
    add_log_entry(*p, transaction_applied);

    /* We don't want to leave the rollforward artifacts around
     * here past last_backfill.  It's ok for the same reason as
     * above */
    if (transaction_applied &&
	p->soid > info.last_backfill) {
      pg_log.roll_forward(handler.get());
    }
  }
  if (transaction_applied && roll_forward_to > pg_log.get_can_rollback_to()) {
    pg_log.roll_forward_to(
      roll_forward_to,
      handler.get());
    last_rollback_info_trimmed_to_applied = roll_forward_to;
  }

  psdout(10) << "approx pg log length =  "
	     << pg_log.get_log().approx_size() << dendl;
  psdout(10) << "dups pg log length =  "
	     << pg_log.get_log().dups.size() << dendl;
  psdout(10) << "transaction_applied = "
	     << transaction_applied << dendl;
  if (!transaction_applied || async)
    psdout(10) << pg_whoami
	       << " is async_recovery or backfill target" << dendl;
  pg_log.trim(trim_to, info, transaction_applied, async);

  // update the local pg, pg log
  dirty_info = true;
  write_if_dirty(t);

  if (!is_primary())
    min_last_complete_ondisk = mlcod;
}

void PeeringState::recover_got(
  const hobject_t &oid, eversion_t v,
  bool is_delete,
  ObjectStore::Transaction &t)
{
  if (v > pg_log.get_can_rollback_to()) {
    /* This can only happen during a repair, and even then, it would
     * be one heck of a race.  If we are repairing the object, the
     * write in question must be fully committed, so it's not valid
     * to roll it back anyway (and we'll be rolled forward shortly
     * anyway) */
    PGLog::LogEntryHandlerRef handler{pl->get_log_handler(t)};
    pg_log.roll_forward_to(v, handler.get());
  }

  psdout(10) << "got missing " << oid << " v " << v << dendl;
  pg_log.recover_got(oid, v, info);
  if (pg_log.get_log().log.empty()) {
    psdout(10) << "last_complete now " << info.last_complete
               << " while log is empty" << dendl;
  } else if (pg_log.get_log().complete_to != pg_log.get_log().log.end()) {
    psdout(10) << "last_complete now " << info.last_complete
	       << " log.complete_to " << pg_log.get_log().complete_to->version
	       << dendl;
  } else {
    psdout(10) << "last_complete now " << info.last_complete
	       << " log.complete_to at end" << dendl;
    //below is not true in the repair case.
    //assert(missing.num_missing() == 0);  // otherwise, complete_to was wrong.
    ceph_assert(info.last_complete == info.last_update);
  }

  if (is_primary()) {
    ceph_assert(missing_loc.needs_recovery(oid));
    if (!is_delete)
      missing_loc.add_location(oid, pg_whoami);
  }

  // update pg
  dirty_info = true;
  write_if_dirty(t);
}

void PeeringState::update_backfill_progress(
  const hobject_t &updated_backfill,
  const pg_stat_t &updated_stats,
  bool preserve_local_num_bytes,
  ObjectStore::Transaction &t) {
  info.set_last_backfill(updated_backfill);
  if (preserve_local_num_bytes) {
    psdout(25) << "primary " << updated_stats.stats.sum.num_bytes
	       << " local " << info.stats.stats.sum.num_bytes << dendl;
    int64_t bytes = info.stats.stats.sum.num_bytes;
    info.stats = updated_stats;
    info.stats.stats.sum.num_bytes = bytes;
  } else {
    psdout(20) << "final " << updated_stats.stats.sum.num_bytes
	       << " replaces local " << info.stats.stats.sum.num_bytes << dendl;
    info.stats = updated_stats;
  }

  dirty_info = true;
  write_if_dirty(t);
}

void PeeringState::adjust_purged_snaps(
  std::function<void(interval_set<snapid_t> &snaps)> f) {
  f(info.purged_snaps);
  dirty_info = true;
  dirty_big_info = true;
}

void PeeringState::on_peer_recover(
  pg_shard_t peer,
  const hobject_t &soid,
  const eversion_t &version)
{
  pl->publish_stats_to_osd();
  // done!
  peer_missing[peer].got(soid, version);
  missing_loc.add_location(soid, peer);
}

void PeeringState::begin_peer_recover(
  pg_shard_t peer,
  const hobject_t soid)
{
  peer_missing[peer].revise_have(soid, eversion_t());
}

void PeeringState::force_object_missing(
  const set<pg_shard_t> &peers,
  const hobject_t &soid,
  eversion_t version)
{
  for (auto &&peer : peers) {
    if (peer != primary) {
      peer_missing[peer].add(soid, version, eversion_t(), false);
    } else {
      pg_log.missing_add(soid, version, eversion_t());
      pg_log.reset_complete_to(&info);
      pg_log.set_last_requested(0);
    }
  }

  missing_loc.rebuild(
    soid,
    pg_whoami,
    acting_recovery_backfill,
    info,
    pg_log.get_missing(),
    peer_missing,
    peer_info);
}

void PeeringState::pre_submit_op(
  const hobject_t &hoid,
  const vector<pg_log_entry_t>& logv,
  eversion_t at_version)
{
  if (at_version > eversion_t()) {
    for (auto &&i : get_acting_recovery_backfill()) {
      if (i == primary) continue;
      pg_info_t &pinfo = peer_info[i];
      // keep peer_info up to date
      if (pinfo.last_complete == pinfo.last_update)
	pinfo.last_complete = at_version;
      pinfo.last_update = at_version;
    }
  }

  bool requires_missing_loc = false;
  for (auto &&i : get_async_recovery_targets()) {
    if (i == primary || !get_peer_missing(i).is_missing(hoid))
      continue;
    requires_missing_loc = true;
    for (auto &&entry: logv) {
      peer_missing[i].add_next_event(entry);
    }
  }

  if (requires_missing_loc) {
    for (auto &&entry: logv) {
      psdout(30) << "missing_loc before: "
		 << missing_loc.get_locations(entry.soid) << dendl;
      missing_loc.add_missing(entry.soid, entry.version,
                              eversion_t(), entry.is_delete());
      // clear out missing_loc
      missing_loc.clear_location(entry.soid);
      for (auto &i: get_actingset()) {
        if (!get_peer_missing(i).is_missing(entry.soid))
          missing_loc.add_location(entry.soid, i);
      }
      psdout(30) << "missing_loc after: "
		 << missing_loc.get_locations(entry.soid) << dendl;
    }
  }
}

void PeeringState::recovery_committed_to(eversion_t version)
{
  psdout(10) << "version " << version
	     << " now ondisk" << dendl;
  last_complete_ondisk = version;

  if (last_complete_ondisk == info.last_update) {
    if (!is_primary()) {
      // Either we are a replica or backfill target.
      // we are fully up to date.  tell the primary!
      pl->send_cluster_message(
	get_primary().osd,
	TOPNSPC::make_message<MOSDPGTrim>(
	  get_osdmap_epoch(),
	  spg_t(info.pgid.pgid, primary.shard),
	  last_complete_ondisk),
	get_osdmap_epoch());
    } else {
      calc_min_last_complete_ondisk();
    }
  }
}

void PeeringState::complete_write(eversion_t v, eversion_t lc)
{
  last_update_ondisk = v;
  last_complete_ondisk = lc;
  calc_min_last_complete_ondisk();
}

void PeeringState::calc_trim_to()
{
  size_t target = pl->get_target_pg_log_entries();

  eversion_t limit = std::min(
    min_last_complete_ondisk,
    pg_log.get_can_rollback_to());
  if (limit != eversion_t() &&
      limit != pg_trim_to &&
      pg_log.get_log().approx_size() > target) {
    size_t num_to_trim = std::min(pg_log.get_log().approx_size() - target,
                             cct->_conf->osd_pg_log_trim_max);
    if (num_to_trim < cct->_conf->osd_pg_log_trim_min &&
        cct->_conf->osd_pg_log_trim_max >= cct->_conf->osd_pg_log_trim_min) {
      return;
    }
    auto it = pg_log.get_log().log.begin();
    eversion_t new_trim_to;
    for (size_t i = 0; i < num_to_trim; ++i) {
      new_trim_to = it->version;
      ++it;
      if (new_trim_to > limit) {
        new_trim_to = limit;
        psdout(10) << "calc_trim_to trimming to min_last_complete_ondisk" << dendl;
        break;
      }
    }
    psdout(10) << "calc_trim_to " << pg_trim_to << " -> " << new_trim_to << dendl;
    pg_trim_to = new_trim_to;
    assert(pg_trim_to <= pg_log.get_head());
    assert(pg_trim_to <= min_last_complete_ondisk);
  }
}

void PeeringState::calc_trim_to_aggressive()
{
  size_t target = pl->get_target_pg_log_entries();

  // limit pg log trimming up to the can_rollback_to value
  eversion_t limit = std::min({
    pg_log.get_head(),
    pg_log.get_can_rollback_to(),
    last_update_ondisk});
  psdout(10) << "limit = " << limit << dendl;

  if (limit != eversion_t() &&
      limit != pg_trim_to &&
      pg_log.get_log().approx_size() > target) {
    psdout(10) << "approx pg log length =  "
             << pg_log.get_log().approx_size() << dendl;
    uint64_t num_to_trim = std::min<uint64_t>(pg_log.get_log().approx_size() - target,
                                              cct->_conf->osd_pg_log_trim_max);
    psdout(10) << "num_to_trim =  " << num_to_trim << dendl;
    if (num_to_trim < cct->_conf->osd_pg_log_trim_min &&
	cct->_conf->osd_pg_log_trim_max >= cct->_conf->osd_pg_log_trim_min) {
      return;
    }
    auto it = pg_log.get_log().log.begin(); // oldest log entry
    auto rit = pg_log.get_log().log.rbegin();
    eversion_t by_n_to_keep; // start from tail
    eversion_t by_n_to_trim = eversion_t::max(); // start from head
    for (size_t i = 0; it != pg_log.get_log().log.end(); ++it, ++rit) {
      i++;
      if (i > target && by_n_to_keep == eversion_t()) {
        by_n_to_keep = rit->version;
      }
      if (i >= num_to_trim && by_n_to_trim == eversion_t::max()) {
        by_n_to_trim = it->version;
      }
      if (by_n_to_keep != eversion_t() &&
          by_n_to_trim != eversion_t::max()) {
        break;
      }
    }

    if (by_n_to_keep == eversion_t()) {
      return;
    }

    pg_trim_to = std::min({by_n_to_keep, by_n_to_trim, limit});
    psdout(10) << "pg_trim_to now " << pg_trim_to << dendl;
    ceph_assert(pg_trim_to <= pg_log.get_head());
  }
}

void PeeringState::apply_op_stats(
  const hobject_t &soid,
  const object_stat_sum_t &delta_stats)
{
  info.stats.stats.add(delta_stats);
  info.stats.stats.floor(0);

  for (auto i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    pg_shard_t bt = *i;
    pg_info_t& pinfo = peer_info[bt];
    if (soid <= pinfo.last_backfill)
      pinfo.stats.stats.add(delta_stats);
  }
}

void PeeringState::update_complete_backfill_object_stats(
  const hobject_t &hoid,
  const pg_stat_t &stats)
{
  for (auto &&bt: get_backfill_targets()) {
    pg_info_t& pinfo = peer_info[bt];
    //Add stats to all peers that were missing object
    if (hoid > pinfo.last_backfill)
      pinfo.stats.add(stats);
  }
}

void PeeringState::update_peer_last_backfill(
  pg_shard_t peer,
  const hobject_t &new_last_backfill)
{
  pg_info_t &pinfo = peer_info[peer];
  pinfo.last_backfill = new_last_backfill;
  if (new_last_backfill.is_max()) {
    /* pinfo.stats might be wrong if we did log-based recovery on the
     * backfilled portion in addition to continuing backfill.
     */
    pinfo.stats = info.stats;
  }
}

void PeeringState::set_revert_with_targets(
  const hobject_t &soid,
  const set<pg_shard_t> &good_peers)
{
  for (auto &&peer: good_peers) {
    missing_loc.add_location(soid, peer);
  }
}

void PeeringState::update_peer_last_complete_ondisk(
  pg_shard_t fromosd,
  eversion_t lcod) {
  psdout(20) << "updating peer_last_complete_ondisk"
             << " of osd: "<< fromosd << " to: "
             << lcod << dendl;
  peer_last_complete_ondisk[fromosd] = lcod;
}

void PeeringState::update_last_complete_ondisk(
  eversion_t lcod) {
  psdout(20) << "updating last_complete_ondisk"
             << " to: " << lcod << dendl;
  last_complete_ondisk = lcod;
}

void PeeringState::prepare_backfill_for_missing(
  const hobject_t &soid,
  const eversion_t &version,
  const vector<pg_shard_t> &targets) {
  for (auto &&peer: targets) {
    peer_missing[peer].add(soid, version, eversion_t(), false);
  }
}

void PeeringState::update_hset(const pg_hit_set_history_t &hset_history)
{
  info.hit_set = hset_history;
}

/*------------ Peering State Machine----------------*/
#undef dout_prefix
#define dout_prefix (context< PeeringMachine >().dpp->gen_prefix(*_dout) \
                    << "state<" << get_state_name() << ">: ")
#undef psdout
#define psdout(x) ldout(context< PeeringMachine >().cct, x)

#define DECLARE_LOCALS                                  \
  PeeringState *ps = context< PeeringMachine >().state; \
  std::ignore = ps;                                     \
  PeeringListener *pl = context< PeeringMachine >().pl; \
  std::ignore = pl


/*------Crashed-------*/
PeeringState::Crashed::Crashed(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Crashed")
{
  context< PeeringMachine >().log_enter(state_name);
  ceph_abort_msg("we got a bad state machine event");
}


/*------Initial-------*/
PeeringState::Initial::Initial(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Initial")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result PeeringState::Initial::react(const MNotifyRec& notify)
{
  DECLARE_LOCALS;
  ps->proc_replica_info(
    notify.from, notify.notify.info, notify.notify.epoch_sent);
  ps->set_last_peering_reset();
  return transit< Primary >();
}

boost::statechart::result PeeringState::Initial::react(const MInfoRec& i)
{
  DECLARE_LOCALS;
  ceph_assert(!ps->is_primary());
  post_event(i);
  return transit< Stray >();
}

boost::statechart::result PeeringState::Initial::react(const MLogRec& i)
{
  DECLARE_LOCALS;
  ceph_assert(!ps->is_primary());
  post_event(i);
  return transit< Stray >();
}

void PeeringState::Initial::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_initial_latency, dur);
}

/*------Started-------*/
PeeringState::Started::Started(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::Started::react(const IntervalFlush&)
{
  psdout(10) << "Ending blocked outgoing recovery messages" << dendl;
  context< PeeringMachine >().state->end_block_outgoing();
  return discard_event();
}

boost::statechart::result PeeringState::Started::react(const AdvMap& advmap)
{
  DECLARE_LOCALS;
  psdout(10) << "Started advmap" << dendl;
  ps->check_full_transition(advmap.lastmap, advmap.osdmap);
  if (ps->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    psdout(10) << "should_restart_peering, transitioning to Reset"
		       << dendl;
    post_event(advmap);
    return transit< Reset >();
  }
  ps->remove_down_peer_info(advmap.osdmap);
  return discard_event();
}

boost::statechart::result PeeringState::Started::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

boost::statechart::result PeeringState::Started::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "Started");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::Started::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_started_latency, dur);
  ps->state_clear(PG_STATE_WAIT | PG_STATE_LAGGY);
}

/*--------Reset---------*/
PeeringState::Reset::Reset(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Reset")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ps->flushes_in_progress = 0;
  ps->set_last_peering_reset();
  ps->log_weirdness();
}

boost::statechart::result
PeeringState::Reset::react(const IntervalFlush&)
{
  psdout(10) << "Ending blocked outgoing recovery messages" << dendl;
  context< PeeringMachine >().state->end_block_outgoing();
  return discard_event();
}

boost::statechart::result PeeringState::Reset::react(const AdvMap& advmap)
{
  DECLARE_LOCALS;
  psdout(10) << "Reset advmap" << dendl;

  ps->check_full_transition(advmap.lastmap, advmap.osdmap);

  if (ps->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    psdout(10) << "should restart peering, calling start_peering_interval again"
		       << dendl;
    ps->start_peering_interval(
      advmap.lastmap,
      advmap.newup, advmap.up_primary,
      advmap.newacting, advmap.acting_primary,
      context< PeeringMachine >().get_cur_transaction());
  }
  ps->remove_down_peer_info(advmap.osdmap);
  ps->check_past_interval_bounds();
  return discard_event();
}

boost::statechart::result PeeringState::Reset::react(const ActMap&)
{
  DECLARE_LOCALS;
  if (ps->should_send_notify() && ps->get_primary().osd >= 0) {
    ps->info.history.refresh_prior_readable_until_ub(
      pl->get_mnow(),
      ps->prior_readable_until_ub);
    context< PeeringMachine >().send_notify(
      ps->get_primary().osd,
      pg_notify_t(
	ps->get_primary().shard, ps->pg_whoami.shard,
	ps->get_osdmap_epoch(),
	ps->get_osdmap_epoch(),
	ps->info,
	ps->past_intervals));
  }

  ps->update_heartbeat_peers();

  return transit< Started >();
}

boost::statechart::result PeeringState::Reset::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

boost::statechart::result PeeringState::Reset::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "Reset");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::Reset::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_reset_latency, dur);
}

/*-------Start---------*/
PeeringState::Start::Start(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Start")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;
  if (ps->is_primary()) {
    psdout(1) << "transitioning to Primary" << dendl;
    post_event(MakePrimary());
  } else { //is_stray
    psdout(1) << "transitioning to Stray" << dendl;
    post_event(MakeStray());
  }
}

void PeeringState::Start::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_start_latency, dur);
}

/*---------Primary--------*/
PeeringState::Primary::Primary(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;
  ceph_assert(ps->want_acting.empty());

  // set CREATING bit until we have peered for the first time.
  if (ps->info.history.last_epoch_started == 0) {
    ps->state_set(PG_STATE_CREATING);
    // use the history timestamp, which ultimately comes from the
    // monitor in the create case.
    utime_t t = ps->info.history.last_scrub_stamp;
    ps->info.stats.last_fresh = t;
    ps->info.stats.last_active = t;
    ps->info.stats.last_change = t;
    ps->info.stats.last_peered = t;
    ps->info.stats.last_clean = t;
    ps->info.stats.last_unstale = t;
    ps->info.stats.last_undegraded = t;
    ps->info.stats.last_fullsized = t;
    ps->info.stats.last_scrub_stamp = t;
    ps->info.stats.last_deep_scrub_stamp = t;
    ps->info.stats.last_clean_scrub_stamp = t;
  }
}

boost::statechart::result PeeringState::Primary::react(const MNotifyRec& notevt)
{
  DECLARE_LOCALS;
  psdout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  ps->proc_replica_info(
    notevt.from, notevt.notify.info, notevt.notify.epoch_sent);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(const ActMap&)
{
  DECLARE_LOCALS;
  psdout(7) << "handle ActMap primary" << dendl;
  pl->publish_stats_to_osd();
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const SetForceRecovery&)
{
  DECLARE_LOCALS;
  ps->set_force_recovery(true);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const UnsetForceRecovery&)
{
  DECLARE_LOCALS;
  ps->set_force_recovery(false);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const RequestScrub& evt)
{
  DECLARE_LOCALS;
  if (ps->is_primary()) {
    pl->scrub_requested(evt.deep, evt.repair);
    psdout(10) << "marking for scrub" << dendl;
  }
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const SetForceBackfill&)
{
  DECLARE_LOCALS;
  ps->set_force_backfill(true);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const UnsetForceBackfill&)
{
  DECLARE_LOCALS;
  ps->set_force_backfill(false);
  return discard_event();
}

void PeeringState::Primary::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  ps->want_acting.clear();
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_primary_latency, dur);
  pl->clear_primary_state();
  ps->state_clear(PG_STATE_CREATING);
}

/*---------Peering--------*/
PeeringState::Peering::Peering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering"),
    history_les_bound(false)
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ceph_assert(!ps->is_peered());
  ceph_assert(!ps->is_peering());
  ceph_assert(ps->is_primary());
  ps->state_set(PG_STATE_PEERING);
}

boost::statechart::result PeeringState::Peering::react(const AdvMap& advmap)
{
  DECLARE_LOCALS;
  psdout(10) << "Peering advmap" << dendl;
  if (prior_set.affected_by_map(*(advmap.osdmap), ps->dpp)) {
    psdout(1) << "Peering, affected_by_map, going to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  ps->adjust_need_up_thru(advmap.osdmap);
  ps->check_prior_readable_down_osds(advmap.osdmap);

  return forward_event();
}

boost::statechart::result PeeringState::Peering::react(const QueryState& q)
{
  DECLARE_LOCALS;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("past_intervals");
  ps->past_intervals.dump(q.f);
  q.f->close_section();

  q.f->open_array_section("probing_osds");
  for (auto p = prior_set.probe.begin(); p != prior_set.probe.end(); ++p)
    q.f->dump_stream("osd") << *p;
  q.f->close_section();

  if (prior_set.pg_down)
    q.f->dump_string("blocked", "peering is blocked due to down osds");

  q.f->open_array_section("down_osds_we_would_probe");
  for (auto p = prior_set.down.begin(); p != prior_set.down.end(); ++p)
    q.f->dump_int("osd", *p);
  q.f->close_section();

  q.f->open_array_section("peering_blocked_by");
  for (auto p = prior_set.blocked_by.begin();
       p != prior_set.blocked_by.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", p->first);
    q.f->dump_int("current_lost_at", p->second);
    q.f->dump_string("comment", "starting or marking this osd lost may let us proceed");
    q.f->close_section();
  }
  q.f->close_section();

  if (history_les_bound) {
    q.f->open_array_section("peering_blocked_by_detail");
    q.f->open_object_section("item");
    q.f->dump_string("detail","peering_blocked_by_history_les_bound");
    q.f->close_section();
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::Peering::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "Peering");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::Peering::exit()
{

  DECLARE_LOCALS;
  psdout(10) << "Leaving Peering" << dendl;
  context< PeeringMachine >().log_exit(state_name, enter_time);
  ps->state_clear(PG_STATE_PEERING);
  pl->clear_probe_targets();

  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_peering_latency, dur);
}


/*------Backfilling-------*/
PeeringState::Backfilling::Backfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Backfilling")
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS;
  ps->backfill_reserved = true;
  pl->on_backfill_reserved();
  ps->state_clear(PG_STATE_BACKFILL_TOOFULL);
  ps->state_clear(PG_STATE_BACKFILL_WAIT);
  ps->state_set(PG_STATE_BACKFILLING);
  pl->publish_stats_to_osd();
}

void PeeringState::Backfilling::backfill_release_reservations()
{
  DECLARE_LOCALS;
  pl->cancel_local_background_io_reservation();
  for (auto it = ps->backfill_targets.begin();
       it != ps->backfill_targets.end();
       ++it) {
    ceph_assert(*it != ps->pg_whoami);
    pl->send_cluster_message(
      it->osd,
      TOPNSPC::make_message<MBackfillReserve>(
	MBackfillReserve::RELEASE,
	spg_t(ps->info.pgid.pgid, it->shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  }
}

void PeeringState::Backfilling::cancel_backfill()
{
  DECLARE_LOCALS;
  backfill_release_reservations();
  pl->on_backfill_canceled();
}

boost::statechart::result
PeeringState::Backfilling::react(const Backfilled &c)
{
  backfill_release_reservations();
  return transit<Recovered>();
}

boost::statechart::result
PeeringState::Backfilling::react(const DeferBackfill &c)
{
  DECLARE_LOCALS;

  psdout(10) << "defer backfill, retry delay " << c.delay << dendl;
  ps->state_set(PG_STATE_BACKFILL_WAIT);
  ps->state_clear(PG_STATE_BACKFILLING);
  cancel_backfill();

  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RequestBackfill()),
    c.delay);
  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::Backfilling::react(const UnfoundBackfill &c)
{
  DECLARE_LOCALS;
  psdout(10) << "backfill has unfound, can't continue" << dendl;
  ps->state_set(PG_STATE_BACKFILL_UNFOUND);
  ps->state_clear(PG_STATE_BACKFILLING);
  cancel_backfill();
  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::Backfilling::react(const RemoteReservationRevokedTooFull &)
{
  DECLARE_LOCALS;

  ps->state_set(PG_STATE_BACKFILL_TOOFULL);
  ps->state_clear(PG_STATE_BACKFILLING);
  cancel_backfill();

  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RequestBackfill()),
    ps->cct->_conf->osd_backfill_retry_interval);

  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::Backfilling::react(const RemoteReservationRevoked &)
{
  DECLARE_LOCALS;
  ps->state_set(PG_STATE_BACKFILL_WAIT);
  cancel_backfill();
  if (ps->needs_backfill()) {
    return transit<WaitLocalBackfillReserved>();
  } else {
    // raced with MOSDPGBackfill::OP_BACKFILL_FINISH, ignore
    return discard_event();
  }
}

void PeeringState::Backfilling::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  ps->backfill_reserved = false;
  ps->state_clear(PG_STATE_BACKFILLING);
  ps->state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_backfilling_latency, dur);
}

/*--WaitRemoteBackfillReserved--*/

PeeringState::WaitRemoteBackfillReserved::WaitRemoteBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitRemoteBackfillReserved"),
    backfill_osd_it(context< Active >().remote_shards_to_reserve_backfill.begin())
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ps->state_set(PG_STATE_BACKFILL_WAIT);
  pl->publish_stats_to_osd();
  post_event(RemoteBackfillReserved());
}

boost::statechart::result
PeeringState::WaitRemoteBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  DECLARE_LOCALS;

  int64_t num_bytes = ps->info.stats.stats.sum.num_bytes;
  psdout(10) << __func__ << " num_bytes " << num_bytes << dendl;
  if (backfill_osd_it !=
      context< Active >().remote_shards_to_reserve_backfill.end()) {
    // The primary never backfills itself
    ceph_assert(*backfill_osd_it != ps->pg_whoami);
    pl->send_cluster_message(
      backfill_osd_it->osd,
      TOPNSPC::make_message<MBackfillReserve>(
	MBackfillReserve::REQUEST,
	spg_t(context< PeeringMachine >().spgid.pgid, backfill_osd_it->shard),
	ps->get_osdmap_epoch(),
	ps->get_backfill_priority(),
        num_bytes,
        ps->peer_bytes[*backfill_osd_it]),
      ps->get_osdmap_epoch());
    ++backfill_osd_it;
  } else {
    ps->peer_bytes.clear();
    post_event(AllBackfillsReserved());
  }
  return discard_event();
}

void PeeringState::WaitRemoteBackfillReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;

  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitremotebackfillreserved_latency, dur);
}

void PeeringState::WaitRemoteBackfillReserved::retry()
{
  DECLARE_LOCALS;
  pl->cancel_local_background_io_reservation();

  // Send CANCEL to all previously acquired reservations
  set<pg_shard_t>::const_iterator it, begin, end;
  begin = context< Active >().remote_shards_to_reserve_backfill.begin();
  end = context< Active >().remote_shards_to_reserve_backfill.end();
  ceph_assert(begin != end);
  for (it = begin; it != backfill_osd_it; ++it) {
    // The primary never backfills itself
    ceph_assert(*it != ps->pg_whoami);
    pl->send_cluster_message(
      it->osd,
      TOPNSPC::make_message<MBackfillReserve>(
	MBackfillReserve::RELEASE,
	spg_t(context< PeeringMachine >().spgid.pgid, it->shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  }

  ps->state_clear(PG_STATE_BACKFILL_WAIT);
  pl->publish_stats_to_osd();

  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RequestBackfill()),
    ps->cct->_conf->osd_backfill_retry_interval);
}

boost::statechart::result
PeeringState::WaitRemoteBackfillReserved::react(const RemoteReservationRejectedTooFull &evt)
{
  DECLARE_LOCALS;
  ps->state_set(PG_STATE_BACKFILL_TOOFULL);
  retry();
  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::WaitRemoteBackfillReserved::react(const RemoteReservationRevoked &evt)
{
  retry();
  return transit<NotBackfilling>();
}

/*--WaitLocalBackfillReserved--*/
PeeringState::WaitLocalBackfillReserved::WaitLocalBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitLocalBackfillReserved")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ps->state_set(PG_STATE_BACKFILL_WAIT);
  pl->request_local_background_io_reservation(
    ps->get_backfill_priority(),
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      LocalBackfillReserved()),
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeferBackfill(0.0)));
  pl->publish_stats_to_osd();
}

void PeeringState::WaitLocalBackfillReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitlocalbackfillreserved_latency, dur);
}

/*----NotBackfilling------*/
PeeringState::NotBackfilling::NotBackfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/NotBackfilling")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_REPAIR);
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::NotBackfilling::react(const QueryUnfound& q)
{
  DECLARE_LOCALS;

  ps->query_unfound(q.f, "NotBackfilling");
  return discard_event();
}

boost::statechart::result
PeeringState::NotBackfilling::react(const RemoteBackfillReserved &evt)
{
  return discard_event();
}

boost::statechart::result
PeeringState::NotBackfilling::react(const RemoteReservationRejectedTooFull &evt)
{
  return discard_event();
}

void PeeringState::NotBackfilling::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_BACKFILL_UNFOUND);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_notbackfilling_latency, dur);
}

/*----NotRecovering------*/
PeeringState::NotRecovering::NotRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/NotRecovering")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_REPAIR);
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::NotRecovering::react(const QueryUnfound& q)
{
  DECLARE_LOCALS;

  ps->query_unfound(q.f, "NotRecovering");
  return discard_event();
}

void PeeringState::NotRecovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_RECOVERY_UNFOUND);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_notrecovering_latency, dur);
}

/*---RepNotRecovering----*/
PeeringState::RepNotRecovering::RepNotRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepNotRecovering")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepNotRecovering::react(const RejectTooFullRemoteReservation &evt)
{
  DECLARE_LOCALS;
  ps->reject_reservation();
  post_event(RemoteReservationRejectedTooFull());
  return discard_event();
}

void PeeringState::RepNotRecovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_repnotrecovering_latency, dur);
}

/*---RepWaitRecoveryReserved--*/
PeeringState::RepWaitRecoveryReserved::RepWaitRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepWaitRecoveryReserved")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepWaitRecoveryReserved::react(const RemoteRecoveryReserved &evt)
{
  DECLARE_LOCALS;
  pl->send_cluster_message(
    ps->primary.osd,
    TOPNSPC::make_message<MRecoveryReserve>(
      MRecoveryReserve::GRANT,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
PeeringState::RepWaitRecoveryReserved::react(
  const RemoteReservationCanceled &evt)
{
  DECLARE_LOCALS;
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  return transit<RepNotRecovering>();
}

void PeeringState::RepWaitRecoveryReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_repwaitrecoveryreserved_latency, dur);
}

/*-RepWaitBackfillReserved*/
PeeringState::RepWaitBackfillReserved::RepWaitBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepWaitBackfillReserved")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepNotRecovering::react(const RequestBackfillPrio &evt)
{

  DECLARE_LOCALS;

  if (!pl->try_reserve_recovery_space(
	evt.primary_num_bytes, evt.local_num_bytes)) {
    post_event(RejectTooFullRemoteReservation());
  } else {
    PGPeeringEventURef preempt;
    if (HAVE_FEATURE(ps->upacting_features, RECOVERY_RESERVATION_2)) {
      // older peers will interpret preemption as TOOFULL
      preempt = std::make_unique<PGPeeringEvent>(
	pl->get_osdmap_epoch(),
	pl->get_osdmap_epoch(),
	RemoteBackfillPreempted());
    }
    pl->request_remote_recovery_reservation(
      evt.priority,
      std::make_unique<PGPeeringEvent>(
	pl->get_osdmap_epoch(),
	pl->get_osdmap_epoch(),
        RemoteBackfillReserved()),
      std::move(preempt));
  }
  return transit<RepWaitBackfillReserved>();
}

boost::statechart::result
PeeringState::RepNotRecovering::react(const RequestRecoveryPrio &evt)
{
  DECLARE_LOCALS;

  // fall back to a local reckoning of priority of primary doesn't pass one
  // (pre-mimic compat)
  int prio = evt.priority ? evt.priority : ps->get_recovery_priority();

  PGPeeringEventURef preempt;
  if (HAVE_FEATURE(ps->upacting_features, RECOVERY_RESERVATION_2)) {
    // older peers can't handle this
    preempt = std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RemoteRecoveryPreempted());
  }

  pl->request_remote_recovery_reservation(
    prio,
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RemoteRecoveryReserved()),
    std::move(preempt));
  return transit<RepWaitRecoveryReserved>();
}

void PeeringState::RepWaitBackfillReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_repwaitbackfillreserved_latency, dur);
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  DECLARE_LOCALS;


  pl->send_cluster_message(
      ps->primary.osd,
      TOPNSPC::make_message<MBackfillReserve>(
	MBackfillReserve::GRANT,
	spg_t(ps->info.pgid.pgid, ps->primary.shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RejectTooFullRemoteReservation &evt)
{
  DECLARE_LOCALS;
  ps->reject_reservation();
  post_event(RemoteReservationRejectedTooFull());
  return discard_event();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RemoteReservationRejectedTooFull &evt)
{
  DECLARE_LOCALS;
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  return transit<RepNotRecovering>();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RemoteReservationCanceled &evt)
{
  DECLARE_LOCALS;
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  return transit<RepNotRecovering>();
}

/*---RepRecovering-------*/
PeeringState::RepRecovering::RepRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepRecovering")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepRecovering::react(const RemoteRecoveryPreempted &)
{
  DECLARE_LOCALS;


  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    ps->primary.osd,
    TOPNSPC::make_message<MRecoveryReserve>(
      MRecoveryReserve::REVOKE,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return discard_event();
}

boost::statechart::result
PeeringState::RepRecovering::react(const BackfillTooFull &)
{
  DECLARE_LOCALS;


  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    ps->primary.osd,
    TOPNSPC::make_message<MBackfillReserve>(
      MBackfillReserve::REVOKE_TOOFULL,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return discard_event();
}

boost::statechart::result
PeeringState::RepRecovering::react(const RemoteBackfillPreempted &)
{
  DECLARE_LOCALS;


  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    ps->primary.osd,
    TOPNSPC::make_message<MBackfillReserve>(
      MBackfillReserve::REVOKE,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return discard_event();
}

void PeeringState::RepRecovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_reprecovering_latency, dur);
}

/*------Activating--------*/
PeeringState::Activating::Activating(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Activating")
{
  context< PeeringMachine >().log_enter(state_name);
}

void PeeringState::Activating::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_activating_latency, dur);
}

PeeringState::WaitLocalRecoveryReserved::WaitLocalRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitLocalRecoveryReserved")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  // Make sure all nodes that part of the recovery aren't full
  if (!ps->cct->_conf->osd_debug_skip_full_check_in_recovery &&
      ps->get_osdmap()->check_full(ps->acting_recovery_backfill)) {
    post_event(RecoveryTooFull());
    return;
  }

  ps->state_clear(PG_STATE_RECOVERY_TOOFULL);
  ps->state_set(PG_STATE_RECOVERY_WAIT);
  pl->request_local_background_io_reservation(
    ps->get_recovery_priority(),
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      LocalRecoveryReserved()),
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeferRecovery(0.0)));
  pl->publish_stats_to_osd();
}

boost::statechart::result
PeeringState::WaitLocalRecoveryReserved::react(const RecoveryTooFull &evt)
{
  DECLARE_LOCALS;
  ps->state_set(PG_STATE_RECOVERY_TOOFULL);
  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DoRecovery()),
    ps->cct->_conf->osd_recovery_retry_interval);
  return transit<NotRecovering>();
}

void PeeringState::WaitLocalRecoveryReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitlocalrecoveryreserved_latency, dur);
}

PeeringState::WaitRemoteRecoveryReserved::WaitRemoteRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitRemoteRecoveryReserved"),
    remote_recovery_reservation_it(context< Active >().remote_shards_to_reserve_recovery.begin())
{
  context< PeeringMachine >().log_enter(state_name);
  post_event(RemoteRecoveryReserved());
}

boost::statechart::result
PeeringState::WaitRemoteRecoveryReserved::react(const RemoteRecoveryReserved &evt) {
  DECLARE_LOCALS;

  if (remote_recovery_reservation_it !=
      context< Active >().remote_shards_to_reserve_recovery.end()) {
    ceph_assert(*remote_recovery_reservation_it != ps->pg_whoami);
    pl->send_cluster_message(
      remote_recovery_reservation_it->osd,
      TOPNSPC::make_message<MRecoveryReserve>(
	MRecoveryReserve::REQUEST,
	spg_t(context< PeeringMachine >().spgid.pgid,
	      remote_recovery_reservation_it->shard),
	ps->get_osdmap_epoch(),
	ps->get_recovery_priority()),
      ps->get_osdmap_epoch());
    ++remote_recovery_reservation_it;
  } else {
    post_event(AllRemotesReserved());
  }
  return discard_event();
}

void PeeringState::WaitRemoteRecoveryReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitremoterecoveryreserved_latency, dur);
}

PeeringState::Recovering::Recovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Recovering")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_RECOVERY_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_TOOFULL);
  ps->state_set(PG_STATE_RECOVERING);
  pl->on_recovery_reserved();
  ceph_assert(!ps->state_test(PG_STATE_ACTIVATING));
  pl->publish_stats_to_osd();
}

void PeeringState::Recovering::release_reservations(bool cancel)
{
  DECLARE_LOCALS;
  ceph_assert(cancel || !ps->pg_log.get_missing().have_missing());

  // release remote reservations
  for (auto i = context< Active >().remote_shards_to_reserve_recovery.begin();
       i != context< Active >().remote_shards_to_reserve_recovery.end();
       ++i) {
    if (*i == ps->pg_whoami) // skip myself
      continue;
    pl->send_cluster_message(
      i->osd,
      TOPNSPC::make_message<MRecoveryReserve>(
	MRecoveryReserve::RELEASE,
	spg_t(ps->info.pgid.pgid, i->shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  }
}

boost::statechart::result
PeeringState::Recovering::react(const AllReplicasRecovered &evt)
{
  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_FORCED_RECOVERY);
  release_reservations();
  pl->cancel_local_background_io_reservation();
  return transit<Recovered>();
}

boost::statechart::result
PeeringState::Recovering::react(const RequestBackfill &evt)
{
  DECLARE_LOCALS;

  release_reservations();

  ps->state_clear(PG_STATE_FORCED_RECOVERY);
  pl->cancel_local_background_io_reservation();
  pl->publish_stats_to_osd();
  // transit any async_recovery_targets back into acting
  // so pg won't have to stay undersized for long
  // as backfill might take a long time to complete..
  if (!ps->async_recovery_targets.empty()) {
    pg_shard_t auth_log_shard;
    bool history_les_bound = false;
    // FIXME: Uh-oh we have to check this return value; choose_acting can fail!
    ps->choose_acting(auth_log_shard, true, &history_les_bound);
  }
  return transit<WaitLocalBackfillReserved>();
}

boost::statechart::result
PeeringState::Recovering::react(const DeferRecovery &evt)
{
  DECLARE_LOCALS;
  if (!ps->state_test(PG_STATE_RECOVERING)) {
    // we may have finished recovery and have an AllReplicasRecovered
    // event queued to move us to the next state.
    psdout(10) << "got defer recovery but not recovering" << dendl;
    return discard_event();
  }
  psdout(10) << "defer recovery, retry delay " << evt.delay << dendl;
  ps->state_set(PG_STATE_RECOVERY_WAIT);
  pl->cancel_local_background_io_reservation();
  release_reservations(true);
  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DoRecovery()),
    evt.delay);
  return transit<NotRecovering>();
}

boost::statechart::result
PeeringState::Recovering::react(const UnfoundRecovery &evt)
{
  DECLARE_LOCALS;
  psdout(10) << "recovery has unfound, can't continue" << dendl;
  ps->state_set(PG_STATE_RECOVERY_UNFOUND);
  pl->cancel_local_background_io_reservation();
  release_reservations(true);
  return transit<NotRecovering>();
}

void PeeringState::Recovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  ps->state_clear(PG_STATE_RECOVERING);
  pl->get_peering_perf().tinc(rs_recovering_latency, dur);
}

PeeringState::Recovered::Recovered(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Recovered")
{
  pg_shard_t auth_log_shard;

  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;

  ceph_assert(!ps->needs_recovery());

  // if we finished backfill, all acting are active; recheck if
  // DEGRADED | UNDERSIZED is appropriate.
  ceph_assert(!ps->acting_recovery_backfill.empty());
  if (ps->get_osdmap()->get_pg_size(context< PeeringMachine >().spgid.pgid) <=
      ps->acting_recovery_backfill.size()) {
    ps->state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
    pl->publish_stats_to_osd();
  }

  // adjust acting set?  (e.g. because backfill completed...)
  bool history_les_bound = false;
  if (ps->acting != ps->up && !ps->choose_acting(auth_log_shard,
						 true, &history_les_bound)) {
    ceph_assert(ps->want_acting.size());
  } else if (!ps->async_recovery_targets.empty()) {
    // FIXME: Uh-oh we have to check this return value; choose_acting can fail!
    ps->choose_acting(auth_log_shard, true, &history_les_bound);
  }

  if (context< Active >().all_replicas_activated  &&
      ps->async_recovery_targets.empty())
    post_event(GoClean());
}

void PeeringState::Recovered::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;

  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_recovered_latency, dur);
}

PeeringState::Clean::Clean(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Clean")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;

  if (ps->info.last_complete != ps->info.last_update) {
    ceph_abort();
  }


  ps->try_mark_clean();

  context< PeeringMachine >().get_cur_transaction().register_on_commit(
    pl->on_clean());
}

void PeeringState::Clean::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_CLEAN);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_clean_latency, dur);
}

template <typename T>
set<pg_shard_t> unique_osd_shard_set(const pg_shard_t & skip, const T &in)
{
  set<int> osds_found;
  set<pg_shard_t> out;
  for (auto i = in.begin(); i != in.end(); ++i) {
    if (*i != skip && !osds_found.count(i->osd)) {
      osds_found.insert(i->osd);
      out.insert(*i);
    }
  }
  return out;
}

/*---------Active---------*/
PeeringState::Active::Active(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active"),
    remote_shards_to_reserve_recovery(
      unique_osd_shard_set(
	context< PeeringMachine >().state->pg_whoami,
	context< PeeringMachine >().state->acting_recovery_backfill)),
    remote_shards_to_reserve_backfill(
      unique_osd_shard_set(
	context< PeeringMachine >().state->pg_whoami,
	context< PeeringMachine >().state->backfill_targets)),
    all_replicas_activated(false)
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS;

  ceph_assert(!ps->backfill_reserved);
  ceph_assert(ps->is_primary());
  psdout(10) << "In Active, about to call activate" << dendl;
  ps->start_flush(context< PeeringMachine >().get_cur_transaction());
  ps->activate(context< PeeringMachine >().get_cur_transaction(),
	       ps->get_osdmap_epoch(),
	       context< PeeringMachine >().get_recovery_ctx());

  // everyone has to commit/ack before we are truly active
  ps->blocked_by.clear();
  for (auto p = ps->acting_recovery_backfill.begin();
       p != ps->acting_recovery_backfill.end();
       ++p) {
    if (p->shard != ps->pg_whoami.shard) {
      ps->blocked_by.insert(p->shard);
    }
  }
  pl->publish_stats_to_osd();
  psdout(10) << "Activate Finished" << dendl;
}

boost::statechart::result PeeringState::Active::react(const AdvMap& advmap)
{
  DECLARE_LOCALS;

  if (ps->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    psdout(10) << "Active advmap interval change, fast return" << dendl;
    return forward_event();
  }
  psdout(10) << "Active advmap" << dendl;

  pl->on_active_advmap(advmap.osdmap);
  if (ps->dirty_big_info) {
    // share updated purged_snaps to mgr/mon so that we (a) stop reporting
    // purged snaps and (b) perhaps share more snaps that we have purged
    // but didn't fit in pg_stat_t.
    ps->share_pg_info();
  }

  bool need_acting_change = false;
  for (size_t i = 0; i < ps->want_acting.size(); i++) {
    int osd = ps->want_acting[i];
    if (!advmap.osdmap->is_up(osd)) {
      pg_shard_t osd_with_shard(osd, shard_id_t(i));
      if (!ps->is_acting(osd_with_shard) && !ps->is_up(osd_with_shard)) {
        psdout(10) << "Active stray osd." << osd << " in want_acting is down"
                   << dendl;
        need_acting_change = true;
      }
    }
  }
  if (need_acting_change) {
     psdout(10) << "Active need acting change, call choose_acting again"
                << dendl;
    // possibly because we re-add some strays into the acting set and
    // some of them then go down in a subsequent map before we could see
    // the map changing the pg temp.
    // call choose_acting again to clear them out.
    // note that we leave restrict_to_up_acting to false in order to
    // not overkill any chosen stray that is still alive.
    pg_shard_t auth_log_shard;
    bool history_les_bound = false;
    ps->remove_down_peer_info(advmap.osdmap);
    ps->choose_acting(auth_log_shard, false, &history_les_bound, true);
  }

  /* Check for changes in pool size (if the acting set changed as a result,
   * this does not matter) */
  if (advmap.lastmap->get_pg_size(ps->info.pgid.pgid) !=
      ps->get_osdmap()->get_pg_size(ps->info.pgid.pgid)) {
    if (ps->get_osdmap()->get_pg_size(ps->info.pgid.pgid) <=
	ps->actingset.size()) {
      ps->state_clear(PG_STATE_UNDERSIZED);
    } else {
      ps->state_set(PG_STATE_UNDERSIZED);
    }
    // degraded changes will be detected by call from publish_stats_to_osd()
  }

  pl->publish_stats_to_osd();

  if (ps->check_prior_readable_down_osds(advmap.osdmap)) {
    pl->recheck_readable();
  }

  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const ActMap&)
{
  DECLARE_LOCALS;
  psdout(10) << "Active: handling ActMap" << dendl;
  ceph_assert(ps->is_primary());

  pl->on_active_actmap();

  if (ps->have_unfound()) {
    // object may have become unfound
    ps->discover_all_missing(context<PeeringMachine>().get_recovery_ctx().msgs);
  }

  uint64_t unfound = ps->missing_loc.num_unfound();
  if (unfound > 0 &&
      ps->all_unfound_are_queried_or_lost(ps->get_osdmap())) {
    if (ps->cct->_conf->osd_auto_mark_unfound_lost) {
      pl->get_clog_error() << context< PeeringMachine >().spgid.pgid << " has " << unfound
			    << " objects unfound and apparently lost, would automatically "
			    << "mark these objects lost but this feature is not yet implemented "
			    << "(osd_auto_mark_unfound_lost)";
    } else
      pl->get_clog_error() << context< PeeringMachine >().spgid.pgid << " has "
                             << unfound << " objects unfound and apparently lost";
  }

  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const MNotifyRec& notevt)
{

  DECLARE_LOCALS;
  ceph_assert(ps->is_primary());
  if (ps->peer_info.count(notevt.from)) {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", already have info from that osd, ignoring"
		       << dendl;
  } else if (ps->peer_purged.count(notevt.from)) {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", already purged that peer, ignoring"
		       << dendl;
  } else {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", calling proc_replica_info and discover_all_missing"
		       << dendl;
    ps->proc_replica_info(
      notevt.from, notevt.notify.info, notevt.notify.epoch_sent);
    if (ps->have_unfound() || (ps->is_degraded() && ps->might_have_unfound.count(notevt.from))) {
      ps->discover_all_missing(
	context<PeeringMachine>().get_recovery_ctx().msgs);
    }
    // check if it is a previous down acting member that's coming back.
    // if so, request pg_temp change to trigger a new interval transition
    pg_shard_t auth_log_shard;
    bool history_les_bound = false;
    // FIXME: Uh-oh we have to check this return value; choose_acting can fail!
    ps->choose_acting(auth_log_shard, false, &history_les_bound, true);
    if (!ps->want_acting.empty() && ps->want_acting != ps->acting) {
      psdout(10) << "Active: got notify from previous acting member "
                 << notevt.from << ", requesting pg_temp change"
                 << dendl;
    }
  }
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MTrim& trim)
{
  DECLARE_LOCALS;
  ceph_assert(ps->is_primary());

  // peer is informing us of their last_complete_ondisk
  ldout(ps->cct,10) << " replica osd." << trim.from << " lcod " << trim.trim_to << dendl;
  ps->update_peer_last_complete_ondisk(pg_shard_t{trim.from, trim.shard},
                                       trim.trim_to);
  // trim log when the pg is recovered
  ps->calc_min_last_complete_ondisk();
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS;
  ceph_assert(ps->is_primary());

  ceph_assert(!ps->acting_recovery_backfill.empty());
  if (infoevt.lease_ack) {
    ps->proc_lease_ack(infoevt.from.osd, *infoevt.lease_ack);
  }
  // don't update history (yet) if we are active and primary; the replica
  // may be telling us they have activated (and committed) but we can't
  // share that until _everyone_ does the same.
  if (ps->is_acting_recovery_backfill(infoevt.from) &&
      ps->peer_activated.insert(infoevt.from).second) {
    psdout(10) << " peer osd." << infoevt.from
	       << " activated and committed" << dendl;
    ps->blocked_by.erase(infoevt.from.shard);
    pl->publish_stats_to_osd();
    if (ps->peer_activated.size() == ps->acting_recovery_backfill.size()) {
      all_activated_and_committed();
    }
  }
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MLogRec& logevt)
{
  DECLARE_LOCALS;
  psdout(10) << "searching osd." << logevt.from
		     << " log for unfound items" << dendl;
  ps->proc_replica_log(
    logevt.msg->info, logevt.msg->log, std::move(logevt.msg->missing), logevt.from);
  bool got_missing = ps->search_for_missing(
    ps->peer_info[logevt.from],
    ps->peer_missing[logevt.from],
    logevt.from,
    context< PeeringMachine >().get_recovery_ctx());
  // If there are missing AND we are "fully" active then start recovery now
  if (got_missing && ps->state_test(PG_STATE_ACTIVE)) {
    post_event(DoRecovery());
  }
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const QueryState& q)
{
  DECLARE_LOCALS;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  {
    q.f->open_array_section("might_have_unfound");
    for (auto p = ps->might_have_unfound.begin();
	 p != ps->might_have_unfound.end();
	 ++p) {
      q.f->open_object_section("osd");
      q.f->dump_stream("osd") << *p;
      if (ps->peer_missing.count(*p)) {
	q.f->dump_string("status", "already probed");
      } else if (ps->peer_missing_requested.count(*p)) {
	q.f->dump_string("status", "querying");
      } else if (!ps->get_osdmap()->is_up(p->osd)) {
	q.f->dump_string("status", "osd is down");
      } else {
	q.f->dump_string("status", "not queried");
      }
      q.f->close_section();
    }
    q.f->close_section();
  }
  {
    q.f->open_object_section("recovery_progress");
    q.f->open_array_section("backfill_targets");
    for (auto p = ps->backfill_targets.begin();
	 p != ps->backfill_targets.end(); ++p)
      q.f->dump_stream("replica") << *p;
    q.f->close_section();
    pl->dump_recovery_info(q.f);
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const QueryUnfound& q)
{
  DECLARE_LOCALS;

  ps->query_unfound(q.f, "Active");
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(
  const ActivateCommitted &evt)
{
  DECLARE_LOCALS;
  auto p = ps->peer_activated.insert(ps->pg_whoami);
  ceph_assert(p.second);
  psdout(10) << "_activate_committed " << evt.epoch
	     << " peer_activated now " << ps->peer_activated
	     << " last_interval_started "
	     << ps->info.history.last_interval_started
	     << " last_epoch_started "
	     << ps->info.history.last_epoch_started
	     << " same_interval_since "
	     << ps->info.history.same_interval_since
	     << dendl;
  ceph_assert(!ps->acting_recovery_backfill.empty());
  if (ps->peer_activated.size() == ps->acting_recovery_backfill.size())
    all_activated_and_committed();
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const AllReplicasActivated &evt)
{

  DECLARE_LOCALS;
  pg_t pgid = context< PeeringMachine >().spgid.pgid;

  all_replicas_activated = true;

  ps->state_clear(PG_STATE_ACTIVATING);
  ps->state_clear(PG_STATE_CREATING);
  ps->state_clear(PG_STATE_PREMERGE);

  bool merge_target;
  if (ps->pool.info.is_pending_merge(pgid, &merge_target)) {
    ps->state_set(PG_STATE_PEERED);
    ps->state_set(PG_STATE_PREMERGE);

    if (ps->actingset.size() != ps->get_osdmap()->get_pg_size(pgid)) {
      if (merge_target) {
	pg_t src = pgid;
	src.set_ps(ps->pool.info.get_pg_num_pending());
	assert(src.get_parent() == pgid);
	pl->set_not_ready_to_merge_target(pgid, src);
      } else {
	pl->set_not_ready_to_merge_source(pgid);
      }
    }
  } else if (!ps->acting_set_writeable()) {
    ps->state_set(PG_STATE_PEERED);
  } else {
    ps->state_set(PG_STATE_ACTIVE);
  }

  auto mnow = pl->get_mnow();
  if (ps->prior_readable_until_ub > mnow) {
    psdout(10) << " waiting for prior_readable_until_ub "
	       << ps->prior_readable_until_ub << " > mnow " << mnow << dendl;
    ps->state_set(PG_STATE_WAIT);
    pl->queue_check_readable(
      ps->last_peering_reset,
      ps->prior_readable_until_ub - mnow);
  } else {
    psdout(10) << " mnow " << mnow << " >= prior_readable_until_ub "
	       << ps->prior_readable_until_ub << dendl;
  }

  if (ps->pool.info.has_flag(pg_pool_t::FLAG_CREATING)) {
    pl->send_pg_created(pgid);
  }

  psdout(1) << __func__ << " AllReplicasActivated Activating complete" << dendl;

  ps->info.history.last_epoch_started = ps->info.last_epoch_started;
  ps->info.history.last_interval_started = ps->info.last_interval_started;
  ps->dirty_info = true;

  ps->share_pg_info();
  pl->publish_stats_to_osd();

  pl->on_activate_complete();

  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const RenewLease& rl)
{
  DECLARE_LOCALS;
  ps->proc_renew_lease();
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MLeaseAck& la)
{
  DECLARE_LOCALS;
  ps->proc_lease_ack(la.from, la.lease_ack);
  return discard_event();
}


boost::statechart::result PeeringState::Active::react(const CheckReadable &evt)
{
  DECLARE_LOCALS;
  pl->recheck_readable();
  return discard_event();
}

/*
 * update info.history.last_epoch_started ONLY after we and all
 * replicas have activated AND committed the activate transaction
 * (i.e. the peering results are stable on disk).
 */
void PeeringState::Active::all_activated_and_committed()
{
  DECLARE_LOCALS;
  psdout(10) << "all_activated_and_committed" << dendl;
  ceph_assert(ps->is_primary());
  ceph_assert(ps->peer_activated.size() == ps->acting_recovery_backfill.size());
  ceph_assert(!ps->acting_recovery_backfill.empty());
  ceph_assert(ps->blocked_by.empty());

  assert(HAVE_FEATURE(ps->upacting_features, SERVER_OCTOPUS));
  // this is overkill when the activation is quick, but when it is slow it
  // is important, because the lease was renewed by the activate itself but we
  // don't know how long ago that was, and simply scheduling now may leave
  // a gap in lease coverage.  keep it simple and aggressively renew.
  ps->renew_lease(pl->get_mnow());
  ps->send_lease();
  ps->schedule_renew_lease();

  // Degraded?
  ps->update_calc_stats();
  if (ps->info.stats.stats.sum.num_objects_degraded) {
    ps->state_set(PG_STATE_DEGRADED);
  } else {
    ps->state_clear(PG_STATE_DEGRADED);
  }

  post_event(PeeringState::AllReplicasActivated());
}


void PeeringState::Active::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);


  DECLARE_LOCALS;
  pl->cancel_local_background_io_reservation();

  ps->blocked_by.clear();
  ps->backfill_reserved = false;
  ps->state_clear(PG_STATE_ACTIVATING);
  ps->state_clear(PG_STATE_DEGRADED);
  ps->state_clear(PG_STATE_UNDERSIZED);
  ps->state_clear(PG_STATE_BACKFILL_TOOFULL);
  ps->state_clear(PG_STATE_BACKFILL_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_TOOFULL);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_active_latency, dur);
  pl->on_active_exit();
}

/*------ReplicaActive-----*/
PeeringState::ReplicaActive::ReplicaActive(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;
  ps->start_flush(context< PeeringMachine >().get_cur_transaction());
}


boost::statechart::result PeeringState::ReplicaActive::react(
  const Activate& actevt) {
  DECLARE_LOCALS;
  psdout(10) << "In ReplicaActive, about to call activate" << dendl;
  ps->activate(
    context< PeeringMachine >().get_cur_transaction(),
    actevt.activation_epoch,
    context< PeeringMachine >().get_recovery_ctx());
  psdout(10) << "Activate Finished" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(
  const ActivateCommitted &evt)
{
  DECLARE_LOCALS;
  psdout(10) << __func__ << " " << evt.epoch << " telling primary" << dendl;

  auto &rctx = context<PeeringMachine>().get_recovery_ctx();
  auto epoch = ps->get_osdmap_epoch();
  pg_info_t i = ps->info;
  i.history.last_epoch_started = evt.activation_epoch;
  i.history.last_interval_started = i.history.same_interval_since;
  rctx.send_info(
    ps->get_primary().osd,
    spg_t(ps->info.pgid.pgid, ps->get_primary().shard),
    epoch,
    epoch,
    i,
    {}, /* lease */
    ps->get_lease_ack());

  if (ps->acting_set_writeable()) {
    ps->state_set(PG_STATE_ACTIVE);
  } else {
    ps->state_set(PG_STATE_PEERED);
  }
  pl->on_activate_committed();

  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MLease& l)
{
  DECLARE_LOCALS;
  spg_t spgid = context< PeeringMachine >().spgid;
  epoch_t epoch = pl->get_osdmap_epoch();

  ps->proc_lease(l.lease);
  pl->send_cluster_message(
    ps->get_primary().osd,
    TOPNSPC::make_message<MOSDPGLeaseAck>(epoch,
		       spg_t(spgid.pgid, ps->get_primary().shard),
		       ps->get_lease_ack()),
    epoch);
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS;
  ps->proc_primary_info(context<PeeringMachine>().get_cur_transaction(),
			infoevt.info);
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MLogRec& logevt)
{
  DECLARE_LOCALS;
  psdout(10) << "received log from " << logevt.from << dendl;
  ObjectStore::Transaction &t = context<PeeringMachine>().get_cur_transaction();
  ps->merge_log(t, logevt.msg->info, std::move(logevt.msg->log), logevt.from);
  ceph_assert(ps->pg_log.get_head() == ps->info.last_update);
  if (logevt.msg->lease) {
    ps->proc_lease(*logevt.msg->lease);
  }

  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MTrim& trim)
{
  DECLARE_LOCALS;
  // primary is instructing us to trim
  ps->pg_log.trim(trim.trim_to, ps->info);
  ps->dirty_info = true;
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const ActMap&)
{
  DECLARE_LOCALS;
  if (ps->should_send_notify() && ps->get_primary().osd >= 0) {
    ps->info.history.refresh_prior_readable_until_ub(
      pl->get_mnow(), ps->prior_readable_until_ub);
    context< PeeringMachine >().send_notify(
      ps->get_primary().osd,
      pg_notify_t(
	ps->get_primary().shard, ps->pg_whoami.shard,
	ps->get_osdmap_epoch(),
	ps->get_osdmap_epoch(),
	ps->info,
	ps->past_intervals));
  }
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(
  const MQuery& query)
{
  DECLARE_LOCALS;
  ps->fulfill_query(query, context<PeeringMachine>().get_recovery_ctx());
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "ReplicaActive");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::ReplicaActive::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_replicaactive_latency, dur);

  ps->min_last_complete_ondisk = eversion_t();
}

/*-------Stray---*/
PeeringState::Stray::Stray(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Stray")
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS;
  ceph_assert(!ps->is_peered());
  ceph_assert(!ps->is_peering());
  ceph_assert(!ps->is_primary());

  if (!ps->get_osdmap()->have_pg_pool(ps->info.pgid.pgid.pool())) {
    ldout(ps->cct,10) << __func__ << " pool is deleted" << dendl;
    post_event(DeleteStart());
  } else {
    ps->start_flush(context< PeeringMachine >().get_cur_transaction());
  }
}

boost::statechart::result PeeringState::Stray::react(const MLogRec& logevt)
{
  DECLARE_LOCALS;
  MOSDPGLog *msg = logevt.msg.get();
  psdout(10) << "got info+log from osd." << logevt.from << " " << msg->info << " " << msg->log << dendl;

  ObjectStore::Transaction &t = context<PeeringMachine>().get_cur_transaction();
  if (msg->info.last_backfill == hobject_t()) {
    // restart backfill
    ps->info = msg->info;
    ps->dirty_info = true;
    ps->dirty_big_info = true;  // maybe.

    PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
    ps->pg_log.reset_backfill_claim_log(msg->log, rollbacker.get());

    ps->pg_log.reset_backfill();
  } else {
    ps->merge_log(t, msg->info, std::move(msg->log), logevt.from);
  }
  if (logevt.msg->lease) {
    ps->proc_lease(*logevt.msg->lease);
  }

  ceph_assert(ps->pg_log.get_head() == ps->info.last_update);

  post_event(Activate(logevt.msg->info.last_epoch_started));
  return transit<ReplicaActive>();
}

boost::statechart::result PeeringState::Stray::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS;
  psdout(10) << "got info from osd." << infoevt.from << " " << infoevt.info << dendl;

  if (ps->info.last_update > infoevt.info.last_update) {
    // rewind divergent log entries
    ObjectStore::Transaction &t = context<PeeringMachine>().get_cur_transaction();
    ps->rewind_divergent_log(t, infoevt.info.last_update);
    ps->info.stats = infoevt.info.stats;
    ps->info.hit_set = infoevt.info.hit_set;
  }

  if (infoevt.lease) {
    ps->proc_lease(*infoevt.lease);
  }

  ceph_assert(infoevt.info.last_update == ps->info.last_update);
  ceph_assert(ps->pg_log.get_head() == ps->info.last_update);

  post_event(Activate(infoevt.info.last_epoch_started));
  return transit<ReplicaActive>();
}

boost::statechart::result PeeringState::Stray::react(const MQuery& query)
{
  DECLARE_LOCALS;
  ps->fulfill_query(query, context<PeeringMachine>().get_recovery_ctx());
  return discard_event();
}

boost::statechart::result PeeringState::Stray::react(const ActMap&)
{
  DECLARE_LOCALS;
  if (ps->should_send_notify() && ps->get_primary().osd >= 0) {
    ps->info.history.refresh_prior_readable_until_ub(
      pl->get_mnow(), ps->prior_readable_until_ub);
    context< PeeringMachine >().send_notify(
      ps->get_primary().osd,
      pg_notify_t(
	ps->get_primary().shard, ps->pg_whoami.shard,
	ps->get_osdmap_epoch(),
	ps->get_osdmap_epoch(),
	ps->info,
	ps->past_intervals));
  }
  return discard_event();
}

void PeeringState::Stray::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_stray_latency, dur);
}


/*--------ToDelete----------*/
PeeringState::ToDelete::ToDelete(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ToDelete")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;
  pl->get_perf_logger().inc(l_osd_pg_removing);
}

void PeeringState::ToDelete::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  // note: on a successful removal, this path doesn't execute. see
  // do_delete_work().
  pl->get_perf_logger().dec(l_osd_pg_removing);

  pl->cancel_local_background_io_reservation();
}

/*----WaitDeleteReserved----*/
PeeringState::WaitDeleteReserved::WaitDeleteReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history,
	       "Started/ToDelete/WaitDeleteReserved")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;
  context< ToDelete >().priority = ps->get_delete_priority();

  pl->cancel_local_background_io_reservation();
  pl->request_local_background_io_reservation(
    context<ToDelete>().priority,
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeleteReserved()),
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeleteInterrupted()));
}

boost::statechart::result PeeringState::ToDelete::react(
  const ActMap& evt)
{
  DECLARE_LOCALS;
  if (ps->get_delete_priority() != priority) {
    psdout(10) << __func__ << " delete priority changed, resetting"
		   << dendl;
    return transit<ToDelete>();
  }
  return discard_event();
}

void PeeringState::WaitDeleteReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
}

/*----Deleting-----*/
PeeringState::Deleting::Deleting(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ToDelete/Deleting")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;
  ps->deleting = true;
  ObjectStore::Transaction &t = context<PeeringMachine>().get_cur_transaction();

  // clear log
  PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
  ps->pg_log.roll_forward(rollbacker.get());

  // adjust info to backfill
  ps->info.set_last_backfill(hobject_t());
  ps->pg_log.reset_backfill();
  ps->dirty_info = true;

  pl->on_removal(t);
}

boost::statechart::result PeeringState::Deleting::react(
  const DeleteSome& evt)
{
  DECLARE_LOCALS;
  std::pair<ghobject_t, bool> p;
  p = pl->do_delete_work(context<PeeringMachine>().get_cur_transaction(),
    next);
  next = p.first;
  return p.second ? discard_event() : terminate();
}

void PeeringState::Deleting::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  ps->deleting = false;
  pl->cancel_local_background_io_reservation();
}

/*--------GetInfo---------*/
PeeringState::GetInfo::GetInfo(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/GetInfo")
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS;
  ps->check_past_interval_bounds();
  ps->log_weirdness();
  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;

  ceph_assert(ps->blocked_by.empty());

  prior_set = ps->build_prior();
  ps->prior_readable_down_osds = prior_set.down;

  if (ps->prior_readable_down_osds.empty()) {
    psdout(10) << " no prior_set down osds, will clear prior_readable_until_ub before activating"
	       << dendl;
  }

  ps->reset_min_peer_features();
  get_infos();
  if (prior_set.pg_down) {
    post_event(IsDown());
  } else if (peer_info_requested.empty()) {
    post_event(GotInfo());
  }
}

void PeeringState::GetInfo::get_infos()
{
  DECLARE_LOCALS;
  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;

  ps->blocked_by.clear();
  for (auto it = prior_set.probe.begin(); it != prior_set.probe.end(); ++it) {
    pg_shard_t peer = *it;
    if (peer == ps->pg_whoami) {
      continue;
    }
    if (ps->peer_info.count(peer)) {
      psdout(10) << " have osd." << peer << " info " << ps->peer_info[peer] << dendl;
      continue;
    }
    if (peer_info_requested.count(peer)) {
      psdout(10) << " already requested info from osd." << peer << dendl;
      ps->blocked_by.insert(peer.osd);
    } else if (!ps->get_osdmap()->is_up(peer.osd)) {
      psdout(10) << " not querying info from down osd." << peer << dendl;
    } else {
      psdout(10) << " querying info from osd." << peer << dendl;
      context< PeeringMachine >().send_query(
	peer.osd,
	pg_query_t(pg_query_t::INFO,
		   it->shard, ps->pg_whoami.shard,
		   ps->info.history,
		   ps->get_osdmap_epoch()));
      peer_info_requested.insert(peer);
      ps->blocked_by.insert(peer.osd);
    }
  }

  ps->check_prior_readable_down_osds(ps->get_osdmap());

  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::GetInfo::react(const MNotifyRec& infoevt)
{

  DECLARE_LOCALS;

  auto p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end()) {
    peer_info_requested.erase(p);
    ps->blocked_by.erase(infoevt.from.osd);
  }

  epoch_t old_start = ps->info.history.last_epoch_started;
  if (ps->proc_replica_info(
	infoevt.from, infoevt.notify.info, infoevt.notify.epoch_sent)) {
    // we got something new ...
    PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;
    if (old_start < ps->info.history.last_epoch_started) {
      psdout(10) << " last_epoch_started moved forward, rebuilding prior" << dendl;
      prior_set = ps->build_prior();
      ps->prior_readable_down_osds = prior_set.down;

      // filter out any osds that got dropped from the probe set from
      // peer_info_requested.  this is less expensive than restarting
      // peering (which would re-probe everyone).
      auto p = peer_info_requested.begin();
      while (p != peer_info_requested.end()) {
	if (prior_set.probe.count(*p) == 0) {
	  psdout(20) << " dropping osd." << *p << " from info_requested, no longer in probe set" << dendl;
	  peer_info_requested.erase(p++);
	} else {
	  ++p;
	}
      }
      get_infos();
    }
    psdout(20) << "Adding osd: " << infoevt.from.osd << " peer features: "
		       << hex << infoevt.features << dec << dendl;
    ps->apply_peer_features(infoevt.features);

    // are we done getting everything?
    if (peer_info_requested.empty() && !prior_set.pg_down) {
      psdout(20) << "Common peer features: " << hex << ps->get_min_peer_features() << dec << dendl;
      psdout(20) << "Common acting features: " << hex << ps->get_min_acting_features() << dec << dendl;
      psdout(20) << "Common upacting features: " << hex << ps->get_min_upacting_features() << dec << dendl;
      post_event(GotInfo());
    }
  }
  return discard_event();
}

boost::statechart::result PeeringState::GetInfo::react(const QueryState& q)
{
  DECLARE_LOCALS;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("requested_info_from");
  for (auto p = peer_info_requested.begin();
       p != peer_info_requested.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_stream("osd") << *p;
    if (ps->peer_info.count(*p)) {
      q.f->open_object_section("got_info");
      ps->peer_info[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::GetInfo::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "GetInfo");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::GetInfo::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_getinfo_latency, dur);
  ps->blocked_by.clear();
}

/*------GetLog------------*/
PeeringState::GetLog::GetLog(my_context ctx)
  : my_base(ctx),
    NamedState(
      context< PeeringMachine >().state_history,
      "Started/Primary/Peering/GetLog"),
    msg(0)
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;

  ps->log_weirdness();

  // adjust acting?
  if (!ps->choose_acting(auth_log_shard, false,
			 &context< Peering >().history_les_bound)) {
    if (!ps->want_acting.empty()) {
      post_event(NeedActingChange());
    } else {
      post_event(IsIncomplete());
    }
    return;
  }

  // am i the best?
  if (auth_log_shard == ps->pg_whoami) {
    post_event(GotLog());
    return;
  }

  const pg_info_t& best = ps->peer_info[auth_log_shard];

  // am i broken?
  if (ps->info.last_update < best.log_tail) {
    psdout(10) << " not contiguous with osd." << auth_log_shard << ", down" << dendl;
    post_event(IsIncomplete());
    return;
  }

  // how much log to request?
  eversion_t request_log_from = ps->info.last_update;
  ceph_assert(!ps->acting_recovery_backfill.empty());
  for (auto p = ps->acting_recovery_backfill.begin();
       p != ps->acting_recovery_backfill.end();
       ++p) {
    if (*p == ps->pg_whoami) continue;
    pg_info_t& ri = ps->peer_info[*p];
    if (ri.last_update < ps->info.log_tail && ri.last_update >= best.log_tail &&
        ri.last_update < request_log_from)
      request_log_from = ri.last_update;
  }

  // how much?
  psdout(10) << " requesting log from osd." << auth_log_shard << dendl;
  context<PeeringMachine>().send_query(
    auth_log_shard.osd,
    pg_query_t(
      pg_query_t::LOG,
      auth_log_shard.shard, ps->pg_whoami.shard,
      request_log_from, ps->info.history,
      ps->get_osdmap_epoch()));

  ceph_assert(ps->blocked_by.empty());
  ps->blocked_by.insert(auth_log_shard.osd);
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::GetLog::react(const AdvMap& advmap)
{
  // make sure our log source didn't go down.  we need to check
  // explicitly because it may not be part of the prior set, which
  // means the Peering state check won't catch it going down.
  if (!advmap.osdmap->is_up(auth_log_shard.osd)) {
    psdout(10) << "GetLog: auth_log_shard osd."
		       << auth_log_shard.osd << " went down" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  // let the Peering state do its checks.
  return forward_event();
}

boost::statechart::result PeeringState::GetLog::react(const MLogRec& logevt)
{
  ceph_assert(!msg);
  if (logevt.from != auth_log_shard) {
    psdout(10) << "GetLog: discarding log from "
		       << "non-auth_log_shard osd." << logevt.from << dendl;
    return discard_event();
  }
  psdout(10) << "GetLog: received master log from osd."
		     << logevt.from << dendl;
  msg = logevt.msg;
  post_event(GotLog());
  return discard_event();
}

boost::statechart::result PeeringState::GetLog::react(const GotLog&)
{

  DECLARE_LOCALS;
  psdout(10) << "leaving GetLog" << dendl;
  if (msg) {
    psdout(10) << "processing master log" << dendl;
    ps->proc_master_log(context<PeeringMachine>().get_cur_transaction(),
			msg->info, std::move(msg->log), std::move(msg->missing),
			auth_log_shard);
  }
  ps->start_flush(context< PeeringMachine >().get_cur_transaction());
  return transit< GetMissing >();
}

boost::statechart::result PeeringState::GetLog::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_stream("auth_log_shard") << auth_log_shard;
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::GetLog::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "GetLog");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::GetLog::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_getlog_latency, dur);
  ps->blocked_by.clear();
}

/*------WaitActingChange--------*/
PeeringState::WaitActingChange::WaitActingChange(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/WaitActingChange")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result PeeringState::WaitActingChange::react(const AdvMap& advmap)
{
  DECLARE_LOCALS;
  OSDMapRef osdmap = advmap.osdmap;

  psdout(10) << "verifying no want_acting " << ps->want_acting << " targets didn't go down" << dendl;
  for (auto p = ps->want_acting.begin(); p != ps->want_acting.end(); ++p) {
    if (!osdmap->is_up(*p)) {
      psdout(10) << " want_acting target osd." << *p << " went down, resetting" << dendl;
      post_event(advmap);
      return transit< Reset >();
    }
  }
  return forward_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const MLogRec& logevt)
{
  psdout(10) << "In WaitActingChange, ignoring MLocRec" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const MInfoRec& evt)
{
  psdout(10) << "In WaitActingChange, ignoring MInfoRec" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const MNotifyRec& evt)
{
  psdout(10) << "In WaitActingChange, ignoring MNotifyRec" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for pg acting set to change");
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "WaitActingChange");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::WaitActingChange::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitactingchange_latency, dur);
}

/*------Down--------*/
PeeringState::Down::Down(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/Down")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ps->state_clear(PG_STATE_PEERING);
  ps->state_set(PG_STATE_DOWN);

  auto &prior_set = context< Peering >().prior_set;
  ceph_assert(ps->blocked_by.empty());
  ps->blocked_by.insert(prior_set.down.begin(), prior_set.down.end());
  pl->publish_stats_to_osd();
}

void PeeringState::Down::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;

  ps->state_clear(PG_STATE_DOWN);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_down_latency, dur);

  ps->blocked_by.clear();
}

boost::statechart::result PeeringState::Down::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment",
		   "not enough up instances of this PG to go active");
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::Down::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "Down");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

boost::statechart::result PeeringState::Down::react(const MNotifyRec& infoevt)
{
  DECLARE_LOCALS;

  ceph_assert(ps->is_primary());
  epoch_t old_start = ps->info.history.last_epoch_started;
  if (!ps->peer_info.count(infoevt.from) &&
      ps->get_osdmap()->has_been_up_since(infoevt.from.osd, infoevt.notify.epoch_sent)) {
    ps->update_history(infoevt.notify.info.history);
  }
  // if we got something new to make pg escape down state
  if (ps->info.history.last_epoch_started > old_start) {
      psdout(10) << " last_epoch_started moved forward, re-enter getinfo" << dendl;
    ps->state_clear(PG_STATE_DOWN);
    ps->state_set(PG_STATE_PEERING);
    return transit< GetInfo >();
  }

  return discard_event();
}


/*------Incomplete--------*/
PeeringState::Incomplete::Incomplete(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/Incomplete")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ps->state_clear(PG_STATE_PEERING);
  ps->state_set(PG_STATE_INCOMPLETE);

  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;
  ceph_assert(ps->blocked_by.empty());
  ps->blocked_by.insert(prior_set.down.begin(), prior_set.down.end());
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::Incomplete::react(const AdvMap &advmap) {
  DECLARE_LOCALS;
  int64_t poolnum = ps->info.pgid.pool();

  // Reset if min_size turn smaller than previous value, pg might now be able to go active
  if (!advmap.osdmap->have_pg_pool(poolnum) ||
      advmap.lastmap->get_pools().find(poolnum)->second.min_size >
      advmap.osdmap->get_pools().find(poolnum)->second.min_size) {
    post_event(advmap);
    return transit< Reset >();
  }

  return forward_event();
}

boost::statechart::result PeeringState::Incomplete::react(const MNotifyRec& notevt) {
  DECLARE_LOCALS;
  psdout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  if (ps->proc_replica_info(
    notevt.from, notevt.notify.info, notevt.notify.epoch_sent)) {
    // We got something new, try again!
    return transit< GetLog >();
  } else {
    return discard_event();
  }
}

boost::statechart::result PeeringState::Incomplete::react(
  const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "not enough complete instances of this PG");
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::Incomplete::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "Incomplete");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::Incomplete::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;

  ps->state_clear(PG_STATE_INCOMPLETE);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_incomplete_latency, dur);

  ps->blocked_by.clear();
}

/*------GetMissing--------*/
PeeringState::GetMissing::GetMissing(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/GetMissing")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;
  ps->log_weirdness();
  ceph_assert(!ps->acting_recovery_backfill.empty());
  eversion_t since;
  for (auto i = ps->acting_recovery_backfill.begin();
       i != ps->acting_recovery_backfill.end();
       ++i) {
    if (*i == ps->get_primary()) continue;
    const pg_info_t& pi = ps->peer_info[*i];
    // reset this so to make sure the pg_missing_t is initialized and
    // has the correct semantics even if we don't need to get a
    // missing set from a shard. This way later additions due to
    // lost+unfound delete work properly.
    ps->peer_missing[*i].may_include_deletes = !ps->perform_deletes_during_peering();

    if (pi.is_empty())
      continue;                                // no pg data, nothing divergent

    if (pi.last_update < ps->pg_log.get_tail()) {
      psdout(10) << " osd." << *i << " is not contiguous, will restart backfill" << dendl;
      ps->peer_missing[*i].clear();
      continue;
    }
    if (pi.last_backfill == hobject_t()) {
      psdout(10) << " osd." << *i << " will fully backfill; can infer empty missing set" << dendl;
      ps->peer_missing[*i].clear();
      continue;
    }

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == ps->info.last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      psdout(10) << " osd." << *i << " has no missing, identical log" << dendl;
      ps->peer_missing[*i].clear();
      continue;
    }

    // We pull the log from the peer's last_epoch_started to ensure we
    // get enough log to detect divergent updates.
    since.epoch = pi.last_epoch_started;
    ceph_assert(pi.last_update >= ps->info.log_tail);  // or else choose_acting() did a bad thing
    if (pi.log_tail <= since) {
      psdout(10) << " requesting log+missing since " << since << " from osd." << *i << dendl;
      context< PeeringMachine >().send_query(
	i->osd,
	pg_query_t(
	  pg_query_t::LOG,
	  i->shard, ps->pg_whoami.shard,
	  since, ps->info.history,
	  ps->get_osdmap_epoch()));
    } else {
      psdout(10) << " requesting fulllog+missing from osd." << *i
			 << " (want since " << since << " < log.tail "
			 << pi.log_tail << ")" << dendl;
      context< PeeringMachine >().send_query(
	i->osd, pg_query_t(
	  pg_query_t::FULLLOG,
	  i->shard, ps->pg_whoami.shard,
	  ps->info.history, ps->get_osdmap_epoch()));
    }
    peer_missing_requested.insert(*i);
    ps->blocked_by.insert(i->osd);
  }

  if (peer_missing_requested.empty()) {
    if (ps->need_up_thru) {
      psdout(10) << " still need up_thru update before going active"
			 << dendl;
      post_event(NeedUpThru());
      return;
    }

    // all good!
    post_event(Activate(ps->get_osdmap_epoch()));
  } else {
    pl->publish_stats_to_osd();
  }
}

boost::statechart::result PeeringState::GetMissing::react(const MLogRec& logevt)
{
  DECLARE_LOCALS;

  peer_missing_requested.erase(logevt.from);
  ps->proc_replica_log(logevt.msg->info,
		       logevt.msg->log,
		       std::move(logevt.msg->missing),
		       logevt.from);

  if (peer_missing_requested.empty()) {
    if (ps->need_up_thru) {
      psdout(10) << " still need up_thru update before going active"
			 << dendl;
      post_event(NeedUpThru());
    } else {
      psdout(10) << "Got last missing, don't need missing "
			 << "posting Activate" << dendl;
      post_event(Activate(ps->get_osdmap_epoch()));
    }
  }
  return discard_event();
}

boost::statechart::result PeeringState::GetMissing::react(const QueryState& q)
{
  DECLARE_LOCALS;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("peer_missing_requested");
  for (auto p = peer_missing_requested.begin();
       p != peer_missing_requested.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_stream("osd") << *p;
    if (ps->peer_missing.count(*p)) {
      q.f->open_object_section("got_missing");
      ps->peer_missing[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::GetMissing::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "GetMising");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::GetMissing::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_getmissing_latency, dur);
  ps->blocked_by.clear();
}

/*------WaitUpThru--------*/
PeeringState::WaitUpThru::WaitUpThru(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/WaitUpThru")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result PeeringState::WaitUpThru::react(const ActMap& am)
{
  DECLARE_LOCALS;
  if (!ps->need_up_thru) {
    post_event(Activate(ps->get_osdmap_epoch()));
  }
  return forward_event();
}

boost::statechart::result PeeringState::WaitUpThru::react(const MLogRec& logevt)
{
  DECLARE_LOCALS;
  psdout(10) << "Noting missing from osd." << logevt.from << dendl;
  ps->peer_missing[logevt.from].claim(std::move(logevt.msg->missing));
  ps->peer_info[logevt.from] = logevt.msg->info;
  return discard_event();
}

boost::statechart::result PeeringState::WaitUpThru::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for osdmap to reflect a new up_thru for this osd");
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::WaitUpThru::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "WaitUpThru");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::WaitUpThru::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitupthru_latency, dur);
}

/*----PeeringState::PeeringMachine Methods-----*/
#undef dout_prefix
#define dout_prefix dpp->gen_prefix(*_dout)

void PeeringState::PeeringMachine::log_enter(const char *state_name)
{
  DECLARE_LOCALS;
  psdout(5) << "enter " << state_name << dendl;
  pl->log_state_enter(state_name);
}

void PeeringState::PeeringMachine::log_exit(const char *state_name, utime_t enter_time)
{
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  psdout(5) << "exit " << state_name << " " << dur << " " << event_count << " " << event_time << dendl;
  pl->log_state_exit(state_name, enter_time, event_count, event_time);
  event_count = 0;
  event_time = utime_t();
}

ostream &operator<<(ostream &out, const PeeringState &ps) {
  out << "pg[" << ps.info
      << " " << pg_vector_string(ps.up);
  if (ps.acting != ps.up)
    out << "/" << pg_vector_string(ps.acting);
  if (ps.is_ec_pg())
    out << "p" << ps.get_primary();
  if (!ps.async_recovery_targets.empty())
    out << " async=[" << ps.async_recovery_targets << "]";
  if (!ps.backfill_targets.empty())
    out << " backfill=[" << ps.backfill_targets << "]";
  out << " r=" << ps.get_role();
  out << " lpr=" << ps.get_last_peering_reset();

  if (ps.deleting)
    out << " DELETING";

  if (!ps.past_intervals.empty()) {
    out << " pi=[" << ps.past_intervals.get_bounds()
	<< ")/" << ps.past_intervals.size();
  }

  if (ps.is_peered()) {
    if (ps.last_update_ondisk != ps.info.last_update)
      out << " luod=" << ps.last_update_ondisk;
    if (ps.last_update_applied != ps.info.last_update)
      out << " lua=" << ps.last_update_applied;
  }

  if (ps.pg_log.get_tail() != ps.info.log_tail ||
      ps.pg_log.get_head() != ps.info.last_update)
    out << " (info mismatch, " << ps.pg_log.get_log() << ")";

  if (!ps.pg_log.get_log().empty()) {
    if ((ps.pg_log.get_log().log.begin()->version <= ps.pg_log.get_tail())) {
      out << " (log bound mismatch, actual=["
	  << ps.pg_log.get_log().log.begin()->version << ","
	  << ps.pg_log.get_log().log.rbegin()->version << "]";
      out << ")";
    }
  }

  out << " crt=" << ps.pg_log.get_can_rollback_to();

  if (ps.last_complete_ondisk != ps.info.last_complete)
    out << " lcod " << ps.last_complete_ondisk;

  out << " mlcod " << ps.min_last_complete_ondisk;

  out << " " << pg_state_string(ps.get_state());
  if (ps.should_send_notify())
    out << " NOTIFY";

  if (ps.prior_readable_until_ub != ceph::signedspan::zero()) {
    out << " pruub " << ps.prior_readable_until_ub
	<< "@" << ps.get_prior_readable_down_osds();
  }
  return out;
}

std::vector<pg_shard_t> PeeringState::get_replica_recovery_order() const
{
  std::vector<std::pair<unsigned int, pg_shard_t>> replicas_by_num_missing,
    async_by_num_missing;
  replicas_by_num_missing.reserve(get_acting_recovery_backfill().size() - 1);
  for (auto &p : get_acting_recovery_backfill()) {
    if (p == get_primary()) {
      continue;
    }
    auto pm = get_peer_missing().find(p);
    assert(pm != get_peer_missing().end());
    auto nm = pm->second.num_missing();
    if (nm != 0) {
      if (is_async_recovery_target(p)) {
	async_by_num_missing.push_back(make_pair(nm, p));
      } else {
	replicas_by_num_missing.push_back(make_pair(nm, p));
      }
    }
  }
  // sort by number of missing objects, in ascending order.
  auto func = [](const std::pair<unsigned int, pg_shard_t> &lhs,
		 const std::pair<unsigned int, pg_shard_t> &rhs) {
    return lhs.first < rhs.first;
  };
  // acting goes first
  std::sort(replicas_by_num_missing.begin(), replicas_by_num_missing.end(), func);
  // then async_recovery_targets
  std::sort(async_by_num_missing.begin(), async_by_num_missing.end(), func);
  replicas_by_num_missing.insert(replicas_by_num_missing.end(),
    async_by_num_missing.begin(), async_by_num_missing.end());

  std::vector<pg_shard_t> ret;
  ret.reserve(replicas_by_num_missing.size());
  for (auto p : replicas_by_num_missing) {
    ret.push_back(p.second);
  }
  return ret;
}


