// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg.h"

#include <functional>

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/max_element.hpp>
#include <boost/range/numeric.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"

#include "osd/OSDMap.h"

#include "os/Transaction.h"

#include "crimson/common/exception.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/os/cyanstore/cyan_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/exceptions.h"
#include "crimson/osd/pg_meta.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/ops_executer.h"
#include "crimson/osd/osd_operations/osdop_params.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/pg_recovery.h"
#include "crimson/osd/replicated_recovery_backend.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace std::chrono {
std::ostream& operator<<(std::ostream& out, const signedspan& d)
{
  auto s = std::chrono::duration_cast<std::chrono::seconds>(d).count();
  auto ns = std::abs((d % 1s).count());
  fmt::print(out, "{}{}s", s, ns ? fmt::format(".{:0>9}", ns) : "");
  return out;
}
}

namespace crimson::osd {

using crimson::common::local_conf;

class RecoverablePredicate : public IsPGRecoverablePredicate {
public:
  bool operator()(const set<pg_shard_t> &have) const override {
    return !have.empty();
  }
};

class ReadablePredicate: public IsPGReadablePredicate {
  pg_shard_t whoami;
public:
  explicit ReadablePredicate(pg_shard_t whoami) : whoami(whoami) {}
  bool operator()(const set<pg_shard_t> &have) const override {
    return have.count(whoami);
  }
};

PG::PG(
  spg_t pgid,
  pg_shard_t pg_shard,
  crimson::os::CollectionRef coll_ref,
  pg_pool_t&& pool,
  std::string&& name,
  cached_map_t osdmap,
  ShardServices &shard_services,
  ec_profile_t profile)
  : pgid{pgid},
    pg_whoami{pg_shard},
    coll_ref{coll_ref},
    pgmeta_oid{pgid.make_pgmeta_oid()},
    osdmap_gate("PG::osdmap_gate", std::nullopt),
    shard_services{shard_services},
    osdmap{osdmap},
    backend(
      PGBackend::create(
	pgid.pgid,
	pg_shard,
	pool,
	coll_ref,
	shard_services,
	profile)),
    recovery_backend(
      std::make_unique<ReplicatedRecoveryBackend>(
	*this, shard_services, coll_ref, backend.get())),
    recovery_handler(
      std::make_unique<PGRecovery>(this)),
    peering_state(
      shard_services.get_cct(),
      pg_shard,
      pgid,
      PGPool(
	osdmap,
	pgid.pool(),
	pool,
	name),
      osdmap,
      this,
      this),
    wait_for_active_blocker(this)
{
  peering_state.set_backend_predicates(
    new ReadablePredicate(pg_whoami),
    new RecoverablePredicate());
  osdmap_gate.got_map(osdmap->get_epoch());
}

PG::~PG() {}

bool PG::try_flush_or_schedule_async() {
  (void)shard_services.get_store().do_transaction(
    coll_ref,
    ObjectStore::Transaction()).then(
      [this, epoch=get_osdmap_epoch()]() {
	return shard_services.start_operation<LocalPeeringEvent>(
	  this,
	  shard_services,
	  pg_whoami,
	  pgid,
	  epoch,
	  epoch,
	  PeeringState::IntervalFlush());
      });
  return false;
}

void PG::queue_check_readable(epoch_t last_peering_reset, ceph::timespan delay)
{
  // handle the peering event in the background
  check_readable_timer.cancel();
  check_readable_timer.set_callback([last_peering_reset, this] {
    (void) shard_services.start_operation<LocalPeeringEvent>(
      this,
      shard_services,
      pg_whoami,
      pgid,
      last_peering_reset,
      last_peering_reset,
      PeeringState::CheckReadable{});
    });
  check_readable_timer.arm(
    std::chrono::duration_cast<seastar::lowres_clock::duration>(delay));
}

void PG::recheck_readable()
{
  bool changed = false;
  const auto mnow = shard_services.get_mnow();
  if (peering_state.state_test(PG_STATE_WAIT)) {
    auto prior_readable_until_ub = peering_state.get_prior_readable_until_ub();
    if (mnow < prior_readable_until_ub) {
      logger().info("{} will wait (mnow {} < prior_readable_until_ub {})",
		    __func__, mnow, prior_readable_until_ub);
    } else {
      logger().info("{} no longer wait (mnow {} >= prior_readable_until_ub {})",
		    __func__, mnow, prior_readable_until_ub);
      peering_state.state_clear(PG_STATE_WAIT);
      peering_state.clear_prior_readable_until_ub();
      changed = true;
    }
  }
  if (peering_state.state_test(PG_STATE_LAGGY)) {
    auto readable_until = peering_state.get_readable_until();
    if (readable_until == readable_until.zero()) {
      logger().info("{} still laggy (mnow {}, readable_until zero)",
		    __func__, mnow);
    } else if (mnow >= readable_until) {
      logger().info("{} still laggy (mnow {} >= readable_until {})",
		    __func__, mnow, readable_until);
    } else {
      logger().info("{} no longer laggy (mnow {} < readable_until {})",
		    __func__, mnow, readable_until);
      peering_state.state_clear(PG_STATE_LAGGY);
      changed = true;
    }
  }
  if (changed) {
    publish_stats_to_osd();
    if (!peering_state.state_test(PG_STATE_WAIT) &&
	!peering_state.state_test(PG_STATE_LAGGY)) {
      // TODO: requeue ops waiting for readable
    }
  }
}

unsigned PG::get_target_pg_log_entries() const
{
  const unsigned num_pgs = shard_services.get_pg_num();
  const unsigned target =
    local_conf().get_val<uint64_t>("osd_target_pg_log_entries_per_osd");
  const unsigned min_pg_log_entries =
    local_conf().get_val<uint64_t>("osd_min_pg_log_entries");
  if (num_pgs > 0 && target > 0) {
    // target an even spread of our budgeted log entries across all
    // PGs.  note that while we only get to control the entry count
    // for primary PGs, we'll normally be responsible for a mix of
    // primary and replica PGs (for the same pool(s) even), so this
    // will work out.
    const unsigned max_pg_log_entries =
      local_conf().get_val<uint64_t>("osd_max_pg_log_entries");
    return std::clamp(target / num_pgs,
		      min_pg_log_entries,
		      max_pg_log_entries);
  } else {
    // fall back to a per-pg value.
    return min_pg_log_entries;
  }
}

void PG::on_activate(interval_set<snapid_t>)
{
  projected_last_update = peering_state.get_info().last_update;
}

void PG::on_activate_complete()
{
  wait_for_active_blocker.on_active();

  if (peering_state.needs_recovery()) {
    logger().info("{}: requesting recovery",
                  __func__);
    (void) shard_services.start_operation<LocalPeeringEvent>(
      this,
      shard_services,
      pg_whoami,
      pgid,
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::DoRecovery{});
  } else if (peering_state.needs_backfill()) {
    logger().info("{}: requesting backfill",
                  __func__);
    (void) shard_services.start_operation<LocalPeeringEvent>(
      this,
      shard_services,
      pg_whoami,
      pgid,
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::RequestBackfill{});
  } else {
    logger().debug("{}: no need to recover or backfill, AllReplicasRecovered",
		   " for pg: {}", __func__, pgid);
    (void) shard_services.start_operation<LocalPeeringEvent>(
      this,
      shard_services,
      pg_whoami,
      pgid,
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::AllReplicasRecovered{});
  }
  backend->on_activate_complete();
}

void PG::prepare_write(pg_info_t &info,
		       pg_info_t &last_written_info,
		       PastIntervals &past_intervals,
		       PGLog &pglog,
		       bool dirty_info,
		       bool dirty_big_info,
		       bool need_write_epoch,
		       ceph::os::Transaction &t)
{
  std::map<string,bufferlist> km;
  std::string key_to_remove;
  if (dirty_big_info || dirty_info) {
    int ret = prepare_info_keymap(
      shard_services.get_cct(),
      &km,
      &key_to_remove,
      get_osdmap_epoch(),
      info,
      last_written_info,
      past_intervals,
      dirty_big_info,
      need_write_epoch,
      true,
      nullptr,
      this);
    ceph_assert(ret == 0);
  }
  pglog.write_log_and_missing(
    t, &km, coll_ref->get_cid(), pgmeta_oid,
    peering_state.get_pool().info.require_rollback());
  if (!km.empty()) {
    t.omap_setkeys(coll_ref->get_cid(), pgmeta_oid, km);
  }
  if (!key_to_remove.empty()) {
    t.omap_rmkey(coll_ref->get_cid(), pgmeta_oid, key_to_remove);
  }
}

void PG::do_delete_work(ceph::os::Transaction &t)
{
  // TODO
  shard_services.dec_pg_num();
}

void PG::scrub_requested(bool deep, bool repair, bool need_auto)
{
  // TODO: should update the stats upon finishing the scrub
  peering_state.update_stats([deep, this](auto& history, auto& stats) {
    const utime_t now = ceph_clock_now();
    history.last_scrub = peering_state.get_info().last_update;
    history.last_scrub_stamp = now;
    history.last_clean_scrub_stamp = now;
    if (deep) {
      history.last_deep_scrub = history.last_scrub;
      history.last_deep_scrub_stamp = now;
    }
    // yes, please publish the stats
    return true;
  });
}

void PG::log_state_enter(const char *state) {
  logger().info("Entering state: {}", state);
}

void PG::log_state_exit(
  const char *state_name, utime_t enter_time,
  uint64_t events, utime_t event_dur) {
  logger().info(
    "Exiting state: {}, entered at {}, {} spent on {} events",
    state_name,
    enter_time,
    event_dur,
    events);
}

ceph::signedspan PG::get_mnow()
{
  return shard_services.get_mnow();
}

HeartbeatStampsRef PG::get_hb_stamps(int peer)
{
  return shard_services.get_hb_stamps(peer);
}

void PG::schedule_renew_lease(epoch_t last_peering_reset, ceph::timespan delay)
{
  // handle the peering event in the background
  renew_lease_timer.cancel();
  renew_lease_timer.set_callback([last_peering_reset, this] {
    (void) shard_services.start_operation<LocalPeeringEvent>(
      this,
      shard_services,
      pg_whoami,
      pgid,
      last_peering_reset,
      last_peering_reset,
      RenewLease{});
    });
  renew_lease_timer.arm(
    std::chrono::duration_cast<seastar::lowres_clock::duration>(delay));
}


void PG::init(
  int role,
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  const pg_history_t& history,
  const PastIntervals& pi,
  bool backfill,
  ObjectStore::Transaction &t)
{
  peering_state.init(
    role, newup, new_up_primary, newacting,
    new_acting_primary, history, pi, backfill, t);
}

seastar::future<> PG::read_state(crimson::os::FuturizedStore* store)
{
  if (__builtin_expect(stopping, false)) {
    return seastar::make_exception_future<>(
	crimson::common::system_shutdown_exception());
  }

  return seastar::do_with(PGMeta(store, pgid), [] (auto& pg_meta) {
    return pg_meta.load();
  }).then([this, store](auto&& ret) {
    auto [pg_info, past_intervals] = std::move(ret);
    return peering_state.init_from_disk_state(
	std::move(pg_info),
	std::move(past_intervals),
	[this, store] (PGLog &pglog) {
	  return pglog.read_log_and_missing_crimson(
	    *store,
	    coll_ref,
	    peering_state.get_info(),
	    pgmeta_oid);
      });
  }).then([this]() {
    int primary, up_primary;
    vector<int> acting, up;
    peering_state.get_osdmap()->pg_to_up_acting_osds(
	pgid.pgid, &up, &up_primary, &acting, &primary);
    peering_state.init_primary_up_acting(
	up,
	acting,
	up_primary,
	primary);
    int rr = OSDMap::calc_pg_role(pg_whoami, acting);
    peering_state.set_role(rr);

    epoch_t epoch = get_osdmap_epoch();
    (void) shard_services.start_operation<LocalPeeringEvent>(
	this,
	shard_services,
	pg_whoami,
	pgid,
	epoch,
	epoch,
	PeeringState::Initialize());

    return seastar::now();
  });
}

void PG::do_peering_event(
  const boost::statechart::event_base &evt,
  PeeringCtx &rctx)
{
  peering_state.handle_event(
    evt,
    &rctx);
  peering_state.write_if_dirty(rctx.transaction);
}

void PG::do_peering_event(
  PGPeeringEvent& evt, PeeringCtx &rctx)
{
  if (!peering_state.pg_has_reset_since(evt.get_epoch_requested())) {
    logger().debug("{} handling {} for pg: {}", __func__, evt.get_desc(), pgid);
    do_peering_event(evt.get_event(), rctx);
  } else {
    logger().debug("{} ignoring {} -- pg has reset", __func__, evt.get_desc());
  }
}

void PG::handle_advance_map(
  cached_map_t next_map, PeeringCtx &rctx)
{
  vector<int> newup, newacting;
  int up_primary, acting_primary;
  next_map->pg_to_up_acting_osds(
    pgid.pgid,
    &newup, &up_primary,
    &newacting, &acting_primary);
  peering_state.advance_map(
    next_map,
    peering_state.get_osdmap(),
    newup,
    up_primary,
    newacting,
    acting_primary,
    rctx);
  osdmap_gate.got_map(next_map->get_epoch());
}

void PG::handle_activate_map(PeeringCtx &rctx)
{
  peering_state.activate_map(rctx);
}

void PG::handle_initialize(PeeringCtx &rctx)
{
  PeeringState::Initialize evt;
  peering_state.handle_event(evt, &rctx);
}


void PG::print(ostream& out) const
{
  out << peering_state << " ";
}


std::ostream& operator<<(std::ostream& os, const PG& pg)
{
  os << " pg_epoch " << pg.get_osdmap_epoch() << " ";
  pg.print(os);
  return os;
}

void PG::WaitForActiveBlocker::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg->pgid;
}

void PG::WaitForActiveBlocker::on_active()
{
  p.set_value();
  p = {};
}

blocking_future<> PG::WaitForActiveBlocker::wait()
{
  if (pg->peering_state.is_active()) {
    return make_blocking_future(seastar::now());
  } else {
    return make_blocking_future(p.get_shared_future());
  }
}

seastar::future<> PG::WaitForActiveBlocker::stop()
{
  p.set_exception(crimson::common::system_shutdown_exception());
  return seastar::now();
}

seastar::future<> PG::submit_transaction(const OpInfo& op_info,
					 const std::vector<OSDOp>& ops,
					 ObjectContextRef&& obc,
					 ceph::os::Transaction&& txn,
					 const osd_op_params_t& osd_op_p)
{
  if (__builtin_expect(stopping, false)) {
    return seastar::make_exception_future<>(
	crimson::common::system_shutdown_exception());
  }

  epoch_t map_epoch = get_osdmap_epoch();

  std::vector<pg_log_entry_t> log_entries;
  log_entries.emplace_back(obc->obs.exists ?
		      pg_log_entry_t::MODIFY : pg_log_entry_t::DELETE,
		    obc->obs.oi.soid, osd_op_p.at_version, obc->obs.oi.version,
		    osd_op_p.user_modify ? osd_op_p.at_version.version : 0,
		    osd_op_p.req->get_reqid(), osd_op_p.req->get_mtime(),
                    op_info.allows_returnvec() && !ops.empty() ? ops.back().rval.code : 0);
  // TODO: refactor the submit_transaction
  if (op_info.allows_returnvec()) {
    // also the per-op values are recorded in the pg log
    log_entries.back().set_op_returns(ops);
    logger().debug("{} op_returns: {}",
                   __func__, log_entries.back().op_returns);
  }
  log_entries.back().clean_regions = std::move(osd_op_p.clean_regions);
  peering_state.append_log_with_trim_to_updated(std::move(log_entries), osd_op_p.at_version,
						txn, true, false);

  return backend->mutate_object(peering_state.get_acting_recovery_backfill(),
				std::move(obc),
				std::move(txn),
				std::move(osd_op_p),
				peering_state.get_last_peering_reset(),
				map_epoch,
				std::move(log_entries)).then(
    [this, last_complete=peering_state.get_info().last_complete,
      at_version=osd_op_p.at_version](auto acked) {
    for (const auto& peer : acked) {
      peering_state.update_peer_last_complete_ondisk(
        peer.shard, peer.last_complete_ondisk);
    }
    peering_state.complete_write(at_version, last_complete);
    return seastar::now();
  });
}

seastar::future<Ref<MOSDOpReply>> PG::do_osd_ops(
  Ref<MOSDOp> m,
  ObjectContextRef obc,
  const OpInfo &op_info)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  using osd_op_errorator = OpsExecuter::osd_op_errorator;
  const auto oid = m->get_snapid() == CEPH_SNAPDIR ? m->get_hobj().get_head()
                                                   : m->get_hobj();
  auto ox =
    std::make_unique<OpsExecuter>(obc, &op_info, *this/* as const& */, m);

  return crimson::do_for_each(
    m->ops, [obc, m, ox = ox.get()](OSDOp& osd_op) {
    logger().debug(
      "do_osd_ops: {} - object {} - handling op {}",
      *m,
      obc->obs.oi.soid,
      ceph_osd_op_name(osd_op.op.op));
    return ox->execute_osd_op(osd_op);
  }).safe_then([this, obc, m, ox = ox.get(), &op_info] {
    logger().debug(
      "do_osd_ops: {} - object {} all operations successful",
      *m,
      obc->obs.oi.soid);
    return std::move(*ox).submit_changes([this, m, &op_info]
      (auto&& txn, auto&& obc, auto&& osd_op_p) -> osd_op_errorator::future<> {
	// XXX: the entire lambda could be scheduled conditionally. ::if_then()?
	if (txn.empty()) {
	  logger().debug(
	    "do_osd_ops: {} - object {} txn is empty, bypassing mutate",
	    *m,
	    obc->obs.oi.soid);
          return osd_op_errorator::now();
        } else {
	  logger().debug(
	    "do_osd_ops: {} - object {} submitting txn",
	    *m,
	    obc->obs.oi.soid);
	   return submit_transaction(op_info,
                                     m->ops,
                                     std::move(obc),
                                     std::move(txn),
                                     std::move(osd_op_p));
	 }
      });
  }).safe_then([this,
                m,
                obc,
                ox_deleter = std::move(ox),
                rvec = op_info.allows_returnvec()] {
    auto result = m->ops.empty() || !rvec ? 0 : m->ops.back().rval.code;
    auto reply = make_message<MOSDOpReply>(m.get(),
                                           result,
                                           get_osdmap_epoch(),
                                           0,
                                           false);
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    logger().debug(
      "do_osd_ops: {} - object {} sending reply",
      *m,
      obc->obs.oi.soid);
    return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
  }, OpsExecuter::osd_op_errorator::all_same_way([=] (const std::error_code& e) {
    assert(e.value() > 0);
    logger().debug(
      "do_osd_ops: {} - object {} got error code {}, {}",
      *m,
      obc->obs.oi.soid,
      e.value(),
      e.message());
    auto reply = make_message<MOSDOpReply>(
      m.get(), -e.value(), get_osdmap_epoch(), 0, false);
    reply->set_enoent_reply_versions(peering_state.get_info().last_update,
				     peering_state.get_info().last_user_version);
    return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
  })).handle_exception_type([=](const crimson::osd::error& e) {
    // we need this handler because throwing path which aren't errorated yet.
    logger().debug(
      "do_osd_ops: {} - object {} got unhandled exception {} ({})",
      *m,
      obc->obs.oi.soid,
      e.code(),
      e.what());
    auto reply = make_message<MOSDOpReply>(
      m.get(), -e.code().value(), get_osdmap_epoch(), 0, false);
    reply->set_enoent_reply_versions(peering_state.get_info().last_update,
				     peering_state.get_info().last_user_version);
    return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
  });
}

seastar::future<Ref<MOSDOpReply>> PG::do_pg_ops(Ref<MOSDOp> m)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  auto ox = std::make_unique<OpsExecuter>(*this/* as const& */, m);
  return seastar::do_for_each(m->ops, [ox = ox.get()](OSDOp& osd_op) {
    logger().debug("will be handling pg op {}", ceph_osd_op_name(osd_op.op.op));
    return ox->execute_pg_op(osd_op);
  }).then([m, this, ox = std::move(ox)] {
    auto reply = make_message<MOSDOpReply>(m.get(), 0, get_osdmap_epoch(),
                                           CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
                                           false);
    return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
  }).handle_exception_type([=](const crimson::osd::error& e) {
    auto reply = make_message<MOSDOpReply>(
      m.get(), -e.code().value(), get_osdmap_epoch(), 0, false);
    reply->set_enoent_reply_versions(peering_state.get_info().last_update,
				     peering_state.get_info().last_user_version);
    return seastar::make_ready_future<Ref<MOSDOpReply>>(std::move(reply));
  });
}

std::pair<hobject_t, RWState::State> PG::get_oid_and_lock(
  const MOSDOp &m,
  const OpInfo &op_info)
{
  auto oid = m.get_snapid() == CEPH_SNAPDIR ?
    m.get_hobj().get_head() : m.get_hobj();

  RWState::State lock_type = RWState::RWNONE;
  if (op_info.rwordered() && op_info.may_read()) {
    lock_type = RWState::RWState::RWEXCL;
  } else if (op_info.rwordered()) {
    lock_type = RWState::RWState::RWWRITE;
  } else {
    ceph_assert(op_info.may_read());
    lock_type = RWState::RWState::RWREAD;
  }
  return std::make_pair(oid, lock_type);
}

std::optional<hobject_t> PG::resolve_oid(
  const SnapSet &ss,
  const hobject_t &oid)
{
  if (oid.snap > ss.seq) {
    return oid.get_head();
  } else {
    // which clone would it be?
    auto clone = std::upper_bound(
      begin(ss.clones), end(ss.clones),
      oid.snap);
    if (clone == end(ss.clones)) {
      // Doesn't exist, > last clone, < ss.seq
      return std::nullopt;
    }
    auto citer = ss.clone_snaps.find(*clone);
    // TODO: how do we want to handle this kind of logic error?
    ceph_assert(citer != ss.clone_snaps.end());

    if (std::find(
	  citer->second.begin(),
	  citer->second.end(),
	  *clone) == citer->second.end()) {
      return std::nullopt;
    } else {
      auto soid = oid;
      soid.snap = *clone;
      return std::optional<hobject_t>(soid);
    }
  }
}

PG::load_obc_ertr::future<
  std::pair<crimson::osd::ObjectContextRef, bool>>
PG::get_or_load_clone_obc(hobject_t oid, ObjectContextRef head)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  ceph_assert(!oid.is_head());
  using ObjectContextRef = crimson::osd::ObjectContextRef;
  auto coid = resolve_oid(head->get_ro_ss(), oid);
  if (!coid) {
    return load_obc_ertr::make_ready_future<
      std::pair<crimson::osd::ObjectContextRef, bool>>(
	std::make_pair(ObjectContextRef(), true)
      );
  }
  auto [obc, existed] = shard_services.obc_registry.get_cached_obc(*coid);
  if (existed) {
    return load_obc_ertr::make_ready_future<
      std::pair<crimson::osd::ObjectContextRef, bool>>(
	std::make_pair(obc, true)
      );
  } else {
    bool got = obc->maybe_get_excl();
    ceph_assert(got);
    return backend->load_metadata(*coid).safe_then(
      [oid, obc=std::move(obc), head](auto &&md) mutable {
	obc->set_clone_state(std::move(md->os), std::move(head));
	return load_obc_ertr::make_ready_future<
	  std::pair<crimson::osd::ObjectContextRef, bool>>(
	    std::make_pair(obc, false)
	  );
      });
  }
}

PG::load_obc_ertr::future<
  std::pair<crimson::osd::ObjectContextRef, bool>>
PG::get_or_load_head_obc(hobject_t oid)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  ceph_assert(oid.is_head());
  auto [obc, existed] = shard_services.obc_registry.get_cached_obc(oid);
  if (existed) {
    logger().debug(
      "{}: found {} in cache",
      __func__,
      oid);
    return load_obc_ertr::make_ready_future<
      std::pair<crimson::osd::ObjectContextRef, bool>>(
	std::make_pair(std::move(obc), true)
      );
  } else {
    logger().debug(
      "{}: cache miss on {}",
      __func__,
      oid);
    bool got = obc->maybe_get_excl();
    ceph_assert(got);
    return backend->load_metadata(oid).safe_then(
      [oid, obc=std::move(obc)](auto md) ->
        load_obc_ertr::future<
          std::pair<crimson::osd::ObjectContextRef, bool>>
      {
	logger().debug(
	  "{}: loaded obs {} for {}",
	  __func__,
	  md->os.oi,
	  oid);
	if (!md->ss) {
	  logger().error(
	    "{}: oid {} missing snapset",
	    __func__,
	    oid);
	  return crimson::ct_error::object_corrupted::make();
	}
	obc->set_head_state(std::move(md->os), std::move(*(md->ss)));
	  logger().debug(
	    "{}: returning obc {} for {}",
	    __func__,
	    obc->obs.oi,
	    obc->obs.oi.soid);
	  return load_obc_ertr::make_ready_future<
	    std::pair<crimson::osd::ObjectContextRef, bool>>(
	      std::make_pair(obc, false)
	    );
      });
  }
}

PG::load_obc_ertr::future<crimson::osd::ObjectContextRef>
PG::get_locked_obc(
  Operation *op, const hobject_t &oid, RWState::State type)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  return get_or_load_head_obc(oid.get_head()).safe_then(
    [this, op, oid, type](auto p) -> load_obc_ertr::future<ObjectContextRef>{
      auto &[head_obc, head_existed] = p;
      if (oid.is_head()) {
	if (head_existed) {
	  return head_obc->get_lock_type(op, type).then([head_obc=head_obc] {
	    ceph_assert(head_obc->loaded);
	    return load_obc_ertr::make_ready_future<ObjectContextRef>(head_obc);
	  });
	} else {
	  head_obc->degrade_excl_to(type);
	  return load_obc_ertr::make_ready_future<ObjectContextRef>(head_obc);
	}
      } else {
	return head_obc->get_lock_type(op, RWState::RWREAD).then(
	  [this, head_obc=head_obc, oid] {
	    ceph_assert(head_obc->loaded);
	    return get_or_load_clone_obc(oid, head_obc);
	  }).safe_then([head_obc=head_obc, op, oid, type](auto p) {
	      auto &[obc, existed] = p;
	      if (existed) {
		return load_obc_ertr::future<>(
		  obc->get_lock_type(op, type)).safe_then([obc=obc] {
		  ceph_assert(obc->loaded);
		  return load_obc_ertr::make_ready_future<ObjectContextRef>(obc);
		});
	      } else {
		obc->degrade_excl_to(type);
		return load_obc_ertr::make_ready_future<ObjectContextRef>(obc);
	      }
	  }).safe_then([head_obc=head_obc](auto obc) {
	    head_obc->put_lock_type(RWState::RWREAD);
	    return load_obc_ertr::make_ready_future<ObjectContextRef>(obc);
	  });
      }
    });
}

seastar::future<> PG::handle_rep_op(Ref<MOSDRepOp> req)
{
  if (__builtin_expect(stopping, false)) {
    return seastar::make_exception_future<>(
	crimson::common::system_shutdown_exception());
  }

  ceph::os::Transaction txn;
  auto encoded_txn = req->get_data().cbegin();
  decode(txn, encoded_txn);
  auto p = req->logbl.cbegin();
  std::vector<pg_log_entry_t> log_entries;
  decode(log_entries, p);
  peering_state.append_log(std::move(log_entries), req->pg_trim_to,
      req->version, req->min_last_complete_ondisk, txn, !txn.empty(), false);
  return shard_services.get_store().do_transaction(coll_ref, std::move(txn))
    .then([req, lcod=peering_state.get_info().last_complete, this] {
      peering_state.update_last_complete_ondisk(lcod);
      const auto map_epoch = get_osdmap_epoch();
      auto reply = make_message<MOSDRepOpReply>(
        req.get(), pg_whoami, 0,
	map_epoch, req->get_min_epoch(), CEPH_OSD_FLAG_ONDISK);
      reply->set_last_complete_ondisk(lcod);
      return shard_services.send_to_osd(req->from.osd, reply, map_epoch);
    });
}

void PG::handle_rep_op_reply(crimson::net::Connection* conn,
			     const MOSDRepOpReply& m)
{
  backend->got_rep_op_reply(m);
}

seastar::future<> PG::stop()
{
  logger().info("PG {} {}", pgid, __func__);
  stopping = true;
  return osdmap_gate.stop().then([this] {
    return wait_for_active_blocker.stop();
  }).then([this] {
    return recovery_handler->stop();
  }).then([this] {
    return recovery_backend->stop();
  }).then([this] {
    return backend->stop();
  });
}

void PG::on_change(ceph::os::Transaction &t) {
  recovery_backend->on_peering_interval_change(t);
  backend->on_actingset_changed({ is_primary() });
}

}
