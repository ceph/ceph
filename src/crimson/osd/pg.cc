// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

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

using std::ostream;
using std::set;
using std::string;
using std::vector;

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
  logger().debug("PG::try_flush_or_schedule_async: flush ...");
  (void)shard_services.get_store().flush(
    coll_ref
  ).then(
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

void PG::publish_stats_to_osd()
{
  if (!is_primary())
    return;
  if (auto new_pg_stats = peering_state.prepare_stats_for_publish(
        pg_stats,
        object_stat_collection_t());
      new_pg_stats.has_value()) {
    pg_stats = std::move(new_pg_stats);
  }
}

void PG::clear_publish_stats()
{
  pg_stats.reset();
}

pg_stat_t PG::get_stats() const
{
  return pg_stats.value_or(pg_stat_t{});
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
      float(0.001),
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
      float(0.001),
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
      float(0.001),
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::AllReplicasRecovered{});
  }
  publish_stats_to_osd();
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

std::pair<ghobject_t, bool>
PG::do_delete_work(ceph::os::Transaction &t, ghobject_t _next)
{
  // TODO
  shard_services.dec_pg_num();
  return {_next, false};
}

void PG::scrub_requested(scrub_level_t scrub_level, scrub_type_t scrub_type)
{
  // TODO: should update the stats upon finishing the scrub
  peering_state.update_stats([scrub_level, this](auto& history, auto& stats) {
    const utime_t now = ceph_clock_now();
    history.last_scrub = peering_state.get_info().last_update;
    history.last_scrub_stamp = now;
    history.last_clean_scrub_stamp = now;
    if (scrub_level == scrub_level_t::deep) {
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
  ObjectStore::Transaction &t)
{
  peering_state.init(
    role, newup, new_up_primary, newacting,
    new_acting_primary, history, pi, t);
}

seastar::future<> PG::read_state(crimson::os::FuturizedStore* store)
{
  if (__builtin_expect(stopping, false)) {
    return seastar::make_exception_future<>(
	crimson::common::system_shutdown_exception());
  }

  return seastar::do_with(PGMeta(*store, pgid), [] (auto& pg_meta) {
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
  PGPeeringEvent& evt, PeeringCtx &rctx)
{
  if (peering_state.pg_has_reset_since(evt.get_epoch_requested()) ||
      peering_state.pg_has_reset_since(evt.get_epoch_sent())) {
    logger().debug("{} ignoring {} -- pg has reset", __func__, evt.get_desc());
  } else {
    logger().debug("{} handling {} for pg: {}", __func__, evt.get_desc(), pgid);
    peering_state.handle_event(
      evt.get_event(),
      &rctx);
    peering_state.write_if_dirty(rctx.transaction);
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
  peering_state.handle_event(PeeringState::Initialize{}, &rctx);
}


void PG::print(ostream& out) const
{
  out << peering_state << " ";
}

void PG::dump_primary(Formatter* f)
{
  peering_state.dump_peering_state(f);

  f->open_array_section("recovery_state");
  PeeringState::QueryState q(f);
  peering_state.handle_event(q, 0);
  f->close_section();

  // TODO: snap_trimq
  // TODO: scrubber state
  // TODO: agent state
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

std::tuple<PG::interruptible_future<>,
           PG::interruptible_future<>>
PG::submit_transaction(
  const OpInfo& op_info,
  const std::vector<OSDOp>& ops,
  ObjectContextRef&& obc,
  ceph::os::Transaction&& txn,
  osd_op_params_t&& osd_op_p)
{
  if (__builtin_expect(stopping, false)) {
    return {seastar::make_exception_future<>(
              crimson::common::system_shutdown_exception()),
            seastar::now()};
  }

  epoch_t map_epoch = get_osdmap_epoch();

  if (__builtin_expect(osd_op_p.at_version.epoch != map_epoch, false)) {
    throw crimson::common::actingset_changed(is_primary());
  }

  std::vector<pg_log_entry_t> log_entries;
  log_entries.emplace_back(obc->obs.exists ?
		      pg_log_entry_t::MODIFY : pg_log_entry_t::DELETE,
		    obc->obs.oi.soid, osd_op_p.at_version, obc->obs.oi.version,
		    osd_op_p.user_modify ? osd_op_p.at_version.version : 0,
		    osd_op_p.req_id, osd_op_p.mtime,
                    op_info.allows_returnvec() && !ops.empty() ? ops.back().rval.code : 0);
  // TODO: refactor the submit_transaction
  if (op_info.allows_returnvec()) {
    // also the per-op values are recorded in the pg log
    log_entries.back().set_op_returns(ops);
    logger().debug("{} op_returns: {}",
                   __func__, log_entries.back().op_returns);
  }
  log_entries.back().clean_regions = std::move(osd_op_p.clean_regions);
  peering_state.pre_submit_op(obc->obs.oi.soid, log_entries, osd_op_p.at_version);
  peering_state.append_log_with_trim_to_updated(std::move(log_entries), osd_op_p.at_version,
						txn, true, false);

  auto [submitted, all_completed] = backend->mutate_object(
      peering_state.get_acting_recovery_backfill(),
      std::move(obc),
      std::move(txn),
      std::move(osd_op_p),
      peering_state.get_last_peering_reset(),
      map_epoch,
      std::move(log_entries));
  return std::make_tuple(std::move(submitted), all_completed.then_interruptible(
    [this, last_complete=peering_state.get_info().last_complete,
      at_version=osd_op_p.at_version](auto acked) {
    for (const auto& peer : acked) {
      peering_state.update_peer_last_complete_ondisk(
        peer.shard, peer.last_complete_ondisk);
    }
    peering_state.complete_write(at_version, last_complete);
    return seastar::now();
  }));
}

void PG::fill_op_params_bump_pg_version(
  osd_op_params_t& osd_op_p,
  const bool user_modify)
{
  osd_op_p.at_version = next_version();
  osd_op_p.pg_trim_to = get_pg_trim_to();
  osd_op_p.min_last_complete_ondisk = get_min_last_complete_ondisk();
  osd_op_p.last_complete = get_info().last_complete;
  if (user_modify) {
    osd_op_p.user_at_version = osd_op_p.at_version.version;
  }
}

PG::interruptible_future<> PG::repair_object(
  const hobject_t& oid,
  eversion_t& v) 
{
  // see also PrimaryLogPG::rep_repair_primary_object()
  assert(is_primary());
  logger().debug("{}: {} peers osd.{}", __func__, oid, get_acting_recovery_backfill());
  // Add object to PG's missing set if it isn't there already
  assert(!get_local_missing().is_missing(oid));
  peering_state.force_object_missing(pg_whoami, oid, v);
  auto [op, fut] = get_shard_services().start_operation<UrgentRecovery>(
    oid, v, this, get_shard_services(), get_osdmap_epoch());
  return std::move(fut);
}

template <class Ret, class SuccessFunc, class FailureFunc>
PG::do_osd_ops_iertr::future<PG::pg_rep_op_fut_t<Ret>>
PG::do_osd_ops_execute(
  seastar::lw_shared_ptr<OpsExecuter> ox,
  std::vector<OSDOp>& ops,
  const OpInfo &op_info,
  SuccessFunc&& success_func,
  FailureFunc&& failure_func)
{
  assert(ox);
  auto rollbacker = ox->create_rollbacker([this] (auto& obc) {
    return reload_obc(obc).handle_error_interruptible(
      load_obc_ertr::assert_all{"can't live with object state messed up"});
  });
  auto failure_func_ptr = seastar::make_lw_shared(std::move(failure_func));
  return interruptor::do_for_each(ops, [ox](OSDOp& osd_op) {
    logger().debug(
      "do_osd_ops_execute: object {} - handling op {}",
      ox->get_target(),
      ceph_osd_op_name(osd_op.op.op));
    return ox->execute_op(osd_op);
  }).safe_then_interruptible([this, ox, &op_info, &ops] {
    logger().debug(
      "do_osd_ops_execute: object {} all operations successful",
      ox->get_target());
    peering_state.apply_op_stats(ox->get_target(), ox->get_stats());
    return std::move(*ox).flush_changes_n_do_ops_effects(
      [this, &op_info, &ops] (auto&& txn,
                              auto&& obc,
                              auto&& osd_op_p,
                              bool user_modify) {
	logger().debug(
	  "do_osd_ops_execute: object {} submitting txn",
	  obc->get_oid());
        fill_op_params_bump_pg_version(osd_op_p, user_modify);
	return submit_transaction(
          op_info,
          ops,
          std::move(obc),
          std::move(txn),
          std::move(osd_op_p));
    });
  }).safe_then_unpack_interruptible(
    [success_func=std::move(success_func), rollbacker, this, failure_func_ptr]
    (auto submitted_fut, auto all_completed_fut) mutable {
    return PG::do_osd_ops_iertr::make_ready_future<pg_rep_op_fut_t<Ret>>(
        std::move(submitted_fut),
        all_completed_fut.safe_then_interruptible_tuple(
          std::move(success_func),
          crimson::ct_error::object_corrupted::handle(
            [rollbacker, this] (const std::error_code& e) mutable {
            // this is a path for EIO. it's special because we want to fix the obejct
            // and try again. that is, the layer above `PG::do_osd_ops` is supposed to
            // restart the execution.
            return rollbacker.rollback_obc_if_modified(e).then_interruptible(
              [obc=rollbacker.get_obc(), this] {
              return repair_object(obc->obs.oi.soid,
                                   obc->obs.oi.version).then_interruptible([] {
                return do_osd_ops_iertr::future<Ret>{crimson::ct_error::eagain::make()};
              });
            });
          }), OpsExecuter::osd_op_errorator::all_same_way(
            [rollbacker, failure_func_ptr]
            (const std::error_code& e) mutable {
            return rollbacker.rollback_obc_if_modified(e).then_interruptible(
              [e, failure_func_ptr] {
              return (*failure_func_ptr)(e);
            });
          })
        )
      );
  }, OpsExecuter::osd_op_errorator::all_same_way(
    [rollbacker, failure_func_ptr]
    (const std::error_code& e) mutable {
    return PG::do_osd_ops_iertr::make_ready_future<pg_rep_op_fut_t<Ret>>(
        seastar::now(),
        rollbacker.rollback_obc_if_modified(e).then_interruptible(
          [e, failure_func_ptr] {
          return (*failure_func_ptr)(e);
        }));
  }));
}

PG::do_osd_ops_iertr::future<PG::pg_rep_op_fut_t<MURef<MOSDOpReply>>>
PG::do_osd_ops(
  Ref<MOSDOp> m,
  ObjectContextRef obc,
  const OpInfo &op_info)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  return do_osd_ops_execute<MURef<MOSDOpReply>>(
    seastar::make_lw_shared<OpsExecuter>(
      Ref<PG>{this}, obc, op_info, *m),
    m->ops,
    op_info,
    [this, m, obc, may_write = op_info.may_write(),
     may_read = op_info.may_read(), rvec = op_info.allows_returnvec()] {
      // TODO: should stop at the first op which returns a negative retval,
      //       cmpext uses it for returning the index of first unmatched byte
      int result = m->ops.empty() ? 0 : m->ops.back().rval.code;
      if (may_read && result >= 0) {
        for (auto &osdop : m->ops) {
          if (osdop.rval < 0 && !(osdop.op.flags & CEPH_OSD_OP_FLAG_FAILOK)) {
            result = osdop.rval.code;
            break;
          }
        }
      } else if (result > 0 && may_write && !rvec) {
          result = 0;
      }
      auto reply = crimson::make_message<MOSDOpReply>(m.get(),
                                             result,
                                             get_osdmap_epoch(),
                                             0,
                                             false);
      reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
      logger().debug(
        "do_osd_ops: {} - object {} sending reply",
        *m,
        m->get_hobj());
      if (obc->obs.exists) {
        reply->set_reply_versions(peering_state.get_info().last_update,
          obc->obs.oi.user_version);
      } else {
        reply->set_reply_versions(peering_state.get_info().last_update,
          peering_state.get_info().last_user_version);
      }
      return do_osd_ops_iertr::make_ready_future<MURef<MOSDOpReply>>(
        std::move(reply));
    },
    [m, this] (const std::error_code& e) {
      auto reply = crimson::make_message<MOSDOpReply>(
        m.get(), -e.value(), get_osdmap_epoch(), 0, false);
      reply->set_enoent_reply_versions(
        peering_state.get_info().last_update,
        peering_state.get_info().last_user_version);
      return do_osd_ops_iertr::make_ready_future<MURef<MOSDOpReply>>(std::move(reply));
    });
}

PG::do_osd_ops_iertr::future<PG::pg_rep_op_fut_t<>>
PG::do_osd_ops(
  ObjectContextRef obc,
  std::vector<OSDOp>& ops,
  const OpInfo &op_info,
  const do_osd_ops_params_t& msg_params,
  do_osd_ops_success_func_t success_func,
  do_osd_ops_failure_func_t failure_func)
{
  return do_osd_ops_execute<void>(
    seastar::make_lw_shared<OpsExecuter>(
      Ref<PG>{this}, std::move(obc), op_info, msg_params),
    ops,
    std::as_const(op_info),
    std::move(success_func),
    std::move(failure_func));
}

PG::interruptible_future<MURef<MOSDOpReply>> PG::do_pg_ops(Ref<MOSDOp> m)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  auto ox = std::make_unique<PgOpsExecuter>(std::as_const(*this),
                                            std::as_const(*m));
  return interruptor::do_for_each(m->ops, [ox = ox.get()](OSDOp& osd_op) {
    logger().debug("will be handling pg op {}", ceph_osd_op_name(osd_op.op.op));
    return ox->execute_op(osd_op);
  }).then_interruptible([m, this, ox = std::move(ox)] {
    auto reply = crimson::make_message<MOSDOpReply>(m.get(), 0, get_osdmap_epoch(),
                                           CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
                                           false);
    reply->claim_op_out_data(m->ops);
    reply->set_reply_versions(peering_state.get_info().last_update,
      peering_state.get_info().last_user_version);
    return seastar::make_ready_future<MURef<MOSDOpReply>>(std::move(reply));
  }).handle_exception_type_interruptible([=](const crimson::osd::error& e) {
    auto reply = crimson::make_message<MOSDOpReply>(
      m.get(), -e.code().value(), get_osdmap_epoch(), 0, false);
    reply->set_enoent_reply_versions(peering_state.get_info().last_update,
				     peering_state.get_info().last_user_version);
    return seastar::make_ready_future<MURef<MOSDOpReply>>(std::move(reply));
  });
}

hobject_t PG::get_oid(const hobject_t& hobj)
{
  return hobj.snap == CEPH_SNAPDIR ? hobj.get_head() : hobj;
}

RWState::State PG::get_lock_type(const OpInfo &op_info)
{

  if (op_info.rwordered() && op_info.may_read()) {
    return RWState::RWEXCL;
  } else if (op_info.rwordered()) {
    return RWState::RWWRITE;
  } else {
    ceph_assert(op_info.may_read());
    return RWState::RWREAD;
  }
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

template<RWState::State State>
PG::load_obc_iertr::future<>
PG::with_head_obc(ObjectContextRef obc, bool existed, with_obc_func_t&& func)
{
  logger().debug("{} {}", __func__, obc->get_oid());
  assert(obc->is_head());
  obc->append_to(obc_set_accessing);
  return obc->with_lock<State, IOInterruptCondition>(
    [existed=existed, obc=obc, func=std::move(func), this] {
    auto loaded = load_obc_iertr::make_ready_future<ObjectContextRef>(obc);
    if (existed) {
      logger().debug("with_head_obc: found {} in cache", obc->get_oid());
    } else {
      logger().debug("with_head_obc: cache miss on {}", obc->get_oid());
      loaded = obc->with_promoted_lock<State, IOInterruptCondition>([this, obc] {
        return load_head_obc(obc);
      });
    }
    return loaded.safe_then_interruptible([func = std::move(func)](auto obc) {
      return std::move(func)(std::move(obc));
    });
  }).finally([this, pgref=boost::intrusive_ptr<PG>{this}, obc=std::move(obc)] {
    logger().debug("with_head_obc: released {}", obc->get_oid());
    obc->remove_from(obc_set_accessing);
  });
}

template<RWState::State State>
PG::load_obc_iertr::future<>
PG::with_head_obc(hobject_t oid, with_obc_func_t&& func)
{
  auto [obc, existed] =
    shard_services.obc_registry.get_cached_obc(std::move(oid));
  return with_head_obc<State>(std::move(obc), existed, std::move(func));
}

template<RWState::State State>
PG::interruptible_future<>
PG::with_existing_head_obc(ObjectContextRef obc, with_obc_func_t&& func)
{
  constexpr bool existed = true;
  return with_head_obc<State>(
    std::move(obc), existed, std::move(func)
  ).handle_error_interruptible(load_obc_ertr::assert_all{"can't happen"});
}

template<RWState::State State>
PG::load_obc_iertr::future<>
PG::with_clone_obc(hobject_t oid, with_obc_func_t&& func)
{
  assert(!oid.is_head());
  return with_head_obc<RWState::RWREAD>(oid.get_head(),
    [oid, func=std::move(func), this](auto head) -> load_obc_iertr::future<> {
    auto coid = resolve_oid(head->get_ro_ss(), oid);
    if (!coid) {
      // TODO: return crimson::ct_error::enoent::make();
      logger().error("with_clone_obc: {} clone not found", coid);
      return load_obc_ertr::make_ready_future<>();
    }
    auto [clone, existed] = shard_services.obc_registry.get_cached_obc(*coid);
    return clone->template with_lock<State>(
      [coid=*coid, existed=existed,
       head=std::move(head), clone=std::move(clone),
       func=std::move(func), this]() -> load_obc_iertr::future<> {
      auto loaded = load_obc_iertr::make_ready_future<ObjectContextRef>(clone);
      if (existed) {
        logger().debug("with_clone_obc: found {} in cache", coid);
      } else {
        logger().debug("with_clone_obc: cache miss on {}", coid);
        loaded = clone->template with_promoted_lock<State>(
          [coid, clone, head, this] {
          return backend->load_metadata(coid).safe_then_interruptible(
            [coid, clone=std::move(clone), head=std::move(head)](auto md) mutable {
            clone->set_clone_state(std::move(md->os), std::move(head));
            return clone;
          });
        });
      }
      return loaded.safe_then_interruptible([func = std::move(func)](auto clone) {
        return std::move(func)(std::move(clone));
      });
    });
  });
}

// explicitly instantiate the used instantiations
template PG::load_obc_iertr::future<>
PG::with_head_obc<RWState::RWNONE>(hobject_t, with_obc_func_t&&);

template<RWState::State State>
PG::interruptible_future<>
PG::with_existing_clone_obc(ObjectContextRef clone, with_obc_func_t&& func)
{
  assert(clone);
  assert(clone->get_head_obc());
  assert(!clone->get_oid().is_head());
  return with_existing_head_obc<RWState::RWREAD>(clone->get_head_obc(),
    [clone=std::move(clone), func=std::move(func)] ([[maybe_unused]] auto head) {
    assert(head == clone->get_head_obc());
    return clone->template with_lock<State>(
      [clone=std::move(clone), func=std::move(func)] {
      return std::move(func)(std::move(clone));
    });
  });
}

PG::load_obc_iertr::future<crimson::osd::ObjectContextRef>
PG::load_head_obc(ObjectContextRef obc)
{
  return backend->load_metadata(obc->get_oid()).safe_then_interruptible(
    [obc=std::move(obc)](auto md)
    -> load_obc_ertr::future<crimson::osd::ObjectContextRef> {
    const hobject_t& oid = md->os.oi.soid;
    logger().debug(
      "load_head_obc: loaded obs {} for {}", md->os.oi, oid);
    if (!md->ss) {
      logger().error(
        "load_head_obc: oid {} missing snapset", oid);
      return crimson::ct_error::object_corrupted::make();
    }
    obc->set_head_state(std::move(md->os), std::move(*(md->ss)));
    logger().debug(
      "load_head_obc: returning obc {} for {}",
      obc->obs.oi, obc->obs.oi.soid);
    return load_obc_ertr::make_ready_future<
      crimson::osd::ObjectContextRef>(obc);
  });
}

PG::load_obc_iertr::future<>
PG::reload_obc(crimson::osd::ObjectContext& obc) const
{
  assert(obc.is_head());
  return backend->load_metadata(obc.get_oid()).safe_then_interruptible<false>([&obc](auto md)
    -> load_obc_ertr::future<> {
    logger().debug(
      "{}: reloaded obs {} for {}",
      __func__,
      md->os.oi,
      obc.get_oid());
    if (!md->ss) {
      logger().error(
        "{}: oid {} missing snapset",
        __func__,
        obc.get_oid());
      return crimson::ct_error::object_corrupted::make();
    }
    obc.set_head_state(std::move(md->os), std::move(*(md->ss)));
    return load_obc_ertr::now();
  });
}

PG::load_obc_iertr::future<>
PG::with_locked_obc(const hobject_t &hobj,
                    const OpInfo &op_info,
                    with_obc_func_t &&f)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  const hobject_t oid = get_oid(hobj);
  switch (get_lock_type(op_info)) {
  case RWState::RWREAD:
    if (oid.is_head()) {
      return with_head_obc<RWState::RWREAD>(oid, std::move(f));
    } else {
      return with_clone_obc<RWState::RWREAD>(oid, std::move(f));
    }
  case RWState::RWWRITE:
    if (oid.is_head()) {
      return with_head_obc<RWState::RWWRITE>(oid, std::move(f));
    } else {
      return with_clone_obc<RWState::RWWRITE>(oid, std::move(f));
    }
  case RWState::RWEXCL:
    if (oid.is_head()) {
      return with_head_obc<RWState::RWEXCL>(oid, std::move(f));
    } else {
      return with_clone_obc<RWState::RWEXCL>(oid, std::move(f));
    }
  default:
    ceph_abort();
  };
}

template <RWState::State State>
PG::interruptible_future<>
PG::with_locked_obc(ObjectContextRef obc, with_obc_func_t &&f)
{
  // TODO: a question from rebase: do we really need such checks when
  // the interruptible stuff is being used?
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  if (obc->is_head()) {
    return with_existing_head_obc<State>(obc, std::move(f));
  } else {
    return with_existing_clone_obc<State>(obc, std::move(f));
  }
}

// explicitly instantiate the used instantiations
template PG::interruptible_future<>
PG::with_locked_obc<RWState::RWEXCL>(ObjectContextRef, with_obc_func_t&&);

PG::interruptible_future<> PG::handle_rep_op(Ref<MOSDRepOp> req)
{
  if (__builtin_expect(stopping, false)) {
    return seastar::make_exception_future<>(
	crimson::common::system_shutdown_exception());
  }

  if (can_discard_replica_op(*req)) {
    return seastar::now();
  }

  ceph::os::Transaction txn;
  auto encoded_txn = req->get_data().cbegin();
  decode(txn, encoded_txn);
  auto p = req->logbl.cbegin();
  std::vector<pg_log_entry_t> log_entries;
  decode(log_entries, p);
  peering_state.append_log(std::move(log_entries), req->pg_trim_to,
      req->version, req->min_last_complete_ondisk, txn, !txn.empty(), false);
  logger().debug("PG::handle_rep_op: do_transaction...");
  return interruptor::make_interruptible(shard_services.get_store().do_transaction(
	coll_ref, std::move(txn))).then_interruptible(
      [req, lcod=peering_state.get_info().last_complete, this] {
      peering_state.update_last_complete_ondisk(lcod);
      const auto map_epoch = get_osdmap_epoch();
      auto reply = crimson::make_message<MOSDRepOpReply>(
        req.get(), pg_whoami, 0,
	map_epoch, req->get_min_epoch(), CEPH_OSD_FLAG_ONDISK);
      reply->set_last_complete_ondisk(lcod);
      return shard_services.send_to_osd(req->from.osd, std::move(reply), map_epoch);
    });
}

void PG::handle_rep_op_reply(crimson::net::ConnectionRef conn,
			     const MOSDRepOpReply& m)
{
  if (!can_discard_replica_op(m)) {
    backend->got_rep_op_reply(m);
  }
}

bool PG::old_peering_msg(
  const epoch_t reply_epoch,
  const epoch_t query_epoch) const
{
  if (const epoch_t lpr = peering_state.get_last_peering_reset();
      lpr > reply_epoch || lpr > query_epoch) {
    logger().debug("{}: pg changed {} lpr {}, reply_epoch {}, query_epoch {}",
                   __func__, get_info().history, lpr, reply_epoch, query_epoch);
    return true;
  }
  return false;
}

bool PG::can_discard_replica_op(const Message& m, epoch_t m_map_epoch) const
{
  // if a repop is replied after a replica goes down in a new osdmap, and
  // before the pg advances to this new osdmap, the repop replies before this
  // repop can be discarded by that replica OSD, because the primary resets the
  // connection to it when handling the new osdmap marking it down, and also
  // resets the messenger sesssion when the replica reconnects. to avoid the
  // out-of-order replies, the messages from that replica should be discarded.
  const auto osdmap = peering_state.get_osdmap();
  const int from_osd = m.get_source().num();
  if (osdmap->is_down(from_osd)) {
    return true;
  }
  // Mostly, this overlaps with the old_peering_msg
  // condition.  An important exception is pushes
  // sent by replicas not in the acting set, since
  // if such a replica goes down it does not cause
  // a new interval.
  if (osdmap->get_down_at(from_osd) >= m_map_epoch) {
    return true;
  }
  // same pg?
  //  if pg changes *at all*, we reset and repeer!
  return old_peering_msg(m_map_epoch, m_map_epoch);
}

seastar::future<> PG::stop()
{
  logger().info("PG {} {}", pgid, __func__);
  stopping = true;
  cancel_local_background_io_reservation();
  cancel_remote_recovery_reservation();
  check_readable_timer.cancel();
  renew_lease_timer.cancel();
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
  logger().debug("{}, {}", __func__, *this);
  for (auto& obc : obc_set_accessing) {
    obc.interrupt(::crimson::common::actingset_changed(is_primary()));
  }
  recovery_backend->on_peering_interval_change(t);
  backend->on_actingset_changed({ is_primary() });
}

bool PG::can_discard_op(const MOSDOp& m) const {
  return __builtin_expect(m.get_map_epoch()
      < peering_state.get_info().history.same_primary_since, false);
}

bool PG::is_degraded_or_backfilling_object(const hobject_t& soid) const {
  /* The conditions below may clear (on_local_recover, before we queue
   * the transaction) before we actually requeue the degraded waiters
   * in on_global_recover after the transaction completes.
   */
  if (peering_state.get_pg_log().get_missing().get_items().count(soid))
    return true;
  ceph_assert(!get_acting_recovery_backfill().empty());
  for (auto& peer : get_acting_recovery_backfill()) {
    if (peer == get_primary()) continue;
    auto peer_missing_entry = peering_state.get_peer_missing().find(peer);
    // If an object is missing on an async_recovery_target, return false.
    // This will not block the op and the object is async recovered later.
    if (peer_missing_entry != peering_state.get_peer_missing().end() &&
	peer_missing_entry->second.get_items().count(soid)) {
	return true;
    }
    // Object is degraded if after last_backfill AND
    // we are backfilling it
    if (is_backfill_target(peer) &&
        peering_state.get_peer_info(peer).last_backfill <= soid &&
	recovery_handler->backfill_state->get_last_backfill_started() >= soid &&
	recovery_backend->is_recovering(soid)) {
      return true;
    }
  }
  return false;
}

PG::interruptible_future<std::tuple<bool, int>>
PG::already_complete(const osd_reqid_t& reqid)
{
  eversion_t version;
  version_t user_version;
  int ret;
  std::vector<pg_log_op_return_item_t> op_returns;

  if (peering_state.get_pg_log().get_log().get_request(
	reqid, &version, &user_version, &ret, &op_returns)) {
    return backend->request_committed(reqid, version).then([ret] {
      return seastar::make_ready_future<std::tuple<bool, int>>(true, ret);
    });
  } else {
    return seastar::make_ready_future<std::tuple<bool, int>>(false, 0);
  }
}

}
