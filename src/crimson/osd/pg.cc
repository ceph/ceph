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

#include <seastar/util/defer.hh>

#include "include/utime_fmt.h"

#include "common/hobject.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"

#include "osd/OSDMap.h"
#include "osd/osd_types_fmt.h"

#include "os/Transaction.h"

#include "crimson/common/coroutine.h"
#include "crimson/common/exception.h"
#include "crimson/common/log.h"
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
#include "crimson/osd/osd_operations/background_recovery.h"
#include "crimson/osd/osd_operations/snaptrim_event.h"
#include "crimson/osd/pg_recovery.h"
#include "crimson/osd/replicated_recovery_backend.h"
#include "crimson/osd/watch.h"

using std::ostream;
using std::set;
using std::string;
using std::vector;

SET_SUBSYS(osd);

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
    osdmap_gate("PG::osdmap_gate"),
    shard_services{shard_services},
    backend(
      PGBackend::create(
	pgid.pgid,
	pg_shard,
	pool,
        *this,
	coll_ref,
	shard_services,
	profile,
	*this)),
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
      PG_FEATURE_CRIMSON_ALL,
      this,
      this),
    scrubber(*this),
    obc_registry{
      local_conf()},
    obc_loader{
      obc_registry,
      *backend.get(),
      *this},
    osdriver(
      &shard_services.get_store(),
      coll_ref,
      pgid.make_snapmapper_oid()),
    snap_mapper(
      this->shard_services.get_cct(),
      &osdriver,
      pgid.ps(),
      pgid.get_split_bits(pool.get_pg_num()),
      pgid.pool(),
      pgid.shard),
    wait_for_active_blocker(this)
{
  scrubber.initiate();
  peering_state.set_backend_predicates(
    new ReadablePredicate(pg_whoami),
    new RecoverablePredicate());
  osdmap_gate.got_map(osdmap->get_epoch());
}

PG::~PG() {}

void PG::check_blocklisted_watchers()
{
  logger().debug("{}", __func__);
  obc_registry.for_each([this](ObjectContextRef obc) {
    assert(obc);
    for (const auto& [key, watch] : obc->watchers) {
      assert(watch->get_pg() == this);
      const auto& ea = watch->get_peer_addr();
      logger().debug("watch: Found {} cookie {}. Checking entity_add_t {}",
                     watch->get_entity(), watch->get_cookie(), ea);
      if (get_osdmap()->is_blocklisted(ea)) {
        logger().info("watch: Found blocklisted watcher for {}", ea);
        watch->do_watch_timeout();
      }
    }
  });
}

bool PG::try_flush_or_schedule_async() {
  logger().debug("PG::try_flush_or_schedule_async: flush ...");
  (void)shard_services.get_store().flush(
    coll_ref
  ).then(
    [this, epoch=get_osdmap_epoch()]() {
      return shard_services.start_operation<LocalPeeringEvent>(
	this,
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

void PG::apply_stats(
  const hobject_t &soid,
  const object_stat_sum_t &delta_stats)
{
  peering_state.apply_op_stats(soid, delta_stats);
  scrubber.handle_op_stats(soid, delta_stats);
  publish_stats_to_osd();
}

void PG::queue_check_readable(epoch_t last_peering_reset, ceph::timespan delay)
{
  // handle the peering event in the background
  logger().debug(
    "{}: PG::queue_check_readable lpr: {}, delay: {}",
    *this, last_peering_reset, delay);
  check_readable_timer.cancel();
  check_readable_timer.set_callback([last_peering_reset, this] {
    logger().debug(
      "{}: PG::queue_check_readable callback lpr: {}",
      *this, last_peering_reset);
    (void) shard_services.start_operation<LocalPeeringEvent>(
      this,
      pg_whoami,
      pgid,
      last_peering_reset,
      last_peering_reset,
      PeeringState::CheckReadable{});
    });
  check_readable_timer.arm(
    std::chrono::duration_cast<seastar::lowres_clock::duration>(delay));
}

PG::interruptible_future<> PG::find_unfound(epoch_t epoch_started)
{
  if (!have_unfound()) {
    return interruptor::now();
  }
  PeeringCtx rctx;
  if (!peering_state.discover_all_missing(rctx)) {
    if (peering_state.state_test(PG_STATE_BACKFILLING)) {
      logger().debug(
        "{} {} no luck, giving up on this pg for now (in backfill)",
        *this, __func__);
      std::ignore = get_shard_services().start_operation<LocalPeeringEvent>(
        this,
        get_pg_whoami(),
        get_pgid(),
        epoch_started,
        epoch_started,
        PeeringState::UnfoundBackfill());
    } else if (peering_state.state_test(PG_STATE_RECOVERING)) {
      logger().debug(
        "{} {} no luck, giving up on this pg for now (in recovery)",
        *this, __func__);
      std::ignore = get_shard_services().start_operation<LocalPeeringEvent>(
        this,
        get_pg_whoami(),
        get_pgid(),
        epoch_started,
        epoch_started,
        PeeringState::UnfoundRecovery());
    }
  }
  return get_shard_services().dispatch_context(get_collection_ref(), std::move(rctx));
}

void PG::recheck_readable()
{
  bool changed = false;
  const auto mnow = shard_services.get_mnow();
  if (peering_state.state_test(PG_STATE_WAIT)) {
    auto prior_readable_until_ub = peering_state.get_prior_readable_until_ub();
    if (mnow < prior_readable_until_ub) {
      logger().info(
	"{}: {} will wait (mnow {} < prior_readable_until_ub {})",
	*this, __func__, mnow, prior_readable_until_ub);
      queue_check_readable(
	peering_state.get_last_peering_reset(),
	prior_readable_until_ub - mnow);
    } else {
      logger().info(
	"{}:{} no longer wait (mnow {} >= prior_readable_until_ub {})",
	*this, __func__, mnow, prior_readable_until_ub);
      peering_state.state_clear(PG_STATE_WAIT);
      peering_state.clear_prior_readable_until_ub();
      changed = true;
    }
  }
  if (peering_state.state_test(PG_STATE_LAGGY)) {
    auto readable_until = peering_state.get_readable_until();
    if (readable_until == readable_until.zero()) {
      logger().info(
	"{}:{} still laggy (mnow {}, readable_until zero)",
	*this, __func__, mnow);
    } else if (mnow >= readable_until) {
      logger().info(
	"{}:{} still laggy (mnow {} >= readable_until {})",
	*this, __func__, mnow, readable_until);
    } else {
      logger().info(
	"{}:{} no longer laggy (mnow {} < readable_until {})",
	*this, __func__, mnow, readable_until);
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
  const unsigned local_num_pgs = shard_services.get_num_local_pgs();
  const unsigned local_target =
    local_conf().get_val<uint64_t>("osd_target_pg_log_entries_per_osd") /
    seastar::smp::count;
  const unsigned min_pg_log_entries =
    local_conf().get_val<uint64_t>("osd_min_pg_log_entries");
  if (local_num_pgs > 0 && local_target > 0) {
    // target an even spread of our budgeted log entries across all
    // PGs.  note that while we only get to control the entry count
    // for primary PGs, we'll normally be responsible for a mix of
    // primary and replica PGs (for the same pool(s) even), so this
    // will work out.
    const unsigned max_pg_log_entries =
      local_conf().get_val<uint64_t>("osd_max_pg_log_entries");
    return std::clamp(local_target / local_num_pgs,
		      min_pg_log_entries,
		      max_pg_log_entries);
  } else {
    // fall back to a per-pg value.
    return min_pg_log_entries;
  }
}

void PG::on_removal(ceph::os::Transaction &t) {
  ceph_assert(log_entry_update_waiting_on.empty());
  t.register_on_commit(
    new LambdaContext(
      [this](int r) {
      ceph_assert(r == 0);
      (void)shard_services.start_operation<LocalPeeringEvent>(
        this, pg_whoami, pgid, float(0.001), get_osdmap_epoch(),
        get_osdmap_epoch(), PeeringState::DeleteSome());
  }));
}

void PG::clear_log_entry_maps()
{
  log_entry_update_waiting_on.clear();
}

void PG::on_activate(interval_set<snapid_t> snaps)
{
  logger().debug("{}: {} snaps={}", *this, __func__, snaps);
  snap_trimq = std::move(snaps);
  projected_last_update = peering_state.get_info().last_update;
}

void PG::on_replica_activate()
{
  logger().debug("{}: {}", *this, __func__);
  scrubber.on_replica_activate();
}

void PG::on_activate_complete()
{
  /* Confusingly, on_activate_complete is invoked when the primary and replicas
   * have recorded the current interval.  At that point, the PG may either become
   * ACTIVE or PEERED, depending on whether the acting set is eligible for client
   * IO.  Only unblock wait_for_active_blocker if we actually became ACTIVE */
  if (peering_state.is_active()) {
    wait_for_active_blocker.unblock();
  }

  if (peering_state.needs_recovery()) {
    logger().info("{}: requesting recovery",
                  __func__);
    (void) shard_services.start_operation<LocalPeeringEvent>(
      this,
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
      pg_whoami,
      pgid,
      float(0.001),
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::AllReplicasRecovered{});
  }
  publish_stats_to_osd();
  recovery_handler->on_activate_complete();
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
    peering_state.get_pgpool().info.require_rollback());
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
  logger().info("removing pg {}", pgid);
  auto fut = interruptor::make_interruptible(
    shard_services.get_store().list_objects(
      coll_ref,
      _next,
      ghobject_t::get_max(),
      local_conf()->osd_target_transaction_size));

  auto [objs_to_rm, next] = fut.get();
  if (objs_to_rm.empty()) {
    logger().info("all objs removed, removing coll for {}", pgid);
    t.remove(coll_ref->get_cid(), pgid.make_snapmapper_oid());
    t.remove(coll_ref->get_cid(), pgmeta_oid);
    t.remove_collection(coll_ref->get_cid());
    (void) shard_services.get_store().do_transaction(
      coll_ref, t.claim_and_reset()).then([this] {
      return shard_services.remove_pg(pgid);
    });
    return {next, false};
  } else {
    for (auto &obj : objs_to_rm) {
      if (obj == pgmeta_oid || obj.is_internal_pg_local()) {
        continue;
      }
      logger().trace("pg {}, removing obj {}", pgid, obj);
      t.remove(coll_ref->get_cid(), obj);
    }
    t.register_on_commit(
      new LambdaContext([this](int r) {
      ceph_assert(r == 0);
      logger().trace("triggering more pg delete {}", pgid);
      (void) shard_services.start_operation<LocalPeeringEvent>(
        this,
        pg_whoami,
        pgid,
        float(0.001),
        get_osdmap_epoch(),
        get_osdmap_epoch(),
        PeeringState::DeleteSome{});
    }));
    return {next, true};
  }
}

Context *PG::on_clean()
{
  recovery_handler->on_pg_clean();
  scrubber.on_primary_active_clean();
  recovery_finisher = new C_PG_FinishRecovery(*this);
  return recovery_finisher;
}

seastar::future<> PG::clear_temp_objects()
{
  logger().info("{} {}", __func__, pgid);
  ghobject_t _next;
  ceph::os::Transaction t;
  auto max_size = local_conf()->osd_target_transaction_size;
  while(true) {
    auto [objs, next] = co_await shard_services.get_store().list_objects(
      coll_ref, _next, ghobject_t::get_max(), max_size);
    if (objs.empty()) {
      if (!t.empty()) {
        co_await shard_services.get_store().do_transaction(
          coll_ref, std::move(t));
      }
      break;
    }
    for (auto &obj : objs) {
      if (obj.hobj.is_temp()) {
        t.remove(coll_ref->get_cid(), obj);
      }
    }
    _next = next;
    if (t.get_num_ops() >= max_size) {
      co_await shard_services.get_store().do_transaction(
        coll_ref, t.claim_and_reset());
    }
  }
}

PG::interruptible_future<seastar::stop_iteration> PG::trim_snap(
  snapid_t to_trim,
  bool needs_pause)
{
  return interruptor::repeat([this, to_trim, needs_pause] {
    logger().debug("{}: going to start SnapTrimEvent, to_trim={}",
                   *this, to_trim);
    return shard_services.start_operation_may_interrupt<
      interruptor, SnapTrimEvent>(
      this,
      snap_mapper,
      to_trim,
      needs_pause
    ).second.handle_error_interruptible(
      crimson::ct_error::enoent::handle([this] {
        logger().error("{}: ENOENT saw, trimming stopped", *this);
        peering_state.state_set(PG_STATE_SNAPTRIM_ERROR);
        publish_stats_to_osd();
        return seastar::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::yes);
      })
    );
  }).then_interruptible([this, trimmed=to_trim] {
    logger().debug("{}: trimmed snap={}", *this, trimmed);
    snap_trimq.erase(trimmed);
    return seastar::make_ready_future<seastar::stop_iteration>(
      seastar::stop_iteration::no);
  });
}

void PG::on_active_actmap()
{
  logger().debug("{}: {} snap_trimq={}", *this, __func__, snap_trimq);
  peering_state.state_clear(PG_STATE_SNAPTRIM_ERROR);
  if (peering_state.is_active() && peering_state.is_clean()) {
    if (peering_state.state_test(PG_STATE_SNAPTRIM)) {
      logger().debug("{}: {} already trimming.", *this, __func__);
      return;
    }
    // loops until snap_trimq is empty or SNAPTRIM_ERROR.
    Ref<PG> pg_ref = this;
    std::ignore = interruptor::with_interruption([this] {
      return interruptor::repeat(
        [this]() -> interruptible_future<seastar::stop_iteration> {
          if (snap_trimq.empty()
              || peering_state.state_test(PG_STATE_SNAPTRIM_ERROR)) {
            return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::yes);
          }
          peering_state.state_set(PG_STATE_SNAPTRIM);
          publish_stats_to_osd();
          const auto to_trim = snap_trimq.range_start();
          const auto needs_pause = !snap_trimq.empty();
          return trim_snap(to_trim, needs_pause);
        }
      ).then_interruptible([this] {
        logger().debug("{}: PG::on_active_actmap() finished trimming",
                       *this);
        peering_state.state_clear(PG_STATE_SNAPTRIM);
        peering_state.state_clear(PG_STATE_SNAPTRIM_ERROR);
        return seastar::now();
      });
    }, [this](std::exception_ptr eptr) {
      logger().debug("{}: snap trimming interrupted", *this);
      ceph_assert(!peering_state.state_test(PG_STATE_SNAPTRIM));
    }, pg_ref, pg_ref->get_osdmap_epoch()).finally([pg_ref, this] {
      publish_stats_to_osd();
    });
  } else {
    logger().debug("{}: pg not clean, skipping snap trim");
    ceph_assert(!peering_state.state_test(PG_STATE_SNAPTRIM));
  }
}

void PG::on_active_advmap(const OSDMapRef &osdmap)
{
  const auto new_removed_snaps = osdmap->get_new_removed_snaps();
  if (auto it = new_removed_snaps.find(get_pgid().pool());
      it != new_removed_snaps.end()) {
    bool bad = false;
    for (auto j : it->second) {
      if (snap_trimq.intersects(j.first, j.second)) {
	decltype(snap_trimq) added, overlap;
	added.insert(j.first, j.second);
	overlap.intersection_of(snap_trimq, added);
        logger().error("{}: {} removed_snaps already contains {}",
                       *this, __func__, overlap);
	bad = true;
	snap_trimq.union_of(added);
      } else {
	snap_trimq.insert(j.first, j.second);
      }
    }
    logger().info("{}: {} new removed snaps {}, snap_trimq now{}",
                  *this, __func__, it->second, snap_trimq);
    assert(!bad || !local_conf().get_val<bool>("osd_debug_verify_cached_snaps"));
  }
}

void PG::scrub_requested(scrub_level_t scrub_level, scrub_type_t scrub_type)
{
  /* We don't actually route the scrub request message into the state machine.
   * Instead, we handle it directly in PGScrubber::handle_scrub_requested).
   */
  ceph_assert(0 == "impossible in crimson");
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

ceph::signedspan PG::get_mnow() const
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
      pg_whoami,
      pgid,
      last_peering_reset,
      last_peering_reset,
      RenewLease{});
    });
  renew_lease_timer.arm(
    std::chrono::duration_cast<seastar::lowres_clock::duration>(delay));
}


seastar::future<> PG::init(
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
  assert(coll_ref);
  return shard_services.get_store().exists(
    get_collection_ref(), pgid.make_snapmapper_oid()
  ).safe_then([&t, this](bool existed) {
      if (!existed) {
        t.touch(coll_ref->get_cid(), pgid.make_snapmapper_oid());
      }
    },
    ::crimson::ct_error::assert_all{"unexpected eio"}
  );
}

seastar::future<> PG::read_state(crimson::os::FuturizedStore::Shard* store)
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
	pg_whoami,
	pgid,
	epoch,
	epoch,
	PeeringState::Initialize());

    return seastar::now();
  }).then([this, store]() {
    logger().debug("{} setting collection options", __func__);
    return store->set_collection_opts(
          coll_ref,
          get_pgpool().info.opts);
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

void PG::init_collection_pool_opts()
{
  std::ignore = shard_services.get_store().set_collection_opts(coll_ref, get_pgpool().info.opts);
}

void PG::on_pool_change()
{
  init_collection_pool_opts();
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

void PG::enqueue_push_for_backfill(
  const hobject_t &obj,
  const eversion_t &v,
  const std::vector<pg_shard_t> &peers)
{
  assert(recovery_handler);
  assert(recovery_handler->backfill_state);
  auto backfill_state = recovery_handler->backfill_state.get();
  backfill_state->enqueue_standalone_push(obj, v, peers);
}

PG::interruptible_future<
  std::tuple<PG::interruptible_future<>,
             PG::interruptible_future<>>>
PG::submit_transaction(
  ObjectContextRef&& obc,
  ObjectContextRef&& new_clone,
  ceph::os::Transaction&& txn,
  osd_op_params_t&& osd_op_p,
  std::vector<pg_log_entry_t>&& log_entries)
{
  if (__builtin_expect(stopping, false)) {
    co_return std::make_tuple(
        interruptor::make_interruptible(seastar::make_exception_future<>(
          crimson::common::system_shutdown_exception())),
        interruptor::now());
  }

  epoch_t map_epoch = get_osdmap_epoch();
  auto at_version = osd_op_p.at_version;

  peering_state.pre_submit_op(obc->obs.oi.soid, log_entries, at_version);
  peering_state.update_trim_to();

  ceph_assert(!log_entries.empty());
  ceph_assert(log_entries.rbegin()->version >= projected_last_update);
  projected_last_update = log_entries.rbegin()->version;

  for (const auto& entry: log_entries) {
    projected_log.add(entry);
  }

  auto [submitted, all_completed] = co_await backend->submit_transaction(
      peering_state.get_acting_recovery_backfill(),
      obc->obs.oi.soid,
      std::move(new_clone),
      std::move(txn),
      std::move(osd_op_p),
      peering_state.get_last_peering_reset(),
      map_epoch,
      std::move(log_entries));
  co_return std::make_tuple(
    std::move(submitted),
    all_completed.then_interruptible(
      [this, last_complete=peering_state.get_info().last_complete, at_version]
      (auto acked) {
      for (const auto& peer : acked) {
        peering_state.update_peer_last_complete_ondisk(
          peer.shard, peer.last_complete_ondisk);
      }
      peering_state.complete_write(at_version, last_complete);
      return seastar::now();
    })
  );
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

PG::interruptible_future<>
PG::BackgroundProcessLock::lock() noexcept
{
  return interruptor::make_interruptible(mutex.lock());
}

// We may need to rollback the ObjectContext on failed op execution.
// Copy the current obc before mutating it in order to recover on failures.
ObjectContextRef duplicate_obc(const ObjectContextRef &obc) {
  ObjectContextRef object_context = new ObjectContext(obc->obs.oi.soid);
  object_context->obs = obc->obs;
  object_context->ssc = new SnapSetContext(*obc->ssc);
  return object_context;
}

PG::interruptible_future<> PG::complete_error_log(const ceph_tid_t& rep_tid,
                                         const eversion_t& version)
{
  auto result = interruptor::now();
  auto last_complete = peering_state.get_info().last_complete;
  ceph_assert(log_entry_update_waiting_on.contains(rep_tid));
  auto& log_update = log_entry_update_waiting_on[rep_tid];
  ceph_assert(log_update.waiting_on.contains(pg_whoami));
  log_update.waiting_on.erase(pg_whoami);
  if (log_update.waiting_on.empty()) {
    log_entry_update_waiting_on.erase(rep_tid);
    peering_state.complete_write(version, last_complete);
    logger().debug("complete_error_log: write complete,"
                   " erasing rep_tid {}", rep_tid);
  } else {
    logger().debug("complete_error_log: rep_tid {} awaiting update from {}",
                   rep_tid, log_update.waiting_on);
    result = interruptor::make_interruptible(
      log_update.all_committed.get_shared_future()
    ).then_interruptible([this, last_complete, rep_tid, version] {
      logger().debug("complete_error_log: rep_tid {} awaited ", rep_tid);
      peering_state.complete_write(version, last_complete);
      ceph_assert(!log_entry_update_waiting_on.contains(rep_tid));
      return seastar::now();
    });
  }
  return result;
}

PG::interruptible_future<eversion_t> PG::submit_error_log(
  Ref<MOSDOp> m,
  const OpInfo &op_info,
  ObjectContextRef obc,
  const std::error_code e,
  ceph_tid_t rep_tid)
{
  // as with submit_executer, need to ensure that log numbering and submission
  // are atomic
  co_await interruptor::make_interruptible(submit_lock.lock());
  auto unlocker = seastar::defer([this] {
    submit_lock.unlock();
  });
  LOG_PREFIX(PG::submit_error_log);
  DEBUGDPP("{} rep_tid: {} error: {}",
	   *this, *m, rep_tid, e);
  const osd_reqid_t &reqid = m->get_reqid();
  mempool::osd_pglog::list<pg_log_entry_t> log_entries;
  log_entries.push_back(pg_log_entry_t(pg_log_entry_t::ERROR,
                                       obc->obs.oi.soid,
                                       get_next_version(),
                                       eversion_t(), 0,
                                       reqid, utime_t(),
                                       -e.value()));
  if (op_info.allows_returnvec()) {
    log_entries.back().set_op_returns(m->ops);
  }
  ceph_assert(is_primary());
  ceph_assert(!log_entries.empty());
  ceph_assert(log_entries.rbegin()->version >= projected_last_update);
  projected_last_update = log_entries.rbegin()->version;
  ceph::os::Transaction t;
  peering_state.merge_new_log_entries(
    log_entries, t, peering_state.get_pg_trim_to(),
    peering_state.get_pg_committed_to());


  set<pg_shard_t> waiting_on;
  for (const auto &peer: get_acting_recovery_backfill()) {
    if (peer == pg_whoami) {
      continue;
    }
    ceph_assert(peering_state.get_peer_missing().count(peer));
    ceph_assert(peering_state.has_peer_info(peer));
    auto log_m = crimson::make_message<MOSDPGUpdateLogMissing>(
      log_entries,
      spg_t(peering_state.get_info().pgid.pgid, peer.shard),
      pg_whoami.shard,
      get_osdmap_epoch(),
      get_last_peering_reset(),
      rep_tid,
      peering_state.get_pg_trim_to(),
      peering_state.get_pg_committed_to());
    waiting_on.insert(peer);

    DEBUGDPP("sending log missing_request (rep_tid: {} entries: {}) to osd {}",
	     *this, rep_tid, log_entries, peer.osd);
    co_await interruptor::make_interruptible(
      shard_services.send_to_osd(
	peer.osd,
	std::move(log_m),
	get_osdmap_epoch()));
  }
  waiting_on.insert(pg_whoami);
  DEBUGDPP("inserting rep_tid {}", *this, rep_tid);
  log_entry_update_waiting_on.insert(
    std::make_pair(rep_tid,
		   log_update_t{std::move(waiting_on)}));
  co_await interruptor::make_interruptible(
    shard_services.get_store().do_transaction(
      get_collection_ref(), std::move(t)
    ));

  peering_state.update_trim_to();
  co_return projected_last_update;
}

PG::run_executer_fut PG::run_executer(
  OpsExecuter &ox,
  ObjectContextRef obc,
  const OpInfo &op_info,
  std::vector<OSDOp>& ops)
{
  LOG_PREFIX(PG::run_executer);
  auto rollbacker = ox.create_rollbacker(
    [stored_obc=duplicate_obc(obc)](auto &obc) mutable {
      obc->update_from(*stored_obc);
    });
  auto rollback_on_error = seastar::defer([&rollbacker] {
    rollbacker.rollback_obc_if_modified();
  });

  for (auto &op: ops) {
    DEBUGDPP("object {} handle op {}", *this, ox.get_target(), op);
    co_await ox.execute_op(op);
  }
  DEBUGDPP("object {} all operations successful", *this, ox.get_target());

  // check for full
  if ((ox.delta_stats.num_bytes > 0 ||
       ox.delta_stats.num_objects > 0) &&
      get_pgpool().info.has_flag(pg_pool_t::FLAG_FULL)) {
    const auto& m = ox.get_message();
    if (m.get_reqid().name.is_mds() ||   // FIXME: ignore MDS for now
	m.has_flag(CEPH_OSD_FLAG_FULL_FORCE)) {
      INFODPP("full, but proceeding due to FULL_FORCE, or MDS", *this);
    } else if (m.has_flag(CEPH_OSD_FLAG_FULL_TRY)) {
      // they tried, they failed.
      INFODPP("full, replying to FULL_TRY op", *this);
      if (get_pgpool().info.has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
	co_await run_executer_fut(
	  crimson::ct_error::edquot::make());
      } else {
	co_await run_executer_fut(
	  crimson::ct_error::enospc::make());
      }
    } else {
      // drop request
      INFODPP("full, dropping request (bad client)", *this);
      co_await run_executer_fut(
	crimson::ct_error::eagain::make());
    }
  }
  rollback_on_error.cancel();
}

PG::submit_executer_fut PG::submit_executer(
  OpsExecuter &&ox,
  const std::vector<OSDOp>& ops) {
  LOG_PREFIX(PG::submit_executer);
  DEBUGDPP("", *this);

  // we need to build the pg log entries and submit the transaction
  // atomically to ensure log ordering
  co_await interruptor::make_interruptible(submit_lock.lock());
  auto unlocker = seastar::defer([this] {
    submit_lock.unlock();
  });

  auto [submitted, completed] = co_await std::move(
    ox
  ).flush_changes_and_submit(
    ops,
    snap_mapper,
    osdriver
  );
  co_return std::make_tuple(
    std::move(submitted).then_interruptible([unlocker=std::move(unlocker)] {}),
    std::move(completed));
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
  }).handle_exception_type_interruptible([=, this](const crimson::osd::error& e) {
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
  ceph_assert(op_info.get_flags());
  if (op_info.rwordered() && op_info.may_read()) {
    return RWState::RWEXCL;
  } else if (op_info.rwordered()) {
    return RWState::RWWRITE;
  } else {
    ceph_assert(op_info.may_read());
    return RWState::RWREAD;
  }
}

void PG::check_blocklisted_obc_watchers(
  ObjectContextRef &obc)
{
  if (obc->watchers.empty()) {
    for (auto &[src, winfo] : obc->obs.oi.watchers) {
      auto watch = crimson::osd::Watch::create(
        obc, winfo, src.second, this);
      watch->disconnect();
      auto [it, emplaced] = obc->watchers.emplace(src, std::move(watch));
      assert(emplaced);
      logger().debug("added watch for obj {}, client {}",
        obc->get_oid(), src.second);
    }
  }
}

void PG::update_stats(const pg_stat_t &stat) {
  peering_state.update_stats(
    [&stat] (auto& history, auto& stats) {
      stats = stat;
      return false;
    }
  );
}

PG::interruptible_future<> PG::handle_rep_op(Ref<MOSDRepOp> req)
{
  LOG_PREFIX(PG::handle_rep_op);
  DEBUGDPP("{}", *this, *req);
  if (can_discard_replica_op(*req)) {
    co_return;
  }

  ceph::os::Transaction txn;
  auto encoded_txn = req->get_data().cbegin();
  decode(txn, encoded_txn);
  auto p = req->logbl.cbegin();
  std::vector<pg_log_entry_t> log_entries;
  decode(log_entries, p);
  update_stats(req->pg_stats);

  co_await update_snap_map(
    log_entries,
    txn);

  log_operation(std::move(log_entries),
                req->pg_trim_to,
                req->version,
                req->pg_committed_to,
                !txn.empty(),
                txn,
                false);
  DEBUGDPP("{} do_transaction", *this, *req);
  co_await interruptor::make_interruptible(
    shard_services.get_store().do_transaction(coll_ref, std::move(txn))
  );

  const auto &lcod = peering_state.get_info().last_complete;
  peering_state.update_last_complete_ondisk(lcod);
  const auto map_epoch = get_osdmap_epoch();
  auto reply = crimson::make_message<MOSDRepOpReply>(
    req.get(), pg_whoami, 0,
    map_epoch, req->get_min_epoch(), CEPH_OSD_FLAG_ONDISK);
  reply->set_last_complete_ondisk(lcod);
  co_await interruptor::make_interruptible(
    shard_services.send_to_osd(req->from.osd, std::move(reply), map_epoch)
  );
  co_return;
}

PG::interruptible_future<> PG::update_snap_map(
  const std::vector<pg_log_entry_t> &log_entries,
  ObjectStore::Transaction& t)
{
  LOG_PREFIX(PG::update_snap_map);
  DEBUGDPP("", *this);
  return interruptor::do_for_each(
    log_entries,
    [this, &t](const auto& entry) mutable {
    if (entry.soid.snap < CEPH_MAXSNAP) {
      // TODO: avoid seastar::async https://tracker.ceph.com/issues/67704
      return interruptor::async(
        [this, entry, _t=osdriver.get_transaction(&t)]() mutable {
        snap_mapper.update_snap_map(entry, &_t);
      });
    }
    return interruptor::now();
  });
}

void PG::log_operation(
  std::vector<pg_log_entry_t>&& logv,
  const eversion_t &trim_to,
  const eversion_t &roll_forward_to,
  const eversion_t &pg_committed_to,
  bool transaction_applied,
  ObjectStore::Transaction &txn,
  bool async) {
  LOG_PREFIX(PG::log_operation);
  DEBUGDPP("", *this);
  if (is_primary()) {
    ceph_assert(trim_to <= peering_state.get_pg_committed_to());
  }
  auto last = logv.rbegin();
  if (is_primary() && last != logv.rend()) {
    DEBUGDPP("on primary, trimming projected log", *this);
    projected_log.skip_can_rollback_to_to_head();
    projected_log.trim(shard_services.get_cct(), last->version,
                       nullptr, nullptr, nullptr);
  }

  if (!is_primary()) { // && !is_ec_pg()
    DEBUGDPP("on replica, clearing obc", *this);
    replica_clear_repop_obc(logv);
  }
  if (!logv.empty()) {
    scrubber.on_log_update(logv.rbegin()->version);
  }
  peering_state.append_log(std::move(logv),
                           trim_to,
                           roll_forward_to,
                           pg_committed_to,
                           txn,
                           !txn.empty(),
                           false);
}

void PG::replica_clear_repop_obc(
  const std::vector<pg_log_entry_t> &logv) {
  LOG_PREFIX(PG::replica_clear_repop_obc);
  DEBUGDPP("clearing obc for {} log entries", logv.size());
  for (auto &&e: logv) {
    DEBUGDPP("clearing entry for {} from: {} to: {}",
	     e.soid,
	     e.soid.get_object_boundary(),
	     e.soid.get_head());
    /* Have to blast all clones, they share a snapset */
    obc_registry.clear_range(
      e.soid.get_object_boundary(), e.soid.get_head());
  }
}

void PG::handle_rep_op_reply(const MOSDRepOpReply& m)
{
  if (!can_discard_replica_op(m)) {
    backend->got_rep_op_reply(m);
  }
}

PG::interruptible_future<> PG::do_update_log_missing(
  Ref<MOSDPGUpdateLogMissing> m,
  crimson::net::ConnectionXcoreRef conn)
{
  if (__builtin_expect(stopping, false)) {
    return seastar::make_exception_future<>(
      crimson::common::system_shutdown_exception());
  }

  ceph_assert(m->get_type() == MSG_OSD_PG_UPDATE_LOG_MISSING);
  ObjectStore::Transaction t;
  std::optional<eversion_t> op_trim_to, op_pg_committed_to;
  if (m->pg_trim_to != eversion_t())
    op_trim_to = m->pg_trim_to;
  if (m->pg_committed_to != eversion_t())
    op_pg_committed_to = m->pg_committed_to;
  logger().debug("op_trim_to = {}, op_pg_committed_to = {}",
    op_trim_to.has_value() ? *op_trim_to : eversion_t(),
    op_pg_committed_to.has_value() ? *op_pg_committed_to : eversion_t());

  peering_state.append_log_entries_update_missing(
    m->entries, t, op_trim_to, op_pg_committed_to);

  return interruptor::make_interruptible(shard_services.get_store().do_transaction(
    coll_ref, std::move(t))).then_interruptible(
    [m, conn, lcod=peering_state.get_info().last_complete, this] {
    if (!peering_state.pg_has_reset_since(m->get_epoch())) {
      peering_state.update_last_complete_ondisk(lcod);
      auto reply =
        crimson::make_message<MOSDPGUpdateLogMissingReply>(
          spg_t(peering_state.get_info().pgid.pgid, get_primary().shard),
          pg_whoami.shard,
          m->get_epoch(),
          m->min_epoch,
          m->get_tid(),
          lcod);
      reply->set_priority(CEPH_MSG_PRIO_HIGH);
      return conn->send(std::move(reply));
    }
    return seastar::now();
  });
}


PG::interruptible_future<> PG::do_update_log_missing_reply(
  Ref<MOSDPGUpdateLogMissingReply> m)
{
  logger().debug("{}: got reply from {}", __func__, m->get_from());

  auto it = log_entry_update_waiting_on.find(m->get_tid());
  if (it != log_entry_update_waiting_on.end()) {
    if (it->second.waiting_on.count(m->get_from())) {
      it->second.waiting_on.erase(m->get_from());
      if (m->last_complete_ondisk != eversion_t()) {
        peering_state.update_peer_last_complete_ondisk(
	  m->get_from(), m->last_complete_ondisk);
      }
    } else {
      logger().error("{} : {} got reply {} from shard we are not waiting for ",
        __func__, peering_state.get_info().pgid, *m, m->get_from());
    }

    if (it->second.waiting_on.empty()) {
      it->second.all_committed.set_value();
      it->second.all_committed = {};
      logger().debug("{}: erasing rep_tid {}",
                     __func__, m->get_tid());
      log_entry_update_waiting_on.erase(it);
    }
  } else {
    logger().error("{} : {} got reply {} on unknown tid {}",
      __func__, peering_state.get_info().pgid, *m, m->get_tid());
  }
  return seastar::now();
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
  logger().debug("{} {}:", *this, __func__);
  clear_log_entry_maps();
  context_registry_on_change();
  obc_loader.notify_on_change(is_primary());
  recovery_backend->on_peering_interval_change(t);
  backend->on_actingset_changed(is_primary());
  wait_for_active_blocker.unblock();
  if (is_primary()) {
    logger().debug("{} {}: requeueing", *this, __func__);
    client_request_orderer.requeue(this);
  } else {
    logger().debug("{} {}: dropping requests", *this, __func__);
    client_request_orderer.clear_and_cancel(*this);
  }
  scrubber.on_interval_change();
  obc_registry.invalidate_on_interval_change();
  // snap trim events are all going to be interrupted,
  // clearing PG_STATE_SNAPTRIM/PG_STATE_SNAPTRIM_ERROR here
  // is save and in time.
  peering_state.state_clear(PG_STATE_SNAPTRIM);
  peering_state.state_clear(PG_STATE_SNAPTRIM_ERROR);
  snap_mapper.reset_backend();
  reset_pglog_based_recovery_op();
}

void PG::context_registry_on_change() {
  std::vector<seastar::shared_ptr<crimson::osd::Watch>> watchers;
  obc_registry.for_each([&watchers](ObjectContextRef obc) {
    assert(obc);
    for (auto j = obc->watchers.begin();
         j != obc->watchers.end();
         j = obc->watchers.erase(j)) {
      watchers.emplace_back(j->second);
    }
  });

  for (auto &watcher : watchers) {
    watcher->discard_state();
  }
}

bool PG::can_discard_op(const MOSDOp& m) const {
  if (m.get_map_epoch() <
      peering_state.get_info().history.same_primary_since) {
    logger().debug("{} changed after {} dropping {} ",
                   __func__ , m.get_map_epoch(), m);
    return true;
  }

  if ((m.get_flags() & (CEPH_OSD_FLAG_BALANCE_READS |
                        CEPH_OSD_FLAG_LOCALIZE_READS))
    && !is_primary()
    && (m.get_map_epoch() <
        peering_state.get_info().history.same_interval_since))
    {
      // Note: the Objecter will resend on interval change without the primary
      // changing if it actually sent to a replica.  If the primary hasn't
      // changed since the send epoch, we got it, and we're primary, it won't
      // have resent even if the interval did change as it sent it to the primary
      // (us).
      return true;
    }
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
        recovery_handler->backfill_state &&
	recovery_handler->backfill_state->get_last_backfill_started() >= soid &&
	recovery_backend->is_recovering(soid)) {
      return true;
    }
  }
  return false;
}

bool PG::should_send_op(
  pg_shard_t peer,
  const hobject_t &hoid) const
{
  if (peer == get_primary())
    return true;
  bool should_send =
    (hoid.pool != (int64_t)get_info().pgid.pool() ||
    // An object has been fully pushed to the backfill target if and only if
    // either of the following conditions is met:
    // 1. peer_info.last_backfill has passed "hoid"
    // 2. last_backfill_started has passed "hoid" and "hoid" is not in the peer
    //    missing set
    hoid <= peering_state.get_peer_info(peer).last_backfill ||
    (has_backfill_state() && hoid <= get_last_backfill_started() &&
     !is_missing_on_peer(peer, hoid)));
  if (!should_send) {
    ceph_assert(is_backfill_target(peer));
    logger().debug("{} issue_repop shipping empty opt to osd."
                   "{}, object {} beyond std::max(last_backfill_started, "
                   "peer_info[peer].last_backfill {})",
                   __func__, peer, hoid,
                   peering_state.get_peer_info(peer).last_backfill);
  }
  return should_send;
  // TODO: should consider async recovery cases in the future which are not supported
  //       by crimson yet
}

PG::interruptible_future<std::optional<PG::complete_op_t>>
PG::already_complete(const osd_reqid_t& reqid)
{
  eversion_t version;
  version_t user_version;
  int ret;
  std::vector<pg_log_op_return_item_t> op_returns;

  if (check_in_progress_op(
        reqid, &version, &user_version, &ret, &op_returns)) {
    complete_op_t dupinfo{
      user_version,
      version,
      ret};
    return backend->request_committed(reqid, version).then([dupinfo] {
      return seastar::make_ready_future<std::optional<complete_op_t>>(dupinfo);
    });
  } else {
    return seastar::make_ready_future<std::optional<complete_op_t>>(std::nullopt);
  }
}

void PG::remove_maybe_snapmapped_object(
  ceph::os::Transaction &t,
  const hobject_t &soid)
{
  t.remove(
    coll_ref->get_cid(),
    ghobject_t{soid, ghobject_t::NO_GEN, pg_whoami.shard});
  if (soid.snap < CEPH_MAXSNAP) {
    OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
    int r = snap_mapper.remove_oid(soid, &_t);
    if (!(r == 0 || r == -ENOENT)) {
      logger().debug("{}: remove_oid returned {}", __func__, cpp_strerror(r));
      ceph_abort();
    }
  }
}

void PG::PGLogEntryHandler::remove(const hobject_t &soid) {
  LOG_PREFIX(PG::PGLogEntryHandler::remove);
  DEBUGDPP("remove {} on pglog rollback", *pg, soid);
  pg->remove_maybe_snapmapped_object(*t, soid);
}

void PG::set_pglog_based_recovery_op(PglogBasedRecovery *op) {
  ceph_assert(!pglog_based_recovery_op);
  pglog_based_recovery_op = op;
}

void PG::reset_pglog_based_recovery_op() {
  pglog_based_recovery_op = nullptr;
}

void PG::cancel_pglog_based_recovery_op() {
  ceph_assert(pglog_based_recovery_op);
  pglog_based_recovery_op->cancel();
  reset_pglog_based_recovery_op();
}

void PG::C_PG_FinishRecovery::finish(int r) {
  LOG_PREFIX(PG::C_PG_FinishRecovery::finish);
  auto &peering_state = pg.get_peering_state();
  if (peering_state.is_deleting() || !peering_state.is_clean()) {
    DEBUGDPP("raced with delete or repair", pg);
    return;
  }
  if (this == pg.recovery_finisher) {
    peering_state.purge_strays();
    pg.recovery_finisher = nullptr;
  } else {
    DEBUGDPP("stale recovery finsher", pg);
  }
}
bool PG::check_in_progress_op(
  const osd_reqid_t& reqid,
  eversion_t *version,
  version_t *user_version,
  int *return_code,
  std::vector<pg_log_op_return_item_t> *op_returns
  ) const
{
  return (
    projected_log.get_request(reqid, version, user_version, return_code,
                              op_returns) ||
    peering_state.get_pg_log().get_log().get_request(
      reqid, version, user_version, return_code, op_returns));
}

}
