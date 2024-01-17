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

#include "common/hobject_fmt.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"

#include "osd/OSDMap.h"
#include "osd/osd_types_fmt.h"

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
#include "crimson/osd/osd_operations/background_recovery.h"
#include "crimson/osd/osd_operations/snaptrim_event.h"
#include "crimson/osd/pg_recovery.h"
#include "crimson/osd/replicated_recovery_backend.h"
#include "crimson/osd/watch.h"

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
    osdmap_gate("PG::osdmap_gate"),
    shard_services{shard_services},
    backend(
      PGBackend::create(
	pgid.pgid,
	pg_shard,
	pool,
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
      pgid.make_pgmeta_oid()),
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
  clear_log_entry_maps();
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
  log_entry_version.clear();
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
  wait_for_active_blocker.unblock();

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
    t.remove(coll_ref->get_cid(), pgmeta_oid);
    t.remove_collection(coll_ref->get_cid());
    (void) shard_services.get_store().do_transaction(
      coll_ref, std::move(t)).then([this] {
      return shard_services.remove_pg(pgid);
    });
    return {next, false};
  } else {
    for (auto &obj : objs_to_rm) {
      if (obj == pgmeta_oid) {
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
  scrubber.on_primary_active_clean();
  return nullptr;
}

void PG::on_active_actmap()
{
  logger().debug("{}: {} snap_trimq={}", *this, __func__, snap_trimq);
  peering_state.state_clear(PG_STATE_SNAPTRIM_ERROR);
  // loops until snap_trimq is empty or SNAPTRIM_ERROR.
  Ref<PG> pg_ref = this;
  std::ignore = seastar::do_until(
    [this] { return snap_trimq.empty()
                    || peering_state.state_test(PG_STATE_SNAPTRIM_ERROR);
    },
    [this] {
      peering_state.state_set(PG_STATE_SNAPTRIM);
      publish_stats_to_osd();
      const auto to_trim = snap_trimq.range_start();
      snap_trimq.erase(to_trim);
      const auto needs_pause = !snap_trimq.empty();
      return seastar::repeat([to_trim, needs_pause, this] {
        logger().debug("{}: going to start SnapTrimEvent, to_trim={}",
                       *this, to_trim);
        return shard_services.start_operation<SnapTrimEvent>(
          this,
          snap_mapper,
          to_trim,
          needs_pause
        ).second.handle_error(
          crimson::ct_error::enoent::handle([this] {
            logger().error("{}: ENOENT saw, trimming stopped", *this);
            peering_state.state_set(PG_STATE_SNAPTRIM_ERROR);
            publish_stats_to_osd();
            return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::yes);
          }), crimson::ct_error::eagain::handle([this] {
            logger().info("{}: EAGAIN saw, trimming restarted", *this);
            return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::no);
          })
        );
      }).then([this, trimmed=to_trim] {
        logger().debug("{}: trimmed snap={}", *this, trimmed);
      });
    }
  ).finally([this, pg_ref=std::move(pg_ref)] {
    logger().debug("{}: PG::on_active_actmap() finished trimming",
                   *this);
    peering_state.state_clear(PG_STATE_SNAPTRIM);
    peering_state.state_clear(PG_STATE_SNAPTRIM_ERROR);
    publish_stats_to_osd();
  });
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
  });
}

PG::interruptible_future<> PG::do_peering_event(
  PGPeeringEvent& evt, PeeringCtx &rctx)
{
  if (peering_state.pg_has_reset_since(evt.get_epoch_requested()) ||
      peering_state.pg_has_reset_since(evt.get_epoch_sent())) {
    logger().debug("{} ignoring {} -- pg has reset", __func__, evt.get_desc());
    return interruptor::now();
  } else {
    logger().debug("{} handling {} for pg: {}", __func__, evt.get_desc(), pgid);
    // all peering event handling needs to be run in a dedicated seastar::thread,
    // so that event processing can involve I/O reqs freely, for example: PG::on_removal,
    // PG::on_new_interval
    return interruptor::async([this, &evt, &rctx] {
      peering_state.handle_event(
        evt.get_event(),
        &rctx);
      peering_state.write_if_dirty(rctx.transaction);
    });
  }
}

seastar::future<> PG::handle_advance_map(
  cached_map_t next_map, PeeringCtx &rctx)
{
  return seastar::async([this, next_map=std::move(next_map), &rctx] {
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
  });
}

seastar::future<> PG::handle_activate_map(PeeringCtx &rctx)
{
  return seastar::async([this, &rctx] {
    peering_state.activate_map(rctx);
  });
}

seastar::future<> PG::handle_initialize(PeeringCtx &rctx)
{
  return seastar::async([this, &rctx] {
    peering_state.handle_event(PeeringState::Initialize{}, &rctx);
  });
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

std::tuple<PG::interruptible_future<>,
           PG::interruptible_future<>>
PG::submit_transaction(
  ObjectContextRef&& obc,
  ceph::os::Transaction&& txn,
  osd_op_params_t&& osd_op_p,
  std::vector<pg_log_entry_t>&& log_entries)
{
  if (__builtin_expect(stopping, false)) {
    return {seastar::make_exception_future<>(
              crimson::common::system_shutdown_exception()),
            seastar::now()};
  }

  epoch_t map_epoch = get_osdmap_epoch();
  ceph_assert(!has_reset_since(osd_op_p.at_version.epoch));

  peering_state.pre_submit_op(obc->obs.oi.soid, log_entries, osd_op_p.at_version);
  peering_state.append_log_with_trim_to_updated(std::move(log_entries), osd_op_p.at_version,
						txn, true, false);

  ceph_assert(!log_entries.empty());
  ceph_assert(log_entries.rbegin()->version >= projected_last_update);
  projected_last_update = log_entries.rbegin()->version;

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

template <class Ret, class SuccessFunc, class FailureFunc>
PG::do_osd_ops_iertr::future<PG::pg_rep_op_fut_t<Ret>>
PG::do_osd_ops_execute(
  seastar::lw_shared_ptr<OpsExecuter> ox,
  ObjectContextRef obc,
  const OpInfo &op_info,
  Ref<MOSDOp> m,
  std::vector<OSDOp>& ops,
  SuccessFunc&& success_func,
  FailureFunc&& failure_func)
{
  assert(ox);
  auto rollbacker = ox->create_rollbacker([this] (auto& obc) {
    return obc_loader.reload_obc(obc).handle_error_interruptible(
      load_obc_ertr::assert_all{"can't live with object state messed up"});
  });
  auto maybe_submit_error_log = [&, op_info, m, obc]
    (const std::error_code& e, const ceph_tid_t& rep_tid) {
    // call submit_error_log only for non-internal clients
    if constexpr (!std::is_same_v<Ret, void>) {
      if(op_info.may_write()) {
        return submit_error_log(m, op_info, obc, e, rep_tid);
      }
    }
    return seastar::now();
  };
  auto error_func_ptr = seastar::make_lw_shared(std::move(maybe_submit_error_log));
  auto failure_func_ptr = seastar::make_lw_shared(std::move(failure_func));
  return interruptor::do_for_each(ops, [ox](OSDOp& osd_op) {
    logger().debug(
      "do_osd_ops_execute: object {} - handling op {}",
      ox->get_target(),
      ceph_osd_op_name(osd_op.op.op));
    return ox->execute_op(osd_op);
  }).safe_then_interruptible([this, ox, &ops] {
    logger().debug(
      "do_osd_ops_execute: object {} all operations successful",
      ox->get_target());
    // check for full
    if ((ox->delta_stats.num_bytes > 0 ||
      ox->delta_stats.num_objects > 0) &&
      get_pgpool().info.has_flag(pg_pool_t::FLAG_FULL)) {
      const auto& m = ox->get_message();
      if (m.get_reqid().name.is_mds() ||   // FIXME: ignore MDS for now
        m.has_flag(CEPH_OSD_FLAG_FULL_FORCE)) {
        logger().info(" full, but proceeding due to FULL_FORCE or MDS");
      } else if (m.has_flag(CEPH_OSD_FLAG_FULL_TRY)) {
        // they tried, they failed.
        logger().info(" full, replying to FULL_TRY op");
        if (get_pgpool().info.has_flag(pg_pool_t::FLAG_FULL_QUOTA))
          return interruptor::make_ready_future<OpsExecuter::rep_op_fut_tuple>(
            seastar::now(),
            OpsExecuter::osd_op_ierrorator::future<>(
              crimson::ct_error::edquot::make()));
        else
          return interruptor::make_ready_future<OpsExecuter::rep_op_fut_tuple>(
            seastar::now(),
            OpsExecuter::osd_op_ierrorator::future<>(
              crimson::ct_error::enospc::make()));
      } else {
        // drop request
        logger().info(" full, dropping request (bad client)");
        return interruptor::make_ready_future<OpsExecuter::rep_op_fut_tuple>(
          seastar::now(),
          OpsExecuter::osd_op_ierrorator::future<>(
            crimson::ct_error::eagain::make()));
      }
    }
    return std::move(*ox).flush_changes_n_do_ops_effects(
      ops,
      snap_mapper,
      osdriver,
      [this] (auto&& txn,
              auto&& obc,
              auto&& osd_op_p,
              auto&& log_entries) {
	logger().debug(
	  "do_osd_ops_execute: object {} submitting txn",
	  obc->get_oid());
	return submit_transaction(
          std::move(obc),
          std::move(txn),
          std::move(osd_op_p),
          std::move(log_entries));
    });
  }).safe_then_unpack_interruptible(
    [success_func=std::move(success_func), error_func_ptr, rollbacker, this, failure_func_ptr]
    (auto submitted_fut, auto _all_completed_fut) mutable {

    auto all_completed_fut = _all_completed_fut.safe_then_interruptible_tuple(
      std::move(success_func),
      crimson::ct_error::object_corrupted::handle(
      [rollbacker, this] (const std::error_code& e) mutable {
      // this is a path for EIO. it's special because we want to fix the obejct
      // and try again. that is, the layer above `PG::do_osd_ops` is supposed to
      // restart the execution.
      return rollbacker.rollback_obc_if_modified(e).then_interruptible(
      [obc=rollbacker.get_obc(), this] {
        return repair_object(obc->obs.oi.soid,
                             obc->obs.oi.version
        ).then_interruptible([] {
          return do_osd_ops_iertr::future<Ret>{crimson::ct_error::eagain::make()};
        });
      });
    }), OpsExecuter::osd_op_errorator::all_same_way(
        [this, rollbacker, failure_func_ptr]
        (const std::error_code& e) mutable {
          // handle non-fatal errors only
          ceph_assert(e.value() == EDQUOT ||
                      e.value() == ENOSPC ||
                      e.value() == EAGAIN);
          return rollbacker.rollback_obc_if_modified(e).then_interruptible(
          [this, e, failure_func_ptr] {
            // no need to record error log
            return (*failure_func_ptr)(e , shard_services.get_tid(), false);
          });
    }));

    return PG::do_osd_ops_iertr::make_ready_future<pg_rep_op_fut_t<Ret>>(
      std::move(submitted_fut),
      std::move(all_completed_fut)
    );
  }, OpsExecuter::osd_op_errorator::all_same_way(
    [this, error_func_ptr, rollbacker, failure_func_ptr]
    (const std::error_code& e) mutable {

    ceph_tid_t rep_tid = shard_services.get_tid();
    return rollbacker.rollback_obc_if_modified(e).then_interruptible(
    [error_func_ptr, e, rep_tid, failure_func_ptr] {
      // record error log
      return (*error_func_ptr)(e, rep_tid).then(
      [failure_func_ptr, e, rep_tid] {
        return PG::do_osd_ops_iertr::make_ready_future<pg_rep_op_fut_t<Ret>>(
          std::move(seastar::now()),
          std::move((*failure_func_ptr)(e, rep_tid, true))
        );
      });
    });
  }));
}

seastar::future<> PG::submit_error_log(
  Ref<MOSDOp> m,
  const OpInfo &op_info,
  ObjectContextRef obc,
  const std::error_code e,
  ceph_tid_t rep_tid)
{
  logger().debug("{}: {} rep_tid: {} error: {}",
                 __func__, *m, rep_tid, e);
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
  log_entry_version[rep_tid] = projected_last_update = log_entries.rbegin()->version;
  ceph::os::Transaction t;
  peering_state.merge_new_log_entries(
    log_entries, t, peering_state.get_pg_trim_to(),
    peering_state.get_min_last_complete_ondisk());

  return seastar::do_with(log_entries, t, set<pg_shard_t>{},
    [this, rep_tid](auto& log_entries, auto& t,auto& waiting_on) mutable {
    return seastar::do_for_each(get_acting_recovery_backfill(),
      [this, log_entries, t=std::move(t), waiting_on, rep_tid]
      (auto& i) mutable {
      pg_shard_t peer(i);
      if (peer == pg_whoami) {
        return seastar::now();
      }
      ceph_assert(peering_state.get_peer_missing().count(peer));
      ceph_assert(peering_state.has_peer_info(peer));
      auto log_m = crimson::make_message<MOSDPGUpdateLogMissing>(
                   log_entries,
                   spg_t(peering_state.get_info().pgid.pgid, i.shard),
                   pg_whoami.shard,
                   get_osdmap_epoch(),
                   get_last_peering_reset(),
                   rep_tid,
                   peering_state.get_pg_trim_to(),
                   peering_state.get_min_last_complete_ondisk());
      waiting_on.insert(peer);
      logger().debug("submit_error_log: sending log"
        "missing_request (rep_tid: {} entries: {})"
        " to osd {}", rep_tid, log_entries, peer.osd);
      return shard_services.send_to_osd(peer.osd,
                                        std::move(log_m),
                                        get_osdmap_epoch());
    }).then([this, waiting_on, t=std::move(t), rep_tid] () mutable {
      waiting_on.insert(pg_whoami);
      logger().debug("submit_error_log: inserting rep_tid {}", rep_tid);
      log_entry_update_waiting_on.insert(
        std::make_pair(rep_tid,
                       log_update_t{std::move(waiting_on)}));
      return shard_services.get_store().do_transaction(
        get_collection_ref(), std::move(t)
      ).then([this] {
        peering_state.update_trim_to();
        return seastar::now();
      });
    });
  });
}

PG::do_osd_ops_iertr::future<PG::pg_rep_op_fut_t<MURef<MOSDOpReply>>>
PG::do_osd_ops(
  Ref<MOSDOp> m,
  crimson::net::ConnectionRef conn,
  ObjectContextRef obc,
  const OpInfo &op_info,
  const SnapContext& snapc)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  return do_osd_ops_execute<MURef<MOSDOpReply>>(
    seastar::make_lw_shared<OpsExecuter>(
      Ref<PG>{this}, obc, op_info, *m, conn, snapc),
    obc,
    op_info,
    m,
    m->ops,
    // success_func
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
      } else if (result < 0 && (m->ops.empty() ?
        0 : m->ops.back().op.flags & CEPH_OSD_OP_FLAG_FAILOK)) {
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
    // failure_func
    [m, &op_info, obc, this]
    (const std::error_code& e, const ceph_tid_t& rep_tid, bool record_error) {
    logger().error("do_osd_ops_execute::failure_func {} got error: {} record_error: {}",
                    *m, e, record_error);
    epoch_t epoch = get_osdmap_epoch();
    auto last_complete = peering_state.get_info().last_complete;
    auto fut = seastar::now();
    if (record_error && !peering_state.pg_has_reset_since(epoch) && op_info.may_write()) {
      logger().debug("do_osd_ops_execute::failure_func finding rep_tid {}",
                      rep_tid);
      ceph_assert(log_entry_version.contains(rep_tid));
      auto it = log_entry_update_waiting_on.find(rep_tid);
      ceph_assert(it != log_entry_update_waiting_on.end());
      auto it2 = it->second.waiting_on.find(pg_whoami);
      ceph_assert(it2 != it->second.waiting_on.end());
      it->second.waiting_on.erase(it2);
      if (it->second.waiting_on.empty()) {
        log_entry_update_waiting_on.erase(it);
        peering_state.complete_write(log_entry_version[rep_tid], last_complete);
        log_entry_version.erase(rep_tid);
        logger().debug("do_osd_ops_execute::failure_func write complete,"
                        " erasing rep_tid {}", rep_tid);

      } else {
        fut = it->second.all_committed.get_shared_future().then(
          [this, last_complete, rep_tid] {
          logger().debug("do_osd_ops_execute::failure_func awaited {}", rep_tid);
          peering_state.complete_write(log_entry_version[rep_tid], last_complete);
          ceph_assert(!log_entry_update_waiting_on.contains(rep_tid));
          return seastar::now();
        });
      }
    }
    return fut.then([this, m, e] {
      return log_reply(m, e);
    });
  });
}

PG::do_osd_ops_iertr::future<MURef<MOSDOpReply>>
PG::log_reply(
  Ref<MOSDOp> m,
  const std::error_code& e)
{
  auto reply = crimson::make_message<MOSDOpReply>(
    m.get(), -e.value(), get_osdmap_epoch(), 0, false);
  if (m->ops.empty() ? 0 :
    m->ops.back().op.flags & CEPH_OSD_OP_FLAG_FAILOK) {
      reply->set_result(0);
    }
  // For all ops except for CMPEXT, the correct error value is encoded
  // in e.value(). For CMPEXT, osdop.rval has the actual error value.
  if (e.value() == ct_error::cmp_fail_error_value) {
    assert(!m->ops.empty());
    for (auto &osdop : m->ops) {
      if (osdop.rval < 0) {
        reply->set_result(osdop.rval);
        break;
      }
    }
  }
  reply->set_enoent_reply_versions(
    peering_state.get_info().last_update,
    peering_state.get_info().last_user_version);
  reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
  return do_osd_ops_iertr::make_ready_future<MURef<MOSDOpReply>>(
    std::move(reply));
}

PG::do_osd_ops_iertr::future<PG::pg_rep_op_fut_t<>>
PG::do_osd_ops(
  ObjectContextRef obc,
  std::vector<OSDOp>& ops,
  const OpInfo &op_info,
  const do_osd_ops_params_t &&msg_params)
{
  // This overload is generally used for internal client requests,
  // use an empty SnapContext.
  return seastar::do_with(
    std::move(msg_params),
    [=, this, &ops, &op_info](auto &msg_params) {
    return do_osd_ops_execute<void>(
      seastar::make_lw_shared<OpsExecuter>(
        Ref<PG>{this},
        obc,
        op_info,
        msg_params,
        msg_params.get_connection(),
        SnapContext{}
      ),
      obc,
      op_info,
      Ref<MOSDOp>(),
      ops,
      // success_func
      [] {
        return do_osd_ops_iertr::now();
      },
      // failure_func
      [] (const std::error_code& e, const ceph_tid_t& rep_tid, bool record_error) {
        return do_osd_ops_iertr::now();
      });
  });
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

PG::load_obc_iertr::future<>
PG::with_locked_obc(const hobject_t &hobj,
                    const OpInfo &op_info,
                    with_obc_func_t &&f)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  const hobject_t oid = get_oid(hobj);
  auto wrapper = [f=std::move(f), this](auto head, auto obc) {
    check_blocklisted_obc_watchers(obc);
    return f(head, obc);
  };
  switch (get_lock_type(op_info)) {
  case RWState::RWREAD:
      return obc_loader.with_obc<RWState::RWREAD>(oid, std::move(wrapper));
  case RWState::RWWRITE:
      return obc_loader.with_obc<RWState::RWWRITE>(oid, std::move(wrapper));
  case RWState::RWEXCL:
      return obc_loader.with_obc<RWState::RWEXCL>(oid, std::move(wrapper));
  default:
    ceph_abort();
  };
}

PG::interruptible_future<> PG::handle_rep_op(Ref<MOSDRepOp> req)
{
  if (__builtin_expect(stopping, false)) {
    return seastar::make_exception_future<>(
	crimson::common::system_shutdown_exception());
  }

  logger().debug("{}: {}", __func__, *req);
  if (can_discard_replica_op(*req)) {
    return seastar::now();
  }

  ceph::os::Transaction txn;
  auto encoded_txn = req->get_data().cbegin();
  decode(txn, encoded_txn);
  auto p = req->logbl.cbegin();
  std::vector<pg_log_entry_t> log_entries;
  decode(log_entries, p);
  log_operation(std::move(log_entries),
                req->pg_trim_to,
                req->version,
                req->min_last_complete_ondisk,
                !txn.empty(),
                txn,
                false);
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

void PG::log_operation(
  std::vector<pg_log_entry_t>&& logv,
  const eversion_t &trim_to,
  const eversion_t &roll_forward_to,
  const eversion_t &min_last_complete_ondisk,
  bool transaction_applied,
  ObjectStore::Transaction &txn,
  bool async) {
  logger().debug("{}", __func__);
  if (is_primary()) {
    ceph_assert(trim_to <= peering_state.get_last_update_ondisk());
  }
  /* TODO: when we add snap mapper and projected log support,
   * we'll likely want to update them here.
   *
   * See src/osd/PrimaryLogPG.h:log_operation for how classic
   * handles these cases.
   */
#if 0
  if (transaction_applied) {
    //TODO:
    //update_snap_map(logv, t);
  }
  auto last = logv.rbegin();
  if (is_primary() && last != logv.rend()) {
    projected_log.skip_can_rollback_to_to_head();
    projected_log.trim(cct, last->version, nullptr, nullptr, nullptr);
  }
#endif
  if (!is_primary()) { // && !is_ec_pg()
    replica_clear_repop_obc(logv);
  }
  if (!logv.empty()) {
    scrubber.on_log_update(logv.rbegin()->version);
  }
  peering_state.append_log(std::move(logv),
                           trim_to,
                           roll_forward_to,
                           min_last_complete_ondisk,
                           txn,
                           !txn.empty(),
                           false);
}

void PG::replica_clear_repop_obc(
  const std::vector<pg_log_entry_t> &logv) {
    logger().debug("{} clearing {} entries", __func__, logv.size());
    for (auto &&e: logv) {
      logger().debug(" {} get_object_boundary(from): {} "
                     " head version(to): {}",
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
  crimson::net::ConnectionRef conn)
{
  if (__builtin_expect(stopping, false)) {
    return seastar::make_exception_future<>(
      crimson::common::system_shutdown_exception());
  }

  ceph_assert(m->get_type() == MSG_OSD_PG_UPDATE_LOG_MISSING);
  ObjectStore::Transaction t;
  std::optional<eversion_t> op_trim_to, op_roll_forward_to;
  if (m->pg_trim_to != eversion_t())
    op_trim_to = m->pg_trim_to;
  if (m->pg_roll_forward_to != eversion_t())
    op_roll_forward_to = m->pg_roll_forward_to;
  logger().debug("op_trim_to = {}, op_roll_forward_to = {}",
    op_trim_to, op_roll_forward_to);

  peering_state.append_log_entries_update_missing(
    m->entries, t, op_trim_to, op_roll_forward_to);

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
      log_entry_version.erase(m->get_tid());
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
    client_request_orderer.requeue(shard_services, this);
  } else {
    logger().debug("{} {}: dropping requests", *this, __func__);
    client_request_orderer.clear_and_cancel(*this);
  }
  scrubber.on_interval_change();
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

PG::interruptible_future<std::optional<PG::complete_op_t>>
PG::already_complete(const osd_reqid_t& reqid)
{
  eversion_t version;
  version_t user_version;
  int ret;
  std::vector<pg_log_op_return_item_t> op_returns;

  if (peering_state.get_pg_log().get_log().get_request(
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

}
