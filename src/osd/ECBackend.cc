// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ECBackend.h"

#include <iostream>

#include "ECInject.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "common/debug.h"
#include "ECMsgTypes.h"
#include "ECTypes.h"
#include "ECSwitch.h"
#include "ECJournalFlush.h"
#include "ECJournalQueue.h"
#include "common/config_obs.h"

#include "PrimaryLogPG.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

using std::dec;
using std::hex;
using std::less;
using std::list;
using std::make_pair;
using std::map;
using std::pair;
using std::ostream;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using ceph::bufferhash;
using ceph::bufferlist;
using ceph::bufferptr;
using ceph::ErasureCodeInterfaceRef;
using ceph::Formatter;

static ostream &_prefix(std::ostream *_dout, ECBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

static ostream& _prefix(std::ostream *_dout, ECBackend::ECRecoveryBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

struct ECBackend::ECRecoveryBackend::ECRecoveryHandle : public PGBackend::RecoveryHandle {
  list<ECCommon::RecoveryBackend::RecoveryOp> ops;
};

// Flushes may only be deferred by a scrub range or a degraded object for a
// short while; the PG lock is dropped between retries.
static constexpr std::chrono::milliseconds journal_flush_retry_ms{50};
// While segments exist the driver also wakes at this rate: a drain requested
// through the config observer (no PG lock there) or a deferred stripe must
// not wait for the next client write.
static constexpr std::chrono::milliseconds journal_housekeeping_ms{1000};
// An idle PG whose slot objects may be on disk still looks now and then: a
// drain requested meanwhile has to remove them (remove_journal_slots).
static constexpr std::chrono::milliseconds journal_idle_check_ms{10000};

struct ECBackend::JournalState final : md_config_obs_t {
  ECBackend* backend;
  ECWriteJournal::Journal journal;
  // osd_ec_journal_drain is a runtime option; a config observer keeps
  // it off the per-op path (no ConfigProxy lock or key lookup per pump).
  std::atomic<bool> drain_conf{false};
  // osd_ec_journal_max_write_bytes, kept the same way: the largest
  // write across a stripe boundary that the journal takes, as one record
  // per stripe (journal_write). 0: only writes inside one stripe.
  std::atomic<uint64_t> max_cross_conf{0};
  uint64_t max_cross() const {
    return std::min<uint64_t>(max_cross_conf.load(),
                              backend->sinfo.get_stripe_width());
  }
  ECWriteJournal::DrainWaiters shutdown_drain;
  // Stripes are independent: each flush is its own EC write of one stripe,
  // and the journal hands out a different stripe per begin_flush. Reclaiming
  // a sealed segment means flushing every live stripe in it, so doing that
  // one round trip at a time bounds the whole PG's write rate at 1/latency
  // and keeps client writes waiting for room in do_op meanwhile.
  unsigned flushes_in_flight = 0;
  // Objects with flushes in flight. A flush takes no object lock, so
  // recovery checks this before pushing (journal_flush_in_flight).
  std::map<hobject_t, unsigned> flushing;
  unsigned max_flushes; // osd_ec_journal_max_flushes
  // Shards that store every record: this (primary) shard first, then shard 0
  // and the parity shards, m + 1 of them. Under ec_optimizations these are
  // the shards that can become primary, and the ones every journaled write
  // already sends its log entry and OI to.
  std::vector<shard_id_t> holders;
  bool pumping = false;
  // A write the journal cannot take waits in do_op, before it has a version,
  // while the object it touches has journaled stripes: those flush first,
  // complete or not, while other objects' stripes keep gathering records.
  // (A drain or an unhealthy acting set flushes every stripe.)
  //
  // A PG-wide barrier for every unsupported write collapsed the logs: librbd
  // merges adjacent 4 KiB writes to one object (rbd_io_scheduler=simple), and
  // on a saturated cluster each PG saw an 8 KiB write every minute or so.
  std::vector<hobject_t> barrier_objects;
  // Replay after an interval change (journal_on_activate): records read
  // back from this shard's slots and judged live are adopted into their
  // slots and flushed first. Nothing new is admitted until the other
  // holders' listings and the fetches are in (replaying); from then on new
  // writes take the free slots while the backlog drains (replay_pending).
  // Records of objects this shard has to recover first wait here until it
  // has (journal_object_recovered); their slots are held meanwhile.
  bool replaying = false;
  bool replay_pending = false;
  ceph::mono_clock::time_point activated_at;
  std::optional<double> replay_listed_ms; // activation to admission open
  std::optional<double> replay_admit_ms;  // activation to the first record
  std::map<hobject_t, std::vector<ECWriteJournal::Record>> deferred;
  uint64_t replay_scanned = 0;  // records the slots' headers list
  uint64_t replay_adopted = 0;  // ... judged live and flushed again
  uint64_t replay_dead = 0;     // ... already materialized or rolled back
  uint64_t replay_deferred = 0; // ... waiting for the object's recovery
  uint64_t replay_unreadable = 0; // ... whose payload this shard cannot read
  uint64_t replay_absent = 0;   // live per the log, but no holder had it
  uint64_t requeued_records = 0; // writes in flight at an interval change, run again
  uint64_t replay_fetched = 0;  // ... read from another holder instead
  // Live entries of the authoritative log whose record is not in this
  // shard's slots: read from the other holders in turn. Their objects count
  // as dirty meanwhile, so no op of them runs before the record is adopted.
  ECWriteJournal::ReplayFetches fetches;
  std::vector<pg_shard_t> fetch_sources;
  std::vector<ECWriteJournal::Record> fetched;
  // Replay: slot listings requested from the other holders and not in yet.
  // A record only another holder has (this shard missed its commit) is not
  // known until then, so meanwhile no op of the PG runs and nothing
  // flushes: a direct write or a flush would bury it (replay_blocked).
  unsigned header_reads = 0;
  std::set<eversion_t> replay_indexed;  // versions this shard's slots hold
  std::set<eversion_t> replay_fetching; // ... being fetched from a holder
  // Journaled writes in flight: a record sent to every holder, keyed by its
  // sub-write tid, until all of them committed it. Acknowledged in order per
  // object once durable (journal_record_committed).
  struct Inflight {
    OpRequestRef op;
    ObjectContextRef obc;
    hobject_t object;
    osd_reqid_t reqid;
    version_t user_version = 0; // the one the write took (ec_journal_acked)
    // One per record: a write that crosses a stripe boundary has two.
    std::vector<ECWriteJournal::Ticket> tickets;
    unsigned pending = 0; // holders yet to commit
    uint64_t bytes = 0;
    bool done = false;
  };
  std::map<ceph_tid_t, Inflight> inflight;
  std::map<hobject_t, std::deque<ceph_tid_t>> inflight_by_object;
  // Resends of journaled writes whose record is in flight, answered right
  // after the original (journal_hold_resend). A resend of a durable one is
  // answered in do_op at once (journal_find_request); one of a
  // materialized record by the PG log.
  std::map<osd_reqid_t, std::vector<OpRequestRef>> resends;
  uint64_t dup_replies = 0; // resends answered by the journal
  uint64_t dup_held = 0;    // ... of them held for the record's commit
  // Client reads are served from the journal. Until a stripe flushes, its
  // shards hold the bytes from before the journaled writes, so a read lays
  // the stripe's live blocks over what the shards return, or answers from
  // the blocks alone when they cover it (objects_read_async); nothing is
  // flushed for it. PrimaryLogPG asks journal_read_ready() just before it
  // issues a read, and parks one that has to wait in do_op with the
  // object's other ops, not here. Such a read waits for
  //  - a record of the stripes it reads that is still in flight: shown
  //    before every holder has it, it could be gone again after a failover;
  //  - the object's records that replay has yet to list, fetch or judge,
  //    and the flush of stripes replay adopted records of (ahead of
  //    anything else): an adopted record may be on fewer holders than one
  //    acknowledged in this interval;
  //  - a flush of a stripe it reads: one in flight, or one that has to run
  //    and waits for the reads of that stripe to end (flush_wanted).
  // read_barriers: the objects such reads wait for and the range they read,
  // until the kick that requeues them (journal_ready).
  struct ReadBarrier {
    uint64_t offset = 0;
    uint64_t end = 0;
    bool flush = false; // waits for adopted stripes to flush
  };
  std::map<hobject_t, ReadBarrier> read_barriers;
  // A flush and a client read of the same stripe never overlap in time: a
  // read that rebuilds a chunk from parity would combine chunks from before
  // and after the flush. The journal counts the reads in flight per stripe
  // (read_begin/read_end around the shard reads) and hands out no stripe
  // that is being read; a read of a stripe that is being flushed waits
  // (ReadState::flushing). Other stripes of the object are not involved.
  // flush_waits: stripes a flush was passed over for because of such reads;
  // the end of the last one wakes the driver. flush_wanted: those whose
  // flush has to run (reclaim, a drain, replay, a waiting op); new reads of
  // them wait until it has run, or a stream of reads would starve it. Both
  // are rebuilt by every pump.
  std::set<ECWriteJournal::Journal::StripeKey> flush_waits;
  std::set<ECWriteJournal::Journal::StripeKey> flush_wanted;
  // A flush that has to run waits for reads of a stripe this range overlaps.
  bool flush_wanted_in(const hobject_t& object, uint64_t offset,
                       uint64_t length) const {
    const uint64_t width = backend->sinfo.get_stripe_width();
    const auto i = flush_wanted.lower_bound({object, offset / width * width});
    return i != flush_wanted.end() && i->first == object &&
      i->second < offset + length;
  }
  enum class ReadWait { none, commit, replay, flush };
  // What a client read of this range has to wait for now; *adopted: for the
  // flush of stripes replay adopted.
  ReadWait read_wait(const hobject_t& object, uint64_t offset, uint64_t length,
                     bool* adopted = nullptr) const {
    using state_t = ECWriteJournal::ReadState;
    if (replay_blocked(object)) {
      return ReadWait::replay;
    }
    if (length && flush_wanted_in(object, offset, length)) {
      return ReadWait::flush;
    }
    switch (journal.read_state(object, offset, length)) {
    case state_t::clean:
    case state_t::ready:
      break;
    case state_t::in_flight:
      return ReadWait::commit;
    case state_t::flushing:
      return ReadWait::flush;
    case state_t::adopted:
      if (adopted) {
        *adopted = true;
      }
      return ReadWait::replay;
    }
    return ReadWait::none;
  }
  // A plain overwrite the journal does not take runs at once as an ordinary
  // EC write and takes the journaled stripes it overlaps with it
  // (journal_take_for_write): a stripe it overwrites whole loses its records
  // unwritten, and the live blocks of a stripe it writes partly ride in its
  // own transaction. It forces no flush. It waits in do_op only for what it
  // cannot take: a record of its range still in flight (taken then, the
  // write could be acknowledged ahead of it), a flush or another direct
  // write that holds one of its stripes, and the object's records that
  // replay has yet to list, fetch or judge.
  // write_barriers: the objects such writes wait for and the range they
  // write (offset, end), until the kick that requeues them (journal_ready).
  std::map<hobject_t, std::pair<uint64_t, uint64_t>> write_barriers;
  ReadWait write_wait(const hobject_t& object, uint64_t offset,
                      uint64_t length) const {
    using state_t = ECWriteJournal::ReadState;
    if (replay_blocked(object)) {
      return ReadWait::replay;
    }
    switch (journal.write_state(object, offset, length)) {
    case state_t::in_flight:
      return ReadWait::commit;
    case state_t::flushing:
      return ReadWait::flush;
    default:
      return ReadWait::none;
    }
  }
  // The PG's slot objects may exist on the holders: every append writes one,
  // and an earlier run may have left them. A drain that has flushed
  // everything removes them (remove_journal_slots), so a drained pool holds
  // nothing a release without the journal would not know: its scrub lists
  // them and flags every such PG inconsistent. removing_slots holds the
  // interval (same_interval_since) of a removal in flight.
  bool slots_on_disk = true;
  std::optional<epoch_t> removing_slots;
  uint64_t slot_removals = 0;
  uint64_t open_segment = 0;
  ceph::mono_clock::time_point wake_at{};
  uint64_t admitted = 0;
  uint64_t acked = 0;
  uint64_t full_stripes = 0;
  uint64_t partial_stripes = 0;
  uint64_t pressure_flushes = 0; // incomplete stripes flushed to reclaim a segment
  uint64_t adopted_flushes = 0;  // ... because replay adopted their records
  uint64_t drain_flushes = 0;    // ... on drain
  uint64_t barrier_flushes = 0;  // ... ahead of an unsupported mutation
  uint64_t barrier_events = 0;   // unsupported writes that had to wait
  uint64_t read_barrier_events = 0; // client reads that had to wait
  uint64_t read_waits_commit = 0;   // ... for a record in flight
  uint64_t read_waits_replay = 0;   // ... for replay, or an adopted stripe's flush
  uint64_t read_waits_flush = 0;    // ... for a flush that has to run
  uint64_t read_overlays = 0;       // reads with journal blocks over shard data
  uint64_t reads_from_journal = 0;  // reads answered without reading a shard
  uint64_t deferred_flushes = 0; // candidates passed over (scrub, degraded)
  uint64_t flush_read_skips = 0; // ... for client reads of the stripe in flight
  uint64_t flushed_bytes = 0;
  uint64_t pressure_events = 0;  // journal writes that waited for room

  // Admission (journal_admit): every eligible write is journaled, except a
  // whole aligned stripe that has no records and a write to a stripe that
  // has none while the log is full.
  uint64_t admit_live = 0;    // journaled: the stripe already had records
  uint64_t admit_new = 0;     // journaled: the stripe had none
  uint64_t admit_cross = 0;   // ... of both: across a stripe boundary, two records
  uint64_t bypass_full = 0;   // direct EC write: whole stripe, nothing to read
  uint64_t bypass_room = 0;   // ... log full, stripe clean: no wait
  uint64_t bypass_digest = 0; // ... the object carried a data digest
  // Direct writes beside journaled stripes (journal_take_for_write).
  uint64_t direct_takes = 0;    // direct writes that took journaled stripes
  uint64_t taken_stripes = 0;   // ... the stripes they took
  uint64_t dropped_stripes = 0; // ... of them covered whole: nothing written
  uint64_t write_waits_commit = 0; // direct writes that waited for a record in flight
  uint64_t write_waits_flush = 0;  // ... for a flush or direct write of a stripe
  uint64_t write_waits_replay = 0; // ... for replay's records of the object

  // A flush that must wait for the flusher, or a drain, is woken by the
  // append commit that makes it possible; everything else by the timer.
  bool draining() const {
    return replaying || drain_conf.load() || shutdown_drain.requested();
  }
  bool replay_blocked(const hobject_t& object) const {
    return header_reads > 0 || fetches.blocked(object) ||
      deferred.contains(object);
  }
  bool flushable() const {
    return journal.flush_ready() || barrier_ready() || reads_ready() ||
      journal.adopted_ready() ||
      (draining() ? journal.partial_ready() : journal.reclaim_ready());
  }
  bool reads_ready() const {
    for (const auto& [object, barrier] : read_barriers) {
      if (barrier.flush && journal.object_ready(object, barrier.offset,
            barrier.end - barrier.offset)) {
        return true;
      }
    }
    return false;
  }
  bool barrier_ready() const {
    for (const auto& object : barrier_objects) {
      if (journal.object_ready(object)) {
        return true;
      }
    }
    return false;
  }

  struct Timer final : common::intrusive_timer::callback_t {
    JournalState* state;
    explicit Timer(JournalState* state) : state(state) {}
    void lock() override { state->backend->parent->pg_lock(); }
    void unlock() override { state->backend->parent->pg_unlock(); }
    void add_ref() override { state->backend->parent->pg_add_ref(); }
    void dec_ref() override { state->backend->parent->pg_dec_ref(); }
    void invoke() override { state->backend->pump_journal(); }
  } timer{this};

  JournalState(ECBackend* backend, ghobject_t prefix,
               ECWriteJournal::Limits limits,
               unsigned max_flushes, std::vector<shard_id_t> holders)
    : backend(backend), journal(std::move(prefix), limits),
      max_flushes(max_flushes), holders(std::move(holders)) {
    drain_conf = backend->cct->_conf.get_val<bool>("osd_ec_journal_drain");
    max_cross_conf = uint64_t(backend->cct->_conf.get_val<Option::size_t>(
      "osd_ec_journal_max_write_bytes"));
    backend->cct->_conf.add_observer(this);
  }
  ~JournalState() override {
    // Blocks until an in-flight handle_conf_change (atomic store only) ends.
    backend->cct->_conf.remove_observer(this);
  }
  std::vector<std::string> get_tracked_keys() const noexcept override {
    return {"osd_ec_journal_drain", "osd_ec_journal_max_write_bytes"};
  }
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>&) override {
    // Config thread, no PG lock: only the atomic. pump_journal picks it up
    // on the next submit, commit or timer wake.
    drain_conf = conf.get_val<bool>("osd_ec_journal_drain");
    max_cross_conf = uint64_t(conf.get_val<Option::size_t>(
      "osd_ec_journal_max_write_bytes"));
  }
};

// The RMW pipeline's roll-forward kick, deferred: only on a pool enrolled in
// the EC journal (the constructor creates this for no other pool, which
// keeps the immediate kick). finish_rmw asks schedule()
// instead of sending the dummy op at once; the PG timer then calls
// RMWPipeline::kick_rollforward under the PG lock, which sends it only if
// the pipeline is still idle and still has something to roll forward. The
// delay is a runtime option kept in an atomic by a config observer, like the
// journal's, so the per-op path takes no ConfigProxy lock.
struct ECBackend::RollforwardKick final : md_config_obs_t,
                                          common::intrusive_timer::callback_t {
  ECBackend* backend;
  std::atomic<uint64_t> delay_ms{0};

  explicit RollforwardKick(ECBackend* backend) : backend(backend) {
    delay_ms = backend->cct->_conf.get_val<uint64_t>(
      "osd_ec_rollforward_delay_ms");
    backend->cct->_conf.add_observer(this);
  }
  ~RollforwardKick() override {
    backend->cct->_conf.remove_observer(this);
  }
  std::vector<std::string> get_tracked_keys() const noexcept override {
    return {"osd_ec_rollforward_delay_ms"};
  }
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>&) override {
    delay_ms = conf.get_val<uint64_t>("osd_ec_rollforward_delay_ms");
  }

  void lock() override { backend->parent->pg_lock(); }
  void unlock() override { backend->parent->pg_unlock(); }
  void add_ref() override { backend->parent->pg_add_ref(); }
  void dec_ref() override { backend->parent->pg_dec_ref(); }
  void invoke() override { backend->rmw_pipeline.kick_rollforward(); }

  // Under the PG lock, from finish_rmw: true if the kick is left to the
  // timer. Each request starts the delay again, so the kick goes out only
  // once the pipeline has been quiet for the whole delay, and it sends the
  // committed_to of that moment. (A timer left running from the first
  // request fired in the gaps of a steady load: on a journaled pool, whose
  // pipeline carries only the writes the journal does not take, after about
  // every second one of them, and the next write's acknowledgement queued
  // behind the kick's commit on every shard.)
  bool schedule() {
    const uint64_t delay = delay_ms.load(std::memory_order_relaxed);
    if (delay == 0) {
      return false;
    }
    auto& timer = backend->get_parent()->get_pg_timer();
    if (is_scheduled()) {
      timer.cancel(*this);
    }
    timer.schedule_after(*this, std::chrono::milliseconds(delay));
    return true;
  }
  void cancel() {
    if (is_scheduled()) {
      backend->get_parent()->get_pg_timer().cancel(*this);
    }
  }
};

ECBackend::ECBackend(
  PGBackend::Listener *pg,
  CephContext *cct,
  ErasureCodeInterfaceRef ec_impl,
  uint64_t stripe_width,
  ECSwitch *s,
  ECExtentCache::LRU &ec_extent_cache_lru)
  : ECCommon(*pg->get_dpp()),
    parent(pg), cct(cct), switcher(s),
#ifdef WITH_CRIMSON
    read_pipeline(cct, ec_impl, this->sinfo, get_parent()->get_eclistener(), *this),
#else
    read_pipeline(cct, ec_impl, this->sinfo, get_parent()->get_eclistener()),
#endif
    rmw_pipeline(cct, ec_impl, this->sinfo, get_parent()->get_eclistener(),
                 *this, ec_extent_cache_lru),
    recovery_backend(cct, switcher->coll, ec_impl, this->sinfo, read_pipeline,
                      get_parent(), this),
    ec_impl(ec_impl),
    sinfo(ec_impl, &(get_parent()->get_pool()), stripe_width) {

  /* EC makes some assumptions about how the plugin organises the *data* shards:
   * - The chunk size is constant for a particular profile.
   * - A stripe consists of k chunks.
   */
  ceph_assert((ec_impl->get_data_chunk_count() *
    ec_impl->get_chunk_size(stripe_width)) == stripe_width);
  // All enrollment settings are startup-only; no lookups on disabled writes.
  init_journal();
  // Only a pool enrolled in the journal defers its roll-forward kick: there
  // the pipeline carries just the writes the journal does not take and is
  // idle between them. Every other pool keeps the immediate kick, exactly
  // as without this code (RMWPipeline::finish_rmw with no defer_rollforward).
  if (journal_state) {
    rollforward_kick = std::make_unique<RollforwardKick>(this);
    rmw_pipeline.defer_rollforward = [this] {
      return rollforward_kick->schedule();
    };
  }
}

PGBackend::RecoveryHandle *ECBackend::open_recovery_op() {
  return static_cast<PGBackend::RecoveryHandle*>(
    recovery_backend.open_recovery_op());
}

ECBackend::ECRecoveryBackend::ECRecoveryHandle *ECBackend::ECRecoveryBackend::open_recovery_op() {
  return new ECRecoveryHandle;
}

void ECBackend::handle_recovery_push(
  const PushOp &op,
  RecoveryMessages *m,
  bool is_repair) {
  if (get_parent()->pg_is_remote_backfilling()) {
    get_parent()->pg_add_local_num_bytes(op.data.length());
    get_parent()->pg_add_num_bytes(op.data.length() * sinfo.get_k());
    dout(10) << __func__ << " " << op.soid
             << " add new actual data by " << op.data.length()
             << " add new num_bytes by " << op.data.length() * sinfo.get_k()
             << dendl;
  }

  recovery_backend.handle_recovery_push(op, m, is_repair);

  if (op.after_progress.data_complete &&
    op.after_progress.omap_complete &&
    !(get_parent()->pgb_is_primary()) &&
    get_parent()->pg_is_remote_backfilling()) {
    struct stat st;
    int r = switcher->store->stat(switcher->ch, ghobject_t(
                                    op.soid, ghobject_t::NO_GEN,
                                    get_parent()->whoami_shard().shard), &st);
    if (r == 0) {
      get_parent()->pg_sub_local_num_bytes(st.st_size);
      // XXX: This can be way overestimated for small objects
      get_parent()->pg_sub_num_bytes(st.st_size * sinfo.get_k());
      dout(10) << __func__ << " " << op.soid
               << " sub actual data by " << st.st_size
               << " sub num_bytes by " << st.st_size * sinfo.get_k()
               << dendl;
    }
  }
}

void ECBackend::ECRecoveryBackend::maybe_load_obc(
  const std::map<std::string, ceph::bufferlist, std::less<>>& raw_attrs,
  RecoveryOp &op)
{
  if (!op.obc) {
    // attrs only reference the origin bufferlist (decode from
    // ECSubReadReply message) whose size is much greater than attrs
    // in recovery. If obc cache it (get_obc maybe cache the attr),
    // this causes the whole origin bufferlist would not be free
    // until obc is evicted from obc cache. So rebuild the
    // bufferlist before cache it.
    for (map<string, bufferlist>::iterator it = op.xattrs.begin();
         it != op.xattrs.end();
         ++it) {
      it->second.rebuild();
    }
    // Need to remove ECUtil::get_hinfo_key() since it should not leak out
    // of the backend (see bug #12983)
    map<string, bufferlist, less<>> sanitized_attrs(op.xattrs);
    sanitized_attrs.erase(ECUtil::get_hinfo_key());
    op.obc = get_parent()->get_obc(op.hoid, sanitized_attrs);
    ceph_assert(op.obc);
    op.recovery_info.size = op.obc->obs.oi.size;
    op.recovery_info.oi = op.obc->obs.oi;
  }
}

struct SendPushReplies : public Context {
  PGBackend::Listener *l;
  epoch_t epoch;
  std::map<int, MOSDPGPushReply*> replies;

  SendPushReplies(
    PGBackend::Listener *l,
    epoch_t epoch,
    std::map<int, MOSDPGPushReply*> &in) : l(l), epoch(epoch) {
    replies.swap(in);
  }

  void finish(int) override {
    std::vector<std::pair<int, Message*>> messages;
    messages.reserve(replies.size());
    for (auto & reply : replies) {
      messages.push_back(reply);
    }
    if (!messages.empty()) {
      l->send_message_osd_cluster(messages, epoch);
    }
    replies.clear();
  }

  ~SendPushReplies() override {
    for (auto & [_, reply] : replies) {
      reply->put();
    }
    replies.clear();
  }
};

void ECBackend::ECRecoveryBackend::commit_txn_send_replies(
  ceph::os::Transaction &&txn,
  std::map<int, MOSDPGPushReply*> replies) {
  txn.register_on_complete(
    get_parent()->bless_context(
      new SendPushReplies(
        get_parent(),
        get_osdmap_epoch(),
        replies)));
  get_parent()->queue_transaction(std::move(txn));
}

void ECBackend::run_recovery_op(
  PGBackend::RecoveryHandle *_h,
  int priority) {
  ceph_assert(_h);
  auto &h = static_cast<ECBackend::ECRecoveryBackend::ECRecoveryHandle&>(*_h);
  recovery_backend.run_recovery_op(h, priority);
  switcher->send_recovery_deletes(priority, h.deletes);
  delete _h;
}

void ECBackend::ECRecoveryBackend::run_recovery_op(
  ECBackend::ECRecoveryBackend::ECRecoveryHandle &h,
  int priority) {
  RecoveryMessages m;
  for (list<RecoveryOp>::iterator i = h.ops.begin();
       i != h.ops.end();
       ++i) {
    dout(10) << __func__ << ": starting " << *i << dendl;
    ceph_assert(!recovery_ops.count(i->hoid));
    RecoveryOp &op = recovery_ops.insert(make_pair(i->hoid, *i)).first->second;
    continue_recovery_op(op, &m);
  }
  dispatch_recovery_messages(m, priority);
}

int ECBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc,
  PGBackend::RecoveryHandle *_h) {
  auto *h = static_cast<ECBackend::ECRecoveryBackend::ECRecoveryHandle*>(_h);
  h->ops.push_back(recovery_backend.recover_object(hoid, v, head, obc));
  return 0;
}

bool ECBackend::can_handle_while_inactive(
  OpRequestRef _op) {
  return false;
}

bool ECBackend::_handle_message(
  OpRequestRef _op) {
  dout(10) << __func__ << ": " << *_op->get_req() << dendl;
  int priority = _op->get_req()->get_priority();
  switch (_op->get_req()->get_type()) {
  case MSG_OSD_EC_WRITE: {
    // NOTE: this is non-const because handle_sub_write modifies the embedded
    // ObjectStore::Transaction in place (and then std::move's it).  It does
    // not conflict with ECSubWrite's operator<<.
    MOSDECSubOpWrite *op = static_cast<MOSDECSubOpWrite*>(
      _op->get_nonconst_req());
    parent->maybe_preempt_replica_scrub(op->op.soid);
    handle_sub_write(op->op.from, _op, op->op, _op->pg_trace,
                     *get_parent()->get_eclistener());
    return true;
  }
  case MSG_OSD_EC_WRITE_REPLY: {
    const MOSDECSubOpWriteReply *op = static_cast<const MOSDECSubOpWriteReply*>(
      _op->get_req());
    handle_sub_write_reply(op->op.from, op->op, _op->pg_trace);
    return true;
  }
  case MSG_OSD_EC_READ: {
    auto op = _op->get_req<MOSDECSubOpRead>();
    MOSDECSubOpReadReply *reply = new MOSDECSubOpReadReply;
    reply->pgid = get_parent()->primary_spg_t();
    reply->map_epoch = switcher->get_osdmap_epoch();
    reply->min_epoch = get_parent()->get_interval_start_epoch();
    handle_sub_read(op->op.from, op->op, &(reply->op), _op->pg_trace);
    reply->trace = _op->pg_trace;
    reply->pgid.reset_shard(op->op.from.shard);
    get_parent()->send_message_osd_cluster(
      reply, _op->get_req()->get_connection());
    return true;
  }
  case MSG_OSD_EC_READ_REPLY: {
    // NOTE: this is non-const because handle_sub_read_reply steals resulting
    // buffers.  It does not conflict with ECSubReadReply operator<<.
    MOSDECSubOpReadReply *op = static_cast<MOSDECSubOpReadReply*>(
      _op->get_nonconst_req());
    handle_sub_read_reply(op->op.from, op->op, _op->pg_trace);
    // dispatch_recovery_messages() in the case of recovery_reads
    // is called via the `on_complete` callback
    return true;
  }
  case MSG_OSD_PG_PUSH: {
    auto op = _op->get_req<MOSDPGPush>();
    RecoveryMessages rm;
    for (vector<PushOp>::const_iterator i = op->pushes.begin();
         i != op->pushes.end();
         ++i) {
      handle_recovery_push(*i, &rm, op->is_repair);
    }
    recovery_backend.dispatch_recovery_messages(rm, priority);
    return true;
  }
  case MSG_OSD_PG_PUSH_REPLY: {
    const MOSDPGPushReply *op = static_cast<const MOSDPGPushReply*>(
      _op->get_req());
    RecoveryMessages rm;
    for (vector<PushReplyOp>::const_iterator i = op->replies.begin();
         i != op->replies.end();
         ++i) {
      recovery_backend.handle_recovery_push_reply(*i, op->from, &rm);
    }
    recovery_backend.dispatch_recovery_messages(rm, priority);
    return true;
  }
  default:
    return false;
  }
  return false;
}

struct SubWriteCommitted : public Context {
  ECBackend *pg;
  OpRequestRef msg;
  ceph_tid_t tid;
  eversion_t version;
  eversion_t last_complete;
  const ZTracer::Trace trace;

  SubWriteCommitted(
    ECBackend *pg,
    OpRequestRef msg,
    ceph_tid_t tid,
    eversion_t version,
    eversion_t last_complete,
    const ZTracer::Trace &trace)
    : pg(pg), msg(msg), tid(tid),
      version(version), last_complete(last_complete), trace(trace) {}

  void finish(int) override {
    if (msg)
      msg->mark_event("sub_op_committed");
    pg->sub_write_committed(tid, version, last_complete, trace);
  }
};

void ECBackend::sub_write_committed(
  ceph_tid_t tid, eversion_t version, eversion_t last_complete,
  const ZTracer::Trace &trace) {
  if (get_parent()->pgb_is_primary()) {
    ECSubWriteReply reply;
    reply.tid = tid;
    reply.last_complete = last_complete;
    reply.committed = true;
    reply.applied = true;
    reply.from = get_parent()->whoami_shard();
    handle_sub_write_reply(
      get_parent()->whoami_shard(),
      reply, trace);
  } else {
    get_parent()->update_last_complete_ondisk(last_complete);
    MOSDECSubOpWriteReply *r = new MOSDECSubOpWriteReply;
    r->pgid = get_parent()->primary_spg_t();
    r->map_epoch = switcher->get_osdmap_epoch();
    r->min_epoch = get_parent()->get_interval_start_epoch();
    r->op.tid = tid;
    r->op.last_complete = last_complete;
    r->op.committed = true;
    r->op.applied = true;
    r->op.from = get_parent()->whoami_shard();
    r->set_priority(CEPH_MSG_PRIO_HIGH);
    r->trace = trace;
    r->trace.event("sending sub op commit");
    get_parent()->send_message_osd_cluster(
      get_parent()->primary_shard().osd, r, switcher->get_osdmap_epoch());
  }
}

void ECBackend::handle_sub_write(
  pg_shard_t from,
  OpRequestRef msg,
  ECSubWrite &op,
  const ZTracer::Trace &trace,
  ECListener &) {
  if (msg) {
    msg->mark_event("sub_op_started");
  }
  trace.event("handle_sub_write");

  if (cct->_conf->bluestore_debug_inject_read_err &&
    ECInject::test_write_error3(op.soid)) {
    ceph_abort_msg("Error inject - OSD down");
  }
  if (op.unlogged) {
    // An EC journal record (ECBackend::journal_write): slot data and
    // header, and on a rotation the slot reset. Nothing of the PG log, info,
    // stats or object state; the commit reply tells the primary this copy
    // is durable.
    ObjectStore::Transaction localt;
    localt.register_on_commit(
      get_parent()->bless_context(
        new SubWriteCommitted(
          this, msg, op.tid, eversion_t(),
          get_parent()->get_info().last_complete, trace)));
    if (!get_parent()->pg_is_undersized() &&
        get_parent()->whoami_shard().shard >= sinfo.get_k()) {
      op.t.set_fadvise_flag(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);
    }
    vector<ObjectStore::Transaction> tls;
    tls.reserve(2);
    tls.push_back(std::move(op.t));
    tls.push_back(std::move(localt));
    get_parent()->queue_transactions(tls, msg);
    return;
  }
  if (!get_parent()->pgb_is_primary())
    get_parent()->update_stats(op.stats);
  ObjectStore::Transaction localt;
  if (!op.temp_added.empty()) {
    switcher->add_temp_objs(op.temp_added);
  }
  if (op.backfill_or_async_recovery) {
    for (set<hobject_t>::iterator i = op.temp_removed.begin();
         i != op.temp_removed.end();
         ++i) {
      dout(10) << __func__ << ": removing object " << *i
	       << " since we won't get the transaction" << dendl;
      localt.remove(
        switcher->coll,
        ghobject_t(
          *i,
          ghobject_t::NO_GEN,
          get_parent()->whoami_shard().shard));
    }
  }
  switcher->clear_temp_objs(op.temp_removed);
  dout(30) << __func__ << " missing before " <<
    get_parent()->get_log().get_missing().get_items() << dendl;

  // Update EC omap journal on primary-capable non-acting-primary shards from log entries.
  if (get_parent()->get_pool().supports_omap() &&
      !get_parent()->pgb_is_primary() &&
      !sinfo.is_nonprimary_shard(get_parent()->whoami_shard().shard)) {
    for (auto &&e: op.log_entries) {
      if (e.is_delete() || e.is_lost_delete() || e.is_replace() || (e.is_clone() && !e.soid.is_snap())) {
        if (!op.backfill_or_async_recovery) {
          ec_omap_journal.append_delete(e.soid, e.version.version, e.is_lost_delete());
          dout(20) << __func__ << " appending delete to journal: "
                   << (e.is_clone() ? "clone" : "delete")
                   << " " << e.soid << " version=" << e.version.version
                   << " lost_delete=" << e.is_lost_delete() << dendl;
        } else {
          dout(20) << __func__ << " skipping journal append_delete during backfill/recovery: "
                   << (e.is_clone() ? "clone" : "delete")
                   << " " << e.soid << " version=" << e.version.version
                   << " lost_delete=" << e.is_lost_delete() << dendl;
        }
      }
    }
  }

  // flag set to true during async recovery
  bool async = false;
  pg_missing_tracker_t pmissing = get_parent()->get_local_missing();
  if (pmissing.is_missing(op.soid)) {
    async = true;
    dout(30) << __func__ << " is_missing " <<
      pmissing.is_missing(op.soid) << dendl;
    for (auto &&e: op.log_entries) {
      dout(30) << " add_next_event entry " << e << dendl;
      get_parent()->add_local_next_event(e);
      dout(30) << " entry is_delete " << e.is_delete() << dendl;
    }
  }

  dout(20) << __func__ << " log_operation: "
           << "log_entries.size=" << op.log_entries.size()
           << " updated_hit_set_history=" << (op.updated_hit_set_history ? "present" : "none")
           << " trim_to=" << op.trim_to
           << " roll_forward_to=" << op.pg_committed_to
           << " pg_committed_to=" << op.pg_committed_to
           << " transaction_applied=" << !op.backfill_or_async_recovery
           << " async=" << async
           << dendl;
  get_parent()->log_operation(
    std::move(op.log_entries),
    op.updated_hit_set_history,
    op.trim_to,
    op.pg_committed_to,
    op.pg_committed_to,
    !op.backfill_or_async_recovery,
    localt,
    async);

  if (!get_parent()->pg_is_undersized() &&
    get_parent()->whoami_shard().shard >= sinfo.get_k())
    op.t.set_fadvise_flag(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  localt.register_on_commit(
    get_parent()->bless_context(
      new SubWriteCommitted(
        this, msg, op.tid,
        op.at_version,
        get_parent()->get_info().last_complete, trace)));
  vector<ObjectStore::Transaction> tls;
  tls.reserve(2);
  tls.push_back(std::move(op.t));
  tls.push_back(std::move(localt));
  dout(20) << __func__ << " queue_transactions=";
  Formatter *f = Formatter::create("json");
  f->open_array_section("tls");
  for (ObjectStore::Transaction t: tls) {
    f->open_object_section("t");
    t.dump(f);
    f->close_section();
  }
  f->close_section();
  f->flush(*_dout);
  delete f;
  *_dout << dendl;
  get_parent()->queue_transactions(tls, msg);
  dout(30) << __func__ << " missing after" << get_parent()->get_log().
                                                            get_missing().
                                                            get_items() << dendl
  ;
  if (op.at_version != eversion_t()) {
    // dummy rollforward transaction doesn't get at_version (and doesn't advance it)
    get_parent()->op_applied(op.at_version);
  }
}

void ECBackend::handle_sub_read(
  pg_shard_t from,
  const ECSubRead &op,
  ECSubReadReply *reply,
  const ZTracer::Trace &trace) {
  trace.event("handle sub read");
  shard_id_t shard = get_parent()->whoami_shard().shard;
  for (auto &&[hoid, to_read]: op.to_read) {
    int r = 0;
    for (auto &&[offset, len, flags]: to_read) {
      bufferlist bl;
      r = switcher->store->read(
        switcher->ch,
        ghobject_t(hoid, ghobject_t::NO_GEN, shard),
        offset, len, bl, flags); // Allow EIO return

      if (r < 0) {
        // if we are doing fast reads, it's possible for one of the shard
        // reads to cross paths with another update and get a (harmless)
        // ENOENT.  Suppress the message to the cluster log in that case.
        if (r == -ENOENT && get_parent()->get_pool().fast_read) {
          dout(5) << __func__ << ": Error " << r
		  << " reading " << hoid << ", fast read, probably ok"
		  << dendl;
        } else {
          get_parent()->clog_error() << "Error " << r
            << " reading object " << hoid;
          dout(5) << __func__ << ": Error " << r
		  << " reading " << hoid << dendl;
        }
        goto error;
      } else {
        dout(20) << __func__ << " read request=" << len << " r=" << r << " len="
          << bl.length() << dendl;
        reply->buffers_read[hoid].push_back(make_pair(offset, bl));
      }
    }
    continue;
  error:
    // Do NOT check osd_read_eio_on_bad_digest here.  We need to report
    // the state of our chunk in case other chunks could substitute.
    reply->buffers_read.erase(hoid);
    reply->errors[hoid] = r;
  }
  for (set<hobject_t>::iterator i = op.attrs_to_read.begin();
       i != op.attrs_to_read.end();
       ++i) {
    dout(10) << __func__ << ": fulfilling attr request on "
	     << *i << dendl;
    if (reply->errors.contains(*i))
      continue;
    int r;
    if (ECWriteJournal::is_slot_object(*i)) {
      // EC journal replay: a slot's record headers live in its OMAP, which
      // the read pipeline does not carry. The replaying primary lists the
      // other holders' slots through this request and gets the headers back
      // as the slot's attributes (journal_on_activate).
      ceph::bufferlist omap_header;
      std::map<std::string, ceph::bufferlist> headers;
      r = switcher->store->omap_get(
        switcher->ch, ghobject_t(*i, ghobject_t::NO_GEN, shard),
        &omap_header, &headers);
      if (r >= 0) {
        auto& attrs = reply->attrs_read[*i];
        for (auto& [key, value] : headers) {
          attrs[key] = std::move(value);
        }
      }
    } else {
      r = switcher->store->getattrs(
        switcher->ch,
        ghobject_t(
          *i, ghobject_t::NO_GEN, shard),
        reply->attrs_read[*i]);
    }
    if (r < 0) {
      // If we read error, we should not return the attrs too.
      reply->attrs_read.erase(*i);
      reply->buffers_read.erase(*i);
      reply->errors[*i] = r;
      continue;
    }

    if (!op.omap_headers_to_read.contains(*i)) {
      continue;
    }

    if (!get_parent()->get_pool().supports_omap()) {
      continue;
    }

    // During recovery, if the OI attribute isn't accessible, we speculatively
    // say that we want to read omap data.
    // Now we have access to the OI attribute so we can determine if omap data
    // should be read or not.
    auto oi_iter = reply->attrs_read[*i].find(OI_ATTR);
    if (oi_iter == reply->attrs_read[*i].end()) {
      continue;
    }

    try {
      object_info_t oi;
      auto p = oi_iter->second.cbegin();
      decode(oi, p);
      if (!oi.is_omap()) {
        reply->omaps_complete[*i] = true;
        dout(20) << __func__ << ": object " << *i
                 << " has no omap flag, skipping omap read" << dendl;
      } else {
        dout(20) << __func__ << ": object " << *i
                 << " has omap flag set" << dendl;
      }
    } catch (ceph::buffer::error& e) {
      lgeneric_derr(cct) << __func__ << ": failed to decode OI for " << *i
              << ": " << e.what() << dendl;
      get_parent()->clog_warn() << "corrupt object info attribute for " << *i;
      reply->errors[*i] = -EIO;
      continue;
    }
  }

  if (get_parent()->get_pool().supports_omap()) {
    for (set<hobject_t>::iterator i = op.omap_headers_to_read.begin();
         i != op.omap_headers_to_read.end();
         ++i) {
      dout(20) << __func__ << ": fulfilling omap header request on "
               << *i << dendl;
      if (reply->errors.contains(*i)) {
        continue;
      }

      // Skip if attrs already proved omap processing is complete.
      if (reply->omaps_complete.contains(*i) && reply->omaps_complete[*i]) {
        dout(20) << __func__ << ": skipping omap header read for " << *i
                 << " (no omap flag)" << dendl;
        continue;
      }

      int r = omap_get_header(
        switcher->ch,
        ghobject_t(*i, ghobject_t::NO_GEN, shard),
        &reply->omap_headers_read[*i], false,
        switcher->store);
      if (r < 0) {
        reply->attrs_read.erase(*i);
        reply->omap_headers_read.erase(*i);
        reply->buffers_read.erase(*i);
        reply->errors[*i] = r;
      }
    }

    for (auto const& [hoid, read_from] : op.omap_read_from) {
      auto const&[start_key, max_bytes] = read_from;
      dout(20) << __func__ << ": fulfilling omap read request on " << hoid
              << " from key " << start_key << dendl;

      if (reply->errors.contains(hoid))
        continue;

      // Skip if attrs already proved omap processing is complete.
      if (reply->omaps_complete.contains(hoid) && reply->omaps_complete[hoid]) {
        dout(20) << __func__ << ": skipping omap entries read for " << hoid
                 << " (no omap flag)" << dendl;
        continue;
      }

      std::map<std::string, ceph::buffer::list> current_batch;
      reply->omaps_complete[hoid] = false;

      uint64_t available = max_bytes;
      const auto result = omap_iterate(
        switcher->ch,
        ghobject_t(hoid, ghobject_t::NO_GEN, shard),
        ObjectStore::omap_iter_seek_t{
        .seek_position = start_key,
        .seek_type = ObjectStore::omap_iter_seek_t::UPPER_BOUND
        },
        [max_entries=cct->_conf->osd_recovery_max_omap_entries_per_chunk, &available, &current_batch]
        (std::string_view key, std::string_view value) {
          const auto num_new_bytes = key.size() + value.size();
          if (auto cur_num_entries = current_batch.size(); cur_num_entries > 0) {
            if (max_entries > 0 && cur_num_entries >= max_entries) {
              return ObjectStore::omap_iter_ret_t::STOP;
            }
            if (num_new_bytes >= available) {
              return ObjectStore::omap_iter_ret_t::STOP;
            }
          }
          bufferlist val_bl;
          val_bl.append(value);
          current_batch.insert(make_pair(key, val_bl));
          available -= std::min<uint64_t>(available, num_new_bytes);
          return ObjectStore::omap_iter_ret_t::NEXT;
        }, switcher->store);

      if (result < 0) {
        reply->attrs_read.erase(hoid);
        reply->omap_headers_read.erase(hoid);
        reply->buffers_read.erase(hoid);
        reply->errors[hoid] = result;
        current_batch.clear();
      } else if (result == 0) {
        reply->omaps_complete[hoid] = true;
      }
      reply->omap_entries_read[hoid] = std::move(current_batch);
    }
  }

  reply->from = get_parent()->whoami_shard();
  reply->tid = op.tid;
}

void ECBackend::handle_sub_read_n_reply(
  pg_shard_t from,
  ECSubRead &op,
  const ZTracer::Trace &trace)
{
  ECSubReadReply reply;
  handle_sub_read(from, op, &reply, trace);
  handle_sub_read_reply(from, reply, trace);
}

void ECBackend::handle_sub_write_reply(
  pg_shard_t from,
  const ECSubWriteReply &ec_write_reply_op,
  const ZTracer::Trace &trace) {
  if (journal_state && journal_state->inflight.contains(ec_write_reply_op.tid)) {
    // A copy of a journal record committed on a holder (journal_write).
    if (ec_write_reply_op.committed) {
      trace.event("journal record committed");
      journal_record_committed(from, ec_write_reply_op.tid);
    }
    return;
  }
  RMWPipeline::OpRef &op = rmw_pipeline.tid_to_op_map.at(ec_write_reply_op.tid);
  if (ec_write_reply_op.committed) {
    trace.event("sub write committed");
    ceph_assert(op->pending_commits > 0);
    op->pending_commits--;
    if (from != get_parent()->whoami_shard()) {
      get_parent()->update_peer_last_complete_ondisk(
        from, ec_write_reply_op.last_complete);
    }
  }

  if (cct->_conf->bluestore_debug_inject_read_err &&
    (op->pending_commits == 1) &&
    ECInject::test_write_error2(op->hoid)) {
    std::string cmd =
      "{ \"prefix\": \"osd down\", \"ids\": [\"" + std::to_string(
        get_parent()->whoami()) + "\"] }";
    vector<std::string> vcmd{cmd};
    dout(0) << __func__ << " Error inject - marking OSD down" << dendl;
    get_parent()->start_mon_command(std::move(vcmd), {}, nullptr, nullptr, nullptr);
  }

  if (op->pending_commits == 0) {
    rmw_pipeline.try_finish_rmw();
  }
}

void ECBackend::handle_sub_read_reply(
    pg_shard_t from,
    ECSubReadReply &op,
    const ZTracer::Trace &trace) {
  trace.event("ec sub read reply");
  dout(10) << __func__ << ": reply " << op << dendl;
  map<ceph_tid_t, ReadOp>::iterator iter = read_pipeline.tid_to_read_map.
                                                         find(op.tid);
  if (iter == read_pipeline.tid_to_read_map.end()) {
    //canceled
    dout(20) << __func__ << ": dropped " << op << dendl;
    return;
  }
  ReadOp &rop = iter->second;
  if (cct->_conf->bluestore_debug_inject_read_err) {
    for (auto i = op.buffers_read.begin();
         i != op.buffers_read.end();
         ++i) {
      if (ECInject::test_read_error0(
        ghobject_t(i->first, ghobject_t::NO_GEN, op.from.shard))) {
        dout(0) << __func__ << " Error inject - EIO error for shard "
                << op.from.shard << dendl;
        op.buffers_read.erase(i->first);
        op.attrs_read.erase(i->first);
        op.errors[i->first] = -EIO;
        rop.debug_log.emplace_back(ECUtil::INJECT_EIO, op.from);
      }
    }
  }
  for (auto &&[hoid, offset_buffer_map]: op.buffers_read) {
    ceph_assert(!op.errors.contains(hoid));
    // If attribute error we better not have sent a buffer
    if (!rop.to_read.contains(hoid)) {
      rop.debug_log.emplace_back(ECUtil::CANCELLED, op.from);

      // We canceled this read! @see filter_read_op
      dout(20) << __func__ << " to_read skipping" << dendl;
      continue;
    }

    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }

    auto &buffers_read = rop.complete.at(hoid).buffers_read;
    for (auto &&[offset, buffer_list]: offset_buffer_map) {
      buffers_read.insert_in_shard(from.shard, offset, buffer_list);
    }
    rop.debug_log.emplace_back(ECUtil::READ_DONE, op.from, buffers_read);

    // zero length reads may need to be zero padded during recovery
    if (!buffers_read.contains_shard(from.shard)) {
      rop.complete.at(hoid).zero_length_reads.insert(from.shard);
    }
  }
  for (auto &&[hoid, req]: rop.to_read) {
    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }
    auto &complete = rop.complete.at(hoid);
    if (!req.shard_reads.contains(from.shard)) {
      continue;
    }
    const shard_read_t &read = req.shard_reads.at(from.shard);
    if (!complete.errors.contains(from)) {
      dout(20) << __func__ <<" read:" << read << dendl;
      complete.processed_read_requests[from.shard].union_of(read.extents);
    }
  }
  for (auto &&[hoid, attr]: op.attrs_read) {
    ceph_assert(!op.errors.contains(hoid));
    // if read error better not have sent an attribute
    if (!rop.to_read.contains(hoid)) {
      // We canceled this read! @see filter_read_op
      dout(20) << __func__ << " to_read skipping" << dendl;
      continue;
    }
    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }
    rop.complete.at(hoid).attrs.emplace();
    (*(rop.complete.at(hoid).attrs)).swap(attr);
  }
  if (get_parent()->get_pool().supports_omap()) {
    for (auto &&[hoid, header]: op.omap_headers_read) {
      ceph_assert(!op.errors.contains(hoid));
      // if read error better not have sent an attribute
      if (!rop.to_read.contains(hoid)) {
        // We canceled this read! @see filter_read_op
        dout(20) << __func__ << " to_read skipping" << dendl;
        continue;
      }
      if (!rop.complete.contains(hoid)) {
        rop.complete.emplace(hoid, &sinfo);
      }
      rop.complete.at(hoid).omap_header.emplace();
      (*(rop.complete.at(hoid).omap_header)).swap(header);
    }
    for (auto &&[hoid, entries]: op.omap_entries_read) {
      ceph_assert(!op.errors.contains(hoid));
      // if read error better not have sent any entries
      if (!rop.to_read.contains(hoid)) {
        // We canceled this read! @see filter_read_op
        dout(20) << __func__ << " to_read skipping" << dendl;
        continue;
      }
      if (!rop.complete.contains(hoid)) {
        rop.complete.emplace(hoid, &sinfo);
      }
      rop.complete.at(hoid).omap_entries.emplace();
      (*(rop.complete.at(hoid).omap_entries)).swap(entries);
    }
    for (auto &&[hoid, omap_complete]: op.omaps_complete) {
      ceph_assert(!op.errors.contains(hoid));
      // if read error better not have sent any entries
      if (!rop.to_read.contains(hoid)) {
        // We canceled this read! @see filter_read_op
        dout(20) << __func__ << " to_read skipping" << dendl;
        continue;
      }
      if (!rop.complete.contains(hoid)) {
        rop.complete.emplace(hoid, &sinfo);
      }
      rop.complete.at(hoid).omap_complete = omap_complete;
    }
  }
  for (auto &&[hoid, err]: op.errors) {
    if (!rop.complete.contains(hoid)) {
      rop.complete.emplace(hoid, &sinfo);
    }
    auto &complete = rop.complete.at(hoid);
    complete.errors.emplace(from, err);
    rop.debug_log.emplace_back(ECUtil::ERROR, op.from, complete.buffers_read);
    complete.buffers_read.erase_shard(from.shard);
    complete.processed_read_requests.erase(from.shard);
    // If there was an error for non-zero data on this shard, then we must also
    // ignore all zeros, or minimum_to_decode may conclude that it has enough
    // shards available.
    rop.to_read.at(hoid).zeros_for_decode.erase(from.shard);
    dout(20) << __func__ << " shard=" << from << " error=" << err << dendl;
  }

  map<pg_shard_t, set<ceph_tid_t>>::iterator siter =
    read_pipeline.shard_to_read_map.find(from);
  ceph_assert(siter != read_pipeline.shard_to_read_map.end());
  ceph_assert(siter->second.count(op.tid));
  siter->second.erase(op.tid);

  ceph_assert(rop.in_progress.count(from));
  rop.in_progress.erase(from);
  unsigned is_complete = 0;
  bool need_resend = false;
  bool all_sub_reads_done = rop.in_progress.empty();
  // For redundant reads check for completion as each shard comes in,
  // or in a non-recovery read check for completion once all the shards read.
  if (rop.do_redundant_reads || all_sub_reads_done) {
    for (auto &&[oid, read_result]: rop.complete) {
      shard_id_set have;
      read_result.processed_read_requests.populate_shard_id_set(have);
      shard_id_set dummy_minimum;
      shard_id_set want_to_read;
      rop.to_read.at(oid).shard_want_to_read.
          populate_shard_id_set(want_to_read);

      dout(20) << __func__ << " read_result: " << read_result << dendl;
      // If all reads are done, we can safely assume that zero buffers can
      // be applied.
      if (all_sub_reads_done) {
        rop.to_read.at(oid).zeros_for_decode.populate_shard_id_set(have);
      }

      int err = -EIO; // If attributes needed but not read.
      const auto& to_read = rop.to_read.at(oid);
      const auto& complete = rop.complete.at(oid);
      bool attrs_satisfied = !to_read.want_attrs || complete.attrs;

      bool omap_satisfied = true;
      if (get_parent()->get_pool().supports_omap()
          && (to_read.want_omap_header || to_read.want_omap_keys)) {
        for (const auto &[shard_id, shard_read] : to_read.shard_reads) {
          if (shard_read.omap_source) {
            if (complete.errors.contains(shard_read.pg_shard)) {
              omap_satisfied = false;
            }
            break;
          }
        }
      }

      if (attrs_satisfied && omap_satisfied) {
        err = ec_impl->minimum_to_decode(want_to_read, have, dummy_minimum);
      }

      if (err) {
        dout(20) << __func__ << " minimum_to_decode failed" << dendl;
        if (all_sub_reads_done) {
          // If we don't have enough copies, try other pg_shard_ts if available.
          // During recovery there may be multiple osds with copies of the same shard,
          // so getting EIO from one may result in multiple passes through this code path.

          rop.debug_log.emplace_back(ECUtil::REQUEST_MISSING, op.from);
          int r = read_pipeline.send_all_remaining_reads(oid, rop);
          if (r == 0 && !rop.do_redundant_reads) {
            // We found that new reads are required to do a decode.
            need_resend = true;
            continue;
          }
          // else insufficient shards are available, keep the errors.

          // Couldn't read any additional shards so handle as completed with errors
          // We don't want to confuse clients / RBD with objectstore error
          // values in particular ENOENT.  We may have different error returns
          // from different shards, so we'll return minimum_to_decode() error
          // (usually EIO) to reader.  It is likely an error here is due to a
          // damaged pg.
          rop.complete.at(oid).r = err;
          ++is_complete;
        }
      }

      if (!err) {
        ceph_assert(rop.complete.at(oid).r == 0);
        if (!rop.complete.at(oid).errors.empty()) {
          if (cct->_conf->osd_read_ec_check_for_errors) {
            rop.debug_log.emplace_back(ECUtil::COMPLETE_ERROR, op.from);
            dout(10) << __func__ << ": Not ignoring errors, use one shard" << dendl;
            err = rop.complete.at(oid).errors.begin()->second;
            rop.complete.at(oid).r = err;
          } else {
            get_parent()->clog_warn() << "Error(s) ignored for "
              << iter->first << " enough copies available";
            dout(10) << __func__ << " Error(s) ignored for " << iter->first
		     << " enough copies available" << dendl;
            rop.debug_log.emplace_back(ECUtil::ERROR_CLEAR, op.from);
            rop.complete.at(oid).errors.clear();
          }
        }
        // avoid re-read for completed object as we may send remaining reads for
        // uncompleted objects
        rop.to_read.at(oid).shard_reads.clear();
        rop.to_read.at(oid).want_attrs = false;
        rop.to_read.at(oid).want_omap_header = false;
        rop.to_read.at(oid).want_omap_keys = false;
        ++is_complete;
      }
    }
  }
  if (need_resend) {
    read_pipeline.do_read_op(rop);
  } else if (rop.in_progress.empty() ||
             is_complete == rop.complete.size()) {
    dout(20) << __func__ << " Complete: " << rop << dendl;
    rop.trace.event("ec read complete");
    rop.debug_log.emplace_back(ECUtil::COMPLETE, op.from);

    /* If do_redundant_reads is set then there might be some in progress
     * reads remaining.  We need to make sure that these non-read shards
     * do not get padded. If there was no in progress read, then the zero
     * padding is allowed to stay.
     */
    for (auto pg_shard : rop.in_progress) {
      for (auto &&[oid, read] : rop.to_read) {
        read.zeros_for_decode.erase(pg_shard.shard);
      }
    }
    read_pipeline.complete_read_op(std::move(rop));
  } else {
    dout(10) << __func__ << " readop not complete: " << rop << dendl;
  }
}

void ECBackend::check_recovery_sources(const OSDMapRef &osdmap) {
  struct FinishReadOp : public GenContext<ThreadPool::TPHandle&> {
    ECCommon::ReadPipeline &read_pipeline;
    ceph_tid_t tid;

    FinishReadOp(ECCommon::ReadPipeline &read_pipeline, ceph_tid_t tid)
      : read_pipeline(read_pipeline), tid(tid) {}

    void finish(ThreadPool::TPHandle &) override {
      auto ropiter = read_pipeline.tid_to_read_map.find(tid);
      ceph_assert(ropiter != read_pipeline.tid_to_read_map.end());
      read_pipeline.complete_read_op(std::move(ropiter->second));
    }
  };
  read_pipeline.check_recovery_sources(
    osdmap,
    [this](const hobject_t &obj) {
      recovery_backend.recovery_ops.erase(obj);
    },
    [this](const ReadOp &op) {
      get_parent()->schedule_recovery_work(
        get_parent()->bless_unlocked_gencontext(
          new FinishReadOp(read_pipeline, op.tid)),
        1);
    });
}

void ECBackend::on_change() {
  ec_omap_journal.clear_all();
  reset_journal();
  // A kick pending from the old interval has nothing to send: the pipeline
  // forgets committed_to below, and the next interval's first write rolls
  // every shard forward itself.
  if (rollforward_kick) {
    rollforward_kick->cancel();
  }
  rmw_pipeline.on_change();
  read_pipeline.on_change();
  rmw_pipeline.on_change2();
  clear_recovery_state();
}

void ECBackend::clear_recovery_state() {
  recovery_backend.recovery_ops.clear();
}

void ECBackend::dump_recovery_info(Formatter *f) const {
  dump_journal(f);
  f->open_array_section("recovery_ops");
  for (map<hobject_t, RecoveryBackend::RecoveryOp>::const_iterator i =
         recovery_backend.recovery_ops.begin();
       i != recovery_backend.recovery_ops.end();
       ++i) {
    f->open_object_section("op");
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("read_ops");
  for (map<ceph_tid_t, ReadOp>::const_iterator i = read_pipeline.tid_to_read_map
                                                                .begin();
       i != read_pipeline.tid_to_read_map.end();
       ++i) {
    f->open_object_section("read_op");
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

struct ECClassicalOp : ECCommon::RMWPipeline::Op {
  PGTransactionUPtr t;

  void generate_transactions(
    ceph::ErasureCodeInterfaceRef &ec_impl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    map<hobject_t, ECUtil::shard_extent_map_t> *written,
    shard_id_map<ObjectStore::Transaction> *transactions,
    DoutPrefixProvider *dpp,
    const OSDMapRef &osdmap,
    bool& first_write_in_interval,
    ECOmapJournal &ec_omap_journal) final {
    ceph_assert(t);
    ECTransaction::generate_transactions(
      t.get(),
      plan,
      ec_impl,
      pgid,
      sinfo,
      remote_shard_extent_map,
      log_entries,
      written,
      transactions,
      &temp_added,
      &temp_cleared,
      dpp,
      osdmap,
      first_write_in_interval,
      ec_omap_journal,
      pipeline->get_parent()->get_log());
  }

  bool skip_transaction(
      std::set<shard_id_t> &pending_roll_forward,
      shard_id_t shard,
      ceph::os::Transaction &transaction) final {
    if (transaction.empty()) {
      return true;
    }
    pending_roll_forward.insert(shard);
    return false;
  }
};

// Removes the journal's slot objects from the holders once a drain has
// flushed everything (ECBackend::remove_journal_slots): an unlogged op, no
// log entry or version, and only the holders get a (non-empty) transaction.
struct ECJournalSlotRemoval final : ECCommon::RMWPipeline::Op {
  std::map<shard_id_t, std::vector<ghobject_t>> slots; // per holder

  void generate_transactions(
    ceph::ErasureCodeInterfaceRef &ec_impl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    map<hobject_t, ECUtil::shard_extent_map_t> *written,
    shard_id_map<ObjectStore::Transaction> *transactions,
    DoutPrefixProvider *dpp,
    const OSDMapRef &osdmap,
    bool& first_write_in_interval,
    ECOmapJournal &ec_omap_journal) final {
    for (const auto& [holder, objects] : slots) {
      if (!transactions->contains(holder)) {
        continue; // not in the acting set now: keeps its slots until next time
      }
      auto& t = transactions->at(holder);
      for (const auto& object : objects) {
        t.remove(coll_t(spg_t(pgid, holder)), object);
      }
    }
  }

  bool skip_transaction(
      std::set<shard_id_t> &pending_roll_forward,
      shard_id_t shard,
      ceph::os::Transaction &transaction) final {
    return transaction.empty();
  }
};

ECBackend::~ECBackend() = default;

void ECBackend::init_journal()
{
  if (journal_state ||
      !cct->_conf.get_val<bool>("osd_ec_journal_enable") ||
      cct->_conf.get_val<int64_t>("osd_ec_journal_pool") !=
        get_parent()->get_info().pgid.pgid.pool()) {
    return;
  }
  const auto limits = ECWriteJournal::read_limits(cct->_conf,
    sinfo.get_stripe_width());
  if (!get_parent()->get_pool().allows_ecoptimizations() ||
      !ECWriteJournal::supported_geometry(sinfo.get_k(), sinfo.get_m(),
                                          sinfo.get_chunk_size()) ||
      !limits.valid()) {
    // Every PG instance of the pool (primary or not) gets here; one error
    // line per OSD process is enough.
    static std::atomic<bool> refused_once{false};
    if (!refused_once.exchange(true)) {
      derr << "EC journal POC enrollment refused for pool "
           << get_parent()->get_info().pgid.pgid.pool()
           << ": requires optimized 4+3/8+3/12+3 with 4096-byte chunks and "
           << "a valid osd_ec_journal_max_bytes" << dendl;
    } else {
      dout(10) << "EC journal POC enrollment refused (see earlier error)" << dendl;
    }
    return;
  }
  const auto pgid = get_parent()->get_info().pgid.pgid;
  const auto whoami = get_parent()->whoami_shard().shard;
  // Slot objects ecj.<n> live in the PG-local internal namespace, which
  // clients cannot address and listing (scrub, backfill, pgls) skips. They
  // are not temp objects: an interval change or restart leaves them, and the
  // next segment in a slot removes the old contents first.
  const hobject_t anchor(object_t("ecj"), "", CEPH_NOSNAP, pgid.ps(),
                         pgid.pool(), std::string(hobject_t::INTERNAL_PG_LOCAL_NS));
  ghobject_t prefix(anchor, ghobject_t::NO_GEN, whoami);
  // Holders: this shard (a primary is always shard 0 or parity), then shard
  // 0 and the parity shards in order: m + 1 in all.
  const size_t copies = sinfo.get_m() + 1;
  std::vector<shard_id_t> holders;
  auto add_holder = [&](shard_id_t shard) {
    if (holders.size() < copies &&
        std::find(holders.begin(), holders.end(), shard) == holders.end()) {
      holders.push_back(shard);
    }
  };
  add_holder(whoami);
  add_holder(shard_id_t(0));
  for (const auto shard : sinfo.get_parity_shards()) {
    add_holder(shard);
  }
  journal_state = std::make_unique<JournalState>(this, std::move(prefix), limits,
    std::max<unsigned>(1,
      cct->_conf.get_val<uint64_t>("osd_ec_journal_max_flushes")),
    std::move(holders));
  dout(0) << "EC write journal POC enabled for " << pgid
          << "; " << journal_state->holders.size()
          << " copies of each record" << dendl;
}

void ECBackend::schedule_journal(std::chrono::milliseconds delay)
{
  auto& state = *journal_state;
  const auto when = ceph::mono_clock::now() + delay;
  if (state.timer.is_scheduled()) {
    if (state.wake_at <= when) {
      return;
    }
    get_parent()->get_pg_timer().cancel(state.timer);
  }
  // Timer invocation acquires the PG lock, so even zero delay cannot reenter
  // RMWPipeline::finish_rmw recursively. Commit callbacks preempt the timeout.
  state.wake_at = when;
  get_parent()->get_pg_timer().schedule_after(state.timer, delay);
}

void ECBackend::reset_journal()
{
  if (!journal_state) {
    return;
  }
  auto& s = *journal_state;
  if (s.timer.is_scheduled()) {
    get_parent()->get_pg_timer().cancel(s.timer);
  }
  // Commits and flushes registered before the change never run. Every
  // acknowledged record is on the slots of every holder, so the journal
  // drops its memory; the next primary replays from the slots.
  if (s.journal.dirty()) {
    dout(10) << __func__ << " dropping " << s.journal.live_blocks()
             << " live blocks; the next primary replays them" << dendl;
  }
  s.journal.on_reset();
  s.replaying = false;
  s.replay_pending = false;
  s.replay_listed_ms.reset();
  s.replay_admit_ms.reset();
  s.deferred.clear();
  s.fetches.clear();
  s.fetch_sources.clear();
  s.fetched.clear();
  // Blessed callbacks registered before the change never run, so the
  // in-flight flushes they would have completed are dropped with them.
  s.flushes_in_flight = 0;
  s.flushing.clear();
  // Its op is dropped with the pipeline's; the slots may still be there.
  s.removing_slots.reset();
  s.open_segment = 0;
  s.barrier_objects.clear();
  // The reads and writes that waited are parked in do_op; PrimaryLogPG
  // requeues them after the interval change.
  s.read_barriers.clear();
  s.write_barriers.clear();
  s.flush_waits.clear();
  s.flush_wanted.clear();
  s.shutdown_drain.finish(-ECANCELED);
  // Records in flight never get all their commits in this interval, and a
  // client resends a write only when the primary changes, so the writes go
  // back to the PG to run again in the next interval if this shard stays
  // primary (ec_journal_requeue), oldest first; a copy a holder did commit
  // is replayed and superseded by the write's new record. Otherwise the
  // next primary replays whatever copies committed and the client resends.
  if (!s.inflight.empty()) {
    std::list<OpRequestRef> requeue;
    for (auto& [tid, flight] : s.inflight) {
      if (flight.op) {
        requeue.push_back(flight.op);
      }
    }
    s.requeued_records += requeue.size();
    dout(10) << __func__ << " " << requeue.size()
             << " journaled writes in flight at the interval change" << dendl;
    // Resends held for those writes run again behind them and meet the
    // new record of their write (journal_find_request).
    for (auto& [reqid, ops] : s.resends) {
      requeue.insert(requeue.end(), ops.begin(), ops.end());
    }
    s.inflight.clear();
    s.inflight_by_object.clear();
    get_parent()->ec_journal_requeue(std::move(requeue));
  }
  s.resends.clear();
  s.header_reads = 0;
  s.replay_indexed.clear();
  s.replay_fetching.clear();
  // Do not destroy the timer here: its thread may be releasing a reference
  // after cancellation. Its lifetime is the backend's lifetime.
}

void ECBackend::dump_journal(Formatter* f) const
{
  if (!journal_state) {
    return;
  }
  const auto& s = *journal_state;
  f->open_object_section("ec_write_journal_poc");
  f->dump_unsigned("copies", s.holders.size());
  f->open_array_section("holders");
  for (const auto holder : s.holders) {
    f->dump_int("shard", int(holder));
  }
  f->close_section();
  f->dump_unsigned("bytes", s.journal.bytes());
  f->dump_unsigned("payload_bytes", s.journal.payload_bytes());
  f->dump_unsigned("header_bytes", s.journal.bytes() - s.journal.payload_bytes());
  f->dump_unsigned("records", s.journal.records());
  f->dump_unsigned("segments", s.journal.segment_count());
  f->dump_unsigned("live_blocks", s.journal.live_blocks());
  f->dump_unsigned("live_bytes", s.journal.live_bytes());
  f->dump_unsigned("superseded_blocks", s.journal.superseded_blocks());
  f->dump_bool("pressure", s.journal.pressure());
  f->dump_bool("reclaiming", s.journal.reclaim_bound().has_value());
  f->dump_unsigned("flushes_in_flight", s.flushes_in_flight);
  f->dump_unsigned("max_flushes", s.max_flushes);
  f->dump_unsigned("records_in_flight", s.inflight.size());
  f->dump_unsigned("requests", s.journal.request_count());
  f->dump_unsigned("request_window", s.journal.window_size());
  f->dump_unsigned("pinned_segments", s.journal.pinned_segments());
  f->dump_unsigned("dup_replies", s.dup_replies);
  f->dump_unsigned("dup_held", s.dup_held);
  f->dump_unsigned("resends_held", s.resends.size());
  f->dump_unsigned("object_metas", s.journal.object_meta_count());
  f->dump_unsigned("newest_user_version", s.journal.newest_user_version());
  f->dump_unsigned("header_reads", s.header_reads);
  f->dump_unsigned("admitted", s.admitted);
  f->dump_unsigned("acked", s.acked);
  f->dump_unsigned("full_stripes", s.full_stripes);
  f->dump_unsigned("partial_stripes", s.partial_stripes);
  f->dump_unsigned("pressure_flushes", s.pressure_flushes);
  f->dump_unsigned("adopted_flushes", s.adopted_flushes);
  f->dump_unsigned("drain_flushes", s.drain_flushes);
  f->dump_unsigned("barrier_flushes", s.barrier_flushes);
  f->dump_unsigned("barrier_events", s.barrier_events);
  f->dump_unsigned("barrier_objects", s.barrier_objects.size());
  f->dump_unsigned("read_barrier_events", s.read_barrier_events);
  f->dump_unsigned("read_waits_commit", s.read_waits_commit);
  f->dump_unsigned("read_waits_replay", s.read_waits_replay);
  f->dump_unsigned("read_waits_flush", s.read_waits_flush);
  f->dump_unsigned("reads_waiting", s.read_barriers.size());
  f->dump_unsigned("read_overlays", s.read_overlays);
  f->dump_unsigned("reads_from_journal", s.reads_from_journal);
  f->dump_unsigned("deferred_flushes", s.deferred_flushes);
  f->dump_unsigned("flush_read_skips", s.flush_read_skips);
  f->dump_unsigned("stripes_being_read", s.journal.stripes_being_read());
  f->dump_unsigned("flushed_logical_bytes", s.flushed_bytes);
  f->dump_unsigned("pressure_events", s.pressure_events);
  f->dump_unsigned("admit_live", s.admit_live);
  f->dump_unsigned("admit_new", s.admit_new);
  f->dump_unsigned("admit_cross", s.admit_cross);
  f->dump_unsigned("max_write_bytes", s.max_cross());
  f->dump_unsigned("bypass_full", s.bypass_full);
  f->dump_unsigned("bypass_room", s.bypass_room);
  f->dump_unsigned("bypass_digest", s.bypass_digest);
  f->dump_unsigned("direct_takes", s.direct_takes);
  f->dump_unsigned("taken_stripes", s.taken_stripes);
  f->dump_unsigned("dropped_stripes", s.dropped_stripes);
  f->dump_unsigned("write_waits_commit", s.write_waits_commit);
  f->dump_unsigned("write_waits_flush", s.write_waits_flush);
  f->dump_unsigned("write_waits_replay", s.write_waits_replay);
  f->dump_unsigned("writes_waiting", s.write_barriers.size());
  f->dump_bool("replaying", s.replaying);
  f->dump_bool("replay_pending", s.replay_pending);
  f->dump_unsigned("adopted_segments", s.journal.adopted_segments());
  f->dump_unsigned("held_slots", s.journal.held_slots());
  f->dump_float("replay_listed_ms", s.replay_listed_ms.value_or(-1));
  f->dump_float("replay_admit_ms", s.replay_admit_ms.value_or(-1));
  f->dump_unsigned("replay_scanned", s.replay_scanned);
  f->dump_unsigned("replay_adopted", s.replay_adopted);
  f->dump_unsigned("replay_dead", s.replay_dead);
  f->dump_unsigned("replay_deferred", s.replay_deferred);
  f->dump_unsigned("replay_unreadable", s.replay_unreadable);
  f->dump_unsigned("replay_absent", s.replay_absent);
  f->dump_unsigned("requeued_records", s.requeued_records);
  f->dump_unsigned("replay_fetched", s.replay_fetched);
  f->dump_int("replay_error", s.fetches.error());
  f->dump_unsigned("fetches_pending", s.fetches.pending().size());
  f->dump_unsigned("deferred_objects", s.deferred.size());
  f->dump_bool("slots_on_disk", s.slots_on_disk);
  f->dump_bool("removing_slots", s.removing_slots.has_value());
  f->dump_unsigned("slot_removals", s.slot_removals);
  f->close_section();
}

void ECBackend::pump_journal()
{
  auto& s = *journal_state;
  ceph_assert(!s.pumping);
  s.pumping = true;
  const bool healthy = !get_parent()->pg_is_undersized() &&
    get_parent()->get_acting_shards().size() == sinfo.get_k_plus_m() &&
    get_parent()->get_backfill_shards().empty();
  const bool drain = s.draining();
  // A drain, or an acting set that can no longer hold every copy, flushes
  // every stripe; nothing more is appended (journal_admit refuses).
  const bool clean = drain || !healthy;
  // A PG that is only peered (acting set below min_size) must not write at
  // all: its interval cannot go read-write, so the next peering treats every
  // entry written in it as divergent (a later primary picks an older log and
  // shards holding those entries cannot merge it). Records stay in their
  // slots; the activation of a writeable interval replays and flushes them.
  // Nor while replay still lists the other holders' slots: a flush would
  // bury a record not known yet.
  const bool writeable = get_parent()->pg_ec_journal_writeable() &&
    s.header_reads == 0;
  if (clean) {
    s.journal.seal();
  }

  // Writes waiting in do_op for an object's stripes: once they are all
  // flushed the object needs no more isolation (ec_journal_kick below).
  std::erase_if(s.barrier_objects, [this](const hobject_t& object) {
    return !journal_blocked(object);
  });

  // Complete stripes flush whenever the flusher has capacity, without reads.
  // Incomplete ones only when a write the journal cannot take waits for the
  // object (its stripes, first) or a drain for a clean journal (all of
  // them), when the log has to reclaim its oldest segment (only the stripes
  // pinning it), or when replay adopted their records (first those a read
  // waits for): every extra record they gather before that is a read and a
  // rewrite saved. Client reads flush nothing else: they are served from
  // the journal.
  //
  // Each flush is an ordinary logged write of the object, submitted through
  // the PG (ec_journal_flush) like any other op, so torn flushes roll back,
  // lagging shards are recovered and the object's materialized attr records
  // what it covered. This runs from the journal timer only, never inside
  // another op's submission.
  // A stripe passed over for a scrub or a degraded object is retried soon.
  // One whose object waits for a replay fetch or for recovery is not: the
  // fetch or the recovery wakes the driver, and a failed fetch keeps the
  // object blocked until the next interval.
  bool retry = false;
  bool isolating = false; // ahead of a read or write of the stripe's object
  std::optional<uint64_t> reclaim_upto;
  s.flush_waits.clear();
  s.flush_wanted.clear();
  const auto skip = [&](const ECWriteJournal::Journal::StripeKey& key) {
    if (s.replay_blocked(key.first)) {
      return true;
    }
    // Same gating as a client write in do_op: never rewrite a stripe the
    // scrubber is comparing, or an object that is degraded/backfilling.
    if (get_parent()->pg_ec_journal_flush_blocked(key.first)) {
      retry = true;
      return true;
    }
    // Nor a stripe that a client read is reading (see flush_waits): the
    // end of that read wakes the driver. A flush that has to run holds new
    // reads of the stripe back meanwhile.
    if (s.journal.reading(key)) {
      ++s.flush_read_skips;
      s.flush_waits.insert(key);
      if (isolating || s.journal.must_flush(key, clean, reclaim_upto)) {
        s.flush_wanted.insert(key);
      }
      return true;
    }
    return false;
  };
  while (writeable && s.flushes_in_flight < s.max_flushes) {
    reclaim_upto = clean ? std::optional<uint64_t>{} : s.journal.reclaim_bound();
    bool deferred = false;
    std::optional<ECWriteJournal::Stripe> stripe;
    isolating = true;
    for (const auto& [object, barrier] : s.read_barriers) {
      if (stripe) {
        break;
      }
      if (!barrier.flush) {
        continue;
      }
      // Only the stripes replay adopted: the rest of the range is read
      // through the journal.
      bool passed = false;
      stripe = s.journal.begin_flush_object(object, skip, &passed,
        barrier.offset, barrier.end - barrier.offset, true);
      deferred |= passed;
    }
    for (const auto& object : s.barrier_objects) {
      if (stripe) {
        break;
      }
      bool passed = false;
      stripe = s.journal.begin_flush_object(object, skip, &passed);
      deferred |= passed;
    }
    if (!stripe) {
      isolating = false;
      bool passed = false;
      stripe = s.journal.begin_flush(clean, skip, &passed, reclaim_upto);
      deferred |= passed;
    }
    if (deferred && retry) {
      ++s.deferred_flushes;
      schedule_journal(journal_flush_retry_ms);
    }
    retry = false;
    if (!stripe) {
      break;
    }
    const bool full = stripe->full();
    if (!full) {
      (clean ? s.drain_flushes :
       isolating ? s.barrier_flushes :
       reclaim_upto ? s.pressure_flushes : s.adopted_flushes)++;
    }
    // Contiguous runs of the stripe's live blocks, at object offsets.
    std::map<uint64_t, ceph::bufferlist> runs;
    uint64_t run_end = 0;
    for (const auto& [off, data] : stripe->blocks) {
      const uint64_t at = stripe->offset + off;
      if (!runs.empty() && run_end == at) {
        runs.rbegin()->second.append(data);
      } else {
        runs.emplace(at, data);
      }
      run_end = at + data.length();
    }
    ++s.flushes_in_flight;
    const auto object = stripe->object;
    ++s.flushing[object];
    const auto offset = stripe->offset;
    const auto covered = stripe->newest_version;
    auto reqids = stripe->reqids; // the write logs them (extra_reqids)
    // Runs from eval_repop with the PG lock held.
    get_parent()->ec_journal_flush(object, std::move(runs), offset, covered,
      std::move(reqids), [this, flushed = std::move(*stripe), full] {
        auto& s = *journal_state;
        const int finished = s.journal.finish_flush(flushed, 0);
        ceph_assert(finished == 0);
        ceph_assert(s.flushes_in_flight > 0);
        --s.flushes_in_flight;
        auto flushing = s.flushing.find(flushed.object);
        ceph_assert(flushing != s.flushing.end());
        if (--flushing->second == 0) {
          s.flushing.erase(flushing);
        }
        (full ? s.full_stripes : s.partial_stripes)++;
        s.flushed_bytes += flushed.width;
        // A read that waited for this flush (journal_read_ready) goes at
        // once; the pump sees to released segments and waiting writes.
        journal_kick_reads(flushed.object);
        schedule_journal();
        get_parent()->ec_journal_flush_committed(flushed.object);
      });
  }
  s.pumping = false;
  // Commits wake the driver for complete stripes and reclaim; the timer
  // does the housekeeping described above.
  std::optional<std::chrono::milliseconds> wake;
  if (!s.journal.empty() || !s.barrier_objects.empty()) {
    wake = journal_housekeeping_ms;
  }
  if (!wake && s.slots_on_disk && get_parent()->pgb_is_primary()) {
    wake = journal_idle_check_ms;
  }
  if ((!writeable || s.fetches.error()) && wake) {
    wake = std::max(*wake, journal_housekeeping_ms); // nothing can flush yet
  }
  if (wake) {
    schedule_journal(*wake);
  }
  if (s.replaying && !s.fetches.error() && s.fetches.empty() &&
      s.header_reads == 0) {
    // Every record of the slots is adopted, dead or deferred for recovery,
    // and the slots of the deferred ones are held: new writes may be
    // journaled into the free slots while the adopted records flush first.
    s.replaying = false;
    s.replay_listed_ms = std::chrono::duration<double, std::milli>(
      ceph::mono_clock::now() - s.activated_at).count();
    dout(1) << "EC journal replay: " << s.replay_adopted << " records adopted into "
            << s.journal.adopted_segments() << " slots, " << s.replay_deferred
            << " deferred for recovery, " << s.journal.held_slots()
            << " slots held, " << *s.replay_listed_ms << " ms after activation;"
            << " admitting" << dendl;
  }
  if (s.replay_pending && !s.replaying && !s.journal.adopting() &&
      s.deferred.empty() && s.fetches.empty()) {
    s.replay_pending = false;
    dout(1) << "EC journal replay complete: " << s.replay_adopted
            << " records flushed again, " << s.replay_dead << " already materialized"
            << dendl;
  }
  // A PG that cannot write cannot drain; its records are durable in the
  // slots and replay after the next writeable activation.
  if (s.fetches.error()) {
    s.shutdown_drain.finish(s.fetches.error());
  } else {
    s.shutdown_drain.maybe_finish(
      (s.journal.empty() && !s.replay_pending) || !writeable, true);
  }
  // A configured drain that has flushed everything also removes the slots,
  // with every holder in the acting set (healthy) so none keeps them.
  const bool primary = get_parent()->pgb_is_primary();
  if (primary && s.drain_conf.load() && writeable && healthy &&
      !s.replay_pending && s.journal.empty() && s.flushes_in_flight == 0 &&
      s.slots_on_disk) {
    remove_journal_slots();
  }
  // Ops parked in do_op are requeued there, in order: writes (journal_admit)
  // whose object is clean again or that may find room now, and reads
  // (journal_read_ready) with nothing left to wait for.
  get_parent()->ec_journal_kick();
  std::erase_if(s.read_barriers, [&s](const auto& barrier) {
    return s.read_wait(barrier.first, barrier.second.offset,
                       barrier.second.end - barrier.second.offset) ==
      JournalState::ReadWait::none;
  });
  std::erase_if(s.write_barriers, [&s](const auto& barrier) {
    return s.write_wait(barrier.first, barrier.second.first,
                        barrier.second.second - barrier.second.first) ==
      JournalState::ReadWait::none;
  });
}

void ECBackend::remove_journal_slots()
{
  auto& s = *journal_state;
  const epoch_t interval = get_parent()->get_info().history.same_interval_since;
  if (s.removing_slots) {
    if (*s.removing_slots == interval) {
      return; // in flight
    }
    s.removing_slots.reset(); // dropped by an interval change
  }
  // An unlogged op of the RMW pipeline, like the rollforward kick: slot
  // objects are PG-local and unversioned, as their appends' resets are.
  // Removing an absent slot is fine (the store ignores ENOENT on remove).
  auto op = std::make_shared<ECJournalSlotRemoval>();
  op->hoid = s.journal.slot_object(0, get_parent()->whoami_shard().shard).hobj;
  for (const auto holder : s.holders) {
    auto& objects = op->slots[holder];
    for (uint64_t slot = 0; slot < s.journal.slot_count(); ++slot) {
      objects.push_back(s.journal.slot_object(slot, holder));
    }
  }
  const uint64_t admitted = s.admitted;
  op->on_all_commit = make_lambda_context(
    [this, admitted, interval](int result) {
      auto& s = *journal_state;
      if (s.removing_slots != interval) {
        return;
      }
      s.removing_slots.reset();
      // An append since (drain turned off meanwhile) wrote a slot again.
      if (result == 0 && s.admitted == admitted) {
        s.slots_on_disk = false;
        ++s.slot_removals;
        dout(1) << "EC journal drained: slot objects removed from the holders"
                << dendl;
      }
      schedule_journal();
    });
  s.removing_slots = interval;
  if (!rmw_pipeline.submit_unlogged(op)) {
    s.removing_slots.reset(); // writes are queued: try again once they pass
    schedule_journal(journal_flush_retry_ms);
  }
}

void ECBackend::call_write_ordered(std::function<void()> &&cb)
{
  // No write waits inside the backend once it has a version (do_op admits
  // journal writes), so ordered callbacks go straight to the pipeline.
  rmw_pipeline.call_write_ordered(std::move(cb));
}

void ECBackend::drain_journal_for_shutdown(std::function<void(int)> on_finish)
{
  if (!journal_state) {
    on_finish(0);
    return;
  }
  journal_state->shutdown_drain.request(std::move(on_finish));
  if (!journal_state->pumping) {
    pump_journal();
  }
}

bool ECBackend::journal_admission_open() const
{
  const auto& s = *journal_state;
  // Not while degraded either: writes then list every shard in their log
  // entries (RMWPipeline::cache_ready), and a journaled record would give
  // the data shards a new object version without its data.
  return !s.draining() && !s.journal.error() &&
    !parent->get_eclistener()->pg_is_degraded_or_undersized() &&
    parent->get_acting_shards().size() == sinfo.get_k_plus_m() &&
    parent->get_backfill_shards().empty();
}

PGBackend::ec_journal_admission_t ECBackend::journal_admit(
  const hobject_t& oid, const ObjectState& obs, const SnapSet& snapset,
  snapid_t snap_seq, const std::vector<OSDOp>& ops, bool may_write)
{
  using admission = PGBackend::ec_journal_admission_t;
  if (!journal_state || !get_parent()->pgb_is_primary()) {
    return admission::none;
  }
  if (!may_write) {
    // A read is served from the journal; what it may have to wait for is
    // decided where it is issued (journal_read_ready).
    return admission::classic;
  }
  auto& s = *journal_state;
  if (obs.oi.is_data_digest() && journal_admission_open()) {
    // candidate_write refuses an object that carries a data digest; count
    // the writes that go direct for that alone.
    ObjectState plain = obs;
    plain.oi.clear_data_digest();
    if (ECWriteJournal::candidate_write(oid, ops, plain, snapset, snap_seq,
                                        sinfo.get_stripe_width(),
                                        s.max_cross())) {
      ++s.bypass_digest;
    }
  }
  if (journal_admission_open() && ECWriteJournal::candidate_write(oid, ops,
        obs, snapset, snap_seq, sinfo.get_stripe_width(), s.max_cross())) {
    // Every eligible write is journaled, with two exceptions. A whole
    // aligned stripe goes straight to the EC write path, which needs no
    // read for it: it takes the stripe's records with it, if any, and they
    // die unwritten (journal_take_for_write). And so does a write that
    // finds the log full while its stripe has no records. Otherwise a
    // stripe that has records takes every later write: nothing may overtake
    // journaled data.
    const uint64_t width = sinfo.get_stripe_width();
    const auto& extent = ops.front().op.extent;
    const uint64_t stripe = extent.offset / width * width;
    // A write across a stripe boundary (up to max_cross) lies in two
    // stripes and becomes one record in each (journal_write).
    const bool crossing = extent.length > width - extent.offset % width;
    const bool live = s.journal.dirty(oid, extent.offset, extent.length);
    const bool whole = extent.offset == stripe && extent.length == width;
    // The whole stripe goes direct unless that would have to wait for a
    // record in flight, a flush of the stripe or replay; then it is
    // journaled on top of the stripe's records like any other write.
    const bool direct = whole && !s.journal.error() &&
      s.write_wait(oid, stripe, width) == JournalState::ReadWait::none;
    if (!direct && (live || !whole)) {
      int r;
      if (crossing) {
        std::vector<uint64_t> lengths;
        for (const auto& [at, len] : ECWriteJournal::stripe_fragments(
               extent.offset, extent.length, width)) {
          lengths.push_back(len);
        }
        r = s.journal.check_append(oid, lengths);
      } else {
        r = s.journal.check_append(oid, extent.length);
      }
      if (r == 0) {
        ++(live ? s.admit_live : s.admit_new);
        s.admit_cross += crossing;
        if (!s.replay_admit_ms &&
            s.activated_at != ceph::mono_clock::time_point{}) {
          s.replay_admit_ms = std::chrono::duration<double, std::milli>(
            ceph::mono_clock::now() - s.activated_at).count();
        }
        return admission::journal;
      }
      if (r == -EAGAIN && !live && !s.replay_blocked(oid)) {
        // The log is full, but none of this stripe is in it: the direct
        // write costs less than waiting for room.
        ++s.bypass_room;
        return admission::classic;
      }
      if (r == -EAGAIN) {
        // The log is full. Close the open segment, so that it is released
        // once its live stripes have flushed (oldest first), and wake the
        // flusher. The write waits in do_op, without a version, under the
        // OSD's admission limits.
        s.journal.seal();
        ++s.pressure_events;
        schedule_journal();
        return admission::wait;
      }
      // -E2BIG (a long name in a tiny segment): the ordinary write path,
      // which takes the stripe's records with it (below). A latched error:
      // the same path, behind this object's journaled stripes.
    } else if (direct) {
      // The direct EC write. It overtakes nothing: records its stripe holds
      // go with it. The object's other stripes keep theirs, and the write
      // marks only its own stripe materialized (PrimaryLogPG::finish_ctx).
      ++s.bypass_full;
      return admission::classic;
    }
  }
  uint64_t offset = 0;
  uint64_t length = 0;
  if (!s.journal.error() && ECWriteJournal::plain_overwrite(oid, ops, obs,
        snapset, snap_seq, &offset, &length)) {
    // A plain overwrite the journal does not take (it crosses a stripe
    // boundary, is larger than a stripe, is not block aligned, or the
    // journal is closed to new records) runs as an ordinary EC write beside
    // the object's journaled stripes and takes those it overlaps with it
    // (journal_take_for_write, from PrimaryLogPG::finish_ctx in this same
    // PG-lock section). It waits only for what it cannot take.
    using wait_t = JournalState::ReadWait;
    const auto wait = s.write_wait(oid, offset, length);
    if (wait == wait_t::none) {
      return admission::classic;
    }
    ++(wait == wait_t::commit ? s.write_waits_commit :
       wait == wait_t::flush ? s.write_waits_flush : s.write_waits_replay);
    auto [barrier, fresh] = s.write_barriers.try_emplace(oid, offset,
                                                         offset + length);
    if (!fresh) {
      barrier->second.first = std::min(barrier->second.first, offset);
      barrier->second.second = std::max(barrier->second.second,
                                        offset + length);
    }
    return admission::wait;
  }
  if (journal_blocked(oid)) {
    // Neither journaled nor free to overtake the object's journaled data:
    // wait for its stripes, complete or not, to flush first.
    if (std::find(s.barrier_objects.begin(), s.barrier_objects.end(), oid) ==
        s.barrier_objects.end()) {
      s.barrier_objects.push_back(oid);
    }
    ++s.barrier_events;
    schedule_journal();
    return admission::wait;
  }
  return admission::classic;
}

bool ECBackend::journal_object_dirty(const hobject_t& oid) const
{
  return journal_state && journal_state->journal.dirty(oid);
}

std::optional<eversion_t> ECBackend::journal_oldest_live(const hobject_t& oid,
  std::optional<uint64_t> skip_stripe) const
{
  if (!journal_state) {
    return std::nullopt;
  }
  return journal_state->journal.oldest_version(oid, skip_stripe);
}

bool ECBackend::journal_flush_in_flight(const hobject_t& oid) const
{
  return journal_state && journal_state->flushing.contains(oid);
}

bool ECBackend::journal_ready(const hobject_t& oid) const
{
  if (!journal_state) {
    return true;
  }
  const auto& s = *journal_state;
  // A write held back for the object's stripes goes once they are flushed;
  // a direct write or a read once nothing it has to wait for is left; a
  // write held back for room once a segment has been released. An object
  // can have several of these parked (its ops queue behind the first):
  // they go once none of them has to wait.
  bool held = false;
  if (std::find(s.barrier_objects.begin(), s.barrier_objects.end(), oid) !=
      s.barrier_objects.end()) {
    if (journal_blocked(oid)) {
      return false;
    }
    held = true;
  }
  if (const auto barrier = s.write_barriers.find(oid);
      barrier != s.write_barriers.end()) {
    if (s.write_wait(oid, barrier->second.first,
                     barrier->second.second - barrier->second.first) !=
        JournalState::ReadWait::none) {
      return false;
    }
    held = true;
  }
  if (const auto barrier = s.read_barriers.find(oid);
      barrier != s.read_barriers.end()) {
    if (s.read_wait(oid, barrier->second.offset,
                    barrier->second.end - barrier->second.offset) !=
        JournalState::ReadWait::none) {
      return false;
    }
    held = true;
  }
  return held || !s.journal.pressure();
}

PGBackend::ec_journal_take_t ECBackend::journal_take_for_write(
  const hobject_t& oid, const interval_set<uint64_t>& written)
{
  PGBackend::ec_journal_take_t take;
  if (!journal_state || written.empty()) {
    return take;
  }
  auto& s = *journal_state;
  // do_op let this write run (journal_admit) under this hold of the PG
  // lock, so every stripe its range overlaps is clean or holds durable
  // records only, and none is being flushed. The write takes them as a
  // flush would: their blocks are dead once it commits, and until then they
  // keep their segments, so a write that peering rolls back finds its
  // records still in the slots.
  const uint64_t offset = written.range_start();
  const uint64_t length = written.range_end() - offset;
  std::vector<ECWriteJournal::Stripe> taken;
  const int r = s.journal.take_range(oid, offset, length, &taken);
  ceph_assertf(r == 0, "EC journal POC: a direct write met journaled "
               "stripes it cannot take: %d", r);
  take.oldest_other = s.journal.oldest_version_outside(oid, offset, length);
  if (taken.empty()) {
    return take;
  }
  uint64_t dropped = 0;
  take.runs = ECWriteJournal::absorb_runs(taken, written, &dropped);
  // The write materializes these stripes, so it logs their records'
  // requests (extra_reqids) and the journal lets go of them at its commit.
  for (const auto& stripe : taken) {
    take.reqids.insert(take.reqids.end(), stripe.reqids.begin(),
                       stripe.reqids.end());
  }
  ++s.direct_takes;
  s.taken_stripes += taken.size();
  s.dropped_stripes += dropped;
  dout(20) << __func__ << " " << oid << " " << offset << "~" << length
           << " takes " << taken.size() << " journaled stripes, " << dropped
           << " covered whole, " << take.runs.size() << " runs added" << dendl;
  // Runs from eval_repop with the PG lock held.
  take.on_commit = [this, oid, taken = std::move(taken)] {
    auto& s = *journal_state;
    for (const auto& stripe : taken) {
      const int finished = s.journal.finish_flush(stripe, 0);
      ceph_assert(finished == 0);
    }
    // Ops of the object parked for these stripes go at once; the pump sees
    // to released segments and to writes waiting for room.
    journal_kick_reads(oid);
    schedule_journal();
  };
  return take;
}

bool ECBackend::journal_read_ready(const hobject_t& oid, uint64_t offset,
                                   uint64_t length)
{
  if (!journal_state || !get_parent()->pgb_is_primary()) {
    return true;
  }
  auto& s = *journal_state;
  using wait_t = JournalState::ReadWait;
  bool adopted = false;
  const auto wait = s.read_wait(oid, offset, length, &adopted);
  if (wait == wait_t::none) {
    return true;
  }
  ++s.read_barrier_events;
  ++(wait == wait_t::commit ? s.read_waits_commit :
     wait == wait_t::replay ? s.read_waits_replay : s.read_waits_flush);
  auto [barrier, fresh] = s.read_barriers.try_emplace(oid,
    JournalState::ReadBarrier{offset, offset + length, adopted});
  if (!fresh) {
    barrier->second.offset = std::min(barrier->second.offset, offset);
    barrier->second.end = std::max(barrier->second.end, offset + length);
    barrier->second.flush |= adopted;
  }
  if (adopted) {
    schedule_journal(); // the adopted stripes it reads flush first
  }
  return false;
}

void ECBackend::journal_kick_reads(const hobject_t& oid)
{
  auto& s = *journal_state;
  // The object's parked ops go again once neither a read nor a direct
  // write among them has anything left to wait for (journal_ready).
  const auto barrier = s.read_barriers.find(oid);
  const auto writer = s.write_barriers.find(oid);
  if (barrier == s.read_barriers.end() && writer == s.write_barriers.end()) {
    return;
  }
  if (barrier != s.read_barriers.end() &&
      s.read_wait(oid, barrier->second.offset,
                  barrier->second.end - barrier->second.offset) !=
        JournalState::ReadWait::none) {
    return;
  }
  if (writer != s.write_barriers.end() &&
      s.write_wait(oid, writer->second.first,
                   writer->second.second - writer->second.first) !=
        JournalState::ReadWait::none) {
    return;
  }
  get_parent()->ec_journal_kick();
  s.read_barriers.erase(oid);
  s.write_barriers.erase(oid);
}

void ECBackend::journal_read_done(const hobject_t& oid,
  const std::list<std::pair<ec_align_t,
                            std::pair<ceph::buffer::list*, Context*>>>& read)
{
  auto& s = *journal_state;
  for (const auto& [extent, out] : read) {
    s.journal.read_end(oid, extent.offset, extent.size);
  }
  // A flush that was passed over for these reads can start.
  bool wake = false;
  for (auto i = s.flush_waits.lower_bound({oid, 0});
       i != s.flush_waits.end() && i->first == oid;) {
    if (s.journal.reading(*i)) {
      ++i;
    } else {
      i = s.flush_waits.erase(i);
      wake = true;
    }
  }
  if (wake) {
    schedule_journal();
  }
}

void ECBackend::journal_write(OpRequestRef op, ObjectContextRef obc)
{
  // do_op admitted this write (journal_admit) in the same PG-lock critical
  // section, so the room it found is still there and the object's size is
  // the one it saw: the append cannot fail. The write is not PG-logged: it
  // takes no version, and the object's state is unchanged until the flush.
  auto& s = *journal_state;
  const auto m = op->get_req<MOSDOp>();
  const hobject_t hoid = obc->obs.oi.soid;
  ceph_assert(m->ops.size() == 1);
  const auto& osd_op = m->ops.front();
  const uint64_t offset = osd_op.op.extent.offset;
  const uint64_t length = osd_op.op.extent.length;
  ceph_assert(osd_op.indata.length() == length);
  const ceph_tid_t tid = get_parent()->get_tid();
  auto& flight = s.inflight[tid];
  flight.op = op;
  flight.obc = obc;
  flight.object = hoid;
  flight.bytes = length;
  flight.reqid = m->get_reqid();
  // The write takes the next user version, as a logged write would
  // (PrimaryLogPG::finish_ctx), and the object's cached info shows it and
  // the client's mtime from here on: stat, assert-version and replies see
  // the write. The flush persists both; the journal keeps them for an
  // object context read from disk meanwhile (journal_object_meta).
  const version_t user_version = std::max({
    get_parent()->get_info().last_user_version, obc->obs.oi.user_version,
    s.journal.newest_user_version()}) + 1;
  obc->obs.oi.user_version = user_version;
  obc->obs.oi.mtime = m->get_mtime();
  flight.user_version = user_version;
  // One record per stripe the write lies in: the index, the flushes and
  // replay are per stripe, so a write across a stripe boundary is two
  // records, each judged and materialized on its own. They take consecutive
  // journal versions and go to every holder in one transaction, so a holder
  // has both or neither, and they become durable together.
  // A record's journal version: the interval's start epoch and the
  // sequence its append takes, so versions and sequences order alike.
  std::vector<ECWriteJournal::Placement> placements;
  for (const auto& [at, len] : ECWriteJournal::stripe_fragments(
         offset, length, sinfo.get_stripe_width())) {
    const eversion_t version(get_parent()->get_interval_start_epoch(),
                             s.journal.next_append_sequence());
    ceph::bufferlist piece;
    piece.substr_of(osd_op.indata, at - offset, len);
    ECWriteJournal::Ticket ticket;
    // The record carries the request and the user version the reply will
    // report, so a resend is recognized until the stripe is materialized
    // and a replaying primary rebuilds the object's metadata overlay.
    const int r = s.journal.append(hoid, version, obc->obs.oi.size, at, piece,
      &placements.emplace_back(), &ticket, m->get_mtime(), m->get_reqid(),
      user_version);
    ceph_assertf(r == 0, "EC journal POC: admitted write did not append: %d", r);
    ceph_assert(ticket.sequence == version.version);
    s.open_segment = ticket.segment;
    flight.tickets.push_back(ticket);
  }
  flight.pending = s.holders.size();
  s.inflight_by_object[hoid].push_back(tid);
  {
    object_stat_sum_t delta;
    delta.num_wr++;
    delta.num_wr_kb += shift_round_up(length, 10);
    get_parent()->apply_stats(hoid, delta);
  }

  // One unlogged sub-write per holder with that holder's copy of the
  // record; the holder commits it and replies (handle_sub_write,
  // handle_sub_write_reply). Admission requires every holder acting.
  const auto pgid = get_parent()->get_info().pgid.pgid;
  std::vector<std::pair<int, Message*>> messages;
  ECSubWrite local_write_op;
  bool should_write_local = false;
  unsigned sent = 0;
  for (const auto& pg_shard : get_parent()->get_acting_shards()) {
    if (std::find(s.holders.begin(), s.holders.end(), pg_shard.shard) ==
        s.holders.end()) {
      continue;
    }
    ObjectStore::Transaction t;
    for (const auto& placement : placements) {
      s.journal.emit(placement, coll_t(spg_t(pgid, pg_shard.shard)),
                     pg_shard.shard, t);
    }
    ECSubWrite sop(get_parent()->whoami_shard(), tid, m->get_reqid(), hoid,
                   pg_stat_t(), t, eversion_t(), eversion_t(), eversion_t(),
                   {}, std::nullopt, {}, {}, false);
    sop.unlogged = true;
    ++sent;
    if (pg_shard == get_parent()->whoami_shard()) {
      should_write_local = true;
      local_write_op.claim(sop);
    } else {
      auto *msg = new MOSDECSubOpWrite(sop);
      msg->pgid = spg_t(get_parent()->primary_spg_t().pgid, pg_shard.shard);
      msg->map_epoch = switcher->get_osdmap_epoch();
      msg->min_epoch = get_parent()->get_interval_start_epoch();
      msg->trace = op->pg_trace;
      messages.emplace_back(pg_shard.osd, msg);
    }
  }
  ceph_assertf(sent == s.holders.size(),
               "EC journal POC: %u of %zu holders acting", sent, s.holders.size());
  ++s.admitted;
  s.slots_on_disk = true;
  // While records are resident the driver wakes at least once a second
  // (pump_journal re-arms it). The first record after an idle spell must
  // arm it: a drain set through the config observer only flips an atomic,
  // and neither incomplete stripes nor their commits wake the driver, so
  // an idle PG would otherwise keep its records until its next write.
  schedule_journal(journal_housekeeping_ms);
  if (!messages.empty()) {
    get_parent()->send_message_osd_cluster(messages, switcher->get_osdmap_epoch());
  }
  if (should_write_local) {
    handle_sub_write(get_parent()->whoami_shard(), op, local_write_op,
                     op->pg_trace, *parent->get_eclistener());
  }
}

void ECBackend::journal_record_committed(pg_shard_t from, ceph_tid_t tid)
{
  auto& s = *journal_state;
  auto i = s.inflight.find(tid);
  ceph_assert(i != s.inflight.end());
  auto& flight = i->second;
  ceph_assert(flight.pending > 0);
  if (--flight.pending) {
    return;
  }
  // Every holder has the record: it is durable.
  const bool was_flushable = s.flushable();
  const bool was_full = s.journal.pressure();
  for (const auto& ticket : flight.tickets) {
    const int committed = s.journal.committed(ticket, 0);
    ceph_assert(committed == 0);
  }
  flight.done = true;
  const hobject_t object = flight.object; // the acks below erase flight
  // Acknowledge in order per object: a client's writes of one object
  // complete in the order they were admitted, as they would if logged.
  auto q = s.inflight_by_object.find(object);
  ceph_assert(q != s.inflight_by_object.end());
  auto& queue = q->second;
  while (!queue.empty()) {
    auto j = s.inflight.find(queue.front());
    ceph_assert(j != s.inflight.end());
    if (!j->second.done) {
      break;
    }
    ++s.acked;
    const osd_reqid_t reqid = j->second.reqid;
    ObjectContextRef obc = j->second.obc;
    get_parent()->ec_journal_acked(std::move(j->second.op),
                                   std::move(j->second.obc), j->second.bytes,
                                   j->second.user_version);
    s.inflight.erase(j);
    queue.pop_front();
    // Resends held while the record was in flight get the same answer.
    if (auto r = s.resends.find(reqid); r != s.resends.end()) {
      const auto request = s.journal.find_request(reqid);
      const version_t user_version =
        request ? request->user_version : obc->obs.oi.user_version;
      for (auto& resend : r->second) {
        ++s.dup_replies;
        get_parent()->ec_journal_dup_acked(std::move(resend), obc, user_version);
      }
      s.resends.erase(r);
    }
  }
  if (queue.empty()) {
    s.inflight_by_object.erase(q);
  }
  // A read that waited for this record (journal_read_ready) goes at once
  // if nothing else of its stripes is in flight.
  journal_kick_reads(object);
  // A commit can release a dead segment: writes waiting in do_op for room
  // are requeued by the pump (ec_journal_kick).
  if ((!was_flushable && s.flushes_in_flight < s.max_flushes &&
       s.flushable()) || (was_full && !s.journal.pressure())) {
    schedule_journal();
  }
}

std::optional<PGBackend::ec_journal_request_t> ECBackend::journal_find_request(
  const osd_reqid_t& reqid) const
{
  if (!journal_state) {
    return std::nullopt;
  }
  const auto request = journal_state->journal.find_request(reqid);
  if (!request) {
    return std::nullopt;
  }
  return PGBackend::ec_journal_request_t{request->user_version,
                                         request->durable};
}

void ECBackend::journal_hold_resend(const osd_reqid_t& reqid, OpRequestRef op)
{
  auto& s = *journal_state;
  ++s.dup_held;
  s.resends[reqid].push_back(std::move(op));
}

std::optional<PGBackend::ec_journal_object_meta_t> ECBackend::journal_object_meta(
  const hobject_t& oid) const
{
  if (!journal_state) {
    return std::nullopt;
  }
  const auto meta = journal_state->journal.object_meta(oid);
  if (!meta) {
    return std::nullopt;
  }
  return PGBackend::ec_journal_object_meta_t{meta->user_version, meta->mtime};
}

version_t ECBackend::journal_newest_user_version() const
{
  return journal_state ? journal_state->journal.newest_user_version() : 0;
}

eversion_t ECBackend::journal_version_now() const
{
  if (!journal_state) {
    return eversion_t();
  }
  return journal_state->journal.version_now(parent->get_interval_start_epoch());
}

void ECBackend::journal_split(spg_t child, ObjectStore::Transaction& t)
{
  if (!journal_state) {
    return;
  }
  switcher->ch->flush();
  const int result = journal_state->journal.copy_slots_for_split(child,
    [this](const ghobject_t& slot, ceph::bufferlist& data,
         ECWriteJournal::SlotHeaders& headers) {
      const int read = switcher->store->read(switcher->ch, slot, 0, 0, data);
       ceph::bufferlist omap_header;
       return read < 0 ? read :
         switcher->store->omap_get(switcher->ch, slot, &omap_header, &headers);
    }, t);
  ceph_assertf(result == 0, "EC journal split: cannot copy slots to %s: %d",
               stringify(child).c_str(), result);
}

namespace {
// Lists one holder's slots for replay (journal_on_activate): the headers
// come back as each slot's attributes.
struct JournalHeaderCompleter final : ECCommon::ReadCompleter {
  ECBackend& backend;
  shard_id_t shard;
  JournalHeaderCompleter(ECBackend& backend, shard_id_t shard)
    : backend(backend), shard(shard) {}
  void finish_single_request(const hobject_t& slot,
                             ECCommon::read_result_t&& res,
                             ECCommon::read_request_t&) override {
    if (res.r == 0 && res.errors.empty() && res.attrs) {
      backend.journal_headers_result(shard, slot, *res.attrs);
    }
  }
  void finish(int) && override {
    backend.journal_headers_finished();
  }
};

// Reads the slot ranges replay is missing from one holder (fetch_next).
struct JournalFetchCompleter final : ECCommon::ReadCompleter {
  ECBackend& backend;
  shard_id_t shard;
  JournalFetchCompleter(ECBackend& backend, shard_id_t shard)
    : backend(backend), shard(shard) {}
  void finish_single_request(const hobject_t& slot,
                             ECCommon::read_result_t&& res,
                             ECCommon::read_request_t&) override {
    if (res.r == 0 && res.errors.empty() &&
        res.buffers_read.contains_shard(shard)) {
      backend.journal_fetch_result(slot, res.buffers_read, shard);
    }
  }
  void finish(int) && override {
    backend.journal_fetch_finished();
  }
};
} // namespace

void ECBackend::journal_on_activate()
{
  if (!journal_state || !get_parent()->pgb_is_primary()) {
    return;
  }
  auto& s = *journal_state;
  // Nothing is admitted until the listings below and the fetches are in
  // (draining); the records read back are adopted into their slots and
  // flush first, while new writes take the free slots.
  s.replaying = true;
  s.replay_pending = true;
  s.activated_at = ceph::mono_clock::now();
  s.replay_listed_ms.reset();
  s.replay_admit_ms.reset();
  const auto whoami = get_parent()->whoami_shard().shard;
  const auto pgid = get_parent()->get_info().pgid.pgid;
  const auto split_bits = pgid.get_split_bits(get_parent()->get_pool().get_pg_num());
  const uint64_t width = sinfo.get_stripe_width();
  using state_t = PGBackend::Listener::ec_journal_record_state_t;
  // Every record is judged by its OMAP header first. Only the payloads of
  // records that are still live (or wait for their object's recovery) are
  // read, so a bad block under a dead record costs nothing. A payload this
  // shard cannot read back intact is fetched from the other holders, whose
  // copies of the slot are identical, and only its object waits meanwhile.
  std::vector<ECWriteJournal::Record> records;
  std::set<eversion_t> indexed; // versions this shard's slots hold
  uint64_t unreadable = 0;
  for (uint64_t slot = 0; slot < s.journal.slot_count(); ++slot) {
    const auto object = s.journal.slot_object(slot, whoami);
    struct stat st;
    int result = switcher->store->stat(switcher->ch, object, &st);
    if (result == -ENOENT) {
      continue;
    }
    ECWriteJournal::SlotHeaders headers;
    ceph::bufferlist omap_header;
    if (result >= 0) {
      result = switcher->store->omap_get(switcher->ch, object, &omap_header, &headers);
    }
    std::vector<ECWriteJournal::SlotEntry> entries;
    std::string why;
    if (result >= 0) {
      try {
        entries = ECWriteJournal::decode_slot_headers(slot, st.st_size, headers,
                                                      pgid, split_bits);
      } catch (const ceph::buffer::error& error) {
        result = -EIO;
        why = error.what();
      }
    }
    if (result < 0) {
      // Without the slot's headers nothing names the objects its records
      // belong to (log tags cover only the newest entries): every op of the
      // PG waits, and the next interval reads the slot again.
      s.fetches.fail(result, true);
      get_parent()->clog_error() << "EC journal replay: "
        << get_parent()->get_info().pgid << " cannot index slot " << slot << ": "
        << (why.empty() ? cpp_strerror(result) : why) << "; blocking the PG";
      continue;
    }
    for (auto& entry : entries) {
      ++s.replay_scanned;
      const auto& header = entry.header;
      indexed.insert(header.version);
      if (get_parent()->ec_journal_record_state(header.object, header.version,
            header.offset / width * width) == state_t::dead) {
        ++s.replay_dead;
        continue;
      }
      // Its slot stays out of use until the record is adopted into it or
      // judged dead (judge_replayed), through a fetch or a recovery too.
      s.journal.hold_slot(slot);
      ceph::bufferlist payload;
      const int read = switcher->store->read(switcher->ch, object,
        entry.locator.slot_offset, entry.locator.length, payload);
      if (read >= 0) {
        try {
          records.push_back(entry.locator.decode_record(payload));
          continue;
        } catch (const ceph::buffer::error& error) {
          why = error.what();
        }
      } else {
        why = cpp_strerror(read);
      }
      ++s.replay_unreadable;
      if (++unreadable <= 16) {
        derr << "EC journal replay: " << header.object << " " << header.version
             << " in slot " << slot << " at " << entry.locator.slot_offset
             << ": " << why << "; fetching it from another holder" << dendl;
      }
      s.fetches.add(header.object, header.version, std::move(entry.locator));
    }
  }
  std::sort(records.begin(), records.end(),
    [](const auto& a, const auto& b) { return a.version < b.version; });
  for (auto& record : records) {
    judge_replayed(std::move(record));
  }
  s.replay_indexed = std::move(indexed);
  // Records this shard's slots lack: a record whose commit this shard
  // missed (it crashed or was away while the write was in flight, and came
  // back as primary before its replacement flushed it) is on the other
  // holders, whose slots are laid out identically. Nothing is PG-logged
  // about it, so their slot listings are read through the read pipeline, as
  // the slots' attributes (handle_sub_read), and whatever they list that is
  // missing here is fetched (journal_headers_result). Until every listing
  // is in, no op of the PG runs and nothing flushes (replay_blocked).
  for (const auto& shard : get_parent()->get_acting_shards()) {
    if (shard != get_parent()->whoami_shard() &&
        std::find(s.holders.begin(), s.holders.end(), shard.shard) !=
          s.holders.end()) {
      s.fetch_sources.push_back(shard);
    }
  }
  s.header_reads = s.fetch_sources.size();
  for (const auto& source : s.fetch_sources) {
    std::map<hobject_t, ECCommon::read_request_t> to_read;
    for (uint64_t slot = 0; slot < s.journal.slot_count(); ++slot) {
      const hobject_t object = s.journal.slot_object(slot, source.shard).hobj;
      auto [i, fresh] = to_read.try_emplace(object,
        ECUtil::shard_extent_set_t(sinfo.get_k_plus_m()), WantAttrs::Yes,
        WantOmapHeader::No, WantOmapKeys::No, std::string(), 0,
        uint64_t(ECWriteJournal::record_alignment));
      auto& req = i->second;
      req.shard_want_to_read.map[source.shard].union_insert(
        0, ECWriteJournal::record_alignment);
      auto& read = req.shard_reads[source.shard];
      read.extents.union_insert(0, ECWriteJournal::record_alignment);
      read.pg_shard = source;
    }
    read_pipeline.start_read_op(CEPH_MSG_PRIO_DEFAULT, to_read, false, false,
      std::make_unique<JournalHeaderCompleter>(*this, source.shard));
  }
  if (unreadable) {
    derr << "EC journal replay: " << unreadable << " payloads unreadable here,"
         << " fetching them from other holders" << dendl;
  }
  dout(1) << "EC journal replay: " << s.replay_indexed.size()
          << " records indexed, " << records.size() << " payloads read, "
          << s.replay_adopted << " live, " << s.replay_dead << " materialized, "
          << s.replay_deferred << " deferred for recovery; listing "
          << s.header_reads << " other holders" << dendl;
  if (s.header_reads == 0) {
    journal_replay_listed();
  }
  schedule_journal();
}

void ECBackend::journal_headers_result(
  shard_id_t shard, const hobject_t& slot,
  const std::map<std::string, ceph::bufferlist, std::less<>>& attrs)
{
  auto& s = *journal_state;
  uint64_t number = 0;
  try {
    number = std::stoull(slot.oid.name.substr(
      ECWriteJournal::slot_name_prefix.size()));
  } catch (const std::exception&) {
    return;
  }
  const auto pgid = get_parent()->get_info().pgid.pgid;
  const auto split_bits = pgid.get_split_bits(get_parent()->get_pool().get_pg_num());
  const uint64_t width = sinfo.get_stripe_width();
  const ECWriteJournal::SlotHeaders headers(attrs.begin(), attrs.end());
  std::vector<ECWriteJournal::SlotEntry> entries;
  try {
    entries = ECWriteJournal::decode_slot_headers(number, std::nullopt, headers,
                                                  pgid, split_bits);
  } catch (const ceph::buffer::error& error) {
    // This holder's copy of the slot cannot be read as a whole; the
    // records this shard lacks may still be on another holder.
    derr << "EC journal replay: slot " << number << " listing from shard "
         << shard << " unusable: " << error.what() << dendl;
    return;
  }
  for (auto& entry : entries) {
    const auto& header = entry.header;
    if (s.replay_indexed.contains(header.version) ||
        s.replay_fetching.contains(header.version)) {
      continue;
    }
    ++s.replay_scanned;
    if (get_parent()->ec_journal_record_state(header.object, header.version,
          header.offset / width * width) ==
        PGBackend::Listener::ec_journal_record_state_t::dead) {
      ++s.replay_dead;
      continue;
    }
    dout(10) << __func__ << " " << header.object << " " << header.version
             << " only on other holders; fetching" << dendl;
    s.journal.hold_slot(number);
    s.replay_fetching.insert(header.version);
    s.fetches.add(header.object, header.version, std::move(entry.locator));
  }
}

void ECBackend::journal_headers_finished()
{
  auto& s = *journal_state;
  ceph_assert(s.header_reads > 0);
  if (--s.header_reads == 0) {
    journal_replay_listed();
  }
}

void ECBackend::journal_replay_listed()
{
  auto& s = *journal_state;
  if (!s.fetches.empty()) {
    dout(1) << "EC journal replay: fetching " << s.fetches.pending().size()
            << " records from other holders" << dendl;
    journal_fetch_next();
  }
  // Ops of the objects no fetch holds back may run from here on.
  schedule_journal();
}

bool ECBackend::journal_blocked(const hobject_t& oid) const
{
  const auto& s = *journal_state;
  return s.journal.dirty(oid) || s.replay_blocked(oid);
}


void ECBackend::journal_fetch_next()
{
  auto& s = *journal_state;
  if (s.fetches.error()) {
    return;
  }
  if (s.fetches.empty()) {
    schedule_journal();
    return;
  }
  if (s.fetch_sources.empty()) {
    s.fetches.fail(-EIO);
    for (const auto& pending : s.fetches.pending()) {
      ++s.replay_absent;
      derr << "EC journal replay: " << pending.object << " " << pending.version
           << " is live but no holder returned its record; keeping the object blocked"
           << dendl;
    }
    const auto& first = s.fetches.pending().front();
    get_parent()->clog_error() << "EC journal replay: "
      << get_parent()->get_info().pgid << ": no holder returned "
      << s.fetches.pending().size() << " live records (" << first.object << " "
      << first.version << " first); keeping their objects blocked";
    schedule_journal();
    return;
  }
  const pg_shard_t source = s.fetch_sources.back();
  s.fetch_sources.pop_back();
  // The slot object and the record's place in it are the same on every
  // holder: read those ranges of this holder's copy, verbatim.
  std::map<hobject_t, ECCommon::read_request_t> to_read;
  for (const auto& f : s.fetches.pending()) {
    const hobject_t slot =
      s.journal.slot_object(f.locator.slot, source.shard).hobj;
    auto [i, fresh] = to_read.try_emplace(slot,
      ECUtil::shard_extent_set_t(sinfo.get_k_plus_m()), WantAttrs::No,
      WantOmapHeader::No, WantOmapKeys::No, std::string(), 0,
      f.locator.slot_offset + f.locator.length);
    auto& req = i->second;
    req.object_size = std::max(req.object_size,
                               f.locator.slot_offset + f.locator.length);
    req.shard_want_to_read.map[source.shard].union_insert(f.locator.slot_offset,
                                                          f.locator.length);
    auto& read = req.shard_reads[source.shard];
    read.extents.union_insert(f.locator.slot_offset, f.locator.length);
    read.pg_shard = source;
  }
  dout(10) << __func__ << " " << s.fetches.pending().size() << " records from "
           << source << dendl;
  read_pipeline.start_read_op(CEPH_MSG_PRIO_DEFAULT, to_read, false, false,
    std::make_unique<JournalFetchCompleter>(*this, source.shard));
}

void ECBackend::journal_fetch_result(const hobject_t& slot,
                                     const ECUtil::shard_extent_map_t& data,
                                     shard_id_t shard)
{
  auto& s = *journal_state;
  for (const auto& f : s.fetches.pending()) {
    if (s.journal.slot_object(f.locator.slot, shard).hobj != slot) {
      continue;
    }
    ceph::bufferlist bl;
    data.get_buffer(shard, f.locator.slot_offset, f.locator.length, bl);
    if (bl.length() != f.locator.length) {
      continue; // this holder's copy is short: try the next one
    }
    ECWriteJournal::Record record;
    try {
      record = f.locator.decode_record(bl);
    } catch (const ceph::buffer::error&) {
      continue;
    }
    if (record.object == f.object && record.version == f.version) {
      s.fetched.push_back(std::move(record));
    }
  }
}

void ECBackend::journal_fetch_finished()
{
  auto& s = *journal_state;
  auto fetched = std::move(s.fetched);
  s.fetched.clear();
  std::sort(fetched.begin(), fetched.end(),
    [](const auto& a, const auto& b) { return a.version < b.version; });
  for (auto& record : fetched) {
    if (!s.fetches.complete(record.object, record.version)) {
      continue;
    }
    ++s.replay_fetched;
    judge_replayed(std::move(record));
  }
  journal_fetch_next();
}

void ECBackend::judge_replayed(ECWriteJournal::Record record)
{
  auto& s = *journal_state;
  const uint64_t width = sinfo.get_stripe_width();
  using state_t = PGBackend::Listener::ec_journal_record_state_t;
  switch (get_parent()->ec_journal_record_state(record.object, record.version,
            record.offset / width * width)) {
  case state_t::live: {
    const int r = s.journal.adopt(record);
    ceph_assertf(r == 0, "EC journal replay: cannot adopt %d", r);
    ++s.replay_adopted;
    // The record's user version and mtime are the object's again.
    get_parent()->ec_journal_object_meta_changed(record.object);
    break;
  }
  case state_t::dead:
    ++s.replay_dead;
    s.journal.release_hold(record.slot);
    break;
  case state_t::missing:
    ++s.replay_deferred;
    s.deferred[record.object].push_back(std::move(record));
    break;
  }
}

void ECBackend::journal_object_recovered(const hobject_t& oid)
{
  if (!journal_state) {
    return;
  }
  auto& s = *journal_state;
  auto i = s.deferred.find(oid);
  if (i == s.deferred.end()) {
    return;
  }
  auto records = std::move(i->second);
  s.deferred.erase(i);
  for (auto& record : records) {
    judge_replayed(std::move(record));
  }
  schedule_journal();
}

std::tuple<
  int,
  map<string, bufferlist, less<>>,
  size_t
> ECBackend::get_attrs_n_size_from_disk(const hobject_t &hoid) {
  struct stat st;
  if (int r = object_stat(hoid, &st); r < 0) {
    dout(10) << __func__ << ": stat error " << r << " on" << hoid << dendl;
    return {r, {}, 0};
  }
  map<string, bufferlist, less<>> real_attrs;
  if (int r = switcher->objects_get_attrs_with_hinfo(hoid, &real_attrs); r < 0) {
    dout(10) << __func__ << ": get attr error " << r << " on" << hoid << dendl;
    return {r, {}, 0};
  }
  return {0, real_attrs, st.st_size};
}

void ECBackend::submit_transaction(
  const hobject_t &hoid,
  const object_stat_sum_t &delta_stats,
  const eversion_t &at_version,
  PGTransactionUPtr &&t,
  const eversion_t &trim_to,
  const eversion_t &pg_committed_to,
  vector<pg_log_entry_t> &&log_entries,
  std::optional<pg_hit_set_history_t> &hset_history,
  Context *on_all_commit,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef client_op
) {
  auto op = std::make_shared<ECClassicalOp>();
  auto obc_map = t->obc_map;
  op->t = std::move(t);
  op->hoid = hoid;
  op->delta_stats = delta_stats;
  op->version = at_version;
  op->trim_to = trim_to;
  /* We update PeeringState::pg_committed_to via the callback
   * invoked from ECBackend::handle_sub_write_reply immediately
   * before updating rmw_pipeline.commited_to via
   * rmw_pipeline.check_ops()->finish_rmw(), so these will
   * *usually* match.  However, the PrimaryLogPG::submit_log_entries
   * pathway can perform an out-of-band log update which updates
   * PeeringState::pg_committed_to independently.  Thus, the value
   * passed in is the right one to use. */
  op->pg_committed_to = pg_committed_to;
  op->log_entries = log_entries;
  std::swap(op->updated_hit_set_history, hset_history);
  op->on_all_commit = on_all_commit;
  op->tid = tid;
  op->reqid = reqid;
  op->client_op = client_op;
  op->pipeline = &rmw_pipeline;
  if (client_op) {
    op->trace = client_op->pg_trace;
  }
  ECTransaction::WritePlan &plans = op->plan;

  ceph_assert(op->plan.plans.empty());
  op->plan = get_write_plan(
    sinfo,
    *op->t,
    read_pipeline,
    rmw_pipeline,
    get_parent()->get_dpp());
  ldpp_dout(get_parent()->get_dpp(), 20) << __func__
             << " plans=" << plans
             << dendl;
  rmw_pipeline.start_rmw(std::move(op));
}

int ECBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t object_size,
  const std::list<std::pair<ec_align_t,
  std::pair<ceph::buffer::list*, Context*>>> &to_read,
  CoroHandles coro)
{
  struct ReadState {
    int result = 0;
    bool done = false;
    bool waiting = false;
  };
  auto state = std::make_shared<ReadState>();

  std::weak_ptr<resume_token_t> weak_resume = coro.resume;
  Context *on_finish = new LambdaContext([state, weak_resume](int r) {
    state->result = r;
    state->done = true;

    if (state->waiting) {
      if (auto locked = weak_resume.lock(); locked) {
        (*locked)();
      }
    }
  });

  objects_read_async(hoid, object_size, to_read, on_finish, true);

  // If the async read is not yet complete, yield and wait for it to complete
  if (!state->done) {
    state->waiting = true;
    coro.yield();
  }

  return state->result;
}

int ECBackend::objects_read_local(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    uint32_t op_flags,
    bufferlist *bl) {

  if (!sinfo.supports_direct_reads()) {
    return -EOPNOTSUPP;
  }

  // Cannot return EAGAIN here: the op would get dropped.  This check must have
  // been done earlier.
  ceph_assert(!get_parent()->get_local_missing().is_missing(hoid));

  auto [shard_offset, shard_len] = extent_to_shard_extent(off, len);

  dout(20) << __func__ << " Submitting sync read: "
      << " hoid=" << hoid
      << " shard_offset=" << shard_offset
      << " shard_len=" << shard_len
      << " op_flags=" << op_flags
      << " primary=" << switcher->is_primary()
      << dendl;

  return switcher->store->read(switcher->ch,
          ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
          shard_offset,
          shard_len, *bl, op_flags);
}

std::pair<uint64_t, uint64_t> ECBackend::extent_to_shard_extent(uint64_t off, uint64_t len) {
  // sync reads are supported for sub-chunk reads where no reconstruct is
  // required.
  uint64_t chunk_size = sinfo.get_chunk_size();
  uint64_t start_chunk = off / chunk_size;
  // This calculation is wrong for length = 0, but it doesn't matter if these reads get sent to the primary
  uint64_t end_chunk = (off + len - 1) / chunk_size;
  uint64_t shard_offset, shard_len;
  shard_id_t shard = get_parent()->whoami_shard().shard;
  raw_shard_id_t raw_shard = sinfo.get_raw_shard(shard);

  if (end_chunk == start_chunk) {
    shard_offset = sinfo.ro_offset_to_shard_offset(off, raw_shard);
    shard_len = len;
  } else {
    ECUtil::shard_extent_set_t full_read(sinfo.get_k_plus_m());
    sinfo.ro_range_to_shard_extent_set(off, len, full_read);
    shard_offset = full_read[shard].range_start();
    shard_len = full_read[shard].range_end() - shard_offset;
  }

  return std::pair(shard_offset, shard_len);
}

int ECBackend::objects_readv_sync(const hobject_t &hoid,
     std::map<uint64_t, uint64_t>& m,
     uint32_t op_flags,
     ceph::buffer::list *bl) {

  // Cannot return EAGAIN here: the op would get dropped.  This check must have
  // been done earlier.
  ceph_assert(!get_parent()->get_local_missing().is_missing(hoid));

  auto shard = get_parent()->whoami_shard().shard;
  // Not using extent_set, since we need the one used by readv.
  interval_set im(std::move(m));
  m.clear(); // Make m safe to write to again.
  auto r = switcher->store->readv(switcher->ch, ghobject_t(hoid, ghobject_t::NO_GEN, shard), im, *bl, op_flags);
  if (r >= 0) {
    uint64_t chunk_size = sinfo.get_chunk_size();
    for (auto [off, len] : im) {
      uint64_t ro_offset = sinfo.shard_offset_to_ro_offset(shard, off);
      uint64_t to_next_chunk = ((off / chunk_size) + 1) * chunk_size - off;
      uint64_t ro_len = std::min(to_next_chunk, len);
      while (len > 0 ) {
        dout(20) << __func__ << " shard=" << shard << " extent=" << off << "~" << len <<  ">" << ro_offset << "~" << ro_len << dendl;
        m.emplace(ro_offset, ro_len);
        len -= ro_len;
        ro_offset += ro_len + sinfo.get_stripe_width() - chunk_size;
        ro_len = std::min(len, chunk_size);
      }
    }
  } else {
    return r;
  }

  return 0;
}

void ECBackend::objects_read_async(
    const hobject_t &hoid,
    uint64_t object_size,
    const list<pair<ec_align_t,
                    pair<bufferlist*, Context*>>> &to_read,
    Context *on_complete,
    bool fast_read) {
  // The journal's live blocks over the ranges read, as they are now. The
  // read was let through under this hold of the PG lock (journal_read_ready),
  // so every record there is durable. It holds the object's read lock until
  // it completes, which keeps flushes and direct writes off the shards
  // meanwhile; writes journaled meanwhile do not change this snapshot. So
  // the shards' bytes with these blocks laid over them are the object as of
  // now. Blocks that cover every range are the whole answer.
  ECWriteJournal::Overlay overlay;
  bool journal_only = false;
  bool counted = false; // on the journal's readers, until the read completes
  if (journal_state && get_parent()->pgb_is_primary()) {
    auto& s = *journal_state;
    bool covered = !to_read.empty();
    for (const auto& [read, ctx] : to_read) {
      if (!read.size) {
        covered = false; // reads nothing, waits for nothing
        continue;
      }
      ceph_assertf(
        s.read_wait(hoid, read.offset, read.size) ==
          JournalState::ReadWait::none,
        "EC journal POC: a read was issued past records it has to wait for");
      s.journal.read_overlay(hoid, read.offset, read.size, &overlay);
      covered = covered && overlay.covers(read.offset, read.size);
    }
    if (!overlay.empty()) {
      journal_only = covered;
      ++(journal_only ? s.reads_from_journal : s.read_overlays);
    }
    if (!journal_only) {
      // Shard reads follow: no flush of these stripes until they are done
      // (journal_read_done).
      counted = true;
      for (const auto& [read, ctx] : to_read) {
        s.journal.read_begin(hoid, read.offset, read.size);
      }
    }
  }
  issue_read_async(hoid, object_size, to_read, on_complete, fast_read,
                   std::move(overlay), journal_only, counted);
}

void ECBackend::issue_read_async(
    const hobject_t &hoid,
    uint64_t object_size,
    const list<pair<ec_align_t,
                    pair<bufferlist*, Context*>>> &to_read,
    Context *on_complete,
    bool fast_read,
    ECWriteJournal::Overlay overlay,
    bool journal_only,
    bool counted) {
  map<hobject_t, std::list<ec_align_t>> reads;

  uint32_t flags = 0;
  extent_set es;
  for (const auto &[read, ctx]: to_read) {
    if (journal_only) {
      // Nothing to read from the shards. The read still goes through the
      // pipeline, empty, which completes client reads in the order they
      // were issued.
      break;
    }
    pair<uint64_t, uint64_t> tmp;
    if (!cct->_conf->osd_ec_partial_reads) {
      tmp = sinfo.ro_offset_len_to_stripe_ro_offset_len(read.offset, read.size);
    } else {
      tmp.first = read.offset;
      tmp.second = read.size;
    }
    es.union_insert(tmp.first, tmp.second);
    flags |= read.flags;
  }

  if (!es.empty()) {
    auto &offsets = reads[hoid];
    for (auto [off, len]: es) {
      offsets.emplace_back(ec_align_t{off, len, flags});
    }
  }

  struct cb {
    ECBackend *ec;
    hobject_t hoid;
    list<pair<ec_align_t,
              pair<bufferlist*, Context*>>> to_read;
    unique_ptr<Context> on_complete;
    CephContext *cct;
    // EC journal POC: the journal's blocks over the ranges read, laid over
    // what the shards return; journal_only: they cover every range, and
    // no shard was read; counted: the journal counts this read on its
    // stripes until it completes (not if the interval changes first: the
    // journal forgets its readers then).
    ECWriteJournal::Overlay overlay;
    bool journal_only;
    bool counted;
    cb(const cb &) = delete;
    cb(cb &&) = default;

    cb(ECBackend *ec,
       const hobject_t &hoid,
       const list<pair<ec_align_t,
                       pair<bufferlist*, Context*>>> &to_read,
       Context *on_complete,
       CephContext *cct,
       ECWriteJournal::Overlay overlay,
       bool journal_only,
       bool counted)
      : ec(ec),
        hoid(hoid),
        to_read(to_read),
        on_complete(on_complete),
        cct(cct),
        overlay(std::move(overlay)),
        journal_only(journal_only),
        counted(counted) {}

    void operator()(ECCommon::ec_extents_t &&results) {
      auto dpp = ec->get_parent()->get_dpp();
      ldpp_dout(dpp, 20) << "objects_read_async_cb: got: " << results
			 << dendl;

      int r = 0;
      for (auto &&[read, result]: to_read) {
        auto &&[bufs, ctx] = result;
        if (journal_only) {
          ceph_assert(bufs);
          bufs->clear();
          overlay.apply(read.offset, read.size, *bufs);
          if (ctx) {
            ctx->complete(read.size);
            ctx = nullptr;
          }
          continue;
        }
        auto &got = results.at(hoid);
        if (got.err < 0) {
          // error handling
          if (ctx) {
            ctx->complete(got.err);
          }
          if (r == 0)
            r = got.err;
        } else {
          ceph_assert(bufs);
          uint64_t offset = read.offset;
          uint64_t length = read.size;
          auto range = got.emap.get_containing_range(offset, length);
          uint64_t range_offset = range.first.get_off();
          uint64_t range_length = range.first.get_len();
          ceph_assert(range.first != range.second);
          ceph_assert(range_offset <= offset);
          ldpp_dout(dpp, 20) << "offset: " << offset << dendl;
          ldpp_dout(dpp, 20) << "range offset: " << range_offset << dendl;
          ldpp_dout(dpp, 20) << "length: " << length << dendl;
          ldpp_dout(dpp, 20) << "range length: " << range_length << dendl;
          if (cct->_conf->bluestore_debug_inject_read_err &&
            ECInject::test_parity_read(hoid)) {
            length = range_length;
          }
          ceph_assert((offset + length) <= (range_offset + range_length));
          bufs->substr_of(
            range.first.get_val(),
            offset - range_offset,
            length);
          if (!overlay.empty()) {
            overlay.apply(offset, length, *bufs);
          }
          if (ctx) {
            ctx->complete(length);
            ctx = nullptr;
          }
        }
      }
      if (counted) {
        ec->journal_read_done(hoid, to_read);
      }
      to_read.clear();
      if (on_complete) {
        on_complete.release()->complete(r);
      }
    }

    ~cb() {
      for (auto &&i: to_read) {
        delete i.second.second;
      }
      to_read.clear();
    }
  };
  objects_read_and_reconstruct(
    reads,
    fast_read,
    object_size,
    make_gen_lambda_context<
      ECCommon::ec_extents_t&&, cb>(
      cb(this,
         hoid,
         to_read,
         on_complete,
         cct,
         std::move(overlay),
         journal_only,
         counted)));
}

bool ECBackend::ec_can_decode(const shard_id_set &available_shards) const {
  shard_id_set want_to_read = sinfo.get_all_shards();
  shard_id_set available(available_shards);
  shard_id_set minimum_set;

  int r = ec_impl->minimum_to_decode(want_to_read, available, minimum_set);
  return (r == 0);
}

shard_id_map<bufferlist> ECBackend::ec_encode_acting_set(
    const bufferlist &in_bl) const {
  shard_id_set want_to_encode;
  for (raw_shard_id_t raw_shard_id;raw_shard_id < ec_impl->get_chunk_count();
       ++raw_shard_id) {
    want_to_encode.insert(sinfo.get_shard(raw_shard_id));
  }
  shard_id_map<bufferlist> encoded{ec_impl->get_chunk_count()};
  ec_impl->encode(want_to_encode, in_bl, &encoded);
  return encoded;
}

shard_id_map<bufferlist> ECBackend::ec_decode_acting_set(
    const shard_id_map<bufferlist> &shard_map, int chunk_size) const {
  shard_id_set want_to_read;
  for (raw_shard_id_t raw_shard_id; raw_shard_id < ec_impl->get_chunk_count();
       ++raw_shard_id) {
    shard_id_t shard_id = sinfo.get_shard(raw_shard_id);
    if (!shard_map.contains(shard_id)) want_to_read.insert(shard_id);
  }

  shard_id_map<bufferlist> decoded_buffers(ec_impl->get_chunk_count());
  ec_impl->decode(want_to_read, shard_map, &decoded_buffers, chunk_size);

  shard_id_map<bufferlist> decoded_buffer_map{ec_impl->get_chunk_count()};
  for (auto &[shard_id, bl] : decoded_buffers) {
    decoded_buffer_map[shard_id] = bl;
  }

  return decoded_buffer_map;
}

ECUtil::stripe_info_t ECBackend::ec_get_sinfo() const { return sinfo; }

void ECBackend::objects_read_and_reconstruct(
  const map<hobject_t, std::list<ec_align_t>> &reads,
  bool fast_read,
  uint64_t object_size,
  GenContextURef<ECCommon::ec_extents_t&&> &&func) {
  return read_pipeline.objects_read_and_reconstruct(
    reads, fast_read, object_size, std::move(func));
}

void ECBackend::objects_read_and_reconstruct_for_rmw(
  map<hobject_t, read_request_t> &&to_read,
  GenContextURef<ECCommon::ec_extents_t&&> &&func) {
  return read_pipeline.objects_read_and_reconstruct_for_rmw(
    std::move(to_read), std::move(func));
}

void ECBackend::kick_reads() {
  read_pipeline.kick_reads();
}

int ECBackend::object_stat(
  const hobject_t &hoid,
  struct stat *st) {
  int r = switcher->store->stat(
    switcher->ch,
    ghobject_t{hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard},
    st);
  return r;
}

int ECBackend::objects_get_attrs(
  const hobject_t &hoid,
  map<string, bufferlist, less<>> *out) {
  for (map<string, bufferlist>::iterator i = out->begin();
       i != out->end();
  ) {
    if (ECUtil::is_hinfo_key_string(i->first))
      out->erase(i++);
    else
      ++i;
  }
  return 0;
}

int ECBackend::be_deep_scrub(
  const Scrub::ScrubCounterSet& io_counters,
  const hobject_t &poid,
  ScrubMap &map,
  ScrubMapBuilder &pos,
  ScrubMap::object &o) {
  dout(10) << __func__ << " " << poid << " pos " << pos << dendl;
  int r;

  utime_t sleeptime;
  sleeptime.set_from_double(cct->_conf->osd_debug_deep_scrub_sleep);
  if (sleeptime != utime_t()) {
    lgeneric_derr(cct) << __func__ << " sleeping for " << sleeptime << dendl;
    sleeptime.sleep();
  }

  if (pos.data_pos == 0) {
    pos.data_hash = bufferhash(-1);
  }

  uint64_t stride = cct->_conf->osd_deep_scrub_stride;
  if (stride % sinfo.get_chunk_size())
    stride += sinfo.get_chunk_size() - (stride % sinfo.get_chunk_size());

  auto& perf_logger = *(get_parent()->get_logger());
  perf_logger.inc(io_counters.read_cnt);
  bufferlist bl;
  r = switcher->store->read(
    switcher->ch,
    ghobject_t(
      poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    pos.data_pos,
    stride, bl,
    ECCommon::scrub_fadvise_flags);
  if (r < 0) {
    dout(20) << __func__ << "  " << poid << " got "
	     << r << " on read, read_error" << dendl;
    o.read_error = true;
    return 0;
  }
  if (r > 0) {
    pos.data_hash << bl;
  }
  perf_logger.inc(io_counters.read_bytes, r);
  pos.data_pos += r;
  if (r == (int)stride) {
    return -EINPROGRESS;
  }

  if (sinfo.supports_encode_decode_crcs()) {
    // We pass the calculated digest here
    // This will be used along with the plugin to verify data consistency
    o.digest = pos.data_hash.digest();
  }
  else
  {
    o.digest = 0;
  }

  o.digest_present = true;
  o.omap_digest = -1;
  o.omap_digest_present = true;
  return 0;
}

bool ECBackend::remove_ec_omap_journal_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry) {
  return ec_omap_journal.remove_entry(hoid, entry);
}

std::pair<gen_t, bool> ECBackend::omap_get_generation(const hobject_t& hoid) {
  return ec_omap_journal.get_generation(hoid);
}

void ECBackend::omap_trim_delete_from_journal(const hobject_t &hoid, const version_t version) {
  return ec_omap_journal.trim_delete(hoid, version);
}

int ECBackend::omap_iterate (
  ObjectStore::CollectionHandle &c_, ///< [in] collection
  const ghobject_t &oid, ///< [in] object
  const ObjectStore::omap_iter_seek_t &start_from,
  ///^ [in] where the iterator should point to at the beginning
  const OmapIterFunction &f, ///< [in] function to call for each key/value pair
  ObjectStore *store
) {
  if (!get_parent()->get_pool().supports_omap()) {
    return -EOPNOTSUPP;
  }

  // Updates in update_map take priority over removed_ranges
  auto [update_map, removed_ranges] = ec_omap_journal.get_value_updates(oid.hobj);

  auto journal_it = update_map.begin();
  if (!start_from.seek_position.empty()) {
    journal_it = update_map.lower_bound(start_from.seek_position);
  }

  auto wrapper = [&](const std::string_view store_key, const std::string_view store_value) {
    bool found_store_key_in_journal = false;
    
    while (journal_it != update_map.end() && journal_it->first <= store_key) {
      if (journal_it->first == store_key) {
        found_store_key_in_journal = true;
      }
      if (journal_it->second.value.has_value()) {
        ObjectStore::omap_iter_ret_t r = f(
          journal_it->first,
          std::string_view(
            journal_it->second.value->c_str(),
            journal_it->second.value->length()
          )
        );
        if (r == ObjectStore::omap_iter_ret_t::STOP) {
          return r;
        }
      }
      ++journal_it;
    }

    if (found_store_key_in_journal) {
      return ObjectStore::omap_iter_ret_t::NEXT;
    }

    if (should_be_removed(removed_ranges, store_key)) {
      return ObjectStore::omap_iter_ret_t::NEXT;
    }

    return f(store_key, store_value);
  };

  if (const auto result = store->omap_iterate(c_, oid, start_from, wrapper);
    result < 0) {
    return result;
  } else if (result > 0) {
    return 1;
  }

  auto ret = ObjectStore::omap_iter_ret_t::NEXT;
  while (journal_it != update_map.end()) {
    if (journal_it->second.value.has_value()) {
      ret = f(journal_it->first, std::string_view(journal_it->second.value->c_str(), journal_it->second.value->length()));
      if (ret == ObjectStore::omap_iter_ret_t::STOP) {
        break;
      }
    }
    ++journal_it;
  }

  return ret == ObjectStore::omap_iter_ret_t::STOP ? 1 : 0;
}

int ECBackend::omap_get_values(
  ObjectStore::CollectionHandle &c_, ///< [in] collection
  const ghobject_t &oid,              ///< [in] object
  const std::set<std::string> &keys,  ///< [in] keys to get
  std::map<std::string, ceph::buffer::list> *out, ///< [out] returned key/values
  ObjectStore *store
) {
  if (!get_parent()->get_pool().supports_omap()) {
    return -EOPNOTSUPP;
  }

  auto [update_map, removed_ranges] = ec_omap_journal.get_value_updates(oid.hobj);
  
  set<string> keys_still_to_get;
  for (auto &key : keys) {
    if (auto it = update_map.find(key);
      it != update_map.end()) {
      if (!it->second.value.has_value()) {
        continue;
      }
      (*out)[key] = *(it->second.value);
    } else if (should_be_removed(removed_ranges, key)) {
      continue;
    } else {
      keys_still_to_get.insert(key);
    }
  }
  store->omap_get_values(c_, oid, keys_still_to_get, out);

  return 0;
}

int ECBackend::omap_get_header(
  ObjectStore::CollectionHandle &c_,    ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  ceph::buffer::list *header,      ///< [out] omap header
  const bool allow_eio, ///< [in] don't assert on eio
  ObjectStore *store
) {
  if (!get_parent()->get_pool().supports_omap()) {
    return -EOPNOTSUPP;
  }

  std::optional<ceph::buffer::list> header_from_journal = ec_omap_journal.get_updated_header(oid.hobj);
  if (header_from_journal) {
    *header = *header_from_journal;
    dout(20) << __func__ << ": oid=" << oid
            << " from_journal=true header_size=" << header->length() << dendl;
  } else {
    header->clear();
    store->omap_get_header(c_, oid, header, allow_eio);
    dout(20) << __func__ << ": oid=" << oid
            << " from_journal=false header_size=" << header->length() << dendl;
  }
  return 0;
}

int ECBackend::omap_get(
  ObjectStore::CollectionHandle &c_,    ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  ceph::buffer::list *header,      ///< [out] omap header
  std::map<std::string, ceph::buffer::list> *out, /// < [out] Key to value map
  ObjectStore *store
) {
  if (!get_parent()->get_pool().supports_omap()) {
    return -EOPNOTSUPP;
  }

  // Update map takes priority over removed_ranges
  auto [update_map, removed_ranges] = ec_omap_journal.get_value_updates(oid.hobj);
  const auto updated_header = ec_omap_journal.get_updated_header(oid.hobj);

  if (const int r = store->omap_get(c_, oid, header, out);
    r < 0) {
    return r;
  }

  // Update header if present
  if (updated_header) {
    *header = *updated_header;
    dout(20) << __func__ << ": oid=" << oid
            << " updated_header=true header_size=" << header->length() << dendl;
  } else {
    dout(20) << __func__ << ": oid=" << oid
            << " updated_header=false header_size=" << header->length() << dendl;
  }

  // Remove keys in removed_ranges
  remove_keys_in_ranges(removed_ranges, out);

  // Apply updates in update_map
  for (const auto &[key, val_opt] : update_map) {
    if (val_opt.value.has_value()) {
      (*out)[key] = *(val_opt.value);
    } else {
      out->erase(key);
    }
  }

  return 0;
}

int ECBackend::omap_check_keys(
  ObjectStore::CollectionHandle &c_,    ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  const std::set<std::string> &keys, ///< [in] Keys to check
  std::set<std::string> *out,         ///< [out] Subset of keys defined on oid
  ObjectStore *store
) {
  if (!get_parent()->get_pool().supports_omap()) {
    return -EOPNOTSUPP;
  }

  // Update map takes priority over removed_ranges
  auto [update_map, removed_ranges] = ec_omap_journal.get_value_updates(oid.hobj);
  auto updated_header = ec_omap_journal.get_updated_header(oid.hobj);

  // First check keys in update_map and removed_ranges
  set<string> keys_to_check_on_disk;
  for (const auto &key : keys) {
    if (auto it = update_map.find(key);
      it != update_map.end()) {
      if (it->second.value.has_value()) {
        out->insert(key);
      }
    } else if (should_be_removed(removed_ranges, key)) {
      continue;
    } else {
      keys_to_check_on_disk.insert(key);
    }
  }

  const int r = store->omap_check_keys(c_, oid, keys_to_check_on_disk, out);

  return r;
}

bool ECBackend::should_be_removed(
  const std::map<std::string, std::optional<std::string>>& removed_ranges,
  const std::string_view key) {
  if (removed_ranges.empty()) {
    return false;
  }
  
  // Find range that comes after this key
  auto it = removed_ranges.upper_bound(std::string(key));

  // If all ranges start after the key, it can't be in any range
  if (it == removed_ranges.begin()) {
    return false;
  }

  // Go back to the previous range
  --it;
  // If this range contains the key, return true
  const auto& end_opt = it->second;
  if (!end_opt || key < *end_opt) {
    return true;
  }

  // No ranges contain the key, return false
  return false;
}

void ECBackend::remove_keys_in_ranges(
  const std::map<std::string, std::optional<std::string>>& removed_ranges,
  std::map<std::string, ceph::buffer::list>* out) {
  for (const auto& [start, end] : removed_ranges) {
    out->erase(out->lower_bound(start),
               end ? out->lower_bound(*end) : out->end());
  }
}
