// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/osd_ofi.h"

#include <algorithm>
#include <cstring>
#include <string_view>
#include <thread>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ofi_rma.h"
#include "common/oob_arena.h"
#include "common/oob_config.h"
#include "osd/osd_types.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd_ofi: "

OSDOfi::OSDOfi(CephContext* cct, LogChannelRef clog)
  : cct(cct), clog(std::move(clog)) {}

OSDOfi::~OSDOfi()
{
  // the windows, then the endpoint, which stops the progress thread;
  // the arena, which holds their memory, goes after this executor
  windows.reset();
  ep.reset();
}

namespace {
/// how long OOB_CUTOFF_LATE and OOB_CUTOFF_PAST_TOLERANCE stay raised
constexpr std::chrono::hours ALERT_PERIOD{1};
/// the staging allocations and frees that threw, each of which may have
/// lost a buffer of the arena, which every executor of the daemon shares
std::atomic<uint64_t> staging_leaked{0};

/// A cut-off that could not reopen the endpoint leaves it broken, though
/// not unsafe: no write is at risk, but it fails every transfer from then
/// on. Stop, as for an unsafe endpoint, and say so once. Asked only once
/// a transfer failed, since the endpoint's stats take a lock.
void stop_if_broken(CephContext* cct, const LogChannelRef& clog,
		    const ceph::ofi::Endpoint& ep, std::atomic<bool>& stopped)
{
  // broken before unsafe: the endpoint sets unsafe first, and an unsafe
  // one is check_unsafe()'s to stop, under osd_oob_cutoff_policy
  if (stopped || !ep.stats().broken || ep.unsafe() ||
      stopped.exchange(true)) {
    return;
  }
  const std::string why = ep.last_error();
  derr << "libfabric delivery could not reopen its endpoint: " << why
       << "; libfabric delivery stopped on this osd" << dendl;
  if (clog) {
    clog->error() << "libfabric delivery could not reopen its endpoint: "
		  << why << "; libfabric delivery stopped";
  }
}
}

int OSDOfi::init(const std::string& provider,
		 const ceph::oob::location_t& loc, bool lend_windows,
		 ceph::oob::Arena* arena)
{
  const auto& conf = cct->_conf;
  this->arena = arena;
  ceph::ofi::config_t cfg;
  cfg.provider = provider;
  std::string alt_domain;
  ceph::oob::ofi_where(loc, provider, &cfg.domain, &cfg.node, &alt_domain);
  // each transfer takes a buffer of the arena at its size, at most
  // Arena::MAX bytes; the endpoint registers the whole arena once
  cfg.stage_mem = arena->base();
  cfg.stage_mem_len = arena->size();
  cfg.stage_size = ceph::oob::Arena::MAX;
  // ofi-rma calls these from C with the endpoint's lock held, stage_free
  // on its progress thread too, so no exception may leave them; the
  // arena's free lists allocate as its buffers split and join
  cfg.stage_alloc = [arena](size_t len, uint64_t* off) noexcept {
    try {
      const auto s = arena->alloc(len);
      if (!s) {
	return -EBUSY;
      }
      *off = s.offset;
      return 0;
    } catch (...) {
      staging_leaked++;
      return -ENOMEM;
    }
  };
  cfg.stage_free = [arena](uint64_t off, size_t len) noexcept {
    try {
      arena->free(off, len);
    } catch (...) {
      staging_leaked++;
    }
  };
  // windows only fill while someone polls a manual-progress provider
  cfg.progress_thread = lend_windows;
  // the payloads of OSD-direct PUTs: a provider without reads still opens,
  // for delivery only
  cfg.reads = true;
  cfg.late_tolerance =
    conf.get_val<std::chrono::milliseconds>("osd_oob_cutoff_late_tolerance");
  // a startup option: the endpoint judges late cut-offs by it from here
  // on, and check_unsafe() acts on it
  const auto policy = conf.get_val<std::string>("osd_oob_cutoff_policy");
  cfg.late_fail_closed = policy != "warn";
  abort_on_unsafe = policy == "abort";
  // A FI_THREAD_DOMAIN provider polls nothing while it adds a client:
  // the shards' pushes into gather windows lent before land first, so
  // that none waits for the insert. A shard starts its push within the
  // pool's lease and ends it within its drain; a pool that sets them
  // longer than the defaults may still have one wait.
  cfg.lent_wait = std::chrono::milliseconds(static_cast<int64_t>(
    (pg_pool_t::DEFAULT_RDMA_DELIVERY_LEASE +
     pg_pool_t::DEFAULT_RDMA_DELIVERY_DRAIN) * 1000.0));
  cfg.cutoff_close_hook = [cct = cct] {
    // as a provider that drains its writes on close, not discards them
    const auto delay = cct->_conf.get_val<std::chrono::milliseconds>(
      "osd_ofi_inject_cutoff_delay");
    if (delay.count() > 0) {
      std::this_thread::sleep_for(delay);
    }
    return cct->_conf.get_val<bool>("osd_ofi_inject_cutoff_failure") ?
      -EIO : 0;
  };
  if (ceph::oob::is_verbs_provider(provider) && cfg.domain.empty() &&
      !loc.addr.empty()) {
    derr << "no RDMA device carries " << loc.addr << dendl;
    return -ENODEV;
  }
  // a window gets a new key when it is released, and so does a staging
  // buffer's region when the endpoint re-registers it, which needs the
  // verbs registration cache off: ceph-osd's main() sets
  // FI_MR_CACHE_MONITOR before any thread starts
  std::string err;
  const auto open = [&] { ep = ceph::ofi::Endpoint::open(cfg, &err); };
  std::string no_raw;
  const auto open_endpoint = [&] {
    if (!ceph::oob::needs_net_raw(provider)) {
      open();
    } else if (ceph::oob::with_net_raw(open, &no_raw) == -EPERM) {
      // see global_init(): the switch to another user comes before the
      // mon config
      no_raw += "; an osd started with --setuser keeps it only when "
	"ceph.conf or its command line, not the mon config alone, makes " +
	provider + " the provider of its endpoint: oob_cluster_transport, "
	"or else the first libfabric provider in oob_client_transports";
    }
  };
  open_endpoint();
  if (!ep && !alt_domain.empty()) {
    // the uet provider on a device model such as rocm-ernic names its
    // domain after the RDMA device, not after the interface
    const std::string first = cfg.domain + ": " + err;
    cfg.domain = alt_domain;
    no_raw.clear();
    open_endpoint();
    if (!ep) {
      err = first + "; " + cfg.domain + ": " + err;
    }
  }
  if (!ep) {
    derr << "libfabric endpoint: " << err
	 << (no_raw.empty() ? "" : "; " + no_raw) << dendl;
    return -EIO;
  }

  if (lend_windows) {
    constexpr size_t run = GATHER_WINDOW_SIZE * GATHER_WINDOW_COUNT;
    if (char* mem = arena->reserve(run)) {
      // a window per slot, so that each gets a key of its own, and a new
      // one when it is released
      windows = ceph::ofi::WindowPool::create(*ep, mem, GATHER_WINDOW_SIZE,
					      GATHER_WINDOW_COUNT, &err);
      if (!windows) {
	derr << "registering the gather windows: " << err
	     << "; gathers stay inline" << dendl;
	// nothing registered them: the run goes back to staging
	arena->unreserve(mem, run);
      }
    } else {
      derr << "oob_memory has no room for " << GATHER_WINDOW_COUNT
	   << " gather windows of " << GATHER_WINDOW_SIZE << " bytes and for "
	   << "staging; gathers stay inline" << dendl;
    }
  }
  dout(1) << "libfabric delivery up: " << ep->describe() << ", staging in "
	  << arena->size() << " bytes, " << (windows ? windows->count() : 0)
	  << " gather windows of " << GATHER_WINDOW_SIZE << " bytes" << dendl;
  // the core provider, without the utility provider layered on it: tcp
  // of "tcp;ofi_rxm"
  const std::string_view prov = ep->provider();
  const std::string_view core = prov.substr(0, prov.find(';'));
  const bool kernel_sockets =
    core == "tcp" || core == "sockets" || core == "udp";
  if (kernel_sockets) {
    // closing a socket does not discard what the kernel already queued
    // on it, so a cut-off write can still land after the pool's drain
    dout(0) << "WARNING: " << prov << " cannot cut off writes "
	    << "that miss their deadline: the kernel still delivers what it "
	    << "queued. Use it for tests only." << dendl;
  }
  if (const auto st = ep->stats(); st.close_discards == 0) {
    // a late write is then only cut off once the provider has drained it,
    // which the endpoint measures, and treats as unsafe when the drain
    // takes longer than the tolerance
    dout(0) << "WARNING: " << prov << " says closing an endpoint "
	    << "does not discard its writes; a write that misses its deadline "
	    << "can land late, and a cut-off that takes longer than "
	    << "osd_oob_cutoff_late_tolerance stops libfabric delivery"
	    << dendl;
  } else if (st.close_discards < 0 && core == "shm") {
    // shm, the other provider that moves data in software, does not say
    // either: what it queued for a peer may still run once the endpoint
    // closed
    dout(0) << "WARNING: " << prov << " does not say whether closing an "
	    << "endpoint discards its writes; a write that misses its "
	    << "deadline may still land after the pool's drain. Use it for "
	    << "tests only." << dendl;
  }
  if (!ep->delivery_complete()) {
    dout(0) << "WARNING: " << ep->provider() << " does not promise "
	    << "delivery-complete writes; a reply may race the bytes it "
	    << "reports placed" << dendl;
  }
  return 0;
}

bool OSDOfi::is_available() const
{
  return ep && !stopped && !ep->unsafe();
}

bool OSDOfi::check_unsafe()
{
  if (!ep || !ep->unsafe()) {
    return false;
  }
  if (stopped.exchange(true)) {
    return true;
  }
  const std::string why = ep->last_error();
  derr << "libfabric delivery is unsafe: " << why
       << "; libfabric delivery stopped on this osd" << dendl;
  if (clog) {
    clog->error() << "libfabric delivery is unsafe: " << why
		  << "; libfabric delivery stopped";
  }
  if (abort_on_unsafe) {
    ceph_abort_msg("libfabric delivery could not cut off its writes (" + why +
		   "); osd_oob_cutoff_policy=abort");
  }
  return true;
}

void OSDOfi::check_past_tolerance()
{
  const auto s = ep->stats();
  uint64_t seen = past_reported;
  if (s.cutoffs_past_tolerance <= seen ||
      !past_reported.compare_exchange_strong(seen, s.cutoffs_past_tolerance)) {
    return;
  }
  // The close or cancel returned, so nothing of the writes it cut off is
  // sent any more, and delivery goes on. Bytes may have landed in a
  // client's window after the fence a client that gave its request up
  // counts on: say so where an operator looks.
  const std::string why = ep->last_error();
  derr << "libfabric delivery cut writes off late: " << why << dendl;
  if (clog) {
    clog->error() << "libfabric delivery cut writes off late: " << why;
  }
}

bool OSDOfi::handles(const std::string& token) const
{
  if (!is_available()) {
    return false;
  }
  // the wire, not the provider: two vendors' providers of one protocol
  // take each other's tokens
  auto t = ceph::ofi::parse_token(token);
  return t && t->wire == ep->wire();
}

ssize_t OSDOfi::execute_plan(const std::string& key,
			     const std::string& token,
			     const ceph::buffer::list& data,
			     const ceph::osd::oob::placement_plan& plan,
			     std::chrono::milliseconds budget,
			     bool* started)
{
  if (started) {
    *started = false;
  }
  auto t = ceph::ofi::parse_token(token);
  if (!ep || !t) {
    return -EINVAL;
  }
  std::vector<ceph::ofi::Endpoint::write_t> writes;
  writes.reserve(plan.size());
  for (const auto& tr : plan) {
    writes.push_back({tr.local_ofs, tr.len, tr.client_ofs});
  }
  plans_started++;
  int r = 0;
  std::vector<struct iovec> iov;
  iov.reserve(data.get_num_buffers());
  for (const auto& b : data.buffers()) {
    if (b.length()) {
      iov.push_back({const_cast<char*>(b.c_str()), b.length()});
    }
  }
  r = ep->write(*t, iov.data(), iov.size(), writes, budget, started);
  if (r == -ENOTRECOVERABLE || ep->unsafe()) {
    check_unsafe();
  } else if (r == -ETIMEDOUT) {
    check_past_tolerance();
  }
  if (r < 0) {
    plans_failed++;
    dout(5) << "plan for " << key << " failed: " << cpp_strerror(r)
	    << (r == -EIO || r == -ETIMEDOUT || r == -ECANCELED ||
		r == -ENOTRECOVERABLE ?
		" (" + ep->last_error() + ")" :
		std::string{}) << dendl;
    stop_if_broken(cct, clog, *ep, stopped);
    return r;
  }
  // what the plan moved, which its caller checks against the payload
  const uint64_t placed = ceph::osd::oob::plan_bytes(plan);
  plans_completed++;
  bytes_pushed += placed;
  dout(20) << "placed " << placed << " bytes of " << key << " in "
	   << plan.size() << " range(s)" << dendl;
  return static_cast<ssize_t>(placed);
}

bool OSDOfi::pulls(const std::string& token) const
{
  return ep && ep->reads() && handles(token);
}

ssize_t OSDOfi::execute_pull(const std::string& key,
			     const std::string& token,
			     const ceph::osd::oob::placement_plan& plan,
			     uint64_t total,
			     ceph::buffer::list* out,
			     std::chrono::milliseconds budget,
			     bool* started)
{
  if (started) {
    *started = false;
  }
  auto t = ceph::ofi::parse_token(token);
  if (!ep || !t) {
    return -EINVAL;
  }
  if (!ep->reads()) {
    return -EOPNOTSUPP;
  }
  if (total > ceph::oob::Arena::MAX) {
    // larger than a staging buffer, which ofi-rma refuses too: take no
    // buffer of its size for nothing
    return -E2BIG;
  }
  std::vector<ceph::ofi::Endpoint::read_t> reads;
  reads.reserve(plan.size());
  for (const auto& tr : plan) {
    reads.push_back({tr.client_ofs, tr.len, tr.local_ofs});
  }
  // ofi-rma reads into its staging and copies into this buffer only once
  // every read completed, so a failed pull leaves it untouched
  ceph::buffer::ptr bp = ceph::buffer::create_page_aligned(total);
  struct iovec iov{bp.c_str(), total};
  pulls_started++;
  const int r = ep->read(*t, &iov, 1, reads, budget, started);
  if (r == -ENOTRECOVERABLE || ep->unsafe()) {
    check_unsafe();
  } else if (r == -ETIMEDOUT) {
    check_past_tolerance();
  }
  if (r < 0) {
    pulls_failed++;
    dout(5) << "pull for " << key << " failed: " << cpp_strerror(r)
	    << (r == -EIO || r == -ETIMEDOUT || r == -ECANCELED ||
		r == -ENOTRECOVERABLE || r == -EACCES ?
		" (" + ep->last_error() + ")" :
		std::string{}) << dendl;
    stop_if_broken(cct, clog, *ep, stopped);
    return r;
  }
  pulls_completed++;
  bytes_pulled += total;
  out->clear();
  out->append(std::move(bp));
  dout(20) << "pulled " << total << " bytes of " << key << " in "
	   << plan.size() << " range(s)" << dendl;
  return static_cast<ssize_t>(total);
}

std::optional<OSDOobExecutor::window_t> OSDOfi::acquire_window(size_t size)
{
  if (!windows || !is_available()) {
    return std::nullopt;
  }
  auto lent = windows->acquire(size);
  if (!lent) {
    return std::nullopt;
  }
  window_t w;
  w.id = lent->id;
  w.ptr = lent->ptr;
  w.size = lent->size;
  w.token = std::move(lent->token);
  return w;
}

void OSDOfi::release_window(uint64_t id, uint64_t quarantine_ms)
{
  if (!windows) {
    return;
  }
  // A new key, so that no write meant for this gather lands once the
  // window is lent again: not a peer's that missed it, and not a
  // provider's late duplicate of one that completed, which the delivery
  // lease does not bound. A gather that consumed the push gives no
  // quarantine, and the window is free at once. One that did not - the
  // shard replied inline, or the read was cancelled - keeps it out for
  // the quarantine anyway: the shard's push may still be on the way, and
  // the new key keeps it out only if the provider drops what carries the
  // old one. An insert that holds a FI_THREAD_DOMAIN endpoint puts the
  // re-key off to a later acquire, rather than hold up this op thread.
  windows->release(id, std::chrono::milliseconds(quarantine_ms), true);
}

bool OSDOfi::delivery_complete() const
{
  return ep && ep->delivery_complete();
}

void OSDOfi::window_sync()
{
  if (ep) {
    ep->sync();
  }
}

void OSDOfi::dump_stats(ceph::Formatter* f) const
{
  f->dump_bool("available", is_available());
  if (!ep) {
    return;
  }
  f->dump_string("transport", ep->describe());
  if (arena) {
    arena->dump(f);
  }
  f->dump_unsigned("plans_started", plans_started);
  f->dump_unsigned("plans_completed", plans_completed);
  f->dump_unsigned("plans_failed", plans_failed);
  f->dump_unsigned("bytes_pushed", bytes_pushed);
  f->dump_bool("pulls", ep->reads());
  f->dump_unsigned("pulls_started", pulls_started);
  f->dump_unsigned("pulls_completed", pulls_completed);
  f->dump_unsigned("pulls_failed", pulls_failed);
  f->dump_unsigned("bytes_pulled", bytes_pulled);
  const auto s = ep->stats();
  f->dump_unsigned("writes_posted", s.writes_posted);
  f->dump_unsigned("writes_failed", s.writes_failed);
  f->dump_unsigned("reads_posted", s.reads_posted);
  f->dump_unsigned("reads_failed", s.reads_failed);
  f->dump_unsigned("peers_inserted", s.peers_inserted);
  f->dump_unsigned("staging_busy", s.staging_busy);
  f->dump_unsigned("staging_leaked", staging_leaked);
  f->dump_unsigned("timeouts", s.timeouts);
  f->dump_unsigned("cutoffs", s.resets);
  f->dump_unsigned("plans_cut_off", s.plans_cut_off);
  f->dump_unsigned("cancels_failed", s.cancels_failed);
  f->dump_bool("cancel_discards", s.cancel_discards);
  f->dump_int("close_discards", s.close_discards);
  f->dump_unsigned("cutoffs_failed", s.cutoffs_failed);
  f->dump_unsigned("cutoffs_late", s.cutoffs_late);
  f->dump_unsigned("max_cutoff_lateness_ms", s.max_cutoff_lateness_ms);
  f->dump_unsigned("late_tolerance_ms", s.late_tolerance_ms);
  f->dump_unsigned("cutoffs_past_tolerance", s.cutoffs_past_tolerance);
  f->dump_unsigned("max_poll_gap_ms", s.max_poll_gap_ms);
  f->dump_unsigned("cutoffs_on_behalf", s.cutoffs_on_behalf);
  f->dump_unsigned("cutoff_slack_ms", s.cutoff_slack_ms);
  f->dump_bool("unsafe", s.unsafe);
  f->dump_bool("broken", s.broken);
  f->dump_unsigned("cutoff_cost_ms", s.cutoff_cost_ms);
  f->dump_unsigned("budget_refused", s.budget_refused);
  f->dump_unsigned("late_starts", s.late_starts);
  f->dump_unsigned("peer_timeouts", s.peer_timeouts);
  f->dump_unsigned("peers", s.peers);
  f->dump_unsigned("pending_inserts", s.pending_inserts);
  f->dump_unsigned("inserts_refused", s.inserts_refused);
  f->dump_unsigned("insert_busy", s.insert_busy);
  const auto ws = windows ? windows->stats() : ceph::ofi::WindowPool::stats_t{};
  f->dump_unsigned("windows_acquired", ws.acquired);
  f->dump_unsigned("windows_exhausted", ws.exhausted);
  f->dump_unsigned("windows_declined_insert", ws.declined_insert);
  f->dump_unsigned("windows_rekeys_put_off", ws.rekeys_put_off);
  f->dump_unsigned("gather_crc_mismatch", get_gather_crc_mismatch());
  f->dump_unsigned("windows_rekeyed", ws.rekeyed);
  f->dump_unsigned("windows_rekeyed_quarantined", ws.rekeyed_quarantined);
  f->dump_unsigned("windows_rekey_failed", ws.rekey_failed);
  f->dump_unsigned("rekeys_in_place", s.rekeys_in_place);
  f->dump_unsigned("rekeys_reregistered", s.rekeys_reregistered);
  f->dump_int("rekey_in_place", s.rekey_in_place);
  f->dump_unsigned("key_collisions", s.key_collisions);
  f->dump_string("last_error", ep->last_error());
}

void OSDOfi::get_alerts(std::map<std::string, std::string>& alerts) const
{
  if (!ep) {
    return;
  }
  const auto s = ep->stats();
  // an unsafe endpoint is broken too: unsafe says more
  if (s.unsafe) {
    alerts.emplace("OOB_DELIVERY_UNSAFE",
		   "libfabric delivery could not cut off writes in time (" +
		   ep->last_error() + "); libfabric delivery stopped");
  } else if (s.broken) {
    alerts.emplace("OOB_DELIVERY_DOWN",
		   "libfabric delivery could not reopen its endpoint (" +
		   ep->last_error() + "); libfabric delivery stopped");
  }
  // the late cut-offs stay raised alongside: writes may have landed late
  // whether or not delivery stopped since
  const bool goes_on = !s.unsafe && !s.broken;
  const auto now = std::chrono::steady_clock::now();
  const auto period = std::chrono::seconds(ALERT_PERIOD);
  const auto minutes = std::to_string(
    std::chrono::duration_cast<std::chrono::minutes>(period).count());
  if (s.cutoffs_past_tolerance && now - s.last_past_cutoff < period) {
    std::string pause;
    if (s.max_poll_gap_ms) {
      pause = "; its threads once went " + std::to_string(s.max_poll_gap_ms) +
	" ms without running, as when the process is stopped or paused";
    }
    alerts.emplace("OOB_CUTOFF_PAST_TOLERANCE",
		   std::to_string(s.cutoffs_past_tolerance) + " write cut-off(s) "
		   "ended later than the tolerance of " +
		   std::to_string(s.late_tolerance_ms) + " ms, the latest "
		   "within the last " + minutes + " minutes, by up to " +
		   std::to_string(s.max_cutoff_lateness_ms) + " ms" + pause +
		   (goes_on ? "; delivery goes on" : "") + ". A client that "
		   "gave a request up and reused its window after the pool's "
		   "lease and drain may have had it written");
  } else if (!s.unsafe && s.cutoffs_late &&
	     now - s.last_late_cutoff < period) {
    // not next to an unsafe endpoint: a close or cancel that took longer
    // than the tolerance made it unsafe, and counts only as late although
    // it ended past the tolerance. The unsafe alert says how late.
    alerts.emplace("OOB_CUTOFF_LATE",
		   std::to_string(s.cutoffs_late) + " write cut-off(s) ended "
		   "after their budget, the latest within the last " + minutes +
		   " minutes, by up to " +
		   std::to_string(s.max_cutoff_lateness_ms) + " ms, within "
		   "the tolerance of " + std::to_string(s.late_tolerance_ms) +
		   " ms; a client that gives a request up must allow that "
		   "beyond the pool's delivery lease and drain");
  }
}
