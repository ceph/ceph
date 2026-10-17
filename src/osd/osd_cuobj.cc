// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd_cuobj.h"
#include "osd/osd_dc_target.h"

#include <cuobjserver.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <thread>

#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "common/config.h"
#include "common/oob_arena.h"
#include "common/oob_config.h"
#include "common/debug.h"
#include "common/Formatter.h"
#include "common/rdma_token.h"

#define dout_context m_cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd_cuobj "

namespace {
/// the DC initiators' transport ACK timeout, as the IB exponent: a write
/// not acknowledged within 4.096 us * 2^14, about 67 ms, is sent again
constexpr uint8_t CUOBJ_ACK_TIMEOUT = 14;
/// how often a write not acknowledged is sent again before its QP fails:
/// the most IB allows
constexpr uint8_t CUOBJ_RETRY_COUNT = 7;
static_assert(CUOBJ_ACK_TIMEOUT > 0 && CUOBJ_ACK_TIMEOUT < 32 &&
	      CUOBJ_RETRY_COUNT <= 7);

/// how long a posted cuObject write can go on retrying after the path
/// last acknowledged a packet of it: the first send and each retry wait
/// out the ACK timeout, which IB lets a NIC run at up to four times its
/// nominal value. do_init() sets both, rather than leave them to the
/// library's defaults (16 and 7, 8.6 s by this rule). A plan stops
/// posting and waiting this long before its budget runs out, which
/// bounds a write that is not acknowledged after that. One still moving
/// then restarts the count with each acknowledged packet, and cuObject
/// cannot cancel it: it can land past the budget by as long as it goes
/// on moving.
constexpr auto CUOBJ_RETRY_BUDGET =
  std::chrono::ceil<std::chrono::milliseconds>(
    std::chrono::nanoseconds(4096ull << CUOBJ_ACK_TIMEOUT) * 4 *
    (CUOBJ_RETRY_COUNT + 1));

/// writes posted at once, and events taken per poll() (the library
/// caps poll() at 16 events and documents no larger per-channel bound)
constexpr int POLL_BATCH = 16;

/// A plan polls its channel with only a yield in between for POLL_SPIN
/// after it starts or last sees a completion, then sleeps POLL_SLEEP
/// between polls. A sleep lasts about 50 us more than asked, the kernel's
/// default timer slack, so a plan that slept at once would wait about
/// 55 us for a write that completes in a few. Spinning about as long as
/// one sleep lasts at most doubles the wait for a write that is slower.
constexpr auto POLL_SPIN = std::chrono::microseconds(50);
constexpr auto POLL_SLEEP = std::chrono::microseconds(5);

/// A write's async handle is a number unique in the process, not a
/// pointer into its plan: a write that a plan left posted when it timed
/// out completes during a later plan on the channel, and its handle
/// must still name the plan that posted it, not point at memory that
/// may since be the later plan's own.
static_assert(sizeof(uintptr_t) >= sizeof(uint64_t));

void* as_handle(uint64_t h)
{
  return reinterpret_cast<void*>(static_cast<uintptr_t>(h));
}

uint64_t handle_value(const void* handle)
{
  return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(handle));
}
}

thread_local uint16_t OSDCuObj::tls_channel_id = 0;
thread_local bool OSDCuObj::tls_channel_valid = false;
thread_local ceph::osd::oob::abandoned_plans<OSDCuObj::held_t>
  OSDCuObj::tls_abandoned;
thread_local std::vector<OSDCuObj::delayed_t> OSDCuObj::tls_delayed;

// per-call limit of the cuObj API
static constexpr size_t MAX_RDMA_OP_SIZE = 1ULL << 30;

OSDCuObj::OSDCuObj(CephContext *cct, const std::string& rdma_ip,
		   bool lend_windows, int num_dcis, ceph::oob::Arena* arena)
  : m_cct(cct), m_arena(arena)
{
  if (do_init(rdma_ip, lend_windows, num_dcis) < 0) {
    do_shutdown();
  }
}

OSDCuObj::~OSDCuObj()
{
  do_shutdown();
}

int OSDCuObj::do_init(const std::string& rdma_ip, bool lend_windows,
		      int num_dcis)
{
  // the library chooses its rdma_cm port; no client connects to it
  constexpr uint16_t rdma_port = 0;
  uint64_t dc_key = 0;
  if (std::string err; ceph::oob::dc_key(m_cct, &dc_key, &err) < 0) {
    derr << "ERROR: " << err << dendl;
    return -EINVAL;
  }

  cuObjRDMATunable params;
  params.setNumDcis(num_dcis);
  params.setDcKey(dc_key);
  // the retry budget that the plans' deadlines count on
  params.setTimeout(CUOBJ_ACK_TIMEOUT);
  params.setRetryCount(CUOBJ_RETRY_COUNT);

  dout(1) << "initializing cuObjServer on " << rdma_ip << ":" << rdma_port
	  << " dcis=" << num_dcis << ", DC retry budget "
	  << CUOBJ_RETRY_BUDGET.count() << " ms, staging in "
	  << m_arena->size() << " bytes" << dendl;

  try {
    m_server = std::make_unique<cuObjServer>(
      rdma_ip.c_str(), rdma_port, CUOBJ_PROTO_RDMA_DC_V1, params);
  } catch (const std::exception& e) {
    derr << "ERROR: cuObjServer construction failed: " << e.what() << dendl;
    return -EIO;
  }

  if (!m_server->isConnected()) {
    derr << "ERROR: cuObjServer RDMA session failed to start" << dendl;
    m_server.reset();
    return -ECONNREFUSED;
  }

  // the whole arena, once: each transfer stages in a buffer of it, which
  // the library addresses by its offset
  m_arena_handle = m_server->registerBuffer(m_arena->base(), m_arena->size());
  if (!m_arena_handle) {
    derr << "ERROR: registerBuffer failed for " << m_arena->size()
	 << " bytes of out-of-band memory" << dendl;
    return -EIO;
  }

  if (lend_windows) {
    // a DC target so peers can push their shard reads into this OSD
    // when it gathers an erasure-coded read
    m_win_size = GATHER_WINDOW_SIZE;
    constexpr size_t run = GATHER_WINDOW_SIZE * GATHER_WINDOW_COUNT;
    auto dct = std::make_unique<OSDDcTarget>();
    char* mem = m_arena->reserve(run);
    if (!mem) {
      derr << "WARNING: oob_memory has no room for " << GATHER_WINDOW_COUNT
	   << " gather windows of " << GATHER_WINDOW_SIZE << " bytes and for "
	   << "staging; peers will reply inline" << dendl;
    } else if (dct->open(m_cct, rdma_ip, mem, run, dc_key) == 0) {
      m_dct = std::move(dct);
      m_win_slots.resize(GATHER_WINDOW_COUNT);
    } else {
      // the target registered nothing that stays: the run goes back to
      // staging
      m_arena->unreserve(mem, run);
      derr << "WARNING: no DC target for gather windows; peers will reply "
	   << "inline" << dendl;
    }
  }
  return 0;
}

std::optional<OSDOobExecutor::window_t> OSDCuObj::acquire_window(size_t size)
{
  if (!m_dct || size > m_win_size || m_quiesced) {
    return std::nullopt;
  }
  const auto now = std::chrono::steady_clock::now();
  std::lock_guard l(m_win_mtx);
  for (size_t i = 0; i < m_win_slots.size(); i++) {
    auto& slot = m_win_slots[i];
    if (slot.in_use || now < slot.quarantined_until) {
      continue;
    }
    slot.in_use = true;
    window_t w;
    w.id = i;
    w.ptr = m_dct->pool() + i * m_win_size;
    w.size = m_win_size;
    w.token = m_dct->token(i * m_win_size, m_win_size);
    return w;
  }
  return std::nullopt;
}

void OSDCuObj::release_window(uint64_t id, uint64_t quarantine_ms)
{
  std::lock_guard l(m_win_mtx);
  if (id >= m_win_slots.size()) {
    return;
  }
  m_win_slots[id].in_use = false;
  if (quarantine_ms) {
    m_win_slots[id].quarantined_until = std::chrono::steady_clock::now() +
      std::chrono::milliseconds(quarantine_ms);
  }
}

void OSDCuObj::do_shutdown()
{
  if (m_arena_handle && m_server) {
    m_server->deRegisterBuffer(m_arena_handle);
  }
  m_arena_handle = nullptr;
  m_server.reset();
}

bool OSDCuObj::handles(const std::string& token) const
{
  return !ceph::rdma::is_ofi_token(token);
}

bool OSDCuObj::is_available() const
{
  return m_server && m_server->isConnected();
}

uint16_t OSDCuObj::get_channel_id()
{
  if (!tls_channel_valid) {
    uint16_t id = m_server->allocateChannelId();
    if (id == invalid_channel) {
      derr << "ERROR: cuObject channel allocation failed"
	   << " (one DC initiator for each thread that runs transfers)"
	   << dendl;
      return invalid_channel;
    }
    tls_channel_id = id;
    tls_channel_valid = true;
    dout(20) << "allocated channel " << id << " for this thread" << dendl;
  }
  return tls_channel_id;
}

OSDCuObj::BufEntry* OSDCuObj::acquire_buffer(size_t needed)
{
  // a buffer of the arena at the transfer's size; a transfer larger than
  // Arena::MAX, or one that finds the arena full, is delivered inline
  const auto slice = m_arena->alloc(needed);
  if (!slice) {
    dout(5) << "no out-of-band buffer of " << needed << " bytes free"
	    << dendl;
    return nullptr;
  }
  auto entry = new BufEntry;
  entry->ptr = m_arena->ptr(slice);
  entry->size = slice.len;
  entry->offset = slice.offset;
  entry->handle = m_arena_handle;
  return entry;
}

void OSDCuObj::release_buffer(BufEntry* buf)
{
  if (!buf) {
    return;
  }
  m_arena->free(buf->offset, buf->size);
  delete buf;
}

void OSDCuObj::credit_abandoned(uint64_t handle)
{
  using credit_t = ceph::osd::oob::abandoned_plans<held_t>::credit_t;
  m_stale_completions++;
  held_t held;
  switch (tls_abandoned.credit(handle, &held)) {
  case credit_t::unknown:
    // its plan's buffer went back when the channel was reset, it is
    // what a failed submission posted, the library returned a write
    // that this channel did not post, or it returned one a second time
    // after the plan that posted it took the first: the running plan,
    // or a timed-out plan whose last write completed since
    dout(5) << "completion of write " << handle << " that no plan on "
	    << "this channel waits for" << dendl;
    break;
  case credit_t::repeated:
    // the library returned the write's completion twice: the plan still
    // waits for the writes that it has not returned yet
    dout(5) << "completion of write " << handle << " of a timed-out plan, "
	    << "which completed already" << dendl;
    break;
  case credit_t::counted:
    break;
  case credit_t::last:
    // the plan's last write is done with its buffer
    dout(10) << "timed-out plan of write " << handle << " completed its "
	     << "last write; staging buffer "
	     << (held.partial ? "kept for its failed submission" : "reclaimed")
	     << dendl;
    settle(held);
    break;
  }
}

void OSDCuObj::settle(const held_t& held)
{
  if (!held.partial) {
    release_buffer(held.buf);
    m_buffers_reclaimed++;
    return;
  }
  // the part of the failed submission goes out once what is ahead of it
  // on the channel is done: the plan's own writes, which have all
  // completed now, and the parts of earlier failed submissions, by their
  // own time. The DC retry budget runs on it from then.
  auto start = std::chrono::steady_clock::now();
  for (const auto& d : tls_delayed) {
    start = std::max(start, d.until);
  }
  tls_delayed.push_back({held.buf, start + CUOBJ_RETRY_BUDGET});
  note_retry_until(start + CUOBJ_RETRY_BUDGET);
}

void OSDCuObj::release_delayed(bool all)
{
  const auto now = std::chrono::steady_clock::now();
  for (auto it = tls_delayed.begin(); it != tls_delayed.end(); ) {
    if (!all && now < it->until) {
      ++it;
      continue;
    }
    release_buffer(it->buf);
    m_buffers_reclaimed++;
    it = tls_delayed.erase(it);
  }
}

bool OSDCuObj::channel_busy()
{
  return !tls_abandoned.empty() || !tls_delayed.empty();
}

void OSDCuObj::drop_abandoned()
{
  for (const auto& held : tls_abandoned.drop()) {
    release_buffer(held.buf);
    m_buffers_reclaimed++;
  }
  // the reset flushed what failed submissions posted too
  release_delayed(true);
}

void OSDCuObj::reap_abandoned(uint16_t channel)
{
  // the buffers of failed submissions, once the DC retry budget has run
  // out on what they may have posted
  release_delayed(false);
  if (tls_abandoned.empty()) {
    return;
  }
  cuObjAsyncEvent_t events[POLL_BATCH];
  for (auto& e : events) {
    e.async_handle = nullptr;
  }
  int n = m_server->poll(events, POLL_BATCH, channel);
  // a failed poll returns no completion count: scan for the events
  // that were filled, as execute_plan does
  for (const auto& e : events) {
    if (e.async_handle) {
      credit_abandoned(handle_value(e.async_handle));
    }
  }
  if (n == -EIO) {
    // a write that a timed-out plan left posted failed, and the library
    // reset the QP: nothing more of those plans completes
    dout(5) << "channel " << channel << " was reset with writes of "
	    << tls_abandoned.size() << " timed-out plans posted; "
	    << "reclaiming their staging buffers" << dendl;
    drop_abandoned();
  } else if (n < 0) {
    dout(5) << "poll of channel " << channel << " failed: " << n << dendl;
  }
}

void OSDCuObj::note_retry_until(std::chrono::steady_clock::time_point t)
{
  const auto rep = t.time_since_epoch().count();
  auto last = m_retry_until.load();
  while (last < rep && !m_retry_until.compare_exchange_weak(last, rep)) {
    // another thread moved it: last holds what it stored
  }
}

std::chrono::steady_clock::time_point OSDCuObj::abandoned_until(bool partial)
{
  // the plan's writes may wait on the DC initiator behind the parts of
  // earlier failed submissions, by their own time, and retry for the DC
  // retry budget from then; the part of its own failed submission goes
  // out once they completed, and retries as long again (settle())
  auto start = std::chrono::steady_clock::now();
  for (const auto& d : tls_delayed) {
    start = std::max(start, d.until);
  }
  return start + CUOBJ_RETRY_BUDGET * (partial ? 2 : 1);
}

void OSDCuObj::idle()
{
  // only a thread that ran plans has a channel, and anything on it
  if (!tls_channel_valid || !channel_busy() || !is_available()) {
    return;
  }
  reap_abandoned(tls_channel_id);
}

void OSDCuObj::quiesce(ceph::mono_clock::time_point until)
{
  m_quiesced = true;
  // the caller stopped the threads that run plans: what they left posted
  // can retry until m_retry_until, which each plan that stopped waiting
  // for its writes, and each failed submission, pushed out. A plan
  // started past this point fails before it stages anything.
  auto busy = [this] {
    return std::chrono::steady_clock::now().time_since_epoch().count() <
      m_retry_until.load();
  };
  if (!busy()) {
    return;
  }
  dout(1) << "waiting for the writes that timed-out plans and failed "
	  << "submissions left posted to stop retrying" << dendl;
  while (busy()) {
    if (ceph::mono_clock::now() >= until) {
      derr << "WARNING: stopping while writes still posted may retry"
	   << dendl;
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

ssize_t OSDCuObj::execute_plan(const std::string& key,
			       const std::string& token,
			       const ceph::buffer::list& data,
			       const ceph::osd::oob::placement_plan& plan,
			       std::chrono::milliseconds budget,
			       bool* started)
{
  return run_plan(false, key, token, &data, data.length(), plan, nullptr,
		  budget, started);
}

ssize_t OSDCuObj::run_plan(bool pull, const std::string& key,
			   const std::string& token,
			   const ceph::buffer::list* data, uint64_t local_len,
			   const ceph::osd::oob::placement_plan& plan,
			   ceph::buffer::list* out,
			   std::chrono::milliseconds budget, bool* started)
{
  // a push stages data and writes it to the client; a pull reads from the
  // client into the staging buffer, and hands it to *out once every read
  // completed. Either way the buffer is the NIC's until its last
  // operation completes.
  const char* what = pull ? "read" : "write";
  if (started) {
    *started = false;
  }
  if (!is_available()) {
    return -EOPNOTSUPP;
  }
  if (m_quiesced) {
    // the OSD is stopping, and the arena goes once quiesce() returns
    return -ESHUTDOWN;
  }
  auto window = ceph::rdma::parse_rdma_token(token);
  if (!window) {
    dout(5) << "malformed RDMA token for " << key << dendl;
    return -EINVAL;
  }
  // expand triples into <=1 GiB work items, validating up front
  struct work_item {
    uint64_t local_ofs;   // offset into the staged buffer
    uint64_t remote_ofs;  // offset into the client window
    uint64_t len;
  };
  std::vector<work_item> items;
  uint64_t total = 0;
  for (const auto& t : plan) {
    if (t.len == 0) {
      continue;
    }
    if (t.client_ofs > window->size || t.len > window->size - t.client_ofs ||
	t.local_ofs > local_len ||
	t.len > local_len - t.local_ofs) {
      dout(5) << "placement triple " << t.local_ofs << "/" << t.client_ofs
	      << "~" << t.len << " outside window (" << window->size
	      << ") or data (" << local_len << ") for " << key << dendl;
      return -EINVAL;
    }
    for (uint64_t done = 0; done < t.len; ) {
      const uint64_t chunk = std::min(t.len - done, MAX_RDMA_OP_SIZE);
      items.push_back({t.local_ofs + done, t.client_ofs + done, chunk});
      done += chunk;
    }
    total += t.len;
  }
  if (pull && total != local_len) {
    // every byte handed out must come from a read: the buffer still
    // holds an earlier plan's bytes, maybe another client's
    dout(5) << "pull plan for " << key << " covers " << total << " of "
	    << local_len << " bytes" << dendl;
    return -EINVAL;
  }
  if (items.empty()) {
    if (pull) {
      out->clear();
    }
    return 0;
  }
  // cuObject cannot cancel a write it posted: one still in flight when
  // the OSD stops waiting can retry for the DC retry budget after its
  // last acknowledged packet (CUOBJ_RETRY_BUDGET). Stop waiting that much
  // before the caller's budget runs out, and do not start at all when the
  // budget does not cover it - decided before anything is staged, so that
  // a plan refused here holds no buffer and is not counted as started.
  if (budget <= CUOBJ_RETRY_BUDGET) {
    dout(10) << "budget of " << budget.count() << " ms for " << key
	     << " does not cover the DC retry budget; nothing staged" << dendl;
    m_budget_refused++;
    return -ETIMEDOUT;
  }
  // counted from here, not from after staging: taking a channel and a
  // buffer of the arena, and copying the data, come out of the budget too.
  // On the monotonic clock: a step of the wall clock must not move it.
  const auto deadline =
    ceph::mono_clock::now() + (budget - CUOBJ_RETRY_BUDGET);
  uint16_t channel = get_channel_id();
  if (channel == invalid_channel) {
    return -EIO;
  }
  // A write that an earlier plan on this channel left posted when it
  // timed out may still be retrying. One posted behind it waits for it
  // on the DC initiator, and can still be retrying when this plan's
  // budget runs out; this plan would hold the op worker until its
  // deadline meanwhile. Take what completed of those writes, which also
  // frees their buffers for this plan, and deliver this read inline
  // while any is left. The part that a failed submission may have
  // posted does not hold the channel: a submission can fail before it
  // posts anything, on a descriptor that the library rejects, and a
  // plan posted behind a part that is still retrying times out into the
  // case above. Its buffer stays out of the arena meanwhile (settle()).
  reap_abandoned(channel);
  if (!tls_abandoned.empty()) {
    dout(10) << "channel " << channel << " still has writes of "
	     << tls_abandoned.size() << " timed-out plans posted; nothing "
	     << "staged for " << key << dendl;
    m_channel_busy++;
    return -EBUSY;
  }
  BufEntry* buf = acquire_buffer(local_len);
  if (!buf) {
    // the arena is full, or the transfer larger than a buffer can be:
    // the caller moves the data inline
    return -ENOMEM;
  }
  if (!pull) {
    auto it = data->begin();
    it.copy(local_len, static_cast<char*>(buf->ptr));
  }

  m_plans_started++;
  // no write of a timed-out plan is left on the channel; this plan
  // reserves a handle for each of its writes here, write i carrying
  // first + i
  const uint64_t first =
    m_next_handle.fetch_add(items.size(), std::memory_order_relaxed);
  dout(20) << "executing plan " << first << " for " << key << ": "
	   << items.size() << " " << what << "s, " << total
	   << " bytes, channel " << channel << dendl;

  // batched async submission: at most POLL_BATCH outstanding, polled
  // to completion on the same channel
  size_t next = 0;
  size_t outstanding = 0;
  size_t completed = 0;
  // whether each write posted, first to first + next - 1, is still
  // posted: a completion that the library returns twice counts once
  std::vector<bool> posted;
  posted.reserve(items.size());
  ssize_t err = 0;
  // set when a submission failed: it may have posted part of its write,
  // which no completion need report and which can go on retrying for
  // the DC retry budget, so the staging buffer is kept until then
  bool submit_failed = false;
  // the library reset the channel, flushing all that was posted on it
  bool reset = false;
  // a completion counts toward this plan only when it carries the handle
  // of a write that the plan posted and that has not completed yet: not
  // the failed submission's, first + next, nor one whose completion the
  // library returned before, so that outstanding does not wrap, and the
  // plan does not end while a write is still posted
  auto mine = [&](const void* handle) {
    return ceph::osd::oob::still_posted(first, posted, handle_value(handle));
  };
  // count a completion that mine() found this plan waits for
  auto credit_mine = [&](const void* handle) {
    posted[handle_value(handle) - first] = false;
    outstanding--;
    m_writes_inflight--;
    completed++;
  };
  // what a plan that stops waiting keeps
  auto held = [&] {
    return held_t{buf, submit_failed};
  };
  // polled with only a yield in between until then
  auto spin_until = ceph::mono_clock::now() + POLL_SPIN;
  while (completed < items.size()) {
    while (err == 0 && next < items.size() && outstanding < POLL_BATCH) {
      // a write posted past the deadline could retry past the budget
      if (ceph::mono_clock::now() > deadline) {
	dout(5) << "plan for " << key << " reached its deadline with "
		<< items.size() - next << " writes not posted" << dendl;
	err = -ETIMEDOUT;
	break;
      }
      auto& w = items[next];
      // a submission that fails may still have sent part of its write
      if (started) {
	*started = true;
      }
      // handlePutObject reads the client's bytes into the buffer;
      // handleGetObject writes them out
      ssize_t r = pull ?
	m_server->handlePutObject(
	  key, buf->handle, window->addr + w.remote_ofs, w.len, token, channel,
	  buf->offset + w.local_ofs, nullptr,
	  /*async_handle=*/as_handle(first + next)) :
	m_server->handleGetObject(
	  key, buf->handle, window->addr + w.remote_ofs, w.len, token, channel,
	  buf->offset + w.local_ofs, nullptr,
	  /*async_handle=*/as_handle(first + next));
      if (r < 0) {
	derr << "ERROR: async " << (pull ? "handlePutObject" : "handleGetObject")
	     << " submission failed for " << key << ": " << r << dendl;
	err = r;
	submit_failed = true;
	break;
      }
      next++;
      posted.push_back(true);
      outstanding++;
      m_writes_inflight++;
    }
    if (outstanding == 0) {
      break;  // nothing in flight, and an error or the deadline stops posts
    }
    cuObjAsyncEvent_t events[POLL_BATCH];
    for (auto& e : events) {
      e.async_handle = nullptr;
    }
    int n = m_server->poll(events, POLL_BATCH, channel);
    if (n == 0) {
      // nothing completed yet: poll again at once while a completion is
      // likely soon, then without hot-spinning the op worker
      if (ceph::mono_clock::now() < spin_until) {
	std::this_thread::yield();
      } else {
	std::this_thread::sleep_for(POLL_SLEEP);
      }
      if (ceph::mono_clock::now() > deadline) {
	derr << "ERROR: plan " << first << " for " << key << " timed out with "
	     << outstanding << " writes outstanding; keeping the staging "
	     << "buffer until they complete" << dendl;
	note_retry_until(abandoned_until(submit_failed));
	m_writes_inflight -= outstanding;
	m_buffers_leaked++;
	m_plans_failed++;
	tls_abandoned.abandon(first, std::move(posted), held());
	return -ETIMEDOUT;
      }
      continue;
    }
    if (n < 0) {
      // a failed poll returns no completion count: scan for the events
      // that were filled, then fail the plan
      for (const auto& e : events) {
	if (!e.async_handle) {
	  continue;
	}
	if (!mine(e.async_handle)) {
	  credit_abandoned(handle_value(e.async_handle));
	  continue;
	}
	credit_mine(e.async_handle);
      }
      derr << "ERROR: poll failed for " << key << ": " << n << dendl;
      err = err ? err : -EIO;
      if (n != -EIO && outstanding > 0) {
	// only -EIO says that the library reset the QP: the writes still
	// outstanding may go on reading the staging buffer, so keep it
	// until a later read on this thread finds them completed
	derr << "ERROR: plan " << first << " for " << key << " failed with "
	     << outstanding << " writes outstanding; keeping the staging "
	     << "buffer until they complete" << dendl;
	note_retry_until(abandoned_until(submit_failed));
	m_writes_inflight -= outstanding;
	m_buffers_leaked++;
	m_plans_failed++;
	tls_abandoned.abandon(first, std::move(posted), held());
	return err == -EOPNOTSUPP ? -EIO : err;
      }
      // on -EIO the library has reset the QP, flushing the remaining
      // writes: nothing more of them will complete, count them as
      // flushed
      m_writes_inflight -= outstanding;
      completed += outstanding;
      outstanding = 0;
      reset = n == -EIO;
      if (reset) {
	// the reset flushed what earlier failed submissions posted too
	release_delayed(true);
      }
      break;
    }
    // the channel moves: the next completions are likely close behind
    spin_until = ceph::mono_clock::now() + POLL_SPIN;
    for (int i = 0; i < n; i++) {
      if (!events[i].async_handle) {
	continue;
      }
      if (!mine(events[i].async_handle)) {
	// not a write this plan waits for: what a failed submission posted,
	// this plan's or an earlier one's, a write of a timed-out plan,
	// though none of those is left posted when a plan starts, or a
	// completion that the library returned before
	dout(5) << "plan " << first << " for " << key << " took the "
		<< "completion of write "
		<< handle_value(events[i].async_handle)
		<< ", which it does not wait for: wc_status="
		<< events[i].status << dendl;
	credit_abandoned(handle_value(events[i].async_handle));
	continue;
      }
      credit_mine(events[i].async_handle);
      if (events[i].status != 0 /* IBV_WC_SUCCESS */) {
	derr << "ERROR: RDMA " << what << " completion failed for " << key
	     << ": wc_status=" << events[i].status << dendl;
	err = err ? err : -EIO;
      }
    }
    if (outstanding > 0 && ceph::mono_clock::now() > deadline) {
      // wedged transport: we cannot release the staged buffer while
      // writes may still reference it - keep it until a later read on
      // this thread finds them completed. With none outstanding
      // nothing reads it: the loop completes the plan or, posting
      // nothing more, fails it, and gives the buffer back.
      derr << "ERROR: plan " << first << " for " << key << " timed out with "
	   << outstanding << " writes outstanding; keeping the staging "
	   << "buffer until they complete" << dendl;
      note_retry_until(abandoned_until(submit_failed));
      m_writes_inflight -= outstanding;
      m_buffers_leaked++;
      m_plans_failed++;
      tls_abandoned.abandon(first, std::move(posted), held());
      return -ETIMEDOUT;
    }
  }
  if (err == 0 && pull) {
    // every read completed: the buffer holds the payload
    out->clear();
    out->append(static_cast<const char*>(buf->ptr), local_len);
  }
  if (submit_failed && !reset) {
    // what the failed submission posted may still read or write the
    // buffer: it goes back once the DC retry budget has run out on that
    dout(10) << "plan " << first << " for " << key << " keeps its staging "
	     << "buffer until its failed submission can retry no longer"
	     << dendl;
    m_buffers_leaked++;
    settle(held());
  } else {
    release_buffer(buf);
  }
  if (err < 0) {
    m_plans_failed++;
    return err == -EOPNOTSUPP ? -EIO : err;
  }
  m_plans_completed++;
  (pull ? m_bytes_pulled : m_bytes_pushed) += total;
  dout(20) << "plan for " << key << (pull ? " pulled " : " pushed ") << total
	   << " bytes" << dendl;
  return static_cast<ssize_t>(total);
}

void OSDCuObj::dump_stats(ceph::Formatter* f) const
{
  f->dump_bool("available", is_available());
  if (m_arena) {
    m_arena->dump(f);
  }
  f->dump_unsigned("plans_started", m_plans_started.load());
  f->dump_unsigned("plans_completed", m_plans_completed.load());
  f->dump_unsigned("plans_failed", m_plans_failed.load());
  f->dump_unsigned("bytes_pushed", m_bytes_pushed.load());
  f->dump_unsigned("writes_inflight", m_writes_inflight.load());
  f->dump_unsigned("bytes_pulled", m_bytes_pulled.load());
  f->dump_unsigned("buffers_leaked", m_buffers_leaked.load());
  f->dump_unsigned("buffers_reclaimed", m_buffers_reclaimed.load());
  f->dump_unsigned("stale_completions", m_stale_completions.load());
  f->dump_unsigned("channel_busy", m_channel_busy.load());
  f->dump_unsigned("budget_refused", m_budget_refused.load());
  f->dump_unsigned("gather_crc_mismatch", get_gather_crc_mismatch());
}
