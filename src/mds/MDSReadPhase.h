// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "msg/Message.h"

#include "MDSReadPhaseCtx.h"

class MDSRank;
class CInode;
class CDentry;
class CDir;
class Session;
class MClientRequest;

/*
 * Parallel read phase.
 *
 * Every client request is handled under mds_lock, one at a time, so a
 * rank cannot use more than one core however many clients read from it.
 * Many of those requests are plain reads: a GETATTR or LOOKUP of
 * something already in cache whose locks a reader can take without
 * changing their state.  Such a request changes almost nothing (the caps
 * of one client on one inode, a few counters), so many of them can run at
 * the same time, as long as nothing that changes the cache runs with them.
 *
 * The dispatch thread queues such requests instead of handling them, and
 * runs the queued batch as a "read phase" before it handles any other
 * message, or when the messenger has nothing more queued for it.  During
 * the phase it holds mds_lock, so no writer runs, and it splits the batch
 * by target inode over itself and the worker threads; requests on one
 * inode go to one worker, so the per-inode state they change (that
 * client's cap on that inode) is never shared between workers.  What they
 * would change that is shared is recorded in an MDSReadPhaseCtx and
 * applied after the phase.  A request that does not qualify on closer
 * look (lock not readable, inode not auth or frozen, quarantine, ...) is
 * handled the normal way after the phase, in arrival order.
 *
 * Message order is kept: nothing queued after a read is handled before it,
 * except other reads of the same batch.
 */
class MDSReadPhase {
public:
  MDSReadPhase(MDSRank *m, unsigned nthreads);
  ~MDSReadPhase();

  /* mds_lock held.  Queue m if it is a read this phase can serve, and run
   * the batch if it is full or the messenger has nothing queued behind it.
   * Returns false if m must be dispatched the normal way; the caller must
   * call flush() before it does. */
  bool maybe_queue(const cref_t<Message> &m);

  /* mds_lock held.  Run the queued batch, then dispatch the requests that
   * fell back to the normal path. */
  void flush();

  /* mds_lock held, from tick(): run a batch left behind */
  void flush_late() {
    if (!pending.empty()) {
      stats.late++;
      flush();
    }
  }

  bool empty() const { return pending.empty(); }
  void shutdown();
  void log_stats();

  void set_max_batch(unsigned n) { max_batch = n ? n : 1; }
  void set_min_per_worker(unsigned n) { min_per_worker = n ? n : 1; }

  struct Stats {
    std::atomic<uint64_t> phases{0};
    std::atomic<uint64_t> queued{0};
    std::atomic<uint64_t> served{0};
    std::atomic<uint64_t> fallback{0};
    // batches only tick() ran: nothing that came later ended the wait
    std::atomic<uint64_t> late{0};
  } stats;
  uint64_t logged_phases = 0;

private:
  struct Item {
    cref_t<MClientRequest> req;
    Session *session = nullptr;
    CInode *diri = nullptr;   // LOOKUP only
    CDir *dir = nullptr;      // LOOKUP only
    CDentry *dn = nullptr;    // LOOKUP only
    CInode *in = nullptr;     // target
    // LOOKUP: the parent is no request's target in this batch, so its caps
    // can be read to decide on a dentry lease
    bool lease_ok = false;
    bool served = false;
  };

  bool is_candidate(const cref_t<Message> &m) const;
  bool resolve(Item &it);
  bool serve(Item &it, MDSReadPhaseCtx &ctx);
  bool can_read(Item &it);
  void apply(MDSReadPhaseCtx &ctx);
  void run_parallel(const std::function<void(unsigned)> &fn, unsigned nw);
  void worker_entry(unsigned idx);

  MDSRank *mds;
  std::atomic<unsigned> max_batch{64};
  // a batch gets one worker per this many requests, so that a small batch
  // is served inline instead of paying for waking threads
  std::atomic<unsigned> min_per_worker{8};
  std::vector<cref_t<Message>> pending;

  std::vector<MDSReadPhaseCtx> ctxs;   // one per worker, [0] = dispatch thread
  std::mutex snap_lock;                // SnapRealm snap trace caches
  std::mutex lease_seq_lock;           // Session::lease_seq

  // worker pool
  std::vector<std::thread> threads;
  std::mutex pool_lock;
  struct Slot {
    std::condition_variable cv;   // wakes only this worker
    uint64_t gen = 0;
  };
  std::vector<std::unique_ptr<Slot>> slots;   // [0] unused
  std::condition_variable pool_done;
  unsigned running = 0;
  bool stopping = false;
  const std::function<void(unsigned)> *job = nullptr;
};
