// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "include/buffer.h"
#include "include/common_fwd.h"
#include "osd/oob_abandoned.h"
#include "osd/oob_executor.h"
#include "osd/oob_placement.h"

namespace ceph { class Formatter; }

struct rdma_buffer;
class cuObjServer;
class OSDDcTarget;
namespace ceph::oob { class Arena; }

/**
 * Per-OSD cuObject RDMA endpoint for out-of-band delivery.
 *
 * Owns one cuObjServer (a DC initiator bound to the OSD's RDMA NIC),
 * with which it registers the OSD's out-of-band memory (the arena)
 * once. Each transfer stages its data in a buffer of the arena, taken
 * at the transfer's size, on its way into or out of client memory.
 * Instantiated by OSD::init() when an out-of-band transport option
 * names "cuobj"; PrimaryLogPG reaches it through
 * OSDService::oob_executor_for() for cuObject tokens.
 *
 * Thread safety: execute_plan() may be called
 * concurrently from any number of op worker threads, and of transfer
 * threads when cuObject runs on them (osd_oob_cuobj_transfer_threads).
 * Each thread lazily allocates its own cuObject channel (DCI), and only
 * that thread posts to it and polls it; the arena hands out staging
 * buffers under its own lock.
 */
class OSDCuObj : public OSDOobExecutor {
public:
  /// rdma_ip: the OSD's out-of-band address. lend_windows: cuobj is the
  /// cluster transport, so this executor lends the gather windows, and
  /// opens the DC target behind them. num_dcis: one DC initiator for
  /// each op worker thread, and for each transfer thread when cuObject
  /// runs on them. arena: the OSD's out-of-band memory, which
  /// the executor registers once and stages each transfer in, and in
  /// which it reserves the gather windows; it must outlive the executor
  OSDCuObj(CephContext *cct, const std::string& rdma_ip, bool lend_windows,
	   int num_dcis, ceph::oob::Arena* arena);
  ~OSDCuObj() override;

  OSDCuObj(const OSDCuObj&) = delete;
  OSDCuObj& operator=(const OSDCuObj&) = delete;

  /// true once the local RDMA session started successfully
  bool is_available() const override;

  /// cuObject DC descriptors: every token another executor does not
  /// claim (the descriptor is opaque past its addr:size prefix)
  bool handles(const std::string& token) const override;

  /**
   * Execute a placement plan: stage data once into a registered
   * buffer, then RDMA-write each triple's byte range to
   * token_base + triple.client_ofs, batching asynchronous submissions
   * on this thread's channel and polling them to completion. All or
   * nothing: returns total bytes pushed only if every triple
   * completed, else a negative errno (and the caller must deliver
   * inline instead). Blocks until the batch drains.
   */
  ssize_t execute_plan(const std::string& key,
		       const std::string& token,
		       const ceph::buffer::list& data,
		       const ceph::osd::oob::placement_plan& plan,
		       std::chrono::milliseconds budget,
		       bool* started) override;

  /// true: a plan succeeds only once every one of its writes completed
  /// without error, and cuObject's DC transport is reliable, so a write
  /// completes only after the client's NIC acknowledged all of it. That
  /// is the completion of an RC write, which libfabric's rxm over verbs
  /// reports as delivery-complete, and which the libfabric executor
  /// marks landed too
  bool delivery_complete() const override { return true; }

  /// take what completed of the writes that timed-out plans left posted
  /// on this thread's channel, and give back their staging buffers and
  /// those of failed submissions whose time came; a thread without a
  /// channel has nothing to reap
  void idle() override;

  /// refuse later plans and windows, and wait until no write that a
  /// timed-out plan or a failed submission left posted can still retry,
  /// or until passed; no thread runs a plan or takes a window once it
  /// is called
  void quiesce(ceph::mono_clock::time_point until) override;

  /// asok/debug counters
  void dump_stats(ceph::Formatter* f) const override;
  std::string_view name() const override { return "cuobj"; }
  std::string_view status_help() const override {
    return "cuObject out-of-band RDMA delivery statistics";
  }

  /// gather windows: slots of a pool behind a DC target on this OSD, so
  /// peers' cuObject servers can push shard reads here, when cuobj is the
  /// cluster transport;
  /// none unless the executor lends them. A slot's token is the same on
  /// every lend: nothing re-keys it, and only the quarantine and the
  /// gather's crc32c keep a late write out
  /// (@see OSDOobExecutor::release_window())
  std::optional<window_t> acquire_window(size_t size) override;
  void release_window(uint64_t id, uint64_t quarantine_ms) override;

private:
  /// a plan's transfer through local_len bytes of staging: from data
  /// when pushing, into *out when pulling
  ssize_t run_plan(bool pull, const std::string& key,
		   const std::string& token,
		   const ceph::buffer::list* data, uint64_t local_len,
		   const ceph::osd::oob::placement_plan& plan,
		   ceph::buffer::list* out,
		   std::chrono::milliseconds budget, bool* started);

  /// a staging buffer: a buffer of the arena, which the library knows
  /// as offset in the arena's registration, handle
  struct BufEntry {
    void* ptr = nullptr;
    size_t size = 0;
    uint64_t offset = 0;
    struct rdma_buffer* handle = nullptr;
  };

  int do_init(const std::string& rdma_ip, bool lend_windows, int num_dcis);
  void do_shutdown();

  /// a buffer of the arena of at least needed bytes, or null when the
  /// arena has none free, or none that large
  BufEntry* acquire_buffer(size_t needed);
  void release_buffer(BufEntry* buf);

  /// lazily allocated per-thread channel (DCI); returns
  /// invalid_channel on allocation failure
  uint16_t get_channel_id();
  static constexpr uint16_t invalid_channel = UINT16_MAX;

  /// the staging buffer of a plan that ended with writes still posted
  /// on this thread's channel, when it timed out or a poll failed
  /// without a reset: the thread posts no other plan until they
  /// complete, and the buffer goes back with the last of them. A
  /// submission that failed may have posted part of its write, which no
  /// completion need report: the buffer of its plan then goes back once
  /// the DC retry budget has run out on that part (settle())
  struct held_t {
    BufEntry* buf = nullptr;
    /// a submission of the plan failed
    bool partial = false;
  };
  /// a buffer that only the part of a failed submission may still read
  /// or write, kept until that part can retry no longer
  struct delayed_t {
    BufEntry* buf = nullptr;
    std::chrono::steady_clock::time_point until{};
  };
  /// a completion on this thread's channel that no plan in progress
  /// posted: credit it to the timed-out plan that did
  void credit_abandoned(uint64_t handle);
  /// the library reset this thread's channel, flushing what the
  /// timed-out plans and failed submissions left posted: give their
  /// buffers back
  void drop_abandoned();
  /// take, without waiting, the completions that timed-out plans left
  /// on the channel, and give back the delayed buffers whose time came,
  /// before posting on it again
  void reap_abandoned(uint16_t channel);
  /// a plan's buffer that no posted write of the plan reads any more:
  /// give it back, or keep it in tls_delayed while the part of its
  /// failed submission can still retry
  void settle(const held_t& held);
  /// give back the buffers in tls_delayed whose time came, or all of
  /// them when the channel was reset
  void release_delayed(bool all);
  /// writes of an earlier plan, or the part of a failed submission, may
  /// still be posted on this thread's channel, and hold their staging
  /// buffers
  static bool channel_busy();
  /// a write that a plan left posted may retry until t: quiesce() waits
  /// for the latest such time of any thread
  void note_retry_until(std::chrono::steady_clock::time_point t);
  /// until when the writes of a plan that stops waiting for them now
  /// may retry, by the rule of settle(): they go out behind the parts of
  /// earlier failed submissions, and the part of the plan's own failed
  /// submission (partial) goes out behind them
  static std::chrono::steady_clock::time_point abandoned_until(bool partial);

  CephContext* m_cct;
  std::unique_ptr<cuObjServer> m_server;

  struct WindowSlot {
    bool in_use = false;
    std::chrono::steady_clock::time_point quarantined_until{};
  };
  std::unique_ptr<OSDDcTarget> m_dct;
  std::mutex m_win_mtx;
  std::vector<WindowSlot> m_win_slots;
  size_t m_win_size = 0;
  ceph::oob::Arena* m_arena = nullptr;
  /// the arena, registered with the library once
  struct rdma_buffer* m_arena_handle = nullptr;

  std::atomic<uint64_t> m_plans_started{0};
  std::atomic<uint64_t> m_plans_completed{0};
  std::atomic<uint64_t> m_plans_failed{0};
  std::atomic<uint64_t> m_bytes_pushed{0};
  std::atomic<uint64_t> m_bytes_pulled{0};
  std::atomic<uint32_t> m_writes_inflight{0};
  std::atomic<uint64_t> m_buffers_leaked{0};
  std::atomic<uint64_t> m_buffers_reclaimed{0};
  std::atomic<uint64_t> m_stale_completions{0};
  std::atomic<uint64_t> m_channel_busy{0};
  std::atomic<uint64_t> m_budget_refused{0};
  /// the async handle of the next write a plan posts: unique in the
  /// process, and never 0, which reads as no event
  std::atomic<uint64_t> m_next_handle{1};
  /// quiesce() ran: no plan starts, and no window is lent
  std::atomic<bool> m_quiesced{false};
  /// the latest time, on the steady clock, until which a write that a
  /// timed-out plan or a failed submission left posted on any thread's
  /// channel may still retry
  std::atomic<std::chrono::steady_clock::rep> m_retry_until{0};

  static thread_local uint16_t tls_channel_id;
  static thread_local bool tls_channel_valid;
  static thread_local ceph::osd::oob::abandoned_plans<held_t> tls_abandoned;
  /// buffers of plans whose writes all completed, kept until a failed
  /// submission's part can no longer be posted
  static thread_local std::vector<delayed_t> tls_delayed;
};
