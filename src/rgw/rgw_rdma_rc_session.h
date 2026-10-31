// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/spawn.hpp>
#include <boost/asio/thread_pool.hpp>

#include "acconfig.h"
#include "include/common_fwd.h"

#include "common/oob_arena.h"
#include "rgw_rdma_rc_transport.h"
#include "rgw_rdma_rc_wire.h"

class optional_yield;
namespace rgw::rdma::dc { class Target; }
#include "common/ofi_rma_fwd.h"

/**
 * hipobj-rc-v2 session service.
 *
 * A session is one transfer: created by PREPARE, driven by READY,
 * ended by the FINAL reply, CANCEL, or the expiry reaper. Each
 * session owns an RC Connection and one buffer of the arena, registered
 * for that session alone. For a GET the buffer is where the object
 * lands (pushed by the OSDs straight into it when the gateway's DC
 * target or a libfabric window is available, copied in otherwise)
 * before the RC push to the client; for a PUT it is the staging region
 * the client writes into.
 *
 * The REST handlers call in on radosgw request threads; the reaper
 * runs on its own thread, and tears down the queue pair and the buffer
 * of every session that ended, so that the request that ended a
 * session does not wait for that. The service mutex covers the session
 * table and state changes; the data phase runs outside it with an io
 * reference pinning the session.
 */
namespace rgw::rdma::rc {

using clock = std::chrono::steady_clock;

/// a session's buffer: a slice of the arena, which goes back to the
/// arena when the session ends, once nothing can write into it
struct Buffer {
  ceph::oob::Arena::slice_t slice;
  void* ptr = nullptr;
  /// where ptr is in the registration's address space: 0 for a PUT,
  /// whose client must not learn where the arena is in the gateway's
  /// memory, and ptr itself for a GET
  uint64_t base = 0;
  /// the session's size, which every registration covers exactly
  size_t size = 0;
  /// a PUT's own protection domain, which only mr and the session's
  /// queue pair are on; null for a GET, whose mr is on the device's
  ibv_pd* pd = nullptr;
  ibv_mr* mr = nullptr;
  /// delivery descriptor OSDs write this window through: a cuObject DC
  /// descriptor or a libfabric one; empty when OSD-direct is off
  std::string osd_token;
  /// the buffer's libfabric window, when osd_token is a libfabric one
  uint64_t ofi_window = 0;
  bool ofi_registered = false;
  /// a data phase gave osd_token to the OSDs, which may still write with
  /// it: only then does the window need a new key for another READY, or
  /// the key quarantine where closing it does not revoke the key
  bool token_out = false;
  /// an OSD may still push into a window after its relay ended (see
  /// Service::finish()); the buffer stays out of the arena until this
  /// passes
  clock::time_point held_until = clock::time_point::min();
  /// the user the buffer is charged to (see Limits), and how much, until
  /// it is back in the arena
  std::string charged_to;
  uint64_t charged = 0;
};

enum class Op : uint8_t { GET = 0, PUT = 1 };

enum class State : uint8_t {
  PREPARED,      ///< waiting for READY
  TRANSFERRING,  ///< a READY handler owns the data phase
};

/// what a READY data phase ended with
enum class Outcome : uint8_t {
  OK,
  BUSY,         ///< peer not ready; session returns to PREPARED (409)
                ///< unless an OSD write of the data phase may still
                ///< land in its buffer
  TIMEOUT,
  VERIFY_FAIL,  ///< completion did not match the session
  WIRE_FAIL,
  BACKEND_FAIL, ///< the object store side failed
  CANCELLED,    ///< CANCEL stopped the data phase
};

struct Session {
  std::string id;
  std::string principal;
  std::string owner;     ///< whom the limits count it against
  Op op = Op::GET;
  std::string target;    ///< canonical x-amz-rdma-target value
  uint64_t size = 0;
  uint64_t offset = 0;
  uint32_t cookie = 0;
  uint32_t client_psn = 0;
  uint32_t server_psn = 0;

  PeerEndpoint peer;           ///< gid/lid from PREPARE, qpn/psn at READY
  uint64_t client_mr_addr = 0; ///< client window for a GET push
  uint32_t client_mr_rkey = 0;

  Connection conn;
  std::unique_ptr<Buffer> buf;

  State state = State::PREPARED;
  clock::time_point deadline;  ///< PREPARE or exec deadline by state
  int io_refs = 0;             ///< handlers pinning the session
  bool reap_pending = false;
  Outcome last_outcome = Outcome::OK;
  /// how long an OSD write of the data phase may still land in buf, as
  /// finish() was told; the teardown releases the buffer with it
  std::chrono::milliseconds quarantine{};
  /// its place in the order sessions ended in, once it left the table
  uint64_t ended_seq = 0;
  /// CANCEL asked the data phase to stop: it ends at its next step
  std::atomic<bool> cancelled{false};
  /// finish() owns the queue pair, which CANCEL then leaves alone
  bool finishing = false;
  /// a CANCEL waits for the data phase to stop; another one answers at
  /// once
  bool cancel_waiting = false;
  /// wake the CANCEL that waits for the data phase to stop
  std::vector<std::function<void()>> on_stop;
};

/// service-level errors; the REST layer maps them to HTTP statuses
enum class Error {
  OK,
  ARG,          ///< malformed request (400)
  PEER,         ///< the client's token names the gateway's host, and the
                ///< request does not come from it (400)
  STATE,        ///< wrong state for the call; session preserved (409)
  SESSION,      ///< owner or cookie mismatch; session preserved (404)
  STALE,        ///< session gone or already terminal (404)
  CANCELLED,    ///< CANCEL ended the session that READY claimed (409)
  NO_SESSION,   ///< unknown id (404)
  LIMIT,        ///< session or per-user limit reached (503)
  NO_BUFFER,    ///< no room in the arena (503)
  TOO_LARGE,    ///< transfer exceeds the largest buffer (413)
  WIRE,         ///< verbs failure (500)
  UNSUPPORTED,  ///< no RDMA device (501)
  INTERNAL,
};

struct PrepareRequest {
  /// who owns the session: READY and CANCEL must come from it
  std::string principal;
  /// whom the limits count it against: the account, or the user where
  /// there is none, for every subuser and role session of it; the
  /// principal when empty
  std::string owner;
  Op op = Op::GET;
  std::string target;
  uint64_t size = 0;
  uint64_t offset = 0;
  uint32_t cookie = 0;
  uint32_t client_psn = 0;
  std::string client_token;  ///< 88-hex, or all zeroes for loopback
  /// the request came from a client on this host (its connection from a
  /// loopback address): only such a client may send the loopback marker,
  /// or a token that names a GID of this host
  bool local_peer = false;
};

struct PrepareReply {
  std::string session_id;
  std::string server_token;  ///< 88-hex
  uint32_t server_psn = 0;
  uint32_t server_qpn = 0;
  /// PUT: where the client writes, in the staging buffer's own
  /// registration, which starts at 0
  uint64_t staging_addr = 0;
  uint32_t staging_rkey = 0;
};

struct ReadyRequest {
  std::string principal;
  std::string session_id;
  uint32_t cookie = 0;
  uint32_t client_qpn = 0;
  uint64_t client_mr_addr = 0;
  uint32_t client_mr_rkey = 0;
};

struct SessionInfo {
  Op op = Op::GET;
  std::string target;
  uint64_t size = 0;
  uint64_t offset = 0;
};

/// the session limits. A session counts from the PREPARE that takes it
/// until its reap, also while prepare() builds it outside the service
/// lock, so that concurrent PREPAREs cannot all pass the limit. Its
/// buffer is charged to its user from that PREPARE until the buffer is
/// back in the arena, which can be long after the reap: the teardown
/// and a quarantine come first. Takes no lock; the service calls in
/// under its own
class Limits {
 public:
  /// how long a user that found no room in the arena counts for the
  /// fair share (see share()) without holding a buffer
  static constexpr std::chrono::seconds WANTS_FOR{10};

  Limits() = default;
  /// as the options give them, 64 bits wide. A max_bytes_per_user of 0
  /// limits each user to a fair share of pool bytes instead
  Limits(uint64_t max, uint64_t max_per_user,
         uint64_t max_bytes_per_user = UINT64_MAX, uint64_t pool = 0)
    : max_sessions(max), max_user_sessions(max_per_user),
      max_user_bytes(max_bytes_per_user), pool_bytes(pool) {}

  /// count a session of user, and charge it the bytes of its buffer;
  /// false when the gateway or the user is at a limit. A user that is
  /// charged nothing may take one buffer of any size
  bool take(const std::string& user, uint64_t bytes,
            clock::time_point now = clock::now()) {
    if (total >= max_sessions) {
      return false;
    }
    forget_wants(now);
    const uint64_t cap = share();
    auto [it, added] = per_user.try_emplace(user);
    auto& u = it->second;
    if (u.sessions >= max_user_sessions ||
        (u.bytes > 0 && (bytes > cap || u.bytes > cap - bytes))) {
      if (added) per_user.erase(it);
      return false;
    }
    if (u.bytes == 0 && bytes > 0) {
      ++holders;
      wants.erase(user);
    }
    ++u.sessions;
    u.bytes += bytes;
    ++total;
    return true;
  }
  /// user found no room in the arena for a buffer: with the fair share,
  /// it counts as a user of the arena for WANTS_FOR, so that those who
  /// hold buffers already cannot take every one that comes back
  void wanted(const std::string& user, clock::time_point now = clock::now()) {
    if (max_user_bytes == 0 && bytes(user) == 0) {
      wants[user] = now;
    }
  }
  /// a session that take() counted left the table; its buffer stays
  /// charged until uncharge()
  void end(const std::string& user) {
    auto it = per_user.find(user);
    if (it == per_user.end() || it->second.sessions == 0) {
      return;
    }
    --total;
    --it->second.sessions;
    erase_unused(it);
  }
  /// the buffer that take() charged is back in the arena, or never came
  /// out of it
  void uncharge(const std::string& user, uint64_t bytes) {
    auto it = per_user.find(user);
    if (it == per_user.end()) {
      return;
    }
    auto& u = it->second;
    const bool held = u.bytes > 0;
    u.bytes -= std::min(bytes, u.bytes);
    if (held && u.bytes == 0) {
      --holders;
    }
    erase_unused(it);
  }
  uint64_t sessions() const { return total; }
  /// the bytes charged to user
  uint64_t bytes(const std::string& user) const {
    auto it = per_user.find(user);
    return it == per_user.end() ? 0 : it->second.bytes;
  }
  /// the most bytes a user that holds some may hold: max_bytes_per_user,
  /// or the fair share of the pool. That is all of it while one user
  /// holds buffers and no other wants one, and an equal part for each
  /// user that holds or wants some otherwise
  uint64_t share() const {
    if (max_user_bytes != 0) {
      return max_user_bytes;
    }
    uint64_t users = holders;
    for (const auto& [user, since] : wants) {
      if (bytes(user) == 0) {
        ++users;
      }
    }
    return pool_bytes / std::max<uint64_t>(users, 1);
  }

 private:
  struct use_t {
    uint64_t sessions = 0;
    uint64_t bytes = 0;
  };
  using users_t = std::map<std::string, use_t>;
  void erase_unused(users_t::iterator it) {
    if (it->second.sessions == 0 && it->second.bytes == 0) {
      per_user.erase(it);
    }
  }
  void forget_wants(clock::time_point now) {
    std::erase_if(wants, [now](const auto& w) {
      return now - w.second >= WANTS_FOR;
    });
  }

  uint64_t max_sessions = 0;
  uint64_t max_user_sessions = 0;
  uint64_t max_user_bytes = UINT64_MAX;
  uint64_t pool_bytes = 0;
  uint64_t total = 0;
  users_t per_user;
  /// the users that are charged bytes
  uint64_t holders = 0;
  /// users that found no room in the arena, and when they last did
  std::map<std::string, clock::time_point> wants;
};

/// the order sessions end in, and which of their teardowns run outside
/// the service lock, by the reaper or by a PREPARE short of a buffer. A
/// PREPARE waits only for the sessions that ended before it found the
/// arena full, not for those that end meanwhile: under a steady stream
/// of transfers, those never run out. Takes no lock; the service calls
/// in under its own
class Teardowns {
 public:
  /// a session ended; its place in the order
  uint64_t ended() { return ++last_ended; }
  /// the place of the last session that ended
  uint64_t last() const { return last_ended; }
  /// the teardown of the session that ended at seq begins, or is done
  void begin(uint64_t seq) { running.insert(seq); }
  void end(uint64_t seq) { running.erase(seq); }
  /// whether the teardown of a session that ended by seq still runs
  bool running_upto(uint64_t seq) const {
    return !running.empty() && *running.begin() <= seq;
  }

 private:
  uint64_t last_ended = 0;
  std::set<uint64_t> running;
};

class Service {
 public:
  /// the longest a CANCEL waits for the data phase to stop
  static constexpr std::chrono::seconds CANCEL_WAIT{5};

  /// arena: the gateway's out-of-band memory, which the session buffers
  /// come from; it must outlive the service
  static int init(CephContext* cct, ceph::oob::Arena* arena);
  static void shutdown();
  /// null until init() succeeded
  static Service* get();

  ~Service();

  /// true once the device is open
  bool available() const { return dev.is_open(); }
  /// true when a session goes back for another READY after a BUSY: the
  /// device takes its queue pair from RESET back to INIT
  bool rearms_queue_pairs() const { return dev.reuses_queue_pairs(); }
  /// true when the gateway exposes its windows to the OSDs, over a DC
  /// target or libfabric
  bool osd_direct() const;
  /// true when the OSDs' writes into those windows come back saying that
  /// they landed: a libfabric provider with delivery-complete writes, or
  /// a DC target, which the OSDs' cuObject writes reach only once the
  /// target acknowledged them. A GET relays through the OSDs only then
  /// (RGWGetObj::relay_lands)
  bool relay_lands() const;
  uint32_t queue_depth() const { return send_depth; }

  /// routing lookup before authentication: the target and op of a
  /// session, without the owner check (READY re-checks it)
  Error peek(const std::string& id, SessionInfo& out);

  /// with a yield context, a PREPARE that must wait for the buffers of
  /// sessions that ended suspends the request while it waits
  Error prepare(const PrepareRequest& req, PrepareReply& reply,
                optional_yield y);

  /// claim the transfer: PREPARED -> TRANSFERRING, pair the queue pair
  /// against the client, and (PUT) arm the receive. On success *out is
  /// pinned until finish() and may be used from the calling thread.
  Error claim(const ReadyRequest& req, Session** out);

  /// end the data phase. BUSY re-arms the queue pair and returns to
  /// PREPARED for another READY; OK and anything else end the session,
  /// and s must not be used after. A session that ends with writes still
  /// in its send queue loses its queue pair before this returns.
  /// quarantine keeps the buffer out of the arena for that long after
  /// teardown (a relay that may still have OSD writes in flight), and
  /// the libfabric key quarantine at least when it had a libfabric
  /// window. relayed: the data phase gave the buffer's relay window to
  /// the OSDs. A BUSY with a quarantine reaps too: another READY would
  /// relay through the same buffer while those writes may still land in
  /// it. A BUSY without one, after a relay, gives the buffer's relay
  /// window a new key for the next READY, and reaps when it cannot.
  /// Returns what the session ended with: BUSY when it went back to
  /// PREPARED, WIRE_FAIL for a BUSY that could not, else outcome
  Outcome finish(Session* s, Outcome outcome, uint64_t bytes,
                 std::chrono::milliseconds quarantine = {},
                 bool relayed = false);

  /// end the session. One that a READY transfers stops at its next
  /// step, its queue pair in ERROR so that what the data phase waits for
  /// completes flushed; then, or after CANCEL_WAIT or by the session's
  /// deadline, whichever comes first, this answers: STATE when the data
  /// phase still ran by then. Only one CANCEL of a session waits: one
  /// that comes while another waits answers STATE at once. With a yield
  /// context, the request yields while it waits
  Error cancel(const std::string& id, const std::string& principal,
               optional_yield y);

 private:
  Service() = default;
  int do_init(CephContext* cct);
  void do_shutdown();

  /// a buffer of the arena for one session of op and size bytes,
  /// registered for that session alone: writable from the network only
  /// by whoever writes into it (the client of a PUT, through the queue
  /// pair that shares the buffer's own protection domain; the OSDs of a
  /// relay through the DC target), and, for a GET with OSD-direct relay,
  /// exposed to the OSDs. Null when the arena has no room. Takes no lock
  /// of the service.
  std::unique_ptr<Buffer> make_buffer(Op op, size_t size);
  /// make_buffer() again for a PREPARE that found the arena full, as the
  /// sessions that ended until then give their buffers back: those still
  /// queued are torn down here, one at a time until the buffer fits, and
  /// those torn down elsewhere waited for. Null when it still does not
  /// fit. Takes the lock
  std::unique_ptr<Buffer> make_buffer_from_ended(Op op, size_t size);
  /// a session buffer: make_buffer(), or make_buffer_from_ended() when
  /// the arena is full. With a yield context, on a worker while the
  /// request yields, so that neither the registration nor the wait for
  /// other sessions' teardowns runs on the frontend thread
  std::unique_ptr<Buffer> make_session_buffer(Op op, size_t size,
                                              optional_yield y);
  /// run work on a worker and return its result, while the request
  /// yields on y's executor
  std::unique_ptr<Buffer> on_worker(
    std::function<std::unique_ptr<Buffer>()> work,
    boost::asio::yield_context yield);
  /// give a GET buffer's relay window a new key for another READY, so
  /// that no OSD write of the data phase before lands in it; false when
  /// the old key may still work. Takes no lock of the service.
  bool rekey_relay(Buffer& b);
  /// close what exposes the buffer of a session whose queue pair is gone,
  /// and give it back to the arena; or, when an OSD write may still land
  /// in it, return it to be held until its held_until. Takes no lock of
  /// the service.
  std::unique_ptr<Buffer> release_buffer(std::unique_ptr<Buffer> b,
                                         std::chrono::milliseconds quarantine);
  /// deregister the buffer and give its slice back to the arena; a slice
  /// whose registration does not go away never goes back. Its user is
  /// charged no more for it either way. Takes the lock
  void free_buffer(Buffer& b);
#ifdef WITH_OOB_OFI
  /// whether closing a libfabric window revokes its key
  bool ofi_close_revokes_key();
#endif
  std::string new_session_id();
  /// read the GIDs that name this host again: those of every port of its
  /// verbs devices, and those of the addresses of its interfaces. Takes
  /// the lock to store them
  void refresh_host_gids();

  /// erase and hand to the reaper; caller holds the lock
  void reap_locked(const std::string& id);
  /// tear down the sessions that ended until now, one at a time outside
  /// the lock; caller holds it
  void tear_down_ended(std::unique_lock<std::mutex>& lock);
  /// tear down the session that ended first, outside the lock; caller
  /// holds it, and ended is not empty
  void tear_down_one(std::unique_lock<std::mutex>& lock);
  /// destroy the queue pair of a session that ended, then release its
  /// buffer; a buffer to hold, as release_buffer() gives. Takes no lock
  /// of the service.
  std::unique_ptr<Buffer> tear_down(Session& s);
  void reaper_loop();

  CephContext* cct = nullptr;
  Device dev;
#ifdef HAVE_MLX5DV
  std::unique_ptr<dc::Target> dct;
#endif
#ifdef WITH_OOB_OFI
  std::unique_ptr<ceph::ofi::Endpoint> ofi_ep;
  /// false when a closed window's key keeps working, as with a
  /// provider's registration cache; every relay buffer is then held for
  /// the key quarantine
  bool ofi_revokes_keys = false;
#endif
  ceph::oob::Arena* arena = nullptr;
  size_t buf_size = 0;

  uint32_t send_depth = 0;
  std::chrono::milliseconds t_prep{};
  std::chrono::milliseconds t_exec{};

  mutable std::mutex mtx;
  std::map<std::string, std::unique_ptr<Session>> sessions;
  /// sessions out of the table whose queue pair and buffer are still to
  /// be torn down: destroying a queue pair takes the device a while, so
  /// the reaper does it rather than the request that ended the session.
  /// They no longer count as sessions, but their buffers stay charged to
  /// their users until they are back in the arena
  std::deque<std::unique_ptr<Session>> ended;
  Teardowns teardowns;
  /// a teardown is done
  std::condition_variable teardown_cv;
  /// a session left the table, for cancel() without a yield context
  std::condition_variable stopped_cv;
  Limits limits;
  /// what refresh_host_gids() read last: a token from a client on another
  /// host must not name one, or the session's writes would go to a queue
  /// pair on this host
  gid_set host_gids;
  /// buffers of ended sessions that an OSD write may still land in; the
  /// reaper gives each back to the arena once its held_until passes, and
  /// they stay charged to their users until then
  std::vector<std::unique_ptr<Buffer>> held;
  std::atomic<bool> closing{false};

  std::thread reaper;
  std::condition_variable reaper_cv;
  bool reaper_stop = false;
  /// where make_session_buffer() runs its work
  std::unique_ptr<boost::asio::thread_pool> workers;

  /// set once do_init() succeeded: the frontends take requests already
  static std::atomic<Service*> instance;
};

const char* to_string(Error e);
const char* to_string(Outcome o);

} // namespace rgw::rdma::rc
