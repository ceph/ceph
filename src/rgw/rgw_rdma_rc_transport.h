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
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <infiniband/verbs.h>

#include "include/common_fwd.h"

class optional_yield;

/**
 * Reliable Connection transport for the hipobj-rc-v2 data phase.
 *
 * One Device is opened at startup: the verbs context, a protection
 * domain, and the port topology (GID, LID) that every session shares.
 * Each session owns one Connection: a completion queue and an RC queue
 * pair that PREPARE leaves in INIT and READY pairs against the client's
 * queue pair before the transfer. Each session's buffer is registered
 * for that session alone. A GET's goes on the device's protection
 * domain, so that the same registration also serves a DC target created
 * on it (see rgw_rdma_dc_target.h); a PUT's buffer and queue pair get a
 * protection domain of their own, which no other queue pair is on.
 */
namespace rgw::rdma::rc {

struct DeviceConfig {
  std::string device_name;  ///< verbs device, e.g. mlx5_0; empty = first
  uint8_t port = 1;
  int gid_index = -1;       ///< -1 = pick a RoCEv2 (or the port) GID
  uint64_t client_mtu = 0;  ///< bytes, see path_mtu_of(); 0 = the port's
};

/// the path MTU of at most bytes: the largest of 256, 512, 1024, 2048
/// and 4096 bytes that exceeds neither bytes nor active. 0 bytes takes
/// active; nullopt for 1 to 255 bytes
inline std::optional<ibv_mtu> path_mtu_of(uint64_t bytes, ibv_mtu active)
{
  if (bytes == 0) {
    return active;
  }
  if (bytes < 256) {
    return std::nullopt;
  }
  ibv_mtu m = IBV_MTU_256;
  while (m < IBV_MTU_4096 && (128ull << (m + 1)) <= bytes) {
    m = static_cast<ibv_mtu>(m + 1);
  }
  return std::min(m, active);
}

class Device {
 public:
  Device() = default;
  ~Device();
  Device(const Device&) = delete;
  Device& operator=(const Device&) = delete;

  /// open the device and the protection domain; negative errno on failure
  int open(CephContext* cct, const DeviceConfig& cfg);
  void close();

  bool is_open() const { return ctx != nullptr; }

  ibv_context* context() const { return ctx; }
  ibv_pd* protection_domain() const { return pd; }
  uint8_t port_num() const { return port; }
  int gid_idx() const { return gid_index; }
  const ibv_gid& gid() const { return local_gid; }
  uint16_t lid() const { return local_lid; }
  /// the port's active MTU
  ibv_mtu path_mtu() const { return mtu; }
  /// the path MTU of the queue pairs paired with clients: the port's,
  /// or lower when DeviceConfig::client_mtu sets it
  ibv_mtu client_path_mtu() const { return client_mtu; }
  const std::string& name() const { return dev_name; }
  /// whether a queue pair goes from RESET back to INIT, as a session
  /// pairs it again after a BUSY. irdma (Intel E810) refuses that step,
  /// also for a queue pair that never left INIT
  bool reuses_queue_pairs() const { return qp_reuse; }

  /// a protection domain apart from the device's: memory registered on
  /// it is reachable only through queue pairs created on it. Null, with
  /// errno set, on failure
  ibv_pd* alloc_pd();
  /// 0, or the errno ibv_dealloc_pd() gave
  static int dealloc_pd(ibv_pd* pd);

  /// register [ptr, ptr+len) with access, a mask of IBV_ACCESS_* flags,
  /// on pd, or on the device's protection domain when pd is null. With
  /// an iova, the registration names the memory [iova, iova+len), in
  /// local work requests and remote ones alike, instead of by ptr
  ibv_mr* register_memory(void* ptr, size_t len, int access,
                          ibv_pd* pd = nullptr,
                          std::optional<uint64_t> iova = std::nullopt);
  /// 0, or the errno ibv_dereg_mr() gave: the key then still works
  static int deregister_memory(ibv_mr* mr);

  /// ibv_post_send() that works from any thread. See post_proxy.
  int post_send(ibv_qp* qp, ibv_send_wr* wr, ibv_send_wr** bad);

 private:
  int select_gid(CephContext* cct, int wanted);
  /// take a scratch queue pair to INIT, RESET and INIT again
  bool probe_qp_reuse();
  void start_post_proxy();
  void stop_post_proxy();

  /**
   * Software providers (soft-RoCE, siw) ring the send doorbell with a
   * write() on the device file, and the kernel refuses that write from
   * a thread whose credentials are not the very ones that opened the
   * file. radosgw request threads never qualify: setuid() after the
   * frontends bind (--setuser) gives every thread its own credentials,
   * and so does creating the process keyring after the thread pool is
   * up. Hardware providers ring doorbells through mapped memory and
   * post directly. For software providers every post goes through one
   * thread started by the opener, which shares its credentials.
   */
  struct post_job {
    ibv_qp* qp;
    ibv_send_wr* wr;
    ibv_send_wr** bad;
    int result = 0;
    bool done = false;
  };
  bool post_proxy = false;
  std::thread proxy;
  std::mutex proxy_mtx;
  std::condition_variable proxy_cv;
  std::condition_variable proxy_done;
  std::deque<post_job*> proxy_jobs;
  bool proxy_stop = false;

  ibv_context* ctx = nullptr;
  ibv_pd* pd = nullptr;
  uint8_t port = 1;
  int gid_index = 0;
  ibv_gid local_gid = {};
  uint16_t local_lid = 0;
  ibv_mtu mtu = IBV_MTU_1024;
  ibv_mtu client_mtu = IBV_MTU_1024;
  bool qp_reuse = true;
  std::string dev_name;
};

/// the peer a Connection is paired against
struct PeerEndpoint {
  uint32_t qpn = 0;
  uint32_t psn = 0;       ///< the peer's send PSN (our receive PSN)
  ibv_gid gid = {};
  uint16_t lid = 0;       ///< 0 on RoCE, where the GRH routes alone
};

/// outcome of waiting for one completion
enum class Completion {
  OK,
  TIMEOUT,     ///< nothing completed before the deadline
  BUSY,        ///< the peer was not ready (RNR or retry exhaustion)
  WIRE_ERROR,  ///< any other failed completion or a poll error
};

class Connection {
 public:
  Connection() = default;
  ~Connection();
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  /// create the queue pair on dev and move it to INIT, accepting the
  /// remote operations of access (IBV_ACCESS_REMOTE_* flags). It goes on
  /// pd, or on the device's protection domain when pd is null; dev and
  /// pd must outlive the connection
  int create(CephContext* cct, Device& dev, uint32_t send_depth,
             uint32_t recv_depth, int access, ibv_pd* pd = nullptr);
  void destroy();

  /// INIT -> RTR against the peer -> RTS with our send PSN
  int pair(CephContext* cct, Device& dev, const PeerEndpoint& peer,
           uint32_t local_psn);
  /// back to INIT through RESET so a later pair() can retry
  int rearm(CephContext* cct, Device& dev);
  /// to ERROR: the posted work requests complete flushed, which wakes a
  /// wait for them, and the peer's writes fail
  int abort(CephContext* cct);

  uint32_t qpn() const { return qp ? qp->qp_num : 0; }

  /// RDMA write [ptr, ptr+len) of mr into the peer's (remote_addr,
  /// rkey); with_imm attaches the immediate (network byte order is
  /// applied here) and signals a completion on the peer
  int post_write(ibv_mr* mr, const void* ptr, uint32_t len,
                 uint64_t remote_addr, uint32_t rkey,
                 bool with_imm, uint32_t imm, uint64_t wr_id,
                 bool signaled);

  /// receive buffer for the peer's write-with-immediate: len bytes at
  /// addr, in the address space of mr
  int post_recv(ibv_mr* mr, uint64_t addr, uint32_t len, uint64_t wr_id);

  /// wait for one completion; on OK, wc holds it. With a yield context
  /// the wait suspends the request until the completion channel signals
  /// or the deadline passes, so that the frontend thread serves other
  /// requests meanwhile; without one it polls.
  Completion poll_one(ibv_wc& wc, std::chrono::steady_clock::time_point deadline,
                      optional_yield y);

  /// count of signaled sends not yet reaped by poll_one()
  uint32_t outstanding() const { return inflight; }
  void reaped() { if (inflight) --inflight; }

 private:
  /// wait in y until the channel signals or until passes; negative
  /// errno when reading the channel fails
  int wait_event(optional_yield y,
                 std::chrono::steady_clock::time_point until);

  Device* dev = nullptr;
  ibv_comp_channel* channel = nullptr;
  ibv_cq* cq = nullptr;
  ibv_qp* qp = nullptr;
  int access = 0;
  /// the completion queue was asked for an event on its next completion
  bool armed = false;
  uint32_t inflight = 0;
};

} // namespace rgw::rdma::rc
