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

// The hipobj-rc-v2 session service on the host's first verbs device,
// soft-RoCE (rxe) included. Each test skips on a host without one, or
// when the service cannot start on it.

#include "rgw/rgw_rdma_rc_session.h"

#include <chrono>
#include <filesystem>
#include <atomic>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <cstdlib>
#include <cstring>

#include <infiniband/verbs.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include "common/async/yield_context.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/oob_arena.h"
#include "rgw/rgw_rdma_rc_wire.h"

using namespace rgw::rdma::rc;
using namespace std::chrono_literals;

namespace {

constexpr uint64_t MiB = 1ull << 20;

/// the open completion channels of the verbs devices
int completion_channels()
{
  int n = 0;
  for (const auto& e : std::filesystem::directory_iterator("/proc/self/fd")) {
    std::error_code ec;
    if (std::filesystem::read_symlink(e.path(), ec).string() ==
        "anon_inode:[infinibandevent]") {
      n++;
    }
  }
  return n;
}

/// the loopback marker: the gateway pairs with its own GID
const std::string LOOPBACK(TOKEN_HEX_LEN, '0');

class RcSession : public ::testing::Test {
 protected:
  std::unique_ptr<CephContext> cct;
  std::shared_ptr<ceph::oob::Arena> arena;
  Service* svc = nullptr;

  /// start the service with an arena of blocks of 64 MiB, and the
  /// options in conf; why it cannot start here, or empty
  using conf_t = std::initializer_list<std::pair<const char*, const char*>>;
  std::string start(uint64_t blocks, conf_t conf = {}) {
    cct = std::make_unique<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
    // no relay windows: OSD-direct needs a cluster
    cct->_conf.set_val_or_die("rgw_rdma_osd_direct", "false");
    for (const auto& [k, v] : conf) {
      cct->_conf.set_val_or_die(k, v);
    }
    cct->_conf.apply_changes(nullptr);
    std::string err;
    arena = ceph::oob::Arena::create(blocks * ceph::oob::Arena::MAX, &err);
    if (!arena) {
      return "no arena: " + err;
    }
    if (int r = Service::init(cct.get(), arena.get()); r < 0) {
      return "no RC service on this host: " + cpp_strerror(r);
    }
    svc = Service::get();
    return {};
  }

  void TearDown() override {
    Service::shutdown();
    arena.reset();
  }

  static PrepareRequest request(Op op, const std::string& principal,
                                uint64_t size) {
    PrepareRequest req;
    req.principal = principal;
    req.op = op;
    req.target = "/bucket/key";
    req.size = size;
    req.cookie = 0x1a2b3c4d;
    req.client_psn = 1;
    req.client_token = LOOPBACK;
    req.local_peer = true;
    return req;
  }
  Error prepare(Op op, const std::string& principal, uint64_t size,
                PrepareReply* reply = nullptr) {
    PrepareReply r;
    return svc->prepare(request(op, principal, size), reply ? *reply : r,
                        null_yield);
  }
  static ReadyRequest ready(const std::string& principal,
                            const PrepareReply& r) {
    ReadyRequest req;
    req.principal = principal;
    req.session_id = r.session_id;
    req.cookie = 0x1a2b3c4d;
    // RTR does not look for the peer: any queue pair number pairs
    req.client_qpn = 0x1234;
    req.client_mr_addr = 0x1000;
    req.client_mr_rkey = 0x42;
    return req;
  }
  std::string start_relay(PrepareReply& r, Session*& s);
  /// true once pred holds, or false after for
  static bool eventually(const std::function<bool()>& pred,
                         std::chrono::milliseconds for_) {
    const auto until = clock::now() + for_;
    while (!pred()) {
      if (clock::now() > until) {
        return false;
      }
      std::this_thread::sleep_for(50ms);
    }
    return true;
  }
};

/// a client's queue pair, on the device and port that carry the
/// gateway's GID, brought up as the hipObject client does
struct Client {
  static constexpr size_t SIZE = 64 << 10;
  ibv_context* ctx = nullptr;
  ibv_pd* pd = nullptr;
  ibv_cq* cq = nullptr;
  ibv_qp* qp = nullptr;
  ibv_mr* mr = nullptr;
  char* buf = nullptr;
  int gid_index = -1;
  ibv_gid gid{};
  ibv_mtu mtu = IBV_MTU_1024;

  Client() = default;
  Client(const Client&) = delete;
  ~Client() {
    if (qp) ibv_destroy_qp(qp);
    if (mr) ibv_dereg_mr(mr);
    if (cq) ibv_destroy_cq(cq);
    if (pd) ibv_dealloc_pd(pd);
    if (ctx) ibv_close_device(ctx);
    std::free(buf);
  }

  /// false when no device carries the GID
  bool open(const uint8_t* server_gid) {
    int n = 0;
    ibv_device** list = ibv_get_device_list(&n);
    for (int i = 0; list && i < n && gid_index < 0; i++) {
      ibv_context* c = ibv_open_device(list[i]);
      ibv_port_attr pa{};
      if (!c || ibv_query_port(c, 1, &pa) != 0) {
        if (c) ibv_close_device(c);
        continue;
      }
      for (int g = 0; g < pa.gid_tbl_len; g++) {
        ibv_gid got{};
        if (ibv_query_gid(c, 1, g, &got) == 0 &&
            std::memcmp(got.raw, server_gid, 16) == 0) {
          gid_index = g;
          gid = got;
          mtu = pa.active_mtu;
          break;
        }
      }
      if (gid_index < 0) {
        ibv_close_device(c);
      } else {
        ctx = c;
      }
    }
    if (list) ibv_free_device_list(list);
    if (!ctx) {
      return false;
    }
    pd = ibv_alloc_pd(ctx);
    cq = ibv_create_cq(ctx, 16, nullptr, nullptr, 0);
    buf = static_cast<char*>(std::aligned_alloc(4096, SIZE));
    if (!pd || !cq || !buf) {
      return false;
    }
    mr = ibv_reg_mr(pd, buf, SIZE,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    ibv_qp_init_attr init{};
    init.qp_type = IBV_QPT_RC;
    init.send_cq = cq;
    init.recv_cq = cq;
    init.cap.max_send_wr = 8;
    init.cap.max_recv_wr = 8;
    init.cap.max_send_sge = 1;
    init.cap.max_recv_sge = 1;
    qp = mr ? ibv_create_qp(pd, &init) : nullptr;
    if (!qp) {
      return false;
    }
    ibv_qp_attr a{};
    a.qp_state = IBV_QPS_INIT;
    a.port_num = 1;
    a.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
    return ibv_modify_qp(qp, &a, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                         IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) == 0;
  }

  /// RTR against the gateway's queue pair, then RTS with our PSN
  bool pair(uint32_t server_qpn, uint32_t server_psn, uint32_t psn) {
    ibv_qp_attr a{};
    a.qp_state = IBV_QPS_RTR;
    a.path_mtu = mtu;
    a.dest_qp_num = server_qpn;
    a.rq_psn = server_psn;
    a.max_dest_rd_atomic = 1;
    a.min_rnr_timer = 12;
    a.ah_attr.is_global = 1;
    a.ah_attr.grh.dgid = gid;
    a.ah_attr.grh.sgid_index = static_cast<uint8_t>(gid_index);
    a.ah_attr.grh.hop_limit = 64;
    a.ah_attr.port_num = 1;
    if (ibv_modify_qp(qp, &a, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                      IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                      IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER)) {
      return false;
    }
    a = {};
    a.qp_state = IBV_QPS_RTS;
    a.timeout = 14;
    a.retry_cnt = 7;
    a.rnr_retry = 7;
    a.sq_psn = psn;
    a.max_rd_atomic = 1;
    return ibv_modify_qp(qp, &a, IBV_QP_STATE | IBV_QP_TIMEOUT |
                         IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                         IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC) == 0;
  }

  /// write len bytes of buf to (remote, rkey) with an immediate, and
  /// wait for its completion
  ibv_wc_status write_imm(uint64_t remote, uint32_t rkey, uint32_t len,
                          uint32_t imm) {
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(buf);
    sge.length = len;
    sge.lkey = mr->lkey;
    ibv_send_wr wr{};
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = htonl(imm);
    wr.wr.rdma.remote_addr = remote;
    wr.wr.rdma.rkey = rkey;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    ibv_send_wr* bad = nullptr;
    if (ibv_post_send(qp, &wr, &bad) != 0) {
      return IBV_WC_GENERAL_ERR;
    }
    const auto until = clock::now() + 10s;
    ibv_wc wc{};
    while (ibv_poll_cq(cq, 1, &wc) == 0) {
      if (clock::now() > until) {
        return IBV_WC_RESP_TIMEOUT_ERR;
      }
      std::this_thread::sleep_for(1ms);
    }
    return wc.status;
  }
};

} // anonymous namespace

/// skips the test where the service cannot start
#define START(...)                                          \
  do {                                                      \
    if (auto why = start(__VA_ARGS__); !why.empty()) {      \
      GTEST_SKIP() << why;                                  \
    }                                                       \
  } while (0)

TEST_F(RcSession, BytesPerUser)
{
  // by default, a user alone may fill the arena
  START(4);
  PrepareReply r[4];
  for (int i = 0; i < 4; i++) {
    EXPECT_EQ(Error::OK, prepare(Op::GET, "u1", 64 * MiB, &r[i])) << i;
  }
  // another user finds no room, and then counts for the fair share
  EXPECT_EQ(Error::NO_BUFFER, prepare(Op::GET, "u2", 64 * MiB));
  // u1 gives two buffers back, but may take no more than half of the
  // arena while u2 waits: those go to u2
  for (int i = 0; i < 2; i++) {
    ASSERT_EQ(Error::OK, svc->cancel(r[i].session_id, "u1", null_yield));
  }
  EXPECT_TRUE(eventually([&] {
    return prepare(Op::GET, "u2", 64 * MiB) == Error::OK;
  }, 5s));
  EXPECT_EQ(Error::LIMIT, prepare(Op::GET, "u1", 1));
  EXPECT_EQ(Error::OK, prepare(Op::GET, "u2", 64 * MiB));
}

TEST_F(RcSession, BytesPerUserOption)
{
  START(4, {{"rgw_rdma_rc_max_bytes_per_user", "8388608"}});
  // each buffer is a power of two, 1 MiB at least
  for (int i = 0; i < 8; i++) {
    EXPECT_EQ(Error::OK, prepare(Op::GET, "u1", 1)) << i;
  }
  EXPECT_EQ(Error::LIMIT, prepare(Op::GET, "u1", 1));
}

TEST_F(RcSession, ChargedUntilBackInTheArena)
{
  // a fixed limit of one buffer of 64 MiB
  START(4, {{"rgw_rdma_rc_max_bytes_per_user", "67108864"}});
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::GET, "u1", 64 * MiB, &r));
  Session* s = nullptr;
  ASSERT_EQ(Error::OK, svc->claim(ready("u1", r), &s));
  // a relay that may have left OSD writes in flight: the buffer is held
  // for a while after the session ends
  svc->finish(s, Outcome::BACKEND_FAIL, 0, 1500ms);
  SessionInfo info;
  EXPECT_EQ(Error::NO_SESSION, svc->peek(r.session_id, info));
  // the session is gone, but its buffer is still the user's
  EXPECT_EQ(Error::LIMIT, prepare(Op::GET, "u1", 1));
  EXPECT_EQ(Error::OK, prepare(Op::GET, "u2", 1));
  // until the reaper gives it back to the arena
  EXPECT_TRUE(eventually([&] {
    return prepare(Op::GET, "u1", 1) == Error::OK;
  }, 5s));
}

TEST_F(RcSession, LimitsPerOwner)
{
  // the subusers and role sessions of one account share its limits
  START(4, {{"rgw_rdma_rc_max_sessions_per_user", "2"}});
  const std::string acct = "RGW11111111111111111";
  const std::string role =
    acct + "||arn:aws:sts::" + acct + ":assumed-role/r/";
  auto prepare_as = [&](const std::string& principal,
                        const std::string& owner, PrepareReply& r) {
    auto req = request(Op::GET, principal, 1);
    req.owner = owner;
    return svc->prepare(req, r, null_yield);
  };
  PrepareReply r1, r2, r3;
  EXPECT_EQ(Error::OK, prepare_as(role + "s1", acct, r1));
  EXPECT_EQ(Error::OK, prepare_as(role + "s2", acct, r2));
  EXPECT_EQ(Error::LIMIT, prepare_as(role + "s3", acct, r3));
  EXPECT_EQ(Error::OK, prepare_as("u2|", "u2", r3));
  // a session still belongs to the principal that prepared it alone
  Session* s = nullptr;
  EXPECT_EQ(Error::SESSION, svc->claim(ready(role + "s2", r1), &s));
  EXPECT_EQ(Error::SESSION, svc->cancel(r1.session_id, role + "s2", null_yield));
  EXPECT_EQ(Error::OK, svc->cancel(r1.session_id, role + "s1", null_yield));
  EXPECT_TRUE(eventually([&] {
    PrepareReply r;
    return prepare_as(role + "s3", acct, r) == Error::OK;
  }, 5s));
}

TEST_F(RcSession, ReapedWhenPairingFails)
{
  START(4, {{"rgw_rdma_rc_max_sessions_per_user", "1"}});
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::PUT, "u1", 1, &r));
  ASSERT_EQ(Error::OK, svc->cancel(r.session_id, "u1", null_yield));
  auto server = decode_token(r.server_token);
  ASSERT_TRUE(server);
  // a peer GID of the other address family, which no route reaches from
  // the gateway's GID: RTR fails
  static const uint8_t v4_mapped[12] = {0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 0xff, 0xff};
  const bool server_v4 = std::equal(v4_mapped, v4_mapped + 12, server->gid);
  token_t peer;
  peer.transport = transport_t::RC;
  peer.qpn = 0x1234;
  if (server_v4) {
    // 2001:db8::1
    const uint8_t g[16] = {0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0,
                           0, 0, 0, 0, 0, 0, 0, 1};
    std::copy(g, g + 16, peer.gid);
  } else {
    // ::ffff:192.0.2.1
    const uint8_t g[16] = {0, 0, 0, 0, 0, 0, 0, 0,
                           0, 0, 0xff, 0xff, 192, 0, 2, 1};
    std::copy(g, g + 16, peer.gid);
  }
  auto req = request(Op::PUT, "u1", 1);
  req.client_token = encode_token(peer);
  ASSERT_TRUE(eventually([&] {
    return svc->prepare(req, r, null_yield) == Error::OK;
  }, 5s));
  Session* s = nullptr;
  ASSERT_EQ(Error::WIRE, svc->claim(ready("u1", r), &s));
  // gone at once, with its place, not at the reaper's next scan
  SessionInfo info;
  EXPECT_EQ(Error::NO_SESSION, svc->peek(r.session_id, info));
  EXPECT_EQ(Error::OK, prepare(Op::GET, "u1", 1));
}

TEST_F(RcSession, UnwoundWaitKeepsTheChannel)
{
  START(4);
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::PUT, "u1", 1, &r));
  Session* s = nullptr;
  ASSERT_EQ(Error::OK, svc->claim(ready("u1", r), &s));
  const int channels = completion_channels();
  {
    // a READY that waits for the client's write in a coroutine, which is
    // destroyed while it waits, as at shutdown
    boost::asio::io_context ctx;
    bool waiting = false;
    boost::asio::spawn(ctx, [&](boost::asio::yield_context yield) {
      waiting = true;
      ibv_wc wc{};
      s->conn.poll_one(wc, clock::now() + 30s, optional_yield{yield});
      waiting = false;
    }, [](std::exception_ptr) {});
    ctx.run_for(200ms);
    ASSERT_TRUE(waiting);
  }
  // the unwind left the channel's descriptor to the connection
  EXPECT_EQ(channels, completion_channels());
  svc->finish(s, Outcome::TIMEOUT, 0);
}

TEST_F(RcSession, LoopbackOnlyFromThisHost)
{
  START(4);
  PrepareReply r;
  auto req = request(Op::GET, "u1", 1);
  req.local_peer = false;
  EXPECT_EQ(Error::PEER, svc->prepare(req, r, null_yield));
  req.local_peer = true;
  EXPECT_EQ(Error::OK, svc->prepare(req, r, null_yield));
}

TEST_F(RcSession, HostGidsOnlyFromThisHost)
{
  // a token that is not the loopback marker but names a GID of this
  // host, such as the gateway's own one, pairs the gateway with a queue
  // pair of this host just the same: only a client on the host may send
  // one
  START(4, {{"rgw_rdma_rc_max_sessions_per_user", "1024"}});
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::GET, "u1", 1, &r));
  auto server = decode_token(r.server_token);
  ASSERT_TRUE(server);
  auto naming = [](const uint8_t* gid) {
    token_t t;
    t.transport = transport_t::RC;
    t.qpn = 0x1234;
    std::memcpy(t.gid, gid, sizeof(t.gid));
    return encode_token(t);
  };
  auto req = request(Op::GET, "u1", 1);
  req.local_peer = false;
  req.client_token = naming(server->gid);
  EXPECT_EQ(Error::PEER, svc->prepare(req, r, null_yield));
  req.local_peer = true;
  EXPECT_EQ(Error::OK, svc->prepare(req, r, null_yield));

  // every GID of every port of the host's devices, not only the one the
  // gateway uses, and a loopback address
  req.local_peer = false;
  int gids = 0;
  int n = 0;
  ibv_device** list = ibv_get_device_list(&n);
  for (int i = 0; list && i < n; i++) {
    ibv_context* c = ibv_open_device(list[i]);
    ibv_device_attr da{};
    if (!c || ibv_query_device(c, &da) != 0) {
      if (c) ibv_close_device(c);
      continue;
    }
    for (uint8_t p = 1; p <= da.phys_port_cnt; p++) {
      ibv_port_attr pa{};
      ibv_query_port(c, p, &pa);
      for (int g = 0; g < pa.gid_tbl_len; g++) {
        ibv_gid gid{};
        static const uint8_t zero[16] = {};
        if (ibv_query_gid(c, p, g, &gid) != 0 ||
            std::memcmp(gid.raw, zero, 16) == 0) {
          continue;
        }
        req.client_token = naming(gid.raw);
        EXPECT_EQ(Error::PEER, svc->prepare(req, r, null_yield))
          << ibv_get_device_name(list[i]) << " port " << int(p) << " gid "
          << g;
        gids++;
      }
    }
    ibv_close_device(c);
  }
  if (list) ibv_free_device_list(list);
  EXPECT_GT(gids, 0);
  const uint8_t v4_loopback[16] = {0, 0, 0, 0, 0, 0, 0, 0,
                                   0, 0, 0xff, 0xff, 127, 0, 0, 1};
  req.client_token = naming(v4_loopback);
  EXPECT_EQ(Error::PEER, svc->prepare(req, r, null_yield));

  // a GID of another host is a client's own
  const uint8_t other[16] = {0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0,
                             0, 0, 0, 0, 0, 0, 0, 1};
  req.client_token = naming(other);
  EXPECT_EQ(Error::OK, svc->prepare(req, r, null_yield));
}

TEST_F(RcSession, PutStagingIsZeroBased)
{
  START(4);
  constexpr uint32_t size = 4096;
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::PUT, "u1", size, &r));
  // the client writes at offsets of the staging buffer's registration,
  // and does not learn where the buffer is in the gateway's memory
  EXPECT_EQ(0u, r.staging_addr);
  auto server = decode_token(r.server_token);
  ASSERT_TRUE(server);
  EXPECT_EQ(0u, server->addr);
  EXPECT_EQ(r.staging_rkey, server->rkey);
  EXPECT_EQ(size, server->length);

  Client c;
  if (!c.open(server->gid)) {
    GTEST_SKIP() << "no device of this host carries the gateway's GID";
  }
  ASSERT_TRUE(c.pair(r.server_qpn, r.server_psn, 1));
  auto rr = ready("u1", r);
  rr.client_qpn = c.qp->qp_num;
  Session* s = nullptr;
  ASSERT_EQ(Error::OK, svc->claim(rr, &s));
  for (uint32_t i = 0; i < size; i++) {
    c.buf[i] = static_cast<char>(i * 7 + 3);
  }
  ASSERT_EQ(IBV_WC_SUCCESS,
            c.write_imm(r.staging_addr, r.staging_rkey, size, 0x1a2b3c4d));
  ibv_wc wc{};
  ASSERT_EQ(Completion::OK, s->conn.poll_one(wc, clock::now() + 10s,
                                             null_yield));
  EXPECT_EQ(IBV_WC_RECV_RDMA_WITH_IMM, wc.opcode);
  EXPECT_EQ(size, wc.byte_len);
  EXPECT_EQ(0x1a2b3c4du, ntohl(wc.imm_data));
  EXPECT_EQ(0, std::memcmp(s->buf->ptr, c.buf, size));
  svc->finish(s, Outcome::OK, size);
}

TEST_F(RcSession, PrepareYieldsForEndedSessions)
{
  // one block: a session of 64 MiB fills the arena
  START(1);
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::GET, "u1", 64 * MiB, &r));
  Session* s = nullptr;
  ASSERT_EQ(Error::OK, svc->claim(ready("u1", r), &s));
  svc->finish(s, Outcome::BACKEND_FAIL, 0, 1500ms);

  // a PREPARE in a coroutine, as the frontend runs it, that finds the
  // arena full looks for the buffers of ended sessions on a worker, and
  // resumes on its own executor
  auto prepare_in_coroutine = [&] {
    boost::asio::io_context ctx;
    Error e = Error::INTERNAL;
    std::thread::id prepared_on;
    boost::asio::spawn(ctx, [&](boost::asio::yield_context yield) {
      PrepareReply r2;
      e = svc->prepare(request(Op::GET, "u2", 64 * MiB), r2,
                       optional_yield{yield});
      prepared_on = std::this_thread::get_id();
    }, [](std::exception_ptr ex) { if (ex) std::rethrow_exception(ex); });
    ctx.run();
    // the coroutine resumed on the frontend's thread
    EXPECT_EQ(std::this_thread::get_id(), prepared_on);
    return e;
  };
  // the held buffer stays out of the arena for its quarantine
  EXPECT_EQ(Error::NO_BUFFER, prepare_in_coroutine());
  EXPECT_TRUE(eventually([&] {
    return prepare_in_coroutine() == Error::OK;
  }, 5s));
}

/// start the service with libfabric relay windows, over the tcp
/// provider on the IPv4 address of the RC device's GID, and claim a GET
/// session of 1 MiB that has one; why it cannot, or empty
std::string RcSession::start_relay(PrepareReply& r, Session*& s)
{
  if (auto why = start(4); !why.empty()) {
    return why;
  }
  if (prepare(Op::GET, "u1", 1, &r) != Error::OK) {
    return "no session";
  }
  auto server = decode_token(r.server_token);
  static const uint8_t v4_mapped[12] = {0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 0xff, 0xff};
  if (!server || !std::equal(v4_mapped, v4_mapped + 12, server->gid)) {
    return "the RC device's GID is not an IPv4 address";
  }
  const std::string addr = fmt::format("{}.{}.{}.{}", server->gid[12],
                                       server->gid[13], server->gid[14],
                                       server->gid[15]);
  Service::shutdown();
  if (auto why = start(4, {{"rgw_rdma_osd_direct", "true"},
                           {"oob_cluster_transport", "tcp"},
                           {"oob_addr", addr.c_str()}}); !why.empty()) {
    return why;
  }
  if (prepare(Op::GET, "u1", 1 * MiB, &r) != Error::OK ||
      svc->claim(ready("u1", r), &s) != Error::OK) {
    return "no claimed session";
  }
  if (!svc->osd_direct() || s->buf->osd_token.empty()) {
    svc->finish(s, Outcome::OK, 0);
    return "no libfabric relay window on " + addr;
  }
  return {};
}

TEST_F(RcSession, PrepareOnAPlainExecutor)
{
  // the request may run on a plain multi-threaded io_context, not a
  // strand, as a future frontend or a test does: the buffer's work runs
  // on a worker, and the coroutine resumes on its own executor just the
  // same. The arena is not full, so make_buffer() runs, not the wait
  // for ended sessions
  START(4);
  boost::asio::io_context ctx;
  std::atomic<int> ok = 0;
  const std::thread::id runner = std::this_thread::get_id();
  std::atomic<std::thread::id> resumed_on;
  for (int i = 0; i < 16; i++) {
    boost::asio::spawn(ctx, [&](boost::asio::yield_context yield) {
      PrepareReply r;
      if (svc->prepare(request(Op::GET, "u1", 1 * MiB), r,
                       optional_yield{yield}) == Error::OK) {
        ++ok;
      }
      resumed_on = std::this_thread::get_id();
    }, [](std::exception_ptr ex) { if (ex) std::rethrow_exception(ex); });
  }
  // several threads run the context, so the deferred work cannot rely on
  // a strand to order it after the coroutine suspended
  std::vector<std::thread> pool;
  for (int i = 0; i < 4; i++) {
    pool.emplace_back([&] { ctx.run(); });
  }
  for (auto& t : pool) {
    t.join();
  }
  EXPECT_EQ(16, ok);
  // the coroutine resumed on a context thread, not on a worker
  EXPECT_NE(runner, resumed_on.load());
}

TEST_F(RcSession, BusyRearmGivesTheRelayANewKey)
{
  PrepareReply r;
  Session* s = nullptr;
  if (auto why = start_relay(r, s); !why.empty()) {
    GTEST_SKIP() << why;
  }
  const std::string first = s->buf->osd_token;
  // the client's queue pair was not ready, and no OSD write of the data
  // phase can still land: the session goes back for another READY, but
  // a late duplicate of a write of the first must not land in the
  // second, so the window's key changes
  const Outcome o = svc->finish(s, Outcome::BUSY, 0, {}, true);
  if (o == Outcome::BUSY) {
    SessionInfo info;
    ASSERT_EQ(Error::OK, svc->peek(r.session_id, info));
    EXPECT_NE(first, s->buf->osd_token);
    ASSERT_EQ(Error::OK, svc->claim(ready("u1", r), &s));
    svc->finish(s, Outcome::OK, 0);
  } else {
    // a provider that cannot re-key, or a device that does not take the
    // queue pair back to INIT, ends the session instead
    EXPECT_EQ(Outcome::WIRE_FAIL, o);
    SessionInfo info;
    EXPECT_EQ(Error::NO_SESSION, svc->peek(r.session_id, info));
  }
}

TEST_F(RcSession, BusyAfterAStagedGetKeepsTheWindow)
{
  PrepareReply r;
  Session* s = nullptr;
  if (auto why = start_relay(r, s); !why.empty()) {
    GTEST_SKIP() << why;
  }
  const std::string first = s->buf->osd_token;
  // the data phase staged the object, and no OSD got the window's token:
  // the session goes back for another READY with the same window, also
  // over a provider that could not give the window a new key
  const Outcome o = svc->finish(s, Outcome::BUSY, 0);
  SessionInfo info;
  if (!svc->rearms_queue_pairs()) {
    // a device that does not take the queue pair back to INIT ends the
    // session instead
    EXPECT_EQ(Outcome::WIRE_FAIL, o);
    EXPECT_EQ(Error::NO_SESSION, svc->peek(r.session_id, info));
    return;
  }
  EXPECT_EQ(Outcome::BUSY, o);
  ASSERT_EQ(Error::OK, svc->peek(r.session_id, info));
  EXPECT_EQ(first, s->buf->osd_token);
  ASSERT_EQ(Error::OK, svc->claim(ready("u1", r), &s));
  svc->finish(s, Outcome::OK, 0);
}

TEST_F(RcSession, BusyWithAQuarantineEndsTheSession)
{
  START(4);
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::GET, "u1", 1, &r));
  Session* s = nullptr;
  ASSERT_EQ(Error::OK, svc->claim(ready("u1", r), &s));
  // an OSD write may still land: no retried READY, and no 409
  EXPECT_EQ(Outcome::WIRE_FAIL, svc->finish(s, Outcome::BUSY, 0, 100ms));
  SessionInfo info;
  EXPECT_EQ(Error::NO_SESSION, svc->peek(r.session_id, info));
}

namespace {

/// a READY's data phase, on a thread of its own: wait for the client's
/// write into a PUT session, then end the session
struct DataPhase {
  Service* svc;
  Session* s;
  Outcome result = Outcome::OK;
  std::thread t;

  DataPhase(Service* svc, Session* s) : svc(svc), s(s) {
    t = std::thread([this] {
      ibv_wc wc{};
      Outcome o = Outcome::WIRE_FAIL;
      switch (this->s->conn.poll_one(wc, clock::now() + 20s, null_yield)) {
      case Completion::OK: o = Outcome::OK; break;
      case Completion::TIMEOUT: o = Outcome::TIMEOUT; break;
      case Completion::BUSY: o = Outcome::BUSY; break;
      case Completion::WIRE_ERROR: o = Outcome::WIRE_FAIL; break;
      }
      result = this->svc->finish(this->s, o, 0);
    });
  }
  ~DataPhase() {
    if (t.joinable()) t.join();
  }
};

} // anonymous namespace

TEST_F(RcSession, CancelStopsTheDataPhase)
{
  START(4);
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::PUT, "u1", 4096, &r));
  Session* s = nullptr;
  ASSERT_EQ(Error::OK, svc->claim(ready("u1", r), &s));
  DataPhase phase(svc, s);
  std::this_thread::sleep_for(100ms);
  // the data phase stops long before its deadline, and CANCEL answers
  // once it has
  const auto start = clock::now();
  EXPECT_EQ(Error::OK, svc->cancel(r.session_id, "u1", null_yield));
  EXPECT_LT(clock::now() - start, 5s);
  SessionInfo info;
  EXPECT_EQ(Error::NO_SESSION, svc->peek(r.session_id, info));
  phase.t.join();
  EXPECT_EQ(Outcome::CANCELLED, phase.result);
}

TEST_F(RcSession, CancelYieldsWhileTheDataPhaseStops)
{
  START(4);
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::PUT, "u1", 4096, &r));
  Session* s = nullptr;
  ASSERT_EQ(Error::OK, svc->claim(ready("u1", r), &s));
  DataPhase phase(svc, s);
  std::this_thread::sleep_for(100ms);
  boost::asio::io_context ctx;
  Error e = Error::INTERNAL;
  const auto start = clock::now();
  boost::asio::spawn(ctx, [&](boost::asio::yield_context yield) {
    e = svc->cancel(r.session_id, "u1", optional_yield{yield});
  }, [](std::exception_ptr ex) { if (ex) std::rethrow_exception(ex); });
  ctx.run();
  EXPECT_EQ(Error::OK, e);
  EXPECT_LT(clock::now() - start, 5s);
  phase.t.join();
  EXPECT_EQ(Outcome::CANCELLED, phase.result);
}

TEST_F(RcSession, CancelWaitsOnceAndNotLong)
{
  // a data phase that does not stop, as one in a slow read from RADOS,
  // with a deadline well past CANCEL's wait
  START(4, {{"rgw_rdma_rc_exec_timeout_ms", "20000"}});
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::PUT, "u1", 4096, &r));
  Session* s = nullptr;
  ASSERT_EQ(Error::OK, svc->claim(ready("u1", r), &s));
  // the first CANCEL waits for it, holding its thread
  Error first = Error::INTERNAL;
  clock::duration first_took{};
  std::thread waiting([&] {
    const auto start = clock::now();
    first = svc->cancel(r.session_id, "u1", null_yield);
    first_took = clock::now() - start;
  });
  std::this_thread::sleep_for(200ms);
  // every other CANCEL meanwhile answers at once, instead of holding a
  // thread of its own until the deadline
  for (int i = 0; i < 3; i++) {
    const auto start = clock::now();
    EXPECT_EQ(Error::STATE, svc->cancel(r.session_id, "u1", null_yield));
    EXPECT_LT(clock::now() - start, 1s);
  }
  // and the first answers after CANCEL_WAIT, not at the deadline
  waiting.join();
  EXPECT_EQ(Error::STATE, first);
  EXPECT_GE(first_took, Service::CANCEL_WAIT - 100ms);
  EXPECT_LT(first_took, Service::CANCEL_WAIT + 3s);
  // the session ends when the data phase does
  EXPECT_EQ(Outcome::CANCELLED, svc->finish(s, Outcome::WIRE_FAIL, 0));
  SessionInfo info;
  EXPECT_EQ(Error::NO_SESSION, svc->peek(r.session_id, info));
}

TEST_F(RcSession, AReadyThatCancelStopsAnswersCancelled)
{
  // CANCEL can come at any step of a READY: before it, while READY
  // pairs the queue pair, during the data phase, or while finish() rolls
  // a BUSY back with the lock released. A READY that it stopped answers
  // that CANCEL stopped it, never that the session is gone or that the
  // transport failed
  START(4, {{"rgw_rdma_rc_max_sessions_per_user", "1024"}});
  std::map<std::string, int> claims, finishes;
  for (int i = 0; i < 600; i++) {
    PrepareReply r;
    ASSERT_TRUE(eventually([&] {
      return prepare(Op::PUT, "u1", 4096, &r) == Error::OK;
    }, 5s));
    std::atomic<bool> go = false;
    // across the claim and the rollback, which take some 100 us each
    const auto delay = std::chrono::microseconds(i % 300);
    std::thread canceller([&] {
      while (!go) {}
      const auto until = clock::now() + delay;
      while (clock::now() < until) {}
      svc->cancel(r.session_id, "u1", null_yield);
    });
    go = true;
    Session* s = nullptr;
    const Error e = svc->claim(ready("u1", r), &s);
    claims[to_string(e)]++;
    EXPECT_TRUE(e == Error::OK || e == Error::NO_SESSION ||
                e == Error::CANCELLED) << to_string(e);
    if (e == Error::OK) {
      const Outcome o = svc->finish(s, Outcome::BUSY, 0);
      finishes[to_string(o)]++;
      // a device that does not take the queue pair back to INIT ends
      // the session where CANCEL did not stop it
      EXPECT_TRUE(o == Outcome::BUSY || o == Outcome::CANCELLED ||
                  (!svc->rearms_queue_pairs() && o == Outcome::WIRE_FAIL))
        << to_string(o);
    }
    canceller.join();
    SessionInfo info;
    EXPECT_EQ(Error::NO_SESSION, svc->peek(r.session_id, info));
  }
  for (const auto& [k, n] : claims) {
    std::cout << "claim " << k << ": " << n << std::endl;
  }
  for (const auto& [k, n] : finishes) {
    std::cout << "finish " << k << ": " << n << std::endl;
  }
}

TEST_F(RcSession, CancelAfterTheDataPhaseEnded)
{
  // a session that is not transferring ends at once, as before
  START(4);
  PrepareReply r;
  ASSERT_EQ(Error::OK, prepare(Op::PUT, "u1", 4096, &r));
  EXPECT_EQ(Error::OK, svc->cancel(r.session_id, "u1", null_yield));
  EXPECT_EQ(Error::STALE, svc->cancel(r.session_id, "u1", null_yield));
}
