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

#include "rgw_rdma_rc_transport.h"

// Provenance: the GID preference order below follows the host-memory DC
// client in versitygw's cuwrapper (Apache-2.0), and the RC queue pair
// attributes follow hipObject's v2 transport (MIT), so that the pairing
// matches what hipObject clients expect.

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <thread>

#include <arpa/inet.h>
#include <fcntl.h>

#include <boost/asio/cancel_at.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>

#include "common/async/yield_context.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/scope_guard.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::rdma::rc {

namespace {

/// vendor IDs of the in-kernel software providers
constexpr uint32_t RXE_VENDOR_ID = 0xffffff;
constexpr uint32_t SIW_VENDOR_ID = 0x626d74;

bool gid_is_zero(const ibv_gid& g)
{
  for (uint8_t b : g.raw) {
    if (b) return false;
  }
  return true;
}

bool gid_is_link_local(const ibv_gid& g)
{
  return g.raw[0] == 0xfe && (g.raw[1] & 0xc0) == 0x80;
}

// RoCEv2 entries are preferred: they route across IP subnets. The raw
// bytes of a v1/v2 pair are identical, so only the type query tells
// them apart; providers that cannot answer are not excluded.
bool gid_is_roce_v2(ibv_context* ctx, uint8_t port, int index)
{
  ibv_gid_entry entry;
  std::memset(&entry, 0, sizeof(entry));
  if (ibv_query_gid_ex(ctx, port, static_cast<uint32_t>(index), &entry, 0)) {
    return true;
  }
  return entry.gid_type == IBV_GID_TYPE_ROCE_V2;
}

std::string gid_to_string(const ibv_gid& g)
{
  char buf[INET6_ADDRSTRLEN] = {};
  inet_ntop(AF_INET6, g.raw, buf, sizeof(buf));
  return buf;
}

} // anonymous namespace

Device::~Device()
{
  close();
}

int Device::select_gid(CephContext* cct, int wanted)
{
  if (wanted >= 0) {
    if (ibv_query_gid(ctx, port, wanted, &local_gid)) {
      lderr(cct) << "rgw_rdma_rc: ibv_query_gid(" << wanted << ") failed: "
                 << cpp_strerror(errno) << dendl;
      return -errno;
    }
    // an empty entry of the table reads as all zeroes: no pairing that
    // routes on it can succeed
    if (gid_is_zero(local_gid)) {
      lderr(cct) << "rgw_rdma_rc: " << dev_name << " port " << int(port)
                 << " has no GID at index " << wanted << dendl;
      return -ENODEV;
    }
    gid_index = wanted;
    return 0;
  }

  ibv_port_attr pa;
  std::memset(&pa, 0, sizeof(pa));
  if (ibv_query_port(ctx, port, &pa)) {
    lderr(cct) << "rgw_rdma_rc: ibv_query_port failed: "
               << cpp_strerror(errno) << dendl;
    return -errno;
  }
  if (pa.link_layer == IBV_LINK_LAYER_INFINIBAND) {
    // the port GID at index 0 is the only entry; it is link-local by
    // design
    if (ibv_query_gid(ctx, port, 0, &local_gid) || gid_is_zero(local_gid)) {
      lderr(cct) << "rgw_rdma_rc: InfiniBand port has no GID at index 0"
                 << dendl;
      return -ENODEV;
    }
    gid_index = 0;
    return 0;
  }

  int fallback = -1;
  ibv_gid fallback_gid = {};
  for (int i = 0; i < pa.gid_tbl_len; i++) {
    ibv_gid g = {};
    if (ibv_query_gid(ctx, port, i, &g) || gid_is_zero(g) ||
        gid_is_link_local(g)) {
      continue;
    }
    if (gid_is_roce_v2(ctx, port, i)) {
      gid_index = i;
      local_gid = g;
      return 0;
    }
    if (fallback < 0) {
      fallback = i;
      fallback_gid = g;
    }
  }
  if (fallback >= 0) {
    gid_index = fallback;
    local_gid = fallback_gid;
    return 0;
  }
  lderr(cct) << "rgw_rdma_rc: no usable GID on " << dev_name << " port "
             << int(port) << "; set oob_network to an address of a RoCE v2 GID" << dendl;
  return -ENODEV;
}

int Device::open(CephContext* cct, const DeviceConfig& cfg)
{
  int num = 0;
  ibv_device** list = ibv_get_device_list(&num);
  if (!list || num == 0) {
    lderr(cct) << "rgw_rdma_rc: no verbs devices found" << dendl;
    if (list) ibv_free_device_list(list);
    return -ENODEV;
  }

  ibv_device* dev = nullptr;
  if (!cfg.device_name.empty()) {
    for (int i = 0; i < num; i++) {
      if (cfg.device_name == ibv_get_device_name(list[i])) {
        dev = list[i];
        break;
      }
    }
    if (!dev) {
      lderr(cct) << "rgw_rdma_rc: verbs device " << cfg.device_name
                 << " not found" << dendl;
      ibv_free_device_list(list);
      return -ENODEV;
    }
  } else {
    dev = list[0];
  }

  dev_name = ibv_get_device_name(dev);
  port = cfg.port ? cfg.port : 1;
  ctx = ibv_open_device(dev);
  ibv_free_device_list(list);
  if (!ctx) {
    lderr(cct) << "rgw_rdma_rc: ibv_open_device(" << dev_name << ") failed: "
               << cpp_strerror(errno) << dendl;
    return -EIO;
  }
  pd = ibv_alloc_pd(ctx);
  if (!pd) {
    lderr(cct) << "rgw_rdma_rc: ibv_alloc_pd failed: " << cpp_strerror(errno)
               << dendl;
    close();
    return -EIO;
  }
  if (int r = select_gid(cct, cfg.gid_index); r < 0) {
    close();
    return r;
  }
  ibv_port_attr pa;
  std::memset(&pa, 0, sizeof(pa));
  if (ibv_query_port(ctx, port, &pa)) {
    lderr(cct) << "rgw_rdma_rc: ibv_query_port failed: "
               << cpp_strerror(errno) << dendl;
    close();
    return -EIO;
  }
  local_lid = pa.lid;
  // the path MTU must not exceed the port's active MTU
  mtu = pa.active_mtu >= IBV_MTU_512 ? pa.active_mtu : IBV_MTU_1024;
  // nor, for a queue pair paired with a client, the client's: both ends
  // must use the same one, and hipobj-rc-v2 does not carry it
  if (auto m = path_mtu_of(cfg.client_mtu, mtu); m) {
    client_mtu = *m;
  } else {
    lderr(cct) << "rgw_rdma_rc: a path MTU of " << cfg.client_mtu
               << " bytes is below 256" << dendl;
    close();
    return -EINVAL;
  }

  ibv_device_attr da;
  std::memset(&da, 0, sizeof(da));
  if (ibv_query_device(ctx, &da) == 0 &&
      (da.vendor_id == RXE_VENDOR_ID || da.vendor_id == SIW_VENDOR_ID)) {
    ldout(cct, 1) << "rgw_rdma_rc: " << dev_name << " is a software provider; "
                  << "posting sends from the thread that opened it" << dendl;
    start_post_proxy();
  }
  qp_reuse = probe_qp_reuse();
  if (!qp_reuse) {
    ldout(cct, 1) << "rgw_rdma_rc: " << dev_name << " does not take a queue "
                  << "pair from RESET back to INIT; a READY that finds the "
                  << "client not ready ends its session" << dendl;
  }

  ldout(cct, 1) << "rgw_rdma_rc: opened " << dev_name << " port " << int(port)
                << " gid[" << gid_index << "]=" << gid_to_string(local_gid)
                << " lid=" << local_lid << " mtu=" << (128 << mtu)
                << " client mtu=" << (128 << client_mtu) << dendl;
  return 0;
}

bool Device::probe_qp_reuse()
{
  ibv_cq* cq = ibv_create_cq(ctx, 1, nullptr, nullptr, 0);
  if (!cq) {
    // as the verbs spec allows; Connection::rearm() reports a failure
    return true;
  }
  ibv_qp_init_attr ia;
  std::memset(&ia, 0, sizeof(ia));
  ia.send_cq = cq;
  ia.recv_cq = cq;
  ia.qp_type = IBV_QPT_RC;
  ia.cap.max_send_wr = 1;
  ia.cap.max_recv_wr = 1;
  ia.cap.max_send_sge = 1;
  ia.cap.max_recv_sge = 1;
  bool reuse = true;
  if (ibv_qp* qp = ibv_create_qp(pd, &ia); qp) {
    const int init_mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS;
    const auto to_init = [&] {
      ibv_qp_attr a;
      std::memset(&a, 0, sizeof(a));
      a.qp_state = IBV_QPS_INIT;
      a.port_num = port;
      a.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;
      return ibv_modify_qp(qp, &a, init_mask) == 0;
    };
    if (to_init()) {
      ibv_qp_attr a;
      std::memset(&a, 0, sizeof(a));
      a.qp_state = IBV_QPS_RESET;
      if (ibv_modify_qp(qp, &a, IBV_QP_STATE) == 0) {
        reuse = to_init();
      }
    }
    ibv_destroy_qp(qp);
  }
  ibv_destroy_cq(cq);
  return reuse;
}

void Device::start_post_proxy()
{
  post_proxy = true;
  proxy_stop = false;
  // a thread shares its creator's credentials, and this runs on the
  // thread that just opened the device
  proxy = std::thread([this] {
    std::unique_lock l(proxy_mtx);
    for (;;) {
      proxy_cv.wait(l, [this] { return proxy_stop || !proxy_jobs.empty(); });
      if (proxy_jobs.empty()) {
        return;  // stopping with nothing queued
      }
      post_job* job = proxy_jobs.front();
      proxy_jobs.pop_front();
      l.unlock();
      int r = ibv_post_send(job->qp, job->wr, job->bad);
      l.lock();
      job->result = r;
      job->done = true;
      proxy_done.notify_all();
    }
  });
}

void Device::stop_post_proxy()
{
  if (!proxy.joinable()) {
    return;
  }
  {
    std::lock_guard l(proxy_mtx);
    proxy_stop = true;
  }
  proxy_cv.notify_all();
  proxy.join();
  post_proxy = false;
}

int Device::post_send(ibv_qp* qp, ibv_send_wr* wr, ibv_send_wr** bad)
{
  if (!post_proxy) {
    return ibv_post_send(qp, wr, bad);
  }
  post_job job{qp, wr, bad};
  std::unique_lock l(proxy_mtx);
  proxy_jobs.push_back(&job);
  proxy_cv.notify_one();
  proxy_done.wait(l, [&job] { return job.done; });
  return job.result;
}

void Device::close()
{
  stop_post_proxy();
  if (pd) {
    ibv_dealloc_pd(pd);
    pd = nullptr;
  }
  if (ctx) {
    ibv_close_device(ctx);
    ctx = nullptr;
  }
}

ibv_pd* Device::alloc_pd()
{
  return ibv_alloc_pd(ctx);
}

int Device::dealloc_pd(ibv_pd* pd)
{
  return pd ? ibv_dealloc_pd(pd) : 0;
}

ibv_mr* Device::register_memory(void* ptr, size_t len, int access,
                                ibv_pd* on, std::optional<uint64_t> iova)
{
  if (iova) {
    return ibv_reg_mr_iova(on ? on : pd, ptr, len, *iova, access);
  }
  return ibv_reg_mr(on ? on : pd, ptr, len, access);
}

int Device::deregister_memory(ibv_mr* mr)
{
  return mr ? ibv_dereg_mr(mr) : 0;
}

Connection::~Connection()
{
  destroy();
}

int Connection::create(CephContext* cct, Device& dev, uint32_t send_depth,
                       uint32_t recv_depth, int access, ibv_pd* pd)
{
  this->dev = &dev;
  this->access = access;
  // a request waits for its completions on the channel; reading it must
  // never block the frontend thread
  channel = ibv_create_comp_channel(dev.context());
  if (!channel) {
    lderr(cct) << "rgw_rdma_rc: ibv_create_comp_channel failed: "
               << cpp_strerror(errno) << dendl;
    return -EIO;
  }
  if (const int fl = fcntl(channel->fd, F_GETFL);
      fl < 0 || fcntl(channel->fd, F_SETFL, fl | O_NONBLOCK) < 0) {
    lderr(cct) << "rgw_rdma_rc: making the completion channel non-blocking "
               << "failed: " << cpp_strerror(errno) << dendl;
    destroy();
    return -EIO;
  }
  const int cq_depth = static_cast<int>(send_depth + recv_depth);
  cq = ibv_create_cq(dev.context(), cq_depth, nullptr, channel, 0);
  if (!cq) {
    lderr(cct) << "rgw_rdma_rc: ibv_create_cq failed: " << cpp_strerror(errno)
               << dendl;
    destroy();
    return -EIO;
  }
  ibv_qp_init_attr init;
  std::memset(&init, 0, sizeof(init));
  init.qp_type = IBV_QPT_RC;
  init.send_cq = cq;
  init.recv_cq = cq;
  init.cap.max_send_wr = send_depth;
  init.cap.max_recv_wr = recv_depth;
  init.cap.max_send_sge = 1;
  init.cap.max_recv_sge = 1;
  qp = ibv_create_qp(pd ? pd : dev.protection_domain(), &init);
  if (!qp) {
    lderr(cct) << "rgw_rdma_rc: ibv_create_qp failed: " << cpp_strerror(errno)
               << dendl;
    destroy();
    return -EIO;
  }

  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = dev.port_num();
  attr.qp_access_flags = access;
  if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                    IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
    lderr(cct) << "rgw_rdma_rc: qp -> INIT failed: " << cpp_strerror(errno)
               << dendl;
    destroy();
    return -EIO;
  }
  inflight = 0;
  return 0;
}

void Connection::destroy()
{
  if (qp) {
    ibv_destroy_qp(qp);
    qp = nullptr;
  }
  // wait_event() acknowledges each event it reads, which destroying the
  // queue requires
  if (cq) {
    ibv_destroy_cq(cq);
    cq = nullptr;
  }
  if (channel) {
    ibv_destroy_comp_channel(channel);
    channel = nullptr;
  }
  armed = false;
  inflight = 0;
}

int Connection::pair(CephContext* cct, Device& dev, const PeerEndpoint& peer,
                     uint32_t local_psn)
{
  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = dev.client_path_mtu();
  attr.dest_qp_num = peer.qpn;
  attr.rq_psn = peer.psn;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 12;
  // the GRH carries the peer GID (RoCE routes on it alone); on an
  // InfiniBand link layer the fabric switches on the DLID, which the
  // client's token carries
  attr.ah_attr.is_global = 1;
  attr.ah_attr.dlid = peer.lid;
  attr.ah_attr.grh.dgid = peer.gid;
  attr.ah_attr.grh.hop_limit = 64;
  attr.ah_attr.grh.sgid_index = static_cast<uint8_t>(dev.gid_idx());
  attr.ah_attr.grh.traffic_class = 0;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = dev.port_num();
  if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                    IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                    IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER)) {
    lderr(cct) << "rgw_rdma_rc: qp -> RTR failed: " << cpp_strerror(errno)
               << " (peer qpn=" << peer.qpn << " gid="
               << gid_to_string(peer.gid) << " lid=" << peer.lid << ")"
               << dendl;
    return -EIO;
  }

  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 14;
  attr.retry_cnt = 7;
  // not 7, which retries for ever: when the client has not armed the
  // receive for a GET's final write-with-immediate, the write fails
  // after six retries, each a wait of the client's min_rnr_timer, and
  // READY fails then instead of waiting out the session's deadline
  attr.rnr_retry = 6;
  attr.sq_psn = local_psn;
  attr.max_rd_atomic = 1;
  if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT |
                    IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                    IBV_QP_MAX_QP_RD_ATOMIC)) {
    lderr(cct) << "rgw_rdma_rc: qp -> RTS failed: " << cpp_strerror(errno)
               << dendl;
    return -EIO;
  }
  return 0;
}

int Connection::rearm(CephContext* cct, Device& dev)
{
  // there is no RTS -> INIT edge; RESET is reachable from every state
  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RESET;
  if (ibv_modify_qp(qp, &attr, IBV_QP_STATE)) {
    lderr(cct) << "rgw_rdma_rc: qp -> RESET failed: " << cpp_strerror(errno)
               << dendl;
    return -EIO;
  }
  // drain any completions the failed transfer left behind
  ibv_wc wc;
  while (ibv_poll_cq(cq, 1, &wc) > 0) {
  }
  inflight = 0;

  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = dev.port_num();
  attr.qp_access_flags = access;
  if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                    IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
    lderr(cct) << "rgw_rdma_rc: qp -> INIT (rearm) failed: "
               << cpp_strerror(errno) << dendl;
    return -EIO;
  }
  return 0;
}

int Connection::abort(CephContext* cct)
{
  if (!qp) {
    return 0;
  }
  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_ERR;
  if (ibv_modify_qp(qp, &attr, IBV_QP_STATE)) {
    lderr(cct) << "rgw_rdma_rc: qp -> ERR failed: " << cpp_strerror(errno)
               << dendl;
    return -EIO;
  }
  return 0;
}

int Connection::post_write(ibv_mr* mr, const void* ptr, uint32_t len,
                           uint64_t remote_addr, uint32_t rkey,
                           bool with_imm, uint32_t imm, uint64_t wr_id,
                           bool signaled)
{
  ibv_sge sge;
  std::memset(&sge, 0, sizeof(sge));
  sge.addr = reinterpret_cast<uintptr_t>(ptr);
  sge.length = len;
  sge.lkey = mr->lkey;

  ibv_send_wr wr;
  std::memset(&wr, 0, sizeof(wr));
  wr.wr_id = wr_id;
  wr.opcode = with_imm ? IBV_WR_RDMA_WRITE_WITH_IMM : IBV_WR_RDMA_WRITE;
  wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
  wr.imm_data = htonl(imm);
  wr.wr.rdma.remote_addr = remote_addr;
  wr.wr.rdma.rkey = rkey;
  wr.sg_list = &sge;
  wr.num_sge = len ? 1 : 0;

  ibv_send_wr* bad = nullptr;
  if (int r = dev->post_send(qp, &wr, &bad); r) {
    return r > 0 ? -r : r;
  }
  if (signaled) {
    ++inflight;
  }
  return 0;
}

int Connection::post_recv(ibv_mr* mr, uint64_t addr, uint32_t len,
                          uint64_t wr_id)
{
  ibv_sge sge;
  std::memset(&sge, 0, sizeof(sge));
  sge.addr = addr;
  sge.length = len;
  sge.lkey = mr->lkey;

  ibv_recv_wr wr;
  std::memset(&wr, 0, sizeof(wr));
  wr.wr_id = wr_id;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  ibv_recv_wr* bad = nullptr;
  if (int r = ibv_post_recv(qp, &wr, &bad); r) {
    return r > 0 ? -r : r;
  }
  return 0;
}

Completion Connection::poll_one(ibv_wc& wc,
                                std::chrono::steady_clock::time_point deadline,
                                optional_yield y)
{
  for (;;) {
    const int n = ibv_poll_cq(cq, 1, &wc);
    if (n < 0) {
      return Completion::WIRE_ERROR;
    }
    if (n > 0) {
      if (wc.status == IBV_WC_SUCCESS) {
        return Completion::OK;
      }
      if (wc.status == IBV_WC_RNR_RETRY_EXC_ERR ||
          wc.status == IBV_WC_RETRY_EXC_ERR) {
        // the peer's receive queue was not armed or it did not answer:
        // retryable from a fresh pairing, not a wire defect
        return Completion::BUSY;
      }
      return Completion::WIRE_ERROR;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return Completion::TIMEOUT;
    }
    if (!y) {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
      continue;
    }
    if (!armed) {
      // ask for an event, then poll once more: a completion that came
      // before the request raises none
      if (ibv_req_notify_cq(cq, 0) == 0) {
        armed = true;
        continue;
      }
      // no event will come; poll again after a short wait instead
    }
    const auto until = armed ? deadline :
      std::min(deadline, now + std::chrono::milliseconds(1));
    if (wait_event(y, until) < 0) {
      return Completion::WIRE_ERROR;
    }
  }
}

int Connection::wait_event(optional_yield y,
                           std::chrono::steady_clock::time_point until)
{
  auto& yield = y.get_yield_context();
  // the channel keeps its descriptor: borrow it for this wait only, on
  // the request's executor, which may change between READYs
  boost::asio::posix::stream_descriptor fd(yield.get_executor());
  boost::system::error_code ec;
  fd.assign(channel->fd, ec);
  if (ec) {
    return -ec.value();
  }
  // and give it back on every way out, an unwind of the coroutine during
  // the wait included: the descriptor would close it otherwise, and
  // destroy() closes it again in ibv_destroy_comp_channel(), when the
  // number may name another file
  auto give_back = make_scope_guard([&fd] { fd.release(); });
  fd.async_wait(boost::asio::posix::descriptor_base::wait_read,
                boost::asio::cancel_at(until, yield[ec]));
  // readable means an event, unless the wait ran out
  ibv_cq* ev_cq = nullptr;
  void* ev_ctx = nullptr;
  if (ibv_get_cq_event(channel, &ev_cq, &ev_ctx) == 0) {
    ibv_ack_cq_events(ev_cq, 1);
    armed = false;
  } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
    return -errno;
  }
  return 0;
}

} // namespace rgw::rdma::rc
