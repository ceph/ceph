// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "Infiniband.h"
#include "RDMAStack.h"
#include "Device.h"

#include "common/errno.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "Infiniband "

static const uint32_t MAX_INLINE_DATA = 0;

Infiniband::QueuePair::QueuePair(
    CephContext *c, Device &device, ibv_qp_type type,
    int port, ibv_srq *srq,
    Infiniband::CompletionQueue* txcq, Infiniband::CompletionQueue* rxcq,
    uint32_t max_send_wr, uint32_t max_recv_wr, uint32_t q_key)
: cct(c), ibdev(device),
  type(type),
  ctxt(ibdev.ctxt),
  ib_physical_port(port),
  pd(ibdev.pd->pd),
  srq(srq),
  qp(NULL),
  txcq(txcq),
  rxcq(rxcq),
  initial_psn(0),
  max_send_wr(max_send_wr),
  max_recv_wr(max_recv_wr),
  q_key(q_key),
  dead(false)
{
  initial_psn = lrand48() & 0xffffff;
  if (type != IBV_QPT_RC && type != IBV_QPT_UD && type != IBV_QPT_RAW_PACKET) {
    lderr(cct) << __func__ << " invalid queue pair type" << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
}

int Infiniband::QueuePair::init()
{
  ldout(cct, 20) << __func__ << " started." << dendl;
  ibv_qp_init_attr qpia;
  memset(&qpia, 0, sizeof(qpia));
  qpia.send_cq = txcq->get_cq();
  qpia.recv_cq = rxcq->get_cq();
  qpia.srq = srq;                      // use the same shared receive queue
  qpia.cap.max_send_wr  = max_send_wr; // max outstanding send requests
  qpia.cap.max_send_sge = 1;           // max send scatter-gather elements
  qpia.cap.max_inline_data = MAX_INLINE_DATA;          // max bytes of immediate data on send q
  qpia.qp_type = type;                 // RC, UC, UD, or XRC
  qpia.sq_sig_all = 0;                 // only generate CQEs on requested WQEs

  qp = ibv_create_qp(pd, &qpia);
  if (qp == NULL) {
    lderr(cct) << __func__ << " failed to create queue pair" << cpp_strerror(errno) << dendl;
    if (errno == ENOMEM) {
      lderr(cct) << __func__ << " try reducing ms_async_rdma_receive_buffers, "
				" ms_async_rdma_send_buffers or"
				" ms_async_rdma_buffer_size" << dendl;
    }
    return -1;
  }

  ldout(cct, 20) << __func__ << " successfully create queue pair: "
                 << "qp=" << qp << dendl;

  // move from RESET to INIT state
  ibv_qp_attr qpa;
  memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state   = IBV_QPS_INIT;
  qpa.pkey_index = 0;
  qpa.port_num   = (uint8_t)(ib_physical_port);
  qpa.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
  qpa.qkey       = q_key;

  int mask = IBV_QP_STATE | IBV_QP_PORT;
  switch (type) {
    case IBV_QPT_RC:
      mask |= IBV_QP_ACCESS_FLAGS;
      mask |= IBV_QP_PKEY_INDEX;
      break;
    case IBV_QPT_UD:
      mask |= IBV_QP_QKEY;
      mask |= IBV_QP_PKEY_INDEX;
      break;
    case IBV_QPT_RAW_PACKET:
      break;
    default:
      ceph_abort();
  }

  int ret = ibv_modify_qp(qp, &qpa, mask);
  if (ret) {
    ibv_destroy_qp(qp);
    lderr(cct) << __func__ << " failed to transition to INIT state: "
               << cpp_strerror(errno) << dendl;
    return -1;
  }
  ldout(cct, 20) << __func__ << " successfully change queue pair to INIT:"
                 << " qp=" << qp << dendl;
  return 0;
}

/**
 * Change RC QueuePair into the ERROR state. This is necessary modify
 * the Queue Pair into the Error state and poll all of the relevant
 * Work Completions prior to destroying a Queue Pair.
 * Since destroying a Queue Pair does not guarantee that its Work
 * Completions are removed from the CQ upon destruction. Even if the
 * Work Completions are already in the CQ, it might not be possible to
 * retrieve them. If the Queue Pair is associated with an SRQ, it is
 * recommended wait for the affiliated event IBV_EVENT_QP_LAST_WQE_REACHED
 *
 * \return
 *      -errno if the QueuePair can't switch to ERROR
 *      0 for success.
 */
int Infiniband::QueuePair::to_dead()
{
  if (dead)
    return 0;
  ibv_qp_attr qpa;
  memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_ERR;

  int mask = IBV_QP_STATE;
  int ret = ibv_modify_qp(qp, &qpa, mask);
  if (ret) {
    lderr(cct) << __func__ << " failed to transition to ERROR state: "
               << cpp_strerror(errno) << dendl;
    return -errno;
  }
  dead = true;
  return ret;
}

int Infiniband::QueuePair::get_remote_qp_number(uint32_t *rqp) const
{
  ibv_qp_attr qpa;
  ibv_qp_init_attr qpia;

  int r = ibv_query_qp(qp, &qpa, IBV_QP_DEST_QPN, &qpia);
  if (r) {
    lderr(cct) << __func__ << " failed to query qp: "
      << cpp_strerror(errno) << dendl;
    return -1;
  }

  if (rqp)
    *rqp = qpa.dest_qp_num;
  return 0;
}

/**
 * Get the remote infiniband address for this QueuePair, as set in #plumb().
 * LIDs are "local IDs" in infiniband terminology. They are short, locally
 * routable addresses.
 */
int Infiniband::QueuePair::get_remote_lid(uint16_t *lid) const
{
  ibv_qp_attr qpa;
  ibv_qp_init_attr qpia;

  int r = ibv_query_qp(qp, &qpa, IBV_QP_AV, &qpia);
  if (r) {
    lderr(cct) << __func__ << " failed to query qp: "
      << cpp_strerror(errno) << dendl;
    return -1;
  }

  if (lid)
    *lid = qpa.ah_attr.dlid;
  return 0;
}

/**
 * Get the state of a QueuePair.
 */
int Infiniband::QueuePair::get_state() const
{
  ibv_qp_attr qpa;
  ibv_qp_init_attr qpia;

  int r = ibv_query_qp(qp, &qpa, IBV_QP_STATE, &qpia);
  if (r) {
    lderr(cct) << __func__ << " failed to get state: "
      << cpp_strerror(errno) << dendl;
    return -1;
  }
  return qpa.qp_state;
}

/**
 * Return true if the queue pair is in an error state, false otherwise.
 */
bool Infiniband::QueuePair::is_error() const
{
  ibv_qp_attr qpa;
  ibv_qp_init_attr qpia;

  int r = ibv_query_qp(qp, &qpa, -1, &qpia);
  if (r) {
    lderr(cct) << __func__ << " failed to get state: "
      << cpp_strerror(errno) << dendl;
    return true;
  }
  return qpa.cur_qp_state == IBV_QPS_ERR;
}


Infiniband::CompletionChannel::CompletionChannel(CephContext *c, Device &ibdev)
  : cct(c), ibdev(ibdev), channel(NULL), cq(NULL), cq_events_that_need_ack(0)
{
}

Infiniband::CompletionChannel::~CompletionChannel()
{
  if (channel) {
    int r = ibv_destroy_comp_channel(channel);
    if (r < 0)
      lderr(cct) << __func__ << " failed to destroy cc: " << cpp_strerror(errno) << dendl;
    assert(r == 0);
  }
}

int Infiniband::CompletionChannel::init()
{
  ldout(cct, 20) << __func__ << " started." << dendl;
  channel = ibv_create_comp_channel(ibdev.ctxt);
  if (!channel) {
    lderr(cct) << __func__ << " failed to create receive completion channel: "
                          << cpp_strerror(errno) << dendl;
    return -1;
  }
  int rc = NetHandler(cct).set_nonblock(channel->fd);
  if (rc < 0) {
    ibv_destroy_comp_channel(channel);
    return -1;
  }
  return 0;
}

void Infiniband::CompletionChannel::ack_events()
{
  ibv_ack_cq_events(cq, cq_events_that_need_ack);
  cq_events_that_need_ack = 0;
}

bool Infiniband::CompletionChannel::get_cq_event()
{
  ibv_cq *cq = NULL;
  void *ev_ctx;
  if (ibv_get_cq_event(channel, &cq, &ev_ctx)) {
    if (errno != EAGAIN && errno != EINTR)
      lderr(cct) << __func__ << " failed to retrieve CQ event: "
                 << cpp_strerror(errno) << dendl;
    return false;
  }

  /* accumulate number of cq events that need to
   *    * be acked, and periodically ack them
   *       */
  if (++cq_events_that_need_ack == MAX_ACK_EVENT) {
    ldout(cct, 20) << __func__ << " ack aq events." << dendl;
    ibv_ack_cq_events(cq, MAX_ACK_EVENT);
    cq_events_that_need_ack = 0;
  }

  return true;
}


Infiniband::CompletionQueue::~CompletionQueue()
{
  if (cq) {
    int r = ibv_destroy_cq(cq);
    if (r < 0)
      lderr(cct) << __func__ << " failed to destroy cq: " << cpp_strerror(errno) << dendl;
    assert(r == 0);
  }
}

int Infiniband::CompletionQueue::init()
{
  cq = ibv_create_cq(ibdev.ctxt, queue_depth, this, channel->get_channel(), 0);
  if (!cq) {
    lderr(cct) << __func__ << " failed to create receive completion queue: "
      << cpp_strerror(errno) << dendl;
    return -1;
  }

  if (ibv_req_notify_cq(cq, 0)) {
    lderr(cct) << __func__ << " ibv_req_notify_cq failed: " << cpp_strerror(errno) << dendl;
    ibv_destroy_cq(cq);
    cq = nullptr;
    return -1;
  }

  channel->bind_cq(cq);
  ldout(cct, 20) << __func__ << " successfully create cq=" << cq << dendl;
  return 0;
}

int Infiniband::CompletionQueue::rearm_notify(bool solicite_only)
{
  ldout(cct, 20) << __func__ << " started." << dendl;
  int r = ibv_req_notify_cq(cq, 0);
  if (r < 0)
    lderr(cct) << __func__ << " failed to notify cq: " << cpp_strerror(errno) << dendl;
  return r;
}

int Infiniband::CompletionQueue::poll_cq(int num_entries, ibv_wc *ret_wc_array) {
  int r = ibv_poll_cq(cq, num_entries, ret_wc_array);
  if (r < 0) {
    lderr(cct) << __func__ << " poll_completion_queue occur met error: "
      << cpp_strerror(errno) << dendl;
    return -1;
  }
  return r;
}


Infiniband::ProtectionDomain::ProtectionDomain(CephContext *cct, Device *device)
  : pd(ibv_alloc_pd(device->ctxt))
{
  if (pd == NULL) {
    lderr(cct) << __func__ << " failed to allocate infiniband protection domain: " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
}

Infiniband::ProtectionDomain::~ProtectionDomain()
{
  int rc = ibv_dealloc_pd(pd);
  assert(rc == 0);
}


Infiniband::MemoryManager::Chunk::Chunk(ibv_mr* m, uint32_t len, char* b)
  : mr(m), bytes(len), offset(0), buffer(b)
{
}

Infiniband::MemoryManager::Chunk::~Chunk()
{
  assert(ibv_dereg_mr(mr) == 0);
}

void Infiniband::MemoryManager::Chunk::set_offset(uint32_t o)
{
  offset = o;
}

uint32_t Infiniband::MemoryManager::Chunk::get_offset()
{
  return offset;
}

void Infiniband::MemoryManager::Chunk::set_bound(uint32_t b)
{
  bound = b;
}

void Infiniband::MemoryManager::Chunk::prepare_read(uint32_t b)
{
  offset = 0;
  bound = b;
}

uint32_t Infiniband::MemoryManager::Chunk::get_bound()
{
  return bound;
}

uint32_t Infiniband::MemoryManager::Chunk::read(char* buf, uint32_t len)
{
  uint32_t left = bound - offset;
  if (left >= len) {
    memcpy(buf, buffer+offset, len);
    offset += len;
    return len;
  } else {
    memcpy(buf, buffer+offset, left);
    offset = 0;
    bound = 0;
    return left;
  }
}

uint32_t Infiniband::MemoryManager::Chunk::write(char* buf, uint32_t len)
{
  uint32_t left = bytes - offset;
  if (left >= len) {
    memcpy(buffer+offset, buf, len);
    offset += len;
    return len;
  } else {
    memcpy(buffer+offset, buf, left);
    offset = bytes;
    return left;
  }
}

bool Infiniband::MemoryManager::Chunk::full()
{
  return offset == bytes;
}

bool Infiniband::MemoryManager::Chunk::over()
{
  return Infiniband::MemoryManager::Chunk::offset == bound;
}

void Infiniband::MemoryManager::Chunk::clear()
{
  offset = 0;
  bound = 0;
}

Infiniband::MemoryManager::Cluster::Cluster(MemoryManager& m, uint32_t s)
  : manager(m), buffer_size(s), lock("cluster_lock")
{
}

Infiniband::MemoryManager::Cluster::~Cluster()
{
  const auto chunk_end = chunk_base + num_chunk;
  for (auto chunk = chunk_base; chunk != chunk_end; chunk++) {
    chunk->~Chunk();
  }

  ::free(chunk_base);
  if (manager.enabled_huge_page)
    manager.free_huge_pages(base);
  else
    ::free(base);
}

int Infiniband::MemoryManager::Cluster::fill(uint32_t num)
{
  assert(!base);
  num_chunk = num;
  uint32_t bytes = buffer_size * num;
  if (manager.enabled_huge_page) {
    base = (char*)manager.malloc_huge_pages(bytes);
  } else {
    base = (char*)memalign(CEPH_PAGE_SIZE, bytes);
  }
  end = base + bytes;
  assert(base);
  chunk_base = static_cast<Chunk*>(::malloc(sizeof(Chunk) * num));
  memset(chunk_base, 0, sizeof(Chunk) * num);
  free_chunks.reserve(num);
  Chunk* chunk = chunk_base;
  for (uint32_t offset = 0; offset < bytes; offset += buffer_size){
    ibv_mr* m = ibv_reg_mr(manager.pd->pd, base+offset, buffer_size, IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
    assert(m);
    new(chunk) Chunk(m, buffer_size, base+offset);
    free_chunks.push_back(chunk);
    chunk++;
  }
  return 0;
}

void Infiniband::MemoryManager::Cluster::take_back(std::vector<Chunk*> &ck)
{
  Mutex::Locker l(lock);
  for (auto c : ck) {
    c->clear();
    free_chunks.push_back(c);
  }
}

int Infiniband::MemoryManager::Cluster::get_buffers(std::vector<Chunk*> &chunks, size_t bytes)
{
  uint32_t num = bytes / buffer_size + 1;
  if (bytes % buffer_size == 0)
    --num;
  int r = num;
  Mutex::Locker l(lock);
  if (free_chunks.empty())
    return 0;
  if (!bytes) {
    r = free_chunks.size();
    for (auto c : free_chunks)
      chunks.push_back(c);
    free_chunks.clear();
    return r;
  }
  if (free_chunks.size() < num) {
    num = free_chunks.size();
    r = num;
  }
  for (uint32_t i = 0; i < num; ++i) {
    chunks.push_back(free_chunks.back());
    free_chunks.pop_back();
  }
  return r;
}


Infiniband::MemoryManager::MemoryManager(Device *d, ProtectionDomain *p, bool hugepage)
  : device(d), pd(p)
{
  enabled_huge_page = hugepage;
}

Infiniband::MemoryManager::~MemoryManager()
{
  if (channel)
    delete channel;
  if (send)
    delete send;
}

void* Infiniband::MemoryManager::malloc_huge_pages(size_t size)
{
  size_t real_size = ALIGN_TO_PAGE_SIZE(size + HUGE_PAGE_SIZE);
  char *ptr = (char *)mmap(NULL, real_size, PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS |MAP_POPULATE | MAP_HUGETLB,-1, 0);
  if (ptr == MAP_FAILED) {
    ptr = (char *)malloc(real_size);
    if (ptr == NULL) return NULL;
    real_size = 0;
  }
  *((size_t *)ptr) = real_size;
  return ptr + HUGE_PAGE_SIZE;
}

void Infiniband::MemoryManager::free_huge_pages(void *ptr)
{
  if (ptr == NULL) return;
  void *real_ptr = (char *)ptr -HUGE_PAGE_SIZE;
  size_t real_size = *((size_t *)real_ptr);
  assert(real_size % HUGE_PAGE_SIZE == 0);
  if (real_size != 0)
    munmap(real_ptr, real_size);
  else
    free(real_ptr);
}

void Infiniband::MemoryManager::register_rx_tx(uint32_t size, uint32_t rx_num, uint32_t tx_num)
{
  assert(device);
  assert(pd);
  channel = new Cluster(*this, size);
  channel->fill(rx_num);

  send = new Cluster(*this, size);
  send->fill(tx_num);
}

void Infiniband::MemoryManager::return_tx(std::vector<Chunk*> &chunks)
{
  send->take_back(chunks);
}

int Infiniband::MemoryManager::get_send_buffers(std::vector<Chunk*> &c, size_t bytes)
{
  return send->get_buffers(c, bytes);
}

int Infiniband::MemoryManager::get_channel_buffers(std::vector<Chunk*> &chunks, size_t bytes)
{
  return channel->get_buffers(chunks, bytes);
}


Infiniband::Infiniband(CephContext *cct)
  : cct(cct), lock("IB lock")
{
}

Infiniband::~Infiniband()
{
  if (dispatcher)
    dispatcher->polling_stop();

  delete device_list;
}

void Infiniband::init()
{
  Mutex::Locker l(lock);

  if (initialized)
    return;

  device_list = new DeviceList(cct, this);
  initialized = true;

  dispatcher->polling_start();
}

void Infiniband::set_dispatcher(RDMADispatcher *d)
{
  assert(!d ^ !dispatcher);

  dispatcher = d;
}

Device* Infiniband::get_device(const char* device_name)
{
  return device_list->get_device(device_name);
}

Device *Infiniband::get_device(const struct ibv_context *ctxt)
{
  return device_list->get_device(ctxt);
}

Infiniband::QueuePair::~QueuePair()
{
  if (qp) {
    ldout(cct, 20) << __func__ << " destroy qp=" << qp << dendl;
    assert(!ibv_destroy_qp(qp));
  }
}

/**
 * Given a string representation of the `status' field from Verbs
 * struct `ibv_wc'.
 *
 * \param[in] status
 *      The integer status obtained in ibv_wc.status.
 * \return
 *      A string corresponding to the given status.
 */
const char* Infiniband::wc_status_to_string(int status)
{
  static const char *lookup[] = {
      "SUCCESS",
      "LOC_LEN_ERR",
      "LOC_QP_OP_ERR",
      "LOC_EEC_OP_ERR",
      "LOC_PROT_ERR",
      "WR_FLUSH_ERR",
      "MW_BIND_ERR",
      "BAD_RESP_ERR",
      "LOC_ACCESS_ERR",
      "REM_INV_REQ_ERR",
      "REM_ACCESS_ERR",
      "REM_OP_ERR",
      "RETRY_EXC_ERR",
      "RNR_RETRY_EXC_ERR",
      "LOC_RDD_VIOL_ERR",
      "REM_INV_RD_REQ_ERR",
      "REM_ABORT_ERR",
      "INV_EECN_ERR",
      "INV_EEC_STATE_ERR",
      "FATAL_ERR",
      "RESP_TIMEOUT_ERR",
      "GENERAL_ERR"
  };

  if (status < IBV_WC_SUCCESS || status > IBV_WC_GENERAL_ERR)
    return "<status out of range!>";
  return lookup[status];
}

const char* Infiniband::qp_state_string(int status) {
  switch(status) {
    case IBV_QPS_RESET : return "IBV_QPS_RESET";
    case IBV_QPS_INIT  : return "IBV_QPS_INIT";
    case IBV_QPS_RTR   : return "IBV_QPS_RTR";
    case IBV_QPS_RTS   : return "IBV_QPS_RTS";
    case IBV_QPS_SQD   : return "IBV_QPS_SQD";
    case IBV_QPS_SQE   : return "IBV_QPS_SQE";
    case IBV_QPS_ERR   : return "IBV_QPS_ERR";
    default: return " out of range.";
  }
}

void Infiniband::handle_pre_fork()
{
  device_list->uninit();
}

int Infiniband::poll_tx(int n, Device **d, ibv_wc *wc)
{
  return device_list->poll_tx(n, d, wc);
}

int Infiniband::poll_rx(int n, Device **d, ibv_wc *wc)
{
  return device_list->poll_rx(n, d, wc);
}

int Infiniband::poll_blocking(bool &done)
{
  return device_list->poll_blocking(done);
}

void Infiniband::rearm_notify()
{
  device_list->rearm_notify();
}

void Infiniband::handle_async_event()
{
  device_list->handle_async_event();
}
