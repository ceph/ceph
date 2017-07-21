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
#include "common/errno.h"
#include "common/debug.h"
#include "RDMAStack.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "Infiniband "

static const uint32_t MAX_SHARED_RX_SGE_COUNT = 1;
static const uint32_t MAX_INLINE_DATA = 0;
static const uint32_t TCP_MSG_LEN = sizeof("0000:00000000:00000000:00000000:00000000000000000000000000000000");
static const uint32_t CQ_DEPTH = 30000;

Port::Port(CephContext *cct, struct ibv_context* ictxt, uint8_t ipn): ctxt(ictxt), port_num(ipn), port_attr(new ibv_port_attr)
{
#ifdef HAVE_IBV_EXP
  union ibv_gid cgid;
  struct ibv_exp_gid_attr gid_attr;
  bool malformed = false;

  ldout(cct,1) << __func__ << " using experimental verbs for gid" << dendl;
  int r = ibv_query_port(ctxt, port_num, port_attr);
  if (r == -1) {
    lderr(cct) << __func__  << " query port failed  " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }

  lid = port_attr->lid;

  // search for requested GID in GIDs table
  ldout(cct, 1) << __func__ << " looking for local GID " << (cct->_conf->ms_async_rdma_local_gid)
    << " of type " << (cct->_conf->ms_async_rdma_roce_ver) << dendl;
  r = sscanf(cct->_conf->ms_async_rdma_local_gid.c_str(),
	     "%02hhx%02hhx:%02hhx%02hhx:%02hhx%02hhx:%02hhx%02hhx"
	     ":%02hhx%02hhx:%02hhx%02hhx:%02hhx%02hhx:%02hhx%02hhx",
	     &cgid.raw[ 0], &cgid.raw[ 1],
	     &cgid.raw[ 2], &cgid.raw[ 3],
	     &cgid.raw[ 4], &cgid.raw[ 5],
	     &cgid.raw[ 6], &cgid.raw[ 7],
	     &cgid.raw[ 8], &cgid.raw[ 9],
	     &cgid.raw[10], &cgid.raw[11],
	     &cgid.raw[12], &cgid.raw[13],
	     &cgid.raw[14], &cgid.raw[15]);

  if (r != 16) {
    ldout(cct, 1) << __func__ << " malformed or no GID supplied, using GID index 0" << dendl;
    malformed = true;
  }

  gid_attr.comp_mask = IBV_EXP_QUERY_GID_ATTR_TYPE;

  for (gid_idx = 0; gid_idx < port_attr->gid_tbl_len; gid_idx++) {
    r = ibv_query_gid(ctxt, port_num, gid_idx, &gid);
    if (r) {
      lderr(cct) << __func__  << " query gid of port " << port_num << " index " << gid_idx << " failed  " << cpp_strerror(errno) << dendl;
      ceph_abort();
    }
    r = ibv_exp_query_gid_attr(ctxt, port_num, gid_idx, &gid_attr);
    if (r) {
      lderr(cct) << __func__  << " query gid attributes of port " << port_num << " index " << gid_idx << " failed  " << cpp_strerror(errno) << dendl;
      ceph_abort();
    }

    if (malformed) break; // stay with gid_idx=0
    if ( (gid_attr.type == cct->_conf->ms_async_rdma_roce_ver) &&
	 (memcmp(&gid, &cgid, 16) == 0) ) {
      ldout(cct, 1) << __func__ << " found at index " << gid_idx << dendl;
      break;
    }
  }

  if (gid_idx == port_attr->gid_tbl_len) {
    lderr(cct) << __func__ << " Requested local GID was not found in GID table" << dendl;
    ceph_abort();
  }
#else
  int r = ibv_query_port(ctxt, port_num, port_attr);
  if (r == -1) {
    lderr(cct) << __func__  << " query port failed  " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }

  lid = port_attr->lid;
  r = ibv_query_gid(ctxt, port_num, 0, &gid);
  if (r) {
    lderr(cct) << __func__  << " query gid failed  " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
#endif
}


Device::Device(CephContext *cct, ibv_device* d): device(d), device_attr(new ibv_device_attr), active_port(nullptr)
{
  if (device == NULL) {
    lderr(cct) << __func__ << " device == NULL" << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
  name = ibv_get_device_name(device);
  ctxt = ibv_open_device(device);
  if (ctxt == NULL) {
    lderr(cct) << __func__ << " open rdma device failed. " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
  int r = ibv_query_device(ctxt, device_attr);
  if (r == -1) {
    lderr(cct) << __func__ << " failed to query rdma device. " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
}

void Device::binding_port(CephContext *cct, int port_num) {
  port_cnt = device_attr->phys_port_cnt;
  for (uint8_t i = 0; i < port_cnt; ++i) {
    Port *port = new Port(cct, ctxt, i+1);
    if (i + 1 == port_num && port->get_port_attr()->state == IBV_PORT_ACTIVE) {
      active_port = port;
      ldout(cct, 1) << __func__ << " found active port " << i+1 << dendl;
      break;
    } else {
      ldout(cct, 10) << __func__ << " port " << i+1 << " is not what we want. state: " << port->get_port_attr()->state << ")"<< dendl;
    }
    delete port;
  }
  if (nullptr == active_port) {
    lderr(cct) << __func__ << "  port not found" << dendl;
    assert(active_port);
  }
}


Infiniband::QueuePair::QueuePair(
    CephContext *c, Infiniband& infiniband, ibv_qp_type type,
    int port, ibv_srq *srq,
    Infiniband::CompletionQueue* txcq, Infiniband::CompletionQueue* rxcq,
    uint32_t max_send_wr, uint32_t max_recv_wr, uint32_t q_key)
: cct(c), infiniband(infiniband),
  type(type),
  ctxt(infiniband.device->ctxt),
  ib_physical_port(port),
  pd(infiniband.pd->pd),
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
  pd = infiniband.pd->pd;
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


Infiniband::CompletionChannel::CompletionChannel(CephContext *c, Infiniband &ib)
  : cct(c), infiniband(ib), channel(NULL), cq(NULL), cq_events_that_need_ack(0)
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
  channel = ibv_create_comp_channel(infiniband.device->ctxt);
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
  cq = ibv_create_cq(infiniband.device->ctxt, queue_depth, this, channel->get_channel(), 0);
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
  ibv_dealloc_pd(pd);
}


Infiniband::MemoryManager::Chunk::Chunk(ibv_mr* m, uint32_t len, char* b)
  : mr(m), bytes(len), offset(0), buffer(b)
{
}

Infiniband::MemoryManager::Chunk::~Chunk()
{
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

void Infiniband::MemoryManager::Chunk::post_srq(Infiniband *ib)
{
  ib->post_chunk(this);
}

Infiniband::MemoryManager::Cluster::Cluster(MemoryManager& m, uint32_t s)
  : manager(m), buffer_size(s), lock("cluster_lock")
{
}

Infiniband::MemoryManager::Cluster::~Cluster()
{
  int r = ibv_dereg_mr(chunk_base->mr);
  assert(r == 0);
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
  ibv_mr* m = ibv_reg_mr(manager.pd->pd, base, bytes, IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
  assert(m);
  Chunk* chunk = chunk_base;
  for (uint32_t offset = 0; offset < bytes; offset += buffer_size){
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


Infiniband::Infiniband(CephContext *cct, const std::string &device_name, uint8_t port_num)
  : cct(cct), lock("IB lock"), device_name(device_name), port_num(port_num)
{
}

void Infiniband::init()
{
  Mutex::Locker l(lock);

  if (initialized)
    return;

  device_list = new DeviceList(cct);
  initialized = true;

  device = device_list->get_device(device_name.c_str());
  device->binding_port(cct, port_num);
  assert(device);
  ib_physical_port = device->active_port->get_port_num();
  pd = new ProtectionDomain(cct, device);
  assert(NetHandler(cct).set_nonblock(device->ctxt->async_fd) == 0);

  max_recv_wr = device->device_attr->max_srq_wr;
  if (max_recv_wr > cct->_conf->ms_async_rdma_receive_buffers) {
    max_recv_wr = cct->_conf->ms_async_rdma_receive_buffers;
    ldout(cct, 1) << __func__ << " assigning: " << max_recv_wr << " receive buffers" << dendl;
  } else {
    ldout(cct, 1) << __func__ << " using the max allowed receive buffers: " << max_recv_wr << dendl;
  }

  max_send_wr = device->device_attr->max_qp_wr;
  if (max_send_wr > cct->_conf->ms_async_rdma_send_buffers) {
    max_send_wr = cct->_conf->ms_async_rdma_send_buffers;
    ldout(cct, 1) << __func__ << " assigning: " << max_send_wr << " send buffers"  << dendl;
  } else {
    ldout(cct, 1) << __func__ << " using the max allowed send buffers: " << max_send_wr << dendl;
  }

  ldout(cct, 1) << __func__ << " device allow " << device->device_attr->max_cqe
                << " completion entries" << dendl;

  memory_manager = new MemoryManager(device, pd,
                                     cct->_conf->ms_async_rdma_enable_hugepage);
  memory_manager->register_rx_tx(
      cct->_conf->ms_async_rdma_buffer_size, max_recv_wr, max_send_wr);

  srq = create_shared_receive_queue(max_recv_wr, MAX_SHARED_RX_SGE_COUNT);
  post_channel_cluster();

  dispatcher->polling_start();
}

Infiniband::~Infiniband()
{
  if (!initialized)
    return;

  if (dispatcher)
    dispatcher->polling_stop();

  ibv_destroy_srq(srq);
  delete memory_manager;
  delete pd;
}

void Infiniband::set_dispatcher(RDMADispatcher *d)
{
  assert(!d ^ !dispatcher);

  dispatcher = d;
}

/**
 * Create a shared receive queue. This basically wraps the verbs call. 
 *
 * \param[in] max_wr
 *      The max number of outstanding work requests in the SRQ.
 * \param[in] max_sge
 *      The max number of scatter elements per WR.
 * \return
 *      A valid ibv_srq pointer, or NULL on error.
 */
ibv_srq* Infiniband::create_shared_receive_queue(uint32_t max_wr, uint32_t max_sge)
{
  ibv_srq_init_attr sia;
  memset(&sia, 0, sizeof(sia));
  sia.srq_context = device->ctxt;
  sia.attr.max_wr = max_wr;
  sia.attr.max_sge = max_sge;
  return ibv_create_srq(pd->pd, &sia);
}

int Infiniband::get_tx_buffers(std::vector<Chunk*> &c, size_t bytes)
{
  return memory_manager->get_send_buffers(c, bytes);
}

/**
 * Create a new QueuePair. This factory should be used in preference to
 * the QueuePair constructor directly, since this lets derivatives of
 * Infiniband, e.g. MockInfiniband (if it existed),
 * return mocked out QueuePair derivatives.
 *
 * \return
 *      QueuePair on success or NULL if init fails
 * See QueuePair::QueuePair for parameter documentation.
 */
Infiniband::QueuePair* Infiniband::create_queue_pair(CephContext *cct, CompletionQueue *tx, CompletionQueue* rx, ibv_qp_type type)
{
  Infiniband::QueuePair *qp = new QueuePair(
      cct, *this, type, ib_physical_port, srq, tx, rx, max_send_wr, max_recv_wr);
  if (qp->init()) {
    delete qp;
    return NULL;
  }
  return qp;
}

int Infiniband::post_chunk(Chunk* chunk)
{
  ibv_sge isge;
  isge.addr = reinterpret_cast<uint64_t>(chunk->buffer);
  isge.length = chunk->bytes;
  isge.lkey = chunk->mr->lkey;
  ibv_recv_wr rx_work_request;

  memset(&rx_work_request, 0, sizeof(rx_work_request));
  rx_work_request.wr_id = reinterpret_cast<uint64_t>(chunk);// stash descriptor ptr
  rx_work_request.next = NULL;
  rx_work_request.sg_list = &isge;
  rx_work_request.num_sge = 1;

  ibv_recv_wr *badWorkRequest;
  int ret = ibv_post_srq_recv(srq, &rx_work_request, &badWorkRequest);
  if (ret)
    return -errno;
  return 0;
}

int Infiniband::post_channel_cluster()
{
  vector<Chunk*> free_chunks;
  int r = memory_manager->get_channel_buffers(free_chunks, 0);
  assert(r > 0);
  for (vector<Chunk*>::iterator iter = free_chunks.begin(); iter != free_chunks.end(); ++iter) {
    r = post_chunk(*iter);
    assert(r == 0);
  }
  return 0;
}

Infiniband::CompletionChannel* Infiniband::create_comp_channel(CephContext *c)
{
  Infiniband::CompletionChannel *cc = new Infiniband::CompletionChannel(c, *this);
  if (cc->init()) {
    delete cc;
    return NULL;
  }
  return cc;
}

Infiniband::CompletionQueue* Infiniband::create_comp_queue(
    CephContext *cct, CompletionChannel *cc)
{
  Infiniband::CompletionQueue *cq = new Infiniband::CompletionQueue(
      cct, *this, CQ_DEPTH, cc);
  if (cq->init()) {
    delete cq;
    return NULL;
  }
  return cq;
}

// 1 means no valid buffer read, 0 means got enough buffer
// else return < 0 means error
int Infiniband::recv_msg(CephContext *cct, int sd, IBSYNMsg& im)
{
  char msg[TCP_MSG_LEN];
  char gid[33];
  ssize_t r = ::read(sd, &msg, sizeof(msg));
  // Drop incoming qpt
  if (cct->_conf->ms_inject_socket_failures && sd >= 0) {
    if (rand() % cct->_conf->ms_inject_socket_failures == 0) {
      ldout(cct, 0) << __func__ << " injecting socket failure" << dendl;
      return -EINVAL;
    }
  }
  if (r < 0) {
    r = -errno;
    lderr(cct) << __func__ << " got error " << r << ": "
               << cpp_strerror(r) << dendl;
  } else if (r == 0) { // valid disconnect message of length 0
    ldout(cct, 10) << __func__ << " got disconnect message " << dendl;
  } else if ((size_t)r != sizeof(msg)) { // invalid message
    ldout(cct, 1) << __func__ << " got bad length (" << r << ") " << dendl;
    r = -EINVAL;
  } else { // valid message
    sscanf(msg, "%hu:%x:%x:%x:%s", &(im.lid), &(im.qpn), &(im.psn), &(im.peer_qpn),gid);
    wire_gid_to_gid(gid, &(im.gid));
    ldout(cct, 5) << __func__ << " recevd: " << im.lid << ", " << im.qpn << ", " << im.psn << ", " << im.peer_qpn << ", " << gid  << dendl;
  }
  return r;
}

int Infiniband::send_msg(CephContext *cct, int sd, IBSYNMsg& im)
{
  int retry = 0;
  ssize_t r;

  char msg[TCP_MSG_LEN];
  char gid[33];
retry:
  gid_to_wire_gid(&(im.gid), gid);
  sprintf(msg, "%04x:%08x:%08x:%08x:%s", im.lid, im.qpn, im.psn, im.peer_qpn, gid);
  ldout(cct, 10) << __func__ << " sending: " << im.lid << ", " << im.qpn << ", " << im.psn
                 << ", " << im.peer_qpn << ", "  << gid  << dendl;
  r = ::write(sd, msg, sizeof(msg));
  // Drop incoming qpt
  if (cct->_conf->ms_inject_socket_failures && sd >= 0) {
    if (rand() % cct->_conf->ms_inject_socket_failures == 0) {
      ldout(cct, 0) << __func__ << " injecting socket failure" << dendl;
      return -EINVAL;
    }
  }

  if ((size_t)r != sizeof(msg)) {
    // FIXME need to handle EAGAIN instead of retry
    if (r < 0 && (errno == EINTR || errno == EAGAIN) && retry < 3) {
      retry++;
      goto retry;
    }
    if (r < 0)
      lderr(cct) << __func__ << " send returned error " << errno << ": "
                 << cpp_strerror(errno) << dendl;
    else
      lderr(cct) << __func__ << " send got bad length (" << r << ") " << cpp_strerror(errno) << dendl;
    return -errno;
  }
  return 0;
}

void Infiniband::wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
  char tmp[9];
  uint32_t v32;
  int i;

  for (tmp[8] = 0, i = 0; i < 4; ++i) {
    memcpy(tmp, wgid + i * 8, 8);
    sscanf(tmp, "%x", &v32);
    *(uint32_t *)(&gid->raw[i * 4]) = ntohl(v32);
  }
}

void Infiniband::gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
  for (int i = 0; i < 4; ++i)
    sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *)(gid->raw + i * 4)));
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
