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
#include "RDMAStack.h"

class C_handle_connection_established : public EventCallback {
  RDMAConnectedSocketImpl *csi;
  bool active = true;
 public:
  C_handle_connection_established(RDMAConnectedSocketImpl *w) : csi(w) {}
  void do_request(uint64_t fd) final {
    if (active)
      csi->handle_connection_established();
  }
  void close() {
    active = false;
  }
};

class C_handle_connection_read : public EventCallback {
  RDMAConnectedSocketImpl *csi;
  bool active = true;
 public:
  explicit C_handle_connection_read(RDMAConnectedSocketImpl *w): csi(w) {}
  void do_request(uint64_t fd) final {
    if (active)
      csi->handle_connection();
  }
  void close() {
    active = false;
  }
};

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << " RDMAConnectedSocketImpl "

RDMAConnectedSocketImpl::RDMAConnectedSocketImpl(CephContext *cct, std::shared_ptr<Infiniband> &ib,
                                                 std::shared_ptr<RDMADispatcher>& rdma_dispatcher,
                                                 RDMAWorker *w)
  : cct(cct), connected(0), error(0), ib(ib),
    dispatcher(rdma_dispatcher), worker(w),
    is_server(false), read_handler(new C_handle_connection_read(this)),
    established_handler(new C_handle_connection_established(this)),
    active(false), pending(false)
{
  if (!cct->_conf->ms_async_rdma_cm) {
    qp = ib->create_queue_pair(cct, dispatcher->get_tx_cq(), dispatcher->get_rx_cq(), IBV_QPT_RC, NULL);
    if (!qp) {
      lderr(cct) << __func__ << " queue pair create failed" << dendl;
      return;
    }
    local_qpn = qp->get_local_qp_number();
    notify_fd = eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK);
    dispatcher->register_qp(qp, this);
    dispatcher->perf_logger->inc(l_msgr_rdma_created_queue_pair);
    dispatcher->perf_logger->inc(l_msgr_rdma_active_queue_pair);
  }
}

RDMAConnectedSocketImpl::~RDMAConnectedSocketImpl()
{
  ldout(cct, 20) << __func__ << " destruct." << dendl;
  cleanup();
  //create qp failed, pending_conn not exist, so skip.
  if(qp) {
    worker->remove_pending_conn(this);
  }
  dispatcher->schedule_qp_destroy(local_qpn);

  for (unsigned i=0; i < wc.size(); ++i) {
    dispatcher->post_chunk_to_pool(reinterpret_cast<Chunk*>(wc[i].wr_id));
  }
  for (unsigned i=0; i < buffers.size(); ++i) {
    dispatcher->post_chunk_to_pool(buffers[i]);
  }

  std::lock_guard l{lock};
  if (notify_fd >= 0)
    ::close(notify_fd);
  if (tcp_fd >= 0)
    ::close(tcp_fd);
  error = ECONNRESET;
}

void RDMAConnectedSocketImpl::pass_wc(std::vector<ibv_wc> &&v)
{
  std::lock_guard l{lock};
  if (wc.empty())
    wc = std::move(v);
  else
    wc.insert(wc.end(), v.begin(), v.end());
  notify();
}

void RDMAConnectedSocketImpl::get_wc(std::vector<ibv_wc> &w)
{
  std::lock_guard l{lock};
  if (wc.empty())
    return ;
  w.swap(wc);
}

int RDMAConnectedSocketImpl::activate()
{
  qp->get_local_cm_meta().peer_qpn = qp->get_peer_cm_meta().local_qpn;
  if (qp->modify_qp_to_rtr() != 0)
    return -1;

  if (qp->modify_qp_to_rts() != 0)
    return -1;

  if (!is_server) {
    connected = 1; //indicate successfully
    ldout(cct, 20) << __func__ << " handle fake send, wake it up. QP: " << local_qpn << dendl;
    submit(false);
  }
  active = true;
  peer_qpn = qp->get_local_cm_meta().peer_qpn;

  return 0;
}

int RDMAConnectedSocketImpl::try_connect(const entity_addr_t& peer_addr, const SocketOptions &opts) {
  ldout(cct, 20) << __func__ << " nonblock:" << opts.nonblock << ", nodelay:"
                 << opts.nodelay << ", rbuf_size: " << opts.rcbuf_size << dendl;
  ceph::NetHandler net(cct);

  // we construct a socket to transport ib sync message
  // but we shouldn't block in tcp connecting
  if (opts.nonblock) {
    tcp_fd = net.nonblock_connect(peer_addr, opts.connect_bind_addr);
  } else {
    tcp_fd = net.connect(peer_addr, opts.connect_bind_addr);
  }

  if (tcp_fd < 0) {
    return -errno;
  }

  int r = net.set_socket_options(tcp_fd, opts.nodelay, opts.rcbuf_size);
  if (r < 0) {
    ::close(tcp_fd);
    tcp_fd = -1;
    return -errno;
  }

  ldout(cct, 20) << __func__ << " tcp_fd: " << tcp_fd << dendl;
  net.set_priority(tcp_fd, opts.priority, peer_addr.get_family());
  r = 0;
  if (opts.nonblock) {
    worker->center.create_file_event(tcp_fd, EVENT_READABLE | EVENT_WRITABLE , established_handler);
  } else {
    r = handle_connection_established(false);
  }
  return r;
}

int RDMAConnectedSocketImpl::handle_connection_established(bool need_set_fault) {
  ldout(cct, 20) << __func__ << " start " << dendl;
  // delete read event
  worker->center.delete_file_event(tcp_fd, EVENT_READABLE | EVENT_WRITABLE);
  if (1 == connected) {
    ldout(cct, 1) << __func__ << " warnning: logic failed " << dendl;
    if (need_set_fault) {
      fault();
    }
    return -1;
  }
  // send handshake msg to server
  qp->get_local_cm_meta().peer_qpn = 0;
  int r = qp->send_cm_meta(cct, tcp_fd);
  if (r < 0) {
    ldout(cct, 1) << __func__ << " send handshake msg failed." << r << dendl;
    if (need_set_fault) {
      fault();
    }
    return r;
  }
  worker->center.create_file_event(tcp_fd, EVENT_READABLE, read_handler);
  ldout(cct, 20) << __func__ << " finish " << dendl;
  return 0;
}

void RDMAConnectedSocketImpl::handle_connection() {
  ldout(cct, 20) << __func__ << " QP: " << local_qpn << " tcp_fd: " << tcp_fd << " notify_fd: " << notify_fd << dendl;
  int r = qp->recv_cm_meta(cct, tcp_fd);
  if (r <= 0) {
    if (r != -EAGAIN) {
      dispatcher->perf_logger->inc(l_msgr_rdma_handshake_errors);
      ldout(cct, 1) << __func__ << " recv handshake msg failed." << dendl;
      fault();
    }
    return;
  }

  if (1 == connected) {
    ldout(cct, 1) << __func__ << " warnning: logic failed: read len: " << r << dendl;
    fault();
    return;
  }

  if (!is_server) {// first time: cm meta sync + ack from server
    if (!connected) {
      r = activate();
      ceph_assert(!r);
    }
    notify();
    r = qp->send_cm_meta(cct, tcp_fd);
    if (r < 0) {
      ldout(cct, 1) << __func__ << " send client ack failed." << dendl;
      dispatcher->perf_logger->inc(l_msgr_rdma_handshake_errors);
      fault();
    }
  } else {
    if (qp->get_peer_cm_meta().peer_qpn == 0) {// first time: cm meta sync from client
      if (active) {
        ldout(cct, 10) << __func__ << " server is already active." << dendl;
        return ;
      }
      r = activate();
      ceph_assert(!r);
      r = qp->send_cm_meta(cct, tcp_fd);
      if (r < 0) {
        ldout(cct, 1) << __func__ << " server ack failed." << dendl;
        dispatcher->perf_logger->inc(l_msgr_rdma_handshake_errors);
        fault();
        return ;
      }
    } else { // second time: cm meta ack from client
      connected = 1;
      ldout(cct, 10) << __func__ << " handshake of rdma is done. server connected: " << connected << dendl;
      //cleanup();
      submit(false);
      notify();
    }
  }
}

ssize_t RDMAConnectedSocketImpl::read(char* buf, size_t len)
{
  eventfd_t event_val = 0;
  int r = eventfd_read(notify_fd, &event_val);
  ldout(cct, 20) << __func__ << " notify_fd : " << event_val << " in " << local_qpn
                 << " r = " << r << dendl;

  if (!active) {
    ldout(cct, 1) << __func__ << " when ib not active. len: " << len << dendl;
    return -EAGAIN;
  }

  if (0 == connected) {
    ldout(cct, 1) << __func__ << " when ib not connected. len: " << len <<dendl;
    return -EAGAIN;
  }
  ssize_t read = 0;
  read = read_buffers(buf,len);

  if (is_server && connected == 0) {
    ldout(cct, 20) << __func__ << " we do not need last handshake, QP: " << local_qpn << " peer QP: " << peer_qpn << dendl;
    connected = 1; //if so, we don't need the last handshake
    cleanup();
    submit(false);
  }

  if (!buffers.empty()) {
    notify();
  }

  if (read == 0 && error)
    return -error;
  return read == 0 ? -EAGAIN : read;
}

void RDMAConnectedSocketImpl::buffer_prefetch(void)
{
  std::vector<ibv_wc> cqe;
  get_wc(cqe);
  if(cqe.empty())
    return;

  for(size_t i = 0; i < cqe.size(); ++i) {
    ibv_wc* response = &cqe[i];
    ceph_assert(response->status == IBV_WC_SUCCESS);
    Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);
    chunk->prepare_read(response->byte_len);

    if (chunk->get_size() == 0) {
      chunk->reset_read_chunk();
      dispatcher->perf_logger->inc(l_msgr_rdma_rx_fin);
      if (connected) {
        error = ECONNRESET;
        ldout(cct, 20) << __func__ << " got remote close msg..." << dendl;
      }
      dispatcher->post_chunk_to_pool(chunk);
      continue;
    } else {
      buffers.push_back(chunk);
      ldout(cct, 25) << __func__ << " buffers add a chunk: " << chunk->get_offset() << ":" << chunk->get_bound() << dendl;
    }
  }
  worker->perf_logger->inc(l_msgr_rdma_rx_chunks, cqe.size());
}

ssize_t RDMAConnectedSocketImpl::read_buffers(char* buf, size_t len)
{
  size_t read_size = 0, tmp = 0;
  buffer_prefetch();
  auto pchunk = buffers.begin();
  while (pchunk != buffers.end()) {
    tmp = (*pchunk)->read(buf + read_size, len - read_size);
    read_size += tmp;
    ldout(cct, 25) << __func__ << " read chunk " << *pchunk << " bytes length" << tmp << " offset: "
                   << (*pchunk)->get_offset() << " ,bound: " << (*pchunk)->get_bound() << dendl;

    if ((*pchunk)->get_size() == 0) {
      (*pchunk)->reset_read_chunk();
      dispatcher->post_chunk_to_pool(*pchunk);
      update_post_backlog();
      ldout(cct, 25) << __func__ << " read over one chunk " << dendl;
      pchunk++;
    }

    if (read_size == len) {
      break;
    }
  }

  buffers.erase(buffers.begin(), pchunk);
  ldout(cct, 25) << __func__ << " got " << read_size  << " bytes, buffers size: " << buffers.size() << dendl;
  worker->perf_logger->inc(l_msgr_rdma_rx_bytes, read_size);
  return read_size;
}

ssize_t RDMAConnectedSocketImpl::send(ceph::buffer::list &bl, bool more)
{
  if (error) {
    if (!active)
      return -EPIPE;
    return -error;
  }
  size_t bytes = bl.length();
  if (!bytes)
    return 0;
  {
    std::lock_guard l{lock};
    pending_bl.claim_append(bl);
    if (!connected) {
      ldout(cct, 20) << __func__ << " fake send to upper, QP: " << local_qpn << dendl;
      return bytes;
    }
  }
  ldout(cct, 20) << __func__ << " QP: " << local_qpn << dendl;
  ssize_t r = submit(more);
  if (r < 0 && r != -EAGAIN)
    return r;
  return bytes;
}

size_t RDMAConnectedSocketImpl::tx_copy_chunk(std::vector<Chunk*> &tx_buffers,
    size_t req_copy_len, decltype(std::cbegin(pending_bl.buffers()))& start,
    const decltype(std::cbegin(pending_bl.buffers()))& end)
{
  ceph_assert(start != end);
  auto chunk_idx = tx_buffers.size();
  if (0 == worker->get_reged_mem(this, tx_buffers, req_copy_len)) {
    ldout(cct, 1) << __func__ << " no enough buffers in worker " << worker << dendl;
    worker->perf_logger->inc(l_msgr_rdma_tx_no_mem);
    return 0;
  }

  Chunk *current_chunk = tx_buffers[chunk_idx];
  size_t write_len = 0;
  while (start != end) {
    const uintptr_t addr = reinterpret_cast<uintptr_t>(start->c_str());

    size_t slice_write_len = 0;
    while (slice_write_len < start->length()) {
      size_t real_len = current_chunk->write((char*)addr + slice_write_len, start->length() - slice_write_len);

      slice_write_len += real_len;
      write_len += real_len;
      req_copy_len -= real_len;

      if (current_chunk->full()) {
        if (++chunk_idx == tx_buffers.size())
          return write_len;
        current_chunk = tx_buffers[chunk_idx];
      }
    }

    ++start;
  }
  ceph_assert(req_copy_len == 0);
  return write_len;
}

ssize_t RDMAConnectedSocketImpl::submit(bool more)
{
  if (error)
    return -error;
  std::lock_guard l{lock};
  size_t bytes = pending_bl.length();
  ldout(cct, 20) << __func__ << " we need " << bytes << " bytes. iov size: "
                 << pending_bl.get_num_buffers() << dendl;
  if (!bytes)
    return 0;

  std::vector<Chunk*> tx_buffers;
  auto it = std::cbegin(pending_bl.buffers());
  auto copy_start = it;
  size_t total_copied = 0, wait_copy_len = 0;
  while (it != pending_bl.buffers().end()) {
    if (ib->is_tx_buffer(it->raw_c_str())) {
      if (wait_copy_len) {
        size_t copied = tx_copy_chunk(tx_buffers, wait_copy_len, copy_start, it);
        total_copied += copied;
        if (copied < wait_copy_len)
          goto sending;
        wait_copy_len = 0;
      }
      ceph_assert(copy_start == it);
      tx_buffers.push_back(ib->get_tx_chunk_by_buffer(it->raw_c_str()));
      total_copied += it->length();
      ++copy_start;
    } else {
      wait_copy_len += it->length();
    }
    ++it;
  }
  if (wait_copy_len)
    total_copied += tx_copy_chunk(tx_buffers, wait_copy_len, copy_start, it);

 sending:
  if (total_copied == 0)
    return -EAGAIN;
  ceph_assert(total_copied <= pending_bl.length());
  ceph::buffer::list swapped;
  if (total_copied < pending_bl.length()) {
    worker->perf_logger->inc(l_msgr_rdma_tx_parital_mem);
    pending_bl.splice(total_copied, pending_bl.length() - total_copied, &swapped);
    pending_bl.swap(swapped);
  } else {
    pending_bl.clear();
  }

  ldout(cct, 20) << __func__ << " left bytes: " << pending_bl.length() << " in buffers "
                 << pending_bl.get_num_buffers() << " tx chunks " << tx_buffers.size() << dendl;

  int r = post_work_request(tx_buffers);
  if (r < 0)
    return r;

  ldout(cct, 20) << __func__ << " finished sending " << total_copied << " bytes." << dendl;
  return pending_bl.length() ? -EAGAIN : 0;
}

int RDMAConnectedSocketImpl::post_work_request(std::vector<Chunk*> &tx_buffers)
{
  ldout(cct, 20) << __func__ << " QP: " << local_qpn << " " << tx_buffers[0] << dendl;
  auto current_buffer = tx_buffers.begin();
  ibv_sge isge[tx_buffers.size()];
  uint32_t current_sge = 0;
  ibv_send_wr iswr[tx_buffers.size()];
  uint32_t current_swr = 0;
  ibv_send_wr* pre_wr = NULL;
  uint32_t num = 0; 

  // FIPS zeroization audit 20191115: these memsets are not security related.
  memset(iswr, 0, sizeof(iswr));
  memset(isge, 0, sizeof(isge));
 
  while (current_buffer != tx_buffers.end()) {
    isge[current_sge].addr = reinterpret_cast<uint64_t>((*current_buffer)->buffer);
    isge[current_sge].length = (*current_buffer)->get_offset();
    isge[current_sge].lkey = (*current_buffer)->mr->lkey;
    ldout(cct, 25) << __func__ << " sending buffer: " << *current_buffer << " length: " << isge[current_sge].length  << dendl;

    iswr[current_swr].wr_id = reinterpret_cast<uint64_t>(*current_buffer);
    iswr[current_swr].next = NULL;
    iswr[current_swr].sg_list = &isge[current_sge];
    iswr[current_swr].num_sge = 1;
    iswr[current_swr].opcode = IBV_WR_SEND;
    iswr[current_swr].send_flags = IBV_SEND_SIGNALED;

    num++;
    worker->perf_logger->inc(l_msgr_rdma_tx_bytes, isge[current_sge].length);
    if (pre_wr)
      pre_wr->next = &iswr[current_swr];
    pre_wr = &iswr[current_swr];
    ++current_sge;
    ++current_swr;
    ++current_buffer;
  }

  ibv_send_wr *bad_tx_work_request = nullptr;
  if (ibv_post_send(qp->get_qp(), iswr, &bad_tx_work_request)) {
    ldout(cct, 1) << __func__ << " failed to send data"
                  << " (most probably should be peer not ready): "
                  << cpp_strerror(errno) << dendl;
    worker->perf_logger->inc(l_msgr_rdma_tx_failed);
    return -errno;
  }
  worker->perf_logger->inc(l_msgr_rdma_tx_chunks, tx_buffers.size());
  ldout(cct, 20) << __func__ << " qp state is " << get_qp_state() << dendl;
  return 0;
}

void RDMAConnectedSocketImpl::fin() {
  ibv_send_wr wr;
  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(&wr, 0, sizeof(wr));

  wr.wr_id = reinterpret_cast<uint64_t>(qp);
  wr.num_sge = 0;
  wr.opcode = IBV_WR_SEND;
  wr.send_flags = IBV_SEND_SIGNALED;
  ibv_send_wr* bad_tx_work_request = nullptr;
  if (ibv_post_send(qp->get_qp(), &wr, &bad_tx_work_request)) {
    ldout(cct, 1) << __func__ << " failed to send message="
                  << " ibv_post_send failed(most probably should be peer not ready): "
                  << cpp_strerror(errno) << dendl;
    worker->perf_logger->inc(l_msgr_rdma_tx_failed);
    return ;
  }
}

void RDMAConnectedSocketImpl::cleanup() {
  if (read_handler && tcp_fd >= 0) {
    (static_cast<C_handle_connection_read*>(read_handler))->close();
    worker->center.submit_to(worker->center.get_id(), [this]() {
      worker->center.delete_file_event(tcp_fd, EVENT_READABLE | EVENT_WRITABLE);
    }, false);
    delete read_handler;
    read_handler = nullptr;
  }
  if (established_handler) {
    (static_cast<C_handle_connection_established*>(established_handler))->close();
    delete established_handler;
    established_handler = nullptr;
  }
}

void RDMAConnectedSocketImpl::notify()
{
  eventfd_t event_val = 1;
  int r = eventfd_write(notify_fd, event_val);
  ceph_assert(r == 0);
}

void RDMAConnectedSocketImpl::shutdown()
{
  if (!error)
    fin();
  error = ECONNRESET;
  active = false;
}

void RDMAConnectedSocketImpl::close()
{
  if (!error)
    fin();
  error = ECONNRESET;
  active = false;
}

void RDMAConnectedSocketImpl::set_priority(int sd, int prio, int domain) {
    ceph::NetHandler net(cct);
    net.set_priority(sd, prio, domain);
}

void RDMAConnectedSocketImpl::fault()
{
  ldout(cct, 1) << __func__ << " tcp fd " << tcp_fd << dendl;
  error = ECONNRESET;
  connected = 1;
  notify();
}

void RDMAConnectedSocketImpl::set_accept_fd(int sd)
{
  tcp_fd = sd;
  is_server = true;
  worker->center.submit_to(worker->center.get_id(), [this]() {
			   worker->center.create_file_event(tcp_fd, EVENT_READABLE, read_handler);
			   }, true);
}

void RDMAConnectedSocketImpl::post_chunks_to_rq(int num)
{
  post_backlog += num - ib->post_chunks_to_rq(num, qp);
}

void RDMAConnectedSocketImpl::update_post_backlog()
{
  if (post_backlog)
    post_backlog -= post_backlog - dispatcher->post_chunks_to_rq(post_backlog, qp);
}
