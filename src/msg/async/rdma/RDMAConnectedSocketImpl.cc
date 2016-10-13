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

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << " RDMAConnectedSocketImpl "

int RDMAConnectedSocketImpl::activate()
{
  ibv_qp_attr qpa;
  int r;

  // now connect up the qps and switch to RTR
  memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_RTR;
  qpa.path_mtu = IBV_MTU_1024;
  qpa.dest_qp_num = peer_msg.qpn;
  qpa.rq_psn = peer_msg.psn;
  qpa.max_dest_rd_atomic = 1;
  qpa.min_rnr_timer = 12;
  //qpa.ah_attr.is_global = 0;
  qpa.ah_attr.is_global = 1;
  qpa.ah_attr.grh.hop_limit = 6;
  qpa.ah_attr.grh.dgid = peer_msg.gid;
  qpa.ah_attr.grh.sgid_index = 0;

  qpa.ah_attr.dlid = peer_msg.lid;
  qpa.ah_attr.sl = 0;
  qpa.ah_attr.src_path_bits = 0;
  qpa.ah_attr.port_num = (uint8_t)(infiniband->get_ib_physical_port());

  r = ibv_modify_qp(qp->get_qp(), &qpa, IBV_QP_STATE |
      IBV_QP_AV |
      IBV_QP_PATH_MTU |
      IBV_QP_DEST_QPN |
      IBV_QP_RQ_PSN |
      IBV_QP_MIN_RNR_TIMER |
      IBV_QP_MAX_DEST_RD_ATOMIC);
  if (r) {
    lderr(cct) << __func__ << " failed to transition to RTR state: "
               << cpp_strerror(errno) << dendl;
    return -1;
  }

  ldout(cct, 20) << __func__ << " transition to RTR state successfully." << dendl;

  // now move to RTS
  qpa.qp_state = IBV_QPS_RTS;

  // How long to wait before retrying if packet lost or server dead.
  // Supposedly the timeout is 4.096us*2^timeout.  However, the actual
  // timeout appears to be 4.096us*2^(timeout+1), so the setting
  // below creates a 135ms timeout.
  qpa.timeout = 14;

  // How many times to retry after timeouts before giving up.
  qpa.retry_cnt = 7;

  // How many times to retry after RNR (receiver not ready) condition
  // before giving up. Occurs when the remote side has not yet posted
  // a receive request.
  qpa.rnr_retry = 7; // 7 is infinite retry.
  qpa.sq_psn = my_msg.psn;
  qpa.max_rd_atomic = 1;

  r = ibv_modify_qp(qp->get_qp(), &qpa, IBV_QP_STATE |
      IBV_QP_TIMEOUT |
      IBV_QP_RETRY_CNT |
      IBV_QP_RNR_RETRY |
      IBV_QP_SQ_PSN |
      IBV_QP_MAX_QP_RD_ATOMIC);
  if (r) {
    lderr(cct) << __func__ << " failed to transition to RTS state: "
               << cpp_strerror(errno) << dendl;
    return -1;
  }

  // the queue pair should be ready to use once the client has finished
  // setting up their end.
  ldout(cct, 20) << __func__ << " transition to RTS state successfully." << dendl;
  ldout(cct, 20) << __func__ << " QueuePair: " << qp << " with qp:" << qp->get_qp() << dendl;

  if (!is_server) {
    connected = 1; //indicate successfully
    ldout(cct, 20) << __func__ << " handle fake send, wake it up. QP: " << my_msg.qpn << dendl;
    submit(false);
  }
  active = true;

  return 0;
}

int RDMAConnectedSocketImpl::try_connect(const entity_addr_t& peer_addr, const SocketOptions &opts) {
  ldout(cct, 20) << __func__ << " nonblock:" << opts.nonblock << ", nodelay:"
                 << opts.nodelay << ", rbuf_size: " << opts.rcbuf_size << dendl;
  tcp_fd = infiniband->net.connect(peer_addr);

  if (tcp_fd < 0) {
    return -errno;
  }
  infiniband->net.set_close_on_exec(tcp_fd);

  int r = infiniband->net.set_socket_options(tcp_fd, opts.nodelay, opts.rcbuf_size);
  if (r < 0) {
    ::close(tcp_fd);
    return -errno;
  }

  ldout(cct, 20) << __func__ << " tcp_fd: " << tcp_fd << dendl;
  infiniband->net.set_priority(tcp_fd, opts.priority);
  my_msg.peer_qpn = 0;
  r = infiniband->send_msg(tcp_fd, my_msg);
  if (r < 0)
    return r;

  worker->center.create_file_event(tcp_fd, EVENT_READABLE, con_handler);
  return 0;
}

void RDMAConnectedSocketImpl::handle_connection() {
  ldout(cct, 20) << __func__ << " QP: " << my_msg.qpn << " tcp_fd: " << tcp_fd << " fd: " << notify_fd << dendl;
  int r = infiniband->recv_msg(tcp_fd, peer_msg);
  if (r < 0) {
    if (r != -EAGAIN)
      fault();
    return;
  }

  if (!is_server) {// syn + ack from server
    my_msg.peer_qpn = peer_msg.qpn;
    ldout(cct, 20) << __func__ << " peer msg :  < " << peer_msg.qpn << ", " << peer_msg.psn
                   <<  ", " << peer_msg.lid << ", " << peer_msg.peer_qpn << "> " << dendl;
    if (!connected) {
      r = activate();
      assert(!r);
    }
    notify();
    r = infiniband->send_msg(tcp_fd, my_msg);
    if (r < 0) {
      ldout(cct, 1) << __func__ << " send client ack failed." << dendl;
      fault();
    }
  } else {
    if (peer_msg.peer_qpn == 0) {// syn from client
      if (active) {
        ldout(cct, 10) << __func__ << " server is already active." << dendl;
        return ;
      }
      r = infiniband->send_msg(tcp_fd, my_msg);
      if (r < 0) {
        ldout(cct, 1) << __func__ << " server ack failed." << dendl;
        fault();
        return ;
      }
      r = activate();
      assert(!r);
    } else { // ack from client
      connected = 1;
      cleanup();
      submit(false);
      notify();
    }
  }
}

ssize_t RDMAConnectedSocketImpl::read(char* buf, size_t len)
{
  uint64_t i = 0;
  int r = ::read(notify_fd, &i, sizeof(i));
  ldout(cct, 20) << __func__ << " notify_fd : " << i << " in " << my_msg.qpn << " r = " << r << dendl;
  if (error)
    return -error;
  ssize_t read = 0;
  if (!buffers.empty())
    read = read_buffers(buf,len);

  std::vector<ibv_wc> cqe;
  get_wc(cqe);
  if (cqe.empty())
    return read == 0 ? -EAGAIN : read;

  ldout(cct, 20) << __func__ << " poll queue got " << cqe.size() << " responses. QP: " << my_msg.qpn << dendl;
  for (size_t i = 0; i < cqe.size(); ++i) {
    ibv_wc* response = &cqe[i];
    assert(response->status == IBV_WC_SUCCESS);
    Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);
    ldout(cct, 25) << __func__ << " chunk length: " << response->byte_len << " bytes." << chunk << dendl;
    chunk->prepare_read(response->byte_len);
    if (response->byte_len == 0) {
      if (connected) {
        error = ECONNRESET;
        assert(infiniband->post_chunk(chunk) == 0);
        ldout(cct, 20) << __func__ << " got remote close msg..." << dendl;
      }
      break;
    }
    //assert(response->byte_len);
    if (read == (ssize_t)len) {
      buffers.push_back(chunk);
      ldout(cct, 25) << __func__ << " buffers add a chunk: " << response->byte_len << dendl;
    } else if (read + response->byte_len > (ssize_t)len) {
      read += chunk->read(buf+read, (ssize_t)len-read);
      buffers.push_back(chunk);
      ldout(cct, 25) << __func__ << " buffers add a chunk: " << chunk->get_offset() << ":" << chunk->get_bound() << dendl;
    } else {
      read += chunk->read(buf+read, response->byte_len);
      assert(infiniband->post_chunk(chunk) == 0);
    }
  }

  if (is_server && connected == 0) {
    ldout(cct, 20) << __func__ << " we do not need last handshake, QP: " << my_msg.qpn << " peer QP: " << peer_msg.qpn << dendl;
    connected = 1; //if so, we don't need the last handshake
    cleanup();
    submit(false);
  }

  if (read == 0 && error)
    return -error;
  return read == 0 ? -EAGAIN : read;
}

ssize_t RDMAConnectedSocketImpl::read_buffers(char* buf, size_t len)
{
  size_t read = 0, tmp = 0;
  vector<Chunk*>::iterator c = buffers.begin();
  for (; c != buffers.end() ; ++c) {
    tmp = (*c)->read(buf+read, len-read);
    read += tmp;
    ldout(cct, 25) << __func__ << " this iter read: " << tmp << " bytes." << " offset: " << (*c)->get_offset() << " ,bound: " << (*c)->get_bound()  << ". Chunk:" << *c  << dendl;
    if ((*c)->over()) {
      assert(infiniband->post_chunk(*c) == 0);
      ldout(cct, 25) << __func__ << " one chunk over." << dendl;
    }
    if (read == len) {
      break;
    }
  }

  if (c != buffers.end() && (*c)->over())
    c++;
  buffers.erase(buffers.begin(), c);
  ldout(cct, 25) << __func__ << " got " << read  << " bytes, buffers size: " << buffers.size() << dendl;
  return read;
}

ssize_t RDMAConnectedSocketImpl::zero_copy_read(bufferptr &data)
{
  if (error)
    return -error;
  static const int MAX_COMPLETIONS = 16;
  ibv_wc wc[MAX_COMPLETIONS];
  ssize_t size = 0;

  ibv_wc*  response;
  Chunk* chunk;
  bool loaded = false;
  auto iter = buffers.begin();
  if (iter != buffers.end()) {
    chunk = *iter;
    // FIXME need to handle release
    // auto del = std::bind(&Chunk::post_srq, std::move(chunk), infiniband);
    buffers.erase(iter);
    loaded = true;
    size = chunk->bound;
  }

  std::vector<ibv_wc> cqe;
  get_wc(cqe);
  if (cqe.empty())
    return size == 0 ? -EAGAIN : size;

  ldout(cct, 20) << __func__ << " pool completion queue got " << cqe.size() << " responses."<< dendl;

  for (size_t i = 0; i < cqe.size(); ++i) {
    response = &wc[i];
    chunk = reinterpret_cast<Chunk*>(response->wr_id);
    chunk->prepare_read(response->byte_len);
    if (!loaded && i == 0) {
      // FIXME need to handle release
      // auto del = std::bind(&Chunk::post_srq, std::move(chunk), infiniband);
      size = chunk->bound;
      continue;
    }
    buffers.push_back(chunk);
    iter++;
  }

  if (size == 0)
    return -EAGAIN;
  return size;
}

ssize_t RDMAConnectedSocketImpl::send(bufferlist &bl, bool more)
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
    Mutex::Locker l(lock);
    pending_bl.claim_append(bl);
    if (!connected) {
      ldout(cct, 20) << __func__ << " fake send to upper, QP: " << my_msg.qpn << dendl;
      return bytes;
    }
  }
  ldout(cct, 20) << __func__ << " QP: " << my_msg.qpn << dendl;
  ssize_t r = submit(more);
  if (r < 0 && r != -EAGAIN)
    return r;
  return bytes;
}

ssize_t RDMAConnectedSocketImpl::submit(bool more)
{
  if (error)
    return -error;
  Mutex::Locker l(lock);
  std::vector<Chunk*> tx_buffers;
  size_t bytes = pending_bl.length();
  ldout(cct, 20) << __func__ << " we need " << bytes << " bytes. iov size: "
                 << pending_bl.buffers().size() << dendl;
  if (!bytes)
    return 0;

  int ret = worker->reserve_message_buffer(this, tx_buffers, bytes);
  if (ret == 0) {
    ldout(cct, 10) << __func__ << " no enough buffers in worker " << worker << dendl;
    return -EAGAIN; // that is ok , cause send will return bytes. == 0 enough buffers, < 0 no buffer, >0 not enough
  }
  vector<Chunk*>::iterator current_buffer = tx_buffers.begin();
  list<bufferptr>::const_iterator it = pending_bl.buffers().begin();
  unsigned total = 0;
  while (it != pending_bl.buffers().end()) {
    const uintptr_t addr = reinterpret_cast<const uintptr_t>(it->c_str());
    unsigned copied = 0;
    while (copied < it->length()) {
      uint32_t r = (*current_buffer)->write((char*)addr+copied, it->length() - copied);
      copied += r;
      total += r;
      if ((*current_buffer)->full()){
        ++current_buffer;
        if (current_buffer == tx_buffers.end())
          goto sending;
      }
    }
    ++it;
  }

 sending:
  assert(total <= pending_bl.length());
  bufferlist swapped;
  if (total < pending_bl.length()) {
    pending_bl.splice(total, pending_bl.length()-total, &swapped);
    pending_bl.swap(swapped);
  } else {
    pending_bl.clear();
  }

  ldout(cct, 20) << __func__ << " left bytes: " << pending_bl.length() << " in buffers "
                 << pending_bl.buffers().size() << dendl;

  int r = post_work_request(tx_buffers);
  if (r < 0)
    return r;

  ldout(cct, 20) << __func__ << " finished sending " << bytes << " bytes." << dendl;
  return bytes;
}

int RDMAConnectedSocketImpl::post_work_request(std::vector<Chunk*> &tx_buffers)
{
  ldout(cct, 20) << __func__ << " QP: " << my_msg.qpn << " " << tx_buffers[0] << dendl;
  vector<Chunk*>::iterator current_buffer = tx_buffers.begin();
  ibv_sge isge[tx_buffers.size()];
  uint32_t current_sge = 0;
  ibv_send_wr iswr[tx_buffers.size()];
  uint32_t current_swr = 0;
  ibv_send_wr* pre_wr = NULL;

  memset(iswr, 0, sizeof(iswr));
  memset(isge, 0, sizeof(isge));
  current_buffer = tx_buffers.begin();
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
    /*if (isge[current_sge].length < infiniband->max_inline_data) {
      iswr[current_swr].send_flags = IBV_SEND_INLINE;
      ldout(cct, 20) << __func__ << " send_inline." << dendl;
      }*/

    if (pre_wr)
      pre_wr->next = &iswr[current_swr];
    pre_wr = &iswr[current_swr];
    ++current_sge;
    ++current_swr;
    ++current_buffer;
  }

  ibv_send_wr *bad_tx_work_request;
  if (ibv_post_send(qp->get_qp(), iswr, &bad_tx_work_request)) {
    lderr(cct) << __func__ << " failed to send data"
               << " (most probably should be peer not ready): "
               << cpp_strerror(errno) << dendl;
    return -errno;
  }
  ldout(cct, 20) << __func__ << " qp state is : " << Infiniband::qp_state_string(qp->get_state()) << dendl;
  return 0;
}

void RDMAConnectedSocketImpl::fin() {
  ibv_send_wr wr;
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = reinterpret_cast<uint64_t>(qp);
  wr.num_sge = 0;
  wr.opcode = IBV_WR_SEND;
  wr.send_flags = IBV_SEND_SIGNALED;
  ibv_send_wr* bad_tx_work_request;
  if (ibv_post_send(qp->get_qp(), &wr, &bad_tx_work_request)) {
    lderr(cct) << __func__ << " failed to send message="
               << " ibv_post_send failed(most probably should be peer not ready): "
               << cpp_strerror(errno) << dendl;
    return ;
  }
}

void RDMAConnectedSocketImpl::cleanup() {
  if (con_handler) {
    (static_cast<C_handle_connection*>(con_handler))->close();
    worker->center.submit_to(worker->center.get_id(), [this]() {
      worker->center.delete_file_event(tcp_fd, EVENT_READABLE);
    }, false);
    delete con_handler;
    con_handler = nullptr;
  }
}
