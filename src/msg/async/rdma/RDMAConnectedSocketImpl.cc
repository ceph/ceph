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

int RDMAConnectedSocketImpl::activate() {
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

  connected = 1;//indicate successfully
  return 0;
}

ssize_t RDMAConnectedSocketImpl::read(char* buf, size_t len)
{
  ldout(cct, 20) << __func__ << " need to read bytes: " << len  << " buffers size: " << buffers.size() << dendl;

  if (error)
    return -error;
  ssize_t read = 0;
  if (!buffers.empty())
    read = read_buffers(buf,len);

  std::vector<ibv_wc> cqe;
  get_wc(cqe);
  if (cqe.empty())
    return read == 0 ? -EAGAIN : read;

  ldout(cct, 20) << __func__ << " poll queue got " << cqe.size() << " responses."<< dendl;
  for (size_t i = 0; i < cqe.size(); ++i) {
    ibv_wc* response = &cqe[i];
    assert(response->status == IBV_WC_SUCCESS);
    ldout(cct, 20) << __func__ << " cqe " << response->byte_len << " bytes." << dendl;
    Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);
    chunk->prepare_read(response->byte_len);
    assert(!response->byte_len);
    if (read == (ssize_t)len) {
      buffers.push_back(chunk);
      ldout(cct, 20) << __func__ << " buffers add a chunk: " << response->byte_len << dendl;
    } else if (read + response->byte_len > (ssize_t)len) {
      read += chunk->read(buf+read, (ssize_t)len-read);
      buffers.push_back(chunk);
      ldout(cct, 20) << __func__ << " buffers add a chunk: " << chunk->get_offset() << ":" << chunk->get_bound() << dendl;
    } else {
      read += chunk->read(buf+read, response->byte_len);
      assert(infiniband->post_chunk(chunk) == 0);
    }
  }

  return read == 0 ? -EAGAIN : read;
}

ssize_t RDMAConnectedSocketImpl::read_buffers(char* buf, size_t len)
{
  size_t read = 0, tmp = 0;
  vector<Chunk*>::iterator c = buffers.begin();
  for (; c != buffers.end() ; ++c) {
    tmp = (*c)->read(buf+read, len-read);
    read += tmp;
    ldout(cct, 20) << __func__ << " this iter read: " << tmp << " bytes." << " offset: " << (*c)->get_offset() << " ,bound: " << (*c)->get_bound()  << ". Chunk:" << *c  << dendl;
    if ((*c)->over()) {
      assert(infiniband->post_chunk(*c) == 0);
      ldout(cct, 20) << __func__ << " one chunk over." << dendl;
    }
    if (read == len) {
      break;
    }
  }

  if (c != buffers.end() && (*c)->over())
    c++;
  buffers.erase(buffers.begin(), c);
  ldout(cct, 20) << __func__ << " got " << read << " bytes here. buffers size: " << buffers.size() << dendl;
  return read;
}

ssize_t RDMAConnectedSocketImpl::zero_copy_read(bufferptr &data)
{
  if (error)
    return -error;
  static const int MAX_COMPLETIONS = 16;
  ibv_wc wc[MAX_COMPLETIONS];
  ssize_t size;

  ibv_wc*  response;
  Chunk* chunk;
  bool loaded = false;
  auto iter = buffers.begin();
  if (iter != buffers.end()) {
    chunk = *iter;
    auto del = std::bind(&Chunk::post_srq, std::move(chunk), infiniband);
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
    if(!loaded && i == 0) {
      auto del = std::bind(&Chunk::post_srq, std::move(chunk), infiniband);
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
  if (error)
    return -error;
  size_t bytes = bl.length();
  if (!bytes)
    return 0;
  pending_bl.claim_append(bl);
  ssize_t r = submit(more);
  if (r < 0 && r != -EAGAIN)
    return r;
  return bytes;
}

ssize_t RDMAConnectedSocketImpl::submit(bool more)
{
  if (error)
    return -error;
  std::vector<Chunk*> tx_buffers;
  size_t bytes = pending_bl.length();
  if (worker->reserve_message_buffer(this, tx_buffers, bytes) < 0) {
    ldout(cct, 10) << __func__ << " no enough buffers" << dendl;
    pending_bl.claim_append(pending_bl);
    return -EAGAIN;
  }
  ldout(cct, 20) << __func__ << " prepare " << bytes << " bytes, tx buffer count: " << tx_buffers.size() << dendl;
  vector<Chunk*>::iterator current_buffer = tx_buffers.begin();
  list<bufferptr>::const_iterator it = pending_bl.buffers().begin();
  while (it != pending_bl.buffers().end()) {
    const uintptr_t addr = reinterpret_cast<const uintptr_t>(it->c_str());
    uint32_t copied = 0;
    //  ldout(cct, 20) << __func__ << " app_buffer: " << addr << " length:  " << it->length()  << dendl;
    while(copied < it->length()) {
      //   ldout(cct, 20) << __func__ << " current_buffer: " << *current_buffer << " copied:  " << copied  << dendl;
      size_t ret = (*current_buffer)->write((char*)addr+copied, it->length() - copied);
      copied += ret;
      //  ldout(cct, 20) << __func__ << " ret: " << ret << " copied:  " << copied  << dendl;
      if((*current_buffer)->full()){
        ++current_buffer;
      }
    }
    ++it;
  }

  ssize_t r = post_work_request(tx_buffers);
  if (r < 0)
    return r;

  ldout(cct, 20) << __func__ << " finished sending " << bytes << " bytes." << dendl;
  pending_bl.clear();
  return bytes;
}

int RDMAConnectedSocketImpl::post_work_request(std::vector<Chunk*> &tx_buffers)
{
  vector<Chunk*>::iterator current_buffer = tx_buffers.begin();
  ibv_sge isge[tx_buffers.size()];
  uint32_t current_sge = 0;
  ibv_send_wr iswr[tx_buffers.size()];
  uint32_t current_swr = 0;
  ibv_send_wr* pre_wr = NULL;

  current_buffer = tx_buffers.begin();
  while (current_buffer != tx_buffers.end()) {
    isge[current_sge].addr = reinterpret_cast<uint64_t>((*current_buffer)->buffer);
    isge[current_sge].length = (*current_buffer)->get_offset();
    isge[current_sge].lkey = (*current_buffer)->mr->lkey;
    ldout(cct, 20) << __func__ << " current_buffer: " << *current_buffer << " length: " << isge[current_sge].length  << dendl;

    iswr[current_swr].wr_id = reinterpret_cast<uint64_t>(*current_buffer);
    iswr[current_swr].next = NULL;
    iswr[current_swr].sg_list = &isge[current_sge];
    iswr[current_swr].num_sge = 1;
    iswr[current_swr].opcode = IBV_WR_SEND;
    iswr[current_swr].send_flags = IBV_SEND_SIGNALED;
    /*if(isge[current_sge].length < infiniband->max_inline_data) {
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
  return 0;
}
