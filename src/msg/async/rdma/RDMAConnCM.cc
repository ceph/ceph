// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Mellanox <amirva@mellanox.com>
 *
 * Author: Amir Vadai <amirva@mellanox.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <sys/socket.h>
#include <netdb.h>

#include "Infiniband.h"
#include "RDMAStack.h"
#include "RDMAConnCM.h"
#include "Device.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << " RDMAConnCM "

CMHandler::CMHandler(CephContext *cct, RDMAConnCM *s, RDMAWorker *w, struct rdma_cm_id *_id)
  : cct(cct), csc(s), worker(w), channel(nullptr), refs({2}),
    cm_handler(new C_handle_cm_event(this))
{
  int err;

  if (_id) {
    id = _id;
    id->context = this;

    return;
  }

  // XXX Maybe should share the same channel with all sockets?
  // XXX seems to be inefficient to have a channel per connection
  channel = rdma_create_event_channel();
  assert(channel);

  worker->center.create_file_event(channel->fd, EVENT_READABLE, cm_handler);

  err = rdma_create_id(channel, &id, this, RDMA_PS_TCP);
  assert(!err);

  ldout(cct, 1) << __func__ << " " << *this << dendl;
}

CMHandler::~CMHandler()
{
  ldout(cct, 1) << __func__ << " " << *this << dendl;
  
  rdma_destroy_id(id);

  if (channel)
    rdma_destroy_event_channel(channel);
}

void CMHandler::put()
{
  refs--;

  ldout(cct, 1) << __func__ << *this ": ref-- = " < refs << dendl;

  if (!refs)
    delete this;
}

void CMHandler::disconnect()
{
  ldout(cct, 1) << __func__ << " " << *this << dendl;
  rdma_disconnect(id);
}

void CMHandler::handle_cm_event(int fd)
{
  struct rdma_cm_event *event;
  int err;

  err = rdma_get_cm_event(id->channel, &event);
  assert(!err);

  if (!csc) {
    ldout(cct, 1) << __func__ << " CM event: " << rdma_event_str(event->event) << dendl;
    assert(event->event == RDMA_CM_EVENT_DISCONNECTED ||
           event->event == RDMA_CM_EVENT_TIMEWAIT_EXIT);

    rdma_ack_cm_event(event);

    if (channel) {
      worker->center.submit_to(worker->center.get_id(), [this]() {
			       worker->center.delete_file_event(channel->fd, EVENT_READABLE);
			       }, false);
      /* Removing here the event for the channel - the channel itself will be
       * destroyed upon IBV_EVENT_QP_LAST_WQE_REACHED async event will happen
       * Couldn't delete the file event near the destory_channel() since can't
       * block in that context.
       */
    }

    if (event->event == RDMA_CM_EVENT_TIMEWAIT_EXIT)
      put();

    return;
  }

  ldout(cct, 1) << __func__ << " " << *csc << " CM event: " << rdma_event_str(event->event) << dendl;

  switch (event->event) {
  case RDMA_CM_EVENT_ADDR_RESOLVED:
    csc->cm_addr_resolved();
    break;

  case RDMA_CM_EVENT_ROUTE_RESOLVED:
    csc->cm_route_resolved();
    break;

  case RDMA_CM_EVENT_ESTABLISHED:
    csc->cm_established(event->param.conn.qp_num);
    break;

  case RDMA_CM_EVENT_DISCONNECTED:
    csc->cm_disconnected();
    disconnect();
    break;

  case RDMA_CM_EVENT_TIMEWAIT_EXIT:
    /* must ack before put() to make sure no pending events exist */
    rdma_ack_cm_event(event);

    put();
    return;

  case RDMA_CM_EVENT_REJECTED:
    csc->cm_rejected();
    break;

  case RDMA_CM_EVENT_ADDR_ERROR:
  case RDMA_CM_EVENT_ROUTE_ERROR:
  case RDMA_CM_EVENT_CONNECT_ERROR:
  case RDMA_CM_EVENT_UNREACHABLE:
    ldout(cct, 1) << __func__  << " event: " << rdma_event_str(event->event) <<
      ", error: " << event->status << dendl;
    csc->cm_error();
    break;

  case RDMA_CM_EVENT_DEVICE_REMOVAL:
    assert(0);

  default:
    assert(0);
    break;
  }

  rdma_ack_cm_event(event);
}


RDMAConnCM::RDMAConnCM(CephContext *cct, Infiniband *ib,
					     RDMADispatcher* s, RDMAWorker *w)
  : RDMAConnectedSocketImpl(cct, ib, s, w),
    cm_handler(new CMHandler(cct, this, w))
{
}

RDMAConnCM::RDMAConnCM(CephContext *cct, Infiniband *ib,
					     RDMADispatcher* s, RDMAWorker *w,
					     struct rdma_cm_id *cma_id,
                                             uint32_t peer_qpn)
  : RDMAConnectedSocketImpl(cct, ib, s, w),
    cm_handler(new CMHandler(cct, this, w, cma_id)),
    peer_qpn(peer_qpn)
{
  assert(cma_id);

  ibdev = ib->get_device(cm_handler->id->verbs);
  assert(ibdev);

  ibport = cm_handler->id->port_num;

  ldout(cct, 1) << __func__ << " Device: " << *ibdev << " port: " << ibport << dendl;

  is_server = true;
}

RDMAConnCM::~RDMAConnCM()
{
  ldout(cct, 1) << __func__ << " called" << dendl;

  cm_handler->set_orphan();
  cm_handler = nullptr;
}

int RDMAConnCM::try_connect(const entity_addr_t &peer_addr,
					   const SocketOptions &opts)
{
  int err;
  char buf[NI_MAXHOST] = { 0 };
  char serv[NI_MAXSERV] = { 0 };
  struct rdma_addrinfo hints = { 0 };
  struct rdma_addrinfo *rai;
 
  hints.ai_port_space = RDMA_PS_TCP;

  getnameinfo(peer_addr.get_sockaddr(), peer_addr.get_sockaddr_len(),
	      buf, sizeof(buf),
	      serv, sizeof(serv),
	      NI_NUMERICHOST | NI_NUMERICSERV);

  ldout(cct, 1) << __func__ << " dest: " << buf << ":" << peer_addr.get_port() << dendl;

  ldout(cct, 1) << __func__ << " nonblock:" << opts.nonblock << ", nodelay:"
    << opts.nodelay << ", rbuf_size: " << opts.rcbuf_size << dendl;

  err = rdma_getaddrinfo(buf, serv, &hints, &rai);
  assert(!err);

#define TIMEOUT 2000
  err = rdma_resolve_addr(cm_handler->id, rai->ai_src_addr, rai->ai_dst_addr, TIMEOUT);
  assert(!err);

  rdma_freeaddrinfo(rai);

  ldout(cct, 1) << __func__ << " waiting for address resolution " << dendl;

  // making the address resolution synchronious because we need a QP (to fill
  // up notify_fd when returning from this function
  cm_handler->handle_cm_event();

  return 0;
}

int RDMAConnCM::alloc_resources()
{
  assert(ibdev);
  assert(ibport);

  ibdev->init(ibport);

  qp = ibdev->create_queue_pair(ibport, IBV_QPT_RC, cm_handler);
  assert(qp);

  register_qp(qp);

  return 0;
}

void RDMAConnCM::fin()
{
  uint64_t wr_id = reinterpret_cast<uint64_t>(cm_handler);

  ldout(cct, 1) << __func__ << " sending " << *cm_handler << 
    " ( " << cm_handler << " )" << dendl;

  RDMAConnectedSocketImpl::fin(wr_id);
}

struct sockaddr *RDMAConnCM::get_peer_addr()
{
  return cm_handler->id ? &cm_handler->id->route.addr.dst_addr : NULL;
}

struct sockaddr *RDMAConnCM::get_my_addr()
{
  return cm_handler->id ? &cm_handler->id->route.addr.src_addr : NULL;
}

void RDMAConnCM::cm_addr_resolved()
{
  int err;

  ibdev = infiniband->get_device(cm_handler->id->verbs);
  assert(ibdev);

  ibport = cm_handler->id->port_num;

  ldout(cct, 1) << __func__ << " Device: " << *ibdev << " port: " << ibport << dendl;

  err = alloc_resources();
  assert(!err);

  err = rdma_resolve_route(cm_handler->id, TIMEOUT);
  assert(!err);
}

void RDMAConnCM::cm_route_resolved()
{
  int err;

  err = rdma_connect(cm_handler->id, NULL);
  assert(!err);
}

void RDMAConnCM::cm_established(uint32_t qpn)
{
  peer_qpn = qpn;
ldout(cct, 1) << __func__ << ":" << __LINE__ << " connection = 1 zzzzzzzzzzzzzzzzz" << dendl;
  connected = 1;
  ldout(cct, 1) << __func__ << " handle fake send, wake it up. QP: " << qp->get_local_qp_number() << dendl;
  submit(false);

  active = true;

  notify();
}

void RDMAConnCM::cm_disconnected()
{
  error = ECONNRESET;
  close();
}

void RDMAConnCM::cm_rejected()
{
  fault();
}

void RDMAConnCM::cm_error()
{
  assert(0);
}

void RDMAConnCM::shutdown()
{
  ldout(cct, 1) << __func__ << dendl;
  if (error)
    cm_handler->disconnect();

  RDMAConnectedSocketImpl::shutdown();
}

void RDMAConnCM::cleanup()
{
};

int RDMAConnCM::remote_qpn()
{
  return peer_qpn;
}

int RDMAConnCM::activate()
{
ldout(cct, 1) << __func__ << ":" << __LINE__ << " connection = 1 zzzzzzzzzzzzzzzzz" << dendl;
  connected = 1;
//  cleanup();
  submit(false);
  notify();

  return RDMAConnectedSocketImpl::activate();
}

ostream &RDMAConnCM::print(ostream &o)
{
  return o << "RDMAConnCM: {" <<
    " lqpn: " << local_qpn() <<
    " rqpn: " << remote_qpn() <<
    " cm_id: " << cm_handler <<
    " }";
}

RDMAServerConnCM::RDMAServerConnCM(CephContext *cct, Infiniband *ib, RDMADispatcher *s, RDMAWorker *w, entity_addr_t& a)
  : RDMAServerSocketImpl(cct, ib, s, w, a), channel(nullptr), listen_id(nullptr),
    cm_handler(new C_handle_cm_event(this)),
    lock("RDMAConnCM::lock")
{
  int err;

  accept_fd = eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK);
  assert(accept_fd >= 0);

  channel = rdma_create_event_channel();
  assert(channel);

  worker->center.create_file_event(channel->fd, EVENT_READABLE, cm_handler);

  err = rdma_create_id(channel, &listen_id, this, RDMA_PS_TCP);
  assert(!err);

  err = rdma_bind_addr(listen_id, (struct sockaddr *)sa.get_sockaddr());
  assert(!err);
}

RDMAServerConnCM::~RDMAServerConnCM()
{
  worker->center.delete_file_event(channel->fd, EVENT_READABLE);
  ::close(accept_fd);
}

int RDMAServerConnCM::listen(entity_addr_t &sa, const SocketOptions &opt)
{
  int err;

  err = rdma_listen(listen_id, 128);
  assert(!err);

  ldout(cct, 1) << __func__ << " bind to " << sa.get_sockaddr() << " on port " << sa.get_port()  << dendl;

  return 0;
}


int RDMAServerConnCM::on_connect_request(struct rdma_cm_event *event)
{
  struct rdma_cm_id *new_id = event->id;
  struct rdma_conn_param *peer = &event->param.conn;
  RDMAConnCM *server;
  struct rdma_conn_param conn_param = { 0 };
  int err;

  ldout(cct, 1) << __func__ << dendl;

  server = new RDMAConnCM(cct, infiniband, dispatcher, worker, new_id, peer->qp_num);
  assert(server);

  err = server->alloc_resources();
  assert(!err);

  conn_param.qp_num = server->local_qpn();
  conn_param.srq = 1;

  err = rdma_accept(new_id, &conn_param);
  assert(!err);

  ldout(cct, 20) << __func__ << " accepted a new QP" << dendl;

  uint64_t i = 1;
  err = ::write(accept_fd, &i, sizeof(i));
  assert(err == sizeof(i));

  return 0;
}

int RDMAServerConnCM::accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w)
{
  RDMAConnCM *socket;
  uint64_t i;
  int err;

  {
    Mutex::Locker l(lock);
    if (accepted_sockets.empty())
      return -EAGAIN;

    socket = accepted_sockets.front();
    accepted_sockets.pop();
    ldout(cct, 1) << __func__ << ":" << __LINE__ << " new socket: " << socket << dendl;
  }

  err = ::read(accept_fd, &i, sizeof(i));
  if (err != sizeof(i)) {
    lderr(cct) << __func__ << " error reading from accept_fd: err: " << err << 
      " errno: " << cpp_strerror(errno) << dendl;

    ceph_abort();
  }

  std::unique_ptr<RDMAConnectedSocketImpl> csi(socket);
  *sock = ConnectedSocket(std::move(csi));
  if (out) {
    out->set_sockaddr(socket->get_peer_addr());
  }

  return 0;
}

void RDMAServerConnCM::handle_cm_event()
{
  struct rdma_cm_event *event;
  int err;

  err = rdma_get_cm_event(channel, &event);
  assert(!err);

  ldout(cct, 1) << __func__ << " CM event: " << rdma_event_str(event->event) << dendl;

  switch (event->event) {
  case RDMA_CM_EVENT_CONNECT_REQUEST:
    err = on_connect_request(event);
    assert(!err);

    break;

  case RDMA_CM_EVENT_ESTABLISHED:
  {
    CMHandler *cm = (CMHandler *)event->id->context;
    RDMAConnCM *socket = cm->get_socket();
    socket->activate();

    ldout(cct, 1) << __func__ << ":" << __LINE__ << " new socket: " << *socket << dendl;

    {
      Mutex::Locker l(lock);
      accepted_sockets.push(socket);
    }
  }
    break;

  case RDMA_CM_EVENT_DISCONNECTED:
  {
    CMHandler *cm = (CMHandler *)event->id->context;

    ldout(cct, 1) << __func__ << ":" << __LINE__ << " got disconnect" << dendl;
    cm->disconnect();
    err = -ECONNABORTED;
  }
    break;

  case RDMA_CM_EVENT_TIMEWAIT_EXIT:
  {
    CMHandler *cm = (CMHandler *)event->id->context;

    rdma_ack_cm_event(event);
    cm->put();
    return;
  }

  default:
    break;
  }

  rdma_ack_cm_event(event);
}

void RDMAServerConnCM::abort_accept()
{
  rdma_destroy_id(listen_id);
  rdma_destroy_event_channel(channel);
}

int RDMAServerConnCM::fd() const
{
  return accept_fd;
}
