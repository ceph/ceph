// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright(c) 2021 Liu, Changcheng <changcheng.liu@aliyun.com>
 *
 */

#include "include/compat.h"
#include "include/sock_compat.h"

#include "msg/async/net_handler.h"

#include "UCXStack.h"

#define FN_NAME (__CEPH_ASSERT_FUNCTION == nullptr ? __func__ : __CEPH_ASSERT_FUNCTION)
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ucx_msg
#undef dout_prefix
#define dout_prefix _prefix(_dout, FN_NAME)
static ostream& _prefix(std::ostream *_dout,
                        std::string_view func_name)
{
  return *_dout << "UCXSerSktImpl: " << func_name << ": ";
}

UCXSerSktImpl::UCXSerSktImpl(CephContext *cct, UCXWorker *ucx_worker,
                             std::shared_ptr<UCXProEngine> ucp_worker_engine,
                             entity_addr_t& listen_addr, unsigned addr_slot)
  : ServerSocketImpl(listen_addr.get_type(), addr_slot),
    cct(cct), net(cct), ucx_worker(ucx_worker), ucp_worker_engine(ucp_worker_engine),
    listen_addr(listen_addr)
{
}

int UCXSerSktImpl::listen(const SocketOptions &skt_opts)
{
  struct sockaddr_in listen_addr;
  memset(&listen_addr, 0, sizeof(listen_addr));
  listen_addr = this->listen_addr.in4_addr();

  ucp_listener_params_t listener_params;
  listener_params.field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                               UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
  listener_params.sockaddr.addr = (const sockaddr*)&listen_addr;
  listener_params.sockaddr.addrlen = sizeof(listen_addr);
  listener_params.conn_handler.cb  = recv_req_con_cb;
  listener_params.conn_handler.arg = reinterpret_cast<void*>(this);

  listen_skt_notify_fd = eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK);

  ucs_status_t status = ucp_listener_create(ucp_worker_engine->get_ucp_worker(),
                                            &listener_params,
                                            &ucp_ser_listener);
  if (status != UCS_OK) {
      lderr(cct) << "ucp_listener_create() failed: " << ucs_status_string(status)
                 << dendl;
      return false;
  }

  ldout(cct, 20) << "started listener " << ucp_ser_listener << " on "
                 << sockaddr_str((const sockaddr*)&listen_addr, sizeof(listen_addr)) << ", "
                 << "sever listen fd: " << listen_skt_notify_fd
                 << dendl;
  return true;
}

int UCXSerSktImpl::accept(ConnectedSocket *ser_con_socket, const SocketOptions &opt,
                          entity_addr_t *peer_addr, Worker *ucx_worker)
{
  eventfd_t listen_sync_val = 0xdead;
  int rst = eventfd_read(listen_skt_notify_fd, &listen_sync_val);
  ldout(cct, 20) << "listen_skt_notify_fd : " << listen_sync_val << ", "
                 << "rst = " << rst << dendl;
  if (listen_sync_val == 0xdead) {
    ceph_assert(conn_requests.empty());
    return -EAGAIN;
  }

  ceph_assert(ser_con_socket);
  ceph_assert(!conn_requests.empty());

  auto conn_request = conn_requests.front();
  conn_requests.pop_front();

  auto ucx_ser_conskt = new UCXConSktImpl(cct,
                                          dynamic_cast<UCXWorker*>(ucx_worker),
                                          ucp_worker_engine);
  ucx_ser_conskt->set_conn_request(conn_request);
  ucs_status_t status = ucx_ser_conskt->create_server_ep();
  if (status == UCS_OK) {
    ucp_conn_request_attr_t conn_req_attr;
    conn_req_attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    status = ucp_conn_request_query(conn_request.conn_request, &conn_req_attr);
    if (status == UCS_OK) {
      peer_addr->set_type(addr_type);
      peer_addr->set_sockaddr((sockaddr*)(&conn_req_attr.client_address));
    }
  } else {
    lderr(cct) << "failed to create accept ep"
               << dendl;
    return -1;
  }

  ucx_ser_conskt->set_connection_status(1);
  ucx_ser_conskt->active = true;
  *ser_con_socket = ConnectedSocket(std::unique_ptr<UCXConSktImpl>(ucx_ser_conskt));

  return 0;
}

void UCXSerSktImpl::abort_accept()
{
  if (listen_skt_notify_fd >= 0) {
    ::close(listen_skt_notify_fd);
  }
}

int UCXSerSktImpl::fd() const
{
  ceph_assert(listen_skt_notify_fd != -1);
  return listen_skt_notify_fd;
}

const std::string
UCXSerSktImpl::sockaddr_str(const struct sockaddr* saddr, size_t addrlen)
{
  char buf[128];
  int port;

  if (saddr->sa_family != AF_INET) {
      return "<unknown address family>";
  }

  struct sockaddr_storage addr = {0};
  memcpy(&addr, saddr, addrlen);
  switch (addr.ss_family) {
  case AF_INET:
      inet_ntop(AF_INET, &((struct sockaddr_in*)&addr)->sin_addr,
                buf, sizeof(buf));
      port = ntohs(((struct sockaddr_in*)&addr)->sin_port);
      break;
  case AF_INET6:
      inet_ntop(AF_INET6, &((struct sockaddr_in6*)&addr)->sin6_addr,
                buf, sizeof(buf));
      port = ntohs(((struct sockaddr_in6*)&addr)->sin6_port);
      break;
  default:
      return "<invalid address>";
  }

  snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), ":%d", port);
  return buf;
}

void UCXSerSktImpl::listen_notify()
{
  eventfd_t listen_sync_val = 0x1;
  int rst = eventfd_write(listen_skt_notify_fd, listen_sync_val);
  ceph_assert(rst == 0);
}

void UCXSerSktImpl::recv_req_con_cb(ucp_conn_request_h conn_req, void *arg)
{
  UCXSerSktImpl *this_self = reinterpret_cast<UCXSerSktImpl*>(arg);

  ucp_conn_request_attr_t conn_req_attr;
  conn_req_attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
  ucs_status_t status = ucp_conn_request_query(conn_req, &conn_req_attr);

  if (status == UCS_OK) {
    ldout(this_self->cct, 20) << "got new connection request "
                              << conn_req << " from client "
                              << this_self->sockaddr_str(
                                   (const struct sockaddr*)
                                      &conn_req_attr.client_address,
                                    sizeof(conn_req_attr.client_address))
                              << dendl;
  } else {
    lderr(this_self->cct) << "got new connection request " << conn_req << ", "
                          << "ucp_conn_request_query() failed "
                          << "(" << ucs_status_string(status) << ")"
                          << dendl;
  }

  conn_req_t conn_request;
  conn_request.conn_request = conn_req;
  gettimeofday(&conn_request.arrival_time, NULL);

  this_self->conn_requests.push_back(conn_request);
  this_self->listen_notify(); // notify upper layer to accept connection
}
