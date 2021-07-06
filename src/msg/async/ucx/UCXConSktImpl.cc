// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright(c) 2021 Liu, Changcheng <changcheng.liu@aliyun.com>
 *
 */

#include <ucs/sys/sock.h>

#include "UCXStack.h"

#define FN_NAME (__CEPH_ASSERT_FUNCTION == nullptr ? __func__ : __CEPH_ASSERT_FUNCTION)
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ucx_msg
#undef dout_prefix
#define dout_prefix _prefix(_dout, FN_NAME)
static ostream& _prefix(std::ostream *_dout,
                        std::string_view func_name)
{
  return *_dout << "UCXConSktImpl: " << func_name << ": ";
}

UCXConSktImpl::UCXConSktImpl(CephContext *cct, UCXWorker *ucx_worker,
                             std::shared_ptr<UCXProEngine> ucp_worker_engine)
  : cct(cct), ucx_worker(ucx_worker), ucp_worker_engine(ucp_worker_engine),
    active(false)
{
  data_event_fd = eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK);
  ldout(cct, 25) << "connected fd: " << data_event_fd << dendl;
}

UCXConSktImpl::~UCXConSktImpl()
{
  ldout(cct, 25) << dendl;
  ucx_worker->remove_pending_conn(this);
}

int UCXConSktImpl::is_connected()
{
  return connected == 1;
}

void UCXConSktImpl::set_connection_status(int con_status)
{
  connected = con_status;
}

void UCXConSktImpl::set_active_status(bool active_status)
{
  active = active_status;
}

ssize_t UCXConSktImpl::read(char* buf, size_t len)
{
  eventfd_t data_event_val = 0;
  int rst = eventfd_read(data_event_fd, &data_event_val);
  ldout(cct, 30) << "read data_event_fd  : " << data_event_fd << ", "
                 << "val : " << data_event_val << ", "
                 << "rst = " << rst << dendl;

  std::lock_guard l{recv_lock};
  if (queue_bl.size() != 0) {
    recv_pending_bl.claim_append(queue_bl.front());
    queue_bl.pop();
  }

  auto bl_len = recv_pending_bl.length();
  len = len > bl_len ? bl_len : len;
  if (len == 0) {
    return -EAGAIN;
    return 0;
  }
  memcpy(buf, recv_pending_bl.c_str(), len);
  recv_pending_bl.splice(0, len);
  if (recv_pending_bl.length() || queue_bl.size()) {
      data_notify();
  }
  return len == 0 ? -EAGAIN : len;
}

ssize_t UCXConSktImpl::send_segments()
{
  std::lock_guard l{send_lock};
  if (send_pending_bl.length() == 0) {
    return 0;
  }
  size_t sent_bytes = 0;
  auto pb = std::cbegin(send_pending_bl.buffers());
  uint64_t left_pbrs = send_pending_bl.get_num_buffers();

  ucp_request_param_t param;
  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK  |
                       UCP_OP_ATTR_FIELD_FLAGS;
  param.cb.send      = am_data_send_callback;
  param.flags        = UCP_AM_SEND_FLAG_EAGER | UCP_AM_RECV_ATTR_FIELD_REPLY_EP;
  param.datatype     = 0;

  while (left_pbrs) {
    iomsg_t msg{sn_send++, pb->length(), IO_WRITE};
    ucs_status_ptr_t send_req = ucp_am_send_nbx(conn_ep, 0xcafebeef, &msg,
	                                    sizeof(msg), pb->c_str(),
					    pb->length(), &param);
    //TODO: send_am// process_request;
    wait_status_t wait_status = ucp_worker_engine->wait_completion(send_req, 5);
    if (wait_status == WAIT_STATUS_TIMED_OUT) {
      wait_status = ucp_worker_engine->wait_completion(send_req);
    }
    if (wait_status != WAIT_STATUS_OK) {
      lderr(cct) << "failed to send data" << dendl;
      lderr(cct) << "potential mem leak " << send_req << dendl;
      break;
    }
    sent_bytes += pb->length();
    left_pbrs--;
    pb++;
  }
  if (sent_bytes) {
    bufferlist swapped;
    if (sent_bytes < send_pending_bl.length()) {
      send_pending_bl.splice(sent_bytes, send_pending_bl.length() - sent_bytes,
	                     &swapped);
      send_pending_bl.swap(swapped);
    } else {
      send_pending_bl.clear();
    }
  }
  return sent_bytes;
}

ssize_t UCXConSktImpl::send(ceph::bufferlist &bl, bool more)
{
  size_t bytes = bl.length();
  if (bytes == 0) {
    return 0;
  }
  {
    std::lock_guard l{send_lock};
    send_pending_bl.claim_append(bl);
  }
  ssize_t rst = send_segments();
  if (rst < 0 && rst != -EAGAIN) {
    return rst;
  }
  return bytes;
}

void UCXConSktImpl::shutdown()
{
  err_con = 1;
  active = false;
}

void UCXConSktImpl::close()
{
  active = false;
}

int UCXConSktImpl::fd() const
{
  ceph_assert(data_event_fd != -1);
  return data_event_fd;
}

void
UCXConSktImpl::handle_io_am_write_request(const iomsg_t *msg, void *data,
                                          const ucp_am_recv_param_t *param)
{
  ldout(cct, 30) << "receiving AM IO write data" << dendl;
  ceph_assert(msg->data_size != 0);
  ceph_assert(conn_ep != NULL);

  bufferlist bl(msg->data_size);

  if (!(param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV)) {
    bl.append((char*)data, msg->data_size);
  } else {
    ceph_assert(param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV); // todo: implment support RNDV
  }
  while (msg->sn != sn_recv) {
    ldout(cct, 20) << " recv sn: " << msg->sn << ", pending sn: " << sn_recv << dendl;
  }
  queue_bl.push(bl);
  sn_recv++;
  data_notify();
}

void UCXConSktImpl::set_conn_request(const conn_req_t &conn_request)
{
  this->conn_request = conn_request;
}

void UCXConSktImpl::handle_connection_error(ucs_status_t status)
{
  lderr(cct) << "detected error: " << ucs_status_string(status)
             << dendl;
}

void UCXConSktImpl::ep_error_cb(void *arg, ucp_ep_h ep, ucs_status_t status)
{
  UCXConSktImpl *self_this = reinterpret_cast<UCXConSktImpl*>(arg);
  lderr(self_this->cct) << dendl;
  self_this->handle_connection_error(status);
}

void UCXConSktImpl::am_data_recv_callback(void *request, ucs_status_t status,
                                          size_t length, void *user_data)
{
  ucx_request *r = reinterpret_cast<ucx_request*>(request);
  r->status = status;
  r->completed = true;

  // mock:
  r->callback = NULL;
  r->status = UCS_OK;
  r->completed = false;
  r->recv_length = 0;
  r->pos.next = NULL;
  r->pos.prev = NULL;

  ucp_request_free(request);
}

void UCXConSktImpl::am_data_send_callback(void *request, ucs_status_t status,
                                          void *user_data)
{
  ucx_request *r = reinterpret_cast<ucx_request*>(request);
  r->status = status;
  r->completed = true;

  // mock:
  r->callback = NULL;
  r->status = UCS_OK;
  r->completed = false;
  r->recv_length = 0;
  r->pos.next = NULL;
  r->pos.prev = NULL;

  ucp_request_free(request);
}

ucs_status_t UCXConSktImpl::create_server_ep()
{
  ucp_conn_request_attr_t conn_req_attr;
  conn_req_attr.field_mask =
    UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;

  ucs_status_t status =
    ucp_conn_request_query(conn_request.conn_request, &conn_req_attr);
  if (status != UCS_OK) {
    lderr(cct) << "ucp_conn_request_query() failed: "
               << ucs_status_string(status)
               << dendl;
  }

  ucp_ep_params_t ep_params;
  ep_params.field_mask = UCP_EP_PARAM_FIELD_ERR_HANDLER |
                         UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
                         UCP_EP_PARAM_FIELD_CONN_REQUEST;
  ep_params.conn_request = conn_request.conn_request;
  ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
  ep_params.err_handler.cb = ep_error_cb;
  ep_params.err_handler.arg = reinterpret_cast<void*>(this);

  status = ucp_ep_create(ucp_worker_engine->get_ucp_worker(),
                         &ep_params, &conn_ep);
  if (status != UCS_OK) {
    ceph_assert(conn_ep == NULL);
    lderr(cct) << "ucp_ep_create() failed : "
               << ucs_status_string(status)
	       << dendl;
    handle_connection_error(status);
    return status;
  }

  conn_id = reinterpret_cast<uint64_t>(conn_ep);
  ucp_worker_engine->add_connections(conn_id, this);
  return UCS_OK;
}

void UCXConSktImpl::data_notify()
{
  eventfd_t data_event_notify = 0x1;
  int rst = eventfd_write(data_event_fd, data_event_notify);
  ceph_assert(rst == 0);
}

int UCXConSktImpl::client_start_connect(const entity_addr_t &server_addr,
                                        const SocketOptions &opts)
{
  struct sockaddr_in connect_addr = server_addr.u.sin;
  char sockaddr_str[UCS_SOCKADDR_STRING_LEN] = {0};
  ldout(cct, 20) << "Connecting to "
                 << ucs_sockaddr_str((struct sockaddr *)&connect_addr,
                                     sockaddr_str,
                                     UCS_SOCKADDR_STRING_LEN)
                 << dendl;

  ucp_ep_params_t ep_params;
  ep_params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
  ep_params.field_mask = UCP_EP_PARAM_FIELD_FLAGS |
                         UCP_EP_PARAM_FIELD_SOCK_ADDR |
                         UCP_EP_PARAM_FIELD_ERR_HANDLER |
                         UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
  ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
  ep_params.err_handler.cb = ep_error_cb;
  ep_params.err_handler.arg = reinterpret_cast<void*>(this);
  ep_params.sockaddr.addr = (struct sockaddr *)&connect_addr;
  ep_params.sockaddr.addrlen = sizeof(connect_addr);

  ucs_status_t
  status = ucp_ep_create(ucp_worker_engine->get_ucp_worker(),
                         &ep_params, &conn_ep);
  if (status != UCS_OK) {
    ceph_assert(conn_ep == NULL);
    lderr(cct) << "ucp_ep_create() failed : "
               << ucs_status_string(status)
               << dendl;
    handle_connection_error(status);
    return status;
  } else {
    set_connection_status(1);
  }

  conn_id = reinterpret_cast<uint64_t>(conn_ep);
  ucp_worker_engine->add_connections(conn_id, this);
  return UCS_OK;
}
