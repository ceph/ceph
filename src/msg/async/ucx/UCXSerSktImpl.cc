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
                             entity_addr_t& ser_addr, unsigned addr_slot)
  : ServerSocketImpl(ser_addr.get_type(), addr_slot),
    cct(cct), ucx_worker(ucx_worker)
{
}

int UCXSerSktImpl::listen(entity_addr_t &listen_addr, const SocketOptions &skt_opts)
{
  int rst = 0;

  return rst;
}

int UCXSerSktImpl::accept(ConnectedSocket *peer_socket, const SocketOptions &opt,
                          entity_addr_t *peer_addr, Worker *ucx_worker)
{
  ldout(cct, 15) << dendl;

  int rst = 0;

  return rst;
}

void UCXSerSktImpl::abort_accept()
{
  if (listen_socket >= 0) {
    ::close(listen_socket);
  }
}

int UCXSerSktImpl::fd() const
{
  return listen_socket;
}
