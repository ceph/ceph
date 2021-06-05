// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright(c) 2021 Liu, Changcheng <changcheng.liu@aliyun.com>
 *
 */

#include <poll.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "include/str_list.h"
#include "include/compat.h"
#include "common/Cycles.h"
#include "common/deleter.h"
#include "UCXStack.h"

#define FN_NAME (__CEPH_ASSERT_FUNCTION == nullptr ? __func__ : __CEPH_ASSERT_FUNCTION)
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ucx_msg
#undef dout_prefix
#define dout_prefix _prefix(_dout, FN_NAME)
static ostream& _prefix(std::ostream *_dout,
                        std::string_view func_name)
{
  return *_dout << "UCXStack: " << func_name << ": ";
}

UCXWorker::UCXWorker(CephContext *cct, unsigned worker_id)
  : Worker(cct, worker_id)
{
}

UCXWorker::~UCXWorker()
{
}

int UCXWorker::listen(entity_addr_t &listen_addr, unsigned addr_slot,
                      const SocketOptions &ser_opts, ServerSocket *ser_skt)
{
  int rst = 0;

  auto ucx_serskt = new UCXSerSktImpl(cct, this, listen_addr, addr_slot);
  rst = ucx_serskt->listen(listen_addr, ser_opts);
  if (rst < 0) {
    lderr(cct) << "listen" << listen_addr << " failed." << dendl;
    delete ucx_serskt;
    return rst;
  }

  *ser_skt = ServerSocket(std::unique_ptr<ServerSocketImpl>(ucx_serskt));
  return rst;
}

int UCXWorker::connect(const entity_addr_t &peer_addr,
                       const SocketOptions &peer_opts,
                       ConnectedSocket *peer_skt)
{
  int rst = 0;

  auto ucx_peerskt = new UCXConSktImpl(cct, this);

  if (rst < 0) {
    lderr(cct) << "connect" << peer_addr << " failed." << dendl;
    delete ucx_peerskt;
    return rst;
  }

  *peer_skt = ConnectedSocket(std::unique_ptr<UCXConSktImpl>(ucx_peerskt));
  return rst;
}

void UCXWorker::destroy()
{
}

void UCXWorker::initialize()
{
}

UCXStack::UCXStack(CephContext *cct)
  : NetworkStack(cct)
{
  ldout(cct, 20) << "creating UCXStack: " << this << dendl;
}

UCXStack::~UCXStack()
{
}

Worker* UCXStack::create_worker(CephContext *cct, unsigned worker_id)
{
  auto ucx_worker = new UCXWorker(cct, worker_id);
  return ucx_worker;
}

void UCXStack::spawn_worker(std::function<void ()> &&func)
{
}

void UCXStack::join_worker(unsigned idx)
{
}
