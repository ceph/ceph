// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright(c) 2021 Liu, Changcheng <changcheng.liu@aliyun.com>
 *
 */

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

UCXConSktImpl::UCXConSktImpl(CephContext *cct, UCXWorker *ucx_worker)
  : cct(cct), ucx_worker(ucx_worker),
    is_server(false), active(false), pending(false)
{
}

UCXConSktImpl::~UCXConSktImpl()
{
  ldout(cct, 20) << dendl;
  ucx_worker->remove_pending_conn(this);

  std::lock_guard l{lock};
}

int UCXConSktImpl::is_connected()
{
  return connected == 1;
}

ssize_t UCXConSktImpl::read(char* buf, size_t len)
{
  return 0;
}

ssize_t UCXConSktImpl::send(ceph::bufferlist &bl, bool more)
{
  return 0;
}

void UCXConSktImpl::shutdown()
{
}

void UCXConSktImpl::close()
{
  active = false;
}

int UCXConSktImpl::fd() const
{
  return event_fd;
}
