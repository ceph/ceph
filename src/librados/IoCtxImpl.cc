// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "IoCtxImpl.h"

#include "librados/AioCompletionImpl.h"
#include "librados/RadosClient.h"

#define DOUT_SUBSYS rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

librados::IoCtxImpl::IoCtxImpl()
  : aio_write_list_lock("librados::IoCtxImpl::aio_write_list_lock")
{
}

librados::IoCtxImpl::IoCtxImpl(RadosClient *c, int pid, const char *pool_name_, snapid_t s)
  : ref_cnt(0), client(c), poolid(pid),
  pool_name(pool_name_), snap_seq(s), assert_ver(0),
  notify_timeout(c->cct->_conf->client_notify_timeout), oloc(pid),
  aio_write_list_lock("librados::IoCtxImpl::aio_write_list_lock"), aio_write_seq(0)
{
}

void librados::IoCtxImpl::set_snap_read(snapid_t s)
{
  if (!s)
    s = CEPH_NOSNAP;
  ldout(client->cct, 10) << "set snap read " << snap_seq << " -> " << s << dendl;
  snap_seq = s;
}

int librados::IoCtxImpl::set_snap_write_context(snapid_t seq, vector<snapid_t>& snaps)
{
  ::SnapContext n;
  ldout(client->cct, 10) << "set snap write context: seq = " << seq << " and snaps = " << snaps << dendl;
  n.seq = seq;
  n.snaps = snaps;
  if (!n.is_valid())
    return -EINVAL;
  snapc = n;
  return 0;
}

void librados::IoCtxImpl::queue_aio_write(AioCompletionImpl *c)
{
  get();
  aio_write_list_lock.Lock();
  assert(!c->io);
  c->io = this;
  c->aio_write_seq = ++aio_write_seq;
  aio_write_list.push_back(&c->aio_write_list_item);
  aio_write_list_lock.Unlock();
}

void librados::IoCtxImpl::complete_aio_write(AioCompletionImpl *c)
{
  aio_write_list_lock.Lock();
  assert(c->io == this);
  c->io = NULL;
  c->aio_write_list_item.remove_myself();
  aio_write_cond.Signal();
  aio_write_list_lock.Unlock();
  put();
}

void librados::IoCtxImpl::flush_aio_writes()
{
  aio_write_list_lock.Lock();
  tid_t seq = aio_write_seq;
  while (!aio_write_list.empty() &&
	 aio_write_list.front()->aio_write_seq <= seq)
    aio_write_cond.Wait(aio_write_list_lock);
  aio_write_list_lock.Unlock();
}
