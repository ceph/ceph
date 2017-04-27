// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "UserspaceEvent.h"

#include "common/dout.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_dpdk
#undef dout_prefix
#define dout_prefix *_dout << "dpdk "

int UserspaceEventManager::get_eventfd()
{
  int fd;
  if (!unused_fds.empty()) {
    fd = unused_fds.front();
    unused_fds.pop_front();
  } else {
    fd = ++max_fd;
    fds.resize(fd + 1);
  }

  Tub<UserspaceFDImpl> &impl = fds[fd];
  assert(!impl);
  impl.construct();
  ldout(cct, 20) << __func__ << " fd=" << fd << dendl;
  return fd;
}

int UserspaceEventManager::notify(int fd, int mask)
{
  ldout(cct, 20) << __func__ << " fd=" << fd << " mask=" << mask << dendl;
  if ((size_t)fd >= fds.size())
    return -ENOENT;

  Tub<UserspaceFDImpl> &impl = fds[fd];
  if (!impl)
    return -ENOENT;

  ldout(cct, 20) << __func__ << " activing=" << int(impl->activating_mask)
                 << " listening=" << int(impl->listening_mask)
                 << " waiting_idx=" << int(impl->waiting_idx) << dendl;

  impl->activating_mask |= mask;
  if (impl->waiting_idx)
    return 0;

  if (impl->listening_mask & mask) {
    if (waiting_fds.size() <= max_wait_idx)
      waiting_fds.resize(waiting_fds.size()*2);
    impl->waiting_idx = ++max_wait_idx;
    waiting_fds[max_wait_idx] = fd;
  }

  ldout(cct, 20) << __func__ << " activing=" << int(impl->activating_mask)
                 << " listening=" << int(impl->listening_mask)
                 << " waiting_idx=" << int(impl->waiting_idx) << " done " << dendl;
  return 0;
}

void UserspaceEventManager::close(int fd)
{
  ldout(cct, 20) << __func__ << " fd=" << fd << dendl;
  if ((size_t)fd >= fds.size())
    return ;

  Tub<UserspaceFDImpl> &impl = fds[fd];
  if (!impl)
    return ;

  if (fd == max_fd)
    --max_fd;
  else
    unused_fds.push_back(fd);

  if (impl->activating_mask) {
    if (waiting_fds[max_wait_idx] == fd) {
      assert(impl->waiting_idx == max_wait_idx);
      --max_wait_idx;
    }
    waiting_fds[impl->waiting_idx] = -1;
  }
  impl.destroy();
}

int UserspaceEventManager::poll(int *events, int *masks, int num_events, struct timeval *tp)
{
  int fd;
  uint32_t i = 0;
  int count = 0;
  assert(num_events);
  // leave zero slot for waiting_fds
  while (i < max_wait_idx) {
    fd = waiting_fds[++i];
    if (fd == -1)
      continue;

    events[count] = fd;
    Tub<UserspaceFDImpl> &impl = fds[fd];
    assert(impl);
    masks[count] = impl->listening_mask & impl->activating_mask;
    assert(masks[count]);
    ldout(cct, 20) << __func__ << " fd=" << fd << " mask=" << masks[count] << dendl;
    impl->activating_mask &= (~masks[count]);
    impl->waiting_idx = 0;
    if (++count >= num_events)
      break;
  }
  if (i < max_wait_idx) {
    memmove(&waiting_fds[1], &waiting_fds[i+1], sizeof(int)*(max_wait_idx-i));
  }
  max_wait_idx -= i;
  return count;
}
