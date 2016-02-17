// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_USERSPACEEVENT_H
#define CEPH_USERSPACEEVENT_H

#include <errno.h>
#include <vector>

#include "common/Tub.h"

class UserspaceEventManager {
  struct UserspaceFDImpl {
    uint32_t waiting_idx = 0;
    int16_t read_errno = 0;
    int16_t write_errno = 0;
    int8_t listening_mask = 0;
    int8_t activating_mask = 0;
  };
  int max_fd = 0;
  uint32_t max_wait_idx = 0;
  uint32_t waiting_size = 0;
  std::vector<Tub<UserspaceFDImpl> > fds;
  std::vector<int> waiting_fds;
  std::list<uint32_t> unused_fds;

 public:
  UserspaceEventManager(){
    waiting_fds.resize(1024);
  }

  int get_eventfd() {
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
    return fd;
  }

  int listen(int fd, int mask) {
    if ((size_t)fd > fds.size())
      return -ENOENT;

    Tub<UserspaceFDImpl> &impl = fds[fd];
    if (!impl)
      return -ENOENT;

    assert(mask);
    impl->listening_mask |= mask;
    if (impl->activating_mask & impl->listening_mask) {
      if (waiting_fds.size() >= max_wait_idx)
        waiting_fds.resize(waiting_fds.size()*2);
      impl->waiting_idx = ++max_wait_idx;
      waiting_fds[max_wait_idx] = fd;
      ++waiting_size;
    }
    return 0;
  }

  int unlisten(int fd, int mask) {
    if ((size_t)fd > fds.size())
      return -ENOENT;

    Tub<UserspaceFDImpl> &impl = fds[fd];
    if (!impl)
      return -ENOENT;

    assert(mask);
    impl->listening_mask = impl->listening_mask & (~mask);
    if (impl->activating_mask & impl->listening_mask) {
      assert(impl->waiting_idx);
      return 0;
    }
    if (impl->waiting_idx) {
      if (waiting_fds[max_wait_idx] == fd)
        --max_wait_idx;
      waiting_fds[fd] = -1;
      --waiting_size;
    }
    return 0;
  }

  int notify(int fd, int mask = EVENT_READABLE) {
    if ((size_t)fd > fds.size())
      return -ENOENT;

    Tub<UserspaceFDImpl> &impl = fds[fd];
    if (!impl)
      return -ENOENT;

    if (mask & impl->activating_mask) {
      assert(impl->waiting_idx);
      return 0;
    }

    if (impl->listening_mask & mask) {
      impl->waiting_idx = ++max_wait_idx;
      waiting_fds[max_wait_idx] = fd;
      ++waiting_size;
    }
    impl->activating_mask |= mask;
    return 0;
  }

  void close(int fd) {
    if ((size_t)fd > fds.size())
      return ;

    Tub<UserspaceFDImpl> &impl = fds[fd];
    if (!impl)
      return ;

    if (fd == max_fd)
      --max_fd;
    else
      unused_fds.push_back(fd);

    if (impl->activating_mask) {
      assert(impl->waiting_idx);
    if (waiting_fds[max_wait_idx] == fd)
      --max_wait_idx;
      waiting_fds[fd] = -1;
      --waiting_size;
    }
    impl.destroy();
  }

  int poll(int *events, int *masks, int num_events, struct timeval *tp) {
    int fd;
    uint32_t i, min_events = MIN(num_events, waiting_size);
    // leave zero slot for waiting_fds
    for (i = 0; i < min_events; ++i) {
      fd = waiting_fds[i+1];
      if (fd == -1)
        continue;

      events[i] = fd;
      assert(fds[fd]);
      masks[i] = fds[fd]->listening_mask & fds[fd]->activating_mask;
      assert(masks[i]);
    }
    if (i < waiting_size)
      memcpy(&waiting_fds[1], &waiting_fds[i+1], sizeof(int)*(waiting_size-i));
    max_wait_idx -= i;
    waiting_size -= i;
    return i;
  }
};

#endif //CEPH_USERSPACEEVENT_H
