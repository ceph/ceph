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
    uint32_t magic = 4921;
  };
  int max_fd = 0;
  uint32_t max_wait_idx = 0;
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
    if ((size_t)fd >= fds.size())
      return -ENOENT;

    Tub<UserspaceFDImpl> &impl = fds[fd];
    if (!impl)
      return -ENOENT;

    impl->listening_mask |= mask;
    if (impl->activating_mask & impl->listening_mask && !impl->waiting_idx) {
      if (waiting_fds.size() <= max_wait_idx)
        waiting_fds.resize(waiting_fds.size()*2);
      impl->waiting_idx = ++max_wait_idx;
      waiting_fds[max_wait_idx] = fd;
    }
    return 0;
  }

  int unlisten(int fd, int mask) {
    if ((size_t)fd >= fds.size())
      return -ENOENT;

    Tub<UserspaceFDImpl> &impl = fds[fd];
    if (!impl)
      return -ENOENT;

    impl->listening_mask &= (~mask);
    if (!(impl->activating_mask & impl->listening_mask) && impl->waiting_idx) {
      if (waiting_fds[max_wait_idx] == fd) {
        assert(impl->waiting_idx == max_wait_idx);
        --max_wait_idx;
      }
      waiting_fds[impl->waiting_idx] = -1;
      impl->waiting_idx = 0;
    }
    return 0;
  }

  int notify(int fd, int mask) {
    if ((size_t)fd >= fds.size())
      return -ENOENT;

    Tub<UserspaceFDImpl> &impl = fds[fd];
    if (!impl)
      return -ENOENT;

    impl->activating_mask |= mask;
    if (impl->waiting_idx)
      return 0;

    if (impl->listening_mask & mask) {
      if (waiting_fds.size() <= max_wait_idx)
        waiting_fds.resize(waiting_fds.size()*2);
      impl->waiting_idx = ++max_wait_idx;
      waiting_fds[max_wait_idx] = fd;
    }
    return 0;
  }

  void close(int fd) {
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

  int poll(int *events, int *masks, int num_events, struct timeval *tp) {
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

  bool check() {
    for (auto &&m : fds) {
      if (m && m->magic != 4921)
        return false;
    }
    return true;
  }
};

#endif //CEPH_USERSPACEEVENT_H
