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

#include <cstddef>
#include <errno.h>
#include <string.h>

#include <vector>
#include <list>

#include "include/assert.h"
#include "include/int_types.h"
#include "common/Tub.h"

class CephContext;

class UserspaceEventManager {
  struct UserspaceFDImpl {
    uint32_t waiting_idx = 0;
    int16_t read_errno = 0;
    int16_t write_errno = 0;
    int8_t listening_mask = 0;
    int8_t activating_mask = 0;
    uint32_t magic = 4921;
  };
  CephContext *cct;
  int max_fd = 0;
  uint32_t max_wait_idx = 0;
  std::vector<Tub<UserspaceFDImpl> > fds;
  std::vector<int> waiting_fds;
  std::list<uint32_t> unused_fds;

 public:
  UserspaceEventManager(CephContext *c): cct(c) {
    waiting_fds.resize(1024);
  }

  int get_eventfd();

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

  int notify(int fd, int mask);
  void close(int fd);
  int poll(int *events, int *masks, int num_events, struct timeval *tp);

  bool check() {
    for (auto &&m : fds) {
      if (m && m->magic != 4921)
        return false;
    }
    return true;
  }
};

#endif //CEPH_USERSPACEEVENT_H
