// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Ming Lei <ming.lei@clyso.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_COMMON_WAKEUPFD_H
#define CEPH_COMMON_WAKEUPFD_H

#include <cerrno>
#include <cstdint>
#include <poll.h>
#include <unistd.h>
#include <sys/eventfd.h>

#include "include/ceph_assert.h"
#include "include/compat.h"

/// A pollable wakeup primitive wrapping an eventfd: notify() makes
/// fd() readable until the counter is consumed.  Semantics:
///
///  - notify() is thread-safe, cheap, and COALESCING: any number of
///    notifies between two consumes yield one readable event.  The
///    counter is incremented by the kernel at notify time,
///    independently of any reader, so a notify can never be lost -
///    an fd left unconsumed stays readable (sticky), which makes a
///    "wake written before the waiter even started waiting" work.
///  - consume() drains the counter; wait_and_consume() blocks first.
///    Single consumer: concurrent consume()/wait_and_consume() from
///    multiple threads is not supported (a wake could be swallowed
///    by the wrong consumer).
///    After consume() the caller must drain whatever queues the
///    notifies advertised: the counter carries no payload.
///  - fd() is a GENUINE eventfd, so beyond poll/epoll/select it may
///    be handed to kernel-side signalling APIs that require one
///    (io_set_eventfd()/IOCB_FLAG_RESFD for libaio,
///    io_uring_register_eventfd()); the kernel then acts as another
///    notifier of the same fd.
///
/// eventfd(2) is native on every platform this code builds for
/// (linux; freebsd >= 13).  If an eventfd-less platform ever needs
/// this, a self-pipe fallback belongs here and nowhere else - but
/// note only userspace notification can be emulated that way; the
/// kernel-side signalling APIs above require a real eventfd.
class WakeupFd {
public:
  WakeupFd() {
    fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    ceph_assert(fd_ >= 0);
  }
  ~WakeupFd() {
    if (fd_ >= 0) {
      VOID_TEMP_FAILURE_RETRY(::close(fd_));
    }
  }
  WakeupFd(const WakeupFd&) = delete;
  WakeupFd& operator=(const WakeupFd&) = delete;

  int fd() const {
    return fd_;
  }

  void notify() {
    uint64_t one = 1;
    ssize_t r = TEMP_FAILURE_RETRY(::write(fd_, &one, sizeof(one)));
    // EAGAIN means the counter is at max, i.e. the fd is already
    // readable - which is all a notify has to guarantee
    ceph_assert(r == sizeof(one) || (r < 0 && errno == EAGAIN));
  }

  /// nonblocking drain; true if any notify was pending
  bool consume() {
    uint64_t v;
    ssize_t r = TEMP_FAILURE_RETRY(::read(fd_, &v, sizeof(v)));
    if (r < 0) {
      ceph_assert(errno == EAGAIN);  // nothing pending (fd is nonblocking)
      return false;
    }
    ceph_assert(r == sizeof(v));
    return true;
  }

  /// block until notified (fd is nonblocking, so poll then drain)
  void wait_and_consume() {
    while (!consume()) {
      struct pollfd pfd = { .fd = fd_, .events = POLLIN, .revents = 0 };
      int r = TEMP_FAILURE_RETRY(::poll(&pfd, 1, -1));
      ceph_assert(r >= 0);
    }
  }

private:
  int fd_ = -1;
};

#endif
