// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "include/rados/librados_fwd.hpp"
#include <memory>
#include "common/ceph_mutex.h"
#include "services/svc_rados.h"
#include "rgw_aio.h"

namespace rgw {

// a throttle for aio operations that enforces a maximum window on outstanding
// bytes. only supports a single waiter, so all public functions must be called
// from the same thread
class AioThrottle : public Aio {
 protected:
  const uint64_t window;
  uint64_t pending_size = 0;

  bool is_available() const { return pending_size <= window; }
  bool has_completion() const { return !completed.empty(); }
  bool is_drained() const { return pending.empty(); }

  struct Pending : AioResultEntry {
    AioThrottle *parent = nullptr;
    uint64_t cost = 0;
    librados::AioCompletion *completion = nullptr;
  };
  OwningList<Pending> pending;
  AioResultList completed;

  enum class Wait { None, Available, Completion, Drained };
  Wait waiter = Wait::None;

  bool waiter_ready() const;

  ceph::mutex mutex = ceph::make_mutex("AioThrottle");
  ceph::condition_variable cond;

  void get(Pending& p);
  void put(Pending& p);

  static void aio_cb(void *cb, void *arg);

 public:
  AioThrottle(uint64_t window) : window(window) {}

  virtual ~AioThrottle() {
    // must drain before destructing
    ceph_assert(pending.empty());
    ceph_assert(completed.empty());
  }

  AioResultList submit(RGWSI_RADOS::Obj& obj,
                       librados::ObjectReadOperation *op,
                       uint64_t cost, uint64_t id) override;

  AioResultList submit(RGWSI_RADOS::Obj& obj,
                       librados::ObjectWriteOperation *op,
                       uint64_t cost, uint64_t id) override;

  AioResultList poll() override;

  AioResultList wait() override;

  AioResultList drain() override;
};

} // namespace rgw
