// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <boost/asio/any_io_executor.hpp>
#include "librados/librados_asio.h"
#include "librados/redirect_version.h"
#include "cls/lock/cls_lock_client.h"
#include "lease.h"

namespace ceph::async {

/// A LockClient for with_lease() based on librados and cls_lock.
class RadosLockClient : public LockClient {
 public:
  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const { return ex; }

  RadosLockClient(executor_type ex,
                  librados::IoCtx ioctx,
                  std::string oid,
                  rados::cls::lock::Lock lock,
                  bool ephemeral)
    : ex(std::move(ex)),
      ioctx(std::move(ioctx)),
      oid(std::move(oid)),
      lock(std::move(lock)),
      ephemeral(ephemeral)
  {}

 private:
  executor_type ex;
  librados::IoCtx ioctx;
  std::string oid;
  rados::cls::lock::Lock lock;
  bool ephemeral = false;

  void acquire(ceph::timespan dur, Handler h) override {
    librados::ObjectWriteOperation op;
    lock.set_duration(dur);
    if (ephemeral) {
      lock.lock_exclusive_ephemeral(&op);
    } else {
      lock.lock_exclusive(&op);
    }
    librados::async_operate(*this, ioctx, oid, &op, 0, nullptr,
                            librados::redirect_version(std::move(h)));
  }
  void renew(ceph::timespan dur, Handler h) override {
    librados::ObjectWriteOperation op;
    op.assert_exists();
    lock.set_must_renew(true);
    lock.set_duration(dur);
    if (ephemeral) {
      lock.lock_exclusive_ephemeral(&op);
    } else {
      lock.lock_exclusive(&op);
    }
    librados::async_operate(*this, ioctx, oid, &op, 0, nullptr,
                            librados::redirect_version(std::move(h)));
  }
  void release(Handler h) override {
    librados::ObjectWriteOperation op;
    op.assert_exists();
    lock.unlock(&op);
    librados::async_operate(*this, ioctx, oid, &op, 0, nullptr,
                            librados::redirect_version(std::move(h)));
  }
};

} // namespace ceph::async
