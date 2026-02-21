// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corporation
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <string>
#include <string_view>

#include <seastar/core/future.hh>

#include "include/buffer.h"
#include "include/types.h"
#include "common/ceph_time.h"

namespace crimson::osdc {
class Objecter;
}

namespace crimson::client {

/**
 * Pool-scoped I/O context. Delegates read, write, stat to Objecter
 * with object_locator_t built from pool_id.
 */
class IoCtx {
public:
  IoCtx(crimson::osdc::Objecter& objecter,
        int64_t pool_id,
        snapid_t snap_seq = CEPH_NOSNAP);

  seastar::future<ceph::bufferlist> read(const std::string& oid,
                                         uint64_t off,
                                         uint64_t len);

  seastar::future<> write(const std::string& oid,
                          uint64_t off,
                          ceph::bufferlist&& bl);

  seastar::future<std::pair<uint64_t, ceph::real_time>> stat(
      const std::string& oid);

  seastar::future<> discard(const std::string& oid,
                            uint64_t off,
                            uint64_t len);

  seastar::future<> write_zeroes(const std::string& oid,
                                 uint64_t off,
                                 uint64_t len);

  seastar::future<> compare_and_write(const std::string& oid,
                                      uint64_t cmp_off,
                                      ceph::bufferlist&& cmp_bl,
                                      uint64_t write_off,
                                      ceph::bufferlist&& write_bl);

  /// Execute cls method on object. Returns output bufferlist.
  seastar::future<ceph::bufferlist> exec(const std::string& oid,
                                        std::string_view cname,
                                        std::string_view method,
                                        ceph::bufferlist&& indata);

  int64_t get_pool_id() const { return pool_id; }
  snapid_t get_snap_seq() const { return snap_seq; }

private:
  crimson::osdc::Objecter& objecter;
  int64_t pool_id;
  snapid_t snap_seq;
};

} // namespace crimson::client
