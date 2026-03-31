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

#include "crimson/client/io_context.h"

#include "crimson/common/log.h"
#include "crimson/osdc/objecter.h"
#include "include/object.h"
#include "osd/osd_types.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_client);
  }
}

namespace crimson::client {

IoCtx::IoCtx(crimson::osdc::Objecter& objecter,
             int64_t pool_id,
             snapid_t snap_seq)
  : objecter(objecter), pool_id(pool_id), snap_seq(snap_seq)
{}

seastar::future<ceph::bufferlist> IoCtx::read(const std::string& oid,
                                              uint64_t off,
                                              uint64_t len)
{
  logger().debug("IoCtx::read oid={} off={} len={}", oid, off, len);
  object_locator_t oloc(pool_id);
  return objecter.read(object_t(oid), oloc, off, len, snap_seq);
}

seastar::future<> IoCtx::write(const std::string& oid,
                               uint64_t off,
                               ceph::bufferlist&& bl)
{
  logger().debug("IoCtx::write oid={} off={} len={}", oid, off, bl.length());
  object_locator_t oloc(pool_id);
  return objecter.write(object_t(oid), oloc, off, std::move(bl), snap_seq);
}

seastar::future<std::pair<uint64_t, ceph::real_time>> IoCtx::stat(
    const std::string& oid)
{
  logger().debug("IoCtx::stat oid={}", oid);
  object_locator_t oloc(pool_id);
  return objecter.stat(object_t(oid), oloc, snap_seq);
}

seastar::future<> IoCtx::discard(const std::string& oid,
                                  uint64_t off,
                                  uint64_t len)
{
  logger().debug("IoCtx::discard oid={} off={} len={}", oid, off, len);
  object_locator_t oloc(pool_id);
  return objecter.discard(object_t(oid), oloc, off, len, snap_seq);
}

seastar::future<> IoCtx::write_zeroes(const std::string& oid,
                                      uint64_t off,
                                      uint64_t len)
{
  logger().debug("IoCtx::write_zeroes oid={} off={} len={}", oid, off, len);
  object_locator_t oloc(pool_id);
  return objecter.write_zeroes(object_t(oid), oloc, off, len, snap_seq);
}

seastar::future<> IoCtx::compare_and_write(const std::string& oid,
                                           uint64_t cmp_off,
                                           ceph::bufferlist&& cmp_bl,
                                           uint64_t write_off,
                                           ceph::bufferlist&& write_bl)
{
  logger().debug("IoCtx::compare_and_write oid={} cmp_off={} write_off={}",
                 oid, cmp_off, write_off);
  object_locator_t oloc(pool_id);
  return objecter.compare_and_write(object_t(oid), oloc,
                                    cmp_off, std::move(cmp_bl),
                                    write_off, std::move(write_bl),
                                    snap_seq);
}

seastar::future<ceph::bufferlist> IoCtx::exec(const std::string& oid,
                                              std::string_view cname,
                                              std::string_view method,
                                              ceph::bufferlist&& indata)
{
  logger().debug("IoCtx::exec oid={} cls={}.{}", oid, cname, method);
  object_locator_t oloc(pool_id);
  return objecter.exec(object_t(oid), oloc, cname, method, std::move(indata),
                      snap_seq);
}

} // namespace crimson::client
