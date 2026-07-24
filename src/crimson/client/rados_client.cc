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

#include "crimson/client/rados_client.h"

#include <cerrno>
#include <system_error>

#include "crimson/common/log.h"
#include "crimson/osdc/objecter.h"
#include "osd/OSDMap.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_client);
  }
}

namespace crimson::client {

RadosClient::RadosClient(crimson::net::Messenger& msgr,
                         crimson::mon::Client& monc)
  : msgr(msgr),
    monc(monc),
    objecter(std::make_unique<crimson::osdc::Objecter>(msgr, monc))
{}

RadosClient::~RadosClient() = default;

crimson::osdc::Objecter& RadosClient::get_objecter()
{
  return *objecter;
}

seastar::future<> RadosClient::connect()
{
  logger().info("RadosClient::connect");
  return objecter->start();
}

seastar::future<IoCtx> RadosClient::create_ioctx(std::string_view pool_name)
{
  logger().debug("RadosClient::create_ioctx pool_name={}", pool_name);
  return objecter->wait_for_osdmap()
    .then([this, pool_name = std::string(pool_name)] {
      return objecter->with_osdmap(
        [pool_name](const OSDMap& o) {
          return o.lookup_pg_pool_name(pool_name);
        });
    })
    .then([this](int64_t pool_id) {
      if (pool_id < 0) {
        return seastar::make_exception_future<IoCtx>(
          std::system_error(-pool_id, std::system_category(), "pool not found"));
      }
      logger().debug("RadosClient::create_ioctx pool_id={}", pool_id);
      return seastar::make_ready_future<IoCtx>(IoCtx(*objecter, pool_id));
    });
}

seastar::future<IoCtx> RadosClient::create_ioctx(int64_t pool_id)
{
  logger().debug("RadosClient::create_ioctx pool_id={}", pool_id);
  return objecter->wait_for_osdmap()
    .then([this, pool_id] {
      return objecter->with_osdmap(
        [pool_id](const OSDMap& o) {
          return o.have_pg_pool(pool_id);
        });
    })
    .then([this, pool_id](bool exists) {
      if (!exists) {
        return seastar::make_exception_future<IoCtx>(
          std::system_error(ENOENT, std::system_category(), "pool not found"));
      }
      return seastar::make_ready_future<IoCtx>(IoCtx(*objecter, pool_id));
    });
}

seastar::future<> RadosClient::shutdown()
{
  logger().info("RadosClient::shutdown");
  return objecter->stop();
}

} // namespace crimson::client
