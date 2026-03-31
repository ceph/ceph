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

#include <memory>
#include <string>
#include <string_view>

#include <seastar/core/future.hh>

#include "crimson/client/io_context.h"

namespace crimson::mon {
class Client;
}

namespace crimson::net {
class Messenger;
}

namespace crimson::osdc {
class Objecter;
}

namespace crimson::client {

class IoCtx;

/**
 * RADOS client facade. Holds Messenger, MonClient, Objecter.
 * Provides connect(), create_ioctx(pool_name), shutdown().
 *
 * Usage:
 *   auto auth_handler = std::make_unique<DummyAuthHandler>();
 *   auto msgr = crimson::net::Messenger::create(entity_name_t::CLIENT(), ...);
 *   crimson::mon::Client monc{*msgr, *auth_handler};
 *   RadosClient client(*msgr, monc);
 *
 *   msgr->set_auth_client(&monc);
 *   msgr->start({&monc, &client.get_objecter()});
 *   monc.start().get();
 *   client.connect().get();
 *
 *   auto ioctx = client.create_ioctx("rbd").get();
 *   co_await ioctx.write("obj", 0, std::move(bl));
 *   auto data = co_await ioctx.read("obj", 0, 4096);
 *
 *   client.shutdown().get();
 */
class RadosClient {
public:
  RadosClient(crimson::net::Messenger& msgr,
              crimson::mon::Client& monc);

  ~RadosClient();

  /// Objecter reference for adding to Messenger dispatchers before start.
  crimson::osdc::Objecter& get_objecter();

  /// Start Objecter (subscribe to osdmap). Call after msgr.start() and monc.start().
  seastar::future<> connect();

  /// Create pool-scoped IoCtx by pool name. Waits for OSDMap, then looks up pool.
  /// Fails with -ENOENT if pool not found.
  seastar::future<IoCtx> create_ioctx(std::string_view pool_name);

  /// Create pool-scoped IoCtx by pool id. Validates pool exists in OSDMap.
  seastar::future<IoCtx> create_ioctx(int64_t pool_id);

  /// Stop Objecter.
  seastar::future<> shutdown();

private:
  crimson::net::Messenger& msgr;
  crimson::mon::Client& monc;
  std::unique_ptr<crimson::osdc::Objecter> objecter;
};

} // namespace crimson::client
