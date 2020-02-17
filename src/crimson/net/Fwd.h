// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

#include "msg/Connection.h"
#include "msg/MessageRef.h"
#include "msg/msg_types.h"

using auth_proto_t = int;

class AuthConnectionMeta;
using AuthConnectionMetaRef = seastar::lw_shared_ptr<AuthConnectionMeta>;

namespace crimson::net {

using msgr_tag_t = uint8_t;
using stop_t = seastar::stop_iteration;

class Connection;
using ConnectionRef = seastar::shared_ptr<Connection>;
// NOTE: ConnectionXRef should only be used in seastar world, because
// lw_shared_ptr<> is not safe to be accessed by unpinned alien threads.
using ConnectionXRef = seastar::lw_shared_ptr<seastar::foreign_ptr<ConnectionRef>>;

class Dispatcher;

class Messenger;

template <typename T, typename... Args>
seastar::future<T*> create_sharded(Args... args) {
  // seems we should only construct/stop shards on #0
  return seastar::smp::submit_to(0, [=] {
    auto sharded_obj = seastar::make_lw_shared<seastar::sharded<T>>();
    return sharded_obj->start(args...).then([sharded_obj]() {
      seastar::engine().at_exit([sharded_obj]() {
          return sharded_obj->stop().finally([sharded_obj] {});
        });
      return sharded_obj.get();
    });
  }).then([] (seastar::sharded<T> *ptr_shard) {
    // return the pointer valid for the caller CPU
    return &ptr_shard->local();
  });
}

} // namespace crimson::net
