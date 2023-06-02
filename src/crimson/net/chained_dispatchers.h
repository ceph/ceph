// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/smp.hh>

#include "Fwd.h"
#include "crimson/common/log.h"

namespace crimson::net {

class Dispatcher;

class ChainedDispatchers {
public:
  void assign(const dispatchers_t& _dispatchers) {
    assert(empty());
    assert(!_dispatchers.empty());
    dispatchers = _dispatchers;
  }
  void clear() {
    dispatchers.clear();
  }
  bool empty() const {
    return dispatchers.empty();
  }
  seastar::future<> ms_dispatch(crimson::net::ConnectionRef, MessageRef);
  void ms_handle_accept(crimson::net::ConnectionRef conn, seastar::shard_id);
  void ms_handle_connect(crimson::net::ConnectionRef conn, seastar::shard_id);
  void ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace);
  void ms_handle_remote_reset(crimson::net::ConnectionRef conn);

 private:
  dispatchers_t dispatchers;
};

}
