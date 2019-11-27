// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/shared_ptr.hh>

#include "crimson/net/Connection.h"

namespace crimson::osd {

// NOTE: really need to have this public. Otherwise `shared_from_this()`
// will abort. According to cppreference.com:
//
//   "The constructors of std::shared_ptr detect the presence
//   of an unambiguous and accessible (ie. public inheritance
//   is mandatory) (since C++17) enable_shared_from_this base".
//
// I expect the `seastar::shared_ptr` shares this behaviour.
class Watch : public seastar::enable_shared_from_this<Watch> {
  // this is a private tag for the public constructor that turns it into
  // de facto private one. The motivation behind the hack is make_shared
  // used by create().
  struct private_ctag_t{};

public:
  Watch(private_ctag_t) {
  }

  seastar::future<> connect(crimson::net::ConnectionRef, bool) {
    return seastar::now();
  }
  bool is_connected() const {
    return true;
  }
  void got_ping(utime_t) {
    // NOP
  }

  seastar::future<> remove(bool) {
    return seastar::now();
  }

  template <class... Args>
  static seastar::shared_ptr<Watch> create(Args&&... args) {
    return seastar::make_shared<Watch>(private_ctag_t{},
                                       std::forward<Args>(args)...);
  };
};

using WatchRef = seastar::shared_ptr<Watch>;

} // namespace crimson::osd
