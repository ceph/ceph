// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iterator>
#include <set>

#include <seastar/core/shared_ptr.hh>

#include "crimson/net/Connection.h"

namespace crimson::osd {

class Notify;
using NotifyRef = seastar::shared_ptr<Notify>;

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

  struct NotifyCmp {
    inline bool operator()(NotifyRef lhs, NotifyRef rhs) const;
  };
  std::set<NotifyRef, NotifyCmp> in_progress_notifies;
  crimson::net::ConnectionRef conn;

  seastar::future<> start_notify(NotifyRef);
  seastar::future<> send_notify_msg(NotifyRef);

  friend Notify;

public:
  Watch(private_ctag_t) {
  }

  seastar::future<> connect(crimson::net::ConnectionRef, bool);
  bool is_alive() const {
    return true;
  }
  bool is_connected() const {
    return static_cast<bool>(conn);
  }
  void got_ping(utime_t) {
    // NOP
  }

  seastar::future<> remove(bool) {
    return seastar::now();
  }

  /// Call when notify_ack received on notify_id
  seastar::future<> notify_ack(
    uint64_t notify_id, ///< [in] id of acked notify
    const ceph::bufferlist& reply_bl) { ///< [in] notify reply buffer
    return seastar::now();
  }

  template <class... Args>
  static seastar::shared_ptr<Watch> create(Args&&... args) {
    return seastar::make_shared<Watch>(private_ctag_t{},
                                       std::forward<Args>(args)...);
  };
};

using WatchRef = seastar::shared_ptr<Watch>;

class Notify {
  std::set<WatchRef> watchers;

  uint64_t get_id() const { return 0; }
  void propagate() {}

  template <class WatchIteratorT>
  Notify(WatchIteratorT begin, WatchIteratorT end);
  // this is a private tag for the public constructor that turns it into
  // de facto private one. The motivation behind the hack is make_shared
  // used by create_n_propagate factory.
  struct private_ctag_t{};

  friend Watch;

public:
  template <class... Args>
  Notify(private_ctag_t, Args&&... args) : Notify(std::forward<Args>(args)...) {
  }

  template <class WatchIteratorT>
  static seastar::future<> create_n_propagate(
    WatchIteratorT begin,
    WatchIteratorT end);
};


template <class WatchIteratorT>
Notify::Notify(WatchIteratorT begin, WatchIteratorT end)
  : watchers(begin, end) {
}

template <class WatchIteratorT>
seastar::future<> Notify::create_n_propagate(
  WatchIteratorT begin,
  WatchIteratorT end)
{
  static_assert(
    std::is_same_v<typename std::iterator_traits<WatchIteratorT>::value_type,
                   crimson::osd::WatchRef>);
  auto notify = seastar::make_shared<Notify>(private_ctag_t{}, begin, end);
  seastar::do_for_each(begin, end, [=] (auto& watchref) {
    return watchref->start_notify(notify);
  }).then([notify = std::move(notify)] {;
    notify->propagate();
  });
}

} // namespace crimson::osd
