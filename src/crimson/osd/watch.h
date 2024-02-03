// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iterator>
#include <map>
#include <set>

#include <seastar/core/shared_ptr.hh>

#include "crimson/net/Connection.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/pg.h"
#include "include/denc.h"

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

  std::set<NotifyRef, std::less<>> in_progress_notifies;
  crimson::net::ConnectionXcoreRef conn;
  crimson::osd::ObjectContextRef obc;

  watch_info_t winfo;
  entity_name_t entity_name;
  Ref<PG> pg;

  seastar::timer<seastar::lowres_clock> timeout_timer;

  seastar::future<> start_notify(NotifyRef);
  seastar::future<> send_notify_msg(NotifyRef);
  seastar::future<> send_disconnect_msg();

  friend Notify;
  friend class WatchTimeoutRequest;

public:
  Watch(private_ctag_t,
        crimson::osd::ObjectContextRef obc,
        const watch_info_t& winfo,
        const entity_name_t& entity_name,
        Ref<PG> pg)
    : obc(std::move(obc)),
      winfo(winfo),
      entity_name(entity_name),
      pg(std::move(pg)),
      timeout_timer([this] {
        return do_watch_timeout();
      }) {
    assert(this->pg);
  }
  ~Watch();

  seastar::future<> connect(crimson::net::ConnectionXcoreRef, bool);
  void disconnect();
  bool is_alive() const {
    return true;
  }
  bool is_connected() const {
    return static_cast<bool>(conn);
  }
  void got_ping(utime_t);

  void discard_state();

  seastar::future<> remove();

  /// Call when notify_ack received on notify_id
  seastar::future<> notify_ack(
    uint64_t notify_id, ///< [in] id of acked notify
    const ceph::bufferlist& reply_bl); ///< [in] notify reply buffer

  template <class... Args>
  static seastar::shared_ptr<Watch> create(Args&&... args) {
    return seastar::make_shared<Watch>(private_ctag_t{},
                                       std::forward<Args>(args)...);
  };

  uint64_t get_watcher_gid() const {
    return entity_name.num();
  }
  auto get_pg() const {
    return pg;
  }
  auto& get_entity() const {
    return entity_name;
  }
  auto& get_cookie() const {
    return winfo.cookie;
  }
  auto& get_peer_addr() const {
    return winfo.addr;
  }
  void cancel_notify(const uint64_t notify_id);
  void do_watch_timeout();
};

using WatchRef = seastar::shared_ptr<Watch>;

struct notify_reply_t {
  uint64_t watcher_gid;
  uint64_t watcher_cookie;
  ceph::bufferlist bl;

  bool operator<(const notify_reply_t& rhs) const;
  DENC(notify_reply_t, v, p) {
    // there is no versioning / preamble
    denc(v.watcher_gid, p);
    denc(v.watcher_cookie, p);
    denc(v.bl, p);
  }
};
std::ostream &operator<<(std::ostream &out, const notify_reply_t &rhs);

class Notify : public seastar::enable_shared_from_this<Notify> {
  std::set<WatchRef> watchers;
  const notify_info_t ninfo;
  crimson::net::ConnectionXcoreRef conn;
  const uint64_t client_gid;
  const uint64_t user_version;
  bool complete{false};
  bool discarded{false};
  seastar::timer<seastar::lowres_clock> timeout_timer{
    [this] { do_notify_timeout(); }
  };

  ~Notify();

  /// (gid,cookie) -> reply_bl for everyone who acked the notify
  std::multiset<notify_reply_t> notify_replies;

  uint64_t get_id() const { return ninfo.notify_id; }

  /// Sends notify completion if watchers.empty() or timeout
  seastar::future<> send_completion(
    std::set<WatchRef> timedout_watchers = {});

  /// Called on Notify timeout
  void do_notify_timeout();

  Notify(crimson::net::ConnectionXcoreRef conn,
         const notify_info_t& ninfo,
         const uint64_t client_gid,
         const uint64_t user_version);
  template <class WatchIteratorT>
  Notify(WatchIteratorT begin,
         WatchIteratorT end,
         crimson::net::ConnectionXcoreRef conn,
         const notify_info_t& ninfo,
         const uint64_t client_gid,
         const uint64_t user_version);
  // this is a private tag for the public constructor that turns it into
  // de facto private one. The motivation behind the hack is make_shared
  // used by create_n_propagate factory.
  struct private_ctag_t{};

  using ptr_t = seastar::shared_ptr<Notify>;
  friend bool operator<(const ptr_t& lhs, const ptr_t& rhs) {
    assert(lhs);
    assert(rhs);
    return lhs->get_id() < rhs->get_id();
  }
  friend bool operator<(const ptr_t& ptr, const uint64_t id) {
    assert(ptr);
    return ptr->get_id() < id;
  }
  friend bool operator<(const uint64_t id, const ptr_t& ptr) {
    assert(ptr);
    return id < ptr->get_id();
  }

  friend Watch;

public:
  template <class... Args>
  Notify(private_ctag_t, Args&&... args) : Notify(std::forward<Args>(args)...) {
  }

  template <class WatchIteratorT, class... Args>
  static seastar::future<> create_n_propagate(
    WatchIteratorT begin,
    WatchIteratorT end,
    Args&&... args);

  seastar::future<> remove_watcher(WatchRef watch);
  seastar::future<> complete_watcher(WatchRef watch,
                                     const ceph::bufferlist& reply_bl);
};


template <class WatchIteratorT>
Notify::Notify(WatchIteratorT begin,
               WatchIteratorT end,
               crimson::net::ConnectionXcoreRef conn,
               const notify_info_t& ninfo,
               const uint64_t client_gid,
               const uint64_t user_version)
  : watchers(begin, end),
    ninfo(ninfo),
    conn(std::move(conn)),
    client_gid(client_gid),
    user_version(user_version) {
  assert(!std::empty(watchers));
  if (ninfo.timeout) {
    timeout_timer.arm(std::chrono::seconds{ninfo.timeout});
  }
}

template <class WatchIteratorT, class... Args>
seastar::future<> Notify::create_n_propagate(
  WatchIteratorT begin,
  WatchIteratorT end,
  Args&&... args)
{
  static_assert(
    std::is_same_v<typename std::iterator_traits<WatchIteratorT>::value_type,
                   crimson::osd::WatchRef>);
  if (begin == end) {
    auto notify = seastar::make_shared<Notify>(
      private_ctag_t{},
      std::forward<Args>(args)...);
    return notify->send_completion();
  } else {
    auto notify = seastar::make_shared<Notify>(
      private_ctag_t{},
      begin, end,
      std::forward<Args>(args)...);
    return seastar::do_for_each(begin, end, [=] (auto& watchref) {
      return watchref->start_notify(notify);
    });
  }
}

} // namespace crimson::osd

WRITE_CLASS_DENC(crimson::osd::notify_reply_t)

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::notify_reply_t> : fmt::ostream_formatter {};
#endif
