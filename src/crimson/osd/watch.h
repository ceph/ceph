// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <iterator>
#include <set>

#include <boost/container/flat_map.hpp>

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

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

  // A dedicated private tag for the unit-test-only constructor below. Being
  // private, it can only be named by `Watch` itself, so the sole way to reach
  // that constructor is `create_for_test()`. Its presence in the signature
  // also doubly documents -- at the definition and at every (test-only) call
  // site -- that the constructor is not for production use.
  struct unit_test_ctag_t{};

  std::set<NotifyRef, std::less<>> in_progress_notifies;
  crimson::net::ConnectionXcoreRef conn;
  crimson::osd::ObjectContextRef obc;

  watch_info_t winfo;
  entity_name_t entity_name;
  Ref<PG> pg;
  // set once the watch is torn down (remove()/discard_state()); guards a reset
  // that races watch removal. Mirrors classic Watch::discarded.
  bool discarded = false;

  seastar::timer<seastar::lowres_clock> timeout_timer;

  seastar::future<> start_notify(NotifyRef);
  seastar::future<> send_notify_msg(NotifyRef);
  seastar::future<> send_disconnect_msg();

  // Register/unregister this watch in its connection's per-connection registry
  // (crimson::osd::WatchConState, living in the connection's OSDConnectionPriv)
  // so that a reset of the connection can find and disconnect it. These perform
  // a cross-core hop because the watch lives on its PG's core while the registry
  // lives on the connection's home core. Defined in watch_conn.cc (they touch
  // OSD-only symbols and so must stay out of watch.cc, which is also compiled
  // into unit tests).
  seastar::future<> register_on_conn();
  seastar::future<> deregister_from_conn();

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

  // UNIT-TEST ONLY. Builds a Watch without an ObjectContext, a PG or a
  // connection -- just enough state to exercise the connection- and
  // PG-agnostic bookkeeping (notably the in_progress_notifies handling in
  // cancel_notify()/notify_ack()). Because `obc` and `pg` are left null, the
  // caller must NOT arm timeout_timer or invoke anything that dereferences
  // them (e.g. do_watch_timeout(), remove(), start_notify() on a connected
  // watch). The timeout callback is deliberately a no-op: wiring
  // do_watch_timeout() here would make every test translation unit depend on
  // the PG operation framework (WatchTimeoutRequest et al.), which is exactly
  // what this seam avoids. Reachable only through create_for_test(); see
  // unit_test_ctag_t.
  Watch(unit_test_ctag_t,
        const watch_info_t& winfo,
        const entity_name_t& entity_name)
    : winfo(winfo),
      entity_name(entity_name),
      timeout_timer([] { /* never armed in tests; see note above */ }) {
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
  bool is_connected_to(const crimson::net::Connection* con) const {
    // identity comparison only; safe to call from any core.
    return conn.get() == con;
  }
  bool is_discarded() const {
    return discarded;
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

  // UNIT-TEST ONLY factory for the unit_test_ctag_t constructor above.
  static seastar::shared_ptr<Watch> create_for_test(
      const watch_info_t& winfo,
      const entity_name_t& entity_name) {
    return seastar::make_shared<Watch>(unit_test_ctag_t{}, winfo, entity_name);
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

// A per-connection registry of the watches currently reachable over one client
// connection. It lives in that connection's OSDConnectionPriv (on the
// connection's home core) and lets OSD::ms_handle_reset() find and disconnect
// every watch of a reset connection. This is crimson's equivalent of classic
// WatchConState (src/osd/Watch.h) / Session::wstate.
//
// A Watch is owned by its ObjectContext and therefore lives on its PG's core,
// which may differ from the connection's core. Entries are consequently held as
// cross-core `seastar::foreign_ptr<WatchRef>` and keyed by the Watch's address
// (used purely as an opaque identity). All methods run on the connection's core;
// reset() fans out one cross-core hop per watch to disconnect it on its own
// core. Defined in watch_conn.cc.
class WatchConState {
  // A flat_map (contiguous, one growable allocation) rather than std::map: the
  // set is small (the objects a single connection watches), never touched on
  // the notify data path, mutated only on watch establishment/teardown, and
  // iterated only on reset -- so cache-friendly iteration matters more than
  // node stability or ordered lookup, and the pointer key is opaque identity.
  boost::container::flat_map<const void*, seastar::foreign_ptr<WatchRef>> watches;

public:
  /// Register a (foreign) watch under its identity key.
  void add_watch(const void* key, seastar::foreign_ptr<WatchRef> watch);
  /// Unregister a watch; a no-op if it is not present.
  void remove_watch(const void* key);
  bool empty() const {
    return watches.empty();
  }
  /// Disconnect every registered watch that is still connected to `con`,
  /// emptying the registry. Called on a connection reset.
  seastar::future<> reset(const crimson::net::Connection* con);
};

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
