// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <seastar/core/do_with.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/smp.hh>

#include "crimson/osd/watch.h"
#include "crimson/osd/osd_connection_priv.h"

#include <fmt/ostream.h>

// The connection-coupled half of the Watch machinery: (re)connect, teardown,
// and the per-connection watch registry (WatchConState) used to disconnect a
// connection's watches on reset. It is separated from watch.cc because it
// touches OSD-only symbols (get_osd_priv / OSDConnectionPriv), which watch.cc
// must avoid so that it can also be compiled into the standalone unit tests.
//
// Cores: a Watch lives on its PG's core; its connection is a foreign reference
// to a Connection homed on a possibly-different core. Registering therefore
// hops PG-core -> connection-core, and reset (running on the connection-core)
// hops back connection-core -> PG-core once per watch.

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

seastar::future<> Watch::register_on_conn()
{
  ceph_assert(conn);
  const auto conn_shard = conn.get_owner_shard();
  auto* const raw_conn = conn.get();
  const void* const key = this;
  // Move a foreign ref of ourselves onto the connection's core and file it in
  // that connection's registry. `conn` (held by this Watch, kept alive by the
  // caller for the duration of connect()) keeps `raw_conn` valid across the hop.
  return seastar::smp::submit_to(conn_shard,
    [raw_conn, key, fwatch=seastar::make_foreign(shared_from_this())]() mutable {
      get_osd_priv(raw_conn).ensure_watch_conn_state().add_watch(
        key, std::move(fwatch));
    });
}

seastar::future<> Watch::deregister_from_conn()
{
  if (!conn) {
    return seastar::now();
  }
  const auto conn_shard = conn.get_owner_shard();
  auto* const raw_conn = conn.get();
  const void* const key = this;
  // Keep the connection alive across the hop (via the finally keep-alive) in
  // case this Watch holds its last reference and drops `conn` right after.
  return seastar::smp::submit_to(conn_shard,
    [raw_conn, key] {
      if (raw_conn->has_user_private()) {
        auto& priv = get_osd_priv(raw_conn);
        if (priv.watch_conn_state) {
          priv.watch_conn_state->remove_watch(key);
        }
      }
    }).finally([keep=conn] {});
}

seastar::future<> Watch::connect(crimson::net::ConnectionXcoreRef conn, bool)
{
  if (this->conn == conn) {
    logger().debug("conn={} already connected", *conn);
    return seastar::now();
  }
  // If we were connected over a different connection, leave its registry before
  // adopting the new one, so a later reset of the old connection cannot
  // disconnect us. deregister_from_conn() reads the current (old) `conn`, so it
  // must run before we overwrite it below.
  return deregister_from_conn().then(
    [this, this_shared=shared_from_this(),
     new_conn=std::move(conn)]() mutable {
      timeout_timer.cancel();
      timeout_timer.arm(std::chrono::seconds{winfo.timeout_seconds});
      this->conn = std::move(new_conn);
      return register_on_conn();
    }).then([this, this_shared=shared_from_this()] {
      // Now (re)connected: replay every notify buffered while disconnected.
      // Mirrors classic Watch::connect() (src/osd/Watch.cc). start_notify()
      // records a notify in in_progress_notifies even while disconnected but
      // only *delivers* it once connected, so the buffered ones must be
      // (re)sent here. Resending an already-delivered NOTIFY is harmless -- the
      // client de-duplicates by notify_id. Snapshot first so a concurrent
      // notify_ack()/cancel_notify() cannot invalidate the iteration.
      return seastar::do_with(
        std::vector<NotifyRef>(std::begin(in_progress_notifies),
                               std::end(in_progress_notifies)),
        [this_shared](auto& buffered) {
          return seastar::do_for_each(buffered, [this_shared](auto& notify) {
            return this_shared->send_notify_msg(notify);
          });
        });
    });
}

void Watch::discard_state()
{
  logger().debug("{} gid={} cookie={}", __func__, get_watcher_gid(), get_cookie());
  ceph_assert(obc);
  in_progress_notifies.clear();
  timeout_timer.cancel();
  discarded = true;
  if (conn) {
    // Leave the connection's registry -- this also breaks the Watch<->Connection
    // reference cycle (Watch -> conn -> OSDConnectionPriv -> WatchConState ->
    // foreign Watch). Fire-and-forget: discard_state() is synchronous, and
    // deregister_from_conn() keeps the connection alive across its own hop.
    std::ignore = deregister_from_conn();
    conn = {};
  }
}

seastar::future<> Watch::remove()
{
  logger().debug("{} gid={} cookie={}", __func__, get_watcher_gid(), get_cookie());
  // in contrast to ceph-osd crimson sends CEPH_WATCH_EVENT_DISCONNECT directly
  // from the timeout handler and _after_ CEPH_WATCH_EVENT_NOTIFY_COMPLETE.
  // this simplifies the Watch::remove() interface as callers aren't obliged
  // anymore to decide whether EVENT_DISCONNECT needs to be send or not -- it
  // becomes an implementation detail of Watch.
  return seastar::do_for_each(in_progress_notifies,
    [this_shared=shared_from_this()] (auto notify) {
      logger().debug("Watch::remove gid={} cookie={} notify(id={})",
                     this_shared->get_watcher_gid(),
                     this_shared->get_cookie(),
                     notify->ninfo.notify_id);
      return notify->remove_watcher(this_shared);
    }).then([this_shared=shared_from_this()] {
      this_shared->discard_state();  // also deregisters from the connection
      return seastar::now();
    });
}

void WatchConState::add_watch(const void* key,
                              seastar::foreign_ptr<WatchRef> watch)
{
  watches.insert_or_assign(key, std::move(watch));
}

void WatchConState::remove_watch(const void* key)
{
  watches.erase(key);
}

seastar::future<> WatchConState::reset(const crimson::net::Connection* con)
{
  logger().debug("WatchConState::reset {} watch(es)", watches.size());
  // Take the whole set at once so concurrent (de)registration cannot race the
  // fan-out, then disconnect each watch on its own core. Mirrors classic
  // WatchConState::reset() (src/osd/Watch.cc).
  return seastar::do_with(std::exchange(watches, {}),
    [con](auto& entries) {
      return seastar::parallel_for_each(entries, [con](auto& kv) {
        const auto shard = kv.second.get_owner_shard();
        auto* const watch = kv.second.get();
        return seastar::smp::submit_to(shard, [watch, con] {
          // On the watch's own core. Skip a watch that was already torn down or
          // that reconnected onto a different connection between the reset and
          // this hop (classic's is_connected(con) guard).
          if (!watch->is_discarded() && watch->is_connected_to(con)) {
            watch->disconnect();
          }
        });
      });
    });
  // `entries` (and its foreign refs) is destroyed here on the connection's core.
}

OSDConnectionPriv::OSDConnectionPriv() = default;
OSDConnectionPriv::~OSDConnectionPriv() = default;

WatchConState &OSDConnectionPriv::ensure_watch_conn_state()
{
  if (!watch_conn_state) {
    watch_conn_state = std::make_unique<WatchConState>();
  }
  return *watch_conn_state;
}

} // namespace crimson::osd
