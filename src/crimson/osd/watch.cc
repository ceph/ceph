// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/insert.hpp>

#include "crimson/osd/watch.h"

#include "messages/MWatchNotify.h"

#include <fmt/ostream.h>


namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

Watch::~Watch()
{
  logger().debug("{} gid={} cookie={}", __func__, get_watcher_gid(), get_cookie());
}

seastar::future<> Watch::connect(crimson::net::ConnectionXcoreRef conn, bool)
{
  if (this->conn == conn) {
    logger().debug("conn={} already connected", *conn);
    return seastar::now();
  }
  timeout_timer.cancel();
  timeout_timer.arm(std::chrono::seconds{winfo.timeout_seconds});
  this->conn = std::move(conn);
  return seastar::now();
}

void Watch::disconnect()
{
  ceph_assert(!conn);
  timeout_timer.cancel();
  timeout_timer.arm(std::chrono::seconds{winfo.timeout_seconds});
}

seastar::future<> Watch::send_notify_msg(NotifyRef notify)
{
  logger().info("{} for notify(id={})", __func__, notify->ninfo.notify_id);
  return conn->send(crimson::make_message<MWatchNotify>(
    winfo.cookie,
    notify->user_version,
    notify->ninfo.notify_id,
    CEPH_WATCH_EVENT_NOTIFY,
    notify->ninfo.bl,
    notify->client_gid));
}

seastar::future<> Watch::start_notify(NotifyRef notify)
{
  logger().debug("{} gid={} cookie={} starting notify(id={})",
                 __func__,  get_watcher_gid(), get_cookie(),
                 notify->ninfo.notify_id);
  if (notify->complete) {
    // The notify already completed -- in practice because it timed out while
    // Notify::create_n_propagate() was still walking the watchers and this
    // watcher had not been reached yet. Do not record or deliver it: emplacing
    // an already-complete notify would leave a stale in_progress_notifies entry
    // (the timer has fired, so nothing would later remove it unless the client
    // acks), and delivering it would push a NOTIFY whose timeout completion was
    // already sent to the notifier. Same core as do_notify_timeout(), so reading
    // `complete` here is race-free.
    logger().debug("{} notify(id={}) already complete, skipping",
                   __func__, notify->ninfo.notify_id);
    return seastar::now();
  }
  auto [ it, emplaced ] = in_progress_notifies.emplace(std::move(notify));
  ceph_assert(emplaced);
  ceph_assert(is_alive());
  return is_connected() ? send_notify_msg(*it) : seastar::now();
}

seastar::future<> Watch::notify_ack(
  const uint64_t notify_id,
  const ceph::bufferlist& reply_bl)
{
  logger().debug("{} gid={} cookie={} notify_id={}",
                 __func__,  get_watcher_gid(), get_cookie(), notify_id);
  const auto it = in_progress_notifies.find(notify_id);
  if (it == std::end(in_progress_notifies)) {
    logger().error("{} notify_id={} not found on the in-progress list."
                   " Suppressing but this should not happen.",
                   __func__, notify_id);
    return seastar::now();
  }
  auto notify = *it;
  logger().debug("Watch::notify_ack gid={} cookie={} found notify(id={})",
    get_watcher_gid(),
    get_cookie(),
    notify->get_id());
  // let's ensure we're extending the life-time till end of this method
  static_assert(std::is_same_v<decltype(notify), NotifyRef>);
  in_progress_notifies.erase(it);
  return notify->complete_watcher(shared_from_this(), reply_bl);
}

seastar::future<> Watch::send_disconnect_msg()
{
  if (!is_connected()) {
    return seastar::now();
  }
  ceph::bufferlist empty;
  return conn->send(crimson::make_message<MWatchNotify>(
    winfo.cookie,
    0,
    0,
    CEPH_WATCH_EVENT_DISCONNECT,
    empty));
}

void Watch::discard_state()
{
  logger().debug("{} gid={} cookie={}", __func__, get_watcher_gid(), get_cookie());
  ceph_assert(obc);
  in_progress_notifies.clear();
  timeout_timer.cancel();
}

void Watch::got_ping(utime_t)
{
  if (is_connected()) {
    // using cancel() + arm() as rearm() has no overload for time delta.
    timeout_timer.cancel();
    timeout_timer.arm(std::chrono::seconds{winfo.timeout_seconds});
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
    }).then([this] {
      discard_state();
      return seastar::now();
    });
}

void Watch::cancel_notify(const uint64_t notify_id)
{
  logger().debug("{} gid={} cookie={} notify(id={})",
                 __func__,  get_watcher_gid(), get_cookie(),
                 notify_id);
  const auto it = in_progress_notifies.find(notify_id);
  if (it == std::end(in_progress_notifies)) {
    // A notify timeout can fire while `Notify::create_n_propagate()` is still
    // walking the watchers: `Notify::watchers` is populated synchronously and
    // the timer is armed up front, but each watcher only records the notify in
    // `in_progress_notifies` once its (asynchronous) `start_notify()` runs.
    // `do_notify_timeout()` then iterates the full watcher set and may reach a
    // watcher that has not started this notify yet. Treat that as a no-op
    // rather than dereferencing `end()`. Mirrors `notify_ack()`.
    logger().debug("{} notify_id={} not on the in-progress list, ignoring",
                   __func__, notify_id);
    return;
  }
  in_progress_notifies.erase(it);
}

bool notify_reply_t::operator<(const notify_reply_t& rhs) const
{
  // comparing std::pairs to emphasize our legacy. ceph-osd stores
  // notify_replies as std::multimap<std::pair<gid, cookie>, bl>.
  // unfortunately, what seems to be an implementation detail, got
  // exposed as part of our public API (the `reply_buffer` parameter
  // of the `rados_notify` family).
  const auto lhsp = std::make_pair(watcher_gid, watcher_cookie);
  const auto rhsp = std::make_pair(rhs.watcher_gid, rhs.watcher_cookie);
  return lhsp < rhsp;
}

std::ostream &operator<<(std::ostream &out, const notify_reply_t &rhs)
{
  out << "notify_reply_t{watcher_gid=" << rhs.watcher_gid
      << ", watcher_cookie=" << rhs.watcher_cookie << "}";
  return out;
}

Notify::Notify(crimson::net::ConnectionXcoreRef conn,
               const notify_info_t& ninfo,
               const uint64_t client_gid,
               const uint64_t user_version)
  : ninfo(ninfo),
    conn(std::move(conn)),
    client_gid(client_gid),
    user_version(user_version)
{}

Notify::~Notify()
{
  logger().debug("{} for notify(id={})", __func__, ninfo.notify_id);
}

seastar::future<> Notify::remove_watcher(WatchRef watch)
{
  logger().debug("{} for notify(id={})", __func__, ninfo.notify_id);

  if (discarded || complete) {
    logger().debug("{} for notify(id={}) discarded/complete already"
                   " discarded: {} complete: {}", __func__,
                   ninfo.notify_id, discarded ,complete);
    return seastar::now();
  }
  [[maybe_unused]] const auto num_removed = watchers.erase(watch);
  assert(num_removed > 0);
  if (watchers.empty()) {
    complete = true;
    [[maybe_unused]] bool was_armed = timeout_timer.cancel();
    assert(was_armed);
    return send_completion();
  } else {
    return seastar::now();
  }
}


seastar::future<> Notify::complete_watcher(
  WatchRef watch,
  const ceph::bufferlist& reply_bl)
{
  logger().debug("{} for notify(id={})", __func__, ninfo.notify_id);

  if (discarded || complete) {
    logger().debug("{} for notify(id={}) discarded/complete already"
                   " discarded: {} complete: {}", __func__,
                   ninfo.notify_id, discarded ,complete);
    return seastar::now();
  }
  notify_replies.emplace(notify_reply_t{
    watch->get_watcher_gid(),
    watch->get_cookie(),
    reply_bl});
  return remove_watcher(std::move(watch));
}

seastar::future<> Notify::send_completion(
  std::set<WatchRef> timedout_watchers)
{
  logger().info("{} -- {} in progress watchers, timedout watchers {}",
                __func__, watchers.size(), timedout_watchers.size());
  logger().debug("{} sending notify replies: {}", __func__, notify_replies);

  ceph::bufferlist empty;
  auto reply = crimson::make_message<MWatchNotify>(
    ninfo.cookie,
    user_version,
    ninfo.notify_id,
    CEPH_WATCH_EVENT_NOTIFY_COMPLETE,
    empty,
    client_gid);
  ceph::bufferlist reply_bl;
  {
    std::vector<std::pair<uint64_t,uint64_t>> missed;
    missed.reserve(std::size(timedout_watchers));
    boost::insert(
      missed, std::begin(missed),
      timedout_watchers | boost::adaptors::transformed([] (auto w) {
        return std::make_pair(w->get_watcher_gid(), w->get_cookie());
      }));
    ceph::encode(notify_replies, reply_bl);
    ceph::encode(missed, reply_bl);
  }
  reply->set_data(std::move(reply_bl));
  if (!timedout_watchers.empty()) {
    reply->return_code = -ETIMEDOUT;
  }
  return conn->send(std::move(reply));
}

void Notify::do_notify_timeout()
{
  logger().debug("{} complete={}", __func__, complete);
  if (complete) {
    return;
  }
  // it might be that `this` is kept alive only because of the reference
  // a watcher stores and which is being removed by `cancel_notify()`.
  // to avoid use-after-free we bump up the ref counter with `guard_ptr`.
  [[maybe_unused]] auto guard_ptr = shared_from_this();
  // Mark the notify complete before cancelling watchers and sending the timeout
  // completion. Propagation (Notify::create_n_propagate) may still be walking
  // the watchers: a watcher whose start_notify() has not run yet is skipped by
  // cancel_notify() (a no-op for a not-yet-recorded notify) and is still sent a
  // NOTIFY once propagation resumes. Without `complete` set, that watcher's
  // later ACK would reach complete_watcher()/remove_watcher() with `watchers`
  // already emptied here, tripping their asserts (debug) or emitting a second
  // NOTIFY_COMPLETE (release). Setting `complete` makes the late ACK a no-op via
  // the guards in those methods.
  complete = true;
  for (auto& watcher : watchers) {
    logger().debug("canceling watcher cookie={} gid={} use_count={}",
      watcher->get_cookie(),
      watcher->get_watcher_gid(),
      watcher->use_count());
    watcher->cancel_notify(ninfo.notify_id);
  }
  std::ignore = send_completion(std::move(watchers));
  watchers.clear();
}

} // namespace crimson::osd
