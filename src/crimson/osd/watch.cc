// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/insert.hpp>

#include "crimson/osd/watch.h"
#include "messages/MWatchNotify.h"


namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

seastar::future<> Watch::connect(crimson::net::ConnectionRef conn, bool)
{
  if (this->conn == conn) {
    logger().debug("conn={} already connected", conn);
  }

  this->conn = std::move(conn);
  return seastar::now();
}

seastar::future<> Watch::send_notify_msg(NotifyRef notify)
{
  logger().info("{} for notify(id={})", __func__, notify->ninfo.notify_id);
  return conn->send(make_message<MWatchNotify>(
    winfo.cookie,
    notify->user_version,
    notify->ninfo.notify_id,
    CEPH_WATCH_EVENT_NOTIFY,
    notify->ninfo.bl,
    notify->client_gid));
}

seastar::future<> Watch::start_notify(NotifyRef notify)
{
  logger().info("{} adding notify(id={})", __func__, notify->ninfo.notify_id);
  auto [ it, emplaced ] = in_progress_notifies.emplace(std::move(notify));
  ceph_assert(emplaced);
  ceph_assert(is_alive());
  return is_connected() ? send_notify_msg(*it) : seastar::now();
}

seastar::future<> Watch::notify_ack(
  const uint64_t notify_id,
  const ceph::bufferlist& reply_bl)
{
  logger().info("{}", __func__);
  return seastar::do_for_each(in_progress_notifies,
    [this_shared=shared_from_this(), &reply_bl] (auto notify) {
      return notify->complete_watcher(this_shared, reply_bl);
    }
  ).then([this] {
    in_progress_notifies.clear();
    return seastar::now();
  });
}

seastar::future<> Watch::send_disconnect_msg()
{
  if (!is_connected()) {
    return seastar::now();
  }
  ceph::bufferlist empty;
  return conn->send(make_message<MWatchNotify>(
    winfo.cookie,
    0,
    0,
    CEPH_WATCH_EVENT_DISCONNECT,
    empty));
}

void Watch::discard_state()
{
  ceph_assert(obc);
  in_progress_notifies.clear();
}

seastar::future<> Watch::remove(const bool send_disconnect)
{
  logger().info("{}", __func__);
  auto disconnected = send_disconnect ? send_disconnect_msg()
                                      : seastar::now();
  return std::move(disconnected).then([this] {
    return seastar::do_for_each(in_progress_notifies,
      [this_shared=shared_from_this()] (auto notify) {
        return notify->remove_watcher(this_shared);
      }).then([this] {
        discard_state();
        return seastar::now();
      });
    });
}

void Watch::cancel_notify(const uint64_t notify_id)
{
  logger().info("{} notify_id={}", __func__, notify_id);
  const auto it = in_progress_notifies.find(notify_id);
  assert(it != std::end(in_progress_notifies));
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
      << ", watcher_cookie=" << rhs.watcher_cookie
      << ", bl=" << rhs.bl << "}";
  return out;
}

seastar::future<> Notify::remove_watcher(WatchRef watch)
{
  if (discarded || complete) {
    return seastar::now();
  }
  [[maybe_unused]] const auto num_removed = watchers.erase(watch);
  assert(num_removed > 0);
  return maybe_send_completion();
}


seastar::future<> Notify::complete_watcher(
  WatchRef watch,
  const ceph::bufferlist& reply_bl)
{
  if (discarded || complete) {
    return seastar::now();
  }
  notify_replies.emplace(notify_reply_t{
    watch->get_watcher_gid(),
    watch->get_cookie(),
    reply_bl});
  return remove_watcher(std::move(watch));
}

seastar::future<> Notify::maybe_send_completion(
  std::set<WatchRef> timedout_watchers)
{
  logger().info("{} -- {} in progress watchers, {} timedout watchers {}",
                __func__, watchers.size(), timedout_watchers.size());
  if (watchers.empty() || !timedout_watchers.empty()) {
    logger().debug("{} sending notify replies: {}", __func__, notify_replies);
    complete = true;
    timeout_timer.cancel();

    ceph::bufferlist empty;
    auto reply = make_message<MWatchNotify>(
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
  return seastar::now();
}

void Notify::do_timeout()
{
  logger().debug("{} complete={}", __func__, complete);
  if (complete) {
    return;
  }
  decltype(watchers) timedout_watchers;
  std::swap(watchers, timedout_watchers);
  for (auto& watcher : timedout_watchers) {
    watcher->cancel_notify(ninfo.notify_id);
  }
  std::ignore = maybe_send_completion(std::move(timedout_watchers));
}

} // namespace crimson::osd
