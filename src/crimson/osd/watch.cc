// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/insert.hpp>

#include "crimson/osd/watch.h"
#include "crimson/osd/osd_operations/internal_client_request.h"

#include "messages/MWatchNotify.h"


namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

// a watcher can remove itself if it has not seen a notification after a period of time.
// in the case, we need to drop it also from the persisted `ObjectState` instance.
// this operation resembles a bit the `_UNWATCH` subop.
class WatchTimeoutRequest final : public InternalClientRequest {
public:
  WatchTimeoutRequest(WatchRef watch, Ref<PG> pg)
    : InternalClientRequest(std::move(pg)),
      watch(std::move(watch)) {
  }

  const hobject_t& get_target_oid() const final;
  PG::do_osd_ops_params_t get_do_osd_ops_params() const final;
  std::vector<OSDOp> create_osd_ops() final;

private:
  WatchRef watch;
};

const hobject_t& WatchTimeoutRequest::get_target_oid() const
{
  assert(watch->obc);
  return watch->obc->get_oid();
}

PG::do_osd_ops_params_t
WatchTimeoutRequest::get_do_osd_ops_params() const
{
  osd_reqid_t reqid;
  reqid.name = watch->entity_name;
  PG::do_osd_ops_params_t params{
    watch->conn,
    reqid,
    ceph_clock_now(),
    get_pg().get_osdmap_epoch(),
    entity_inst_t{ watch->entity_name, watch->winfo.addr },
    0
  };
  logger().debug("{}: params.reqid={}", __func__, params.reqid);
  return params;
}

std::vector<OSDOp> WatchTimeoutRequest::create_osd_ops()
{
  logger().debug("{}", __func__);
  assert(watch);
  OSDOp osd_op;
  osd_op.op.op = CEPH_OSD_OP_WATCH;
  osd_op.op.flags = 0;
  osd_op.op.watch.op = CEPH_OSD_WATCH_OP_UNWATCH;
  osd_op.op.watch.cookie = watch->winfo.cookie;
  return std::vector{std::move(osd_op)};
}

Watch::~Watch()
{
  logger().debug("{} gid={} cookie={}", __func__, get_watcher_gid(), get_cookie());
}

seastar::future<> Watch::connect(crimson::net::ConnectionXcoreRef conn, bool)
{
  if (this->conn == conn) {
    logger().debug("conn={} already connected", conn);
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
    logger().error("{} notify_id={} not found on the in-progess list."
                   " Supressing but this should not happen.",
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
  assert(it != std::end(in_progress_notifies));
  in_progress_notifies.erase(it);
}

void Watch::do_watch_timeout()
{
  assert(pg);
  auto [op, fut] = pg->get_shard_services().start_operation<WatchTimeoutRequest>(
    shared_from_this(), pg);
  std::ignore = std::move(fut).then([op=std::move(op), this] {
    return send_disconnect_msg();
  });
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

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::WatchTimeoutRequest> : fmt::ostream_formatter {};
#endif
