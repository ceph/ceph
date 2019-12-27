// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/watch.h"
#include "messages/MWatchNotify.h"


namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

bool Watch::NotifyCmp::operator()(NotifyRef lhs, NotifyRef rhs) const
{
  ceph_assert(lhs);
  ceph_assert(rhs);
  return lhs->get_id() < rhs->get_id();
}

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
  logger().info("{} adding &notify={}", __func__, notify.get());
  auto [ it, emplaced ] = in_progress_notifies.emplace(std::move(notify));
  ceph_assert(emplaced);
  ceph_assert(is_alive());
  return is_connected() ? send_notify_msg(*it) : seastar::now();
}

} // namespace crimson::osd
