// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "heartbeat.h"

#include <boost/range/join.hpp>

#include "messages/MOSDPing.h"
#include "messages/MOSDFailure.h"

#include "crimson/common/config_proxy.h"
#include "crimson/common/formatter.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/osd/shard_services.h"
#include "crimson/mon/MonClient.h"

#include "osd/OSDMap.h"

using crimson::common::local_conf;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

Heartbeat::Heartbeat(const crimson::osd::ShardServices& service,
                     crimson::mon::Client& monc,
                     crimson::net::MessengerRef front_msgr,
                     crimson::net::MessengerRef back_msgr)
  : service{service},
    monc{monc},
    front_msgr{front_msgr},
    back_msgr{back_msgr},
    // do this in background
    timer{[this] {
      heartbeat_check();
      (void)send_heartbeats();
    }}
{}

seastar::future<> Heartbeat::start(entity_addrvec_t front_addrs,
                                   entity_addrvec_t back_addrs)
{
  logger().info("heartbeat: start");
  // i only care about the address, so any unused port would work
  for (auto& addr : boost::join(front_addrs.v, back_addrs.v)) {
    addr.set_port(0);
  }

  using crimson::net::SocketPolicy;
  front_msgr->set_policy(entity_name_t::TYPE_OSD,
                         SocketPolicy::lossy_client(0));
  back_msgr->set_policy(entity_name_t::TYPE_OSD,
                        SocketPolicy::lossy_client(0));
  auto chained_dispatchers = seastar::make_lw_shared<ChainedDispatchers>();
  chained_dispatchers->push_back(*this);
  return seastar::when_all_succeed(start_messenger(*front_msgr,
						   front_addrs,
						   chained_dispatchers),
                                   start_messenger(*back_msgr,
						   back_addrs,
						   chained_dispatchers))
    .then([this] {
      timer.arm_periodic(
        std::chrono::seconds(local_conf()->osd_heartbeat_interval));
    });
}

seastar::future<>
Heartbeat::start_messenger(crimson::net::Messenger& msgr,
                           const entity_addrvec_t& addrs,
			   ChainedDispatchersRef chained_dispatchers)
{
  return msgr.try_bind(addrs,
                       local_conf()->ms_bind_port_min,
                       local_conf()->ms_bind_port_max)
  .then([&msgr, chained_dispatchers]() mutable {
    return msgr.start(chained_dispatchers);
  });
}

seastar::future<> Heartbeat::stop()
{
  logger().info("{}", __func__);
  timer.cancel();
  if (!front_msgr->dispatcher_chain_empty())
    front_msgr->remove_dispatcher(*this);
  if (!back_msgr->dispatcher_chain_empty())
    back_msgr->remove_dispatcher(*this);
  return gate.close().then([this] {
    return seastar::when_all_succeed(front_msgr->shutdown(),
				     back_msgr->shutdown());
  });
}

const entity_addrvec_t& Heartbeat::get_front_addrs() const
{
  return front_msgr->get_myaddrs();
}

const entity_addrvec_t& Heartbeat::get_back_addrs() const
{
  return back_msgr->get_myaddrs();
}

void Heartbeat::set_require_authorizer(bool require_authorizer)
{
  if (front_msgr->get_require_authorizer() != require_authorizer) {
    front_msgr->set_require_authorizer(require_authorizer);
    back_msgr->set_require_authorizer(require_authorizer);
  }
}

void Heartbeat::add_peer(osd_id_t _peer, epoch_t epoch)
{
  auto [iter, added] = peers.try_emplace(_peer, *this, _peer);
  auto& peer = iter->second;
  peer.set_epoch(epoch);
}

Heartbeat::osds_t Heartbeat::remove_down_peers()
{
  osds_t osds;
  for (auto& [osd, peer] : peers) {
    auto osdmap = service.get_osdmap_service().get_map();
    if (!osdmap->is_up(osd)) {
      remove_peer(osd);
    } else if (peer.get_epoch() < osdmap->get_epoch()) {
      osds.push_back(osd);
    }
  }
  return osds;
}

void Heartbeat::add_reporter_peers(int whoami)
{
  auto osdmap = service.get_osdmap_service().get_map();
  // include next and previous up osds to ensure we have a fully-connected set
  set<int> want;
  if (auto next = osdmap->get_next_up_osd_after(whoami); next >= 0) {
    want.insert(next);
  }
  if (auto prev = osdmap->get_previous_up_osd_before(whoami); prev >= 0) {
    want.insert(prev);
  }
  // make sure we have at least **min_down** osds coming from different
  // subtree level (e.g., hosts) for fast failure detection.
  auto min_down = local_conf().get_val<uint64_t>("mon_osd_min_down_reporters");
  auto subtree = local_conf().get_val<string>("mon_osd_reporter_subtree_level");
  osdmap->get_random_up_osds_by_subtree(
    whoami, subtree, min_down, want, &want);
  auto epoch = osdmap->get_epoch();
  for (int osd : want) {
    add_peer(osd, epoch);
  };
}

void Heartbeat::update_peers(int whoami)
{
  const auto min_peers = static_cast<size_t>(
    local_conf().get_val<int64_t>("osd_heartbeat_min_peers"));
  add_reporter_peers(whoami);
  auto extra = remove_down_peers();
  // too many?
  for (auto& osd : extra) {
    if (peers.size() <= min_peers) {
      break;
    }
    remove_peer(osd);
  }
  // or too few?
  auto osdmap = service.get_osdmap_service().get_map();
  auto epoch = osdmap->get_epoch();
  for (auto next = osdmap->get_next_up_osd_after(whoami);
    peers.size() < min_peers && next >= 0 && next != whoami;
    next = osdmap->get_next_up_osd_after(next)) {
    add_peer(next, epoch);
  }
}

void Heartbeat::remove_peer(osd_id_t peer)
{
  auto found = peers.find(peer);
  assert(found != peers.end());
  peers.erase(peer);
}

seastar::future<> Heartbeat::ms_dispatch(crimson::net::Connection* conn,
                                         MessageRef m)
{
  return gate.dispatch(__func__, *this, [this, conn, &m] {
    switch (m->get_type()) {
    case MSG_OSD_PING:
      return handle_osd_ping(conn, boost::static_pointer_cast<MOSDPing>(m));
    default:
      return seastar::now();
    }
  });
}

void Heartbeat::ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace)
{
  // TODO: we should already have enough information to know which peer the
  // conn belongs, so no need to do linear search here.
  for (auto& [osd, peer] : peers) {
    peer.handle_reset(conn);
  }
}

seastar::future<> Heartbeat::handle_osd_ping(crimson::net::Connection* conn,
                                             Ref<MOSDPing> m)
{
  switch (m->op) {
  case MOSDPing::PING:
    return handle_ping(conn, m);
  case MOSDPing::PING_REPLY:
    return handle_reply(conn, m);
  case MOSDPing::YOU_DIED:
    return handle_you_died();
  default:
    return seastar::now();
  }
}

seastar::future<> Heartbeat::handle_ping(crimson::net::Connection* conn,
                                         Ref<MOSDPing> m)
{
  auto min_message = static_cast<uint32_t>(
    local_conf()->osd_heartbeat_min_size);
  auto reply =
    make_message<MOSDPing>(
      m->fsid,
      service.get_osdmap_service().get_map()->get_epoch(),
      MOSDPing::PING_REPLY,
      m->ping_stamp,
      m->mono_ping_stamp,
      service.get_mnow(),
      service.get_osdmap_service().get_up_epoch(),
      min_message);
  return conn->send(reply);
}

seastar::future<> Heartbeat::handle_reply(crimson::net::Connection* conn,
                                          Ref<MOSDPing> m)
{
  const osd_id_t from = m->get_source().num();
  auto found = peers.find(from);
  if (found == peers.end()) {
    // stale reply
    return seastar::now();
  }
  auto& peer = found->second;
  return peer.handle_reply(conn, m);
}

seastar::future<> Heartbeat::handle_you_died()
{
  // TODO: ask for newer osdmap
  return seastar::now();
}

void Heartbeat::heartbeat_check()
{
  failure_queue_t failure_queue;
  const auto now = clock::now();
  for (const auto& [osd, peer] : peers) {
    auto failed_since = peer.failed_since(now);
    if (!clock::is_zero(failed_since)) {
      failure_queue.emplace(osd, failed_since);
    }
  }
  if (!failure_queue.empty()) {
    // send_failures can run in background, because
    // 	1. After the execution of send_failures, no msg is actually
    // 	   sent, which means the sending operation is not done,
    // 	   which further seems to involve problems risks that when
    // 	   osd shuts down, the left part of the sending operation
    // 	   may reference OSD and Heartbeat instances that are already
    // 	   deleted. However, remaining work of that sending operation
    // 	   involves no reference back to OSD or Heartbeat instances,
    // 	   which means it wouldn't involve the above risks.
    // 	2. messages are sent in order, if later checks find out
    // 	   the previous "failed" peers to be healthy, that "still
    // 	   alive" messages would be sent after the previous "osd
    // 	   failure" messages which is totally safe.
    (void)send_failures(std::move(failure_queue));
  }
}

seastar::future<> Heartbeat::send_heartbeats()
{
  const auto mnow = service.get_mnow();
  const auto now = clock::now();

  std::vector<seastar::future<>> futures;
  for (auto& [osd, peer] : peers) {
    peer.send_heartbeat(now, mnow, futures);
  }
  return seastar::when_all_succeed(futures.begin(), futures.end());
}

seastar::future<> Heartbeat::send_failures(failure_queue_t&& failure_queue)
{
  std::vector<seastar::future<>> futures;
  const auto now = clock::now();
  for (auto [osd, failed_since] : failure_queue) {
    if (failure_pending.count(osd)) {
      continue;
    }
    auto failed_for = chrono::duration_cast<chrono::seconds>(
	now - failed_since).count();
    auto osdmap = service.get_osdmap_service().get_map();
    auto failure_report =
	make_message<MOSDFailure>(monc.get_fsid(),
				  osd,
				  osdmap->get_addrs(osd),
				  static_cast<int>(failed_for),
				  osdmap->get_epoch());
    failure_pending.emplace(osd, failure_info_t{failed_since,
                                                osdmap->get_addrs(osd)});
    futures.push_back(monc.send_message(failure_report));
  }

  return seastar::when_all_succeed(futures.begin(), futures.end());
}

seastar::future<> Heartbeat::send_still_alive(osd_id_t osd,
                                              const entity_addrvec_t& addrs)
{
  auto still_alive = make_message<MOSDFailure>(
    monc.get_fsid(),
    osd,
    addrs,
    0,
    service.get_osdmap_service().get_map()->get_epoch(),
    MOSDFailure::FLAG_ALIVE);
  return monc.send_message(still_alive).then([=] {
    failure_pending.erase(osd);
    return seastar::now();
  });
}

void Heartbeat::print(std::ostream& out) const
{
  out << "heartbeat";
}

void Heartbeat::Peer::connect()
{
  logger().info("peer osd.{} added", peer);
  auto osdmap = heartbeat.service.get_osdmap_service().get_map();
  // TODO: use addrs
  con_front = heartbeat.front_msgr->connect(
      osdmap->get_hb_front_addrs(peer).front(), CEPH_ENTITY_TYPE_OSD);
  con_back = heartbeat.back_msgr->connect(
      osdmap->get_hb_back_addrs(peer).front(), CEPH_ENTITY_TYPE_OSD);
}

Heartbeat::Peer::Peer(Heartbeat& heartbeat, osd_id_t peer)
  : heartbeat(heartbeat), peer(peer)
{ connect(); }

void Heartbeat::Peer::disconnect()
{
  logger().info("peer osd.{} removed", peer);
  con_front->mark_down();
  con_back->mark_down();
}

Heartbeat::Peer::~Peer()
{ disconnect(); }

bool Heartbeat::Peer::is_unhealthy(clock::time_point now) const
{
  if (ping_history.empty()) {
    // we haven't sent a ping yet or we have got all replies,
    // in either way we are safe and healthy for now
    return false;
  } else {
    auto oldest_ping = ping_history.begin();
    return now > oldest_ping->second.deadline;
  }
}

bool Heartbeat::Peer::is_healthy(clock::time_point now) const
{
  if (con_front && clock::is_zero(last_rx_front)) {
    return false;
  }
  if (con_back && clock::is_zero(last_rx_back)) {
    return false;
  }
  // only declare to be healthy until we have received the first
  // replies from both front/back connections
  return !is_unhealthy(now);
}

Heartbeat::clock::time_point
Heartbeat::Peer::failed_since(clock::time_point now) const
{
  if (clock::is_zero(first_tx)) {
    return clock::zero();
  }
  if (!is_unhealthy(now)) {
    return clock::zero();
  }

  auto oldest_deadline = ping_history.begin()->second.deadline;
  auto failed_since = std::min(last_rx_back, last_rx_front);
  if (clock::is_zero(failed_since)) {
    logger().error("failed_since: no reply from osd.{} "
                   "ever on either front or back, first ping sent {} "
                   "(oldest deadline {})",
                   peer, first_tx, oldest_deadline);
    failed_since = first_tx;
  } else {
    logger().error("failed_since: no reply from osd.{} "
                   "since back {} front {} (oldest deadline {})",
                   peer, last_rx_back, last_rx_front, oldest_deadline);
  }
  return failed_since;
}

void Heartbeat::Peer::send_heartbeat(
    clock::time_point now,
    ceph::signedspan mnow,
    std::vector<seastar::future<>>& futures)
{
  if (clock::is_zero(first_tx)) {
    first_tx = now;
  }
  last_tx = now;

  const utime_t sent_stamp{now};
  const auto deadline =
    now + std::chrono::seconds(local_conf()->osd_heartbeat_grace);
  [[maybe_unused]] auto [reply, added] =
    ping_history.emplace(sent_stamp, reply_t{deadline, 0});
  for (auto& con : {con_front, con_back}) {
    if (con) {
      auto min_message = static_cast<uint32_t>(
        local_conf()->osd_heartbeat_min_size);
      auto ping = make_message<MOSDPing>(
        heartbeat.monc.get_fsid(),
        heartbeat.service.get_osdmap_service().get_map()->get_epoch(),
        MOSDPing::PING,
        sent_stamp,
        mnow,
        mnow,
        heartbeat.service.get_osdmap_service().get_up_epoch(),
        min_message);
      reply->second.unacknowledged++;
      futures.push_back(con->send(std::move(ping)));
    }
  }
}

seastar::future<> Heartbeat::Peer::handle_reply(
    crimson::net::Connection* conn, Ref<MOSDPing> m)
{
  auto ping = ping_history.find(m->ping_stamp);
  if (ping == ping_history.end()) {
    // old replies, deprecated by newly sent pings.
    return seastar::now();
  }
  const auto now = clock::now();
  auto& unacked = ping->second.unacknowledged;
  if (conn == con_back.get()) {
    last_rx_back = now;
    unacked--;
  } else if (conn == con_front.get()) {
    last_rx_front = now;
    unacked--;
  }
  if (unacked == 0) {
    ping_history.erase(ping_history.begin(), ++ping);
  }
  if (is_healthy(now)) {
    // cancel false reports
    if (auto pending = heartbeat.failure_pending.find(peer);
        pending != heartbeat.failure_pending.end()) {
      return heartbeat.send_still_alive(peer, pending->second.addrs);
    }
  }
  return seastar::now();
}

void Heartbeat::Peer::handle_reset(crimson::net::ConnectionRef conn)
{
  if (con_front != conn && con_back != conn) {
    return;
  }
  disconnect();
  first_tx = {};
  last_tx = {};
  last_rx_front = {};
  last_rx_back = {};
  ping_history = {};
  connect();
}
