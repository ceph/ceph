// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "heartbeat.h"

#include <boost/range/join.hpp>
#include <fmt/chrono.h>
#include <fmt/os.h>

#include "messages/MOSDPing.h"
#include "messages/MOSDFailure.h"

#include "crimson/common/config_proxy.h"
#include "crimson/common/formatter.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/osd/shard_services.h"
#include "crimson/mon/MonClient.h"

#include "osd/OSDMap.h"

using std::set;
using std::string;
using crimson::common::local_conf;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

Heartbeat::Heartbeat(osd_id_t whoami,
                     crimson::osd::ShardServices& service,
                     crimson::mon::Client& monc,
                     crimson::net::Messenger &front_msgr,
                     crimson::net::Messenger &back_msgr)
  : whoami{whoami},
    service{service},
    monc{monc},
    front_msgr{front_msgr},
    back_msgr{back_msgr},
    // do this in background
    timer{[this] {
      heartbeat_check();
      (void)send_heartbeats();
    }},
    failing_peers{*this}
{}

seastar::future<> Heartbeat::start(entity_addrvec_t front_addrs,
                                   entity_addrvec_t back_addrs)
{
  logger().info("heartbeat: start front_addrs={}, back_addrs={}",
                front_addrs, back_addrs);
  // i only care about the address, so any unused port would work
  for (auto& addr : boost::join(front_addrs.v, back_addrs.v)) {
    addr.set_port(0);
  }

  using crimson::net::SocketPolicy;
  front_msgr.set_policy(entity_name_t::TYPE_OSD,
                         SocketPolicy::lossy_client(0));
  back_msgr.set_policy(entity_name_t::TYPE_OSD,
                        SocketPolicy::lossy_client(0));
  return seastar::when_all_succeed(start_messenger(front_msgr,
						   front_addrs),
                                   start_messenger(back_msgr,
						   back_addrs))
    .then_unpack([this] {
      timer.arm_periodic(
        std::chrono::seconds(local_conf()->osd_heartbeat_interval));
    });
}

seastar::future<>
Heartbeat::start_messenger(crimson::net::Messenger& msgr,
                           const entity_addrvec_t& addrs)
{
  return msgr.bind(addrs).safe_then([this, &msgr]() mutable {
    return msgr.start({this});
  }, crimson::net::Messenger::bind_ertr::all_same_way(
      [addrs] (const std::error_code& e) {
    logger().error("heartbeat messenger bind({}): {}", addrs, e);
    ceph_abort();
  }));
}

seastar::future<> Heartbeat::stop()
{
  logger().info("{}", __func__);
  timer.cancel();
  front_msgr.stop();
  back_msgr.stop();
  return gate.close().then([this] {
    return seastar::when_all_succeed(front_msgr.shutdown(),
				     back_msgr.shutdown());
  }).then_unpack([] {
    return seastar::now();
  });
}

const entity_addrvec_t& Heartbeat::get_front_addrs() const
{
  return front_msgr.get_myaddrs();
}

const entity_addrvec_t& Heartbeat::get_back_addrs() const
{
  return back_msgr.get_myaddrs();
}

crimson::net::Messenger& Heartbeat::get_front_msgr() const
{
  return front_msgr;
}

crimson::net::Messenger& Heartbeat::get_back_msgr() const
{
  return back_msgr;
}

void Heartbeat::add_peer(osd_id_t _peer, epoch_t epoch)
{
  assert(whoami != _peer);
  auto [iter, added] = peers.try_emplace(_peer, *this, _peer);
  auto& peer = iter->second;
  peer.set_epoch_added(epoch);
}

Heartbeat::osds_t Heartbeat::remove_down_peers()
{
  osds_t old_osds; // osds not added in this epoch
  for (auto i = peers.begin(); i != peers.end(); ) {
    auto osdmap = service.get_map();
    const auto& [osd, peer] = *i;
    if (!osdmap->is_up(osd)) {
      i = peers.erase(i);
    } else {
      if (peer.get_epoch_added() < osdmap->get_epoch()) {
        old_osds.push_back(osd);
      }
      ++i;
    }
  }
  return old_osds;
}

void Heartbeat::add_reporter_peers(int whoami)
{
  auto osdmap = service.get_map();
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
  auto osdmap = service.get_map();
  auto epoch = osdmap->get_epoch();
  for (auto next = osdmap->get_next_up_osd_after(whoami);
    peers.size() < min_peers && next >= 0 && next != whoami;
    next = osdmap->get_next_up_osd_after(next)) {
    add_peer(next, epoch);
  }
}

Heartbeat::osds_t Heartbeat::get_peers() const
{
  osds_t osds;
  osds.reserve(peers.size());
  for (auto& peer : peers) {
    osds.push_back(peer.first);
  }
  return osds;
}

void Heartbeat::remove_peer(osd_id_t peer)
{
  assert(peers.count(peer) == 1);
  peers.erase(peer);
}

std::optional<seastar::future<>>
Heartbeat::ms_dispatch(crimson::net::ConnectionRef conn, MessageRef m)
{
  bool dispatched = true;
  gate.dispatch_in_background(__func__, *this, [this, conn, &m, &dispatched] {
    switch (m->get_type()) {
    case MSG_OSD_PING:
      return handle_osd_ping(conn, boost::static_pointer_cast<MOSDPing>(m));
    default:
      dispatched = false;
      return seastar::now();
    }
  });
  return (dispatched ? std::make_optional(seastar::now()) : std::nullopt);
}

void Heartbeat::ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace)
{
  auto peer = conn->get_peer_id();
  if (conn->get_peer_type() != entity_name_t::TYPE_OSD ||
      peer == entity_name_t::NEW) {
    return;
  }
  if (auto found = peers.find(peer);
      found != peers.end()) {
    found->second.handle_reset(conn, is_replace);
  }
}

void Heartbeat::ms_handle_connect(
    crimson::net::ConnectionRef conn,
    seastar::shard_id prv_shard)
{
  ceph_assert_always(seastar::this_shard_id() == prv_shard);
  auto peer = conn->get_peer_id();
  if (conn->get_peer_type() != entity_name_t::TYPE_OSD ||
      peer == entity_name_t::NEW) {
    return;
  }
  if (auto found = peers.find(peer);
      found != peers.end()) {
    found->second.handle_connect(conn);
  }
}

void Heartbeat::ms_handle_accept(
    crimson::net::ConnectionRef conn,
    seastar::shard_id prv_shard,
    bool is_replace)
{
  ceph_assert_always(seastar::this_shard_id() == prv_shard);
  auto peer = conn->get_peer_id();
  if (conn->get_peer_type() != entity_name_t::TYPE_OSD ||
      peer == entity_name_t::NEW) {
    return;
  }
  if (auto found = peers.find(peer);
      found != peers.end()) {
    found->second.handle_accept(conn, is_replace);
  }
}

seastar::future<> Heartbeat::handle_osd_ping(crimson::net::ConnectionRef conn,
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

seastar::future<> Heartbeat::handle_ping(crimson::net::ConnectionRef conn,
                                         Ref<MOSDPing> m)
{
  auto min_message = static_cast<uint32_t>(
    local_conf()->osd_heartbeat_min_size);
  auto reply =
    crimson::make_message<MOSDPing>(
      m->fsid,
      service.get_map()->get_epoch(),
      MOSDPing::PING_REPLY,
      m->ping_stamp,
      m->mono_ping_stamp,
      service.get_mnow(),
      service.get_up_epoch(),
      min_message);
  return conn->send(std::move(reply)
  ).then([this, m, conn] {
    return maybe_share_osdmap(conn, m);
  });
}

seastar::future<> Heartbeat::maybe_share_osdmap(
  crimson::net::ConnectionRef conn,
  Ref<MOSDPing> m)
{
  const osd_id_t from = m->get_source().num();
  const epoch_t current_osdmap_epoch = service.get_map()->get_epoch();
  auto found = peers.find(from);
  if (found == peers.end()) {
    return seastar::now();
  }
  auto& peer = found->second;

  if (m->map_epoch > peer.get_projected_epoch()) {
    logger().debug("{} updating peer {} session's projected_epoch"
                   "from {} to ping map epoch of {}",
                   __func__, from, peer.get_projected_epoch(),
                   m->map_epoch);
    peer.set_projected_epoch(m->map_epoch);
  }

  if (current_osdmap_epoch <= peer.get_projected_epoch()) {
    logger().debug("{} peer {} projected_epoch {} is already later "
		   "than our osdmap epoch of {}",
		   __func__ , from, peer.get_projected_epoch(),
		   current_osdmap_epoch);
    return seastar::now();
  }

  const epoch_t send_from = peer.get_projected_epoch() + 1;
  logger().debug("{} sending peer {} peer maps ({}, {}]",
		 __func__,
		 from,
		 send_from,
		 current_osdmap_epoch);
  peer.set_projected_epoch(current_osdmap_epoch);
  return service.send_incremental_map_to_osd(from, send_from);
}

seastar::future<> Heartbeat::handle_reply(crimson::net::ConnectionRef conn,
                                          Ref<MOSDPing> m)
{
  const osd_id_t from = m->get_source().num();
  auto found = peers.find(from);
  if (found == peers.end()) {
    // stale reply
    return seastar::now();
  }
  auto& peer = found->second;
  return peer.handle_reply(conn, m
  ).then([this, conn, m] {
    return maybe_share_osdmap(conn, m);
  });
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
    failing_peers.add_pending(osd, failed_since, now, futures);
  }

  return seastar::when_all_succeed(futures.begin(), futures.end());
}

void Heartbeat::print(std::ostream& out) const
{
  out << "heartbeat";
}

Heartbeat::Connection::~Connection()
{
  if (conn) {
    conn->mark_down();
  }
}

bool Heartbeat::Connection::matches(crimson::net::ConnectionRef _conn) const
{
  return (conn && conn == _conn);
}

bool Heartbeat::Connection::accepted(
    crimson::net::ConnectionRef accepted_conn,
    bool is_replace)
{
  ceph_assert(accepted_conn);
  ceph_assert(accepted_conn != conn);
  if (accepted_conn->get_peer_addr() != listener.get_peer_addr(type)) {
    return false;
  }

  if (is_replace) {
    logger().info("Heartbeat::Connection::accepted(): "
                  "{} racing", *this);
    racing_detected = true;
  }
  if (conn) {
    // there is no assumption about the ordering of the reset and accept
    // events for the 2 racing connections.
    if (is_connected) {
      logger().warn("Heartbeat::Connection::accepted(): "
                    "{} is accepted while connected, is_replace={}",
                    *this, is_replace);
      conn->mark_down();
      set_unconnected();
    }
  }
  conn = accepted_conn;
  set_connected();
  return true;
}

void Heartbeat::Connection::reset(bool is_replace)
{
  if (is_replace) {
    logger().info("Heartbeat::Connection::reset(): "
                  "{} racing, waiting for the replacing accept",
                  *this);
    racing_detected = true;
  }

  if (is_connected) {
    set_unconnected();
  } else {
    conn = nullptr;
  }

  if (is_replace) {
    // waiting for the replacing accept event
  } else if (!racing_detected || is_winner_side) {
    connect();
  } else { // racing_detected && !is_winner_side
    logger().info("Heartbeat::Connection::reset(): "
                  "{} racing detected and lose, "
                  "waiting for peer connect me", *this);
  }
}

seastar::future<> Heartbeat::Connection::send(MessageURef msg)
{
  assert(is_connected);
  return conn->send(std::move(msg));
}

void Heartbeat::Connection::validate()
{
  assert(is_connected);
  auto peer_addr = listener.get_peer_addr(type);
  if (conn->get_peer_addr() != peer_addr) {
    logger().info("Heartbeat::Connection::validate(): "
                  "{} has new address {} over {}, reset",
                  *this, peer_addr, conn->get_peer_addr());
    conn->mark_down();
    racing_detected = false;
    reset();
  }
}

void Heartbeat::Connection::retry()
{
  racing_detected = false;
  if (!is_connected) {
    if (conn) {
      conn->mark_down();
      reset();
    } else {
      connect();
    }
  }
}

void Heartbeat::Connection::set_connected()
{
  assert(conn);
  assert(!is_connected);
  ceph_assert(conn->is_connected());
  is_connected = true;
  listener.increase_connected();
}

void Heartbeat::Connection::set_unconnected()
{
  assert(conn);
  assert(is_connected);
  conn = nullptr;
  is_connected = false;
  listener.decrease_connected();
}

void Heartbeat::Connection::connect()
{
  assert(!conn);
  auto addr = listener.get_peer_addr(type);
  conn = msgr.connect(addr, entity_name_t(CEPH_ENTITY_TYPE_OSD, peer));
  if (conn->is_connected()) {
    set_connected();
  }
}

Heartbeat::clock::time_point
Heartbeat::Session::failed_since(Heartbeat::clock::time_point now) const
{
  if (do_health_screen(now) == health_state::UNHEALTHY) {
    auto oldest_deadline = ping_history.begin()->second.deadline;
    auto failed_since = std::min(last_rx_back, last_rx_front);
    if (clock::is_zero(failed_since)) {
      logger().error("Heartbeat::Session::failed_since(): no reply from osd.{} "
                     "ever on either front or back, first ping sent {} "
                     "(oldest deadline {})",
                     peer, first_tx, oldest_deadline);
      failed_since = first_tx;
    } else {
      logger().error("Heartbeat::Session::failed_since(): no reply from osd.{} "
                     "since back {} front {} (oldest deadline {})",
                     peer, last_rx_back, last_rx_front, oldest_deadline);
    }
    return failed_since;
  } else {
    return clock::zero();
  }
}

void Heartbeat::Session::set_inactive_history(clock::time_point now)
{
  assert(!connected);
  if (ping_history.empty()) {
    const utime_t sent_stamp{now};
    const auto deadline =
      now + std::chrono::seconds(local_conf()->osd_heartbeat_grace);
    ping_history.emplace(sent_stamp, reply_t{deadline, 0});
  } else { // the entry is already added
    assert(ping_history.size() == 1);
  }
}

Heartbeat::Peer::Peer(Heartbeat& heartbeat, osd_id_t peer)
  : ConnectionListener(2), heartbeat{heartbeat}, peer{peer}, session{peer},
  con_front(peer, heartbeat.whoami > peer, Connection::type_t::front,
            heartbeat.front_msgr, *this),
  con_back(peer, heartbeat.whoami > peer, Connection::type_t::back,
           heartbeat.back_msgr, *this)
{
  logger().info("Heartbeat::Peer: osd.{} added", peer);
}

Heartbeat::Peer::~Peer()
{
  logger().info("Heartbeat::Peer: osd.{} removed", peer);
}

void Heartbeat::Peer::send_heartbeat(
    clock::time_point now, ceph::signedspan mnow,
    std::vector<seastar::future<>>& futures)
{
  session.set_tx(now);
  if (session.is_started()) {
    do_send_heartbeat(now, mnow, &futures);
    for_each_conn([] (auto& conn) {
      conn.validate();
    });
  } else {
    // we should send MOSDPing but still cannot at this moment
    if (pending_send) {
      // we have already pending for a entire heartbeat interval
      logger().warn("Heartbeat::Peer::send_heartbeat(): "
                    "heartbeat to osd.{} is still pending...", peer);
      for_each_conn([] (auto& conn) {
        conn.retry();
      });
    } else {
      logger().info("Heartbeat::Peer::send_heartbeat(): "
                    "heartbeat to osd.{} is pending send...", peer);
      session.set_inactive_history(now);
      pending_send = true;
    }
  }
}

void Heartbeat::Peer::handle_reset(
    crimson::net::ConnectionRef conn, bool is_replace)
{
  int cnt = 0;
  for_each_conn([&] (auto& _conn) {
    if (_conn.matches(conn)) {
      ++cnt;
      _conn.reset(is_replace);
    }
  });

  if (cnt == 0) {
    logger().info("Heartbeat::Peer::handle_reset(): {} ignores conn, is_replace={} -- {}",
                  *this, is_replace, *conn);
  } else if (cnt > 1) {
    logger().error("Heartbeat::Peer::handle_reset(): {} handles conn {} times -- {}",
                  *this, cnt, *conn);
  }
}

void Heartbeat::Peer::handle_connect(crimson::net::ConnectionRef conn)
{
  int cnt = 0;
  for_each_conn([&] (auto& _conn) {
    if (_conn.matches(conn)) {
      ++cnt;
      _conn.connected();
    }
  });

  if (cnt == 0) {
    logger().error("Heartbeat::Peer::handle_connect(): {} ignores conn -- {}",
                   *this, *conn);
    conn->mark_down();
  } else if (cnt > 1) {
    logger().error("Heartbeat::Peer::handle_connect(): {} handles conn {} times -- {}",
                  *this, cnt, *conn);
  }
}

void Heartbeat::Peer::handle_accept(crimson::net::ConnectionRef conn, bool is_replace)
{
  int cnt = 0;
  for_each_conn([&] (auto& _conn) {
    if (_conn.accepted(conn, is_replace)) {
      ++cnt;
    }
  });

  if (cnt == 0) {
    logger().warn("Heartbeat::Peer::handle_accept(): {} ignores conn -- {}",
                  *this, *conn);
  } else if (cnt > 1) {
    logger().error("Heartbeat::Peer::handle_accept(): {} handles conn {} times -- {}",
                  *this, cnt, *conn);
  }
}

seastar::future<> Heartbeat::Peer::handle_reply(
    crimson::net::ConnectionRef conn, Ref<MOSDPing> m)
{
  if (!session.is_started()) {
    // we haven't sent any ping yet
    return seastar::now();
  }
  type_t type;
  if (con_front.matches(conn)) {
    type = type_t::front;
  } else if (con_back.matches(conn)) {
    type = type_t::back;
  } else {
    return seastar::now();
  }
  const auto now = clock::now();
  if (session.on_pong(m->ping_stamp, type, now)) {
    if (session.do_health_screen(now) == Session::health_state::HEALTHY) {
      return heartbeat.failing_peers.cancel_one(peer);
    }
  }
  return seastar::now();
}

entity_addr_t Heartbeat::Peer::get_peer_addr(type_t type)
{
  const auto osdmap = heartbeat.service.get_map();
  if (type == type_t::front) {
    return osdmap->get_hb_front_addrs(peer).front();
  } else {
    return osdmap->get_hb_back_addrs(peer).front();
  }
}

void Heartbeat::Peer::on_connected()
{
  logger().info("Heartbeat::Peer: osd.{} connected (send={})",
                peer, pending_send);
  session.on_connected();
  if (pending_send) {
    pending_send = false;
    do_send_heartbeat(clock::now(), heartbeat.service.get_mnow(), nullptr);
  }
}

void Heartbeat::Peer::on_disconnected()
{
  logger().info("Heartbeat::Peer: osd.{} disconnected", peer);
  session.on_disconnected();
}

void Heartbeat::Peer::do_send_heartbeat(
    Heartbeat::clock::time_point now,
    ceph::signedspan mnow,
    std::vector<seastar::future<>>* futures)
{
  const utime_t sent_stamp{now};
  const auto deadline =
    now + std::chrono::seconds(local_conf()->osd_heartbeat_grace);
  session.on_ping(sent_stamp, deadline);
  for_each_conn([&, this] (auto& conn) {
    auto min_message = static_cast<uint32_t>(
      local_conf()->osd_heartbeat_min_size);
    auto ping = crimson::make_message<MOSDPing>(
      heartbeat.monc.get_fsid(),
      heartbeat.service.get_map()->get_epoch(),
      MOSDPing::PING,
      sent_stamp,
      mnow,
      mnow,
      heartbeat.service.get_up_epoch(),
      min_message);
    if (futures) {
      futures->push_back(conn.send(std::move(ping)));
    }
  });
}

bool Heartbeat::FailingPeers::add_pending(
  osd_id_t peer,
  clock::time_point failed_since,
  clock::time_point now,
  std::vector<seastar::future<>>& futures)
{
  if (failure_pending.count(peer)) {
    return false;
  }
  auto failed_for = std::chrono::duration_cast<std::chrono::seconds>(
      now - failed_since).count();
  auto osdmap = heartbeat.service.get_map();
  auto failure_report =
      crimson::make_message<MOSDFailure>(heartbeat.monc.get_fsid(),
                                peer,
                                osdmap->get_addrs(peer),
                                static_cast<int>(failed_for),
                                osdmap->get_epoch());
  failure_pending.emplace(peer, failure_info_t{failed_since,
                                               osdmap->get_addrs(peer)});
  futures.push_back(heartbeat.monc.send_message(std::move(failure_report)));
  logger().info("{}: osd.{} failed for {}", __func__, peer, failed_for);
  return true;
}

seastar::future<> Heartbeat::FailingPeers::cancel_one(osd_id_t peer)
{
  if (auto pending = failure_pending.find(peer);
      pending != failure_pending.end()) {
    auto fut = send_still_alive(peer, pending->second.addrs);
    failure_pending.erase(peer);
    return fut;
  }
  return seastar::now();
}

seastar::future<>
Heartbeat::FailingPeers::send_still_alive(
    osd_id_t osd, const entity_addrvec_t& addrs)
{
  auto still_alive = crimson::make_message<MOSDFailure>(
    heartbeat.monc.get_fsid(),
    osd,
    addrs,
    0,
    heartbeat.service.get_map()->get_epoch(),
    MOSDFailure::FLAG_ALIVE);
  logger().info("{}: osd.{}", __func__, osd);
  return heartbeat.monc.send_message(std::move(still_alive));
}
