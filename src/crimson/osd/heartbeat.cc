#include "heartbeat.h"

#include <boost/range/join.hpp>

#include "messages/MOSDPing.h"
#include "messages/MOSDFailure.h"

#include "crimson/common/auth_service.h"
#include "crimson/common/config_proxy.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/mon/MonClient.h"

#include "osd/OSDMap.h"

using ceph::common::local_conf;

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }

  template<typename Message, typename... Args>
  Ref<Message> make_message(Args&&... args)
  {
    return {new Message{std::forward<Args>(args)...}, false};
  }
}

Heartbeat::Heartbeat(const OSDMapService& service,
                     ceph::mon::Client& monc,
                     ceph::net::Messenger& front_msgr,
                     ceph::net::Messenger& back_msgr)
  : service{service},
    monc{monc},
    front_msgr{front_msgr},
    back_msgr{back_msgr},
    timer{[this] {send_heartbeats();}}
{}

seastar::future<> Heartbeat::start(entity_addrvec_t front_addrs,
                                   entity_addrvec_t back_addrs)
{
  logger().info("heartbeat: start");
  // i only care about the address, so any unused port would work
  for (auto& addr : boost::join(front_addrs.v, back_addrs.v)) {
    addr.set_port(0);
  }
  return seastar::when_all_succeed(start_messenger(front_msgr, front_addrs),
                                   start_messenger(back_msgr, back_addrs))
    .then([this] {
      timer.arm_periodic(
        std::chrono::seconds(local_conf()->osd_heartbeat_interval));
    });
}

seastar::future<>
Heartbeat::start_messenger(ceph::net::Messenger& msgr,
                           const entity_addrvec_t& addrs)
{
  if (local_conf()->ms_crc_data) {
    msgr.set_crc_data();
  }
  if (local_conf()->ms_crc_header) {
    msgr.set_crc_header();
  }
  return msgr.try_bind(addrs,
                       local_conf()->ms_bind_port_min,
                       local_conf()->ms_bind_port_max).then([&msgr, this] {
    return msgr.start(this);
  });
}

seastar::future<> Heartbeat::stop()
{
  return seastar::now();
}

const entity_addrvec_t& Heartbeat::get_front_addrs() const
{
  return front_msgr.get_myaddrs();
}

const entity_addrvec_t& Heartbeat::get_back_addrs() const
{
  return back_msgr.get_myaddrs();
}

seastar::future<> Heartbeat::add_peer(osd_id_t peer, epoch_t epoch)
{
  auto found = peers.find(peer);
  if (found == peers.end()) {
    logger().info("add_peer({})", peer);
    auto osdmap = service.get_map();
    // TODO: msgr v2
    return seastar::when_all_succeed(
        front_msgr.connect(osdmap->get_hb_front_addrs(peer).legacy_addr(),
                           CEPH_ENTITY_TYPE_OSD),
        back_msgr.connect(osdmap->get_hb_back_addrs(peer).legacy_addr(),
                          CEPH_ENTITY_TYPE_OSD))
      .then([this, peer, epoch] (auto xcon_front, auto xcon_back) {
        PeerInfo info;
        // sharded-messenger compatible mode
        info.con_front = xcon_front->release();
        info.con_back = xcon_back->release();
        info.epoch = epoch;
        peers.emplace(peer, std::move(info));
      });
  } else {
    found->second.epoch = epoch;
    return seastar::now();
  }
}

seastar::future<Heartbeat::osds_t> Heartbeat::remove_down_peers()
{
  osds_t osds;
  for (auto& peer : peers) {
    osds.push_back(peer.first);
  }
  return seastar::map_reduce(std::move(osds),
    [this](auto& osd) {
      auto osdmap = service.get_map();
      if (!osdmap->is_up(osd)) {
        return remove_peer(osd).then([] {
          return seastar::make_ready_future<osd_id_t>(-1);
        });
      } else if (peers[osd].epoch < osdmap->get_epoch()) {
        return seastar::make_ready_future<osd_id_t>(osd);
      } else {
        return seastar::make_ready_future<osd_id_t>(-1);
      }
    }, osds_t{},
    [this](osds_t&& extras, osd_id_t extra) {
      if (extra >= 0) {
        extras.push_back(extra);
      }
      return extras;
    });
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
  for (auto osd : want) {
    add_peer(osd, osdmap->get_epoch());
  }
}

seastar::future<> Heartbeat::update_peers(int whoami)
{
  const auto min_peers = static_cast<size_t>(
    local_conf().get_val<int64_t>("osd_heartbeat_min_peers"));
  return remove_down_peers().then([=](osds_t&& extra) {
    add_reporter_peers(whoami);
    // too many?
    struct iteration_state {
      osds_t::const_iterator where;
      osds_t::const_iterator end;
    };
    return seastar::do_with(iteration_state{extra.begin(),extra.end()},
      [=](iteration_state& s) {
        return seastar::do_until(
          [min_peers, &s, this] {
            return peers.size() < min_peers || s.where == s.end; },
          [&s, this] {
            return remove_peer(*s.where); }
        );
    });
  }).then([=] {
    // or too few?
    auto osdmap = service.get_map();
    for (auto next = osdmap->get_next_up_osd_after(whoami);
      peers.size() < min_peers && next >= 0 && next != whoami;
      next = osdmap->get_next_up_osd_after(next)) {
      add_peer(next, osdmap->get_epoch());
    }
    return seastar::now();
  });
}

seastar::future<> Heartbeat::remove_peer(osd_id_t peer)
{
  auto found = peers.find(peer);
  assert(found != peers.end());
  logger().info("remove_peer({})", peer);
  return seastar::when_all_succeed(found->second.con_front->close(),
                                   found->second.con_back->close()).then(
    [this, peer] {
      peers.erase(peer);
      return seastar::now();
    });
}

seastar::future<> Heartbeat::ms_dispatch(ceph::net::ConnectionRef conn,
                                         MessageRef m)
{
  switch (m->get_type()) {
  case MSG_OSD_PING:
    return handle_osd_ping(conn, boost::static_pointer_cast<MOSDPing>(m));
  default:
    return seastar::now();
  }
}

seastar::future<> Heartbeat::ms_handle_reset(ceph::net::ConnectionRef conn)
{
  auto found = std::find_if(peers.begin(), peers.end(),
                            [conn](const peers_map_t::value_type& peer) {
                              return (peer.second.con_front == conn ||
                                      peer.second.con_back == conn);
                            });
  if (found == peers.end()) {
    return seastar::now();
  }
  const auto peer = found->first;
  const auto epoch = found->second.epoch;
  return remove_peer(peer).then([peer, epoch, this] {
    return add_peer(peer, epoch);
  });
}

seastar::future<> Heartbeat::handle_osd_ping(ceph::net::ConnectionRef conn,
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

seastar::future<> Heartbeat::handle_ping(ceph::net::ConnectionRef conn,
                                         Ref<MOSDPing> m)
{
  auto min_message = static_cast<uint32_t>(
    local_conf()->osd_heartbeat_min_size);
  auto reply =
    make_message<MOSDPing>(m->fsid,
                           service.get_map()->get_epoch(),
                           MOSDPing::PING_REPLY,
                           m->stamp,
                           min_message);
  return conn->send(reply);
}

seastar::future<> Heartbeat::handle_reply(ceph::net::ConnectionRef conn,
                                          Ref<MOSDPing> m)
{
  const osd_id_t from = m->get_source().num();
  auto found = peers.find(from);
  if (found == peers.end()) {
    // stale reply
    return seastar::now();
  }
  auto& peer = found->second;
  auto ping = peer.ping_history.find(m->stamp);
  if (ping == peer.ping_history.end()) {
    // old replies, deprecated by newly sent pings.
    return seastar::now();
  }
  const auto now = clock::now();
  auto& unacked = ping->second.unacknowledged;
  if (conn == peer.con_back) {
    peer.last_rx_back = now;
    unacked--;
  } else if (conn == peer.con_front) {
    peer.last_rx_front = now;
    unacked--;
  }
  if (unacked == 0) {
    peer.ping_history.erase(peer.ping_history.begin(), ++ping);
  }
  if (peer.is_healthy(now)) {
    // cancel false reports
    failure_queue.erase(from);
    if (auto pending = failure_pending.find(from);
        pending != failure_pending.end()) {
      return send_still_alive(from, pending->second.addrs);
    }
  }
  return seastar::now();
}

seastar::future<> Heartbeat::handle_you_died()
{
  // TODO: ask for newer osdmap
  return seastar::now();
}

AuthAuthorizer* Heartbeat::ms_get_authorizer(peer_type_t peer) const
{
  return monc.get_authorizer(peer);
}

seastar::future<> Heartbeat::send_heartbeats()
{
  using peers_item_t = typename peers_map_t::value_type;
  return seastar::parallel_for_each(peers,
    [this](peers_item_t& item) {
      const auto now = clock::now();
      const auto deadline =
        now + std::chrono::seconds(local_conf()->osd_heartbeat_grace);
      auto& [peer, info] = item;
      info.last_tx = now;
      if (clock::is_zero(info.first_tx)) {
        info.first_tx = now;
      }
      const utime_t sent_stamp{now};
      auto [reply, added] = info.ping_history.emplace(sent_stamp,
                                                      reply_t{deadline, 0});
      std::vector<ceph::net::ConnectionRef> conns{info.con_front,
                                                  info.con_back};
      return seastar::parallel_for_each(std::move(conns),
        [sent_stamp, &reply=reply->second, this] (auto con) {
          if (con) {
            auto min_message = static_cast<uint32_t>(
              local_conf()->osd_heartbeat_min_size);
            auto ping = make_message<MOSDPing>(monc.get_fsid(),
                                               service.get_map()->get_epoch(),
                                               MOSDPing::PING,
                                               sent_stamp,
                                               min_message);
            return con->send(ping).then([&reply] {
              reply.unacknowledged++;
              return seastar::now();
            });
          } else {
            return seastar::now();
          }
        });
    });
}

seastar::future<> Heartbeat::send_failures()
{
  using failure_item_t = typename failure_queue_t::value_type;
  return seastar::parallel_for_each(failure_queue,
    [this](failure_item_t& failure_item) {
      auto [osd, failed_since] = failure_item;
      if (failure_pending.count(osd)) {
        return seastar::now();
      }
      auto failed_for = chrono::duration_cast<chrono::seconds>(
        clock::now() - failed_since).count();
      auto osdmap = service.get_map();
      auto failure_report =
        make_message<MOSDFailure>(monc.get_fsid(),
                                  osd,
                                  osdmap->get_addrs(osd),
                                  static_cast<int>(failed_for),
                                  osdmap->get_epoch());
      failure_pending.emplace(osd, failure_info_t{failed_since,
                                                  osdmap->get_addrs(osd)});
      return monc.send_message(failure_report);
    }).then([this] {
      failure_queue.clear();
      return seastar::now();
    });
}

seastar::future<> Heartbeat::send_still_alive(osd_id_t osd,
                                              const entity_addrvec_t& addrs)
{
  auto still_alive = make_message<MOSDFailure>(monc.get_fsid(),
                                               osd,
                                               addrs,
                                               0,
                                               service.get_map()->get_epoch(),
                                               MOSDFailure::FLAG_ALIVE);
  return monc.send_message(still_alive).then([=] {
    failure_pending.erase(osd);
    return seastar::now();
  });
}

bool Heartbeat::PeerInfo::is_unhealthy(clock::time_point now) const
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

bool Heartbeat::PeerInfo::is_healthy(clock::time_point now) const
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
