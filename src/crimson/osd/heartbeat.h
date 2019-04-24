// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <seastar/core/future.hh>
#include "common/ceph_time.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Fwd.h"

class MOSDPing;
class OSDMapService;

namespace ceph::mon {
  class Client;
}

template<typename Message> using Ref = boost::intrusive_ptr<Message>;

class Heartbeat : public ceph::net::Dispatcher {
public:
  using osd_id_t = int;

  Heartbeat(const OSDMapService& service,
	    ceph::mon::Client& monc,
	    ceph::net::Messenger& front_msgr,
	    ceph::net::Messenger& back_msgr);

  seastar::future<> start(entity_addrvec_t front,
			  entity_addrvec_t back);
  seastar::future<> stop();

  seastar::future<> add_peer(osd_id_t peer, epoch_t epoch);
  seastar::future<> update_peers(int whoami);
  seastar::future<> remove_peer(osd_id_t peer);

  seastar::future<> send_heartbeats();
  seastar::future<> send_failures();

  const entity_addrvec_t& get_front_addrs() const;
  const entity_addrvec_t& get_back_addrs() const;

  // Dispatcher methods
  seastar::future<> ms_dispatch(ceph::net::ConnectionRef conn,
				MessageRef m) override;
  seastar::future<> ms_handle_reset(ceph::net::ConnectionRef conn) override;
  AuthAuthorizer* ms_get_authorizer(peer_type_t peer) const override;

private:
  seastar::future<> handle_osd_ping(ceph::net::ConnectionRef conn,
				    Ref<MOSDPing> m);
  seastar::future<> handle_ping(ceph::net::ConnectionRef conn,
				Ref<MOSDPing> m);
  seastar::future<> handle_reply(ceph::net::ConnectionRef conn,
				 Ref<MOSDPing> m);
  seastar::future<> handle_you_died();

  seastar::future<> send_still_alive(osd_id_t, const entity_addrvec_t&);

  using osds_t = std::vector<osd_id_t>;
  /// remove down OSDs
  /// @return peers not needed in this epoch
  seastar::future<osds_t> remove_down_peers();
  /// add enough reporters for fast failure detection
  void add_reporter_peers(int whoami);

  seastar::future<> start_messenger(ceph::net::Messenger& msgr,
				    const entity_addrvec_t& addrs);
private:
  const OSDMapService& service;
  ceph::mon::Client& monc;
  ceph::net::Messenger& front_msgr;
  ceph::net::Messenger& back_msgr;

  seastar::timer<seastar::lowres_clock> timer;
  // use real_clock so it can be converted to utime_t
  using clock = ceph::coarse_real_clock;

  struct reply_t {
    clock::time_point deadline;
    // one sent over front conn, another sent over back conn
    uint8_t unacknowledged = 0;
  };
  struct PeerInfo {
    /// peer connection (front)
    ceph::net::ConnectionRef con_front;
    /// peer connection (back)
    ceph::net::ConnectionRef con_back;
    /// time we sent our first ping request
    clock::time_point first_tx;
    /// last time we sent a ping request
    clock::time_point last_tx;
    /// last time we got a ping reply on the front side
    clock::time_point last_rx_front;
    /// last time we got a ping reply on the back side
    clock::time_point last_rx_back;
    /// most recent epoch we wanted this peer
    epoch_t epoch;
    /// history of inflight pings, arranging by timestamp we sent
    std::map<utime_t, reply_t> ping_history;

    bool is_unhealthy(clock::time_point now) const;
    bool is_healthy(clock::time_point now) const;
  };
  using peers_map_t = std::map<osd_id_t, PeerInfo>;
  peers_map_t peers;

  // osds which are considered failed
  // osd_id => when was the last time that both front and back pings were acked
  //           use for calculating how long the OSD has been unresponsive
  using failure_queue_t = std::map<osd_id_t, clock::time_point>;
  failure_queue_t failure_queue;
  struct failure_info_t {
    clock::time_point failed_since;
    entity_addrvec_t addrs;
  };
  // osds we've reported to monior as failed ones, but they are not marked down
  // yet
  std::map<osd_id_t, failure_info_t> failure_pending;
};
