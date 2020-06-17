// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <seastar/core/future.hh>
#include "common/ceph_time.h"
#include "crimson/net/chained_dispatchers.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Fwd.h"

class MOSDPing;

namespace crimson::osd {
  class ShardServices;
}

namespace crimson::mon {
  class Client;
}

template<typename Message> using Ref = boost::intrusive_ptr<Message>;

class Heartbeat : public crimson::net::Dispatcher {
public:
  using osd_id_t = int;

  Heartbeat(osd_id_t whoami,
            const crimson::osd::ShardServices& service,
	    crimson::mon::Client& monc,
	    crimson::net::MessengerRef front_msgr,
	    crimson::net::MessengerRef back_msgr);

  seastar::future<> start(entity_addrvec_t front,
			  entity_addrvec_t back);
  seastar::future<> stop();

  void add_peer(osd_id_t peer, epoch_t epoch);
  void update_peers(int whoami);
  void remove_peer(osd_id_t peer);

  const entity_addrvec_t& get_front_addrs() const;
  const entity_addrvec_t& get_back_addrs() const;

  void set_require_authorizer(bool);

  // Dispatcher methods
  seastar::future<> ms_dispatch(crimson::net::Connection* conn,
				MessageRef m) override;
  void ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace) override;
  void ms_handle_connect(crimson::net::ConnectionRef conn) override;
  void ms_handle_accept(crimson::net::ConnectionRef conn) override;

  void print(std::ostream&) const;
private:
  seastar::future<> handle_osd_ping(crimson::net::Connection* conn,
				    Ref<MOSDPing> m);
  seastar::future<> handle_ping(crimson::net::Connection* conn,
				Ref<MOSDPing> m);
  seastar::future<> handle_reply(crimson::net::Connection* conn,
				 Ref<MOSDPing> m);
  seastar::future<> handle_you_died();

  using osds_t = std::vector<osd_id_t>;
  /// remove down OSDs
  /// @return peers not needed in this epoch
  osds_t remove_down_peers();
  /// add enough reporters for fast failure detection
  void add_reporter_peers(int whoami);

  seastar::future<> start_messenger(crimson::net::Messenger& msgr,
				    const entity_addrvec_t& addrs,
				    ChainedDispatchersRef);
private:
  const osd_id_t whoami;
  const crimson::osd::ShardServices& service;
  crimson::mon::Client& monc;
  crimson::net::MessengerRef front_msgr;
  crimson::net::MessengerRef back_msgr;

  seastar::timer<seastar::lowres_clock> timer;
  // use real_clock so it can be converted to utime_t
  using clock = ceph::coarse_real_clock;

  class Connector;
  class Connection;
  class Session;
  class Peer;
  using peers_map_t = std::map<osd_id_t, Peer>;
  peers_map_t peers;

  // osds which are considered failed
  // osd_id => when was the last time that both front and back pings were acked
  //           or sent.
  //           use for calculating how long the OSD has been unresponsive
  using failure_queue_t = std::map<osd_id_t, clock::time_point>;
  seastar::future<> send_failures(failure_queue_t&& failure_queue);
  seastar::future<> send_heartbeats();
  void heartbeat_check();

  // osds we've reported to monior as failed ones, but they are not marked down
  // yet
  crimson::common::Gated gate;

  class FailingPeers {
   public:
    FailingPeers(Heartbeat& heartbeat) : heartbeat(heartbeat) {}
    bool add_pending(osd_id_t peer,
                     clock::time_point failed_since,
                     clock::time_point now,
                     std::vector<seastar::future<>>& futures);
    seastar::future<> cancel_one(osd_id_t peer);

   private:
    seastar::future<> send_still_alive(osd_id_t, const entity_addrvec_t&);

    Heartbeat& heartbeat;

    struct failure_info_t {
      clock::time_point failed_since;
      entity_addrvec_t addrs;
    };
    std::map<osd_id_t, failure_info_t> failure_pending;
  } failing_peers;
};

inline std::ostream& operator<<(std::ostream& out, const Heartbeat& hb) {
  hb.print(out);
  return out;
}

class Heartbeat::Connector {
 public:
  Connector(size_t connections) : connections{connections} {}

  void increase_connected() {
    assert(connected < connections);
    ++connected;
    if (connected == connections) {
      all_connected();
    }
  }
  void decrease_connected() {
    assert(connected > 0);
    if (connected == connections) {
      connection_lost();
    }
    --connected;
  }
  enum class type_t { front, back };
  virtual entity_addr_t get_peer_addr(type_t) = 0;

 protected:
  virtual void all_connected() = 0;
  virtual void connection_lost() = 0;

 private:
  const size_t connections;
  size_t connected = 0;
};

class Heartbeat::Connection {
 public:
  using type_t = Connector::type_t;
  Connection(osd_id_t peer, bool is_winner_side, type_t type,
             crimson::net::Messenger& msgr, Connector& connector)
    : peer{peer}, is_winner_side{is_winner_side}, type{type},
      msgr{msgr}, connector{connector} {
    connect();
  }
  ~Connection();

  bool match(crimson::net::Connection* _conn) const;
  bool match(crimson::net::ConnectionRef conn) const {
    return match(conn.get());
  }
  void connected() {
    set_connected();
  }
  void accepted(crimson::net::ConnectionRef);
  void replaced();
  void reset();
  seastar::future<> send(MessageRef msg);
  void validate();
  // retry connection if still pending
  void retry();

 private:
  void set_connected();
  void connect();

  const osd_id_t peer;
  const bool is_winner_side;
  const type_t type;
  crimson::net::Messenger& msgr;
  Connector& connector;

  crimson::net::ConnectionRef conn;
  bool is_connected = false;
  bool racing_detected = false;

 friend std::ostream& operator<<(std::ostream& os, const Connection c) {
   if (c.type == type_t::front) {
     return os << "con_front(osd." << c.peer << ")";
   } else {
     return os << "con_back(osd." << c.peer << ")";
   }
 }
};

class Heartbeat::Session {
 public:
  Session(osd_id_t peer) : peer{peer} {}

  void set_epoch(epoch_t epoch_) { epoch = epoch_; }
  epoch_t get_epoch() const { return epoch; }
  bool is_started() const { return started; }
  bool pinged() const {
    if (clock::is_zero(first_tx)) {
      // i can never receive a pong without sending any ping message first.
      assert(clock::is_zero(last_rx_front) &&
             clock::is_zero(last_rx_back));
      return false;
    } else {
      return true;
    }
  }

  enum class health_state {
    UNKNOWN,
    UNHEALTHY,
    HEALTHY,
  };
  health_state do_health_screen(clock::time_point now) const {
    if (!pinged()) {
      // we are not healty nor unhealty because we haven't sent anything yet
      return health_state::UNKNOWN;
    } else if (!ping_history.empty() && ping_history.begin()->second.deadline < now) {
      return health_state::UNHEALTHY;
    } else if (!clock::is_zero(last_rx_front) &&
               !clock::is_zero(last_rx_back)) {
      // only declare to be healthy until we have received the first
      // replies from both front/back connections
      return health_state::HEALTHY;
    } else {
      return health_state::UNKNOWN;
    }
  }

  clock::time_point failed_since(clock::time_point now) const;

  void set_tx(clock::time_point now) {
    if (!pinged()) {
      first_tx = now;
    }
    last_tx = now;
  }

  void start() {
    assert(!started);
    started = true;
    ping_history.clear();
  }

  void emplace_history(const utime_t& sent_stamp,
                       const clock::time_point& deadline) {
    assert(started);
    [[maybe_unused]] auto [reply, added] =
      ping_history.emplace(sent_stamp, reply_t{deadline, 2});
  }

  bool handle_reply(const utime_t& ping_stamp,
                    Connection::type_t type,
                    clock::time_point now) {
    assert(started);
    auto ping = ping_history.find(ping_stamp);
    if (ping == ping_history.end()) {
      // old replies, deprecated by newly sent pings.
      return false;
    }
    auto& unacked = ping->second.unacknowledged;
    assert(unacked);
    if (type == Connection::type_t::front) {
      last_rx_front = now;
      unacked--;
    } else {
      last_rx_back = now;
      unacked--;
    }
    if (unacked == 0) {
      ping_history.erase(ping_history.begin(), ++ping);
    }
    return true;
  }

  void lost() {
    assert(started);
    started = false;
    if (!ping_history.empty()) {
      // we lost our ping_history of the last session, but still need to keep
      // the oldest deadline for unhealthy check.
      auto oldest = ping_history.begin();
      auto sent_stamp = oldest->first;
      auto deadline = oldest->second.deadline;
      ping_history.clear();
      ping_history.emplace(sent_stamp, reply_t{deadline, 0});
    }
  }

  // maintain an entry in ping_history for unhealthy check
  void set_inactive_history(clock::time_point);

 private:
  const osd_id_t peer;
  bool started = false;
  // time we sent our first ping request
  clock::time_point first_tx;
  // last time we sent a ping request
  clock::time_point last_tx;
  // last time we got a ping reply on the front side
  clock::time_point last_rx_front;
  // last time we got a ping reply on the back side
  clock::time_point last_rx_back;
  // most recent epoch we wanted this peer
  epoch_t epoch;

  struct reply_t {
    clock::time_point deadline;
    // one sent over front conn, another sent over back conn
    uint8_t unacknowledged = 0;
  };
  // history of inflight pings, arranging by timestamp we sent
  std::map<utime_t, reply_t> ping_history;
};

class Heartbeat::Peer final : private Heartbeat::Connector {
 public:
  Peer(Heartbeat&, osd_id_t);
  ~Peer();
  Peer(Peer&&) = delete;
  Peer(const Peer&) = delete;
  Peer& operator=(const Peer&) = delete;

  void set_epoch(epoch_t epoch) { session.set_epoch(epoch); }
  epoch_t get_epoch() const { return session.get_epoch(); }

  // if failure, return time_point since last active
  // else, return clock::zero()
  clock::time_point failed_since(clock::time_point now) const {
    return session.failed_since(now);
  }
  void send_heartbeat(
      clock::time_point, ceph::signedspan, std::vector<seastar::future<>>&);
  seastar::future<> handle_reply(crimson::net::Connection*, Ref<MOSDPing>);
  void handle_reset(crimson::net::ConnectionRef conn, bool is_replace) {
    for_each_conn([&] (auto& _conn) {
      if (_conn.match(conn)) {
        if (is_replace) {
          _conn.replaced();
        } else {
          _conn.reset();
        }
      }
    });
  }
  void handle_connect(crimson::net::ConnectionRef conn) {
    for_each_conn([&] (auto& _conn) {
      if (_conn.match(conn)) {
        _conn.connected();
      }
    });
  }
  void handle_accept(crimson::net::ConnectionRef conn) {
    for_each_conn([&] (auto& _conn) {
      _conn.accepted(conn);
    });
  }

 private:
  entity_addr_t get_peer_addr(type_t type) override;
  void all_connected() override;
  void connection_lost() override;
  void do_send_heartbeat(
      clock::time_point, ceph::signedspan, std::vector<seastar::future<>>*);

  template <typename Func>
  void for_each_conn(Func&& f) {
    f(con_front);
    f(con_back);
  }

  Heartbeat& heartbeat;
  const osd_id_t peer;
  Session session;
  // if need to send heartbeat when session started
  bool pending_send = false;
  Connection con_front;
  Connection con_back;
};
