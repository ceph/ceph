// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CEPH_DPDK_TCP_H_
#define CEPH_DPDK_TCP_H_

#include <unordered_map>
#include <map>
#include <queue>
#include <functional>
#include <deque>
#include <chrono>
#include <random>
#include <stdexcept>
#include <system_error>

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>

#include "msg/async/dpdk/EventDPDK.h"

#include "include/utime.h"
#include "common/Throttle.h"
#include "common/ceph_time.h"
#include "msg/async/Event.h"
#include "IPChecksum.h"
#include "IP.h"
#include "const.h"
#include "byteorder.h"
#include "shared_ptr.h"
#include "PacketUtil.h"

struct tcp_hdr;

enum class tcp_state : uint16_t {
  CLOSED          = (1 << 0),
  LISTEN          = (1 << 1),
  SYN_SENT        = (1 << 2),
  SYN_RECEIVED    = (1 << 3),
  ESTABLISHED     = (1 << 4),
  FIN_WAIT_1      = (1 << 5),
  FIN_WAIT_2      = (1 << 6),
  CLOSE_WAIT      = (1 << 7),
  CLOSING         = (1 << 8),
  LAST_ACK        = (1 << 9),
  TIME_WAIT       = (1 << 10)
};

inline tcp_state operator|(tcp_state s1, tcp_state s2) {
  return tcp_state(uint16_t(s1) | uint16_t(s2));
}

inline std::ostream & operator<<(std::ostream & str, tcp_state s) {
  switch (s) {
    case tcp_state::CLOSED: return str << "CLOSED";
    case tcp_state::LISTEN: return str << "LISTEN";
    case tcp_state::SYN_SENT: return str << "SYN_SENT";
    case tcp_state::SYN_RECEIVED: return str << "SYN_RECEIVED";
    case tcp_state::ESTABLISHED: return str << "ESTABLISHED";
    case tcp_state::FIN_WAIT_1: return str << "FIN_WAIT_1";
    case tcp_state::FIN_WAIT_2: return str << "FIN_WAIT_2";
    case tcp_state::CLOSE_WAIT: return str << "CLOSE_WAIT";
    case tcp_state::CLOSING: return str << "CLOSING";
    case tcp_state::LAST_ACK: return str << "LAST_ACK";
    case tcp_state::TIME_WAIT: return str << "TIME_WAIT";
    default: return str << "UNKNOWN";
  }
}

struct tcp_option {
  // The kind and len field are fixed and defined in TCP protocol
  enum class option_kind: uint8_t { mss = 2, win_scale = 3, sack = 4, timestamps = 8,  nop = 1, eol = 0 };
  enum class option_len:  uint8_t { mss = 4, win_scale = 3, sack = 2, timestamps = 10, nop = 1, eol = 1 };
  struct mss {
    option_kind kind = option_kind::mss;
    option_len len = option_len::mss;
    uint16_t mss;
    struct mss hton() {
      struct mss m = *this;
      m.mss = ::hton(m.mss);
      return m;
    }
  } __attribute__((packed));
  struct win_scale {
    option_kind kind = option_kind::win_scale;
    option_len len = option_len::win_scale;
    uint8_t shift;
  } __attribute__((packed));
  struct sack {
    option_kind kind = option_kind::sack;
    option_len len = option_len::sack;
  } __attribute__((packed));
  struct timestamps {
    option_kind kind = option_kind::timestamps;
    option_len len = option_len::timestamps;
    uint32_t t1;
    uint32_t t2;
  } __attribute__((packed));
  struct nop {
    option_kind kind = option_kind::nop;
  } __attribute__((packed));
  struct eol {
    option_kind kind = option_kind::eol;
  } __attribute__((packed));
  static const uint8_t align = 4;

  void parse(uint8_t* beg, uint8_t* end);
  uint8_t fill(tcp_hdr* th, uint8_t option_size);
  uint8_t get_size(bool syn_on, bool ack_on);

  // For option negotiattion
  bool _mss_received = false;
  bool _win_scale_received = false;
  bool _timestamps_received = false;
  bool _sack_received = false;

  // Option data
  uint16_t _remote_mss = 536;
  uint16_t _local_mss;
  uint8_t _remote_win_scale = 0;
  uint8_t _local_win_scale = 0;
};
inline uint8_t*& operator+=(uint8_t*& x, tcp_option::option_len len) { x += uint8_t(len); return x; }
inline uint8_t& operator+=(uint8_t& x, tcp_option::option_len len) { x += uint8_t(len); return x; }

struct tcp_sequence {
  uint32_t raw;
};

tcp_sequence ntoh(tcp_sequence ts) {
  return tcp_sequence { ::ntoh(ts.raw) };
}

tcp_sequence hton(tcp_sequence ts) {
  return tcp_sequence { ::hton(ts.raw) };
}

inline std::ostream& operator<<(std::ostream& os, tcp_sequence s) {
  return os << s.raw;
}

inline tcp_sequence make_seq(uint32_t raw) { return tcp_sequence{raw}; }
inline tcp_sequence& operator+=(tcp_sequence& s, int32_t n) { s.raw += n; return s; }
inline tcp_sequence& operator-=(tcp_sequence& s, int32_t n) { s.raw -= n; return s; }
inline tcp_sequence operator+(tcp_sequence s, int32_t n) { return s += n; }
inline tcp_sequence operator-(tcp_sequence s, int32_t n) { return s -= n; }
inline int32_t operator-(tcp_sequence s, tcp_sequence q) { return s.raw - q.raw; }
inline bool operator==(tcp_sequence s, tcp_sequence q)  { return s.raw == q.raw; }
inline bool operator!=(tcp_sequence s, tcp_sequence q) { return !(s == q); }
inline bool operator<(tcp_sequence s, tcp_sequence q) { return s - q < 0; }
inline bool operator>(tcp_sequence s, tcp_sequence q) { return q < s; }
inline bool operator<=(tcp_sequence s, tcp_sequence q) { return !(s > q); }
inline bool operator>=(tcp_sequence s, tcp_sequence q) { return !(s < q); }

struct tcp_hdr {
  uint16_t src_port;
  uint16_t dst_port;
  tcp_sequence seq;
  tcp_sequence ack;
  uint8_t rsvd1 : 4;
  uint8_t data_offset : 4;
  uint8_t f_fin : 1;
  uint8_t f_syn : 1;
  uint8_t f_rst : 1;
  uint8_t f_psh : 1;
  uint8_t f_ack : 1;
  uint8_t f_urg : 1;
  uint8_t rsvd2 : 2;
  uint16_t window;
  uint16_t checksum;
  uint16_t urgent;

  tcp_hdr hton() {
    tcp_hdr hdr = *this;
    hdr.src_port = ::hton(src_port);
    hdr.dst_port = ::hton(dst_port);
    hdr.seq = ::hton(seq);
    hdr.ack = ::hton(ack);
    hdr.window = ::hton(window);
    hdr.checksum = ::hton(checksum);
    hdr.urgent = ::hton(urgent);
    return hdr;
  }

  tcp_hdr ntoh() {
    tcp_hdr hdr = *this;
    hdr.src_port = ::ntoh(src_port);
    hdr.dst_port = ::ntoh(dst_port);
    hdr.seq = ::ntoh(seq);
    hdr.ack = ::ntoh(ack);
    hdr.window = ::ntoh(window);
    hdr.checksum = ::ntoh(checksum);
    hdr.urgent = ::ntoh(urgent);
    return hdr;
  }
} __attribute__((packed));

struct tcp_tag {};
using tcp_packet_merger = packet_merger<tcp_sequence, tcp_tag>;

template <typename InetTraits>
class tcp {
 public:
  using ipaddr = typename InetTraits::address_type;
  using inet_type = typename InetTraits::inet_type;
  using connid = l4connid<InetTraits>;
  using connid_hash = typename connid::connid_hash;
  class connection;
  class listener;
 private:
  class tcb;

  class C_handle_delayed_ack : public EventCallback {
    tcb *tc;

   public:
    C_handle_delayed_ack(tcb *t): tc(t) { }
    void do_request(int r) {
      tc->_nr_full_seg_received = 0;
      tc->output();
    }
  };

  class C_handle_retransmit : public EventCallback {
    tcb *tc;

   public:
    C_handle_retransmit(tcb *t): tc(t) { }
    void do_request(int r) {
      tc->retransmit();
    }
  };

  class C_handle_persist : public EventCallback {
    tcb *tc;

   public:
    C_handle_persist(tcb *t): tc(t) { }
    void do_request(int r) {
      tc->persist();
    }
  };

  class C_all_data_acked : public EventCallback {
    tcb *tc;

   public:
    C_all_data_acked(tcb *t): tc(t) {}
    void do_request(int fd_or_id) {
      tc->close_final_cleanup();
    }
  };

  class C_actual_remove_tcb : public EventCallback {
    lw_shared_ptr<tcb> tc;
   public:
    C_actual_remove_tcb(tcb *t): tc(t->shared_from_this()) {}
    void do_request(int r) {
      delete this;
    }
  };

  class tcb : public enable_lw_shared_from_this<tcb> {
    using clock_type = ceph::coarse_real_clock;
    static constexpr tcp_state CLOSED         = tcp_state::CLOSED;
    static constexpr tcp_state LISTEN         = tcp_state::LISTEN;
    static constexpr tcp_state SYN_SENT       = tcp_state::SYN_SENT;
    static constexpr tcp_state SYN_RECEIVED   = tcp_state::SYN_RECEIVED;
    static constexpr tcp_state ESTABLISHED    = tcp_state::ESTABLISHED;
    static constexpr tcp_state FIN_WAIT_1     = tcp_state::FIN_WAIT_1;
    static constexpr tcp_state FIN_WAIT_2     = tcp_state::FIN_WAIT_2;
    static constexpr tcp_state CLOSE_WAIT     = tcp_state::CLOSE_WAIT;
    static constexpr tcp_state CLOSING        = tcp_state::CLOSING;
    static constexpr tcp_state LAST_ACK       = tcp_state::LAST_ACK;
    static constexpr tcp_state TIME_WAIT      = tcp_state::TIME_WAIT;
    tcp_state _state = CLOSED;
    tcp& _tcp;
    UserspaceEventManager &manager;
    connection* _conn = nullptr;
    bool _connect_done = false;
    ipaddr _local_ip;
    ipaddr _foreign_ip;
    uint16_t _local_port;
    uint16_t _foreign_port;
    struct unacked_segment {
      Packet p;
      uint16_t data_len;
      unsigned nr_transmits;
      clock_type::time_point tx_time;
    };
    struct send {
      tcp_sequence unacknowledged;
      tcp_sequence next;
      uint32_t window;
      uint8_t window_scale;
      uint16_t mss;
      tcp_sequence urgent;
      tcp_sequence wl1;
      tcp_sequence wl2;
      tcp_sequence initial;
      std::deque<unacked_segment> data;
      std::deque<Packet> unsent;
      uint32_t unsent_len = 0;
      uint32_t queued_len = 0;
      bool closed = false;
      // Wait for all data are acked
      int _all_data_acked_fd = -1;
      // Limit number of data queued into send queue
      Throttle user_queue_space;
      // Round-trip time variation
      std::chrono::microseconds rttvar;
      // Smoothed round-trip time
      std::chrono::microseconds srtt;
      bool first_rto_sample = true;
      clock_type::time_point syn_tx_time;
      // Congestion window
      uint32_t cwnd;
      // Slow start threshold
      uint32_t ssthresh;
      // Duplicated ACKs
      uint16_t dupacks = 0;
      unsigned syn_retransmit = 0;
      unsigned fin_retransmit = 0;
      uint32_t limited_transfer = 0;
      uint32_t partial_ack = 0;
      tcp_sequence recover;
      bool window_probe = false;
      send(CephContext *c): user_queue_space(c, "DPDK::tcp::tcb::user_queue_space", 81920) {}
    } _snd;
    struct receive {
      tcp_sequence next;
      uint32_t window;
      uint8_t window_scale;
      uint16_t mss;
      tcp_sequence urgent;
      tcp_sequence initial;
      std::deque<Packet> data;
      tcp_packet_merger out_of_order;
    } _rcv;
    EventCenter *center;
    int fd;
    // positive means no errno, 0 means eof, nagetive means error
    int16_t _errno = 1;
    tcp_option _option;
    EventCallbackRef delayed_ack_event;
    Tub<uint64_t> _delayed_ack_fd;
    // Retransmission timeout
    std::chrono::microseconds _rto{1000*1000};
    std::chrono::microseconds _persist_time_out{1000*1000};
    static constexpr std::chrono::microseconds _rto_min{1000*1000};
    static constexpr std::chrono::microseconds _rto_max{60000*1000};
    // Clock granularity
    static constexpr std::chrono::microseconds _rto_clk_granularity{1000};
    static constexpr uint16_t _max_nr_retransmit{5};
    EventCallbackRef retransmit_event;
    Tub<uint64_t> retransmit_fd;
    EventCallbackRef persist_event;
    EventCallbackRef all_data_ack_event;
    Tub<uint64_t> persist_fd;
    uint16_t _nr_full_seg_received = 0;
    struct isn_secret {
      // 512 bits secretkey for ISN generating
      uint32_t key[16];
      isn_secret () {
        std::random_device rd;
        std::default_random_engine e(rd());
        std::uniform_int_distribution<uint32_t> dist{};
        for (auto& k : key) {
          k = dist(e);
        }
      }
    };
    static isn_secret _isn_secret;
    tcp_sequence get_isn();
    circular_buffer<typename InetTraits::l4packet> _packetq;
    bool _poll_active = false;
   public:
    // callback
    void close_final_cleanup();
    ostream& _prefix(std::ostream *_dout);

   public:
    tcb(tcp& t, connid id);
    ~tcb();
    void input_handle_listen_state(tcp_hdr* th, Packet p);
    void input_handle_syn_sent_state(tcp_hdr* th, Packet p);
    void input_handle_other_state(tcp_hdr* th, Packet p);
    void output_one(bool data_retransmit = false);
    bool is_all_data_acked();
    int send(Packet p);
    void connect();
    Tub<Packet> read();
    void close();
    void remove_from_tcbs() {
      auto id = connid{_local_ip, _foreign_ip, _local_port, _foreign_port};
      _tcp._tcbs.erase(id);
    }
    Tub<typename InetTraits::l4packet> get_packet();
    void output() {
      if (!_poll_active) {
        _poll_active = true;

        auto tcb = this->shared_from_this();
        _tcp._inet.wait_l2_dst_address(_foreign_ip, Packet(), [tcb] (const ethernet_address &dst, Packet p, int r) {
          if (r == 0) {
            tcb->_tcp.poll_tcb(dst, std::move(tcb));
          } else if (r == -ETIMEDOUT) {
            // in other states connection should time out
            if (tcb->in_state(SYN_SENT)) {
              tcb->_errno = -ETIMEDOUT;
              tcb->cleanup();
            }
          } else if (r == -EBUSY) {
            // retry later
            tcb->_poll_active = false;
            tcb->start_retransmit_timer();
          }
        });
      }
    }

    int16_t get_errno() const {
      return _errno;
    }

    tcp_state& state() {
      return _state;
    }

    uint64_t peek_sent_available() {
      if (!in_state(ESTABLISHED))
        return 0;
      uint64_t left = _snd.user_queue_space.get_max() - _snd.user_queue_space.get_current();
      return left;
    }

    int is_connected() const {
      if (_errno <= 0)
        return _errno;
      return _connect_done;
    }

   private:
    void respond_with_reset(tcp_hdr* th);
    bool merge_out_of_order();
    void insert_out_of_order(tcp_sequence seq, Packet p);
    void trim_receive_data_after_window();
    bool should_send_ack(uint16_t seg_len);
    void clear_delayed_ack();
    Packet get_transmit_packet();
    void retransmit_one() {
      bool data_retransmit = true;
      output_one(data_retransmit);
    }
    void start_retransmit_timer() {
      if (retransmit_fd)
        center->delete_time_event(*retransmit_fd);
      retransmit_fd.construct(center->create_time_event(_rto.count(), retransmit_event));
    };
    void stop_retransmit_timer() {
      if (retransmit_fd) {
        center->delete_time_event(*retransmit_fd);
        retransmit_fd.destroy();
      }
    };
    void start_persist_timer() {
      if (persist_fd)
        center->delete_time_event(*persist_fd);
      persist_fd.construct(center->create_time_event(_persist_time_out.count(), persist_event));
    };
    void stop_persist_timer() {
      if (persist_fd) {
        center->delete_time_event(*persist_fd);
        persist_fd.destroy();
      }
    };
    void persist();
    void retransmit();
    void fast_retransmit();
    void update_rto(clock_type::time_point tx_time);
    void update_cwnd(uint32_t acked_bytes);
    void cleanup();
    uint32_t can_send() {
      if (_snd.window_probe) {
        return 1;
      }
      // Can not send more than advertised window allows
      auto x = std::min(uint32_t(_snd.unacknowledged + _snd.window - _snd.next), _snd.unsent_len);
      // Can not send more than congestion window allows
      x = std::min(_snd.cwnd, x);
      if (_snd.dupacks == 1 || _snd.dupacks == 2) {
        // RFC5681 Step 3.1
        // Send cwnd + 2 * smss per RFC3042
        auto flight = flight_size();
        auto max = _snd.cwnd + 2 * _snd.mss;
        x = flight <= max ? std::min(x, max - flight) : 0;
        _snd.limited_transfer += x;
      } else if (_snd.dupacks >= 3) {
        // RFC5681 Step 3.5
        // Sent 1 full-sized segment at most
        x = std::min(uint32_t(_snd.mss), x);
      }
      return x;
    }
    uint32_t flight_size() {
      uint32_t size = 0;
      std::for_each(_snd.data.begin(), _snd.data.end(),
                    [&] (unacked_segment& seg) { size += seg.p.len(); });
      return size;
    }
    uint16_t local_mss() {
      return _tcp.get_hw_features().mtu - tcp_hdr_len_min - InetTraits::ip_hdr_len_min;
    }
    void queue_packet(Packet p) {
      _packetq.emplace_back(
          typename InetTraits::l4packet{_foreign_ip, std::move(p)});
    }
    void signal_data_received() {
      manager.notify(fd, EVENT_READABLE);
    }
    void signal_all_data_acked() {
      if (_snd._all_data_acked_fd >= 0 && _snd.unsent_len == 0 && _snd.queued_len == 0)
        manager.notify(_snd._all_data_acked_fd, EVENT_READABLE);
    }
    void do_syn_sent() {
      _state = SYN_SENT;
      _snd.syn_tx_time = clock_type::now(_tcp.cct);
      // Send <SYN> to remote
      output();
    }
    void do_syn_received() {
      _state = SYN_RECEIVED;
      _snd.syn_tx_time = clock_type::now(_tcp.cct);
      // Send <SYN,ACK> to remote
      output();
    }
    void do_established() {
      _state = ESTABLISHED;
      update_rto(_snd.syn_tx_time);
      _connect_done = true;
      manager.notify(fd, EVENT_READABLE|EVENT_WRITABLE);
    }
    void do_reset() {
      _state = CLOSED;
      // Free packets to be sent which are waiting for user_queue_space
      _snd.user_queue_space.reset();
      cleanup();
      _errno = -ECONNRESET;
      manager.notify(fd, EVENT_READABLE);

      if (_snd._all_data_acked_fd >= 0)
        manager.notify(_snd._all_data_acked_fd, EVENT_READABLE);
    }
    void do_time_wait() {
      // FIXME: Implement TIME_WAIT state timer
      _state = TIME_WAIT;
      cleanup();
    }
    void do_closed() {
      _state = CLOSED;
      cleanup();
    }
    void do_setup_isn() {
      _snd.initial = get_isn();
      _snd.unacknowledged = _snd.initial;
      _snd.next = _snd.initial + 1;
      _snd.recover = _snd.initial;
    }
    void do_local_fin_acked() {
      _snd.unacknowledged += 1;
      _snd.next += 1;
    }
    bool syn_needs_on() {
      return in_state(SYN_SENT | SYN_RECEIVED);
    }
    bool fin_needs_on() {
      return in_state(FIN_WAIT_1 | CLOSING | LAST_ACK) && _snd.closed &&
             _snd.unsent_len == 0 && _snd.queued_len == 0;
    }
    bool ack_needs_on() {
      return !in_state(CLOSED | LISTEN | SYN_SENT);
    }
    bool foreign_will_not_send() {
      return in_state(CLOSING | TIME_WAIT | CLOSE_WAIT | LAST_ACK | CLOSED);
    }
    bool in_state(tcp_state state) {
      return uint16_t(_state) & uint16_t(state);
    }
    void exit_fast_recovery() {
      _snd.dupacks = 0;
      _snd.limited_transfer = 0;
      _snd.partial_ack = 0;
    }
    uint32_t data_segment_acked(tcp_sequence seg_ack);
    bool segment_acceptable(tcp_sequence seg_seq, unsigned seg_len);
    void init_from_options(tcp_hdr* th, uint8_t* opt_start, uint8_t* opt_end);
    friend class connection;

    friend class C_handle_delayed_ack;
    friend class C_handle_retransmit;
    friend class C_handle_persist;
    friend class C_all_data_acked;
  };

  CephContext *cct;
  // ipv4_l4<ip_protocol_num::tcp>
  inet_type& _inet;
  EventCenter *center;
  UserspaceEventManager &manager;
  std::unordered_map<connid, lw_shared_ptr<tcb>, connid_hash> _tcbs;
  std::unordered_map<uint16_t, listener*> _listening;
  std::random_device _rd;
  std::default_random_engine _e;
  std::uniform_int_distribution<uint16_t> _port_dist{41952, 65535};
  circular_buffer<std::pair<lw_shared_ptr<tcb>, ethernet_address>> _poll_tcbs;
  // queue for packets that do not belong to any tcb
  circular_buffer<ipv4_traits::l4packet> _packetq;
  Throttle _queue_space;
  // Limit number of data queued into send queue
 public:
  class connection {
    lw_shared_ptr<tcb> _tcb;
   public:
    explicit connection(lw_shared_ptr<tcb> tcbp) : _tcb(std::move(tcbp)) { _tcb->_conn = this; }
    connection(const connection&) = delete;
    connection(connection&& x) noexcept : _tcb(std::move(x._tcb)) {
      _tcb->_conn = this;
    }
    ~connection();
    void operator=(const connection&) = delete;
    connection& operator=(connection&& x) {
      if (this != &x) {
        this->~connection();
        new (this) connection(std::move(x));
      }
      return *this;
    }
    int fd() const {
      return _tcb->fd;
    }
    int send(Packet p) {
      return _tcb->send(std::move(p));
    }
    Tub<Packet> read() {
      return _tcb->read();
    }
    int16_t get_errno() const {
      return _tcb->get_errno();
    }
    void close_read();
    void close_write();
    entity_addr_t remote_addr() const {
      entity_addr_t addr;
      auto net_ip = _tcb->_foreign_ip.hton();
      memcpy((void*)&addr.in4_addr().sin_addr.s_addr,
             &net_ip, sizeof(addr.in4_addr().sin_addr.s_addr));
      addr.set_family(AF_INET);
      return addr;
    }
    uint64_t peek_sent_available() {
      return _tcb->peek_sent_available();
    }
    int is_connected() const { return _tcb->is_connected(); }
  };
  class listener {
    tcp& _tcp;
    uint16_t _port;
    int _fd = -1;
    int16_t _errno;
    queue<connection> _q;
    size_t _q_max_length;

   private:
    listener(tcp& t, uint16_t port, size_t queue_length)
        : _tcp(t), _port(port), _errno(0), _q(), _q_max_length(queue_length) {
    }
   public:
    listener(const listener&) = delete;
    void operator=(const listener&) = delete;
    listener(listener&& x)
        : _tcp(x._tcp), _port(x._port), _fd(std::move(x._fd)), _errno(x._errno),
          _q(std::move(x._q)) {
      if (_fd >= 0)
        _tcp._listening[_port] = this;
    }
    ~listener() {
      abort_accept();
    }
    int listen() {
      if (_tcp._listening.find(_port) != _tcp._listening.end())
        return -EADDRINUSE;
      _tcp._listening.emplace(_port, this);
      _fd = _tcp.manager.get_eventfd();
      return 0;
    }
    Tub<connection> accept() {
      Tub<connection> c;
      if (!_q.empty()) {
        c = std::move(_q.front());
        _q.pop();
      }
      return c;
    }
    void abort_accept() {
      while (!_q.empty())
        _q.pop();
      if (_fd >= 0) {
        _tcp._listening.erase(_port);
        _tcp.manager.close(_fd);
        _fd = -1;
      }
    }
    int16_t get_errno() const {
      return _errno;
    }
    bool full() const {
      return _q.size() == _q_max_length;
    }
    int fd() const {
      return _fd;
    }
    friend class tcp;
  };
 public:
  explicit tcp(CephContext *c, inet_type& inet, EventCenter *cen);
  void received(Packet p, ipaddr from, ipaddr to);
  bool forward(forward_hash& out_hash_data, Packet& p, size_t off);
  listener listen(uint16_t port, size_t queue_length = 100);
  connection connect(const entity_addr_t &addr);
  const hw_features& get_hw_features() const { return _inet._inet.get_hw_features(); }
  void poll_tcb(const ethernet_address &dst, lw_shared_ptr<tcb> tcb) {
    _poll_tcbs.emplace_back(std::move(tcb), dst);
  }
  bool push_listen_queue(uint16_t port, tcb *t) {
    auto listener = _listening.find(port);
    if (listener == _listening.end() || listener->second->full()) {
      return false;
    }
    listener->second->_q.push(connection(t->shared_from_this()));
    manager.notify(listener->second->_fd, EVENT_READABLE);
    return true;
  }

 private:
  void send_packet_without_tcb(ipaddr from, ipaddr to, Packet p);
  void respond_with_reset(tcp_hdr* rth, ipaddr local_ip, ipaddr foreign_ip);
  friend class listener;
};

template <typename InetTraits>
tcp<InetTraits>::tcp(CephContext *c, inet_type& inet, EventCenter *cen)
    : cct(c), _inet(inet), center(cen),
      manager(static_cast<DPDKDriver*>(cen->get_driver())->manager),
      _e(_rd()), _queue_space(cct, "DPDK::tcp::queue_space", 81920) {
  int tcb_polled = 0u;
  _inet.register_packet_provider([this, tcb_polled] () mutable {
    Tub<typename InetTraits::l4packet> l4p;
    auto c = _poll_tcbs.size();
    if (!_packetq.empty() && (!(tcb_polled % 128) || c == 0)) {
      l4p = std::move(_packetq.front());
      _packetq.pop_front();
      _queue_space.put(l4p->p.len());
    } else {
      while (c--) {
        tcb_polled++;
        lw_shared_ptr<tcb> tcb;
        ethernet_address dst;
        std::tie(tcb, dst) = std::move(_poll_tcbs.front());
        _poll_tcbs.pop_front();
        l4p = std::move(tcb->get_packet());
        if (l4p) {
          l4p->e_dst = dst;
          break;
        }
      }
    }
    return l4p;
  });
}

template <typename InetTraits>
auto tcp<InetTraits>::listen(uint16_t port, size_t queue_length) -> listener {
  return listener(*this, port, queue_length);
}

template <typename InetTraits>
typename tcp<InetTraits>::connection tcp<InetTraits>::connect(const entity_addr_t &addr) {
  uint16_t src_port;
  connid id;
  auto src_ip = _inet._inet.host_address();
  auto dst_ip = ipv4_address(addr);
  auto dst_port = addr.get_port();

  do {
    src_port = _port_dist(_e);
    id = connid{src_ip, dst_ip, src_port, (uint16_t)dst_port};
    if (_tcbs.find(id) == _tcbs.end()) {
      if (_inet._inet.netif()->hw_queues_count() == 1 ||
          _inet._inet.netif()->hash2cpu(
              id.hash(_inet._inet.netif()->rss_key())) == center->get_id())
        break;
    }
  } while (true);

  auto tcbp = make_lw_shared<tcb>(*this, id);
  _tcbs.insert({id, tcbp});
  tcbp->connect();
  return connection(tcbp);
}

template <typename InetTraits>
bool tcp<InetTraits>::forward(forward_hash& out_hash_data, Packet& p, size_t off) {
  auto th = p.get_header<tcp_hdr>(off);
  if (th) {
    out_hash_data.push_back(th->src_port);
    out_hash_data.push_back(th->dst_port);
  }
  return true;
}

template <typename InetTraits>
void tcp<InetTraits>::received(Packet p, ipaddr from, ipaddr to) {
  auto th = p.get_header<tcp_hdr>(0);
  if (!th) {
    return;
  }
  // th->data_offset is correct even before ntoh()
  if (unsigned(th->data_offset * 4) < sizeof(*th)) {
    return;
  }

  if (!get_hw_features().rx_csum_offload) {
    checksummer csum;
    InetTraits::tcp_pseudo_header_checksum(csum, from, to, p.len());
    csum.sum(p);
    if (csum.get() != 0) {
      return;
    }
  }
  auto h = th->ntoh();
  auto id = connid{to, from, h.dst_port, h.src_port};
  auto tcbi = _tcbs.find(id);
  lw_shared_ptr<tcb> tcbp;
  if (tcbi == _tcbs.end()) {
    auto listener = _listening.find(id.local_port);
    if (listener == _listening.end() || listener->second->full()) {
      // 1) In CLOSE state
      // 1.1 all data in the incoming segment is discarded.  An incoming
      // segment containing a RST is discarded. An incoming segment not
      // containing a RST causes a RST to be sent in response.
      // FIXME:
      //      if ACK off: <SEQ=0><ACK=SEG.SEQ+SEG.LEN><CTL=RST,ACK>
      //      if ACK on:  <SEQ=SEG.ACK><CTL=RST>
      return respond_with_reset(&h, id.local_ip, id.foreign_ip);
    } else {
      // 2) In LISTEN state
      // 2.1 first check for an RST
      if (h.f_rst) {
        // An incoming RST should be ignored
        return;
      }
      // 2.2 second check for an ACK
      if (h.f_ack) {
        // Any acknowledgment is bad if it arrives on a connection
        // still in the LISTEN state.
        // <SEQ=SEG.ACK><CTL=RST>
        return respond_with_reset(&h, id.local_ip, id.foreign_ip);
      }
      // 2.3 third check for a SYN
      if (h.f_syn) {
        // check the security
        // NOTE: Ignored for now
        tcbp = make_lw_shared<tcb>(*this, id);
        _tcbs.insert({id, tcbp});
        return tcbp->input_handle_listen_state(&h, std::move(p));
      }
      // 2.4 fourth other text or control
      // So you are unlikely to get here, but if you do, drop the
      // segment, and return.
      return;
    }
  } else {
    tcbp = tcbi->second;
    if (tcbp->state() == tcp_state::SYN_SENT) {
      // 3) In SYN_SENT State
      return tcbp->input_handle_syn_sent_state(&h, std::move(p));
    } else {
      // 4) In other state, can be one of the following:
      // SYN_RECEIVED, ESTABLISHED, FIN_WAIT_1, FIN_WAIT_2
      // CLOSE_WAIT, CLOSING, LAST_ACK, TIME_WAIT
      return tcbp->input_handle_other_state(&h, std::move(p));
    }
  }
}

// Send packet does not belong to any tcb
template <typename InetTraits>
void tcp<InetTraits>::send_packet_without_tcb(ipaddr from, ipaddr to, Packet p) {
  if (_queue_space.get_or_fail(p.len())) { // drop packets that do not fit the queue
    _inet.wait_l2_dst_address(to, std::move(p), [this, to] (const ethernet_address &e_dst, Packet p, int r) mutable {
      if (r == 0)
        _packetq.emplace_back(ipv4_traits::l4packet{to, std::move(p), e_dst, ip_protocol_num::tcp});
    });
  }
}

template <typename InetTraits>
tcp<InetTraits>::connection::~connection() {
  if (_tcb) {
    _tcb->_conn = nullptr;
    close_read();
    close_write();
  }
}

template <typename InetTraits>
tcp<InetTraits>::tcb::tcb(tcp& t, connid id)
    : _tcp(t), manager(t.manager), _local_ip(id.local_ip) , _foreign_ip(id.foreign_ip),
      _local_port(id.local_port), _foreign_port(id.foreign_port),
      _snd(_tcp.cct),
      center(t.center),
      fd(t.manager.get_eventfd()),
      delayed_ack_event(new tcp<InetTraits>::C_handle_delayed_ack(this)),
      retransmit_event(new tcp<InetTraits>::C_handle_retransmit(this)),
      persist_event(new tcp<InetTraits>::C_handle_persist(this)),
      all_data_ack_event(new tcp<InetTraits>::C_all_data_acked(this)) {}

template <typename InetTraits>
tcp<InetTraits>::tcb::~tcb()
{
  if (_delayed_ack_fd)
    center->delete_time_event(*_delayed_ack_fd);
  if (retransmit_fd)
    center->delete_time_event(*retransmit_fd);
  if (persist_fd)
    center->delete_time_event(*persist_fd);
  delete delayed_ack_event;
  delete retransmit_event;
  delete persist_event;
  delete all_data_ack_event;
  manager.close(fd);
  fd = -1;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::respond_with_reset(tcp_hdr* rth)
{
  _tcp.respond_with_reset(rth, _local_ip, _foreign_ip);
}

template <typename InetTraits>
uint32_t tcp<InetTraits>::tcb::data_segment_acked(tcp_sequence seg_ack) {
  uint32_t total_acked_bytes = 0;
  // Full ACK of segment
  while (!_snd.data.empty()
         && (_snd.unacknowledged + _snd.data.front().p.len() <= seg_ack)) {
    auto acked_bytes = _snd.data.front().p.len();
    _snd.unacknowledged += acked_bytes;
    // Ignore retransmitted segments when setting the RTO
    if (_snd.data.front().nr_transmits == 0) {
      update_rto(_snd.data.front().tx_time);
    }
    update_cwnd(acked_bytes);
    total_acked_bytes += acked_bytes;
    _snd.user_queue_space.put(_snd.data.front().data_len);
    manager.notify(fd, EVENT_WRITABLE);
    _snd.data.pop_front();
  }
  // Partial ACK of segment
  if (_snd.unacknowledged < seg_ack) {
    auto acked_bytes = seg_ack - _snd.unacknowledged;
    if (!_snd.data.empty()) {
      auto& unacked_seg = _snd.data.front();
      unacked_seg.p.trim_front(acked_bytes);
    }
    _snd.unacknowledged = seg_ack;
    update_cwnd(acked_bytes);
    total_acked_bytes += acked_bytes;
  }
  return total_acked_bytes;
}

template <typename InetTraits>
bool tcp<InetTraits>::tcb::segment_acceptable(tcp_sequence seg_seq, unsigned seg_len) {
  if (seg_len == 0 && _rcv.window == 0) {
    // SEG.SEQ = RCV.NXT
    return seg_seq == _rcv.next;
  } else if (seg_len == 0 && _rcv.window > 0) {
    // RCV.NXT =< SEG.SEQ < RCV.NXT+RCV.WND
    return (_rcv.next <= seg_seq) && (seg_seq < _rcv.next + _rcv.window);
  } else if (seg_len > 0 && _rcv.window > 0) {
    // RCV.NXT =< SEG.SEQ < RCV.NXT+RCV.WND
    //    or
    // RCV.NXT =< SEG.SEQ+SEG.LEN-1 < RCV.NXT+RCV.WND
    bool x = (_rcv.next <= seg_seq) && seg_seq < (_rcv.next + _rcv.window);
    bool y = (_rcv.next <= seg_seq + seg_len - 1) && (seg_seq + seg_len - 1 < _rcv.next + _rcv.window);
    return x || y;
  } else  {
    // SEG.LEN > 0 RCV.WND = 0, not acceptable
    return false;
  }
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::init_from_options(tcp_hdr* th, uint8_t* opt_start, uint8_t* opt_end) {
  // Handle tcp options
  _option.parse(opt_start, opt_end);

  // Remote receive window scale factor
  _snd.window_scale = _option._remote_win_scale;
  // Local receive window scale factor
  _rcv.window_scale = _option._local_win_scale;

  // Maximum segment size remote can receive
  _snd.mss = _option._remote_mss;
  // Maximum segment size local can receive
  _rcv.mss = _option._local_mss = local_mss();

  // Linux's default window size
  _rcv.window = 29200 << _rcv.window_scale;
  _snd.window = th->window << _snd.window_scale;

  // Segment sequence number used for last window update
  _snd.wl1 = th->seq;
  // Segment acknowledgment number used for last window update
  _snd.wl2 = th->ack;

  // Setup initial congestion window
  if (2190 < _snd.mss) {
    _snd.cwnd = 2 * _snd.mss;
  } else if (1095 < _snd.mss && _snd.mss <= 2190) {
    _snd.cwnd = 3 * _snd.mss;
  } else {
    _snd.cwnd = 4 * _snd.mss;
  }

  // Setup initial slow start threshold
  _snd.ssthresh = th->window << _snd.window_scale;
}

template <typename InetTraits>
Packet tcp<InetTraits>::tcb::get_transmit_packet() {
  // easy case: empty queue
  if (_snd.unsent.empty()) {
    return Packet();
  }
  auto can_send = this->can_send();
  // Max number of TCP payloads we can pass to NIC
  uint32_t len;
  if (_tcp.get_hw_features().tx_tso) {
    // FIXME: Info tap device the size of the splitted packet
    len = _tcp.get_hw_features().max_packet_len - tcp_hdr_len_min - InetTraits::ip_hdr_len_min;
  } else {
    len = std::min(uint16_t(_tcp.get_hw_features().mtu - tcp_hdr_len_min - InetTraits::ip_hdr_len_min), _snd.mss);
  }
  can_send = std::min(can_send, len);
  // easy case: one small packet
  if (_snd.unsent.front().len() <= can_send) {
    auto p = std::move(_snd.unsent.front());
    _snd.unsent.pop_front();
    _snd.unsent_len -= p.len();
    return p;
  }
  // moderate case: need to split one packet
  if (_snd.unsent.front().len() > can_send) {
    auto p = _snd.unsent.front().share(0, can_send);
    _snd.unsent.front().trim_front(can_send);
    _snd.unsent_len -= p.len();
    return p;
  }
  // hard case: merge some packets, possibly split last
  auto p = std::move(_snd.unsent.front());
  _snd.unsent.pop_front();
  can_send -= p.len();
  while (!_snd.unsent.empty()
         && _snd.unsent.front().len() <= can_send) {
    can_send -= _snd.unsent.front().len();
    p.append(std::move(_snd.unsent.front()));
    _snd.unsent.pop_front();
  }
  // FIXME: this will result in calling "deleter" of packet which free managed objects
  // will used later
  // if (!_snd.unsent.empty() && can_send) {
  //   auto& q = _snd.unsent.front();
  //   p.append(q.share(0, can_send));
  //   q.trim_front(can_send);
  // }
  _snd.unsent_len -= p.len();
  return p;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::output_one(bool data_retransmit) {
  if (in_state(CLOSED)) {
    return;
  }

  Packet p = data_retransmit ? _snd.data.front().p.share() : get_transmit_packet();
  Packet clone = p.share();  // early clone to prevent share() from calling packet::unuse_internal_data() on header.
  uint16_t len = p.len();
  bool syn_on = syn_needs_on();
  bool ack_on = ack_needs_on();

  auto options_size = _option.get_size(syn_on, ack_on);
  auto th = p.prepend_header<tcp_hdr>(options_size);

  th->src_port = _local_port;
  th->dst_port = _foreign_port;

  th->f_syn = syn_on;
  th->f_ack = ack_on;
  if (ack_on) {
    clear_delayed_ack();
  }
  th->f_urg = false;
  th->f_psh = false;

  tcp_sequence seq;
  if (data_retransmit) {
    seq = _snd.unacknowledged;
  } else {
    seq = syn_on ? _snd.initial : _snd.next;
    _snd.next += len;
  }
  th->seq = seq;
  th->ack = _rcv.next;
  th->data_offset = (sizeof(*th) + options_size) / 4;
  th->window = _rcv.window >> _rcv.window_scale;
  th->checksum = 0;

  // FIXME: does the FIN have to fit in the window?
  bool fin_on = fin_needs_on();
  th->f_fin = fin_on;

  // Add tcp options
  _option.fill(th, options_size);
  *th = th->hton();

  offload_info oi;
  checksummer csum;
  uint16_t pseudo_hdr_seg_len = 0;

  oi.tcp_hdr_len = sizeof(tcp_hdr) + options_size;

  if (_tcp.get_hw_features().tx_csum_l4_offload) {
    oi.needs_csum = true;

    //
    // tx checksum offloading: both virtio-net's VIRTIO_NET_F_CSUM dpdk's
    // PKT_TX_TCP_CKSUM - requires th->checksum to be initialized to ones'
    // complement sum of the pseudo header.
    //
    // For TSO the csum should be calculated for a pseudo header with
    // segment length set to 0. All the rest is the same as for a TCP Tx
    // CSUM offload case.
    //
    if (_tcp.get_hw_features().tx_tso && len > _snd.mss) {
      oi.tso_seg_size = _snd.mss;
    } else {
      pseudo_hdr_seg_len = sizeof(*th) + options_size + len;
    }
  } else {
    pseudo_hdr_seg_len = sizeof(*th) + options_size + len;
    oi.needs_csum = false;
  }

  InetTraits::tcp_pseudo_header_checksum(csum, _local_ip, _foreign_ip,
                                         pseudo_hdr_seg_len);

  if (_tcp.get_hw_features().tx_csum_l4_offload) {
    th->checksum = ~csum.get();
  } else {
    csum.sum(p);
    th->checksum = csum.get();
  }

  oi.protocol = ip_protocol_num::tcp;

  p.set_offload_info(oi);

  if (!data_retransmit && (len || syn_on || fin_on)) {
    auto now = clock_type::now(_tcp.cct);
    if (len) {
      unsigned nr_transmits = 0;
      _snd.data.emplace_back(unacked_segment{std::move(clone),
                                             len, nr_transmits, now});
    }
    if (!retransmit_fd) {
      start_retransmit_timer();
    }
  }

  queue_packet(std::move(p));
}

template <typename InetTraits>
bool tcp<InetTraits>::tcb::is_all_data_acked() {
  if (_snd.data.empty() && _snd.unsent_len == 0 && _snd.queued_len == 0) {
    return true;
  }
  return false;
}

template <typename InetTraits>
Tub<Packet> tcp<InetTraits>::tcb::read() {
  Tub<Packet> p;
  if (_rcv.data.empty())
    return p;

  p.construct();
  for (auto&& q : _rcv.data) {
    p->append(std::move(q));
  }
  _rcv.data.clear();
  return p;
}

template <typename InetTraits>
int tcp<InetTraits>::tcb::send(Packet p) {
  // We can not send after the connection is closed
  assert(!_snd.closed);

  if (in_state(CLOSED))
    return -ECONNRESET;

  auto len = p.len();
  if (!_snd.user_queue_space.get_or_fail(len)) {
    // note: caller must ensure enough queue space to send
    assert(0);
  }
  // TODO: Handle p.len() > max user_queue_space case
  _snd.queued_len += len;
  _snd.unsent_len += len;
  _snd.queued_len -= len;
  _snd.unsent.push_back(std::move(p));
  if (can_send() > 0) {
    output();
  }
  return len;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::close() {
  if (in_state(CLOSED) || _snd.closed) {
    return ;
  }
  // TODO: We should make this asynchronous

  _errno = -EPIPE;
  center->delete_file_event(fd, EVENT_READABLE|EVENT_WRITABLE);
  bool acked = is_all_data_acked();
  if (!acked) {
    _snd._all_data_acked_fd = manager.get_eventfd();
    center->create_file_event(_snd._all_data_acked_fd, EVENT_READABLE, all_data_ack_event);
  } else {
    close_final_cleanup();
  }
}

template <typename InetTraits>
bool tcp<InetTraits>::tcb::should_send_ack(uint16_t seg_len) {
  // We've received a TSO packet, do ack immediately
  if (seg_len > _rcv.mss) {
    _nr_full_seg_received = 0;
    if (_delayed_ack_fd) {
      center->delete_time_event(*_delayed_ack_fd);
      _delayed_ack_fd.destroy();
    }
    return true;
  }

  // We've received a full sized segment, ack for every second full sized segment
  if (seg_len == _rcv.mss) {
    if (_nr_full_seg_received++ >= 1) {
      _nr_full_seg_received = 0;
      if (_delayed_ack_fd) {
        center->delete_time_event(*_delayed_ack_fd);
        _delayed_ack_fd.destroy();
      }
      return true;
    }
  }

  // If the timer is armed and its callback hasn't been run.
  if (_delayed_ack_fd) {
    return false;
  }

  // If the timer is not armed, schedule a delayed ACK.
  // The maximum delayed ack timer allowed by RFC1122 is 500ms, most
  // implementations use 200ms.
  _delayed_ack_fd.construct(center->create_time_event(200*1000, delayed_ack_event));
  return false;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::clear_delayed_ack() {
  if (_delayed_ack_fd) {
    center->delete_time_event(*_delayed_ack_fd);
    _delayed_ack_fd.destroy();
  }
}

template <typename InetTraits>
bool tcp<InetTraits>::tcb::merge_out_of_order() {
  bool merged = false;
  if (_rcv.out_of_order.map.empty()) {
    return merged;
  }
  for (auto it = _rcv.out_of_order.map.begin(); it != _rcv.out_of_order.map.end();) {
    auto& p = it->second;
    auto seg_beg = it->first;
    auto seg_len = p.len();
    auto seg_end = seg_beg + seg_len;
    if (seg_beg <= _rcv.next && seg_end > _rcv.next) {
      // This segment has been received out of order and its previous
      // segment has been received now
      auto trim = _rcv.next - seg_beg;
      if (trim) {
        p.trim_front(trim);
        seg_len -= trim;
      }
      _rcv.next += seg_len;
      _rcv.data.push_back(std::move(p));
      // Since c++11, erase() always returns the value of the following element
      it = _rcv.out_of_order.map.erase(it);
      merged = true;
    } else if (_rcv.next >= seg_end) {
      // This segment has been receive already, drop it
      it = _rcv.out_of_order.map.erase(it);
    } else {
      // seg_beg > _rcv.need, can not merge. Note, seg_beg can grow only,
      // so we can stop looking here.
      it++;
      break;
    }
  }
  return merged;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::insert_out_of_order(tcp_sequence seg, Packet p) {
  _rcv.out_of_order.merge(seg, std::move(p));
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::trim_receive_data_after_window() {
  abort();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::fast_retransmit() {
  if (!_snd.data.empty()) {
    auto& unacked_seg = _snd.data.front();
    unacked_seg.nr_transmits++;
    retransmit_one();
    output();
  }
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::update_rto(clock_type::time_point tx_time) {
  // Update RTO according to RFC6298
  auto R = std::chrono::duration_cast<std::chrono::microseconds>(clock_type::now(_tcp.cct) - tx_time);
  if (_snd.first_rto_sample) {
    _snd.first_rto_sample = false;
    // RTTVAR <- R/2
    // SRTT <- R
    _snd.rttvar = R / 2;
    _snd.srtt = R;
  } else {
    // RTTVAR <- (1 - beta) * RTTVAR + beta * |SRTT - R'|
    // SRTT <- (1 - alpha) * SRTT + alpha * R'
    // where alpha = 1/8 and beta = 1/4
    auto delta = _snd.srtt > R ? (_snd.srtt - R) : (R - _snd.srtt);
    _snd.rttvar = _snd.rttvar * 3 / 4 + delta / 4;
    _snd.srtt = _snd.srtt * 7 / 8 +  R / 8;
  }
  // RTO <- SRTT + max(G, K * RTTVAR)
  _rto =  _snd.srtt + std::max(_rto_clk_granularity, 4 * _snd.rttvar);

  // Make sure 1 sec << _rto << 60 sec
  _rto = std::max(_rto, _rto_min);
  _rto = std::min(_rto, _rto_max);
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::update_cwnd(uint32_t acked_bytes) {
  uint32_t smss = _snd.mss;
  if (_snd.cwnd < _snd.ssthresh) {
    // In slow start phase
    _snd.cwnd += std::min(acked_bytes, smss);
  } else {
    // In congestion avoidance phase
    uint32_t round_up = 1;
    _snd.cwnd += std::max(round_up, smss * smss / _snd.cwnd);
  }
}


template <typename InetTraits>
void tcp<InetTraits>::tcb::cleanup() {
  manager.notify(fd, EVENT_READABLE);
  _snd.closed = true;
  _snd.unsent.clear();
  _snd.data.clear();
  _rcv.out_of_order.map.clear();
  _rcv.data.clear();
  stop_retransmit_timer();
  clear_delayed_ack();
  center->dispatch_event_external(new tcp<InetTraits>::C_actual_remove_tcb(this));
  remove_from_tcbs();
}

template <typename InetTraits>
tcp_sequence tcp<InetTraits>::tcb::get_isn() {
  // Per RFC6528, TCP SHOULD generate its Initial Sequence Numbers
  // with the expression:
  //   ISN = M + F(localip, localport, remoteip, remoteport, secretkey)
  //   M is the 4 microsecond timer
  using namespace std::chrono;
  uint32_t hash[4];
  hash[0] = _local_ip.ip;
  hash[1] = _foreign_ip.ip;
  hash[2] = (_local_port << 16) + _foreign_port;
  hash[3] = _isn_secret.key[15];
  CryptoPP::Weak::MD5::Transform(hash, _isn_secret.key);
  auto seq = hash[0];
  auto m = duration_cast<microseconds>(clock_type::now(_tcp.cct).time_since_epoch());
  seq += m.count() / 4;
  return make_seq(seq);
}

template <typename InetTraits>
Tub<typename InetTraits::l4packet> tcp<InetTraits>::tcb::get_packet() {
  _poll_active = false;
  if (_packetq.empty()) {
    output_one();
  }

  Tub<typename InetTraits::l4packet> p;
  if (in_state(CLOSED)) {
    return p;
  }

  assert(!_packetq.empty());

  p = std::move(_packetq.front());
  _packetq.pop_front();
  if (!_packetq.empty() || (_snd.dupacks < 3 && can_send() > 0)) {
    // If there are packets to send in the queue or tcb is allowed to send
    // more add tcp back to polling set to keep sending. In addition, dupacks >= 3
    // is an indication that an segment is lost, stop sending more in this case.
    output();
  }
  return p;
}

template <typename InetTraits>
void tcp<InetTraits>::connection::close_read() {
  // do nothing
  // _tcb->manager.notify(_tcb->fd, EVENT_READABLE);
}

template <typename InetTraits>
void tcp<InetTraits>::connection::close_write() {
  _tcb->close();
}

template <typename InetTraits>
constexpr uint16_t tcp<InetTraits>::tcb::_max_nr_retransmit;

template <typename InetTraits>
constexpr std::chrono::microseconds tcp<InetTraits>::tcb::_rto_min;

template <typename InetTraits>
constexpr std::chrono::microseconds tcp<InetTraits>::tcb::_rto_max;

template <typename InetTraits>
constexpr std::chrono::microseconds tcp<InetTraits>::tcb::_rto_clk_granularity;

template <typename InetTraits>
typename tcp<InetTraits>::tcb::isn_secret tcp<InetTraits>::tcb::_isn_secret;


#endif /* TCP_HH_ */
