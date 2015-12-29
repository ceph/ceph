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
#include <functional>
#include <deque>
#include <chrono>
#include <random>
#include <stdexcept>
#include <system_error>

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>

#include "include/utime.h"
#include "IPChecksum.h"
#include "IP.h"
#include "const.h"
#include "shared_ptr.h"
#include "PacketUtil.h"

struct tcp_hdr;

inline auto tcp_error(int err) {
  return std::system_error(err, std::system_category());
}

inline auto tcp_reset_error() {
  return tcp_error(ECONNRESET);
};

inline auto tcp_connect_error() {
  return tcp_error(ECONNABORTED);
}

inline auto tcp_refused_error() {
  return tcp_error(ECONNREFUSED);
};

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

template <typename... Args>
void tcp_debug(const char* fmt, Args&&... args) {
#if TCP_DEBUG
  print(fmt, std::forward<Args>(args)...);
#endif
}

struct tcp_option {
  // The kind and len field are fixed and defined in TCP protocol
  enum class option_kind: uint8_t { mss = 2, win_scale = 3, sack = 4, timestamps = 8,  nop = 1, eol = 0 };
  enum class option_len:  uint8_t { mss = 4, win_scale = 3, sack = 2, timestamps = 10, nop = 1, eol = 1 };
  struct mss {
    option_kind kind = option_kind::mss;
    option_len len = option_len::mss;
    packed<uint16_t> mss;
    template <typename Adjuster>
    void adjust_endianness(Adjuster a) { a(mss); }
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
    packed<uint32_t> t1;
    packed<uint32_t> t2;
    template <typename Adjuster>
    void adjust_endianness(Adjuster a) { a(t1, t2); }
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

struct tcp_seq {
  uint32_t raw;
};

inline tcp_seq ntoh(tcp_seq s) {
  return tcp_seq { ntoh(s.raw) };
}

inline tcp_seq hton(tcp_seq s) {
  return tcp_seq { hton(s.raw) };
}

inline std::ostream& operator<<(std::ostream& os, tcp_seq s) {
  return os << s.raw;
}

inline tcp_seq make_seq(uint32_t raw) { return tcp_seq{raw}; }
inline tcp_seq& operator+=(tcp_seq& s, int32_t n) { s.raw += n; return s; }
inline tcp_seq& operator-=(tcp_seq& s, int32_t n) { s.raw -= n; return s; }
inline tcp_seq operator+(tcp_seq s, int32_t n) { return s += n; }
inline tcp_seq operator-(tcp_seq s, int32_t n) { return s -= n; }
inline int32_t operator-(tcp_seq s, tcp_seq q) { return s.raw - q.raw; }
inline bool operator==(tcp_seq s, tcp_seq q)  { return s.raw == q.raw; }
inline bool operator!=(tcp_seq s, tcp_seq q) { return !(s == q); }
inline bool operator<(tcp_seq s, tcp_seq q) { return s - q < 0; }
inline bool operator>(tcp_seq s, tcp_seq q) { return q < s; }
inline bool operator<=(tcp_seq s, tcp_seq q) { return !(s > q); }
inline bool operator>=(tcp_seq s, tcp_seq q) { return !(s < q); }

struct tcp_hdr {
  packed<uint16_t> src_port;
  packed<uint16_t> dst_port;
  packed<tcp_seq> seq;
  packed<tcp_seq> ack;
  uint8_t rsvd1 : 4;
  uint8_t data_offset : 4;
  uint8_t f_fin : 1;
  uint8_t f_syn : 1;
  uint8_t f_rst : 1;
  uint8_t f_psh : 1;
  uint8_t f_ack : 1;
  uint8_t f_urg : 1;
  uint8_t rsvd2 : 2;
  packed<uint16_t> window;
  packed<uint16_t> checksum;
  packed<uint16_t> urgent;

  template <typename Adjuster>
  void adjust_endianness(Adjuster a) { a(src_port, dst_port, seq, ack, window, checksum, urgent); }
} __attribute__((packed));

struct tcp_tag {};
using tcp_packet_merger = packet_merger<tcp_seq, tcp_tag>;

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

  class tcb : public enable_lw_shared_from_this<tcb> {
    using clock_type = lowres_clock;
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
    connection* _conn = nullptr;
    int _connect_done = 0;
    ipaddr _local_ip;
    ipaddr _foreign_ip;
    uint16_t _local_port;
    uint16_t _foreign_port;
    struct unacked_segment {
      Packet p;
      uint16_t data_len;
      unsigned nr_transmits;
      utime_t tx_time;
    };
    struct send {
      tcp_seq unacknowledged;
      tcp_seq next;
      uint32_t window;
      uint8_t window_scale;
      uint16_t mss;
      tcp_seq urgent;
      tcp_seq wl1;
      tcp_seq wl2;
      tcp_seq initial;
      std::deque<unacked_segment> data;
      std::deque<Packet> unsent;
      uint32_t unsent_len = 0;
      uint32_t queued_len = 0;
      bool closed = false;
      // Wait for all data are acked
      int _all_data_acked_fd = -1;
      // Round-trip time variation
      utime_t rttvar;
      // Smoothed round-trip time
      utime_t srtt;
      bool first_rto_sample = true;
      utime_t syn_tx_time;
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
      tcp_seq recover;
      bool window_probe = false;
    } _snd;
    struct receive {
      tcp_seq next;
      uint32_t window;
      uint8_t window_scale;
      uint16_t mss;
      tcp_seq urgent;
      tcp_seq initial;
      std::deque<Packet> data;
      tcp_packet_merger out_of_order;
    } _rcv;
    EventCenter *center;
    UserspaceEventManager &manager;
    int fd;
    int16_t errno = 1;
    tcp_option _option;
    EventCallbackRef delayed_ack_event;
    Tub<uint64_t> _delayed_ack_fd;
    // Retransmission timeout
    utime_t _rto{1, 0};
    utime_t _persist_time_out{1, 0};
    static constexpr utime_t _rto_min{1, 0};
    static constexpr utime_t _rto_max{60, 0};
    // Clock granularity
    static constexpr utime_t _rto_clk_granularity{0, 1000*1000};
    static constexpr uint16_t _max_nr_retransmit{5};
    EventCallbackRef retransmit_event;
    Tub<uint64_t> retransmit_fd;
    EventCallbackRef persist_event;
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
    tcp_seq get_isn();
    circular_buffer<typename InetTraits::l4packet> _packetq;
    bool _poll_active = false;
   public:
    // callback
    void close_final_cleanup();

   public:
    tcb(tcp& t, connid id);
    ~tcb();
    void input_handle_listen_state(tcp_hdr* th, Packet p);
    void input_handle_syn_sent_state(tcp_hdr* th, Packet p);
    void input_handle_other_state(tcp_hdr* th, Packet p);
    void output_one(bool data_retransmit = false);
    bool is_all_data_acked();
    int send(packet p);
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
        _inet.wait_l2_dst_address(_foreign_ip, [tcb] (const ethernet_address &dst, int r) {
          if (r == 0) {
            _poll_tcbs.emplace_back(tcb, dst);
          } else if (r == -ETIMEOUT) {
            // in other states connection should time out
            if (tcb->in_state(SYN_SENT)) {
              _connect_done = -ETIMEDOUT;
              tcb->cleanup();
            }
          } else if (r == -EBUSY) {
            // retry later
            _poll_active = false;
            tcb->start_retransmit_timer();
          }
        });
      }
    }

    const int16_t errno() const {
      return errno;
    }

    tcp_state& state() {
      return _state;
    }
  private:
    void respond_with_reset(tcp_hdr* th);
    bool merge_out_of_order();
    void insert_out_of_order(tcp_seq seq, Packet p);
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
      retransmit_fd.construct(center->create_time_event(_rto.to_nsec()*1000, retransmit_event));
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
      persist_fd.construct(center->create_time_event(_persist_time_out.to_nsec()*1000, persist_event));
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
    void update_rto(utime_t tx_time);
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
      return _tcp.hw_features().mtu - tcp_hdr_len_min - InetTraits::ip_hdr_len_min;
    }
    void queue_packet(packet p) {
      _packetq.emplace_back(
              typename InetTraits::l4packet{_foreign_ip, std::move(p)});
    }
    void signal_data_received() {
      manager.notify(fd);
    }
    void signal_all_data_acked() {
      if (_snd._all_data_acked_fd >= 0 && _snd.unsent_len == 0 && _snd.queued_len == 0)
        manager.notify(_snd._all_data_acked_fd);
    }
    void do_syn_sent() {
      _state = SYN_SENT;
      _snd.syn_tx_time = ceph_clock_now(_tcp.cct);
      // Send <SYN> to remote
      output();
    }
    void do_syn_received() {
      _state = SYN_RECEIVED;
      _snd.syn_tx_time.ceph_clock_now(_tcp.cct);
      // Send <SYN,ACK> to remote
      output();
    }
    void do_established() {
      _state = ESTABLISHED;
      update_rto(_snd.syn_tx_time);
      _connect_done = 1;
    }
    void do_reset() {
      _state = CLOSED;
      // Free packets to be sent which are waiting for _snd.user_queue_space
      _snd.user_queue_space.reset();
      cleanup();
      manager.notify(fd, EVENT_READABLE, -ECONNRESET);
      errno = -ECONNRESET;

      if (_snd._all_data_acked_fd >= 0)
        manager.notify(_snd._all_data_acked_fd, EVENT_READABLE, -ECONNRESET);
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
    uint32_t data_segment_acked(tcp_seq seg_ack);
    bool segment_acceptable(tcp_seq seg_seq, unsigned seg_len);
    void init_from_options(tcp_hdr* th, uint8_t* opt_start, uint8_t* opt_end);
    friend class connection;
  };
  CephContext *cct;
  inet_type& _inet;
  EventCenter *center;
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
  Throttle user_queue_space = {212992};
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
    const int16_t errno() const {
      return errno;
    }
    void close_read();
    void close_write();
  };
  class listener {
    tcp& _tcp;
    uint16_t _port;
    int16_t errno;
    queue<connection> _q;
  private:
    listener(tcp& t, uint16_t port, size_t queue_length)
            : _tcp(t), _port(port), _q(queue_length) {
      _tcp._listening.emplace(_port, this);
    }
  public:
    listener(listener&& x)
            : _tcp(x._tcp), _port(x._port), _q(std::move(x._q)) {
      _tcp._listening[_port] = this;
      x._port = 0;
    }
    ~listener() {
      if (_port) {
        _tcp._listening.erase(_port);
      }
    }
    Tub<connection> accept() {
      Tub<connection> c;
      if (!_q.empty())
        c.construct(_q.pop());
      return c;
    }

    void abort_accept() {
      _q.clear();
    }
    const int16_t errno() const {
      return errno;
    }
    friend class tcp;
  };
 public:
  explicit tcp(inet_type& inet);
  void received(packet p, ipaddr from, ipaddr to);
  bool forward(forward_hash& out_hash_data, Packet& p, size_t off);
  listener listen(uint16_t port, size_t queue_length = 100);
  connection connect(socket_address sa);
  const hw_features& hw_features() const { return _inet._inet.hw_features(); }
private:
  void send_packet_without_tcb(ipaddr from, ipaddr to, Packet p);
  void respond_with_reset(tcp_hdr* rth, ipaddr local_ip, ipaddr foreign_ip);
  friend class listener;
};

template <typename InetTraits>
tcp<InetTraits>::tcp(inet_type& inet, EventCenter *cen)
        : _inet(inet), center(cen), _e(_rd()),
          _queue_space(inet.cct, "DPDK::tcp::queue_space", 212992),
          user_queue_space(inet.cct, "DPDK::tcp::user_queue_space", 212992) {
  int tcb_polled = 0u;
  _inet.register_packet_provider([this, tcb_polled] () mutable {
    Tub<typename InetTraits::l4packet> l4p;
    auto c = _poll_tcbs.size();
    if (!_packetq.empty() && (!(tcb_polled % 128) || c == 0)) {
      l4p.construct(_packetq.front());
      _packetq.pop_front();
      _queue_space.put(*l4p.p.len());
    } else {
      while (c--) {
        tcb_polled++;
        lw_shared_ptr<tcb> tcb;
        ethernet_address dst;
        std::tie(tcb, dst) = std::move(_poll_tcbs.front());
        _poll_tcbs.pop_front();
        l4p = tcb->get_packet();
        if (l4p) {
          *l4p.e_dst = dst;
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
typename tcp<InetTraits>::connection tcp<InetTraits>::connect(socket_address sa) {
  uint16_t src_port;
  connid id;
  auto src_ip = _inet._inet.host_address();
  auto dst_ip = ipv4_address(sa);
  auto dst_port = ntoh(sa.u.in.sin_port);

  do {
    src_port = _port_dist(_e);
    id = connid{src_ip, dst_ip, src_port, dst_port};
  } while (_inet._inet.netif()->hash2cpu(id.hash(_inet._inet.netif()->rss_key())) != center->cpu_id()
           || _tcbs.find(id) != _tcbs.end());

  auto tcbp = make_lw_shared<tcb>(*this, id);
  _tcbs.insert({id, tcbp});
  tcbp->connect();
  return tcbp;
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

  if (!hw_features().rx_csum_offload) {
    checksummer csum;
    InetTraits::tcp_pseudo_header_checksum(csum, from, to, p.len());
    csum.sum(p);
    if (csum.get() != 0) {
      return;
    }
  }
  auto h = ntoh(*th);
  auto id = connid{to, from, h.dst_port, h.src_port};
  auto tcbi = _tcbs.find(id);
  lw_shared_ptr<tcb> tcbp;
  if (tcbi == _tcbs.end()) {
    auto listener = _listening.find(id.local_port);
    if (listener == _listening.end() || listener->second->_q.full()) {
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
        listener->second->_q.push(connection(tcbp));
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
    auto pkt = std::move(p);
    _inet.wait_l2_dst_address(to, [this, to, pkt] (const ethernet_address &e_dst, int r) mutable {
      if (r == 0)
        _packetq.emplace_back(ipv4_traits::l4packet{to, std::move(pkt), e_dst, ip_protocol_num::tcp});
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
class C_handle_delayed_ack : public EventCallback {
  tcp<InetTraits>::tcb *tcb;

 public:
  C_handle_delayed_ack(tcp<InetTraits>::tcb *t): tcb(t) { }
  void do_request(int r) {
    _nr_full_seg_received = 0; output();
  }
};

template <typename InetTraits>
class C_handle_retransmit : public EventCallback {
  tcp<InetTraits>::tcb *tcb;

 public:
  C_handle_retransmit(tcp<InetTraits>::tcb *t): tcb(t) { }
  void do_request(int r) {
    tcb->retransmit();
  }
};

template <typename InetTraits>
class C_handle_persist : public EventCallback {
  tcp<InetTraits>::tcb *tcb;

 public:
  C_handle_persist(tcp<InetTraits>::tcb *t): tcb(t) { }
  void do_request(int r) {
    tcb->persist();
  }
};

template <typename InetTraits>
tcp<InetTraits>::tcb::tcb(tcp& t, connid id)
        : _tcp(t), _local_ip(id.local_ip) , _foreign_ip(id.foreign_ip),
          _local_port(id.local_port), _foreign_port(id.foreign_port),
          center(t.center),
          manager(static_cast<DPDKDriver>(t.center->get_driver()).manager),
          fd(manager.get_eventfd()),
          delayed_ack_event(new C_handle_delayed_ack(this)),
          retransmit_event(new C_handle_retransmit(this)),
          persist_event(new C_handle_persist(this)) {}

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
  manager.close(fd);
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::respond_with_reset(tcp_hdr* rth)
{
  _tcp.respond_with_reset(rth, _local_ip, _foreign_ip);
}

template <typename InetTraits>
void tcp<InetTraits>::respond_with_reset(tcp_hdr* rth, ipaddr local_ip, ipaddr foreign_ip)
{
  if (rth->f_rst) {
    return;
  }
  Packet p;
  auto th = p.prepend_header<tcp_hdr>();
  th->src_port = rth->dst_port;
  th->dst_port = rth->src_port;
  if (rth->f_ack) {
    th->seq = rth->ack;
  }
  // If this RST packet is in response to a SYN packet. We ACK the ISN.
  if (rth->f_syn) {
    th->ack = rth->seq + 1;
    th->f_ack = true;
  }
  th->f_rst = true;
  th->data_offset = sizeof(*th) / 4;
  th->checksum = 0;
  *th = hton(*th);

  checksummer csum;
  offload_info oi;
  InetTraits::tcp_pseudo_header_checksum(csum, local_ip, foreign_ip, sizeof(*th));
  if (hw_features().tx_csum_l4_offload) {
    th->checksum = ~csum.get();
    oi.needs_csum = true;
  } else {
    csum.sum(p);
    th->checksum = csum.get();
    oi.needs_csum = false;
  }

  oi.protocol = ip_protocol_num::tcp;
  oi.tcp_hdr_len = sizeof(tcp_hdr);
  p.set_offload_info(oi);

  send_packet_without_tcb(local_ip, foreign_ip, std::move(p));
}

template <typename InetTraits>
uint32_t tcp<InetTraits>::tcb::data_segment_acked(tcp_seq seg_ack) {
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
    user_queue_space.put(_snd.data.front().data_len);
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
bool tcp<InetTraits>::tcb::segment_acceptable(tcp_seq seg_seq, unsigned seg_len) {
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
void tcp<InetTraits>::tcb::input_handle_listen_state(tcp_hdr* th, Packet p) {
  auto opt_len = th->data_offset * 4 - sizeof(tcp_hdr);
  auto opt_start = reinterpret_cast<uint8_t*>(p.get_header(0, th->data_offset * 4)) + sizeof(tcp_hdr);
  auto opt_end = opt_start + opt_len;
  p.trim_front(th->data_offset * 4);
  tcp_seq seg_seq = th->seq;

  // Set RCV.NXT to SEG.SEQ+1, IRS is set to SEG.SEQ
  _rcv.next = seg_seq + 1;
  _rcv.initial = seg_seq;

  // ISS should be selected and a SYN segment sent of the form:
  // <SEQ=ISS><ACK=RCV.NXT><CTL=SYN,ACK>
  // SND.NXT is set to ISS+1 and SND.UNA to ISS
  // NOTE: In previous code, _snd.next is set to ISS + 1 only when SYN is
  // ACKed. Now, we set _snd.next to ISS + 1 here, so in output_one(): we
  // have
  //     th->seq = syn_on ? _snd.initial : _snd.next
  // to make sure retransmitted SYN has correct SEQ number.
  do_setup_isn();

  _rcv.urgent = _rcv.next;

  ldout(cct, 10) << __func__ << " listen: LISTEN -> SYN_RECEIVED" << dendl;
  init_from_options(th, opt_start, opt_end);
  do_syn_received();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::input_handle_syn_sent_state(tcp_hdr* th, Packet p) {
  auto opt_len = th->data_offset * 4 - sizeof(tcp_hdr);
  auto opt_start = reinterpret_cast<uint8_t*>(p.get_header(0, th->data_offset * 4)) + sizeof(tcp_hdr);
  auto opt_end = opt_start + opt_len;
  p.trim_front(th->data_offset * 4);
  tcp_seq seg_seq = th->seq;
  auto seg_ack = th->ack;

  bool acceptable = false;
  // 3.1 first check the ACK bit
  if (th->f_ack) {
    // If SEG.ACK =< ISS, or SEG.ACK > SND.NXT, send a reset (unless the
    // RST bit is set, if so drop the segment and return)
    if (seg_ack <= _snd.initial || seg_ack > _snd.next) {
      return respond_with_reset(th);
    }

    // If SND.UNA =< SEG.ACK =< SND.NXT then the ACK is acceptable.
    acceptable = _snd.unacknowledged <= seg_ack && seg_ack <= _snd.next;
  }

  // 3.2 second check the RST bit
  if (th->f_rst) {
    // If the ACK was acceptable then signal the user "error: connection
    // reset", drop the segment, enter CLOSED state, delete TCB, and
    // return.  Otherwise (no ACK) drop the segment and return.
    if (acceptable) {
      return do_reset();
    } else {
      return;
    }
  }

  // 3.3 third check the security and precedence
  // NOTE: Ignored for now

  // 3.4 fourth check the SYN bit
  if (th->f_syn) {
    // RCV.NXT is set to SEG.SEQ+1, IRS is set to SEG.SEQ.  SND.UNA should
    // be advanced to equal SEG.ACK (if there is an ACK), and any segments
    // on the retransmission queue which are thereby acknowledged should be
    // removed.
    _rcv.next = seg_seq + 1;
    _rcv.initial = seg_seq;
    if (th->f_ack) {
      // TODO: clean retransmission queue
      _snd.unacknowledged = seg_ack;
    }
    if (_snd.unacknowledged > _snd.initial) {
      // If SND.UNA > ISS (our SYN has been ACKed), change the connection
      // state to ESTABLISHED, form an ACK segment
      // <SEQ=SND.NXT><ACK=RCV.NXT><CTL=ACK>
      ldout(cct, 20) << __func__ << "syn: SYN_SENT -> ESTABLISHED" << dendl;
      init_from_options(th, opt_start, opt_end);
      do_established();
      output();
    } else {
      // Otherwise enter SYN_RECEIVED, form a SYN,ACK segment
      // <SEQ=ISS><ACK=RCV.NXT><CTL=SYN,ACK>
      ldout(cct, 20) << __func__ << "syn: SYN_SENT -> SYN_RECEIVED" << dendl;
      do_syn_received();
    }
  }

  // 3.5 fifth, if neither of the SYN or RST bits is set then drop the
  // segment and return.
  return;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::input_handle_other_state(tcp_hdr* th, Packet p) {
  p.trim_front(th->data_offset * 4);
  bool do_output = false;
  bool do_output_data = false;
  tcp_seq seg_seq = th->seq;
  auto seg_ack = th->ack;
  auto seg_len = p.len();

  // 4.1 first check sequence number
  if (!segment_acceptable(seg_seq, seg_len)) {
    //<SEQ=SND.NXT><ACK=RCV.NXT><CTL=ACK>
    return output();
  }

  // In the following it is assumed that the segment is the idealized
  // segment that begins at RCV.NXT and does not exceed the window.
  if (seg_seq < _rcv.next) {
    // ignore already acknowledged data
    auto dup = std::min(uint32_t(_rcv.next - seg_seq), seg_len);
    p.trim_front(dup);
    seg_len -= dup;
    seg_seq += dup;
  }
  // FIXME: We should trim data outside the right edge of the receive window as well

  if (seg_seq != _rcv.next) {
    insert_out_of_order(seg_seq, std::move(p));
    // A TCP receiver SHOULD send an immediate duplicate ACK
    // when an out-of-order segment arrives.
    return output();
  }

  // 4.2 second check the RST bit
  if (th->f_rst) {
    if (in_state(SYN_RECEIVED)) {
      // If this connection was initiated with a passive OPEN (i.e.,
      // came from the LISTEN state), then return this connection to
      // LISTEN state and return.  The user need not be informed.  If
      // this connection was initiated with an active OPEN (i.e., came
      // from SYN_SENT state) then the connection was refused, signal
      // the user "connection refused".  In either case, all segments
      // on the retransmission queue should be removed.  And in the
      // active OPEN case, enter the CLOSED state and delete the TCB,
      // and return.
      _connect_done = -ECONNREFUSED;
      return do_reset();
    }
    if (in_state(ESTABLISHED | FIN_WAIT_1 | FIN_WAIT_2 | CLOSE_WAIT)) {
      // If the RST bit is set then, any outstanding RECEIVEs and SEND
      // should receive "reset" responses.  All segment queues should be
      // flushed.  Users should also receive an unsolicited general
      // "connection reset" signal.  Enter the CLOSED state, delete the
      // TCB, and return.
      return do_reset();
    }
    if (in_state(CLOSING | LAST_ACK | TIME_WAIT)) {
      // If the RST bit is set then, enter the CLOSED state, delete the
      // TCB, and return.
      return do_closed();
    }
  }

  // 4.3 third check security and precedence
  // NOTE: Ignored for now

  // 4.4 fourth, check the SYN bit
  if (th->f_syn) {
    // SYN_RECEIVED, ESTABLISHED, FIN_WAIT_1, FIN_WAIT_2
    // CLOSE_WAIT, CLOSING, LAST_ACK, TIME_WAIT

    // If the SYN is in the window it is an error, send a reset, any
    // outstanding RECEIVEs and SEND should receive "reset" responses,
    // all segment queues should be flushed, the user should also
    // receive an unsolicited general "connection reset" signal, enter
    // the CLOSED state, delete the TCB, and return.
    respond_with_reset(th);
    return do_reset();

    // If the SYN is not in the window this step would not be reached
    // and an ack would have been sent in the first step (sequence
    // number check).
  }

  // 4.5 fifth check the ACK field
  if (!th->f_ack) {
    // if the ACK bit is off drop the segment and return
    return;
  } else {
    // SYN_RECEIVED STATE
    if (in_state(SYN_RECEIVED)) {
      // If SND.UNA =< SEG.ACK =< SND.NXT then enter ESTABLISHED state
      // and continue processing.
      if (_snd.unacknowledged <= seg_ack && seg_ack <= _snd.next) {
        ldout(cct, 20) << __func__ << "SYN_RECEIVED -> ESTABLISHED" << dendl;
        do_established();
      } else {
        // <SEQ=SEG.ACK><CTL=RST>
        return respond_with_reset(th);
      }
    }
    auto update_window = [this, th, seg_seq, seg_ack] {
      ldout(cct, 20) << __func__ << "window update seg_seq=" << seg_seq
                     << " seg_ack=" << seg_ack << " old window=" << th->window
                     << " new window=" << _snd.window_scale << dendl;
      _snd.window = th->window << _snd.window_scale;
      _snd.wl1 = seg_seq;
      _snd.wl2 = seg_ack;
      if (_snd.window == 0) {
        _persist_time_out = _rto;
        start_persist_timer();
      } else {
        stop_persist_timer();
      }
    };
    // ESTABLISHED STATE or
    // CLOSE_WAIT STATE: Do the same processing as for the ESTABLISHED state.
    if (in_state(ESTABLISHED | CLOSE_WAIT)){
      // If SND.UNA < SEG.ACK =< SND.NXT then, set SND.UNA <- SEG.ACK.
      if (_snd.unacknowledged < seg_ack && seg_ack <= _snd.next) {
        // Remote ACKed data we sent
        auto acked_bytes = data_segment_acked(seg_ack);

        // If SND.UNA < SEG.ACK =< SND.NXT, the send window should be updated.
        if (_snd.wl1 < seg_seq || (_snd.wl1 == seg_seq && _snd.wl2 <= seg_ack)) {
          update_window();
        }

        // some data is acked, try send more data
        do_output_data = true;

        auto set_retransmit_timer = [this] {
          if (_snd.data.empty()) {
            // All outstanding segments are acked, turn off the timer.
            stop_retransmit_timer();
            // Signal the waiter of this event
            signal_all_data_acked();
          } else {
            // Restart the timer becasue new data is acked.
            start_retransmit_timer();
          }
        };

        if (_snd.dupacks >= 3) {
          // We are in fast retransmit / fast recovery phase
          uint32_t smss = _snd.mss;
          if (seg_ack > _snd.recover) {
            ldout(cct, 20) << __func__ << " ack: full_ack" << dendl;
            // Set cwnd to min (ssthresh, max(FlightSize, SMSS) + SMSS)
            _snd.cwnd = std::min(_snd.ssthresh, std::max(flight_size(), smss) + smss);
            // Exit the fast recovery procedure
            exit_fast_recovery();
            set_retransmit_timer();
          } else {
            ldout(cct, 20) << __func__ << " ack: partial_ack" << dendl;
            // Retransmit the first unacknowledged segment
            fast_retransmit();
            // Deflate the congestion window by the amount of new data
            // acknowledged by the Cumulative Acknowledgment field
            _snd.cwnd -= acked_bytes;
            // If the partial ACK acknowledges at least one SMSS of new
            // data, then add back SMSS bytes to the congestion window
            if (acked_bytes >= smss) {
              _snd.cwnd += smss;
            }
            // Send a new segment if permitted by the new value of
            // cwnd.  Do not exit the fast recovery procedure For
            // the first partial ACK that arrives during fast
            // recovery, also reset the retransmit timer.
            if (++_snd.partial_ack == 1) {
              start_retransmit_timer();
            }
          }
        } else {
          // RFC5681: The fast retransmit algorithm uses the arrival
          // of 3 duplicate ACKs (as defined in section 2, without
          // any intervening ACKs which move SND.UNA) as an
          // indication that a segment has been lost.
          //
          // So, here we reset dupacks to zero becasue this ACK moves
          // SND.UNA.
          exit_fast_recovery();
          set_retransmit_timer();
        }
      } else if (!_snd.data.empty() && seg_len == 0 &&
                 th->f_fin == 0 && th->f_syn == 0 &&
                 th->ack == _snd.unacknowledged &&
                 uint32_t(th->window << _snd.window_scale) == _snd.window) {
        // Note:
        // RFC793 states:
        // If the ACK is a duplicate (SEG.ACK < SND.UNA), it can be ignored
        // RFC5681 states:
        // The TCP sender SHOULD use the "fast retransmit" algorithm to detect
        // and repair loss, based on incoming duplicate ACKs.
        // Here, We follow RFC5681.
        _snd.dupacks++;
        uint32_t smss = _snd.mss;
        // 3 duplicated ACKs trigger a fast retransmit
        if (_snd.dupacks == 1 || _snd.dupacks == 2) {
          // RFC5681 Step 3.1
          // Send cwnd + 2 * smss per RFC3042
          do_output_data = true;
        } else if (_snd.dupacks == 3) {
          // RFC6582 Step 3.2
          if (seg_ack - 1 > _snd.recover) {
            _snd.recover = _snd.next - 1;
            // RFC5681 Step 3.2
            _snd.ssthresh = std::max((flight_size() - _snd.limited_transfer) / 2, 2 * smss);
            fast_retransmit();
          } else {
            // Do not enter fast retransmit and do not reset ssthresh
          }
          // RFC5681 Step 3.3
          _snd.cwnd = _snd.ssthresh + 3 * smss;
        } else if (_snd.dupacks > 3) {
          // RFC5681 Step 3.4
          _snd.cwnd += smss;
          // RFC5681 Step 3.5
          do_output_data = true;
        }
      } else if (seg_ack > _snd.next) {
        // If the ACK acks something not yet sent (SEG.ACK > SND.NXT)
        // then send an ACK, drop the segment, and return
        return output();
      } else if (_snd.window == 0 && th->window > 0) {
        update_window();
        do_output_data = true;
      }
    }
    // FIN_WAIT_1 STATE
    if (in_state(FIN_WAIT_1)) {
      // In addition to the processing for the ESTABLISHED state, if
      // our FIN is now acknowledged then enter FIN-WAIT-2 and continue
      // processing in that state.
      if (seg_ack == _snd.next + 1) {
        ldout(cct, 20) << __func__ << " ack: FIN_WAIT_1 -> FIN_WAIT_2" << dendl;
        _state = FIN_WAIT_2;
        do_local_fin_acked();
      }
    }
    // FIN_WAIT_2 STATE
    if (in_state(FIN_WAIT_2)) {
      // In addition to the processing for the ESTABLISHED state, if
      // the retransmission queue is empty, the user’s CLOSE can be
      // acknowledged ("ok") but do not delete the TCB.
      // TODO
    }
    // CLOSING STATE
    if (in_state(CLOSING)) {
      if (seg_ack == _snd.next + 1) {
        ldout(cct, 20) << __func__ << " ack: CLOSING -> TIME_WAIT" << dendl;
        do_local_fin_acked();
        return do_time_wait();
      } else {
        return;
      }
    }
    // LAST_ACK STATE
    if (in_state(LAST_ACK)) {
      if (seg_ack == _snd.next + 1) {
        ldout(cct, 20) << __func__ << " ack: LAST_ACK -> CLOSED" << dendl;
        do_local_fin_acked();
        return do_closed();
      }
    }
    // TIME_WAIT STATE
    if (in_state(TIME_WAIT)) {
      // The only thing that can arrive in this state is a
      // retransmission of the remote FIN. Acknowledge it, and restart
      // the 2 MSL timeout.
      // TODO
    }
  }

  // 4.6 sixth, check the URG bit
  if (th->f_urg) {
    // TODO
  }

  // 4.7 seventh, process the segment text
  if (in_state(ESTABLISHED | FIN_WAIT_1 | FIN_WAIT_1)) {
    if (p.len()) {
      // Once the TCP takes responsibility for the data it advances
      // RCV.NXT over the data accepted, and adjusts RCV.WND as
      // apporopriate to the current buffer availability.  The total of
      // RCV.NXT and RCV.WND should not be reduced.
      _rcv.data.push_back(std::move(p));
      _rcv.next += seg_len;
      auto merged = merge_out_of_order();
      signal_data_received();
      // Send an acknowledgment of the form:
      // <SEQ=SND.NXT><ACK=RCV.NXT><CTL=ACK>
      // This acknowledgment should be piggybacked on a segment being
      // transmitted if possible without incurring undue delay.
      if (merged) {
        // TCP receiver SHOULD send an immediate ACK when the
        // incoming segment fills in all or part of a gap in the
        // sequence space.
        do_output = true;
      } else {
        do_output = should_send_ack(seg_len);
      }
    }
  } else if (in_state(CLOSE_WAIT | CLOSING | LAST_ACK | TIME_WAIT)) {
    // This should not occur, since a FIN has been received from the
    // remote side. Ignore the segment text.
    return;
  }

  // 4.8 eighth, check the FIN bit
  if (th->f_fin) {
    if (in_state(CLOSED | LISTEN | SYN_SENT)) {
      // Do not process the FIN if the state is CLOSED, LISTEN or SYN-SENT
      // since the SEG.SEQ cannot be validated; drop the segment and return.
      return;
    }
    auto fin_seq = seg_seq + seg_len;
    if (fin_seq == _rcv.next) {
      _rcv.next = fin_seq + 1;
      signal_data_received();

      // If this <FIN> packet contains data as well, we can ACK both data
      // and <FIN> in a single packet, so canncel the previous ACK.
      clear_delayed_ack();
      do_output = false;
      // Send ACK for the FIN!
      output();

      if (in_state(SYN_RECEIVED | ESTABLISHED)) {
        ldout(cct, 20) << __func__ << " fin: SYN_RECEIVED or ESTABLISHED -> CLOSE_WAIT" << dendl;
        _state = CLOSE_WAIT;
      }
      if (in_state(FIN_WAIT_1)) {
        // If our FIN has been ACKed (perhaps in this segment), then
        // enter TIME-WAIT, start the time-wait timer, turn off the other
        // timers; otherwise enter the CLOSING state.
        // Note: If our FIN has been ACKed, we should be in FIN_WAIT_2
        // not FIN_WAIT_1 if we reach here.
        ldout(cct, 20) << __func__ << " fin: FIN_WAIT_1 -> CLOSING" << dendl;
        _state = CLOSING;
      }
      if (in_state(FIN_WAIT_2)) {
        ldout(cct, 20) << __func__ << " fin: FIN_WAIT_2 -> TIME_WAIT" << dendl;
        return do_time_wait();
      }
    }
  }
  if (do_output || (do_output_data && can_send())) {
    // Since we will do output, we can canncel scheduled delayed ACK.
    clear_delayed_ack();
    output();
  }
}

template <typename InetTraits>
packet tcp<InetTraits>::tcb::get_transmit_packet() {
  // easy case: empty queue
  if (_snd.unsent.empty()) {
    return Packet();
  }
  auto can_send = this->can_send();
  // Max number of TCP payloads we can pass to NIC
  uint32_t len;
  if (_tcp.hw_features().tx_tso) {
    // FIXME: Info tap device the size of the splitted packet
    len = _tcp.hw_features().max_packet_len - tcp_hdr_len_min - InetTraits::ip_hdr_len_min;
  } else {
    len = std::min(uint16_t(_tcp.hw_features().mtu - tcp_hdr_len_min - InetTraits::ip_hdr_len_min), _snd.mss);
  }
  can_send = std::min(can_send, len);
  // easy case: one small packet
  if (_snd.unsent.size() == 1 && _snd.unsent.front().len() <= can_send) {
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
  if (!_snd.unsent.empty() && can_send) {
    auto& q = _snd.unsent.front();
    p.append(q.share(0, can_send));
    q.trim_front(can_send);
  }
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

  tcp_seq seq;
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
  *th = hton(*th);

  offload_info oi;
  checksummer csum;
  uint16_t pseudo_hdr_seg_len = 0;

  oi.tcp_hdr_len = sizeof(tcp_hdr) + options_size;

  if (_tcp.hw_features().tx_csum_l4_offload) {
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
    if (_tcp.hw_features().tx_tso && len > _snd.mss) {
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

  if (_tcp.hw_features().tx_csum_l4_offload) {
    th->checksum = ~csum.get();
  } else {
    csum.sum(p);
    th->checksum = csum.get();
  }

  oi.protocol = ip_protocol_num::tcp;

  p.set_offload_info(oi);

  if (!data_retransmit && (len || syn_on || fin_on)) {
    utime_t now = ceph_clock_now(cct);
    if (len) {
      unsigned nr_transmits = 0;
      _snd.data.emplace_back(unacked_segment{std::move(clone),
                                             len, nr_transmits, now});
    }
    if (retransmit_fd) {
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
void tcp<InetTraits>::tcb::connect() {
  // An initial send sequence number (ISS) is selected.  A SYN segment of the
  // form <SEQ=ISS><CTL=SYN> is sent.  Set SND.UNA to ISS, SND.NXT to ISS+1,
  // enter SYN-SENT state, and return.
  do_setup_isn();

  // Local receive window scale factor
  _rcv.window_scale = _option._local_win_scale = 7;
  // Maximum segment size local can receive
  _rcv.mss = _option._local_mss = local_mss();
  // Linux's default window size
  _rcv.window = 29200 << _rcv.window_scale;

  do_syn_sent();
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
int tcp<InetTraits>::tcb::send(packet p) {
  // We can not send after the connection is closed
  assert(!_snd.closed);

  if (in_state(CLOSED))
    return -ECONNRESET;

  // TODO: Handle p.len() > max user_queue_space case
  auto len = p.len();
  _snd.queued_len += len;
  // TODO: Fix blocking there
  user_queue_space.get(len);
  _snd.unsent_len += p.len();
  _snd.queued_len -= p.len();
  _snd.unsent.push_back(std::move(p));
  if (can_send() > 0) {
    output();
  }
}

template <typename InetTraits>
class C_all_data_acked : public EventCallback {
  lw_shared_ptr<tcp<InetTraits>::tcb> tcb;

 public:
  C_all_data_acked(lw_shared_ptr<tcb> &cb): tcb(cb) {}
  void do_request(int fd_or_id) {
    tcb->close_final_cleanup();
  }
};

template <typename InetTraits>
void tcp<InetTraits>::tcb::close_final_cleanup() {
  if (_snd._all_data_acked_fd >= 0) {
    manager.close(_snd._all_data_acked_fd);
    _snd._all_data_acked_fd = -1;
  }

  _snd.closed = true;
  ldout(_tcp.cct, 20) << __func__ << "close: unsent_len=" << _snd.unsent_len << dendl;
  if (in_state(CLOSE_WAIT)) {
    ldout(_tcp.cct, 20) << __func__ << "close: CLOSE_WAIT -> LAST_ACK" << dendl;
    _state = LAST_ACK;
  } else if (in_state(ESTABLISHED)) {
    ldout(_tcp.cct, 20) << __func__ << "close: ESTABLISHED -> FIN_WAIT_1" << dendl;
    _state = FIN_WAIT_1;
  }
  // Send <FIN> to remote
  // Note: we call output_one to make sure a packet with FIN actually
  // sent out. If we only call output() and _packetq is not empty,
  // tcp::tcb::get_packet(), packet with FIN will not be generated.
  output_one();
  output();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::close() {
  if (in_state(CLOSED) || _snd.closed) {
    return ;
  }
  // TODO: We should make this asynchronous

  bool acked = is_all_data_acked();
  if (!acked) {
    _snd._all_data_acked_fd = manager.get_eventfd();
    center->create_file_event(_snd._all_data_acked_fd, EVENT_READABLE,
                              new C_all_data_acked(this->shared_from_this()));
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
    if (seg_beg <= _rcv.next && _rcv.next < seg_end) {
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
void tcp<InetTraits>::tcb::insert_out_of_order(tcp_seq seg, Packet p) {
  _rcv.out_of_order.merge(seg, std::move(p));
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::trim_receive_data_after_window() {
  abort();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::persist() {
  tcp_debug("persist timer fired\n");
  // Send 1 byte packet to probe peer's window size
  _snd.window_probe = true;
  output_one();
  _snd.window_probe = false;

  output();
  // Perform binary exponential back-off per RFC1122
  _persist_time_out = std::min(_persist_time_out * 2, _rto_max);
  start_persist_timer();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::retransmit() {
  auto output_update_rto = [this] {
    output();
    // According to RFC6298, Update RTO <- RTO * 2 to perform binary exponential back-off
    this->_rto = std::min(this->_rto * 2, this->_rto_max);
    start_retransmit_timer();
  };

  // Retransmit SYN
  if (syn_needs_on()) {
    if (_snd.syn_retransmit++ < _max_nr_retransmit) {
      output_update_rto();
    } else {
      _connect_done = -ECONNABORTED;
      cleanup();
      return;
    }
  }

  // Retransmit FIN
  if (fin_needs_on()) {
    if (_snd.fin_retransmit++ < _max_nr_retransmit) {
      output_update_rto();
    } else {
      cleanup();
      return;
    }
  }

  // Retransmit Data
  if (_snd.data.empty()) {
    return;
  }

  // If there are unacked data, retransmit the earliest segment
  auto& unacked_seg = _snd.data.front();

  // According to RFC5681
  // Update ssthresh only for the first retransmit
  uint32_t smss = _snd.mss;
  if (unacked_seg.nr_transmits == 0) {
    _snd.ssthresh = std::max(flight_size() / 2, 2 * smss);
  }
  // RFC6582 Step 4
  _snd.recover = _snd.next - 1;
  // Start the slow start process
  _snd.cwnd = smss;
  // End fast recovery
  exit_fast_recovery();

  if (unacked_seg.nr_transmits < _max_nr_retransmit) {
    unacked_seg.nr_transmits++;
  } else {
    // Delete connection when max num of retransmission is reached
    cleanup();
    return;
  }
  retransmit_one();

  output_update_rto();
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
void tcp<InetTraits>::tcb::update_rto(utime_t tx_time) {
  // Update RTO according to RFC6298
  auto R = std::chrono::nanoseconds((ceph_clock_now(_tcp.cct) - tx_time).to_nsec());
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
    utime_t delta = _snd.srtt > R ? (_snd.srtt - R) : (R - _snd.srtt);
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
  _snd.unsent.clear();
  _snd.data.clear();
  _rcv.out_of_order.map.clear();
  _rcv.data.clear();
  stop_retransmit_timer();
  clear_delayed_ack();
  remove_from_tcbs();
}

template <typename InetTraits>
tcp_seq tcp<InetTraits>::tcb::get_isn() {
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
  auto m = ceph_clock_now(cct).to_nsec()/1000;
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

  p.construct(_packetq.front());
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
  manager.notify(fd);
}

template <typename InetTraits>
void tcp<InetTraits>::connection::close_write() {
  _tcb->close();
}

template <typename InetTraits>
constexpr uint16_t tcp<InetTraits>::tcb::_max_nr_retransmit;

template <typename InetTraits>
constexpr std::chrono::milliseconds tcp<InetTraits>::tcb::_rto_min;

template <typename InetTraits>
constexpr std::chrono::milliseconds tcp<InetTraits>::tcb::_rto_max;

template <typename InetTraits>
constexpr std::chrono::milliseconds tcp<InetTraits>::tcb::_rto_clk_granularity;

template <typename InetTraits>
typename tcp<InetTraits>::tcb::isn_secret tcp<InetTraits>::tcb::_isn_secret;

class listen_options;
class server_socket;
class connected_socket;

server_socket tcpv4_listen(tcp<ipv4_traits>& tcpv4, uint16_t port, listen_options opts);

connected_socket tcpv4_connect(tcp<ipv4_traits>& tcpv4, socket_address sa);

#endif /* TCP_HH_ */
