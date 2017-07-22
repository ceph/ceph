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

#include "align.h"
#include "TCP.h"
#include "IP.h"
#include "DPDKStack.h"

#include "common/dout.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_dpdk
#undef dout_prefix
#define dout_prefix *_dout << "tcp "

void tcp_option::parse(uint8_t* beg, uint8_t* end)
{
  while (beg < end) {
    auto kind = option_kind(*beg);
    if (kind != option_kind::nop && kind != option_kind::eol) {
      // Make sure there is enough room for this option
      auto len = *(beg + 1);
      if (beg + len > end) {
        return;
      }
    }
    switch (kind) {
      case option_kind::mss:
        _mss_received = true;
        _remote_mss = ntoh(reinterpret_cast<mss*>(beg)->mss);
        beg += option_len::mss;
        break;
      case option_kind::win_scale:
        _win_scale_received = true;
        _remote_win_scale = reinterpret_cast<win_scale*>(beg)->shift;
        // We can turn on win_scale option, 7 is Linux's default win scale size
        _local_win_scale = 7;
        beg += option_len::win_scale;
        break;
      case option_kind::sack:
        _sack_received = true;
        beg += option_len::sack;
        break;
      case option_kind::nop:
        beg += option_len::nop;
        break;
      case option_kind::eol:
        return;
      default:
        // Ignore options we do not understand
        auto len = *(beg + 1);
        beg += len;
        // Prevent infinite loop
        if (len == 0) {
            return;
        }
        break;
    }
  }
}

uint8_t tcp_option::fill(tcp_hdr* th, uint8_t options_size)
{
  auto hdr = reinterpret_cast<uint8_t*>(th);
  auto off = hdr + sizeof(tcp_hdr);
  uint8_t size = 0;
  bool syn_on = th->f_syn;
  bool ack_on = th->f_ack;

  if (syn_on) {
    if (_mss_received || !ack_on) {
      auto mss = new (off) tcp_option::mss;
      mss->mss = _local_mss;
      off += mss->len;
      size += mss->len;
      *mss = mss->hton();
    }
    if (_win_scale_received || !ack_on) {
      auto win_scale = new (off) tcp_option::win_scale;
      win_scale->shift = _local_win_scale;
      off += win_scale->len;
      size += win_scale->len;
    }
  }
  if (size > 0) {
    // Insert NOP option
    auto size_max = align_up(uint8_t(size + 1), tcp_option::align);
    while (size < size_max - uint8_t(option_len::eol)) {
      new (off) tcp_option::nop;
      off += option_len::nop;
      size += option_len::nop;
    }
    new (off) tcp_option::eol;
    size += option_len::eol;
  }
  assert(size == options_size);

  return size;
}

uint8_t tcp_option::get_size(bool syn_on, bool ack_on)
{
  uint8_t size = 0;
  if (syn_on) {
    if (_mss_received || !ack_on) {
      size += option_len::mss;
    }
    if (_win_scale_received || !ack_on) {
      size += option_len::win_scale;
    }
  }
  if (size > 0) {
    size += option_len::eol;
    // Insert NOP option to align on 32-bit
    size = align_up(size, tcp_option::align);
  }
  return size;
}

ipv4_tcp::ipv4_tcp(ipv4& inet, EventCenter *c)
    : _inet_l4(inet), _tcp(std::unique_ptr<tcp<ipv4_traits>>(new tcp<ipv4_traits>(inet.cct, _inet_l4, c)))
{ }

ipv4_tcp::~ipv4_tcp() { }

void ipv4_tcp::received(Packet p, ipv4_address from, ipv4_address to)
{
  _tcp->received(std::move(p), from, to);
}

bool ipv4_tcp::forward(forward_hash& out_hash_data, Packet& p, size_t off)
{
  return _tcp->forward(out_hash_data, p, off);
}

int tcpv4_listen(tcp<ipv4_traits>& tcpv4, uint16_t port, const SocketOptions &opts,
                 ServerSocket *sock)
{
  auto p = new DPDKServerSocketImpl<tcp<ipv4_traits>>(tcpv4, port, opts);
  int r = p->listen();
  if (r < 0) {
    delete p;
    return r;
  }
  *sock = ServerSocket(std::unique_ptr<ServerSocketImpl>(p));
  return 0;
}

int tcpv4_connect(tcp<ipv4_traits>& tcpv4, const entity_addr_t &addr,
                  ConnectedSocket *sock)
{
  auto conn = tcpv4.connect(addr);
  *sock = ConnectedSocket(std::unique_ptr<ConnectedSocketImpl>(
          new NativeConnectedSocketImpl<tcp<ipv4_traits>>(std::move(conn))));
  return 0;
}

template <typename InetTraits>
void tcp<InetTraits>::respond_with_reset(tcp_hdr* rth, ipaddr local_ip, ipaddr foreign_ip)
{
  ldout(cct, 20) << __func__ << " tcp header rst=" << bool(rth->f_rst) << " fin=" << bool(rth->f_fin)
                 << " syn=" << bool(rth->f_syn) << dendl;
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
  *th = th->hton();

  checksummer csum;
  offload_info oi;
  InetTraits::tcp_pseudo_header_checksum(csum, local_ip, foreign_ip, sizeof(*th));
  if (get_hw_features().tx_csum_l4_offload) {
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

#undef dout_prefix
#define dout_prefix _prefix(_dout)
template<typename InetTraits>
ostream& tcp<InetTraits>::tcb::_prefix(std::ostream *_dout) {
  return *_dout << "tcp " << _local_ip << ":" << _local_port << " -> " << _foreign_ip << ":" << _foreign_port
                << " tcb(" << this << " fd=" << fd << " s=" << _state << ").";
}

template<typename InetTraits>
void tcp<InetTraits>::tcb::input_handle_listen_state(tcp_hdr* th, Packet p)
{
  auto opt_len = th->data_offset * 4 - sizeof(tcp_hdr);
  auto opt_start = reinterpret_cast<uint8_t*>(p.get_header(0, th->data_offset * 4)) + sizeof(tcp_hdr);
  auto opt_end = opt_start + opt_len;
  p.trim_front(th->data_offset * 4);
  tcp_sequence seg_seq = th->seq;

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

  ldout(_tcp.cct, 10) << __func__ << " listen: LISTEN -> SYN_RECEIVED" << dendl;
  init_from_options(th, opt_start, opt_end);
  do_syn_received();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::input_handle_syn_sent_state(tcp_hdr* th, Packet p)
{
  auto opt_len = th->data_offset * 4 - sizeof(tcp_hdr);
  auto opt_start = reinterpret_cast<uint8_t*>(p.get_header(0, th->data_offset * 4)) + sizeof(tcp_hdr);
  auto opt_end = opt_start + opt_len;
  p.trim_front(th->data_offset * 4);
  tcp_sequence seg_seq = th->seq;
  auto seg_ack = th->ack;

  ldout(_tcp.cct, 20) << __func__ << " tcp header seq " << seg_seq.raw << " ack " << seg_ack.raw
                      << " fin=" << bool(th->f_fin) << " syn=" << bool(th->f_syn) << dendl;

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
      ldout(_tcp.cct, 20) << __func__ << " syn: SYN_SENT -> ESTABLISHED" << dendl;
      init_from_options(th, opt_start, opt_end);
      do_established();
      output();
    } else {
      // Otherwise enter SYN_RECEIVED, form a SYN,ACK segment
      // <SEQ=ISS><ACK=RCV.NXT><CTL=SYN,ACK>
      ldout(_tcp.cct, 20) << __func__ << " syn: SYN_SENT -> SYN_RECEIVED" << dendl;
      do_syn_received();
    }
  }

  // 3.5 fifth, if neither of the SYN or RST bits is set then drop the
  // segment and return.
  return;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::input_handle_other_state(tcp_hdr* th, Packet p)
{
  p.trim_front(th->data_offset * 4);
  bool do_output = false;
  bool do_output_data = false;
  tcp_sequence seg_seq = th->seq;
  auto seg_ack = th->ack;
  auto seg_len = p.len();
  ldout(_tcp.cct, 20) << __func__ << " tcp header seq " << seg_seq.raw << " ack " << seg_ack.raw
                      << " snd next " << _snd.next.raw << " unack " << _snd.unacknowledged.raw
                      << " rcv next " << _rcv.next.raw << " len " << seg_len
                      << " fin=" << bool(th->f_fin) << " syn=" << bool(th->f_syn) << dendl;

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
    ldout(_tcp.cct, 10) << __func__ << " dup segment len " << dup << dendl;
    p.trim_front(dup);
    seg_len -= dup;
    seg_seq += dup;
  }
  // FIXME: We should trim data outside the right edge of the receive window as well

  if (seg_seq != _rcv.next) {
    ldout(_tcp.cct, 10) << __func__ << " out of order, expect " << _rcv.next.raw
                        << " actual " << seg_seq.raw
                        << " out of order size " << _rcv.out_of_order.map.size()
                        << dendl;
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
      errno = -ECONNREFUSED;
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
        ldout(_tcp.cct, 20) << __func__ << " SYN_RECEIVED -> ESTABLISHED" << dendl;
        do_established();
        if (_tcp.push_listen_queue(_local_port, this)) {
          ldout(_tcp.cct, 20) << __func__ << " successfully accepting socket" << dendl;
        } else {
          ldout(_tcp.cct, 5) << __func__ << " not exist listener or full queue, reset" << dendl;
          return respond_with_reset(th);
        }
      } else {
        // <SEQ=SEG.ACK><CTL=RST>
        return respond_with_reset(th);
      }
    }
    auto update_window = [this, th, seg_seq, seg_ack] {
      ldout(_tcp.cct, 20) << __func__ << " window update seg_seq=" << seg_seq
                          << " seg_ack=" << seg_ack << " old window=" << th->window
                          << " new window=" << int(_snd.window_scale) << dendl;
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
    if (in_state(ESTABLISHED | CLOSE_WAIT)) {
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
            ldout(_tcp.cct, 20) << __func__ << " ack: full_ack" << dendl;
            // Set cwnd to min (ssthresh, max(FlightSize, SMSS) + SMSS)
            _snd.cwnd = std::min(_snd.ssthresh, std::max(flight_size(), smss) + smss);
            // Exit the fast recovery procedure
            exit_fast_recovery();
            set_retransmit_timer();
          } else {
            ldout(_tcp.cct, 20) << __func__ << " ack: partial_ack" << dendl;
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
        ldout(_tcp.cct, 20) << __func__ << " ack: FIN_WAIT_1 -> FIN_WAIT_2" << dendl;
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
        ldout(_tcp.cct, 20) << __func__ << " ack: CLOSING -> TIME_WAIT" << dendl;
        do_local_fin_acked();
        return do_time_wait();
      } else {
        return;
      }
    }
    // LAST_ACK STATE
    if (in_state(LAST_ACK)) {
      if (seg_ack == _snd.next + 1) {
        ldout(_tcp.cct, 20) << __func__ << " ack: LAST_ACK -> CLOSED" << dendl;
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
  if (in_state(ESTABLISHED | FIN_WAIT_1 | FIN_WAIT_2)) {
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
      ldout(_tcp.cct, 20) << __func__ << " merged=" << merged << " do_output=" << do_output << dendl;
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

      // If this <FIN> packet contains data as well, we can ACK both data
      // and <FIN> in a single packet, so canncel the previous ACK.
      clear_delayed_ack();
      do_output = false;
      // Send ACK for the FIN!
      output();
      signal_data_received();
      _errno = 0;

      if (in_state(SYN_RECEIVED | ESTABLISHED)) {
        ldout(_tcp.cct, 20) << __func__ << " fin: SYN_RECEIVED or ESTABLISHED -> CLOSE_WAIT" << dendl;
        _state = CLOSE_WAIT;
        // EOF
      }
      if (in_state(FIN_WAIT_1)) {
        // If our FIN has been ACKed (perhaps in this segment), then
        // enter TIME-WAIT, start the time-wait timer, turn off the other
        // timers; otherwise enter the CLOSING state.
        // Note: If our FIN has been ACKed, we should be in FIN_WAIT_2
        // not FIN_WAIT_1 if we reach here.
        ldout(_tcp.cct, 20) << __func__ << " fin: FIN_WAIT_1 -> CLOSING" << dendl;
        _state = CLOSING;
      }
      if (in_state(FIN_WAIT_2)) {
        ldout(_tcp.cct, 20) << __func__ << " fin: FIN_WAIT_2 -> TIME_WAIT" << dendl;
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
void tcp<InetTraits>::tcb::connect()
{
  ldout(_tcp.cct, 20) << __func__ << dendl;
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
void tcp<InetTraits>::tcb::close_final_cleanup()
{
  if (_snd._all_data_acked_fd >= 0) {
    center->delete_file_event(_snd._all_data_acked_fd, EVENT_READABLE);
    _tcp.manager.close(_snd._all_data_acked_fd);
    _snd._all_data_acked_fd = -1;
  }

  _snd.closed = true;
  signal_data_received();
  ldout(_tcp.cct, 20) << __func__ << " unsent_len=" << _snd.unsent_len << dendl;
  if (in_state(CLOSE_WAIT)) {
    ldout(_tcp.cct, 20) << __func__ << " CLOSE_WAIT -> LAST_ACK" << dendl;
    _state = LAST_ACK;
  } else if (in_state(ESTABLISHED)) {
    ldout(_tcp.cct, 20) << __func__ << " ESTABLISHED -> FIN_WAIT_1" << dendl;
    _state = FIN_WAIT_1;
  }
  // Send <FIN> to remote
  // Note: we call output_one to make sure a packet with FIN actually
  // sent out. If we only call output() and _packetq is not empty,
  // tcp::tcb::get_packet(), packet with FIN will not be generated.
  output_one();
  output();
  center->delete_file_event(fd, EVENT_READABLE|EVENT_WRITABLE);
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::retransmit()
{
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
      _errno = -ECONNABORTED;
      ldout(_tcp.cct, 5) << __func__ << " syn retransmit exceed max "
                         << _max_nr_retransmit << dendl;
      _errno = -ETIMEDOUT;
      cleanup();
      return;
    }
  }

  // Retransmit FIN
  if (fin_needs_on()) {
    if (_snd.fin_retransmit++ < _max_nr_retransmit) {
      output_update_rto();
    } else {
      ldout(_tcp.cct, 5) << __func__ << " fin retransmit exceed max "
                         << _max_nr_retransmit << dendl;
      _errno = -ETIMEDOUT;
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

  ldout(_tcp.cct, 20) << __func__ << " unack data size " << _snd.data.size()
                      << " nr=" << unacked_seg.nr_transmits << dendl;
  if (unacked_seg.nr_transmits < _max_nr_retransmit) {
    unacked_seg.nr_transmits++;
  } else {
    // Delete connection when max num of retransmission is reached
    ldout(_tcp.cct, 5) << __func__ << " seg retransmit exceed max "
                       << _max_nr_retransmit << dendl;
    _errno = -ETIMEDOUT;
    cleanup();
    return;
  }
  retransmit_one();

  output_update_rto();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::persist() {
  ldout(_tcp.cct, 20) << __func__ << " persist timer fired" << dendl;
  // Send 1 byte packet to probe peer's window size
  _snd.window_probe = true;
  output_one();
  _snd.window_probe = false;

  output();
  // Perform binary exponential back-off per RFC1122
  _persist_time_out = std::min(_persist_time_out * 2, _rto_max);
  start_persist_timer();
}
