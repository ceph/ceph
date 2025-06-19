// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 Clyso GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/tcp_info.h"

#include "common/Formatter.h"

namespace ceph {

#ifdef _WIN32
struct tcp_info {};

bool tcp_info(int fd, struct tcp_info& info) {
  return false;
}
bool dump_tcp_info(int fd, Formatter* f) {
  return false;
}

#else

bool tcp_info(int fd, struct tcp_info& info) {
  socklen_t info_len = sizeof(info);
  return (getsockopt(fd, SOL_TCP, TCP_INFO, &info, &info_len) == 0);
}

static const char* get_tcpi_state_name(uint8_t state) {
  switch (state) {
    case TCP_ESTABLISHED:
      return "established";
    case TCP_SYN_SENT:
      return "syn sent";
    case TCP_SYN_RECV:
      return "syn recv";
    case TCP_FIN_WAIT1:
      return "fin wait1";
    case TCP_FIN_WAIT2:
      return "fin wait2";
    case TCP_TIME_WAIT:
      return "time wait";
    case TCP_CLOSE:
      return "close";
    case TCP_CLOSE_WAIT:
      return "close wait";
    case TCP_LAST_ACK:
      return "last ack";
    case TCP_LISTEN:
      return "listen";
    case TCP_CLOSING:
      return "closing";
    default:
      return "UNKNOWN";
  }
}

bool dump_tcp_info(int fd, Formatter* f) {
  struct tcp_info info;
  if (!tcp_info(fd, info)) {
    return false;
  }

  f->open_object_section("tcp_info");
  f->dump_string("tcpi_state", get_tcpi_state_name(info.tcpi_state));
  f->dump_unsigned("tcpi_retransmits", info.tcpi_retransmits);
  f->dump_unsigned("tcpi_probes", info.tcpi_probes);
  f->dump_unsigned("tcpi_backoff", info.tcpi_backoff);
  f->dump_unsigned("tcpi_rto_us", info.tcpi_rto);
  f->dump_unsigned("tcpi_ato_us", info.tcpi_ato);
  f->dump_unsigned("tcpi_snd_mss", info.tcpi_snd_mss);
  f->dump_unsigned("tcpi_rcv_mss", info.tcpi_rcv_mss);
  f->dump_unsigned("tcpi_unacked", info.tcpi_unacked);
  f->dump_unsigned("tcpi_lost", info.tcpi_lost);
  f->dump_unsigned("tcpi_retrans", info.tcpi_retrans);
  f->dump_unsigned("tcpi_pmtu", info.tcpi_pmtu);
  f->dump_unsigned("tcpi_rtt_us", info.tcpi_rtt);
  f->dump_unsigned("tcpi_rttvar_us", info.tcpi_rttvar);
  f->dump_unsigned("tcpi_total_retrans", info.tcpi_total_retrans);
  f->dump_unsigned("tcpi_last_data_sent_ms", info.tcpi_last_data_sent);
  f->dump_unsigned("tcpi_last_ack_sent_ms", info.tcpi_last_ack_sent);
  f->dump_unsigned("tcpi_last_data_recv_ms", info.tcpi_last_data_recv);
  f->dump_unsigned("tcpi_last_ack_recv_ms", info.tcpi_last_ack_recv);

  f->open_array_section("tcpi_options");
  if (info.tcpi_options & TCPI_OPT_TIMESTAMPS) {
    f->dump_string("option", "timestamps");
  }
  if (info.tcpi_options & TCPI_OPT_SACK) {
    f->dump_string("option", "sack");
  }
  if (info.tcpi_options & TCPI_OPT_WSCALE) {
    f->dump_string("option", "wscale");
  }
  if (info.tcpi_options & TCPI_OPT_ECN) {
    f->dump_string("option", "ecn");
  }
  if (info.tcpi_options & TCPI_OPT_ECN_SEEN) {
    f->dump_string("option", "ecn seen");
  }
  if (info.tcpi_options & TCPI_OPT_SYN_DATA) {
    f->dump_string("option", "syn data");
  }
  f->close_section();

  f->close_section();
  return true;
}

#endif

}  // namespace ceph
