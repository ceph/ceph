// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_ASYNC_MSGR_PERF_COUNTERS_H
#define CEPH_MSG_ASYNC_MSGR_PERF_COUNTERS_H

PERF_COUNTERS_ADD_U64_COUNTER(l_msgr_recv_messages,
  "msgr_recv_messages", "Network received messages");
PERF_COUNTERS_ADD_U64_COUNTER(l_msgr_send_messages,
  "msgr_send_messages", "Network sent messages");
PERF_COUNTERS_ADD_U64_COUNTER(l_msgr_recv_bytes,
  "msgr_recv_bytes", "Network received bytes", nullptr, 0, UNIT_BYTES);
PERF_COUNTERS_ADD_U64_COUNTER(l_msgr_send_bytes,
  "msgr_send_bytes", "Network sent bytes", nullptr, 0, UNIT_BYTES);
PERF_COUNTERS_ADD_U64_COUNTER(l_msgr_active_connections,
  "msgr_active_connections", "Active connection number");
PERF_COUNTERS_ADD_U64_COUNTER(l_msgr_created_connections,
  "msgr_created_connections", "Created connection number");

PERF_COUNTERS_ADD_TIME_AVG(l_msgr_running_total_time,
  "msgr_running_total_time", "The total time of thread running");
PERF_COUNTERS_ADD_TIME_AVG(l_msgr_running_send_time,
  "msgr_running_send_time", "The total time of message sending");
PERF_COUNTERS_ADD_TIME_AVG(l_msgr_running_recv_time,
  "msgr_running_recv_time", "The total time of message receiving");
PERF_COUNTERS_ADD_TIME_AVG(l_msgr_running_fast_dispatch_time,
  "msgr_running_fast_dispatch_time", "The total time of fast dispatch");


using msgr_perf_counters_t = ceph::perf_counters_t<
  l_msgr_recv_messages,
  l_msgr_send_messages,
  l_msgr_recv_bytes,
  l_msgr_send_bytes,
  l_msgr_created_connections,
  l_msgr_active_connections,

  l_msgr_running_total_time,
  l_msgr_running_send_time,
  l_msgr_running_recv_time,
  l_msgr_running_fast_dispatch_time
  >;

#endif // CEPH_MSG_ASYNC_MSGR_PERF_COUNTERS_H
