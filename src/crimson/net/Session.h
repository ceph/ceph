// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel Corp.
 *
 * Author: Yingxin Cheng <yingxincheng@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "Fwd.h"

#include "crimson/thread/Throttle.h"

#include "include/random.h"
#include "auth/AuthSessionHandler.h"
#include "msg/Policy.h"

namespace ceph::net {

struct Session
{
  entity_addr_t my_addr;
  peer_type_t peer_type;
  entity_addr_t peer_addr;
  std::chrono::milliseconds backoff;
  uint32_t connect_seq = 0;
  uint32_t peer_global_seq = 0;
  uint32_t global_seq;
  ceph_timespec last_keepalive_ack;

  ceph::net::Policy<ceph::thread::Throttle> policy;

  uint64_t features;
  /// the seq num of the last transmitted message
  seq_num_t out_seq = 0;
  /// the seq num of the last received message
  seq_num_t in_seq = 0;

  std::unique_ptr<AuthSessionHandler> session_security;
  // messages to be resent after connection gets reset
  std::queue<MessageRef> out_q;
  // messages sent, but not yet acked by peer
  std::queue<MessageRef> sent;

  Session(const entity_addr_t& _my_addr) : my_addr(_my_addr) {}

  void reset() {
    decltype(out_q){}.swap(out_q);
    decltype(sent){}.swap(sent);
    in_seq = 0;
    connect_seq = 0;
    if (HAVE_FEATURE(features, MSG_AUTH)) {
      // Set out_seq to a random value, so CRC won't be predictable.
      // Constant to limit starting sequence number to 2^31.  Nothing special
      // about it, just a big number.
      constexpr uint64_t SEQ_MASK = 0x7fffffff;
      out_seq = ceph::util::generate_random_number<uint64_t>(0, SEQ_MASK);
    } else {
      // previously, seq #'s always started at 0.
      out_seq = 0;
    }
  }

  void discard_sent_up_to(seq_num_t seq) {
    while (!sent.empty() &&
           sent.front()->get_seq() < seq) {
      sent.pop();
    }
  }

  void discard_requeued_up_to(seq_num_t seq) {
    while (!out_q.empty() &&
           out_q.front()->get_seq() < seq) {
      out_q.pop();
    }
  }
};

} // namespace ceph::net
