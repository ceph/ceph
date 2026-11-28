// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Open Edge LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MDS_CHANGE_NOTIFY_QUEUE_H
#define CEPH_MDS_CHANGE_NOTIFY_QUEUE_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <utility>

namespace cephfs_notify {

/// A record waiting for the drain thread, with the stream position it was
/// given at submission time.
struct QueuedEvent {
  std::string record;
  uint64_t epoch = 0;
  uint64_t seq = 0;
};

/**
 * Bounded FIFO of change-notification records that numbers every event.
 *
 * Each pushed event is given the next sequence number of the current epoch
 * *before* the capacity check, so an event refused here (queue full), or
 * later by the endpoint or the transport, still consumes its number and
 * leaves a hole that a consumer can see. Sequence numbers start at 1 in
 * every epoch.
 *
 * Not thread-safe: the owner serializes access. Header-only and free of
 * MDS and librdkafka dependencies so that it can be unit-tested alone.
 */
class PositionedQueue {
public:
  explicit PositionedQueue(size_t capacity)
    : cap(capacity ? capacity : 1) {}

  /// Change the bound (0 is treated as 1). Events already queued stay.
  void set_capacity(size_t capacity) { cap = capacity ? capacity : 1; }

  /// Enter epoch `e`: the next event gets seq 1. Repeating the current
  /// epoch is a no-op, so this can be called on every map update.
  void set_epoch(uint64_t e) {
    if (e != cur_epoch) {
      cur_epoch = e;
      next = 1;
    }
  }

  /// Number the event, then queue it if there is room. Returns false when
  /// the queue is full: the event is dropped, its number stays consumed.
  bool push(std::string record) {
    const uint64_t seq = next++;
    if (q.size() >= cap) {
      ++n_dropped;
      return false;
    }
    q.push_back(QueuedEvent{std::move(record), cur_epoch, seq});
    ++n_accepted;
    return true;
  }

  /// Hand every queued event to the caller (the drain thread), leaving the
  /// queue empty. `out` must be empty.
  void take_all(std::deque<QueuedEvent> &out) { out.swap(q); }

  size_t size() const { return q.size(); }
  size_t capacity() const { return cap; }
  uint64_t epoch() const { return cur_epoch; }
  /// the sequence number the next event will get
  uint64_t next_seq() const { return next; }
  /// events accepted into the queue (lifetime of the queue)
  uint64_t accepted() const { return n_accepted; }
  /// events refused because the queue was full (lifetime of the queue)
  uint64_t dropped() const { return n_dropped; }

private:
  std::deque<QueuedEvent> q;
  size_t cap;
  uint64_t cur_epoch = 0;
  uint64_t next = 1;
  uint64_t n_accepted = 0;
  uint64_t n_dropped = 0;
};

} // namespace cephfs_notify

#endif // CEPH_MDS_CHANGE_NOTIFY_QUEUE_H
