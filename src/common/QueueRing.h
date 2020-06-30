// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef QUEUE_RING_H
#define QUEUE_RING_H

#include "common/ceph_mutex.h"

#include <list>
#include <atomic>
#include <vector>

template <class T>
class QueueRing {
  struct QueueBucket {
    ceph::mutex lock = ceph::make_mutex("QueueRing::QueueBucket::lock");
    ceph::condition_variable cond;
    typename std::list<T> entries;

    QueueBucket() {}
    QueueBucket(const QueueBucket& rhs) {
      entries = rhs.entries;
    }

    void enqueue(const T& entry) {
      lock.lock();
      if (entries.empty()) {
        cond.notify_all();
      }
      entries.push_back(entry);
      lock.unlock();
    }

    void dequeue(T *entry) {
      std::unique_lock l(lock);
      while (entries.empty()) {
        cond.wait(l);
      };
      ceph_assert(!entries.empty());
      *entry = entries.front();
      entries.pop_front();
    };
  };

  std::vector<QueueBucket> buckets;
  int num_buckets;

  std::atomic<int64_t> cur_read_bucket = { 0 };
  std::atomic<int64_t> cur_write_bucket = { 0 };

public:
  QueueRing(int n) : buckets(n), num_buckets(n) {
  }

  void enqueue(const T& entry) {
    buckets[++cur_write_bucket % num_buckets].enqueue(entry);
  };

  void dequeue(T *entry) {
    buckets[++cur_read_bucket % num_buckets].dequeue(entry);
  }
};

#endif
