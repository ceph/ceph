// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "thread_pool.h"

#include <chrono>
#include <pthread.h>

#include "include/ceph_assert.h"
#include "crimson/common/config_proxy.h"

using crimson::common::local_conf;

namespace crimson::os {

ThreadPool::ThreadPool(size_t n_threads,
                       size_t queue_sz,
                       std::vector<uint64_t> cpus)
  : n_threads(n_threads),
    queue_size{round_up_to(queue_sz, seastar::smp::count)},
    pending_queues(n_threads)
{
  auto queue_max_wait = std::chrono::seconds(local_conf()->threadpool_empty_queue_max_wait);
  for (size_t i = 0; i < n_threads; i++) {
    threads.emplace_back([this, cpus, queue_max_wait, i] {
      if (!cpus.empty()) {
        pin(cpus);
      }
      (void) pthread_setname_np(pthread_self(), "alien-store-tp");
      loop(queue_max_wait, i);
    });
  }
}

ThreadPool::~ThreadPool()
{
  for (auto& thread : threads) {
    thread.join();
  }
}

void ThreadPool::pin(const std::vector<uint64_t>& cpus)
{
  cpu_set_t cs;
  CPU_ZERO(&cs);
  for (auto cpu : cpus) {
    CPU_SET(cpu, &cs);
  }
  [[maybe_unused]] auto r = pthread_setaffinity_np(pthread_self(),
                                                   sizeof(cs), &cs);
  ceph_assert(r == 0);
}

void ThreadPool::loop(std::chrono::milliseconds queue_max_wait, size_t shard)
{
  auto& pending = pending_queues[shard];
  for (;;) {
    WorkItem* work_item = nullptr;
    work_item = pending.pop_front(queue_max_wait);
    if (work_item) {
      work_item->process();
    } else if (is_stopping()) {
      break;
    }
  }
}

seastar::future<> ThreadPool::start()
{
  auto slots_per_shard = queue_size / seastar::smp::count;
  return submit_queue.start(slots_per_shard);
}

seastar::future<> ThreadPool::stop()
{
  return submit_queue.stop().then([this] {
    stopping = true;
    for (auto& q : pending_queues) {
      q.stop();
    }
  });
}

} // namespace crimson::os
