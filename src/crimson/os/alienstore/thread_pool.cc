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
  : queue_size{round_up_to(queue_sz, seastar::smp::count)},
    pending{queue_size}
{
  auto queue_max_wait = std::chrono::seconds(local_conf()->threadpool_empty_queue_max_wait);
  for (size_t i = 0; i < n_threads; i++) {
    threads.emplace_back([this, cpus, queue_max_wait] {
      if (!cpus.empty()) {
        pin(cpus);
      }
      loop(queue_max_wait);
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

void ThreadPool::loop(std::chrono::milliseconds queue_max_wait)
{
  for (;;) {
    WorkItem* work_item = nullptr;
    {
      std::unique_lock lock{mutex};
      cond.wait_for(lock, queue_max_wait,
                    [this, &work_item] {
        return pending.pop(work_item) || is_stopping();
      });
    }
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
    cond.notify_all();
  });
}

} // namespace crimson::os
