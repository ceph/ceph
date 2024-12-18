// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "thread_pool.h"

#include <chrono>
#include <pthread.h>

#include "include/ceph_assert.h"
#include "include/intarith.h" // for round_up_to()
#include "crimson/common/config_proxy.h"

using crimson::common::local_conf;

namespace crimson::os {

ThreadPool::ThreadPool(size_t n_threads,
                       size_t queue_sz,
                       const std::optional<seastar::resource::cpuset>& cpus)
  : n_threads(n_threads),
    queue_size{round_up_to(queue_sz, seastar::smp::count)},
    pending_queues(n_threads)
{
  auto queue_max_wait = std::chrono::seconds(local_conf()->threadpool_empty_queue_max_wait);
  for (size_t i = 0; i < n_threads; i++) {
    threads.emplace_back([this, cpus, queue_max_wait, i] {
      if (cpus.has_value()) {
        pin(*cpus);
      }
      block_sighup();
      (void) ceph_pthread_setname("alien-store-tp");
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

void ThreadPool::pin(const seastar::resource::cpuset& cpus)
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

void ThreadPool::block_sighup()
{
  sigset_t sigs;
  sigemptyset(&sigs);
  // alien threads must ignore the SIGHUP. It's necessary as in
  // `crimson/osd/main.cc` we set a handler using the Seastar's
  // signal handling infrastrucute which assumes the `_backend`
  // of `seastar::engine()` is not null. Grep `reactor.cc` for
  // `sigaction` or just visit `reactor::signals::handle_signal()`.
  sigaddset(&sigs, SIGHUP);
  pthread_sigmask(SIG_BLOCK, &sigs, nullptr);
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
