#include "thread_pool.h"

#include <chrono>
#include <pthread.h>
#include "include/intarith.h"

#include "include/ceph_assert.h"
#include "crimson/common/config_proxy.h"

using crimson::common::local_conf;

namespace crimson::os {

ThreadPool::ThreadPool(size_t n_threads,
                       size_t queue_sz,
                       unsigned cpu_id)
  : queue_size{round_up_to(queue_sz, seastar::smp::count)},
    pending{queue_size}
{
  auto queue_max_wait = std::chrono::seconds(local_conf()->threadpool_empty_queue_max_wait);
  for (size_t i = 0; i < n_threads; i++) {
    threads.emplace_back([this, cpu_id, queue_max_wait] {
      pin(cpu_id);
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

void ThreadPool::pin(unsigned cpu_id)
{
  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(cpu_id, &cs);
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
