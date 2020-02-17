#include "ThreadPool.h"

#include <pthread.h>
#include "crimson/net/Config.h"
#include "include/intarith.h"

#include "include/ceph_assert.h"

namespace crimson::thread {

ThreadPool::ThreadPool(size_t n_threads,
                       size_t queue_sz,
                       unsigned cpu_id)
  : queue_size{round_up_to(queue_sz, seastar::smp::count)},
    pending{queue_size}
{
  for (size_t i = 0; i < n_threads; i++) {
    threads.emplace_back([this, cpu_id] {
      pin(cpu_id);
      loop();
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

void ThreadPool::loop()
{
  for (;;) {
    WorkItem* work_item = nullptr;
    {
      std::unique_lock lock{mutex};
      cond.wait_for(lock,
                    crimson::net::conf.threadpool_empty_queue_max_wait,
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

} // namespace crimson::thread
