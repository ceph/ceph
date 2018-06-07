#include "ThreadPool.h"
#include "crimson/net/Config.h"
#include "include/intarith.h"

namespace ceph::thread {

ThreadPool::ThreadPool(size_t n_threads,
                       size_t queue_sz)
  : queue_size{round_up_to(queue_sz, seastar::smp::count)},
    pending{queue_size}
{
  for (size_t i = 0; i < n_threads; i++) {
    threads.emplace_back([this] {
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

void ThreadPool::loop()
{
  for (;;) {
    WorkItem* work_item = nullptr;
    {
      std::unique_lock lock{mutex};
      cond.wait_for(lock,
                    ceph::net::conf.threadpool_empty_queue_max_wait,
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

} // namespace ceph::thread
