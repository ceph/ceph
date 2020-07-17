// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#pragma once

#include <atomic>
#include <condition_variable>
#include <tuple>
#include <type_traits>
#include <boost/lockfree/queue.hpp>
#include <boost/optional.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>

namespace crimson::os {

struct WorkItem {
  virtual ~WorkItem() {}
  virtual void process() = 0;
};

template<typename Func>
struct Task final : WorkItem {
  using T = std::invoke_result_t<Func>;
  using future_state_t = std::conditional_t<std::is_void_v<T>,
					    seastar::future_state<>,
					    seastar::future_state<T>>;
  using futurator_t = seastar::futurize<T>;
public:
  explicit Task(Func&& f)
    : func(std::move(f))
  {}
  void process() override {
    try {
      if constexpr (std::is_void_v<T>) {
        func();
        state.set();
      } else {
        state.set(func());
      }
    } catch (...) {
      state.set_exception(std::current_exception());
    }
    on_done.write_side().signal(1);
  }
  typename futurator_t::type get_future() {
    return on_done.wait().then([this](size_t) {
      if (state.failed()) {
	return futurator_t::make_exception_future(state.get_exception());
      } else {
	return futurator_t::from_tuple(state.get_value());
      }
    });
  }
private:
  Func func;
  future_state_t state;
  seastar::readable_eventfd on_done;
};

struct SubmitQueue {
  seastar::semaphore free_slots;
  seastar::gate pending_tasks;
  explicit SubmitQueue(size_t num_free_slots)
    : free_slots(num_free_slots)
  {}
  seastar::future<> stop() {
    return pending_tasks.close();
  }
};

/// an engine for scheduling non-seastar tasks from seastar fibers
class ThreadPool {
  std::atomic<bool> stopping = false;
  std::mutex mutex;
  std::condition_variable cond;
  std::vector<std::thread> threads;
  seastar::sharded<SubmitQueue> submit_queue;
  const size_t queue_size;
  boost::lockfree::queue<WorkItem*> pending;

  void loop(std::chrono::milliseconds queue_max_wait);
  bool is_stopping() const {
    return stopping.load(std::memory_order_relaxed);
  }
  static void pin(unsigned cpu_id);
  seastar::semaphore& local_free_slots() {
    return submit_queue.local().free_slots;
  }
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
public:
  /**
   * @param queue_sz the depth of pending queue. before a task is scheduled,
   *                 it waits in this queue. we will round this number to
   *                 multiple of the number of cores.
   * @param n_threads the number of threads in this thread pool.
   * @param cpu the CPU core to which this thread pool is assigned
   * @note each @c Task has its own crimson::thread::Condition, which possesses
   * an fd, so we should keep the size of queue under a reasonable limit.
   */
  ThreadPool(size_t n_threads, size_t queue_sz, long cpu);
  ~ThreadPool();
  seastar::future<> start();
  seastar::future<> stop();
  template<typename Func, typename...Args>
  auto submit(Func&& func, Args&&... args) {
    auto packaged = [func=std::move(func),
                     args=std::forward_as_tuple(args...)] {
      return std::apply(std::move(func), std::move(args));
    };
    return seastar::with_gate(submit_queue.local().pending_tasks,
      [packaged=std::move(packaged), this] {
        return local_free_slots().wait()
          .then([packaged=std::move(packaged), this] {
            auto task = new Task{std::move(packaged)};
            auto fut = task->get_future();
            pending.push(task);
            cond.notify_one();
            return fut.finally([task, this] {
              local_free_slots().signal();
              delete task;
            });
          });
        });
  }
};

} // namespace crimson::os
