// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Daniel Oliveira <doliveira@suse.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef COMMON_UTILS_HPP
#define COMMON_UTILS_HPP

/* Include order and names:
 * a) Immediate related header
 * b) C libraries (if any),
 * c) C++ libraries,
 * d) Other support libraries
 * e) Other project's support libraries
 *
 * Within each section the includes should
 * be ordered alphabetically.
 */

#include <sys/time.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>


namespace common_utils {

  using system_clock  = std::chrono::system_clock;

  /// Common constants used.
  static auto constexpr ZERO(0);
  static auto constexpr ONE(1);
  static auto constexpr VECTOR_GROWTH_FACTOR(2);
  static auto constexpr FIRST_PRINTABLE_CHAR(20);
  static const char BLANK(' ');
  static const char DOT('.');
  static const char OPEN_OID('{');
  static const char CLOSE_OID('}');
  static const char COLON(':');
  static const char SLASH('/');
  static const std::string PROTO_SEP("://");
  static const std::string NEWLINE("\n");
  static const std::string EMPTY_STR("");

  /// All common messages are defined here
  //-- TODO: Convert (where possible) std::string to std::string_view.
  static const std::string
      MSG_FAILED_DNS_RESOLV("Failed to resolve host/DNS name: ");
  static const std::string MSG_FAILED_CONNECTION("Failed to connect to: ");
  static const std::string MSG_FAILED_SOCKET("Failed to create socket: ");
  static const std::string MSG_ERR_CODE(" Error code: ");
  static const std::string MSG_ERR_MESSAGE(" Message: ");


  const auto is_digit_or_blank = [](const char c) {
      return (std::isdigit(c) or std::isblank(c));
  };

  const auto is_empty = [](const std::string& s) {
      return s.empty();
  };

  const auto is_open_or_close = [](const char c) {
      return ((c == OPEN_OID) or (c == CLOSE_OID));
  };


  std::string str_trim(const std::string&);
  auto from_const_str_to_char(const std::string&);
  auto from_const_ptr_char_to_string(const char*);
  auto duplicate_ptr_char(const char*str_to_convert);
  uint32_t auth_trap_error(const uint32_t);
  timeval from_chrono_to_timeval(const system_clock::time_point&);
  system_clock::time_point from_timeval_to_chrono(const timeval& time_val);


  template<typename Etp_>
  using enable_enum_t =
    std::enable_if_t<std::is_enum<Etp_>::value,
                            std::underlying_type_t<Etp_>>;

  template<typename Etp_>
  constexpr inline enable_enum_t<Etp_>
  enum_underlying_value(Etp_ enm) noexcept
  {
    return static_cast<std::underlying_type_t<Etp_>>(enm);
  }

  template<typename Enm, typename Etp_>
  constexpr inline typename
  std::enable_if_t<std::is_enum<Enm>::value &&
                   std::is_integral<Etp_>::value, Enm>::type
  convert_to_enum(Etp_ value) noexcept
  {
    return static_cast<Enm>(value);
  }

}   //-- namespace common_utils


namespace timer {
    using chrono_hi_resolution =
    std::chrono::high_resolution_clock;
    using chrono_tp_t          =
    std::chrono::high_resolution_clock::time_point;

    class Timer {
    public:
        Timer() { timer_start(); };

        ~Timer() = default;

        void timer_start() {
          m_start = chrono_hi_resolution::now();
        }

        void timer_end() {
          m_finish = chrono_hi_resolution::now();
        }

        std::chrono::nanoseconds elapsed_in_nano_secs() const {
          using std::chrono::duration_cast;
          using std::chrono::nanoseconds;
          return duration_cast<nanoseconds>
              (chrono_hi_resolution::now() - m_start);
        }

        std::chrono::microseconds elapsed_in_micro_secs() const {
          using std::chrono::duration_cast;
          using std::chrono::microseconds;
          return duration_cast<microseconds>
              (chrono_hi_resolution::now() - m_start);
        }

        std::chrono::milliseconds elapsed_in_milli_secs() const {
          using std::chrono::duration_cast;
          using std::chrono::milliseconds;
          return duration_cast<milliseconds>
              (chrono_hi_resolution::now() - m_start);
        }
        //-- TODO: Templatize elapsed_time


    private:
        chrono_tp_t m_start;
        chrono_tp_t m_finish;

        friend std::ostream& operator<<(std::ostream& out_stream,
                                        const Timer& timer) {
          const std::string MILLISEC_STR("ms");
          return out_stream
              << timer.elapsed_in_milli_secs().count()
              << MILLISEC_STR;
        }
    };
}

/*  Just a simple attempt to get a thread pool implemented.
*/
namespace threading {
    template <typename Type_>
    class ThreadQueue {
    public:
        ~ThreadQueue() { queue_invalidator(); }

        /*  Lets try to pop the first work out of the queue.
            If it all works fine, return true.
        */
        bool queue_pop_try(Type_& work) {
          std::lock_guard<std::mutex> lock{m_queue_mutex};
          if (m_queue_work.empty() || !m_is_valid) {
            return false;
          }

          work = std::move(m_queue_work.front());
          m_queue_work.pop();
          return true;
        }

        /*  Lets wait until we have work, unless we queue_clear the queue,
            or it is destroyed.
            If we are able to write to the work, return true.
        */
        bool queue_pop_wait(Type_& work) {
          std::unique_lock<std::mutex> lock{m_queue_mutex};
          m_queue_condition.wait(lock, [this]() {
                                     return (!m_queue_work.empty() ||
                                             !m_is_valid);
                                 }
          );

          /*  Avoding erroneous wakeups, like empty but valid queue.
          */
          if (!m_is_valid) {
            return false;
          }

          work = std::move(m_queue_work.front());
          m_queue_work.pop();
          return true;
        }

        /*  Lets push work to the queue.
        */
        void queue_push(Type_ work) {
          std::lock_guard<std::mutex> lock{m_queue_mutex};
          m_queue_work.queue(std::move(work));
          m_queue_condition.notify_one();
        }

        /* Is work queue empty?
        */
        bool is_queue_empty() const {
          std::lock_guard<std::mutex> lock{m_queue_mutex};
          return m_queue_work.empty();
        }

        /*  Clear out the queue.
        */
        void queue_clear() {
          std::lock_guard<std::mutex> lock{m_queue_mutex};
          while (!m_queue_work.empty()) {
            m_queue_work.pop();
          }
          m_queue_condition.notify_all();
        }

        /*  We try to avoid/ensure any conditions where a thread is trying
            to exit, but still has some work in the queue_pop_wait.
        */
        void queue_invalidator() {
          std::lock_guard<std::mutex> lock{m_queue_mutex};
          m_is_valid = false;
          m_queue_condition.notify_all();
        }

        /*  Is this a valid queue?
        */
        bool is_queue_valid() const {
          std::lock_guard<std::mutex> lock{m_queue_mutex};
          return m_is_valid;
        }

    private:
        mutable std::mutex m_queue_mutex;
        std::condition_variable m_queue_condition;
        std::queue<Type_> m_queue_work;
        std::atomic_bool m_is_valid{true};
    };


    class ThreadPool {
    public:
        /*  Note that, std::thread::hardware_concurrency() returns the
            number of concurrent threads supported by the implementation.
            The value should be considered only a hint and if not well
            defined or not computable, it will return 0.
            Here, to avoid problems/issues where it returns 0, where (0-1)
            would become std::numeric_limits<int>::max()), so we set it to:
            the max (std::max) in between hardware_concurrency() and 2u,
            then we subtract 1u.
        */
        //-- Todo: We need a better/proper initial thread number.
        ThreadPool() :
            ThreadPool{std::max(
                std::thread::hardware_concurrency(), 2u) - 1u} { }

        explicit ThreadPool(const std::uint32_t max_threads) :
            m_is_it_done{false},
            m_work_queue{},
            m_threads{} {
          try {
            for (auto i = 0u; i < max_threads; ++i) {
              m_threads.emplace_back(&ThreadPool::worker, this);
            }
          }
          catch (...) {
            thread_destroy();
            throw;
          }
        }

        ~ThreadPool() { thread_destroy(); }

        //-- No copies or assignments allowed.
        ThreadPool(const ThreadPool& rhs) = delete;

        ThreadPool& operator=(const ThreadPool& rhs) = delete;

        /*  We use std::future so this object can block/wait for the
            execution to terminate, before leaving the scope.
        */
        template <typename Type_>
        class TaskFutureWrapper {
        public:
            TaskFutureWrapper(std::future<Type_>&& future) :
                m_future{std::move(future)} { }

            ~TaskFutureWrapper() {
              if (m_future.valid()) {
                m_future.get();
              }
            }

            //-- Assignments and move constructs/ops
            TaskFutureWrapper(const TaskFutureWrapper& rhs) = delete;

            TaskFutureWrapper& operator=
                (const TaskFutureWrapper& rhs) = delete;

            TaskFutureWrapper(TaskFutureWrapper&& other) = default;

            TaskFutureWrapper& operator=
                (TaskFutureWrapper&& other) = default;

            auto get() { return m_future.get(); }

        private:
            std::future<Type_> m_future;
        };

        /*  Submit jobs to thread pool.
        */
        template <typename Func_type_, typename... Args>
        auto submit_work(Func_type_&& work_func, Args&& ... args) {
          auto task_link = std::bind(std::forward<Func_type_>(work_func),
                                     std::forward<Args>(args)...);
          using result_type = std::result_of_t<decltype(task_link)()>;
          using packaged_task = std::packaged_task<result_type()>;
          using task_type = ThreadTask<packaged_task>;

          packaged_task task{std::move(task_link)};
          TaskFutureWrapper<result_type> task_result{task.get_future()};
          m_work_queue.queue_push(std::make_unique<task_type>
                                      (std::move(task)));
          return task_result;
        }

    private:
        class IThreadTask {
        public:
            IThreadTask() = default;

            virtual ~IThreadTask() = default;

            //-- Assignments and move constructs/ops
            IThreadTask(const IThreadTask& rhs) = delete;

            IThreadTask& operator=(const IThreadTask& rhs) = delete;

            IThreadTask(IThreadTask&& other) = default;

            IThreadTask& operator=(IThreadTask&& other) = default;

            virtual void run_task() = 0;
        };

        template <typename Func_type_>
        class ThreadTask : public IThreadTask {
        public:
            ThreadTask(Func_type_&& work_func) :
                m_func{std::move(work_func)} { }

            ~ThreadTask() override = default;

            //-- Assignments and move constructs/ops
            ThreadTask(const ThreadTask& rhs) = delete;

            ThreadTask& operator=(const ThreadTask& rhs) = delete;

            ThreadTask(ThreadTask&& other) = default;

            ThreadTask& operator=(ThreadTask&& other) = default;

            void run_task() override { m_func(); }

        private:
            Func_type_ m_func;
        };

        /*  Checking threads to get work from queue pretty often.
        */
        void worker() {
          while (!m_is_it_done) {
            std::unique_ptr<IThreadTask> task_ptr{nullptr};
            if (m_work_queue.queue_pop_wait(task_ptr)) {
              task_ptr->run_task();
            }
          }
        }

        /* We completely invalidate and joining all threads running.
        */
        void thread_destroy() {
          m_is_it_done = true;
          m_work_queue.queue_invalidator();
          for (auto& thread : m_threads) {
            if (thread.joinable()) {
              thread.join();
            }
          }
        }

        std::atomic_bool m_is_it_done;
        ThreadQueue<std::unique_ptr<IThreadTask>> m_work_queue;
        std::vector<std::thread> m_threads;
    };

    namespace default_thread_pool {
        /* Returns the default thread pool
        */
        inline ThreadPool& get_thread_pool() {
          static ThreadPool default_pool;
          return default_pool;
        }

        /*
        * Submit a job to the default thread pool.
        */
        template <typename Func, typename... Args>
        inline auto submitJob(Func&& func, Args&& ... args) {
          return get_thread_pool().submit_work(std::forward<Func>(func),
                                               std::forward<Args>(args)...);
        }
    }
}

#endif    //-- COMMON_UTILS_HPP

// ----------------------------- END-OF-FILE --------------------------------//
