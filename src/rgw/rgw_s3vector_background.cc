// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <memory>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include "common/ceph_time.h"
#include "common/dout.h"
#include <chrono>
#include <fmt/format.h>
#include "common/async/yield_waiter.h"
#include <future>
#include <string>
#include <unordered_map>
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3vector {

class Manager : public DoutPrefixProvider {

  // use mmap/mprotect to allocate 128k coroutine stacks
  auto make_stack_allocator() {
    return boost::context::protected_fixedsize_stack{128*1024};
  }
  using table_name_t = std::pair<std::string, std::string>; // pair of vector bucket name and index name
  using MessageQueue =  boost::lockfree::queue<table_name_t*, boost::lockfree::fixed_sized<true>>;
  using Executor = boost::asio::io_context::executor_type;
  bool shutdown = false;
  CephContext* const cct;
  boost::asio::io_context io_context;
  boost::asio::executor_work_guard<Executor> work_guard;
  std::vector<std::thread> workers;
  rgw::sal::Driver* const driver;
  std::unordered_map<table_name_t, ceph::coarse_real_time, boost::hash<table_name_t>> tables;
  MessageQueue messages;

  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "s3vectors manager: "; }

  class tokens_waiter {
    size_t pending_tokens = 0;
    DoutPrefixProvider* const dpp;
    ceph::async::yield_waiter<void> waiter;

  public:
    class token{
      tokens_waiter* tw;
    public:
      token(const token& other) = delete;
      token(token&& other) : tw(other.tw) {
        other.tw = nullptr; // mark as moved
      }
      token& operator=(const token& other) = delete;
      token(tokens_waiter* _tw) : tw(_tw) {
        ++tw->pending_tokens;
      }

      ~token() {
        if (!tw) {
          return; // already moved
        }
        --tw->pending_tokens;
        if (tw->pending_tokens == 0 && tw->waiter) {
          tw->waiter.complete(boost::system::error_code{});
        }
      }
    };

    tokens_waiter(DoutPrefixProvider* _dpp) : dpp(_dpp) {}
    tokens_waiter(const tokens_waiter& other) = delete;
    tokens_waiter& operator=(const tokens_waiter& other) = delete;

    void async_wait(boost::asio::yield_context yield) {
      if (pending_tokens == 0) {
        return;
      }
      ldpp_dout(dpp, 20) << "INFO: tokens waiter is waiting on " <<
        pending_tokens << " tokens" << dendl;
      boost::system::error_code ec;
      waiter.async_wait(yield[ec]);
      ldpp_dout(dpp, 20) << "INFO: tokens waiter finished waiting for all tokens" << dendl;
    }
  };

  void async_sleep(boost::asio::yield_context yield, const std::chrono::milliseconds& duration) {
    using Clock = ceph::coarse_mono_clock;
    using Timer = boost::asio::basic_waitable_timer<Clock,
        boost::asio::wait_traits<Clock>, Executor>;
    Timer timer(io_context);
    timer.expires_after(duration);
    boost::system::error_code ec;
    timer.async_wait(yield[ec]);
    if (ec) {
      ldpp_dout(this, 1) << "ERROR: async_sleep failed with error: " << ec.message() << dendl;
    }
  }

  // processing of a specific table
  int process_table(const table_name_t& table_name, boost::asio::yield_context yield) {
    // TODO: check if processing is needed based on unindexed rows stats and skip if not needed
    // TODO: check if processign already started for the table and skip if yes
    // TODO: implement actual lancedb table processing logic here
    // for PoC just sleep for some time to simulate processing
    ldpp_dout(this, 20) << "INFO: started processing table: " << table_name.first << "." << table_name.second << dendl;
    async_sleep(yield, std::chrono::milliseconds(1000));
    ldpp_dout(this, 20) << "INFO: done processing table: " << table_name.first << "." << table_name.second << dendl;
    return 0;
  }

  // process all work items
  void process_tables(boost::asio::yield_context yield) {
    ldpp_dout(this, 5) << "INFO: start processing tables" << dendl;
    while (!shutdown) {
      std::vector<table_name_t> tables_to_process;
      const auto message_count = messages.consume_all([&tables_to_process, this](auto message) {
        std::unique_ptr<table_name_t> message_guard(message);
        const auto table_name = std::move(*message);
        auto [it, inserted] = tables.emplace(table_name, ceph::coarse_real_clock::now());
        if (inserted) {
          ldpp_dout(this, 20) << "INFO: will try to process new table: " << table_name.first << "." << table_name.second << dendl;
          tables_to_process.push_back(table_name);
          return;
        }
        const auto now = ceph::coarse_real_clock::now();
        const auto time_since_last_process = now - it->second;
        if (time_since_last_process > std::chrono::milliseconds(5000)) {
          ldpp_dout(this, 20) << "INFO: will try to process table: " << table_name.first << "." << table_name.second <<
          ". " << time_since_last_process << " passed since last processing" << dendl;
          it->second = now;
          tables_to_process.push_back(table_name);
        } else {
          ldpp_dout(this, 20) << "INFO: will skip processing table: " << table_name.first << "." << table_name.second <<
          ". only " << time_since_last_process << " passed since last processing" << dendl;
        }
      });
      tokens_waiter tw(this);
      for (const auto& table_name : tables_to_process) {
        // start processing a table
        tokens_waiter::token token(&tw);
        boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
            [this, table_name](boost::asio::yield_context yield) {
          const int rc = process_table(table_name, yield);
          if (rc < 0) {
            ldpp_dout(this, 1) << "ERROR: failed to process table: " << table_name.first << "." << table_name.second << " with error code: " << rc << dendl;
            tables[table_name] = ceph::coarse_real_clock::now() - std::chrono::milliseconds(5000); // set last processed time to past to allow retry on next loop
          }
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });
      }
      if (!tables_to_process.empty()) {
        // wait for all pending work to finish
        tw.async_wait(yield);
      }
      if (message_count == 0) {
        // if no messages, sleep for a while before checking again
        ldpp_dout(this, 20) << "INFO: no tables to process" << dendl;
        async_sleep(yield, std::chrono::milliseconds(1000));
      }
    }
    ldpp_dout(this, 5) << "INFO: manager stopped. done processing all tables" << dendl;
   }

public:

  ~Manager() = default;

  void stop() {
    ldpp_dout(this, 5) << "INFO: manager received stop signal. shutting down..." << dendl;
    shutdown = true;
    work_guard.reset();
    for (auto& worker : workers) {
      if (worker.joinable()) {
        // try graceful shutdown first
        auto future = std::async(std::launch::async, [&worker]() {worker.join();});
        if (future.wait_for(std::chrono::milliseconds(1000)) == std::future_status::timeout) {
          // force stop if graceful shutdown takes too long
          if (!io_context.stopped()) {
            ldpp_dout(this, 5) << "INFO: force shutdown of manager" << dendl;
            io_context.stop();
          }
          worker.join();
        }
      }
    }
    ldpp_dout(this, 5) << "INFO: manager shutdown ended" << dendl;
  }

  void init() {
    boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
        [this](boost::asio::yield_context yield) {
          process_tables(yield);
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });

    // start the worker threads to do the actual queue processing
    // TODO: use multiple threads
    workers.emplace_back(std::thread([this]() {
      ceph_pthread_setname("notif-worker");
      try {
        ldpp_dout(this, 10) << "INFO: worker started" << dendl;
        io_context.run();
        ldpp_dout(this, 10) << "INFO: worker ended" << dendl;
      } catch (const std::exception& err) {
        ldpp_dout(this, 1) << "ERROR: worker failed with error: " << err.what() << dendl;
        throw err;
      }
    }));
    ldpp_dout(this, 10) << "INfO: started manager" << dendl;
  }

  bool notify_index_update(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name) {
    if (shutdown) {
      ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about index update: manager is shutting down" << dendl;
      return false;
    }
    auto message_guard = std::make_unique<table_name_t>(bucket_name, index_name);
    if (messages.push(message_guard.get())) {
      std::ignore = message_guard.release(); // ownership transferred to the queue
      ldpp_dout(dpp, 20) << "INFO: notified s3vectors manager about index update" << dendl;
      return true;
    }
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about index update: queue is full" << dendl;
    return false;
  }

  Manager(CephContext* _cct, rgw::sal::Driver* _driver) :
    cct(_cct),
    work_guard(boost::asio::make_work_guard(io_context)),
    driver(_driver),
    messages(8192)
    {}
};

std::unique_ptr<Manager> s_manager;

bool init(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver) {
  if (s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to init s3vectors manager: already exists" << dendl;
    return false;
  }
  s_manager = std::make_unique<Manager>(dpp->get_cct(), driver);
  s_manager->init();
  return true;
}

void shutdown() {
  if (!s_manager) return;
  s_manager->stop();
  s_manager.reset();
}

bool notify_index_update(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name) {
  if (!s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify s3vectors manager about table update: manager is not initialized" << dendl;
    return false;
  }
  return s_manager->notify_index_update(dpp, bucket_name, index_name);
}

} // namespace rgw::s3vector

