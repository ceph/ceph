// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_bucket_logging.h"
#include "rgw_bucketlogging.h"
//#include "cls/queue/cls_queue_client.h"
#include "cls/lock/cls_lock_client.h"
#include <memory>
#include <boost/algorithm/hex.hpp>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include "include/function2.hpp"
#include "rgw_sal_rados.h"
#include "rgw_zone_features.h"
#include "rgw_perf_counters.h"
#include "services/svc_zone.h"
#include "common/dout.h"
#include "rgw_url.h"
#include <chrono>
#include <fmt/format.h>
#include "librados/AioCompletionImpl.h"
#include "common/async/yield_waiter.h"
#include <future>
#include <thread>

#include <unordered_map>

#define dout_subsys ceph_subsys_rgw_bucket_logging

namespace rgw::bucket_logging {


using queues_t = std::set<std::string>;

// use mmap/mprotect to allocate 128k coroutine stacks
auto make_stack_allocator() {
  return boost::context::protected_fixedsize_stack{128*1024};
}

const std::string Q_LIST_OBJECT_NAME = "bucket_logging_commit_queues";

class BucketLoggingManager : public DoutPrefixProvider {
  using Executor = boost::asio::io_context::executor_type;
  bool shutdown = false;
  static constexpr auto queues_update_period = std::chrono::milliseconds(30000); // 30s
  static constexpr auto queues_update_retry = std::chrono::milliseconds(1000); // 1s
  static constexpr auto queue_idle_sleep = std::chrono::milliseconds(5000); // 5s
  const utime_t failover_time = utime_t(queues_update_period*3); // 90s
  CephContext* const cct;
  static constexpr auto COOKIE_LEN = 16;
  const std::string lock_cookie;
  boost::asio::io_context io_context;
  boost::asio::executor_work_guard<Executor> work_guard;
  std::thread worker;
  const SiteConfig& site;
  rgw::sal::RadosStore* const rados_store;

private:

  CephContext *get_cct() const override { return cct; }

  unsigned get_subsys() const override { return dout_subsys; }

  std::ostream& gen_prefix(std::ostream& out) const override {
    return out << "rgw bucket_logging: ";
  }


  int parse_queue_name(std::string queue_name, std::string &tenant_name,
                       std::string &bucket_name, std::string &prefix) {

    // <tenant_name>.<bucket_name>.<prefix>
    auto pos = queue_name.find('.');
    if (pos != std::string::npos) {
      tenant_name = queue_name.substr(0, pos);
      auto pos1 = queue_name.find('.', pos + 1);
      if (pos1 != std::string::npos) {
	bucket_name = queue_name.substr(pos + 1, pos1 - pos - 1);
	if (bucket_name.empty()) {
	  return -EINVAL;
	}
	prefix = queue_name.substr(pos1 + 1);
	return 0;
      }
    }
    return -EINVAL;
  }

  // read the list of queues from the queue list object
  int read_queue_list(queues_t& queues, optional_yield y) {
    constexpr auto max_chunk = 1024U;
    std::string start_after;
    bool more = true;
    int rval;
    while (more) {
      librados::ObjectReadOperation op;
      queues_t queues_chunk;
      op.omap_get_keys2(start_after, max_chunk, &queues_chunk, &more, &rval);
      const auto ret = rgw_rados_operate(this, rados_store->getRados()->get_logging_pool_ctx(),
                                         Q_LIST_OBJECT_NAME, std::move(op),
                                         nullptr, y);
      if (ret == -ENOENT) {
        // queue list object was not created - nothing to do
        return 0;
      }
      if (ret < 0) {
        // TODO: do we need to check on rval as well as ret?
        ldpp_dout(this, 1) << "ERROR: failed to read queue list. error: " << ret << dendl;
        return ret;
      }
      queues.merge(queues_chunk);
    }
    return 0;
  }

/*
  // set m1 to be the minimum between m1 and m2
  static int set_min_marker(std::string& m1, const std::string m2) {
    cls_queue_marker mr1;
    cls_queue_marker mr2;
    if (mr1.from_str(m1.c_str()) < 0 || mr2.from_str(m2.c_str()) < 0) {
      return -EINVAL;
    }
    if (mr2.gen <= mr1.gen && mr2.offset < mr1.offset) {
      m1 = m2;
    }
    return 0;
  }
*/
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

  enum class EntryProcessingResult {
    Failure, Successful, Sleeping, Expired
  };
  std::vector<std::string> entryProcessingResultString = {"Failure", "Successful", "Sleeping", "Expired", "Migrating"};

  // processing of a specific entry
  // return whether processing was successful (true) or not (false)
  EntryProcessingResult process_entry(
      const ConfigProxy& conf,
    //  rgw::sal::RadosBucket &log_bucket,
      const std::string& entry,
      const std::string& tenant_name,
      const std::string& bucket_name,
      const std::string& prefix,
      boost::asio::yield_context yield,
      int& ret) {

    std::unique_ptr<rgw::sal::Bucket> log_bucket;
    rgw_bucket bucket_id = rgw_bucket{tenant_name, bucket_name};
    ret = rados_store->load_bucket(this, bucket_id, &log_bucket, yield);
    if (ret < 0) {
      ldpp_dout(this, 10) << "ERROR: failed to load bucket : "
                          << bucket_name << dendl;
      return EntryProcessingResult::Failure;
    }
    ret = log_bucket->commit_logging_object(entry, yield, this, prefix, nullptr, false);
    if (ret < 0) {
      ldpp_dout(this, 10) << "ERROR: failed to commit logging object : "
                          << entry << dendl;
      return EntryProcessingResult::Failure;
    }
    return EntryProcessingResult::Successful;
  }

  void async_sleep(boost::asio::yield_context yield, const std::chrono::milliseconds& duration) {
    using Clock = ceph::coarse_mono_clock;
    using Timer = boost::asio::basic_waitable_timer<Clock,
        boost::asio::wait_traits<Clock>, Executor>;
    Timer timer(io_context);
    timer.expires_after(duration);
    boost::system::error_code ec;
    timer.async_wait(yield[ec]);
    if (ec) {
      ldpp_dout(this, 1) << "ERROR: async_sleep failed with error: "
                         << ec.message() << dendl;
    }
  }

  // unlock (lose ownership) queue
  int unlock_queue(const std::string& queue_name, boost::asio::yield_context yield) {

    librados::ObjectWriteOperation op;
    op.assert_exists();
    rados::cls::lock::unlock(&op, queue_name+"_lock", lock_cookie);
    auto& rados_ioctx = rados_store->getRados()->get_logging_pool_ctx();
    const auto ret = rgw_rados_operate(this, rados_ioctx, queue_name,
                                       std::move(op), yield);
    if (ret == -ENOENT) {
      ldpp_dout(this, 10) << "INFO: queue: " << queue_name
        << ". was removed. nothing to unlock" << dendl;
      return 0;
    }
    if (ret == -EBUSY) {
      ldpp_dout(this, 10) << "INFO: queue: " << queue_name
        << ". already owned by another RGW. no need to unlock" << dendl;
      return 0;
    }
    return ret;
  }

  // processing of a specific queue
  void process_queue(const std::string& queue_name, boost::asio::yield_context yield) {
    constexpr auto max_elements = 1024;
    auto is_idle = false;
    const std::string start_marker;

    std::string tenant_name, bucket_name, prefix;
    int ret = parse_queue_name(queue_name, tenant_name, bucket_name, prefix);
    if (ret) {
      ldpp_dout(this, 10) << "ERROR: queue: " << queue_name
                          << " is invalid. Processing will stop" << dendl;
      return;
    }
    ldpp_dout(this, 10) << "NITHYA: INFO: parse queue_name: " << queue_name
                        << ", bucket_name: " << bucket_name
                        << ", tenant_name: " << tenant_name
                        << ", prefix: " << prefix << dendl;

    while (!shutdown) {
      // if queue was empty the last time, sleep for idle timeout
      if (is_idle) {
        async_sleep(yield, queue_idle_sleep);
      }

/*
      rgw_bucket bucket_id = rgw_bucket{tenant_name, bucket_name};
      rgw::sal::RadosBucket target_bucket(rados_store, bucket_id);
      ret = target_bucket.load_bucket(this, yield);
*/
      if (ret < 0) {
        ldpp_dout(this, 10) << "ERROR: failed to load bucket: " << bucket_name
                            << " is invalid. Processing will stop" << dendl;
        return;
      }

      // get list of entries in the queue
      auto& rados_ioctx = rados_store->getRados()->get_logging_pool_ctx();
      is_idle = true;
      queues_t entries;
      auto total_entries = 0U;
      {
	librados::ObjectReadOperation op;
        op.assert_exists();
        bufferlist obl;
        int rval;
	constexpr auto max_chunk = 1024U;
	std::string start_after;
	bool more = true;
        rados::cls::lock::assert_locked(&op, queue_name+"_lock",
          ClsLockType::EXCLUSIVE,
          lock_cookie,
          "" /*no tag*/);
	op.omap_get_keys2(start_after, max_chunk, &entries, &more, &rval);
        // check ownership and list entries in one batch
        auto ret = rgw_rados_operate(this, rados_ioctx, queue_name, std::move(op),
                                     nullptr, yield);
        if (ret == -ENOENT) {
          // queue was deleted
          ldpp_dout(this, 10) << "INFO: queue: " << queue_name
                              << " was removed. Processing will stop" << dendl;
          return;
        }
        if (ret == -EBUSY) {
          ldpp_dout(this, 10)
              << "WARNING: queue: " << queue_name
              << " ownership moved to another daemon. Processing will stop"
              << dendl;
          return;
        }
        if (ret < 0) {
          ldpp_dout(this, 5) << "WARNING: failed to get list of entries in queue "
                            << "and/or lock queue: " << queue_name
                            << ". error: " << ret << " (will retry)" << dendl;
          continue;
        }
      }
      total_entries = entries.size();
      if (total_entries == 0) {
        // nothing in the queue
        continue;
      }
      // log when queue is not idle
      ldpp_dout(this, 20) << "INFO: found: " << total_entries << " entries in: "
                          << queue_name << dendl;
      auto stop_processing = false;
      std::set<std::string> remove_entries;
      tokens_waiter tw(this);
      for (auto& entry : entries) {
        if (stop_processing) {
          break;
        }

        ldpp_dout(this, 20) << "NITHYA: INFO: processing entry: " << entry << dendl;
        tokens_waiter::token token(&tw);
        boost::asio::spawn(yield, std::allocator_arg, make_stack_allocator(),
          [this, &is_idle, &queue_name, total_entries, &remove_entries,
           &stop_processing, &tenant_name, &bucket_name, &prefix, token = std::move(token),
           &entry](boost::asio::yield_context yield) {
            int result_code;
            auto result =
                process_entry(this->get_cct()->_conf, entry, tenant_name,
                              bucket_name, prefix, yield, result_code);
            if (result == EntryProcessingResult::Successful) {
              ldpp_dout(this, 20) << "INFO: processing of entry: " << entry
                << " from: " << queue_name << " "
                << entryProcessingResultString[static_cast<unsigned int>(result)] << dendl;
              remove_entries.insert(entry);
              is_idle = false;
              return;
            }
            if (result == EntryProcessingResult::Sleeping) {
              ldpp_dout(this, 20) << "INFO: skipped processing of entry: " << entry
                << " from: " << queue_name << dendl;
            } else {
              is_idle = (result_code == -EBUSY);
              ldpp_dout(this, 20) << "INFO: failed processing of entry: " <<
                entry << " from: " << queue_name <<
                " result code: " << cpp_strerror(-result_code) << dendl;
            }
            stop_processing = true;
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });
      }

      if (!entries.empty()) {
        // wait for all pending work to finish
        tw.async_wait(yield);
      }

      // delete all processed entries from queue
      if (!remove_entries.empty()) {

        librados::ObjectWriteOperation op;
        op.assert_exists();
        rados::cls::lock::assert_locked(&op, queue_name+"_lock",
          ClsLockType::EXCLUSIVE,
          lock_cookie,
          "" /*no tag*/);
        op.omap_rm_keys(remove_entries);
        // check ownership and delete entries in one batch
        auto ret = rgw_rados_operate(this, rados_ioctx, queue_name, std::move(op), yield);
        if (ret == -ENOENT) {
          // queue was deleted
          ldpp_dout(this, 10) << "INFO: queue: " << queue_name
                              << ". was removed. processing will stop" << dendl;
          return;
        }
        if (ret == -EBUSY) {
          ldpp_dout(this, 10)
              << "WARNING: queue: " << queue_name
              << " ownership moved to another daemon. processing will stop"
              << dendl;
          return;
        }
        if (ret < 0) {
          ldpp_dout(this, 1) << "ERROR: failed to remove entries and/or lock"
                             << " from queue: "
                             << queue_name << ". error: " << ret << dendl;
          return;
        } else {
          ldpp_dout(this, 20) << "INFO: removed entries from queue: "
                              << queue_name << dendl;
        }
      }
    }
    ldpp_dout(this, 5) << "INFO: manager stopped. done processing for queue: " << queue_name << dendl;
  }

  // lits of owned queues
  using owned_queues_t = std::unordered_set<std::string>;

  // process all queues
  // find which of the queues is owned by this daemon and process it
  void process_commit_queues(boost::asio::yield_context yield) {
    auto has_error = false;
    owned_queues_t owned_queues;
    std::atomic<size_t> processed_queue_count = 0;

    std::vector<std::string> queue_gc;
    std::mutex queue_gc_lock;
    auto& rados_ioctx = rados_store->getRados()->get_logging_pool_ctx();
    auto next_check_time = ceph::coarse_real_clock::zero();
    while (!shutdown) {
      // check if queue list needs to be refreshed
      if (ceph::coarse_real_clock::now() > next_check_time) {
        next_check_time = ceph::coarse_real_clock::now() + queues_update_period;
        const auto tp = ceph::coarse_real_time::clock::to_time_t(next_check_time);
        ldpp_dout(this, 20) << "INFO: processing queue list. next queues "
                            << "processing will happen at: " << std::ctime(&tp)
                            << dendl;
      } else {
        // short sleep duration to prevent busy wait when refreshing queue list
        // or retrying after error
        ldpp_dout(this, 20) << "INFO: NITHYA : processing commit queues list. Sleep now." << dendl;
        async_sleep(yield, queues_update_retry);
        if (!has_error) {
          // in case of error we will retry
          continue;
        }
      }

      queues_t queues;
      auto ret = read_queue_list(queues, yield);
      if (ret < 0) {
        has_error = true;
        ldpp_dout(this, 20) << "ERROR: NITHYA : failed to read queue list. Retry." << dendl;
        continue;
      }

      for (const auto& queue_name : queues) {
        // try to lock the queue to check if it is owned by this rgw
        // or if ownership needs to be taken
        ldpp_dout(this, 20) << "NITHYA: processing queue:" << queue_name << dendl;
        librados::ObjectWriteOperation op;
        op.assert_exists();
        rados::cls::lock::lock(&op, queue_name+"_lock",
              ClsLockType::EXCLUSIVE,
              lock_cookie,
              "" /*no tag*/,
              "" /*no description*/,
              failover_time,
              LOCK_FLAG_MAY_RENEW);

        ret = rgw_rados_operate(this, rados_ioctx, queue_name, std::move(op), yield);
        if (ret == -EBUSY) {
          // lock is already taken by another RGW
          ldpp_dout(this, 20) << "INFO: queue: " << queue_name << " owned (locked) by another daemon" << dendl;
          // if queue was owned by this RGW, processing should be stopped, queue would be deleted from list afterwards
          continue;
        }
        if (ret == -ENOENT) {
          // queue is deleted - processing will stop the next time we try to read from the queue
          ldpp_dout(this, 10) << "INFO: queue: " << queue_name << " should not be locked - already deleted" << dendl;
          continue;
        }
        if (ret < 0) {
          // failed to lock for another reason, continue to process other queues
          ldpp_dout(this, 1) << "ERROR: failed to lock queue: " << queue_name << ". error: " << ret << dendl;
          has_error = true;
          continue;
        }
        // add queue to list of owned queues
        if (owned_queues.insert(queue_name).second) {
          ldpp_dout(this, 10) << "INFO: queue: " << queue_name << " now owned (locked) by this daemon" << dendl;
          // start processing this queue
          boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
                             [this, &queue_gc, &queue_gc_lock, queue_name, &processed_queue_count](boost::asio::yield_context yield) {
            ++processed_queue_count;
            process_queue(queue_name, yield);
            // if queue processing ended, it means that the queue was removed or not owned anymore
            const auto ret = unlock_queue(queue_name, yield);
            if (ret < 0) {
              ldpp_dout(this, 5) << "WARNING: failed to unlock queue: " << queue_name << " with error: " <<
                ret << " (ownership would still move if not renewed)" << dendl;
            } else {
              ldpp_dout(this, 10) << "INFO: queue: " << queue_name << " not locked (ownership can move)" << dendl;
            }
            // mark it for deletion
            std::lock_guard lock_guard(queue_gc_lock);
            queue_gc.push_back(queue_name);
            --processed_queue_count;
            ldpp_dout(this, 10) << "INFO: queue: " << queue_name << " marked for removal" << dendl;
          }, [this, queue_name] (std::exception_ptr eptr) {
            ldpp_dout(this, 10) << "ERROR: queue: " << queue_name << " processing failed" << dendl;
            if (eptr) std::rethrow_exception(eptr);
          });
        } else {
          ldpp_dout(this, 20) << "INFO: queue: " << queue_name << " ownership (lock) renewed" << dendl;
        }
      }
      // erase all queue that were deleted
      {
        std::lock_guard lock_guard(queue_gc_lock);
        std::for_each(queue_gc.begin(), queue_gc.end(), [this, &owned_queues](const std::string& queue_name) {
          owned_queues.erase(queue_name);
          ldpp_dout(this, 10) << "INFO: queue: " << queue_name << " was removed" << dendl;
        });
        queue_gc.clear();
      }
    }
    while (processed_queue_count > 0) {
      ldpp_dout(this, 20) << "INFO: manager stopped. " << processed_queue_count << " queues are still being processed" << dendl;
      async_sleep(yield, queues_update_retry);
    }
    ldpp_dout(this, 5) << "INFO: manager stopped. Done processing all queues" << dendl;
  }

public:

  ~BucketLoggingManager() {
    ldpp_dout(this, 5) << "NITHYA: INFO: ~BucketLoggingManager()" << dendl;
  }

  void stop() {
    ldpp_dout(this, 5) << "INFO: manager received stop signal. shutting down..." << dendl;
    shutdown = true;
    work_guard.reset();
    if (worker.joinable()) {
      // try graceful shutdown first
      auto future = std::async(std::launch::async, [this]() {worker.join();});
      if (future.wait_for(queues_update_retry*2) == std::future_status::timeout) {
        // force stop if graceful shutdown takes too long
        if (!io_context.stopped()) {
          ldpp_dout(this, 5) << "INFO: force shutdown of manager" << dendl;
          io_context.stop();
        }
        worker.join();
      }
    }
    ldpp_dout(this, 5) << "INFO: manager shutdown ended" << dendl;
  }

  void init() {
    boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
        [this](boost::asio::yield_context yield) {
          process_commit_queues(yield);
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });
    // start the worker threads to do the actual queue processing
    worker = std::thread([this]() {
      ceph_pthread_setname("bucket-logging-worker");
      try {
        ldpp_dout(this, 10) << "INFO: NITHYA:  bucket logging worker started" << dendl;
        io_context.run();
        ldpp_dout(this, 10) << "INFO: NITHYA: bucket logging worker ended" << dendl;
      } catch (const std::exception& err) {
        ldpp_dout(this, 1) << "ERROR: NITHYA: bucket logging worker failed with error: "
                           << err.what() << dendl;
        throw err;
      }
    });
    ldpp_dout(this, 10) << "INFO: started bucket logging manager" << dendl;
  }

  BucketLoggingManager(CephContext* _cct, rgw::sal::RadosStore* store, const SiteConfig& site) :
    cct(_cct),
    lock_cookie(gen_rand_alphanumeric(cct, COOKIE_LEN)),
    work_guard(boost::asio::make_work_guard(io_context)),
    site(site),
    rados_store(store)
    {
      ldpp_dout(this, 5) << "NITHYA: INFO: BucketLoggingManager() constructor" << dendl;
    }
};

std::unique_ptr<BucketLoggingManager> s_manager;

bool init(const DoutPrefixProvider* dpp, rgw::sal::RadosStore* store,
          const SiteConfig& site) {
  if (s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to init bucket logging manager: already exists" << dendl;
    return false;
  }
  // TODO: take conf from CephContext
  ldpp_dout(dpp, 1) << "NITHYA initialising manager" << dendl;
  s_manager = std::make_unique<BucketLoggingManager>(dpp->get_cct(), store, site);
  s_manager->init();
  return true;
}

void shutdown() {
  if (!s_manager) return;
  s_manager->stop();
  s_manager.reset();
}

/*
static inline void filter_amz_meta(meta_map_t& dest, const meta_map_t& src) {
  std::copy_if(src.cbegin(), src.cend(),
               std::inserter(dest, dest.end()),
               [](const auto& m) {
                 return (boost::algorithm::starts_with(m.first, RGW_AMZ_META_PREFIX));
               });
}
*/

} // namespace rgw::bucket_logging
