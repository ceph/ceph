// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_bucket_logging.h"
//FIXFIX: better names
#include "rgw_bucketlogging.h"
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

#define dout_subsys ceph_subsys_rgw_bucket_logging

namespace rgw::bucket_logging {

using commit_list_t = std::set<std::string>;

// use mmap/mprotect to allocate 128k coroutine stacks
auto make_stack_allocator() {
  return boost::context::protected_fixedsize_stack{128*1024};
}

const std::string COMMIT_LIST_OBJECT_NAME = "bucket_logging_commit_list";

class BucketLoggingManager : public DoutPrefixProvider {
  using Executor = boost::asio::io_context::executor_type;
  bool m_shutdown = false;
  static constexpr auto m_log_commits_update_period = std::chrono::milliseconds(30000); // 30s
  static constexpr auto m_log_commits_update_retry = std::chrono::milliseconds(1000); // 1s
  static constexpr auto m_log_commits_idle_sleep = std::chrono::milliseconds(100); // 100ms
  const utime_t m_failover_time = utime_t(m_log_commits_update_period*3); // 90s
  CephContext* const m_cct;
  static constexpr auto COOKIE_LEN = 16;
  const std::string m_lock_cookie;
  boost::asio::io_context m_io_context;
  boost::asio::executor_work_guard<Executor> m_work_guard;
  std::thread m_worker;
  const SiteConfig& m_site;
  rgw::sal::RadosStore* const m_rados_store;

private:

  CephContext *get_cct() const override { return m_cct; }

  unsigned get_subsys() const override { return dout_subsys; }

  std::ostream& gen_prefix(std::ostream& out) const override {
    return out << "rgw bucket_logging: ";
  }

  int parse_list_name(std::string list_obj_name, std::string &tenant_name,
                      std::string &bucket_name, std::string &bucket_id,
                      std::string &prefix) {
    // <tenant_name>/<bucket_name>/<bucket_id>/<prefix>
    size_t pstart = 0, pend = 0;
    pend =list_obj_name.find('/');
    if (pend == std::string::npos) {
      return -EINVAL;
    }
    tenant_name = list_obj_name.substr(pstart, pend);

    pstart = pend + 1;
    pend =list_obj_name.find('/', pstart);
    if (pend == std::string::npos) {
      return -EINVAL;
    }
    bucket_name = list_obj_name.substr(pstart, pend - pstart);
    if (bucket_name.empty()) {
    return -EINVAL;
    }

    pstart = pend + 1;
    pend =list_obj_name.find('/', pstart);
    if (pend == std::string::npos) {
      return -EINVAL;
    }
    bucket_id = list_obj_name.substr(pstart, pend - pstart);
    if (bucket_id.empty()) {
      return -EINVAL;
    }
    prefix = list_obj_name.substr(pend + 1);
    return 0;
  }

  // read the list of commit lists from the global list object
  int read_commit_lists(commit_list_t& commits, optional_yield y) {
    constexpr auto max_chunk = 1024U;
    std::string start_after;
    bool more = true;
    int rval;
    while (more) {
      librados::ObjectReadOperation op;
      commit_list_t commits_chunk;
      op.omap_get_keys2(start_after, max_chunk, &commits_chunk, &more, &rval);
      const auto ret = rgw_rados_operate(this, m_rados_store->getRados()->get_logging_pool_ctx(),
                                         COMMIT_LIST_OBJECT_NAME, std::move(op),
                                         nullptr, y);
      if (ret == -ENOENT) {
        // global commit list object was not created - nothing to do
        return 0;
      }
      if (ret < 0) {
        // TODO: do we need to check on rval as well as ret?
        ldpp_dout(this, 5) << "ERROR: failed to read bucket logging commit list. error: "
                           << ret << dendl;
        return ret;
      }
      commits.merge(commits_chunk);
    }
    return 0;
  }

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
      ldpp_dout(dpp, 20) << "INFO: tokens waiter is waiting on "
                         << pending_tokens << " tokens" << dendl;
      boost::system::error_code ec;
      waiter.async_wait(yield[ec]);
      ldpp_dout(dpp, 20) << "INFO: tokens waiter finished waiting for all tokens" << dendl;
    }
  };

  // processing of a specific entry
  // return whether processing was successful (true) or not (false)
  int process_entry(
      const ConfigProxy& conf,
      const std::string& entry,
      const std::string& tenant_name,
      const std::string& bucket_name,
      const std::string& bucket_id,
      const std::string& prefix,
      boost::asio::yield_context yield) {

    std::unique_ptr<rgw::sal::Bucket> log_bucket;
    rgw_bucket bucket = rgw_bucket{tenant_name, bucket_name, bucket_id};
    int ret = m_rados_store->load_bucket(this, bucket, &log_bucket, yield);
    if (ret < 0) {
      ldpp_dout(this, 10) << "ERROR: failed to load bucket : "
                          << bucket_name << " , error: " << ret << dendl;
      return ret;
    }
    ldpp_dout(this, 10) << "INFO: loaded bucket : " << bucket_name
                        << ", id: "<< bucket_id << dendl;
    ret = log_bucket->commit_logging_object(entry, yield, this, prefix, nullptr, false);
    if (ret < 0) {
      ldpp_dout(this, 10) << "ERROR: failed to commit logging object : "
                          << entry << " , error: " << ret << dendl;
    }
    return ret;
  }

  void async_sleep(boost::asio::yield_context yield, const std::chrono::milliseconds& duration) {
    using Clock = ceph::coarse_mono_clock;
    using Timer = boost::asio::basic_waitable_timer<Clock,
        boost::asio::wait_traits<Clock>, Executor>;
    Timer timer(m_io_context);
    timer.expires_after(duration);
    boost::system::error_code ec;
    timer.async_wait(yield[ec]);
    if (ec) {
      ldpp_dout(this, 10) << "ERROR: async_sleep failed with error: "
                         << ec.message() << dendl;
    }
  }

  // unlock (lose ownership) list object
  int unlock_list_object(const std::string& list_obj_name, boost::asio::yield_context yield) {

    librados::ObjectWriteOperation op;
    op.assert_exists();
    rados::cls::lock::unlock(&op, list_obj_name+"_lock", m_lock_cookie);
    auto& rados_ioctx = m_rados_store->getRados()->get_logging_pool_ctx();
    const auto ret = rgw_rados_operate(this, rados_ioctx, list_obj_name,
                                       std::move(op), yield);
    if (ret == -ENOENT) {
      ldpp_dout(this, 10) << "INFO: log commit list: " << list_obj_name
        << ". was removed. nothing to unlock" << dendl;
      return 0;
    }
    if (ret == -EBUSY) {
      ldpp_dout(this, 10) << "INFO: log commit list: " << list_obj_name
        << ". already owned by another RGW. no need to unlock" << dendl;
      return 0;
    }
    return ret;
  }

  // processing of a specific target list
  void process_commit_list(const std::string& list_obj_name, boost::asio::yield_context yield) {
  //  constexpr auto max_elements = 1024;
    auto is_idle = false;
    const std::string start_marker;

    std::string tenant_name, bucket_name, bucket_id, prefix;
    int ret = parse_list_name(list_obj_name, tenant_name, bucket_name,
                              bucket_id, prefix);
    if (ret) {
      ldpp_dout(this, 10) << "ERROR: list name: " << list_obj_name
                          << " is invalid. Processing will stop" << dendl;
      return;
    }
    ldpp_dout(this, 20) << "INFO: parse list_name: " << list_obj_name
                        << ", bucket_name: " << bucket_name
                        << ", bucket_id: " << bucket_id
                        << ", tenant_name: " << tenant_name
                        << ", prefix: " << prefix << dendl;

    // Sanity check : can the bucket be loaded?
    std::unique_ptr<rgw::sal::Bucket> log_bucket;
    rgw_bucket bucket = rgw_bucket{tenant_name, bucket_name, bucket_id};
    ret = m_rados_store->load_bucket(this, bucket, &log_bucket, yield);
    if (ret < 0) {
      ldpp_dout(this, 10) << "ERROR: failed to load bucket : "
                          << bucket_id << " , error: " << ret << dendl;
      return;
    }

    while (!m_shutdown) {
      // if the list was empty the last time, sleep for idle timeout
      if (is_idle) {
        async_sleep(yield, m_log_commits_idle_sleep);
      }

      // Get the entries in the list after locking it
      auto& rados_ioctx = m_rados_store->getRados()->get_logging_pool_ctx();
      is_idle = true;
      std::set<std::string> entries;
      auto total_entries = 0U;
      {
	librados::ObjectReadOperation op;
        op.assert_exists();
        bufferlist obl;
        int rval;
	constexpr auto max_chunk = 1024U;
	std::string start_after;
	bool more = true;
        rados::cls::lock::assert_locked(&op, list_obj_name+"_lock",
          ClsLockType::EXCLUSIVE,
          m_lock_cookie,
          "" /*no tag*/);
	op.omap_get_keys2(start_after, max_chunk, &entries, &more, &rval);
        // check ownership and list entries in one batch
        auto ret = rgw_rados_operate(this, rados_ioctx, list_obj_name,
                                     std::move(op), nullptr, yield);
        if (ret == -ENOENT) {
          // list object was deleted
          ldpp_dout(this, 20) << "INFO: object: " << list_obj_name
                              << " was removed. Processing will stop" << dendl;
          return;
        }
        if (ret == -EBUSY) {
          ldpp_dout(this, 10) << "WARNING: object: " << list_obj_name
              << " ownership moved to another daemon. Processing will stop"
              << dendl;
          return;
        }
        if (ret < 0) {
          ldpp_dout(this, 10) << "WARNING: failed to get list of entries in "
                              << "and/or lock object: " << list_obj_name
                              << ". error: " << ret << " (will retry)" << dendl;
          continue;
        }
      }
      total_entries = entries.size();
      if (total_entries == 0) {
        // nothing in the list object
        continue;
      }
      // log when not idle
      ldpp_dout(this, 20) << "INFO: found: " << total_entries << " entries in: "
                          << list_obj_name << dendl;

      auto stop_processing = false;
      std::set<std::string> remove_entries;
      tokens_waiter tw(this);
      for (auto& entry : entries) {
        if (stop_processing) {
          break;
        }

        ldpp_dout(this, 20) << "INFO: processing entry: " << entry << dendl;
        tokens_waiter::token token(&tw);
        boost::asio::spawn(yield, std::allocator_arg, make_stack_allocator(),
          [this, &is_idle, &list_obj_name, total_entries, &remove_entries,
           &stop_processing, &tenant_name, &bucket_name, &bucket_id, &prefix,
           token = std::move(token), &entry](boost::asio::yield_context yield) {
            auto result =
                process_entry(this->get_cct()->_conf, entry, tenant_name,
                              bucket_name, bucket_id, prefix, yield);
            if (result == 0) {
              ldpp_dout(this, 20) << "INFO: processing of entry: " << entry
                                  << " from: " << list_obj_name
                                  << " was successful." << dendl;
              remove_entries.insert(entry);
              is_idle = false;
              return;
            } else {
              is_idle = (result == -EBUSY);
              ldpp_dout(this, 20) << "INFO: failed to process entry: " << entry
                                  << " from: " << list_obj_name << dendl;
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

      // delete all processed entries from list
      if (!remove_entries.empty()) {
        librados::ObjectWriteOperation op;
        op.assert_exists();
        rados::cls::lock::assert_locked(&op, list_obj_name+"_lock",
          ClsLockType::EXCLUSIVE,
          m_lock_cookie,
          "" /*no tag*/);
        op.omap_rm_keys(remove_entries);
        // check ownership and delete entries in one batch
        auto ret = rgw_rados_operate(this, rados_ioctx, list_obj_name, std::move(op), yield);
        if (ret == -ENOENT) {
          // list was deleted
          ldpp_dout(this, 10) << "INFO: " << list_obj_name
                              << ". was removed. processing will stop" << dendl;
          return;
        }
        if (ret == -EBUSY) {
          ldpp_dout(this, 10)
              << "WARNING: " << list_obj_name
              << " ownership moved to another daemon. processing will stop"
              << dendl;
          return;
        }
        if (ret < 0) {
          ldpp_dout(this, 10) << "ERROR: failed to remove entries and/or lock"
                              << " from : "
                              << list_obj_name << ". error: " << ret << dendl;
          return;
        } else {
          ldpp_dout(this, 20) << "INFO: removed entries from : "
                              << list_obj_name << dendl;
        }
      }
    }
    ldpp_dout(this, 5) << "INFO: BucketLogging manager stopped processing : "
                       << list_obj_name << dendl;
  }

  // process all commit log objects
  // find which of the commit log objects is owned by this daemon and process it
  void process_global_logging_list(boost::asio::yield_context yield) {
    auto has_error = false;
    std::unordered_set<std::string> owned_commit_logs;
    std::atomic<size_t> processed_list_count = 0;

    std::vector<std::string> commit_list_gc;
    std::mutex commit_list_gc_lock;
    auto& rados_ioctx = m_rados_store->getRados()->get_logging_pool_ctx();
    auto next_check_time = ceph::coarse_real_clock::zero();
    while (!m_shutdown) {
      // check if log commit list needs to be refreshed
      if (ceph::coarse_real_clock::now() > next_check_time) {
        next_check_time = ceph::coarse_real_clock::now() + m_log_commits_update_period;
        const auto tp = ceph::coarse_real_time::clock::to_time_t(next_check_time);
        ldpp_dout(this, 20) << "INFO: processing global logging commit list. next "
                            << "processing will happen at: " << std::ctime(&tp)
                            << dendl;
      } else {
        // short sleep duration to prevent busy wait when refreshing list
        // or retrying after error
        ldpp_dout(this, 20) << "INFO: processing global logging commit list. Sleep now." << dendl;
        async_sleep(yield, m_log_commits_update_retry);
        if (!has_error) {
          // in case of error we will retry
          continue;
        }
      }

      std::set<std::string> commit_lists;
      auto ret = read_commit_lists(commit_lists, yield);
      if (ret < 0) {
        has_error = true;
        ldpp_dout(this, 20) << "ERROR: failed to read global logging list. Retry." << dendl;
        continue;
      }

      for (const auto& list_obj_name : commit_lists) {
        // try to lock the object to check if it is owned by this rgw
        // or if ownership needs to be taken
        ldpp_dout(this, 20) << "INFO: processing list:" << list_obj_name << dendl;
        librados::ObjectWriteOperation op;
        op.assert_exists();
        rados::cls::lock::lock(&op, list_obj_name+"_lock",
              ClsLockType::EXCLUSIVE,
              m_lock_cookie,
              "" /*no tag*/,
              "" /*no description*/,
              m_failover_time,
              LOCK_FLAG_MAY_RENEW);

        ret = rgw_rados_operate(this, rados_ioctx, list_obj_name, std::move(op), yield);
        if (ret == -EBUSY) {
          // lock is already taken by another RGW
          ldpp_dout(this, 20) << "INFO: commit list: " << list_obj_name
                              << " owned (locked) by another daemon" << dendl;
          // if the list was owned by this RGW, processing should be stopped, would be deleted from list afterwards
          continue;
        }
        if (ret == -ENOENT) {
          // commit list is deleted - processing will stop the next time we try to read from it
          ldpp_dout(this, 20) << "INFO: commit list: " << list_obj_name
                              << " should not be locked - already deleted" << dendl;
          continue;
        }
        if (ret < 0) {
          // failed to lock for another reason, continue to process other lists
          ldpp_dout(this, 10) << "ERROR: failed to lock list: " << list_obj_name
                              << ". error: " << ret << dendl;
          has_error = true;
          continue;
        }
        // add the commit list to the list of owned commit_logs
        if (owned_commit_logs.insert(list_obj_name).second) {
          ldpp_dout(this, 20) << "INFO: " << list_obj_name << " now owned (locked) by this daemon" << dendl;
          // start processing this list
          boost::asio::spawn(make_strand(m_io_context), std::allocator_arg, make_stack_allocator(),
                             [this, &commit_list_gc, &commit_list_gc_lock,
                             list_obj_name, &processed_list_count](boost::asio::yield_context yield) {
            ++processed_list_count;
            process_commit_list(list_obj_name, yield);
            // if list processing ended, it means that the list object was removed or not owned anymore
            const auto ret = unlock_list_object(list_obj_name, yield);
            if (ret < 0) {
              ldpp_dout(this, 15) << "WARNING: failed to unlock object: " << list_obj_name << " with error: " <<
                ret << " (ownership would still move if not renewed)" << dendl;
            } else {
              ldpp_dout(this, 20) << "INFO: object: " << list_obj_name << " not locked (ownership can move)" << dendl;
            }
            // mark it for deletion
            std::lock_guard lock_guard(commit_list_gc_lock);
            commit_list_gc.push_back(list_obj_name);
            --processed_list_count;
            ldpp_dout(this, 20) << "INFO: " << list_obj_name << " marked for removal" << dendl;
          }, [this, list_obj_name] (std::exception_ptr eptr) {
            ldpp_dout(this, 10) << "ERROR: " << list_obj_name << " processing failed" << dendl;
            if (eptr) std::rethrow_exception(eptr);
          });
        } else {
          ldpp_dout(this, 20) << "INFO: " << list_obj_name << " ownership (lock) renewed" << dendl;
        }
      }
      // erase all list objects that were deleted
      {
        std::lock_guard lock_guard(commit_list_gc_lock);
        std::for_each(commit_list_gc.begin(), commit_list_gc.end(), [this, &owned_commit_logs](const std::string& list_obj_name) {
          owned_commit_logs.erase(list_obj_name);
          ldpp_dout(this, 20) << "INFO: commit list object: " << list_obj_name << " was removed" << dendl;
        });
        commit_list_gc.clear();
      }
    }
    while (processed_list_count > 0) {
      ldpp_dout(this, 20) << "INFO: BucketLogging manager stopped. "
                          << processed_list_count
                          << " commit lists are still being processed" << dendl;
      async_sleep(yield, m_log_commits_update_retry);
    }
    ldpp_dout(this, 5) << "INFO: BucketLogging manager stopped. Done processing all commit lists" << dendl;
  }

public:

  ~BucketLoggingManager() {
  }

  void stop() {
    ldpp_dout(this, 5) << "INFO: BucketLogging manager received stop signal. shutting down..." << dendl;
    m_shutdown = true;
    m_work_guard.reset();
    if (m_worker.joinable()) {
      // try graceful shutdown first
      auto future = std::async(std::launch::async, [this]() {m_worker.join();});
      if (future.wait_for(m_log_commits_update_retry*2) == std::future_status::timeout) {
        // force stop if graceful shutdown takes too long
        if (!m_io_context.stopped()) {
          ldpp_dout(this, 5) << "INFO: force shutdown of BucketLogging Manager" << dendl;
          m_io_context.stop();
        }
        m_worker.join();
      }
    }
    ldpp_dout(this, 5) << "INFO: BucketLogging manager shutdown complete" << dendl;
  }

  void init() {
    boost::asio::spawn(make_strand(m_io_context), std::allocator_arg, make_stack_allocator(),
        [this](boost::asio::yield_context yield) {
          process_global_logging_list(yield);
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });
    // start the worker threads to do the actual list processing
    m_worker = std::thread([this]() {
      ceph_pthread_setname("bucket-logging-worker");
      try {
        ldpp_dout(this, 20) << "INFO: bucket logging worker started" << dendl;
        m_io_context.run();
        ldpp_dout(this, 20) << "INFO: bucket logging worker ended" << dendl;
      } catch (const std::exception& err) {
        ldpp_dout(this, 10) << "ERROR: bucket logging worker failed with error: "
                           << err.what() << dendl;
        throw err;
      }
    });
    ldpp_dout(this, 10) << "INFO: started bucket logging manager" << dendl;
  }

  BucketLoggingManager(CephContext* _cct, rgw::sal::RadosStore* store, const SiteConfig& site) :
    m_cct(_cct),
    m_lock_cookie(gen_rand_alphanumeric(m_cct, COOKIE_LEN)),
    m_work_guard(boost::asio::make_work_guard(m_io_context)),
    m_site(site),
    m_rados_store(store)
    {
      ldpp_dout(this, 5) << "INFO: BucketLoggingManager() constructor" << dendl;
    }
};

std::unique_ptr<BucketLoggingManager> s_manager;

bool init(const DoutPrefixProvider* dpp, rgw::sal::RadosStore* store,
          const SiteConfig& site) {
  if (s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to init BucketLogging manager: already exists" << dendl;
    return false;
  }
  // TODO: take conf from CephContext
  ldpp_dout(dpp, 1) << "INFO: initialising BucketLogging manager" << dendl;
  s_manager = std::make_unique<BucketLoggingManager>(dpp->get_cct(), store, site);
  s_manager->init();
  return true;
}

void shutdown() {
  if (!s_manager) return;
  s_manager->stop();
  s_manager.reset();
}

} // namespace rgw::bucket_logging
