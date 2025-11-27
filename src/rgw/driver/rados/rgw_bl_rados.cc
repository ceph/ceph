// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_bucket_logging.h"
#include "rgw_bl_rados.h"
#include <chrono>
#include <fmt/format.h>
#include <future>
#include <memory>
#include <thread>
#include <boost/algorithm/hex.hpp>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include "common/async/yield_context.h"
#include "common/async/yield_waiter.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "cls/lock/cls_lock_client.h"
#include "include/common_fwd.h"
#include "include/function2.hpp"
#include "librados/AioCompletionImpl.h"
#include "services/svc_zone.h"
#include "rgw_common.h"
#include "rgw_perf_counters.h"
#include "rgw_sal_rados.h"
#include "rgw_zone_features.h"


#define dout_subsys ceph_subsys_rgw_bucket_logging

namespace rgw::bucketlogging {

using commit_list_t = std::set<std::string>;

// use mmap/mprotect to allocate 128k coroutine stacks
auto make_stack_allocator() {
  return boost::context::protected_fixedsize_stack{128*1024};
}

const std::string COMMIT_LIST_OBJECT_NAME = "bucket_logging_global_commit_list";
static const std::string TEMP_POOL_ATTR = "temp_logging_pool";

static std::string get_commit_list_name (const rgw::sal::Bucket* log_bucket,
                                         const std::string& prefix) {
  return fmt::format("{}/{}/{}/{}", log_bucket->get_tenant(),
                     log_bucket->get_name(),
                     log_bucket->get_bucket_id(), prefix);
}

int add_commit_target_entry(const DoutPrefixProvider* dpp,
                            rgw::sal::RadosStore* store,
                            const rgw::sal::Bucket* log_bucket,
                            const std::string& prefix,
                            const std::string& obj_name,
                            const std::string& tail_obj_name,
                            const rgw_pool& temp_data_pool,
                            optional_yield y) {

  auto& ioctx_logging = store->getRados()->get_logging_pool_ctx();
  std::string target_obj_name = get_commit_list_name (log_bucket, prefix);
  {
    librados::ObjectWriteOperation op;
    op.create(false);

    bufferlist bl;
    bl.append(tail_obj_name);
    std::map<std::string, bufferlist> new_commit_list{{obj_name, bl}};
    op.omap_set(new_commit_list);

    bufferlist pool_bl;
    encode(temp_data_pool, pool_bl);
    op.setxattr(TEMP_POOL_ATTR.c_str(), pool_bl);

    auto ret = rgw_rados_operate(dpp, ioctx_logging, target_obj_name, std::move(op), y);
    if (ret < 0){
      ldpp_dout(dpp, 1) << "ERROR: failed to add logging object entry " << obj_name
                        << " to commit list object:" << target_obj_name
                        << ". ret = " << ret << dendl;
      return ret;
    }
  }

  bufferlist empty_bl;
  std::map<std::string, bufferlist> new_commit_list{{target_obj_name, empty_bl}};

  librados::ObjectWriteOperation op;
  op.create(false);
  op.omap_set(new_commit_list);

  auto ret = rgw_rados_operate(dpp, ioctx_logging, COMMIT_LIST_OBJECT_NAME,
                               std::move(op), y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to add entry " << target_obj_name
                      << " to global bucket logging list object: "
                      << COMMIT_LIST_OBJECT_NAME << ". ret = " << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "INFO: added entry " << target_obj_name
                     << " to global bucket logging object: "
                     << COMMIT_LIST_OBJECT_NAME << dendl;
  return 0;
}

int list_pending_commit_objects(const DoutPrefixProvider* dpp,
                                rgw::sal::RadosStore* store,
                                const rgw::sal::Bucket* log_bucket,
                                const std::string& prefix,
                                std::set<std::string>& entries,
                                optional_yield y) {

  auto& ioctx_logging = store->getRados()->get_logging_pool_ctx();
  std::string target_obj_name = get_commit_list_name (log_bucket, prefix);

  entries.clear();

  constexpr auto max_chunk = 1024U;
  std::string start_after;
  bool more = true;
  int rval;
  while (more) {
    librados::ObjectReadOperation op;
    std::set <std::string> entries_chunk;
    op.omap_get_keys2(start_after, max_chunk, &entries_chunk, &more, &rval);
    const auto ret = rgw_rados_operate(dpp, ioctx_logging, target_obj_name,
                                       std::move(op), nullptr, y);
    if (ret == -ENOENT) {
      // log commit list object was not created - nothing to do
      return 0;
    }
    if (ret < 0) {
      // TODO: do we need to check on rval as well as ret?
      ldpp_dout(dpp, 1) << "ERROR: failed to read logging commit list "
                        <<  target_obj_name << " ret: " << ret << dendl;
      return ret;
    }
    entries.merge(entries_chunk);
    if (more) {
      start_after = *entries.rbegin();
    }
  }
  return 0;
}

class BucketLoggingManager : public DoutPrefixProvider {
  using Executor = boost::asio::io_context::executor_type;
  bool m_shutdown = false;
  static constexpr auto m_log_commits_update_period = std::chrono::milliseconds(10000); // 10s
  static constexpr auto m_log_commits_update_retry = std::chrono::milliseconds(1000); // 1s
  static constexpr auto m_log_commits_idle_sleep = std::chrono::milliseconds(5000); // 5s
  const utime_t m_failover_time = utime_t(m_log_commits_update_period*9); // 90s
  CephContext* const m_cct;
  static constexpr auto COOKIE_LEN = 16;
  const std::string m_lock_cookie;
  const std::string m_lock_name = "bl_mgr_lock";
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

  /*
    //TODO: Fix this
    std::regex pattern(R"(([^/]+)/([^/]+)/([^/]+)/(.*))");
    std::smatch matches;

    if (std::regex_match(list_obj_name, matches, pattern)) {
        tenant_name = matches[1].str();
        bucket_name = matches[2].str();
        bucket_id = matches[3].str();
        prefix = matches[4].str();
    } else {
        return -EINVAL;
    }
*/
    return 0;
  }

  // read the list of commit lists from the global list object
  int read_global_logging_list(commit_list_t& commits, optional_yield y) {
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
        ldpp_dout(this, 5) << "ERROR: failed to read bucket logging global commit list. error: "
                           << ret << dendl;
        return ret;
      }
      commits.merge(commits_chunk);
      if (more) {
        start_after = *commits.rbegin();
      }
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
      const std::string& temp_obj_name,
      const std::string& tenant_name,
      const std::string& bucket_name,
      const std::string& bucket_id,
      const std::string& prefix,
      const rgw_pool& obj_pool,
      bool& bucket_deleted,
      boost::asio::yield_context yield) {

    std::unique_ptr<rgw::sal::Bucket> log_bucket;
    rgw_bucket bucket = rgw_bucket{tenant_name, bucket_name, bucket_id};
    int ret = m_rados_store->load_bucket(this, bucket, &log_bucket, yield);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(this, 1) << "ERROR: failed to load bucket : "
                         << bucket_name << " id : " << bucket_id
                         << ", error: " << ret << dendl;
      return ret;
    }
    if (ret == 0) {
      ldpp_dout(this, 20) << "INFO: loaded bucket : " << bucket_name
                          << ", id: "<< bucket_id << dendl;
      ret = log_bucket->commit_logging_object(entry, yield, this, prefix,
                                              nullptr, false);
      if (ret < 0) {
        ldpp_dout(this, 1) << "ERROR: failed to commit logging object : "
                            << entry << " , error: " << ret << dendl;
        return ret;
      }
      ldpp_dout(this, 20) << "INFO: committed logging object : " << entry
                          << " to bucket " << bucket_name
                          << ", id: "<< bucket_id << dendl;
      return 0;
    }
    // The log bucket has been deleted. Clean up pending objects.
    // This is done because the bucket_deletion_cleanup only removes
    // the current temp log objects.
    bucket_deleted = true;
    ldpp_dout(this, 20) << "INFO: log bucket : " << bucket_name
                        << " no longer exists. Removing pending logging object "
                        << temp_obj_name << " from pool " << obj_pool
                        << dendl;
    ret = rgw_delete_system_obj(this, m_rados_store->svc()->sysobj, obj_pool,
                                temp_obj_name, nullptr, yield);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(this, 1) << "ERROR: failed to delete pending logging object : "
                         << temp_obj_name << " in pool " << obj_pool
                         << " for deleted log bucket : " << bucket_name
                         << " . ret = "<< ret << dendl;
      return ret;
    }
    ldpp_dout(this, 20) << "INFO: Deleted pending logging object "
                        << temp_obj_name << " from pool " << obj_pool
                        << " for deleted log bucket : " << bucket_name
                        << dendl;
    return 0;
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
  int unlock_list_object(librados::IoCtx& rados_ioctx,
                         const std::string& list_obj_name,
                         boost::asio::yield_context yield) {

    librados::ObjectWriteOperation op;
    op.assert_exists();
    rados::cls::lock::unlock(&op, m_lock_name, m_lock_cookie);
    const auto ret = rgw_rados_operate(this, rados_ioctx, list_obj_name,
                                       std::move(op), yield);
    if (ret == -ENOENT) {
      ldpp_dout(this, 20) << "INFO: log commit list: " << list_obj_name
                          << " was removed. Nothing to unlock." << dendl;
      return 0;
    }
    if (ret == -EBUSY) {
      ldpp_dout(this, 20) << "INFO: log commit list: " << list_obj_name
                          << " already owned by another RGW. No need to unlock."
                          << dendl;
      return 0;
    }
    return ret;
  }

  int remove_commit_list(librados::IoCtx& rados_ioctx,
                         const std::string& commit_list_name,
                         optional_yield y) {
    {
      librados::ObjectWriteOperation op;
      rados::cls::lock::assert_locked(&op, m_lock_name,
                                      ClsLockType::EXCLUSIVE,
                                      m_lock_cookie,
                                      "" /*no tag*/);
      op.remove();
      auto ret = rgw_rados_operate(this, rados_ioctx, commit_list_name, std::move(op), y);
      if (ret < 0 && ret != -ENOENT) {
        ldpp_dout(this, 1) << "ERROR: failed to remove bucket logging commit list :"
                           << commit_list_name << ". ret = " << ret << dendl;
        return ret;
      }
    }

    librados::ObjectWriteOperation op;
    op.omap_rm_keys({commit_list_name});
    auto ret = rgw_rados_operate(this, rados_ioctx, COMMIT_LIST_OBJECT_NAME,
                                 std::move(op), y);
    if (ret < 0) {
      ldpp_dout(this, 1) << "ERROR: failed to remove entry " << commit_list_name
                         << " from the global bucket logging list : "
                         << COMMIT_LIST_OBJECT_NAME << ". ret = " << ret << dendl;
      return ret;
    }
    ldpp_dout(this, 20) << "INFO: removed entry " << commit_list_name
                        << " from the global bucket logging list : "
                        << COMMIT_LIST_OBJECT_NAME << dendl;

    return 0;
  }

  // processing of a specific target list
  void process_commit_list(librados::IoCtx& rados_ioctx,
                           const std::string& list_obj_name,
                           boost::asio::yield_context yield) {
    auto is_idle = false;
    auto bucket_deleted = false;
    auto skip_bucket_deletion = false;
    const std::string start_marker;

    std::string tenant_name, bucket_name, bucket_id, prefix;
    int ret = parse_list_name(list_obj_name, tenant_name, bucket_name,
                              bucket_id, prefix);
    if (ret) {
      ldpp_dout(this, 1) << "ERROR: commit list name: " << list_obj_name
                          << " is invalid. Processing will stop." << dendl;
      return;
    }
    ldpp_dout(this, 20) << "INFO: parse commit list name: " << list_obj_name
                        << ", bucket_name: " << bucket_name
                        << ", bucket_id: " << bucket_id
                        << ", tenant_name: " << tenant_name
                        << ", prefix: " << prefix << dendl;

    while (!m_shutdown) {
      // if the list was empty the last time, sleep for idle timeout
      if (is_idle) {
        async_sleep(yield, m_log_commits_idle_sleep);
      }

      // Get the entries in the list after locking it
      is_idle = true;
      skip_bucket_deletion = false;
      std::map<std::string, bufferlist> entries;
      rgw_pool temp_data_pool;
      auto total_entries = 0U;
      {
        librados::ObjectReadOperation op;
        op.assert_exists();
        bufferlist obl;
        int rval;
        bufferlist pool_obl;
        int pool_rval;
        constexpr auto max_chunk = 1024U;
        std::string start_after;
        bool more = true;
        rados::cls::lock::assert_locked(&op, m_lock_name,
                                        ClsLockType::EXCLUSIVE,
                                        m_lock_cookie,
                                        "" /*no tag*/);
        op.omap_get_vals2(start_after, max_chunk, &entries, &more, &rval);
        op.getxattr(TEMP_POOL_ATTR.c_str(), &pool_obl, &pool_rval);
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
          ldpp_dout(this, 10) << "WARNING: commit list : " << list_obj_name
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
        if(pool_rval < 0) {
          ldpp_dout(this, 10) << "WARNING: failed to get pool information"
                              << "from commit list: " << list_obj_name << dendl;
          return;
        }
        // Get temp logging object pool information - this is required for
        // deleting the objects when the log bucket is deleted.
        try {
          auto p = pool_obl.cbegin();
          decode(temp_data_pool, p);
        } catch (buffer::error& err) {
          ldpp_dout(this, 1) << "ERROR:  failed to get pool information from object: "
                             << list_obj_name << dendl;
          return;
        }
      }
      total_entries = entries.size();
      if (total_entries == 0) {
        // nothing in the list object
        // Delete the list object if the bucket has been deleted
        std::unique_ptr<rgw::sal::Bucket> log_bucket;
        rgw_bucket bucket = rgw_bucket{tenant_name, bucket_name, bucket_id};
        int ret = m_rados_store->load_bucket(this, bucket, &log_bucket, yield);
        if (ret == -ENOENT) {
          ldpp_dout(this, 20) << "ERROR: failed to load bucket : "
                              << bucket_name << ", id : " << bucket_id
                              << ", error: " << ret << dendl;
          bucket_deleted = true;
        }
      } else {
      // log when not idle
        ldpp_dout(this, 20) << "INFO: found: " << total_entries
                            << " entries in commit list: " << list_obj_name
                            << dendl;
      }
      auto stop_processing = false;
      std::set<std::string> remove_entries;
      tokens_waiter tw(this);
      for (auto const& [key, val] : entries) {
        if (stop_processing) {
          break;
        }
        std::string temp_obj_name = val.to_str();

        ldpp_dout(this, 20) << "INFO: processing entry: " << key
                            << ", value : " << temp_obj_name << dendl;

        tokens_waiter::token token(&tw);
        boost::asio::spawn(yield, std::allocator_arg, make_stack_allocator(),
          [this, &is_idle, &list_obj_name, &remove_entries, &stop_processing,
           &tenant_name, &bucket_name, &bucket_id, &prefix, token = std::move(token),
           key, temp_obj_name, temp_data_pool, &bucket_deleted , &skip_bucket_deletion]
           (boost::asio::yield_context yield) {
            auto result =
                process_entry(this->get_cct()->_conf, key, temp_obj_name, tenant_name,
                              bucket_name, bucket_id, prefix, temp_data_pool,
                              bucket_deleted, yield);
            if (result == 0) {
              ldpp_dout(this, 20) << "INFO: processing of entry: " << key
                                  << " from: " << list_obj_name
                                  << " was successful." << dendl;
              remove_entries.insert(key);
              is_idle = false;
              return;
            } else {
              is_idle = true;
              // Don't delete the bucket if we couldn't process the entry
              // and the bucket has been deleted
              skip_bucket_deletion = true;
              ldpp_dout(this, 20) << "INFO: failed to process entry: " << key
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

      if (bucket_deleted && !skip_bucket_deletion) {
        ret = remove_commit_list(rados_ioctx, list_obj_name, yield);
        if (ret < 0) {
          ldpp_dout(this, 1) << "ERROR: failed to remove bucket logging commit list :"
                              << list_obj_name << ". ret = " << ret << dendl;
          return;
        }
        ldpp_dout(this, 10) << "INFO: bucket :" << bucket_name
                            << ". was removed. processing will stop" << dendl;
        return;
      }
      // delete all processed entries from list
      if (!remove_entries.empty()) {
        // Delete the entries even if the lock no longer belongs
        // to this instance as we have processed the entries.
        librados::ObjectWriteOperation op;
        op.omap_rm_keys(remove_entries);
        auto ret = rgw_rados_operate(this, rados_ioctx, list_obj_name, std::move(op), yield);
        if (ret == -ENOENT) {
          // list was deleted
          ldpp_dout(this, 10) << "INFO: commit list " << list_obj_name
                              << ". was removed. processing will stop" << dendl;
          return;
        }
        if (ret < 0) {
          ldpp_dout(this, 1) << "ERROR: failed to remove entries from commit list: "
                             << list_obj_name << ". error: " << ret
                             << ". This could cause some log records to be lost."
                             << dendl;
          return;
        } else {
          ldpp_dout(this, 20) << "INFO: removed entries from commit list: "
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
      auto ret = read_global_logging_list(commit_lists, yield);
      if (ret < 0) {
        has_error = true;
        ldpp_dout(this, 1) << "ERROR: failed to read global logging list. Retry. ret:"
                           << ret << dendl;
        continue;
      }

      for (const auto& list_obj_name : commit_lists) {
        // try to lock the object to check if it is owned by this rgw
        // or if ownership needs to be taken
        ldpp_dout(this, 20) << "INFO: processing commit list: " << list_obj_name
                            << dendl;
        librados::ObjectWriteOperation op;
        op.assert_exists();
        rados::cls::lock::lock(&op, m_lock_name, ClsLockType::EXCLUSIVE,
                               m_lock_cookie, "" /*no tag*/,
                               "" /*no description*/, m_failover_time,
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
          ldpp_dout(this, 10) << "ERROR: failed to lock commit list: "
                              << list_obj_name << ". error: " << ret << dendl;
          has_error = true;
          continue;
        }
        // add the commit list to the list of owned commit_logs
        if (owned_commit_logs.insert(list_obj_name).second) {
          ldpp_dout(this, 20) << "INFO: " << list_obj_name << " now owned (locked) by this daemon" << dendl;
          // start processing this list
          boost::asio::spawn(make_strand(m_io_context), std::allocator_arg, make_stack_allocator(),
                             [this, &commit_list_gc, &commit_list_gc_lock, &rados_ioctx,
                             list_obj_name, &processed_list_count](boost::asio::yield_context yield) {
            ++processed_list_count;
            process_commit_list(rados_ioctx, list_obj_name, yield);
            // if list processing ended, it means that the list object was removed or not owned anymore
            const auto ret = unlock_list_object(rados_ioctx, list_obj_name, yield);
            if (ret < 0) {
              ldpp_dout(this, 15) << "WARNING: failed to unlock commit list: "
                                  << list_obj_name << " with error: " << ret
                                  << " (ownership would still move if not renewed)"
                                  << dendl;
            } else {
              ldpp_dout(this, 20) << "INFO: commit list: " << list_obj_name
                                  << " not locked (ownership can move)" << dendl;
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
