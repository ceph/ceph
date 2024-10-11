// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_notify.h"
#include "cls/2pc_queue/cls_2pc_queue_client.h"
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
#include "rgw_pubsub.h"
#include "rgw_pubsub_push.h"
#include "rgw_zone_features.h"
#include "rgw_perf_counters.h"
#include "services/svc_zone.h"
#include "common/dout.h"
#include "rgw_url.h"
#include <chrono>

#define dout_subsys ceph_subsys_rgw_notification

namespace rgw::notify {

static inline std::ostream& operator<<(std::ostream& out,
                                       const event_entry_t& e) {
  std::string host;
  std::string user;
  std::string password;
  parse_url_authority(e.push_endpoint, host, user, password);
  return out << "notification id: '" << e.event.configurationId
             << "', topic: '" << e.arn_topic
             << "', endpoint: '" << host
             << "', endpoint_user: '" << user
             << "', bucket_owner: '" << e.event.bucket_ownerIdentity
             << "', bucket: '" << e.event.bucket_name
             << "', object: '" << e.event.object_key
             << "', event type: '" << e.event.eventName << "'";
}

struct persistency_tracker {
  ceph::coarse_real_time last_retry_time {ceph::coarse_real_clock::zero()};
  uint32_t retires_num {0};
};

using queues_t = std::set<std::string>;
using entries_persistency_tracker = ceph::unordered_map<std::string, persistency_tracker>;
using queues_persistency_tracker = ceph::unordered_map<std::string, entries_persistency_tracker>;
using rgw::persistent_topic_counters::CountersManager;

// use mmap/mprotect to allocate 128k coroutine stacks
auto make_stack_allocator() {
  return boost::context::protected_fixedsize_stack{128*1024};
}

const std::string Q_LIST_OBJECT_NAME = "queues_list_object";

struct PublishCommitCompleteArg {
    PublishCommitCompleteArg(const std::string& _queue_name, CephContext* _cct)
            : queue_name{_queue_name}, cct{_cct} {}

    const std::string queue_name;
    CephContext* const cct;
};

void publish_commit_completion(rados_completion_t completion, void* arg) {
  std::unique_ptr<PublishCommitCompleteArg> pcc_args{reinterpret_cast<PublishCommitCompleteArg*>(arg)};
  if (const auto rc = rados_aio_get_return_value(completion); rc < 0) {
    ldout(pcc_args->cct, 1) << "ERROR: failed to commit reservation to queue: "
      << pcc_args->queue_name << ". error: " << rc << dendl;
  }
};

class Manager : public DoutPrefixProvider {
  bool shutdown = false;
  const uint32_t queues_update_period_ms;
  const uint32_t queues_update_retry_ms;
  const uint32_t queue_idle_sleep_us;
  const utime_t failover_time;
  CephContext* const cct;
  static constexpr auto COOKIE_LEN = 16;
  const std::string lock_cookie;
  boost::asio::io_context io_context;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard;
  const uint32_t worker_count;
  std::vector<std::thread> workers;
  const uint32_t stale_reservations_period_s;
  const uint32_t reservations_cleanup_period_s;
  queues_persistency_tracker topics_persistency_tracker;
  const SiteConfig& site;
public:
  rgw::sal::RadosStore& rados_store;

private:

  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "rgw notify: "; }

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
      const auto ret = rgw_rados_operate(this, rados_store.getRados()->get_notif_pool_ctx(), Q_LIST_OBJECT_NAME, &op, nullptr, y);
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

  using Clock = ceph::coarse_mono_clock;
  using Executor = boost::asio::io_context::executor_type;
  using Timer = boost::asio::basic_waitable_timer<Clock,
        boost::asio::wait_traits<Clock>, Executor>;

  class tokens_waiter {
    const std::chrono::hours infinite_duration;
    size_t pending_tokens;
    Timer timer;
 
    struct token {
      tokens_waiter& waiter;
      token(const token& other) : waiter(other.waiter) {
        ++waiter.pending_tokens;
      }
      token(tokens_waiter& _waiter) : waiter(_waiter) {
        ++waiter.pending_tokens;
      }
      
      ~token() {
        --waiter.pending_tokens;
        if (waiter.pending_tokens == 0) {
          waiter.timer.cancel();
        }   
      }   
    };
  
  public:

    tokens_waiter(boost::asio::io_context& io_context) :
      infinite_duration(1000),
      pending_tokens(0),
      timer(io_context) {}  
 
    void async_wait(boost::asio::yield_context yield) {
      if (pending_tokens == 0) {
        return;
      }
      timer.expires_from_now(infinite_duration);
      boost::system::error_code ec; 
      timer.async_wait(yield[ec]);
      ceph_assert(ec == boost::system::errc::operation_canceled);
    }   
 
    token make_token() {    
      return token(*this);
    }   
  };

  enum class EntryProcessingResult {
    Failure, Successful, Sleeping, Expired, Migrating
  };
  std::vector<std::string> entryProcessingResultString = {"Failure", "Successful", "Sleeping", "Expired", "Migrating"};

  // processing of a specific entry
  // return whether processing was successful (true) or not (false)
  EntryProcessingResult process_entry(
      const ConfigProxy& conf,
      persistency_tracker& entry_persistency_tracker,
      const cls_queue_entry& entry,
      RGWPubSubEndpoint* const push_endpoint,
      const rgw_pubsub_topic& topic,
      boost::asio::yield_context yield) {
    event_entry_t event_entry;
    auto iter = entry.data.cbegin();
    try {
      decode(event_entry, iter);
    } catch (buffer::error& err) {
      ldpp_dout(this, 5) << "WARNING: failed to decode entry. error: " << err.what() << dendl;
      return EntryProcessingResult::Failure;
    }

    if (event_entry.creation_time == ceph::coarse_real_clock::zero()) {
      return EntryProcessingResult::Migrating;
    }
    // overwrite the event entry values from the topics object fetched.
    event_entry.event.opaque_data = topic.opaque_data;
    event_entry.arn_topic = topic.dest.arn_topic;
    event_entry.time_to_live = topic.dest.time_to_live;
    event_entry.max_retries = topic.dest.max_retries;
    event_entry.retry_sleep_duration = topic.dest.retry_sleep_duration;
    const auto topic_persistency_ttl = event_entry.time_to_live != DEFAULT_GLOBAL_VALUE ?
        event_entry.time_to_live : conf->rgw_topic_persistency_time_to_live;
    const auto topic_persistency_max_retries = event_entry.max_retries != DEFAULT_GLOBAL_VALUE ?
        event_entry.max_retries : conf->rgw_topic_persistency_max_retries;
    const auto topic_persistency_sleep_duration = event_entry.retry_sleep_duration != DEFAULT_GLOBAL_VALUE ?
        event_entry.retry_sleep_duration : conf->rgw_topic_persistency_sleep_duration;
    const auto time_now = ceph::coarse_real_clock::now();
    if ( (topic_persistency_ttl != 0 && event_entry.creation_time != ceph::coarse_real_clock::zero() &&
         time_now - event_entry.creation_time > std::chrono::seconds(topic_persistency_ttl))
         || ( topic_persistency_max_retries != 0 && entry_persistency_tracker.retires_num >  topic_persistency_max_retries) ) {
      ldpp_dout(this, 1) << "WARNING: Expiring entry marker: " << entry.marker
                         << " for event with " << event_entry
                         << " entry retry_number: "
                         << entry_persistency_tracker.retires_num
                         << " creation_time: " << event_entry.creation_time
                         << " time_now: " << time_now << dendl;
      return EntryProcessingResult::Expired;
    }
    if (time_now - entry_persistency_tracker.last_retry_time < std::chrono::seconds(topic_persistency_sleep_duration) ) {
      return EntryProcessingResult::Sleeping;
    }

    ++entry_persistency_tracker.retires_num;
    entry_persistency_tracker.last_retry_time = time_now;
    ldpp_dout(this, 20) << "Processing event entry with " << event_entry
                        << " retry_number: "
                        << entry_persistency_tracker.retires_num
                        << " current time: " << time_now << dendl;
    const auto ret = push_endpoint->send(this, event_entry.event, yield);
    if (ret < 0) {
      ldpp_dout(this, 5) << "WARNING: push entry marker: " << entry.marker
                         << " failed. error: " << ret
                         << " (will retry) for event with " << event_entry
                         << dendl;
      return EntryProcessingResult::Failure;
    }
    ldpp_dout(this, 5) << "INFO: push entry marker: " << entry.marker
                       << " ok for event with " << event_entry << dendl;
    if (perfcounter)
      perfcounter->inc(l_rgw_pubsub_push_ok);
    return EntryProcessingResult::Successful;
  }

  // clean stale reservation from queue
  void cleanup_queue(const std::string& queue_name, boost::asio::yield_context yield) {
    while (!shutdown) {
      ldpp_dout(this, 20) << "INFO: trying to perform stale reservation cleanup for queue: " << queue_name << dendl;
      const auto now = ceph::coarse_real_time::clock::now();
      const auto stale_time = now - std::chrono::seconds(stale_reservations_period_s);
      librados::ObjectWriteOperation op;
      op.assert_exists();
      rados::cls::lock::assert_locked(&op, queue_name+"_lock", 
        ClsLockType::EXCLUSIVE,
        lock_cookie, 
        "" /*no tag*/);
      cls_2pc_queue_expire_reservations(op, stale_time);
      // check ownership and do reservation cleanup in one batch
      auto ret = rgw_rados_operate(this, rados_store.getRados()->get_notif_pool_ctx(), queue_name, &op, yield);
      if (ret == -ENOENT) {
        // queue was deleted
        ldpp_dout(this, 10) << "INFO: queue: " << queue_name
                            << ". was removed. cleanup will stop" << dendl;
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
        ldpp_dout(this, 5) << "WARNING: failed to cleanup stale reservation from queue and/or lock queue: " << queue_name
          << ". error: " << ret << dendl;
      }
      Timer timer(io_context);
      timer.expires_from_now(std::chrono::seconds(reservations_cleanup_period_s));
      boost::system::error_code ec;
	    timer.async_wait(yield[ec]);
    }
    ldpp_dout(this, 5) << "INFO: manager stopped. done cleanup for queue: " << queue_name << dendl;
  }

  // unlock (lose ownership) queue
  int unlock_queue(const std::string& queue_name, boost::asio::yield_context yield) {
    librados::ObjectWriteOperation op;
    op.assert_exists();
    rados::cls::lock::unlock(&op, queue_name+"_lock", lock_cookie);
    auto& rados_ioctx = rados_store.getRados()->get_notif_pool_ctx();
    const auto ret = rgw_rados_operate(this, rados_ioctx, queue_name, &op, yield);
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

  int get_topic_info(const std::string& queue_name,
                     const cls_queue_entry& queue_entry,
                     rgw_pubsub_topic& topic,
                     boost::asio::yield_context yield) {
    std::string queue_topic_tenant;
    std::string queue_topic_name;
    parse_topic_metadata_key(queue_name, queue_topic_tenant, queue_topic_name);
    rgw_pubsub_topic topic_info;
    RGWPubSub ps(&rados_store, queue_topic_tenant, site);
    int ret = ps.get_topic(this, queue_topic_name, topic_info, yield, nullptr);
    if (ret < 0) {
      ldpp_dout(this, 1) << "WARNING: failed to fetch topic: "
                         << queue_topic_name << " error: " << ret
                         << ". using cached topic attributes!" << dendl;
      event_entry_t event_entry;
      auto iter = queue_entry.data.cbegin();
      try {
        decode(event_entry, iter);
      } catch (buffer::error& err) {
        ldpp_dout(this, 1) << "ERROR: failed to decode entry. error: "
                           << err.what() << dendl;
        return -EIO;
      }
      topic_info.dest.push_endpoint = event_entry.push_endpoint;
      topic_info.dest.push_endpoint_args = event_entry.push_endpoint_args;
      topic_info.dest.arn_topic = event_entry.arn_topic;
      topic_info.dest.arn_topic = event_entry.arn_topic;
      topic_info.dest.time_to_live = event_entry.time_to_live;
      topic_info.dest.max_retries = event_entry.max_retries;
      topic_info.dest.retry_sleep_duration = event_entry.retry_sleep_duration;
      topic_info.opaque_data = event_entry.event.opaque_data;
    }
    topic = std::move(topic_info);
    return 0;
  }

  // processing of a specific queue
  void process_queue(const std::string& queue_name, boost::asio::yield_context yield) {
    constexpr auto max_elements = 1024;
    auto is_idle = false;
    const std::string start_marker;

    // start a the cleanup coroutine for the queue
    boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
            [this, queue_name](boost::asio::yield_context yield) {
              cleanup_queue(queue_name, yield);
            }, [] (std::exception_ptr eptr) {
              if (eptr) std::rethrow_exception(eptr);
            });

    CountersManager queue_counters_container(queue_name, this->get_cct());

    while (!shutdown) {
      // if queue was empty the last time, sleep for idle timeout
      if (is_idle) {
        Timer timer(io_context);
        timer.expires_from_now(std::chrono::microseconds(queue_idle_sleep_us));
        boost::system::error_code ec;
	      timer.async_wait(yield[ec]);
      }

      // get list of entries in the queue
      auto& rados_ioctx = rados_store.getRados()->get_notif_pool_ctx();
      is_idle = true;
      bool truncated = false;
      std::string end_marker;
      std::vector<cls_queue_entry> entries;
      auto total_entries = 0U;
      {
        librados::ObjectReadOperation op;
        op.assert_exists();
        bufferlist obl;
        int rval;
        rados::cls::lock::assert_locked(&op, queue_name+"_lock", 
          ClsLockType::EXCLUSIVE,
          lock_cookie, 
          "" /*no tag*/);
        cls_2pc_queue_list_entries(op, start_marker, max_elements, &obl, &rval);
        // check ownership and list entries in one batch
        auto ret = rgw_rados_operate(this, rados_ioctx, queue_name, &op, nullptr, yield);
        if (ret == -ENOENT) {
          // queue was deleted
          topics_persistency_tracker.erase(queue_name);
          ldpp_dout(this, 10) << "INFO: queue: " << queue_name
                              << ". was removed. processing will stop" << dendl;
          return;
        }
        if (ret == -EBUSY) {
          topics_persistency_tracker.erase(queue_name);
          ldpp_dout(this, 10)
              << "WARNING: queue: " << queue_name
              << " ownership moved to another daemon. processing will stop"
              << dendl;
          return;
        }
        if (ret < 0) {
          ldpp_dout(this, 5) << "WARNING: failed to get list of entries in queue and/or lock queue: " 
            << queue_name << ". error: " << ret << " (will retry)" << dendl;
          continue;
        }
        ret = cls_2pc_queue_list_entries_result(obl, entries, &truncated, end_marker);
        if (ret < 0) {
          ldpp_dout(this, 5) << "WARNING: failed to parse list of entries in queue: " 
            << queue_name << ". error: " << ret << " (will retry)" << dendl;
          continue;
        }
      }
      total_entries = entries.size();
      if (total_entries == 0) {
        // nothing in the queue
        continue;
      }
      // log when queue is not idle
      ldpp_dout(this, 20) << "INFO: found: " << total_entries << " entries in: " << queue_name <<
        ". end marker is: " << end_marker << dendl;
      rgw_pubsub_topic topic_info;
      if (get_topic_info(queue_name, entries.front(), topic_info, yield) < 0) {
        continue;
      }
      RGWPubSubEndpoint::Ptr push_endpoint;
      try {
        push_endpoint = RGWPubSubEndpoint::create(
            topic_info.dest.push_endpoint, topic_info.dest.arn_topic,
            RGWHTTPArgs(topic_info.dest.push_endpoint_args, this), cct);
        ldpp_dout(this, 20)
            << "INFO: push endpoint created: " << topic_info.dest.push_endpoint
            << dendl;
      } catch (const RGWPubSubEndpoint::configuration_error& e) {
        ldpp_dout(this, 5) << "WARNING: failed to create push endpoint: "
                           << topic_info.dest.push_endpoint
                           << ". error: " << e.what()
                           << " (will retry sending events) " << dendl;
        continue;
      }
      is_idle = false;
      auto has_error = false;
      auto remove_entries = false;
      auto entry_idx = 1U;
      tokens_waiter waiter(io_context);
      std::vector<bool> needs_migration_vector(entries.size(), false);
      for (auto& entry : entries) {
        if (has_error) {
          // bail out on first error
          break;
        }

        entries_persistency_tracker& notifs_persistency_tracker = topics_persistency_tracker[queue_name];
        boost::asio::spawn(yield, std::allocator_arg, make_stack_allocator(),
          [this, &notifs_persistency_tracker, &queue_name, entry_idx,
           total_entries, &end_marker, &remove_entries, &has_error,
           token = waiter.make_token(), &entry, &needs_migration_vector,
           push_endpoint = push_endpoint.get(),
           &topic_info](boost::asio::yield_context yield) {
            auto& persistency_tracker = notifs_persistency_tracker[entry.marker];
            auto result =
                process_entry(this->get_cct()->_conf, persistency_tracker,
                              entry, push_endpoint, topic_info, yield);
            if (result == EntryProcessingResult::Successful || result == EntryProcessingResult::Expired
                || result == EntryProcessingResult::Migrating) {
              ldpp_dout(this, 20) << "INFO: processing of entry: " << entry.marker
                << " (" << entry_idx << "/" << total_entries << ") from: " << queue_name << " "
                << entryProcessingResultString[static_cast<unsigned int>(result)] << dendl;
              remove_entries = true;
              needs_migration_vector[entry_idx - 1] = (result == EntryProcessingResult::Migrating);
              notifs_persistency_tracker.erase(entry.marker);
            }  else {
              if (set_min_marker(end_marker, entry.marker) < 0) {
                ldpp_dout(this, 1) << "ERROR: cannot determine minimum between malformed markers: " << end_marker << ", " << entry.marker << dendl;
              } else {
                ldpp_dout(this, 20) << "INFO: new end marker for removal: " << end_marker << " from: " << queue_name << dendl;
              }
              has_error = (result == EntryProcessingResult::Failure);
              ldpp_dout(this, 20) << "INFO: processing of entry: " << 
                entry.marker << " (" << entry_idx << "/" << total_entries << ") from: " << queue_name << " failed" << dendl;
            } 
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });
        ++entry_idx;
      }

      // wait for all pending work to finish
      waiter.async_wait(yield);

      // delete all published entries from queue
      if (remove_entries) {
        std::vector<cls_queue_entry> entries_to_migrate;
        uint64_t index = 0;

        for (const auto& entry: entries) {
          if (end_marker == entry.marker) {
            break;
          }
          if (needs_migration_vector[index]) {
            ldpp_dout(this, 20) << "INFO: migrating entry " << entry.marker << " from: " << queue_name  << dendl;
            entries_to_migrate.push_back(entry);
          }
          index++;
        }

        uint64_t entries_to_remove = index;
        librados::ObjectWriteOperation op;
        op.assert_exists();
        rados::cls::lock::assert_locked(&op, queue_name+"_lock", 
          ClsLockType::EXCLUSIVE,
          lock_cookie, 
          "" /*no tag*/);
        cls_2pc_queue_remove_entries(op, end_marker, entries_to_remove);
        // check ownership and deleted entries in one batch
        auto ret = rgw_rados_operate(this, rados_ioctx, queue_name, &op, yield);
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
          ldpp_dout(this, 1) << "ERROR: failed to remove entries and/or lock queue up to: " << end_marker <<  " from queue: " 
            << queue_name << ". error: " << ret << dendl;
          return;
        } else {
          ldpp_dout(this, 20) << "INFO: removed entries up to: " << end_marker <<  " from queue: " << queue_name << dendl;
        }

        // reserving and committing the migrating entries
        if (!entries_to_migrate.empty()) {
          std::vector<bufferlist> migration_vector;
          std::string tenant_name;
          // TODO: extract tenant name from queue_name once it is fixed
          uint64_t size_to_migrate = 0;
          RGWPubSub ps(&rados_store, tenant_name, site);

          rgw_pubsub_topic topic;
          auto ret_of_get_topic = ps.get_topic(this, queue_name, topic,
                                               yield, nullptr);
          if (ret_of_get_topic < 0) {
            // we can't migrate entries without topic info
            ldpp_dout(this, 1) << "ERROR: failed to fetch topic: " << queue_name << " error: "
              << ret_of_get_topic << ". Aborting migration!" << dendl;
            return;
          }

          for (auto entry: entries_to_migrate) {
            event_entry_t event_entry;
            auto iter = entry.data.cbegin();
            try {
              decode(event_entry, iter);
            } catch (buffer::error& err) {
              ldpp_dout(this, 5) << "WARNING: failed to decode entry. error: " << err.what() << dendl;
              continue;
            }
            size_to_migrate += entry.data.length();
            event_entry.creation_time = ceph::coarse_real_clock::now();
            event_entry.time_to_live = topic.dest.time_to_live;
            event_entry.max_retries = topic.dest.max_retries;
            event_entry.retry_sleep_duration = topic.dest.retry_sleep_duration;

            bufferlist bl;
            encode(event_entry, bl);
            migration_vector.push_back(bl);
          }

          cls_2pc_reservation::id_t reservation_id;
          buffer::list obl;
          int rval;
          cls_2pc_queue_reserve(op, size_to_migrate, migration_vector.size(), &obl, &rval);
          ret = rgw_rados_operate(this, rados_ioctx, queue_name, &op, yield, librados::OPERATION_RETURNVEC);
          if (ret < 0) {
            ldpp_dout(this, 1) << "ERROR: failed to reserve migration space on queue: " << queue_name << ". error: " << ret << dendl;
            return;
          }
          ret = cls_2pc_queue_reserve_result(obl, reservation_id);
          if (ret < 0) {
            ldpp_dout(this, 1) << "ERROR: failed to parse reservation id for migration. error: " << ret << dendl;
            return;
          }

          cls_2pc_queue_commit(op, migration_vector, reservation_id);
          ret = rgw_rados_operate(this, rados_ioctx, queue_name, &op, yield);
          reservation_id = cls_2pc_reservation::NO_ID;
          if (ret < 0) {
            ldpp_dout(this, 1) << "ERROR: failed to commit reservation to queue: " << queue_name << ". error: " << ret << dendl;
          }
        }
      }

      // updating perfcounters with topic stats
      uint64_t entries_size;
      uint32_t entries_number;
      const auto ret = cls_2pc_queue_get_topic_stats(rados_ioctx, queue_name, entries_number, entries_size);
      if (ret < 0) {
        ldpp_dout(this, 1) << "ERROR: topic stats for topic: " << queue_name << ". error: " << ret << dendl;
      } else {
        queue_counters_container.set(l_rgw_persistent_topic_len, entries_number);
        queue_counters_container.set(l_rgw_persistent_topic_size, entries_size);
      }
    }
    ldpp_dout(this, 5) << "INFO: manager stopped. done processing for queue: " << queue_name << dendl;
  }

  // lits of owned queues
  using owned_queues_t = std::unordered_set<std::string>;

  // process all queues
  // find which of the queues is owned by this daemon and process it
  void process_queues(boost::asio::yield_context yield) {
    auto has_error = false;
    owned_queues_t owned_queues;
    size_t processed_queue_count = 0;

    // add randomness to the duration between queue checking
    // to make sure that different daemons are not synced
    std::random_device seed;
    std::mt19937 rnd_gen(seed());
    const auto min_jitter = 100; // ms
    const auto max_jitter = 500; // ms
    std::uniform_int_distribution<> duration_jitter(min_jitter, max_jitter);

    std::vector<std::string> queue_gc;
    std::mutex queue_gc_lock;
    auto& rados_ioctx = rados_store.getRados()->get_notif_pool_ctx();
    while (!shutdown) {
      Timer timer(io_context);
      const auto duration = (has_error ? 
        std::chrono::milliseconds(queues_update_retry_ms) : std::chrono::milliseconds(queues_update_period_ms)) + 
        std::chrono::milliseconds(duration_jitter(rnd_gen));
      timer.expires_from_now(duration);
      const auto tp = ceph::coarse_real_time::clock::to_time_t(ceph::coarse_real_time::clock::now() + duration);
      ldpp_dout(this, 20) << "INFO: next queues processing will happen at: " << std::ctime(&tp)  << dendl;
      boost::system::error_code ec;
      timer.async_wait(yield[ec]);

      queues_t queues;
      auto ret = read_queue_list(queues, yield);
      if (ret < 0) {
        has_error = true;
        continue;
      }

      for (const auto& queue_name : queues) {
        // try to lock the queue to check if it is owned by this rgw
        // or if ownership needs to be taken
        librados::ObjectWriteOperation op;
        op.assert_exists();
        rados::cls::lock::lock(&op, queue_name+"_lock", 
              ClsLockType::EXCLUSIVE,
              lock_cookie, 
              "" /*no tag*/,
              "" /*no description*/,
              failover_time,
              LOCK_FLAG_MAY_RENEW);

        ret = rgw_rados_operate(this, rados_ioctx, queue_name, &op, yield);
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
          }, [] (std::exception_ptr eptr) {
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
          topics_persistency_tracker.erase(queue_name);
          owned_queues.erase(queue_name);
          ldpp_dout(this, 10) << "INFO: queue: " << queue_name << " was removed" << dendl;
        });
        queue_gc.clear();
      }
    }
    Timer timer(io_context);
    while (processed_queue_count > 0) {
      ldpp_dout(this, 5) << "INFO: manager stopped. " << processed_queue_count << " queues are still being processed" << dendl;
      timer.expires_from_now(std::chrono::milliseconds(queues_update_retry_ms));
      boost::system::error_code ec;
      timer.async_wait(yield[ec]);
    }
    ldpp_dout(this, 5) << "INFO: manager stopped. done processing all queues" << dendl;
  }

public:

  ~Manager() {
  }

  void stop() {
    shutdown = true;
    work_guard.reset();
    std::for_each(workers.begin(), workers.end(), [] (auto& worker) { worker.join(); });
  }

  void init() {
    boost::asio::spawn(make_strand(io_context), std::allocator_arg, make_stack_allocator(),
        [this](boost::asio::yield_context yield) {
          process_queues(yield);
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });

    // start the worker threads to do the actual queue processing
    const std::string WORKER_THREAD_NAME = "notif-worker";
    for (auto worker_id = 0U; worker_id < worker_count; ++worker_id) {
      workers.emplace_back([this]() {
        try {
          io_context.run(); 
        } catch (const std::exception& err) {
          ldpp_dout(this, 1) << "ERROR: notification worker failed with error: " << err.what() << dendl;
          throw err;
        }
      });
      const auto thread_name = WORKER_THREAD_NAME+std::to_string(worker_id);
      if (const auto rc = ceph_pthread_setname(workers.back().native_handle(), thread_name.c_str()); rc != 0) {
        ldpp_dout(this, 1) << "ERROR: failed to set notification manager thread name to: " << thread_name
          << ". error: " << rc << dendl;
      }
    }
    ldpp_dout(this, 10) << "INfO: started notification manager with: " << worker_count << " workers" << dendl;
  }

  // ctor: start all threads
  Manager(CephContext* _cct, uint32_t _queues_update_period_ms,
          uint32_t _queues_update_retry_ms, uint32_t _queue_idle_sleep_us, u_int32_t failover_time_ms, 
          uint32_t _stale_reservations_period_s, uint32_t _reservations_cleanup_period_s,
          uint32_t _worker_count, rgw::sal::RadosStore* store,
          const SiteConfig& site) :
    queues_update_period_ms(_queues_update_period_ms),
    queues_update_retry_ms(_queues_update_retry_ms),
    queue_idle_sleep_us(_queue_idle_sleep_us),
    failover_time(std::chrono::milliseconds(failover_time_ms)),
    cct(_cct),
    lock_cookie(gen_rand_alphanumeric(cct, COOKIE_LEN)),
    work_guard(boost::asio::make_work_guard(io_context)),
    worker_count(_worker_count),
    stale_reservations_period_s(_stale_reservations_period_s),
    reservations_cleanup_period_s(_reservations_cleanup_period_s),
    site(site),
    rados_store(*store)
    {}
};

std::unique_ptr<Manager> s_manager;

constexpr size_t MAX_QUEUE_SIZE = 128*1000*1000; // 128MB
constexpr uint32_t Q_LIST_UPDATE_MSEC = 1000*30;     // check queue list every 30seconds
constexpr uint32_t Q_LIST_RETRY_MSEC = 1000;         // retry every second if queue list update failed
constexpr uint32_t IDLE_TIMEOUT_USEC = 100*1000;     // idle sleep 100ms
constexpr uint32_t FAILOVER_TIME_MSEC = 3*Q_LIST_UPDATE_MSEC; // FAILOVER TIME 3x renew time
constexpr uint32_t WORKER_COUNT = 1;                 // 1 worker thread
constexpr uint32_t STALE_RESERVATIONS_PERIOD_S = 120;   // cleanup reservations that are more than 2 minutes old
constexpr uint32_t RESERVATIONS_CLEANUP_PERIOD_S = 30; // reservation cleanup every 30 seconds

bool init(const DoutPrefixProvider* dpp, rgw::sal::RadosStore* store,
          const SiteConfig& site) {
  if (s_manager) {
    ldpp_dout(dpp, 1) << "ERROR: failed to init notification manager: already exists" << dendl;
    return false;
  }
  if (!RGWPubSubEndpoint::init_all(dpp->get_cct())) {
    return false;
  }
  // TODO: take conf from CephContext
  s_manager = std::make_unique<Manager>(dpp->get_cct(),
      Q_LIST_UPDATE_MSEC, Q_LIST_RETRY_MSEC, 
      IDLE_TIMEOUT_USEC, FAILOVER_TIME_MSEC, 
      STALE_RESERVATIONS_PERIOD_S, RESERVATIONS_CLEANUP_PERIOD_S,
      WORKER_COUNT,
      store, site);
  s_manager->init();
  return true;
}

void shutdown() {
  if (!s_manager) return;
  RGWPubSubEndpoint::shutdown_all();
  s_manager->stop();
  s_manager.reset();
}

int add_persistent_topic(const DoutPrefixProvider* dpp, librados::IoCtx& rados_ioctx,
                         const std::string& topic_queue, optional_yield y)
{
  if (topic_queue == Q_LIST_OBJECT_NAME) {
    ldpp_dout(dpp, 1) << "ERROR: topic name cannot be: " << Q_LIST_OBJECT_NAME << " (conflict with queue list object name)" << dendl;
    return -EINVAL;
  }
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, topic_queue, MAX_QUEUE_SIZE);
  auto ret = rgw_rados_operate(dpp, rados_ioctx, topic_queue, &op, y);
  if (ret == -EEXIST) {
    // queue already exists - nothing to do
    ldpp_dout(dpp, 20) << "INFO: queue for topic: " << topic_queue << " already exists. nothing to do" << dendl;
    return 0;
  }
  if (ret < 0) {
    // failed to create queue
    ldpp_dout(dpp, 1) << "ERROR: failed to create queue for topic: " << topic_queue << ". error: " << ret << dendl;
    return ret;
  }

  bufferlist empty_bl;
  std::map<std::string, bufferlist> new_topic{{topic_queue, empty_bl}};
  op.omap_set(new_topic);
  ret = rgw_rados_operate(dpp, rados_ioctx, Q_LIST_OBJECT_NAME, &op, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to add queue: " << topic_queue << " to queue list. error: " << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "INFO: queue: " << topic_queue << " added to queue list"  << dendl;
  return 0;
}

int remove_persistent_topic(const DoutPrefixProvider* dpp, librados::IoCtx& rados_ioctx, const std::string& topic_queue, optional_yield y) {
  librados::ObjectWriteOperation op;
  op.remove();
  auto ret = rgw_rados_operate(dpp, rados_ioctx, topic_queue, &op, y);
  if (ret == -ENOENT) {
    // queue already removed - nothing to do
    ldpp_dout(dpp, 20) << "INFO: queue for topic: " << topic_queue << " already removed. nothing to do" << dendl;
    return 0;
  }
  if (ret < 0) {
    // failed to remove queue
    ldpp_dout(dpp, 1) << "ERROR: failed to remove queue for topic: " << topic_queue << ". error: " << ret << dendl;
    return ret;
  }

  std::set<std::string> topic_to_remove{{topic_queue}};
  op.omap_rm_keys(topic_to_remove);
  ret = rgw_rados_operate(dpp, rados_ioctx, Q_LIST_OBJECT_NAME, &op, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove queue: " << topic_queue << " from queue list. error: " << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "INFO: queue: " << topic_queue << " removed from queue list"  << dendl;
  return 0;
}

rgw::sal::Object* get_object_with_attributes(
  const reservation_t& res, rgw::sal::Object* obj) {
  // in case of copy obj, the tags and metadata are taken from source
  const auto src_obj = res.src_object ? res.src_object : obj;
  if (src_obj->get_attrs().empty()) {
    if (!src_obj->get_bucket()) {
      src_obj->set_bucket(res.bucket);
    }
    const auto ret = src_obj->get_obj_attrs(res.yield, res.dpp);
    if (ret < 0) {
      ldpp_dout(res.dpp, 20) << "failed to get attributes from object: " << 
        src_obj->get_key() << ". ret = " << ret << dendl;
      return nullptr;
    }
  }
  return src_obj;
}

static inline void filter_amz_meta(meta_map_t& dest, const meta_map_t& src) {
  std::copy_if(src.cbegin(), src.cend(),
               std::inserter(dest, dest.end()),
               [](const auto& m) {
                 return (boost::algorithm::starts_with(m.first, RGW_AMZ_META_PREFIX));
               });
}


static inline void metadata_from_attributes(
  reservation_t& res, rgw::sal::Object* obj) {
  auto& metadata = res.x_meta_map;
  const auto src_obj = get_object_with_attributes(res, obj);
  if (!src_obj) {
    return;
  }
  res.metadata_fetched_from_attributes = true;
  for (auto& attr : src_obj->get_attrs()) {
    if (boost::algorithm::starts_with(attr.first, RGW_ATTR_META_PREFIX)) {
      std::string_view key(attr.first);
      key.remove_prefix(sizeof(RGW_ATTR_PREFIX)-1);
      // we want to pass a null terminated version
      // of the bufferlist, hence "to_str().c_str()"
      metadata.emplace(key, attr.second.to_str().c_str());
    }
  }
}

static inline void tags_from_attributes(
  const reservation_t& res, rgw::sal::Object* obj, KeyMultiValueMap& tags) {
  const auto src_obj = get_object_with_attributes(res, obj);
  if (!src_obj) {
    return;
  }
  const auto& attrs = src_obj->get_attrs();
  const auto attr_iter = attrs.find(RGW_ATTR_TAGS);
  if (attr_iter != attrs.end()) {
    auto bliter = attr_iter->second.cbegin();
    RGWObjTags obj_tags;
    try {
      ::decode(obj_tags, bliter);
    } catch(buffer::error&) {
      // not able to decode tags
      return;
    }
    tags = std::move(obj_tags.get_tags());
  }
}

// populate event from request
static inline void populate_event(reservation_t& res,
        rgw::sal::Object* obj,
        uint64_t size,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        const std::string& version, 
        EventType event_type,
        rgw_pubsub_s3_event& event) {
  event.eventTime = mtime;
  event.eventName = to_event_string(event_type);
  event.userIdentity = res.user_id;    // user that triggered the change
  event.x_amz_request_id = res.req_id; // request ID of the original change
  event.x_amz_id_2 = res.store->getRados()->host_id; // RGW on which the change was made
  // configurationId is filled from notification configuration
  event.bucket_name = res.bucket->get_name();
  event.bucket_ownerIdentity = to_string(res.bucket->get_owner());
  const auto region = res.store->get_zone()->get_zonegroup().get_api_name();
  rgw::ARN bucket_arn(res.bucket->get_key());
  bucket_arn.region = region; 
  event.bucket_arn = to_string(bucket_arn);
  event.object_key = res.object_name ? *res.object_name : obj->get_name();
  event.object_size = size;
  event.object_etag = etag;
  event.object_versionId = version;
  event.awsRegion = region;
  // use timestamp as per key sequence id (hex encoded)
  const utime_t ts(real_clock::now());
  boost::algorithm::hex((const char*)&ts, (const char*)&ts + sizeof(utime_t), 
          std::back_inserter(event.object_sequencer));
  set_event_id(event.id, etag, ts);
  event.bucket_id = res.bucket->get_bucket_id();
  // pass meta data
  if (!res.metadata_fetched_from_attributes) {
    // either no metadata exist or no metadata filter was used
    metadata_from_attributes(res, obj);
  }
  event.x_meta_map = res.x_meta_map;
  // pass tags
  if (!res.tagset ||
      (*res.tagset).get_tags().empty()) {
    // try to fetch the tags from the attributes
    tags_from_attributes(res, obj, event.tags);
  } else {
    event.tags = (*res.tagset).get_tags();
  }
  // opaque data will be filled from topic configuration
}

static inline bool notification_match(reservation_t& res,
				      const rgw_pubsub_topic_filter& filter,
				      EventType event,
				      const RGWObjTags* req_tags) {
  if (!match(filter.events, event)) { 
    return false;
  }
  const auto obj = res.object;
  if (!match(filter.s3_filter.key_filter, 
        res.object_name ? *res.object_name : obj->get_name())) {
    return false;
  }

  if (!filter.s3_filter.metadata_filter.kv.empty()) {
    // metadata filter exists
    if (res.s) {
      filter_amz_meta(res.x_meta_map, res.s->info.x_meta_map);
    }
    metadata_from_attributes(res, obj);
    if (!match(filter.s3_filter.metadata_filter, res.x_meta_map)) {
      return false;
    }
  }

  if (!filter.s3_filter.tag_filter.kv.empty()) {
    // tag filter exists
    if (req_tags) {
      // tags in the request
      if (!match(filter.s3_filter.tag_filter, req_tags->get_tags())) {
        return false;
      }
    } else if (res.tagset && !(*res.tagset).get_tags().empty()) {
      // tags were cached in req_state
      if (!match(filter.s3_filter.tag_filter, (*res.tagset).get_tags())) {
        return false;
      }
    } else {
      // try to fetch tags from the attributes
      KeyMultiValueMap tags;
      tags_from_attributes(res, obj, tags);
      if (!match(filter.s3_filter.tag_filter, tags)) {
        return false;
      }
    }
  }

  return true;
}

int publish_reserve(const DoutPrefixProvider* dpp,
                    const SiteConfig& site,
                    const EventTypeList& event_types,
                    reservation_t& res,
                    const RGWObjTags* req_tags) {
  rgw_pubsub_bucket_topics bucket_topics;
  if (all_zonegroups_support(site, zone_features::notification_v2) &&
      res.store->stat_topics_v1(res.user_tenant, res.yield, res.dpp) == -ENOENT) {
    auto ret = get_bucket_notifications(dpp, res.bucket, bucket_topics);
    if (ret < 0) {
      return ret;
    }
  } else {
    const RGWPubSub ps(res.store, res.user_tenant, site);
    const RGWPubSub::Bucket ps_bucket(ps, res.bucket);
    auto rc = ps_bucket.get_topics(res.dpp, bucket_topics, res.yield);
    if (rc < 0) {
      // failed to fetch bucket topics
      return rc;
    }
  }
  for (auto& bucket_topic : bucket_topics.topics) {
    rgw_pubsub_topic_filter& topic_filter = bucket_topic.second;
    rgw_pubsub_topic& topic_cfg = topic_filter.topic;
    for (auto& event_type : event_types) {
      if (!notification_match(res, topic_filter, event_type, req_tags)) {
        // notification does not apply to req_state
        continue;
      }
      ldpp_dout(res.dpp, 20)
          << "INFO: notification: '" << topic_filter.s3_id << "' on topic: '"
          << topic_cfg.dest.arn_topic << "' and bucket: '"
          << res.bucket->get_name() << "' (unique topic: '" << topic_cfg.name
          << "') apply to event of type: '" << to_string(event_type) << "'"
          << dendl;

      // reload the topic in case it changed since the notification was added
      const std::string& topic_tenant = std::visit(fu2::overload(
          [] (const rgw_user& u) -> std::string { return u.tenant; },
          [] (const rgw_account_id& a) -> std::string { return a; }
          ), topic_cfg.owner);
      const RGWPubSub ps(res.store, topic_tenant, site);
      int ret = ps.get_topic(res.dpp, topic_cfg.dest.arn_topic,
                             topic_cfg, res.yield, nullptr);
      if (ret < 0) {
        ldpp_dout(res.dpp, 1)
            << "INFO: failed to load topic: " << topic_cfg.dest.arn_topic
            << ". error: " << ret
            << " while reserving persistent notification event" << dendl;
        if (ret == -ENOENT) {
          // either the topic is deleted but the corresponding notification
          // still exist or in v2 mode the notification could have synced first
          // but topic is not synced yet.
          continue;
        }
        ldpp_dout(res.dpp, 1)
            << "WARN: Using the stored topic from bucket notification struct."
            << dendl;
      }

      cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
      if (topic_cfg.dest.persistent) {
        // TODO: take default reservation size from conf
        constexpr auto DEFAULT_RESERVATION = 4 * 1024U;  // 4K
        res.size = DEFAULT_RESERVATION;
        librados::ObjectWriteOperation op;
        bufferlist obl;
        int rval;
        const auto& queue_name = topic_cfg.dest.persistent_queue;
        cls_2pc_queue_reserve(op, res.size, 1, &obl, &rval);
        auto ret = rgw_rados_operate(
            res.dpp, res.store->getRados()->get_notif_pool_ctx(), queue_name,
            &op, res.yield, librados::OPERATION_RETURNVEC);
        if (ret < 0) {
          ldpp_dout(res.dpp, 1)
              << "ERROR: failed to reserve notification on queue: "
              << queue_name << ". error: " << ret << dendl;
          // if no space is left in queue we ask client to slow down
          return (ret == -ENOSPC) ? -ERR_RATE_LIMITED : ret;
        }
        ret = cls_2pc_queue_reserve_result(obl, res_id);
        if (ret < 0) {
          ldpp_dout(res.dpp, 1)
              << "ERROR: failed to parse reservation id. error: " << ret
              << dendl;
          return ret;
        }
      }

      res.topics.emplace_back(topic_filter.s3_id, topic_cfg, res_id, event_type);
    }
  }
  return 0;
}

int publish_commit(rgw::sal::Object* obj,
		   uint64_t size,
		   const ceph::real_time& mtime,
		   const std::string& etag,
		   const std::string& version,
		   reservation_t& res,
		   const DoutPrefixProvider* dpp)
{
  for (auto& topic : res.topics) {
    if (topic.cfg.dest.persistent &&
	topic.res_id == cls_2pc_reservation::NO_ID) {
      // nothing to commit or already committed/aborted
      continue;
    }
    event_entry_t event_entry;
    populate_event(res, obj, size, mtime, etag, version, topic.event_type,
                   event_entry.event);
    event_entry.event.configurationId = topic.configurationId;
    event_entry.event.opaque_data = topic.cfg.opaque_data;
    if (topic.cfg.dest.persistent) { 
      event_entry.push_endpoint = std::move(topic.cfg.dest.push_endpoint);
      event_entry.push_endpoint_args =
	std::move(topic.cfg.dest.push_endpoint_args);
      event_entry.arn_topic = topic.cfg.dest.arn_topic;
      event_entry.creation_time = ceph::coarse_real_clock::now();
      event_entry.time_to_live = topic.cfg.dest.time_to_live;
      event_entry.max_retries = topic.cfg.dest.max_retries;
      event_entry.retry_sleep_duration = topic.cfg.dest.retry_sleep_duration;
      bufferlist bl;
      encode(event_entry, bl);
      const auto& queue_name = topic.cfg.dest.persistent_queue;
      if (bl.length() > res.size) {
        // try to make a larger reservation, fail only if this is not possible
        ldpp_dout(dpp, 5) << "WARNING: committed size: " << bl.length()
			  << " exceeded reserved size: " << res.size
			  <<
          " . trying to make a larger reservation on queue:" << queue_name
			  << dendl;
        // first cancel the existing reservation
        librados::ObjectWriteOperation op;
        cls_2pc_queue_abort(op, topic.res_id);
        auto ret = rgw_rados_operate(
	  dpp, res.store->getRados()->get_notif_pool_ctx(),
	  queue_name, &op,
	  res.yield);
        if (ret < 0) {
          ldpp_dout(dpp, 1) << "ERROR: failed to abort reservation: "
			    << topic.res_id << 
            " when trying to make a larger reservation on queue: " << queue_name
			    << ". error: " << ret << dendl;
          return ret;
        }
        // now try to make a bigger one
	buffer::list obl;
        int rval;
        cls_2pc_queue_reserve(op, bl.length(), 1, &obl, &rval);
        ret = rgw_rados_operate(
	  dpp, res.store->getRados()->get_notif_pool_ctx(),
          queue_name, &op, res.yield, librados::OPERATION_RETURNVEC);
        if (ret < 0) {
          ldpp_dout(dpp, 1) << "ERROR: failed to reserve extra space on queue: "
			    << queue_name
			    << ". error: " << ret << dendl;
          return (ret == -ENOSPC) ? -ERR_RATE_LIMITED : ret;
        }
        ret = cls_2pc_queue_reserve_result(obl, topic.res_id);
        if (ret < 0) {
          ldpp_dout(dpp, 1) << "ERROR: failed to parse reservation id for "
	    "extra space. error: " << ret << dendl;
          return ret;
        }
      }
      std::vector<buffer::list> bl_data_vec{std::move(bl)};
      librados::ObjectWriteOperation op;
      cls_2pc_queue_commit(op, bl_data_vec, topic.res_id);
      topic.res_id = cls_2pc_reservation::NO_ID;
      auto pcc_arg = make_unique<PublishCommitCompleteArg>(queue_name, dpp->get_cct());
      aio_completion_ptr completion{librados::Rados::aio_create_completion(pcc_arg.get(), publish_commit_completion)};
      auto& io_ctx = res.store->getRados()->get_notif_pool_ctx();
      if (const int ret = io_ctx.aio_operate(queue_name, completion.get(), &op); ret < 0) {
        ldpp_dout(dpp, 1) << "ERROR: failed to commit reservation to queue: "
                          << queue_name << ". error: " << ret << dendl;
        return ret;
      }
      // args will be released inside the callback
      pcc_arg.release();
    } else {
      try {
        // TODO add endpoint LRU cache
        const auto push_endpoint = RGWPubSubEndpoint::create(
	  topic.cfg.dest.push_endpoint,
	  topic.cfg.dest.arn_topic,
	  RGWHTTPArgs(topic.cfg.dest.push_endpoint_args, dpp),
	  dpp->get_cct());
        ldpp_dout(res.dpp, 20) << "INFO: push endpoint created: "
			       << topic.cfg.dest.push_endpoint << dendl;
        const auto ret = push_endpoint->send(dpp, event_entry.event, res.yield);
        if (ret < 0) {
          ldpp_dout(dpp, 1)
              << "ERROR: failed to push sync notification event with error: "
              << ret << " for event with " << event_entry << dendl;
          if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_failed);
          return ret;
        }
        if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_ok);
      } catch (const RGWPubSubEndpoint::configuration_error& e) {
        ldpp_dout(dpp, 1) << "ERROR: failed to create push endpoint for sync "
                             "notification event  with  error: "
                          << e.what() << " event with " << event_entry << dendl;
        if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_failed);
        return -EINVAL;
      }
    }
  }
  return 0;
}

int publish_abort(reservation_t& res) {
  for (auto& topic : res.topics) {
    if (!topic.cfg.dest.persistent ||
	topic.res_id == cls_2pc_reservation::NO_ID) {
      // nothing to abort or already committed/aborted
      continue;
    }
    const auto& queue_name = topic.cfg.dest.persistent_queue;
    librados::ObjectWriteOperation op;
    cls_2pc_queue_abort(op, topic.res_id);
    const auto ret = rgw_rados_operate(
      res.dpp, res.store->getRados()->get_notif_pool_ctx(),
      queue_name, &op, res.yield);
    if (ret < 0) {
      ldpp_dout(res.dpp, 1) << "ERROR: failed to abort reservation: "
			    << topic.res_id <<
        " from queue: " << queue_name << ". error: " << ret << dendl;
      return ret;
    }
    topic.res_id = cls_2pc_reservation::NO_ID;
  }
  return 0;
}

int get_persistent_queue_stats(const DoutPrefixProvider *dpp, librados::IoCtx &rados_ioctx,
                               const std::string &queue_name, rgw_topic_stats &stats, optional_yield y)
{
  // TODO: use optional_yield instead calling rados_ioctx.operate() synchronously
  cls_2pc_reservations reservations;
  auto ret = cls_2pc_queue_list_reservations(rados_ioctx, queue_name, reservations);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to read queue list reservation: " << ret << dendl;
    return ret;
  }
  stats.queue_reservations = reservations.size();

  ret = cls_2pc_queue_get_topic_stats(rados_ioctx, queue_name, stats.queue_entries, stats.queue_size);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to get the queue size or the number of entries: " << ret << dendl;
    return ret;
  }

  return 0;
}

reservation_t::reservation_t(const DoutPrefixProvider* _dpp,
			     rgw::sal::RadosStore* _store,
			     const req_state* _s,
			     rgw::sal::Object* _object,
			     rgw::sal::Object* _src_object,
			     const std::string* _object_name,
			     optional_yield y) :
  dpp(_s), store(_store), s(_s), size(0) /* XXX */,
  object(_object), src_object(_src_object), bucket(_s->bucket.get()),
  object_name(_object_name),
  tagset(_s->tagset),
  metadata_fetched_from_attributes(false),
  user_id(to_string(_s->owner.id)),
  user_tenant(_s->user->get_id().tenant),
  req_id(_s->req_id),
  yield(y)
{
  filter_amz_meta(x_meta_map, _s->info.x_meta_map);
}

reservation_t::reservation_t(const DoutPrefixProvider* _dpp,
			     rgw::sal::RadosStore* _store,
			     rgw::sal::Object* _object,
			     rgw::sal::Object* _src_object,
			     rgw::sal::Bucket* _bucket,
			     const std::string& _user_id,
			     const std::string& _user_tenant,
			     const std::string& _req_id,
			     optional_yield y) :
    dpp(_dpp), store(_store), s(nullptr), size(0) /* XXX */,
    object(_object), src_object(_src_object), bucket(_bucket),
    object_name(nullptr),
    metadata_fetched_from_attributes(false),
    user_id(_user_id),
    user_tenant(_user_tenant),
    req_id(_req_id),
    yield(y)
{}

reservation_t::~reservation_t() {
  publish_abort(*this);
}

void rgw_topic_stats::dump(Formatter *f) const {
  f->open_object_section("Topic Stats");
  f->dump_int("Reservations", queue_reservations);
  f->dump_int("Size", queue_size);
  f->dump_int("Entries", queue_entries);
  f->close_section();
}

} // namespace rgw::notify
