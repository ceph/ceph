#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

#include "rgw_common.h"
#include "rgw_ratelimit_core.h"
#include "rgw_ratelimit_store.h"

class RateLimiterEntry {
public:
  static constexpr int64_t fixed_point_rgw_ratelimit = RGWRateLimitCounterState::fixed_point;

  static int64_t compute_delay(int64_t limit, int64_t needed, int64_t interval)
  {
    return rgw::ratelimit::compute_delay(limit, needed, interval);
  }

  int64_t should_rate_limit(OpType op_type,
                            const RGWRateLimitInfo* ratelimit_info,
                            ceph::timespan curr_timestamp,
                            int64_t interval)
  {
    std::unique_lock lock(ts_lock);
    return rgw::ratelimit::consume(state, op_type, ratelimit_info, curr_timestamp, interval);
  }

  void decrease_bytes(bool is_read, int64_t amount, const RGWRateLimitInfo* info)
  {
    std::unique_lock lock(ts_lock);
    rgw::ratelimit::decrease_bytes(state, is_read, amount, info);
  }

  void giveback_tokens(OpType op_type)
  {
    std::unique_lock lock(ts_lock);
    rgw::ratelimit::giveback(state, op_type);
  }

  RGWRateLimitCounterState& get_state() { return state; }
  const RGWRateLimitCounterState& get_state() const { return state; }

private:
  RGWRateLimitCounterState state;
  std::mutex ts_lock;
};

class LocalRateLimiter : public RateLimitStore, public DoutPrefix {
  static constexpr size_t map_size = 2000000;
  std::shared_mutex insert_lock;
  std::atomic_bool& replacing;
  std::condition_variable& cv;
  using hash_map = std::unordered_map<std::string, RateLimiterEntry>;
  hash_map ratelimit_entries{map_size};

  RateLimiterEntry& find_or_create(const std::string& key)
  {
    std::shared_lock rlock(insert_lock);
    if (ratelimit_entries.size() > 0.9 * map_size && replacing == false) {
      replacing = true;
      cv.notify_all();
    }
    auto ret = ratelimit_entries.find(key);
    rlock.unlock();
    if (ret == ratelimit_entries.end()) {
      std::unique_lock wlock(insert_lock);
      ret = ratelimit_entries.emplace(std::piecewise_construct,
                                      std::forward_as_tuple(key),
                                      std::forward_as_tuple()).first;
    }
    return ret->second;
  }

public:
  LocalRateLimiter(const LocalRateLimiter&) = delete;
  LocalRateLimiter& operator=(const LocalRateLimiter&) = delete;
  LocalRateLimiter() = delete;

  LocalRateLimiter(CephContext* cct, std::atomic_bool& replacing, std::condition_variable& cv)
    : DoutPrefix(cct, ceph_subsys_rgw, "rate limiter: "),
      replacing(replacing),
      cv(cv)
  {
    ratelimit_entries.max_load_factor(1000);
  }

  int64_t should_rate_limit(const char* method,
                            const std::string& key,
                            ceph::coarse_real_time curr_timestamp,
                            const RGWRateLimitInfo* ratelimit_info,
                            const std::string& resource) override
  {
    ldpp_dout(this, 21) << "checking should_rate_limit: key=" << std::quoted(key)
                        << " enabled=" << ratelimit_info->enabled << dendl;
    if (key.empty() || key.length() == 1 || !ratelimit_info->enabled) {
      return 0;
    }
    OpType type = rgw_ratelimit_op_type(method, resource, ratelimit_info);
    auto& entry = find_or_create(key);
    const int64_t interval = get_cct()->_conf->rgw_ratelimit_interval;
    return entry.should_rate_limit(type, ratelimit_info,
                                   curr_timestamp.time_since_epoch(), interval);
  }

  void giveback_tokens(const char* method,
                       const std::string& key,
                       const std::string& resource,
                       const RGWRateLimitInfo* ratelimit_info) override
  {
    if (key.empty() || key.length() == 1 || !ratelimit_info->enabled) {
      return;
    }
    OpType type = rgw_ratelimit_op_type(method, resource, ratelimit_info);
    auto& entry = find_or_create(key);
    entry.giveback_tokens(type);
  }

  void decrease_bytes(const char* method,
                      const std::string& key,
                      int64_t amount,
                      const RGWRateLimitInfo* info) override
  {
    if (key.empty() || key.length() == 1 || !info->enabled) {
      return;
    }
    OpType type = rgw_ratelimit_op_type(method, "", info);
    if ((type == OpType::Read && !info->max_read_bytes) ||
        (type == OpType::Write && !info->max_write_bytes)) {
      return;
    }
    auto& entry = find_or_create(key);
    if (type == OpType::Read) {
      entry.decrease_bytes(true, amount, info);
    } else if (type == OpType::Write) {
      entry.decrease_bytes(false, amount, info);
    }
  }

  void clear()
  {
    ratelimit_entries.clear();
  }
};

using RateLimiter = LocalRateLimiter;

class ActiveRateLimiter : public DoutPrefix {
  std::atomic_uint8_t stopped = {false};
  std::condition_variable cv;
  std::mutex cv_m;
  std::thread runner;
  std::atomic_bool replacing = false;
  std::atomic_uint8_t current_active = 0;
  std::shared_ptr<LocalRateLimiter> ratelimit[2];

  void replace_active()
  {
    ceph_pthread_setname("ratelimit_gc");
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> lk(cv_m);
    while (!stopped) {
      cv.wait(lk);
      current_active = current_active ^ 1;
      ldpp_dout(this, 20) << "replacing active ratelimit data structure" << dendl;
      while (!stopped && ratelimit[(current_active ^ 1)].use_count() > 1) {
        if (cv.wait_for(lk, 1min) != std::cv_status::timeout && stopped) {
          return;
        }
      }
      if (stopped) {
        return;
      }
      ldpp_dout(this, 20) << "clearing passive ratelimit data structure" << dendl;
      ratelimit[(current_active ^ 1)]->clear();
      replacing = false;
    }
  }

public:
  ActiveRateLimiter(const ActiveRateLimiter&) = delete;
  ActiveRateLimiter& operator=(const ActiveRateLimiter&) = delete;
  ActiveRateLimiter() = delete;

  explicit ActiveRateLimiter(CephContext* cct)
    : DoutPrefix(cct, ceph_subsys_rgw, "rate limiter: ")
  {
    ratelimit[0] = std::make_shared<LocalRateLimiter>(cct, replacing, cv);
    ratelimit[1] = std::make_shared<LocalRateLimiter>(cct, replacing, cv);
  }

  ~ActiveRateLimiter()
  {
    ldpp_dout(this, 20) << "stopping ratelimit_gc thread" << dendl;
    cv_m.lock();
    stopped = true;
    cv_m.unlock();
    cv.notify_all();
    runner.join();
  }

  std::shared_ptr<RateLimitStore> get_active()
  {
    return ratelimit[current_active];
  }

  void start()
  {
    ldpp_dout(this, 20) << "starting ratelimit_gc thread" << dendl;
    runner = std::thread(&ActiveRateLimiter::replace_active, this);
  }
};
