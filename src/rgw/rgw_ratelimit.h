#pragma once
#include <chrono>
#include <thread>
#include <condition_variable>
#include "rgw_common.h"


class RateLimiterEntry {
  /* 
    fixed_point_rgw_ratelimit is important to preserve the precision of the token calculation
    for example: a user have a limit of single op per minute, the user will consume its single token and then will send another request, 1s after it.
    in that case, without this method, the user will get 0 tokens although it should get 0.016 tokens.
    using this method it will add 16 tokens to the user, and the user will have 16 tokens, each time rgw will do comparison rgw will divide by fixed_point_rgw_ratelimit, so the user will be blocked anyway until it has enough tokens.
  */
  static constexpr int64_t fixed_point_rgw_ratelimit = 1000;
  // counters are tracked in multiples of fixed_point_rgw_ratelimit
  struct counters {
    int64_t ops = 0;
    int64_t bytes = 0;
  };
  counters read;
  counters write;
  ceph::timespan ts;
  bool first_run = true;
  std::mutex ts_lock;
  // Those functions are returning the integer value of the tokens 
  int64_t read_ops () const
  {
    return read.ops / fixed_point_rgw_ratelimit;
  }
  int64_t write_ops() const
  {
    return write.ops / fixed_point_rgw_ratelimit;
  }
  int64_t read_bytes() const
  {
    return read.bytes / fixed_point_rgw_ratelimit;
  }
  int64_t write_bytes() const
  {
    return write.bytes / fixed_point_rgw_ratelimit;
  }
  bool should_rate_limit_read(int64_t ops_limit, int64_t bw_limit) {
    //check if tenants did not reach their bw or ops limits and that the limits are not 0 (which is unlimited)
    if(((read_ops() - 1 < 0) && (ops_limit > 0)) ||
      (read_bytes() < 0 && bw_limit > 0))
  {
    return true;
  }
    // we don't want to reduce ops' tokens if we've rejected it.
    read.ops -= fixed_point_rgw_ratelimit;
    return false;
  }
  bool should_rate_limit_write(int64_t ops_limit, int64_t bw_limit) 
  {
    //check if tenants did not reach their bw or ops limits and that the limits are not 0 (which is unlimited)
    if(((write_ops() - 1 < 0) && (ops_limit > 0)) ||
      (write_bytes() < 0 && bw_limit > 0))
    {
      return true;
    }

    // we don't want to reduce ops' tokens if we've rejected it.
    write.ops -= fixed_point_rgw_ratelimit;
    return false;
  }
  /* The purpose of this function is to minimum time before overriding the stored timestamp
     This function is necessary to force the increase tokens add at least 1 token when it updates the last stored timestamp.
     That way the user/bucket will not lose tokens because of rounding
  */
  bool minimum_time_reached(ceph::timespan curr_timestamp) const
  {
    using namespace std::chrono;
    constexpr auto min_duration = duration_cast<ceph::timespan>(seconds(60)) / fixed_point_rgw_ratelimit;
    const auto delta = curr_timestamp - ts;
    if (delta < min_duration)
    {
      return false;
    }
    return true;
  }

  void increase_tokens(ceph::timespan curr_timestamp,
                       const RGWRateLimitInfo* info)
  {
    constexpr int fixed_point = fixed_point_rgw_ratelimit;
    if (first_run)
    {
      write.ops = info->max_write_ops * fixed_point;
      write.bytes = info->max_write_bytes * fixed_point;
      read.ops = info->max_read_ops * fixed_point;
      read.bytes = info->max_read_bytes * fixed_point;
      ts = curr_timestamp;
      first_run = false;
      return;
    }
    else if(curr_timestamp > ts && minimum_time_reached(curr_timestamp))
    {
      const int64_t time_in_ms = std::chrono::duration_cast<std::chrono::milliseconds>(curr_timestamp - ts).count() / 60.0 / std::milli::den * fixed_point; // / 60 to make it work with 1 min token bucket
      ts = curr_timestamp;
      const int64_t write_ops = info->max_write_ops * time_in_ms;
      const int64_t write_bw = info->max_write_bytes * time_in_ms;
      const int64_t read_ops = info->max_read_ops * time_in_ms;
      const int64_t read_bw = info->max_read_bytes * time_in_ms;
      read.ops = std::min(info->max_read_ops * fixed_point, read_ops + read.ops);
      read.bytes = std::min(info->max_read_bytes * fixed_point, read_bw + read.bytes);
      write.ops = std::min(info->max_write_ops * fixed_point, write_ops + write.ops);
      write.bytes = std::min(info->max_write_bytes * fixed_point, write_bw + write.bytes);
    }
  }

  public:
    bool should_rate_limit(bool is_read, const RGWRateLimitInfo* ratelimit_info, ceph::timespan curr_timestamp)
    {
      std::unique_lock lock(ts_lock);
      increase_tokens(curr_timestamp, ratelimit_info);
      if (is_read)
      {
        return should_rate_limit_read(ratelimit_info->max_read_ops, ratelimit_info->max_read_bytes);
      }
      return should_rate_limit_write(ratelimit_info->max_write_ops, ratelimit_info->max_write_bytes);
    }
    void decrease_bytes(bool is_read, int64_t amount, const RGWRateLimitInfo* info) {
      std::unique_lock lock(ts_lock);
      // we don't want the tenant to be with higher debt than 120 seconds(2 min) of its limit
      if (is_read)
      {
        read.bytes = std::max(read.bytes - amount * fixed_point_rgw_ratelimit,info->max_read_bytes * fixed_point_rgw_ratelimit * -2);
      } else {
        write.bytes = std::max(write.bytes - amount * fixed_point_rgw_ratelimit,info->max_write_bytes * fixed_point_rgw_ratelimit * -2);
      }
    }
    void giveback_tokens(bool is_read)
    {
      std::unique_lock lock(ts_lock);
      if (is_read) 
      {
        read.ops += fixed_point_rgw_ratelimit;
      } else {
        write.ops += fixed_point_rgw_ratelimit;
      }
    }
};

class RateLimiter {

  static constexpr size_t map_size = 2000000; // will create it with the closest upper prime number
  std::shared_mutex insert_lock;
  std::atomic_bool& replacing;
  std::condition_variable& cv;
  typedef std::unordered_map<std::string, RateLimiterEntry> hash_map;
  hash_map ratelimit_entries{map_size};
  static bool is_read_op(const std::string_view method) {
    if (method == "GET" || method == "HEAD")
    {
      return true;
    }
    return false;
  }

    // find or create an entry, and return its iterator
  auto& find_or_create(const std::string& key) {
    std::shared_lock rlock(insert_lock);
    if (ratelimit_entries.size() > 0.9 * map_size && replacing == false)
    {
      replacing = true;
      cv.notify_all();
    }
    auto ret = ratelimit_entries.find(key);
    rlock.unlock();
    if (ret == ratelimit_entries.end())
    {
      std::unique_lock wlock(insert_lock);
      ret = ratelimit_entries.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(key),
                                 std::forward_as_tuple()).first;
    }
    return ret->second;
  }

  

  public:
    RateLimiter(const RateLimiter&) = delete;
    RateLimiter& operator =(const RateLimiter&) = delete;
    RateLimiter(RateLimiter&&) = delete;
    RateLimiter& operator =(RateLimiter&&) = delete;
    RateLimiter() = delete;
    RateLimiter(std::atomic_bool& replacing, std::condition_variable& cv)
      : replacing(replacing), cv(cv)
    {
      // prevents rehash, so no iterators invalidation
      ratelimit_entries.max_load_factor(1000);
    };

    bool should_rate_limit(const char *method, const std::string& key, ceph::coarse_real_time curr_timestamp, const RGWRateLimitInfo* ratelimit_info) {
      if (key.empty() || key.length() == 1 || !ratelimit_info->enabled)
      {
        return false;
      }
      bool is_read = is_read_op(method);
      auto& it = find_or_create(key);
      auto curr_ts = curr_timestamp.time_since_epoch();
      return it.should_rate_limit(is_read ,ratelimit_info, curr_ts);
    }
    void giveback_tokens(const char *method, const std::string& key)
    {
      bool is_read = is_read_op(method);
      auto& it = find_or_create(key);
      it.giveback_tokens(is_read);
    }
    void decrease_bytes(const char *method, const std::string& key, const int64_t amount, const RGWRateLimitInfo* info) {
      if (key.empty() || key.length() == 1 || !info->enabled)
      {
        return;
      }
      bool is_read = is_read_op(method);
      if ((is_read && !info->max_read_bytes) || (!is_read && !info->max_write_bytes))
      {
        return;
      }
      auto& it = find_or_create(key);
      it.decrease_bytes(is_read, amount, info);
    }
    void clear() {
      ratelimit_entries.clear();
    }
};
// This class purpose is to hold 2 RateLimiter instances, one active and one passive.
// once the active has reached the watermark for clearing it will call the replace_active() thread using cv
// The replace_active will clear the previous RateLimiter after all requests to it has been done (use_count() > 1)
// In the meanwhile new requests will come into the newer active
class ActiveRateLimiter : public DoutPrefix  {
  std::atomic_uint8_t stopped = {false};
  std::condition_variable cv;
  std::mutex cv_m;
  std::thread runner;
  std::atomic_bool replacing = false;
  std::atomic_uint8_t current_active = 0;
  std::shared_ptr<RateLimiter> ratelimit[2];
  void replace_active() {
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> lk(cv_m);
    while (!stopped) {
      cv.wait(lk);
      current_active = current_active ^ 1;
      ldpp_dout(this, 20) << "replacing active ratelimit data structure" << dendl;
      while (!stopped && ratelimit[(current_active ^ 1)].use_count() > 1 ) {
        if (cv.wait_for(lk, 1min) != std::cv_status::timeout && stopped)
        {
          return;
        }
      }
      if (stopped)
      {
        return;
      }
      ldpp_dout(this, 20) << "clearing passive ratelimit data structure" << dendl;
      ratelimit[(current_active ^ 1)]->clear();
      replacing = false;
    }
  }
  public:
    ActiveRateLimiter(const ActiveRateLimiter&) = delete;
    ActiveRateLimiter& operator =(const ActiveRateLimiter&) = delete;
    ActiveRateLimiter(ActiveRateLimiter&&) = delete;
    ActiveRateLimiter& operator =(ActiveRateLimiter&&) = delete;
    ActiveRateLimiter() = delete;
    ActiveRateLimiter(CephContext* cct) :
      DoutPrefix(cct, ceph_subsys_rgw, "rate limiter: ")
    {
      ratelimit[0] = std::make_shared<RateLimiter>(replacing, cv);
      ratelimit[1] = std::make_shared<RateLimiter>(replacing, cv);
    }
    ~ActiveRateLimiter() {
      ldpp_dout(this, 20) << "stopping ratelimit_gc thread" << dendl;
      cv_m.lock();
      stopped = true;
      cv_m.unlock();
      cv.notify_all();
      runner.join();
    }
    std::shared_ptr<RateLimiter> get_active() {
      return ratelimit[current_active];
    }
    void start() {
      ldpp_dout(this, 20) << "starting ratelimit_gc thread" << dendl;
      runner = std::thread(&ActiveRateLimiter::replace_active, this);
      if (const auto rc = ceph_pthread_setname(runner.native_handle(), "ratelimit_gc"); rc != 0) {
        ldpp_dout(this, 1) << "ERROR: failed to set ratelimit_gc thread name. error: " << rc << dendl;
      }
    }
};
