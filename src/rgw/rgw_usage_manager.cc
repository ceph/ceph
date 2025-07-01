#include "rgw_usage_manager.h"
#include "rgw_user.h"
#include <chrono>

using namespace std::chrono_literals;
using rgw::usage_counters::user_usage_cache;
using rgw::usage_counters::bucket_usage_cache;
using rgw::usage_counters::rgw_user_usage_counters_key;
using rgw::usage_counters::rgw_bucket_usage_counters_key;

RGWUsageManager::RGWUsageManager(const DoutPrefixProvider* dpp,
                                 rgw::sal::Driver* driver,
                                 RGWUsageCache* cache)
  : dpp(dpp), driver(driver), cache(cache)
{
  interval = dpp->get_cct()->_conf.get_val<uint64_t>("rgw_usage_cache_refresh_interval");
}

RGWUsageManager::~RGWUsageManager()
{
  stop();
}

void RGWUsageManager::start()
{
  stop_flag = false;
  thr = std::thread(&RGWUsageManager::run, this);
}

void RGWUsageManager::stop()
{
  stop_flag = true;
  if (thr.joinable())
    thr.join();
}

void RGWUsageManager::run()
{
  while (!stop_flag) {
    refresh();
    for (unsigned i=0; i<interval && !stop_flag; ++i) {
      std::this_thread::sleep_for(1s);
    }
  }
}

void RGWUsageManager::refresh()
{
  void* handle = nullptr;
  int r = driver->meta_list_keys_init(dpp, "user", std::string(), &handle);
  if (r < 0)
    return;
  bool truncated = false;
  do {
    std::list<std::string> keys;
    r = driver->meta_list_keys_next(dpp, handle, 1000, keys, &truncated);
    if (r < 0 && r != -ENOENT) {
      break;
    }
    for (auto& id : keys) {
      auto user = driver->get_user(rgw_user(id));
      if (!user)
        continue;
      if (user->load_user(dpp, null_yield) < 0)
        continue;
      std::map<std::string, bucket_meta_entry> buckets;
      if (rgw_user_get_all_buckets_stats(dpp, driver, user.get(), buckets, null_yield) < 0)
        continue;
      uint64_t total_b = 0, total_o = 0;
      for (auto& it : buckets) {
        total_b += it.second.size;
        total_o += it.second.count;
        cache->put_bucket(it.first, it.second.size, it.second.count);
        if (bucket_usage_cache) {
          std::string key = ceph::perf_counters::key_create(rgw_bucket_usage_counters_key, {{"Bucket", it.first}});
          bucket_usage_cache->set_counter(key, l_rgw_bucket_used_bytes, it.second.size);
          bucket_usage_cache->set_counter(key, l_rgw_bucket_num_objects, it.second.count);
        }
      }
      cache->put_user(id, total_b, total_o);
      if (user_usage_cache) {
        std::string key = ceph::perf_counters::key_create(rgw_user_usage_counters_key, {{"User", id}});
        user_usage_cache->set_counter(key, l_rgw_user_used_bytes, total_b);
        user_usage_cache->set_counter(key, l_rgw_user_num_objects, total_o);
      }
    }
  } while (truncated);
  driver->meta_list_keys_complete(handle);
}

