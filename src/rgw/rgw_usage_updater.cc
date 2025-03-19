#include "rgw_usage_updater.h"
#include "rgw_perf_counters.h"
#include "rgw_user.h"
#include "rgw_bucket.h"

using namespace rgw::op_counters;

RGWUsageUpdater::RGWUsageUpdater(RGWRados* store, CephContext* cct)
  : cct(cct), store(store), stop_flag(false) {}

RGWUsageUpdater::~RGWUsageUpdater() {
  stop();
}

void RGWUsageUpdater::start() {
  stop_flag = false;
  updater_thread = std::thread(&RGWUsageUpdater::run, this);
}

void RGWUsageUpdater::stop() {
  stop_flag = true;
  if (updater_thread.joinable()) {
    updater_thread.join();
  }
}

void RGWUsageUpdater::run() {
  while (!stop_flag) {
    update_all_metrics();
    std::this_thread::sleep_for(std::chrono::minutes(1));
  }
}

void RGWUsageUpdater::update_all_metrics() {
  // fetch and update all users
  std::vector<rgw_user> users;
  int r = store->list_users("", 1000, users); // paginate as needed
  if (r >= 0) {
    for (const auto& user : users) {
      update_user_metrics(user);
    }
  }

  // fetch and update all buckets
  std::vector<rgw_bucket> buckets;
  r = store->list_buckets("", "", 1000, &buckets, nullptr, nullptr); // paginate as needed
  if (r >= 0) {
    for (const auto& bucket : buckets) {
      update_bucket_metrics(bucket);
    }
  }
}

void RGWUsageUpdater::update_user_metrics(const rgw_user& user_id) {
  RGWUserInfo info;
  int r = store->get_user(user_id, info, nullptr);
  if (r < 0)
    return;

  auto key = ceph::perf_counters::key_create(rgw_user_op_counters_key, {{"user", user_id.to_str()}});
  auto counters = user_counters_cache->get(key);

  counters->set(l_rgw_user_used_bytes, info.stats.size_actual);
  counters->set(l_rgw_user_num_objects, info.stats.num_objects);
}

void RGWUsageUpdater::update_bucket_metrics(const rgw_bucket& bucket_id) {
  RGWBucketInfo info;
  int r = store->get_bucket_instance_info(bucket_id, info, nullptr);
  if (r < 0)
    return;

  auto key = ceph::perf_counters::key_create(rgw_bucket_op_counters_key, {{"bucket", bucket_id.name}});
  auto counters = bucket_counters_cache->get(key);

  counters->set(l_rgw_bucket_used_bytes, info.usage.size_actual);
  counters->set(l_rgw_bucket_num_objects, info.usage.num_objects);
}
