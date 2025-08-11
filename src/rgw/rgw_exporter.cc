#include "rgw_exporter.h"

#include <chrono>
#include <utility>

#include "common/perf_counters_key.h"

using namespace std::chrono_literals;
using namespace rgw::usage_counters;

namespace rgw{

RGWExporter::RGWExporter(CephContext* cct, RGWUsageCache* cache)
  : cct(cct), cache(cache) {}

RGWExporter::~RGWExporter() {
  stop();
}

void RGWExporter::start() {
  stop_requested = false;
  worker = std::thread(&RGWExporter::run, this);
}

void RGWExporter::stop() {
  stop_requested = true;
  if (worker.joinable()) {
    worker.join();
  }
}

void RGWExporter::run() {
  while (!stop_requested) {
    refresh();
    std::this_thread::sleep_for(1s);
  }
}

void RGWExporter::refresh() {
  /* Placeholder for reading usage from metadata and updating counters */
}

void RGWExporter::update_user(const std::string& user, const RGWUsageRecord& record) {
  if (!user_usage_counters_cache)
    return;
  std::string key = ceph::perf_counters::key_create(rgw_user_usage_counters_key, {{"user", user}});
  user_usage_counters_cache->set_counter(key, l_rgw_usage_user_used_bytes, record.used_bytes);
  user_usage_counters_cache->set_counter(key, l_rgw_usage_user_num_objects, record.num_objects);
}

void RGWExporter::update_bucket(const std::string& bucket, const RGWUsageRecord& record) {
  if (!bucket_usage_counters_cache)
    return;
  std::string key = ceph::perf_counters::key_create(rgw_bucket_usage_counters_key, {{"bucket", bucket}});
  bucket_usage_counters_cache->set_counter(key, l_rgw_usage_bucket_used_bytes, record.used_bytes);
  bucket_usage_counters_cache->set_counter(key, l_rgw_usage_bucket_num_objects, record.num_objects);
}

}