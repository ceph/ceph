#include "rgw_exporter.h"
#include "rgw_rest_conn.h"
#include <sstream>
#include <thread>
#include <chrono>

RGWExporter::RGWExporter(CephContext* cct) : cct(cct) {}

RGWExporter::~RGWExporter() {
	stop();
}

void RGWExporter::start() {
  update_thread = std::thread(&RGWExporter::update_loop, this);
}

void RGWExporter::stop() {
  stop_flag = true;
  if (update_thread.joinable()) {
    update_thread.join();
  }
}

void RGWExporter::update_loop() {
  while (!stop_flag) {
    update_metrics();
    std::this_thread::sleep_for(std::chrono::minutes(5)); // Update every 5 minutes
  }
}

void RGWExporter::update_metrics() {
    fetch_bucket_metrics();
    fetch_user_metrics();
}

void RGWExporter::fetch_bucket_metrics() {
    
    RGWRados rados(cct);
    std::vector<rgw_bucket> buckets;

    // Retrieve all buckets
    int ret = rados.bucket_list(buckets);
    if (ret < 0) {
        ldout(cct, 0) << "Failed to fetch bucket list: " << ret << dendl;
        return;
    }

    bucket_usage_cache.clear();

    for (const auto& bucket : buckets) {
        // Fetch bucket info
        rgw_bucket_info bucket_info;
        ret = rados.read_bucket_info(bucket.tenant, bucket.name, &bucket_info);
        if (ret < 0) {
            ldout(cct, 0) << "Failed to fetch bucket info for bucket: " << bucket.name << " Error: " << ret << dendl;
            continue;
        }

        // Populate cache
        BucketUsage usage;
        usage.name = bucket.name;
        usage.tenant = bucket.tenant;
        usage.size_actual = bucket_info.usage.rgw_main.size_actual;
        usage.size_utilized = bucket_info.usage.rgw_main.size_utilized;
        usage.num_objects = bucket_info.usage.rgw_main.num_objects;

        bucket_usage_cache.emplace_back(usage);
    }
}

void RGWExporter::fetch_user_metrics() {
    RGWRados rados(cct);
    std::vector<std::string> users;

    // Retrieve all users
    int ret = rados.user_list(users);
    if (ret < 0) {
        ldout(cct, 0) << "Failed to fetch user list: " << ret << dendl;
        return;
    }

    user_usage_cache.clear();

    for (const auto& user : users) {
        // Fetch user info
        RGWUserInfo user_info;
        ret = rados.get_user(user, user_info);
        if (ret < 0) {
            ldout(cct, 0) << "Failed to fetch user info for user: " << user << " Error: " << ret << dendl;
            continue;
        }

        // Populate cache
        UserUsage usage;
        usage.user_id = user_info.user_id;
        usage.size_actual = user_info.stats.size_actual;
        usage.num_objects = user_info.stats.num_objects;

        user_usage_cache.emplace_back(usage);
    }
}

std::string RGWExporter::get_prometheus_metrics() {
    std::ostringstream metrics;

    for (const auto& [bucket, usage] : bucket_usage_cache) {
    	metrics << "ceph_rgw_bucket_usage_bytes{bucket=\"" << bucket << "\"} " << usage << "\n";
    }
  
    for (const auto& [user, usage] : user_usage_cache) {
         metrics << "ceph_rgw_user_usage_bytes{user=\"" << user << "\"} " << usage << "\n";
    }
  return metrics.str();
}
