#include "rgw_exporter.h"
#include <sstream>
#include <chrono>
#include <thread>

RGWExporter::RGWExporter() : stop_flag(false), usage_cache(nullptr) {
    // Initialize the usage cache with the LMDB directory.
    // Ensure that the path "/var/lib/ceph/rgw_lmdb" exists and is writable.
    usage_cache = new RGWUsageCache("/var/lib/ceph/rgw_lmdb");
}

RGWExporter::~RGWExporter() {
    stop();
    if (usage_cache) {
        usage_cache->flush_to_lmdb();
        delete usage_cache;
        usage_cache = nullptr;
    }
}

// Pseudo-code function: Retrieve a list of all bucket info objects.
// In an actual implementation, this function would interact with the RGW metadata store.
static std::vector<RGWBucketInfo> get_all_buckets() {
    std::vector<RGWBucketInfo> buckets;
    // Example: Use an existing RGW API to list buckets
    // buckets = RGWRados::list_buckets();  // Replace with actual call
    // For now, assume this function is implemented elsewhere.
    return buckets;
}

// Pseudo-code function: Retrieve a list of all user info objects.
// In a real implementation, this would query the metadata store or use a helper API.
static std::vector<RGWUserInfo> get_all_users() {
    std::vector<RGWUserInfo> users;
    // Example: Use an existing RGW API to list users
    // users = RGWRados::list_users();  // Replace with actual call
    // For now, assume this function is implemented elsewhere.
    return users;
}

void RGWExporter::start() {
    stop_flag = false;
    update_thread = std::thread(&RGWExporter::update_metrics_loop, this);
}

void RGWExporter::stop() {
    stop_flag = true;
    if (update_thread.joinable()) {
        update_thread.join();
    }
}

void RGWExporter::update_metrics_loop() {
    while (!stop_flag) {
        update_metrics();
        // Sleep for a fixed interval (e.g., 30 seconds) between updates.
        std::this_thread::sleep_for(std::chrono::seconds(30));
    }
}

void RGWExporter::update_metrics() {
    
    // Retrieve bucket usage metrics
    std::vector<std::string> buckets = RGWBucket::list_all_buckets(); // Get all bucket names
    for (const auto& bucket : buckets) {
        uint64_t bytes_used = RGWBucket::get_usage(bucket); // Get actual bucket usage
        usage_cache->update_bucket_usage(bucket, bytes_used);
    }

    // Retrieve user usage metrics
    std::vector<std::string> users = RGWUser ::list_all_users(); // Get all user names
    for (const auto& user : users) {
        uint64_t bytes_used = RGWUser ::get_usage(user); // Get actual user usage
        usage_cache->update_user_usage(user, bytes_used);
    }

    // Flush the in-memory usage cache to LMDB.
    if (!usage_cache->flush_to_lmdb()) {
        std::cerr << "Failed to flush usage cache to LMDB." << std::endl;
    }

    // Flush the in-memory usage cache to LMDB.
    usage_cache->flush_to_lmdb();
}

std::string RGWExporter::get_prometheus_metrics() {
    std::ostringstream metrics;
    // Retrieve stored usage values from the usage cache.
    uint64_t bucket_usage = usage_cache->get_bucket_usage("bucket1");
    uint64_t user_usage = usage_cache->get_user_usage("user1");

    metrics << "ceph_rgw_bucket_usage_bytes{bucket=\"bucket1\"} " << bucket_usage << "\n";
    metrics << "ceph_rgw_user_usage_bytes{user=\"user1\"} " << user_usage << "\n";
    return metrics.str();
}
