#include "rgw_usage_manager.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "common/ceph_context.h"
#include "common/perf_counters.h"
#include "common/ceph_time.h"
#include <pthread.h>

RGWUsageManager::RGWUsageManager(CephContext *cct_, RGWUsageCache *cache, PerfCounters *logger)
    : cct(cct_), usage_cache(cache), perf_logger(logger),
      stopping(false) {
    refresh_interval = cct->_conf->rgw_usage_cache_refresh_interval;
}

RGWUsageManager::~RGWUsageManager() {
    stop();
    // cleanup usage cache
    delete usage_cache;
}

void RGWUsageManager::start() {
    // Launch background thread to periodically refresh usage metrics
    refresh_thread = std::thread(&RGWUsageManager::run, this);
}

void RGWUsageManager::stop() {
    bool expected = false;
    if (!stopping.compare_exchange_strong(expected, true)) {
        // already stopping or stopped
        return;
    }
    {
        std::lock_guard<std::mutex> l(cond_lock);
        cond.notify_one(); // wake thread if sleeping
    }
    if (refresh_thread.joinable()) {
        refresh_thread.join();
    }
}

void RGWUsageManager::run() {
    // Set thread name for easier identification
    ceph_pthread_setname(pthread_self(), "rgw-usage-ref");
    // Perform an initial refresh immediately
    refresh_usage();
    // Periodic loop
    std::unique_lock<std::mutex> lk(cond_lock);
    while (!stopping.load()) {
        // Wait for the interval or until stop is signaled
        if (cond.wait_for(lk, ceph::make_timespan(refresh_interval), [this]() { return stopping.load(); })) {
            break;
        }
        lk.unlock();
        refresh_usage();
        lk.lock();
    }
    ldout(cct, 10) << "RGWUsageManager thread exiting" << dendl;
}

// Virtual hook for updating perf counters (can be overridden for testing)
void RGWUsageManager::update_perf_counter(int id, const std::map<std::string, std::string>& labels, uint64_t value) {
    perf_logger->set(label_counter(id, labels), value);
}

void RGWUsageManager::refresh_usage() {
    ldout(cct, 20) << "RGWUsageManager: refreshing usage metrics..." << dendl;
    if (!usage_cache) {
        ldout(cct, 0) << "ERROR: usage_cache not initialized" << dendl;
        return;
    }
    RGWRados *store = g_rgw_store;  // global RGW store (assumed available)
    if (!store) {
        ldout(cct, 0) << "ERROR: RGWRados store is null, cannot refresh usage" << dendl;
        return;
    }
    // Prepare to rebuild cache from scratch
    usage_cache->clear();
    // Temporary accumulators for per-user totals
    std::map<std::string, RGWUsageStats> user_totals;
    // Iterate over all users and their buckets
    std::vector<std::string> users;
    int ret = store->list_users(users);
    if (ret < 0) {
        ldout(cct, 0) << "ERROR: list_users failed: " << ret << dendl;
    }
    for (const auto& user : users) {
        std::vector<rgw_bucket> buckets;
        int r = store->list_buckets(user, buckets);
        if (r < 0) {
            ldout(cct, 0) << "ERROR: list_buckets for user " << user << " failed: " << r << dendl;
            continue;
        }
        for (auto& bucket_handle : buckets) {
            RGWBucketInfo bucket_info;
            int r2 = store->get_bucket_info(user, bucket_handle.name, bucket_info);
            if (r2 < 0) {
                ldout(cct, 0) << "ERROR: get_bucket_info for " << bucket_handle.name << " failed: " << r2 << dendl;
                continue;
            }
            // Extract usage stats from bucket_info
            RGWUsageStats stats;
            stats.num_objects = bucket_info.num_objects;
            uint64_t used_bytes = 0;
            // Prefer actual storage size if available (size_actual or size_utilized)
            used_bytes = bucket_info.size_utilized ? bucket_info.size_utilized : bucket_info.size;
            stats.used_bytes = used_bytes;
            // Update LMDB cache and PerfCounters for this bucket
            std::string bucket_key = "bucket:" + bucket_info.owner.to_str() + "/" + bucket_info.bucket.name;
            usage_cache->put_bucket_usage(bucket_key, stats);
            // Labeled perf counter update: labels = {Bucket, User}
            std::map<std::string,std::string> labels;
            labels["Bucket"] = bucket_info.bucket.name;
            labels["User"] = bucket_info.owner.to_str();
            update_perf_counter(l_rgw_bucket_used_bytes, labels, stats.used_bytes);
            update_perf_counter(l_rgw_bucket_num_objects, labels, stats.num_objects);
            // Accumulate for user
            RGWUsageStats& agg = user_totals[bucket_info.owner.to_str()];
            agg.used_bytes += stats.used_bytes;
            agg.num_objects += stats.num_objects;
        }
    }
    // Update user-level metrics
    for (auto& kv : user_totals) {
        const std::string& user_id = kv.first;
        RGWUsageStats& total = kv.second;
        usage_cache->put_user_usage(user_id, total);
        std::map<std::string,std::string> ulabel = { {"User", user_id} };
        update_perf_counter(l_rgw_user_used_bytes, ulabel, total.used_bytes);
        update_perf_counter(l_rgw_user_num_objects, ulabel, total.num_objects);
    }
    ldout(cct, 20) << "RGWUsageManager: usage metrics refresh complete (" 
                   << user_totals.size() << " users, " << " updated)" << dendl;
}
