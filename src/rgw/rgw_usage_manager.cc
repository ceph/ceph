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
    delete usage_cache;
}

void RGWUsageManager::start() {
    refresh_thread = std::thread(&RGWUsageManager::run, this);
}

void RGWUsageManager::stop() {
    bool expected = false;
    if (!stopping.compare_exchange_strong(expected, true)) {
        return;
    }
    {
        std::lock_guard<std::mutex> l(cond_lock);
        cond.notify_one();
    }
    if (refresh_thread.joinable()) {
        refresh_thread.join();
    }
}

void RGWUsageManager::run() {
    ceph_pthread_setname(pthread_self(), "rgw-usage-ref");
    refresh_usage();
    std::unique_lock<std::mutex> lk(cond_lock);
    while (!stopping.load()) {
        if (cond.wait_for(lk, ceph::make_timespan(refresh_interval), [this]() { return stopping.load(); })) {
            break;
        }
        lk.unlock();
        refresh_usage();
        lk.lock();
    }
    ldout(cct, 10) << "RGWUsageManager thread exiting" << dendl;
}

void RGWUsageManager::update_perf_counter(int id, const std::map<std::string, std::string>& labels, uint64_t value) {
    perf_logger->set(label_counter(id, labels), value);
}

void RGWUsageManager::refresh_usage() {
    ldout(cct, 20) << "RGWUsageManager: refreshing usage metrics..." << dendl;

    if (!usage_cache) {
        ldout(cct, 0) << "ERROR: usage_cache not initialized" << dendl;
        return;
    }

    RGWRados *store = g_rgw_store;
    if (!store) {
        ldout(cct, 0) << "ERROR: RGWRados store is null, cannot refresh usage" << dendl;
        return;
    }

    usage_cache->clear();

    std::map<std::string, RGWUsageStats> user_totals;
    std::vector<std::string> users;

    int ret = store->list_users(users);
    if (ret < 0) {
        ldout(cct, 0) << "ERROR: list_users failed: " << ret << dendl;
        return;
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

            RGWUsageStats stats;
            stats.num_objects = bucket_info.num_objects;
            stats.used_bytes = bucket_info.size_utilized ? bucket_info.size_utilized : bucket_info.size;

            std::string bucket_key = "bucket:" + bucket_info.owner.to_str() + "/" + bucket_info.bucket.name;
            usage_cache->put_bucket_stats(bucket_key, stats);

            std::map<std::string, std::string> labels = {
                {"bucket", bucket_info.bucket.name},
                {"user", bucket_info.owner.to_str()}
            };
            update_perf_counter(l_rgw_bucket_used_bytes, labels, stats.used_bytes);
            update_perf_counter(l_rgw_bucket_num_objects, labels, stats.num_objects);

            RGWUsageStats& agg = user_totals[bucket_info.owner.to_str()];
            agg.used_bytes += stats.used_bytes;
            agg.num_objects += stats.num_objects;
        }
    }

    for (auto& kv : user_totals) {
        const std::string& user_id = kv.first;
        RGWUsageStats& total = kv.second;

        usage_cache->put_user_stats(user_id, total);
        std::map<std::string, std::string> ulabel = { {"user", user_id} };
        update_perf_counter(l_rgw_user_used_bytes, ulabel, total.used_bytes);
        update_perf_counter(l_rgw_user_num_objects, ulabel, total.num_objects);
    }

    ldout(cct, 20) << "RGWUsageManager: usage metrics refresh complete ("
                   << user_totals.size() << " users updated)" << dendl;
}
