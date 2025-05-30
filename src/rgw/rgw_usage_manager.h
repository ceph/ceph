#pragma once

#include <thread>
#include <atomic>
#include <condition_variable>
#include <map>
#include <string>
#include "rgw_usage_cache.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"

// RGWUsageManager periodically refreshes usage metrics from RGWRados and
// stores them in LMDB (via RGWUsageCache) and Prometheus PerfCounters.
class RGWUsageManager {
public:
    RGWUsageManager(CephContext *cct, RGWUsageCache *cache, PerfCounters *logger);
    ~RGWUsageManager();

    // Start the background refresh thread
    void start();

    // Signal the thread to stop and wait for it to join
    void stop();

    // Virtual hook (used for tests/mocking): updates a single perf counter
    virtual void update_perf_counter(int id,
                                     const std::map<std::string, std::string>& labels,
                                     uint64_t value);

private:
    CephContext *cct;
    RGWUsageCache *usage_cache;
    PerfCounters *perf_logger;

    std::thread refresh_thread;
    std::atomic<bool> stopping;
    std::condition_variable cond;
    std::mutex cond_lock;

    unsigned refresh_interval;  // in seconds, from config

    // Thread entrypoint
    void run();

    // Refreshes bucket/user usage stats from RGWRados and writes to LMDB + perfcounters
    void refresh_usage();
};
