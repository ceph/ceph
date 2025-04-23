#pragma once

#include <thread>
#include <atomic>
#include <condition_variable>
#include "rgw_usage_cache.h"

class RGWUsageManager {
public:
    RGWUsageManager(CephContext *cct, RGWUsageCache *cache, PerfCounters *logger);
    ~RGWUsageManager();
    // Start the background refresh thread
    void start();
    // Signal the thread to stop and wait for it
    void stop();
    // (For testing) Update perf counters for a given metric ID and label set
    virtual void update_perf_counter(int id, const std::map<std::string, std::string>& labels, uint64_t value);

private:
    CephContext *cct;
    RGWUsageCache *usage_cache;
    PerfCounters *perf_logger;
    std::thread refresh_thread;
    std::atomic<bool> stopping;
    std::condition_variable cond;
    std::mutex cond_lock;
    unsigned refresh_interval;
    // Thread loop
    void run();
    // Refresh usage data from RGW metadata and update cache & perf counters
    void refresh_usage();
};
