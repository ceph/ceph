#ifndef RGW_EXPORTER_H
#define RGW_EXPORTER_H

#include <thread>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <string>
#include "rgw_bucket.h"
#include "rgw_user.h"
#include "rgw_usage_cache.h"

class RGWExporter {
public:
    RGWExporter();
    ~RGWExporter();

    // Start and stop the background thread for periodic updates.
    void start();
    void stop();

    // Get metrics in Prometheus format.
    std::string get_prometheus_metrics();

private:
    // Background update loop
    void update_metrics_loop();
    // Fetch the latest bucket and user metrics and update in-memory cache.
    void update_metrics();

    // Thread management
    std::atomic<bool> stop_flag;
    std::thread update_thread;

    // Usage cache for storing metrics (LMDB-backed)
    RGWUsageCache *usage_cache;
};

#endif // RGW_EXPORTER_H
