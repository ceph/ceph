#pragma once
#ifndef RGW_EXPORTER_H
#define RGW_EXPORTER_H

#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_user.h"
#include "common/ceph_context.h"
#include <map>
#include <string>
#include <lmdb.h>
#include  //  header for serialization functions


class RGWExporter {
public:
    explicit RGWExporter(CephContext* cct);
    ~RGWExporter();

    void start();  // Start the exporter service
    void stop();   // Stop the exporter service
    void update_metrics();  // Aggregate metrics from users and buckets
    std::string get_prometheus_metrics();  // Format and return metrics

private:
    CephContext* cct;
    std::map<std::string, uint64_t> bucket_usage_cache;
    std::map<std::string, uint64_t> user_usage_cache;
    bool stop_flag;  // Control flag to manage the execution of the background thread 
    std::thread update_thread; // Background thread dedicated to periodically fetching and updating metric related to bucket and user usage

    void fetch_bucket_metrics();
    void fetch_user_metrics();
    void update_loop();
};
