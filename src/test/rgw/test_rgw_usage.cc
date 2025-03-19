#include "rgw_usage_cache.h"
#include "rgw_exporter.h"
#include <gtest/gtest.h>

// Test that the usage cache updates and retrieves bucket metrics correctly.
TEST(RGWUsageCacheTest, BucketUsageUpdateAndRetrieve) {
    RGWUsageCache usage_cache("/tmp/rgw_test_lmdb");
    usage_cache.update_bucket_usage("test_bucket", 4096);
    uint64_t usage = usage_cache.get_bucket_usage("test_bucket");
    EXPECT_EQ(usage, 4096);

    // Simulate flush to LMDB and re-read (if applicable).
    EXPECT_TRUE(usage_cache.flush_to_lmdb());
}

// Test that the usage cache updates and retrieves user metrics correctly.
TEST(RGWUsageCacheTest, UserUsageUpdateAndRetrieve) {
    RGWUsageCache usage_cache("/tmp/rgw_test_lmdb");
    usage_cache.update_user_usage("test_user", 8192);
    uint64_t usage = usage_cache.get_user_usage("test_user");
    EXPECT_EQ(usage, 8192);

    EXPECT_TRUE(usage_cache.flush_to_lmdb());
}

// Test the RGWExporter functionality.
TEST(RGWExporterTest, PrometheusMetricsOutput) {
    RGWExporter exporter;
    // Start the exporter so the background thread runs (simulate one update cycle).
    exporter.start();
    
    // Simulate a brief wait for update cycle (in production, the update interval is 30 seconds; here we simulate).
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // Stop the exporter to finish the test cleanly.
    exporter.stop();
    
    // Retrieve the formatted Prometheus metrics.
    std::string metrics = exporter.get_prometheus_metrics();
    // Check that the expected metric strings appear.
    EXPECT_NE(metrics.find("ceph_rgw_bucket_usage_bytes{bucket=\"bucket1\"}"), std::string::npos);
    EXPECT_NE(metrics.find("ceph_rgw_user_usage_bytes{user=\"user1\"}"), std::string::npos);
}
