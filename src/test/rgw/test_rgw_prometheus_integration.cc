#include "rgw_exporter.h"
#include <gtest/gtest.h>
#include <regex>

// This integration test verifies that the metrics output is in Prometheus format.
TEST(RGWPrometheusIntegrationTest, MetricsFormat) {
    RGWExporter exporter;
    exporter.start();
    
    // Let the background thread update metrics
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    exporter.stop();
    std::string output = exporter.get_prometheus_metrics();
    
    // Basic regex to validate Prometheus format for our example metrics.
    // Expected format: <metric_name>{label="value",...} <number>
    std::regex prometheus_regex("ceph_rgw_bucket_usage_bytes\\{bucket=\"[^\"]+\"\\}\\s+\\d+");
    EXPECT_TRUE(std::regex_search(output, prometheus_regex))
        << "Output does not match expected Prometheus format: " << output;
}
