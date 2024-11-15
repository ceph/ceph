// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "include/int_types.h"
#include "include/types.h"

#include "common/admin_socket_client.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/safe_io.h"

#include "common/code_environment.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/msgr.h"
#include "gtest/gtest.h"

#include <errno.h>
#include <fcntl.h>
#include <map>
#include <poll.h>
#include <sstream>
#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "../kv/RocksDBStore.h"
#include "../kv/KeyValueHistogram.h"
#include "include/buffer.h"

#include <nlohmann/json.hpp>
#include <filesystem>

namespace fs = std::filesystem;

using namespace std;
using json = nlohmann::json;

// Helper function for SOCKETPATH
std::string generate_rand_socket_path()
{
    return "/tmp/test_admin_socket_" + to_string(getpid()) + "_" + to_string(rand()) + ".sock";
}
std::string generate_unique_db_path()
{
    return "/tmp/test_rocksdb_store_" + to_string(getpid()) + "_" + to_string(rand());
}

struct RocksDBMetricsTestParams
{
    bool perf_counters_enabled;
    std::string mode;
};

class RocksDBMetricsTest : public ::testing::TestWithParam<RocksDBMetricsTestParams>
{
protected:
    static boost::intrusive_ptr<CephContext> cct_ptr;
    static CephContext *cct;
    static std::string admin_socket_path;
    RocksDBStore *store;
    std::string db_path;

    // Set up the global context once for all tests
    static void SetUpTestSuite()
    {
        admin_socket_path = generate_rand_socket_path();
        std::map<std::string, std::string> defaults = {
            {"admin_socket", admin_socket_path}};
        std::vector<const char *> args;
        cct_ptr = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
                              CODE_ENVIRONMENT_UTILITY,
                              CINIT_FLAG_NO_DEFAULT_CONFIG_FILE |
                                  CINIT_FLAG_NO_CCT_PERF_COUNTERS);
        cct = cct_ptr.get();
        common_init_finish(g_ceph_context);
    }

    // Tear down the global context after all tests
    static void TearDownTestSuite()
    {
        if (cct_ptr)
        {
            cct_ptr = nullptr;
            cct = nullptr;
        }
        // Remove the admin socket file
        fs::remove(admin_socket_path);
    }

    void SetUp() override
    {
        // RocksDB store path
        db_path = generate_unique_db_path();
        fs::remove_all(db_path);

        RocksDBMetricsTestParams params = GetParam();
        // RocksDBStore with empty options and default environment
        std::map<std::string, std::string> kv_options = {
            {"rocksdb.statistics.enable", params.perf_counters_enabled ? "true" : "false"}};

        void *env = nullptr;

        // Create RocksDBStore instance
        store = new RocksDBStore(cct, db_path, kv_options, env);

        std::ostringstream out;
        int r = store->create_and_open(out);
        ASSERT_EQ(r, 0) << "Failed to create and open RocksDBStore: " << out.str();
    }

    void TearDown() override
    {
        if (store)
        {
            store->unregister_stats_hook();
            store->close();
            delete store;
        }

        // Remove the RocksDB store directory
        fs::remove_all(db_path);
    }

    // Helper function to Write/Read
    void perform_write_operations(RocksDBStore *store, int num_entries)
    {
        for (int i = 0; i < num_entries; ++i)
        {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            auto txn = store->get_transaction();
            bufferlist bl;
            bl.append(value);
            txn->set("default", key, bl);
            store->submit_transaction(txn);
        }
        // flushing
        store->compact();
    }
    void perform_read_operations(RocksDBStore *store, int num_entries)
    {
        for (int i = 0; i < num_entries; ++i)
        {
            std::string key = "key" + std::to_string(i);
            bufferlist bl;
            int r = store->get("default", key, &bl);
            EXPECT_EQ(r, 0) << "Failed to read key: " << key;
        }
    }
};

// Static Members
boost::intrusive_ptr<CephContext> RocksDBMetricsTest::cct_ptr = nullptr;
CephContext *RocksDBMetricsTest::cct = nullptr;
std::string RocksDBMetricsTest::admin_socket_path = "";

// Instantiate the test suite with parameters
INSTANTIATE_TEST_SUITE_P(
    PerfCounters,
    RocksDBMetricsTest,
    ::testing::Values(
        RocksDBMetricsTestParams{true, "telemetry"},
        RocksDBMetricsTestParams{false, "telemetry"}
        )
);

// Testing with Params
TEST_P(RocksDBMetricsTest, PerfCountersAndModesBehavior)
{
    RocksDBMetricsTestParams params = GetParam();

    // Instantiate AdminSocketClient
    AdminSocketClient client(admin_socket_path.c_str());
    std::string message;

    std::cout << "[TEST] Perf Counters " 
              << (params.perf_counters_enabled ? "Enabled" : "Disabled") 
              << ", Mode: " << params.mode << std::endl;

    // write and read operations for metrics
    perform_write_operations(store, 100); 
    perform_read_operations(store, 50);    

    // Prepare JSON request
    json request_json;
    request_json["prefix"] = "dump_rocksdb_stats";
    request_json["level"] = params.mode;
    std::string request = request_json.dump();

    std::cout << "Sending JSON request: " << request << std::endl;

    // Send request and capture response
    std::string response = client.do_request(request, &message);

    std::cout << "Received JSON response: " << message << std::endl;

    // Parse JSON response
    json response_json;
    try
    {
        response_json = json::parse(message);
    }
    catch (json::parse_error &e)
    {
        FAIL() << "Failed to parse JSON response on PerfCountersAndModesBehavior: " << e.what();
        return; 
    }

    // Assertions on the response structure
    ASSERT_TRUE(response_json.contains("main")) << "Response JSON does not contain 'main'";
    ASSERT_TRUE(response_json.contains("histogram")) << "Response JSON does not contain 'histogram'";
    ASSERT_TRUE(response_json.contains("columns")) << "Response JSON does not contain 'columns'";

    // -------------------------
    // === Main Metrics Assertions ===
    // -------------------------
    const auto& main = response_json["main"];

    if (params.perf_counters_enabled)
    {
        EXPECT_GT(main["rocksdb.bytes.read"].get<int>(), 0) << "'rocksdb.bytes.read' should be greater than 0.";
        EXPECT_GT(main["rocksdb.bytes.written"].get<int>(), 0) << "'rocksdb.bytes.written' should be greater than 0.";

        // Dependent On Mode
        if (params.mode == "telemetry" || params.mode == "all")
        {
            // memtable metrics
            EXPECT_EQ(main["rocksdb.memtable.hit"].get<int>(), 0) << "'rocksdb.memtable.hit' should be 0.";
            EXPECT_EQ(main["rocksdb.memtable.miss"].get<int>(), 50) << "'rocksdb.memtable.miss' should be 50.";

            // block cache metrics
            EXPECT_EQ(main["rocksdb.block.cache.data.hit"].get<int>(), 49) << "'rocksdb.block.cache.data.hit' should be 49.";
            EXPECT_EQ(main["rocksdb.block.cache.data.miss"].get<int>(), 1) << "'rocksdb.block.cache.data.miss' should be 1.";
            EXPECT_EQ(main["rocksdb.block.cache.filter.add"].get<int>(), 1) << "'rocksdb.block.cache.filter.add' should be 1.";
            EXPECT_EQ(main["rocksdb.block.cache.filter.miss"].get<int>(), 1) << "'rocksdb.block.cache.filter.miss' should be 1.";
            EXPECT_EQ(main["rocksdb.block.cache.hit"].get<int>(), 150) << "'rocksdb.block.cache.hit' should be 150.";
            EXPECT_EQ(main["rocksdb.block.cache.miss"].get<int>(), 3) << "'rocksdb.block.cache.miss' should be 3.";
        }

    }
    else
    {
        //Perf Counter disabled should be -1/0
        EXPECT_EQ(main["rocksdb.bytes.read"].get<int>(), -1) << "'rocksdb.bytes.read' should be -1.";
        EXPECT_EQ(main["rocksdb.bytes.written"].get<int>(), -1) << "'rocksdb.bytes.written' should be -1.";
        EXPECT_EQ(main["rocksdb.memtable.hit"].get<int>(), -1) << "'rocksdb.memtable.hit' should be -1.";
        EXPECT_EQ(main["rocksdb.memtable.miss"].get<int>(), -1) << "'rocksdb.memtable.miss' should be -1.";
        EXPECT_EQ(main["rocksdb.block.cache.data.hit"].get<int>(), -1) << "'rocksdb.block.cache.data.hit' should be -1.";
        EXPECT_EQ(main["rocksdb.block.cache.data.miss"].get<int>(), -1) << "'rocksdb.block.cache.data.miss' should be -1.";
        EXPECT_EQ(main["rocksdb.block.cache.filter.add"].get<int>(), -1) << "'rocksdb.block.cache.filter.add' should be -1.";
        EXPECT_EQ(main["rocksdb.block.cache.filter.miss"].get<int>(), -1) << "'rocksdb.block.cache.filter.miss' should be -1.";
        EXPECT_EQ(main["rocksdb.block.cache.hit"].get<int>(), -1) << "'rocksdb.block.cache.hit' should be -1.";
        EXPECT_EQ(main["rocksdb.block.cache.miss"].get<int>(), -1) << "'rocksdb.block.cache.miss' should be -1.";
    }

    // -------------------------
    // === Histogram Metrics Assertions ===
    // -------------------------
    const auto& histogram = response_json["histogram"];

    if (params.perf_counters_enabled)
    {
        // rocksdb.db.get.micros
        ASSERT_TRUE(histogram.contains("rocksdb.db.get.micros")) << "Histogram does not contain 'rocksdb.db.get.micros'";
        EXPECT_GT(histogram["rocksdb.db.get.micros"]["count"].get<int>(), 0) 
            << "'rocksdb.db.get.micros.count' should be greater than 0.";
        EXPECT_GT(histogram["rocksdb.db.get.micros"]["avg"].get<double>(), 0.0) 
            << "'rocksdb.db.get.micros.avg' should be greater than 0.0.";

        // rocksdb.db.write.micros
        ASSERT_TRUE(histogram.contains("rocksdb.db.write.micros")) << "Histogram does not contain 'rocksdb.db.write.micros'";
        EXPECT_GT(histogram["rocksdb.db.write.micros"]["count"].get<int>(), 0) 
            << "'rocksdb.db.write.micros.count' should be greater than 0.";
        EXPECT_GT(histogram["rocksdb.db.write.micros"]["avg"].get<double>(), 0.0) 
            << "'rocksdb.db.write.micros.avg' should be greater than 0.0.";

        // rocksdb.sst.read.micros
        ASSERT_TRUE(histogram.contains("rocksdb.sst.read.micros")) << "Histogram does not contain 'rocksdb.sst.read.micros'";
        EXPECT_GT(histogram["rocksdb.sst.read.micros"]["count"].get<int>(), 0) 
            << "'rocksdb.sst.read.micros.count' should be greater than 0.";
        EXPECT_GT(histogram["rocksdb.sst.read.micros"]["avg"].get<int>(), 0) 
            << "'rocksdb.sst.read.micros.avg' should be greater than 0.";

        // Optionally, verify other histograms if needed
    }
    else
    {
        //Perf Counter disabled should be -1/0
        // rocksdb.db.get.micros
        ASSERT_TRUE(histogram.contains("rocksdb.db.get.micros")) << "Histogram does not contain 'rocksdb.db.get.micros'";
        EXPECT_EQ(histogram["rocksdb.db.get.micros"]["count"].get<int>(), -1) 
            << "'rocksdb.db.get.micros.count' should be -1.";
        EXPECT_EQ(histogram["rocksdb.db.get.micros"]["avg"].get<int>(), -1) 
            << "'rocksdb.db.get.micros.avg' should be -1.";

        // rocksdb.db.write.micros
        ASSERT_TRUE(histogram.contains("rocksdb.db.write.micros")) << "Histogram does not contain 'rocksdb.db.write.micros'";
        EXPECT_EQ(histogram["rocksdb.db.write.micros"]["count"].get<int>(), -1) 
            << "'rocksdb.db.write.micros.count' should be -1.";
        EXPECT_EQ(histogram["rocksdb.db.write.micros"]["avg"].get<int>(), -1) 
            << "'rocksdb.db.write.micros.avg' should be -1.";

        // rocksdb.sst.read.micros
        ASSERT_TRUE(histogram.contains("rocksdb.sst.read.micros")) << "Histogram does not contain 'rocksdb.sst.read.micros'";
        EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["count"].get<int>(), -1) 
            << "'rocksdb.sst.read.micros.count' should be -1.";
        EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["avg"].get<int>(), -1) 
            << "'rocksdb.sst.read.micros.avg' should be -1.";
    }

    // -------------------------
    // === Columns Metrics Assertions ===
    // -------------------------
    const auto& columns = response_json["columns"];

    if (params.perf_counters_enabled)
    {
        if (params.mode == "telemetry" || params.mode == "all")
        {
            // all_columns.sum.NumFiles 
            ASSERT_TRUE(columns.contains("all_columns")) << "Columns section does not contain 'all_columns'";
            EXPECT_EQ(columns["all_columns"]["sum"]["NumFiles"].get<int>(), 1) 
                << "'columns.all_columns.sum.NumFiles' should be 1.";
            EXPECT_EQ(columns["all_columns"]["sum"]["KeyDrop"].get<int>(), 0) 
                << "'columns.all_columns.sum.KeyDrop' should be 0.";
            EXPECT_EQ(columns["all_columns"]["sum"]["KeyIn"].get<int>(), 0) 
                << "'columns.all_columns.sum.KeyIn' should be 0.";

            // total_slowdown and total_stop
            EXPECT_EQ(columns["all_columns"]["total_slowdown"].get<int>(), 0) 
                << "'columns.all_columns.total_slowdown' should be 0.";
            EXPECT_EQ(columns["all_columns"]["total_stop"].get<int>(), 0) 
                << "'columns.all_columns.total_stop' should be 0.";
        }

    }
    else
    {
        // When performance counters are disabled, 'all_columns.sum.NumFiles' should still be 1
        ASSERT_TRUE(columns.contains("all_columns")) << "Columns section does not contain 'all_columns'";
        EXPECT_EQ(columns["all_columns"]["sum"]["NumFiles"].get<int>(), 1) 
            << "'columns.all_columns.sum.NumFiles' should be 1.";

        // Other columns metrics should have -1 or 0 if present
        // Since in provided JSON with perf_counters off, 'KeyDrop' and 'KeyIn' are 0
        EXPECT_EQ(columns["all_columns"]["sum"]["KeyDrop"].get<int>(), 0) 
            << "'columns.all_columns.sum.KeyDrop' should be 0.";
        EXPECT_EQ(columns["all_columns"]["sum"]["KeyIn"].get<int>(), 0) 
            << "'columns.all_columns.sum.KeyIn' should be 0.";

        // Verify total_slowdown and total_stop
        EXPECT_EQ(columns["all_columns"]["total_slowdown"].get<int>(), 0) 
            << "'columns.all_columns.total_slowdown' should be 0.";
        EXPECT_EQ(columns["all_columns"]["total_stop"].get<int>(), 0) 
            << "'columns.all_columns.total_stop' should be 0.";
    }
}


// Test to dump objectstore stats after performing write and read operations
// TEST_F(RocksDBMetricsTest, DumpObjectstoreStats) {
//     AdminSocketClient client(admin_socket_path.c_str());
//     string message;

//     // Perform write and read operations
//     perform_write_operations(store, 150);
//     perform_read_operations(store, 50);

//     json request_json;
//     request_json["prefix"] = "dump_rocksdb_stats";
//     request_json["level"] = "objectstore";
//     string request = request_json.dump();

//     std::cout << "Sending JSON request: " << request << std::endl;

//     string response = client.do_request(request, &message);

//     cout << "Received JSON response: " << message << std::endl;

//     json response_json;
//     try
//     {
//         response_json = json::parse(message);
//     }
//     catch (json::parse_error &e)
//     {
//         FAIL() << "Failed to parse JSON response on TEST DumpObjectstoreStats: " << e.what();
//     }

//     // Simple Test
//     ASSERT_TRUE(response_json.contains("main")) << "Response JSON in DumpObjectstoreStats does not contain 'main'";
//     ASSERT_TRUE(response_json.contains("histogram")) << "Response JSON in DumpObjectStoreStats does not contain 'histogram'";
//     ASSERT_TRUE(response_json.contains("columns")) << "Response JSON in DumpObjectStoreStats does not contain 'columns'";

//     // Main Metrics
//     EXPECT_GT(response_json["main"]["rocksdb.bytes.read"], 0) << "Bytes read should be greater than 0.";
//     EXPECT_GT(response_json["main"]["rocksdb.bytes.written"], 0) << "Bytes written should be greater than 0.";
//     EXPECT_GE(response_json["main"]["rocksdb.memtable.hit"], 0) << "Memtable hits should be >= 0.";
//     EXPECT_GE(response_json["main"]["rocksdb.memtable.miss"], 0) << "Memtable misses should be >= 0.";

//     // Histogram Metrics
//     ASSERT_TRUE(response_json["histogram"].contains("rocksdb.db.get.micros")) << "Histogram does not contain db.get.micros";
//     EXPECT_GT(response_json["histogram"]["rocksdb.db.get.micros"]["count"], 0) << "db.get.micros count should be greater than 0.";
//     EXPECT_GT(response_json["histogram"]["rocksdb.db.get.micros"]["avg"], 0) << "Avg db.get.micros should be greater than 0.";

//     ASSERT_TRUE(response_json["histogram"].contains("rocksdb.db.write.micros")) << "Histogram does not contain db.write.micros";
//     EXPECT_GT(response_json["histogram"]["rocksdb.db.write.micros"]["count"], 0) << "db.write.micros count should be greater than 0.";
//     EXPECT_GT(response_json["histogram"]["rocksdb.db.write.micros"]["avg"], 0) << "Avg db.write.micros should be greater than 0.";

//     // SST Read Metrics
//     ASSERT_TRUE(response_json["histogram"].contains("rocksdb.sst.read.micros")) << "Histogram does not contain sst.read.micros";
//     EXPECT_GT(response_json["histogram"]["rocksdb.sst.read.micros"]["count"], 0) << "sst.read.micros count should be greater than 0.";
//     EXPECT_GT(response_json["histogram"]["rocksdb.sst.read.micros"]["avg"], 0) << "Avg sst.read.micros should be greater than 0.";

//     // Columns Metrics
//     ASSERT_TRUE(response_json["columns"].contains("default")) << "Columns section does not contain 'default'";
//     EXPECT_EQ(response_json["columns"]["default"]["sum"]["NumFiles"], 1) << "Number of SST files should be 1.";
//     EXPECT_EQ(response_json["columns"]["default"]["l0"]["NumFiles"], 0) << "Number of SST files should be 0.";
//     EXPECT_EQ(response_json["columns"]["default"]["l1"]["NumFiles"], 1) << "Number of SST files should be 1.";

//     // verify block cache metrics
//     EXPECT_GT(response_json["main"]["rocksdb.block.cache.hit"], 0) << "Block cache hits should be greater than 0.";
//     EXPECT_GE(response_json["main"]["rocksdb.block.cache.miss"], 0) << "Block cache misses should be >= 0.";
// }

// Test to dump debug stats after performing write operations
// TEST_F(RocksDBMetricsTest, DumpDebugStats) {
// AdminSocketClient client(admin_socket_path.c_str());
//     string message;

//     // Perform write and read operations
//     perform_write_operations(store, 150);
//     perform_read_operations(store, 50);

//     json request_json;
//     request_json["prefix"] = "dump_rocksdb_stats";
//     request_json["level"] = "debug";
//     string request = request_json.dump();

//     std::cout << "Sending JSON request: " << request << std::endl;

//     string response = client.do_request(request, &message);

//     cout << "Received JSON response: " << message << std::endl;

//     json response_json;
//     try
//     {
//         response_json = json::parse(message);
//     }
//     catch (json::parse_error &e)
//     {
//         FAIL() << "Failed to parse JSON response on TEST DumpDebugStats: " << e.what();
//     }

//     // Simple Test
//     ASSERT_TRUE(response_json.contains("main")) << "Response JSON in DumpDebugStats does not contain 'main'";
//     ASSERT_TRUE(response_json.contains("histogram")) << "Response JSON in DumpDebugStats does not contain 'histogram'";
//     ASSERT_TRUE(response_json.contains("columns")) << "Response JSON in DumpDebugStats does not contain 'columns'";
// }

// // Test to dump all stats after performing write and read operations
// TEST_F(RocksDBMetricsTest, DumpAllStats) {
//     AdminSocketClient client(admin_socket_path.c_str());
//     std::string message;

//     // Perform write and read operations to generate metrics
//     perform_write_operations(store, 50);
//     perform_read_operations(store, 50);

//     // Request all stats in JSON format
//     ASSERT_EQ("", client.do_request("{ \"prefix\": \"dump_rocksdb_stats\", \"level\": \"all\", \"format\": \"json\" }", &message));

//     // Ensure the message is not empty
//     ASSERT_FALSE(message.empty());

//     // Parse the JSON response
//     json j;
//     try {
//         j = json::parse(message);
//     } catch (json::parse_error& e) {
//         FAIL() << "JSON parsing failed: " << e.what();
//     }

//     // Verify that the main, histogram, and columns sections exist
//     ASSERT_TRUE(j.contains("stats"));
//     ASSERT_TRUE(j["stats"].contains("main"));
//     ASSERT_TRUE(j["stats"].contains("histogram"));
//     ASSERT_TRUE(j["stats"].contains("columns"));

//     // Optionally, verify specific metric values
//     // For example, check that bytes read and written are greater than zero
//     if (j["stats"]["main"].contains("rocksdb.bytes.read")) {
//         EXPECT_GT(j["stats"]["main"]["rocksdb.bytes.read"], 0);
//     }
//     if (j["stats"]["main"].contains("rocksdb.bytes.written")) {
//         EXPECT_GT(j["stats"]["main"]["rocksdb.bytes.written"], 0);
//     }
// }

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}