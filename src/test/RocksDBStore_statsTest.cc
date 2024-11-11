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

class RocksDBMetricsTest : public ::testing::Test
{
protected:
    static boost::intrusive_ptr<CephContext> cct_ptr;
    static CephContext *cct;
    static std::string admin_socket_path;
    static std::unique_ptr<rocksdb::Statistics> stats;
    RocksDBStore *store;
    std::string db_path;

    // Set up the global context once for all tests
    static void SetUpTestSuite()
    {
        // CephContext with a unique admin socket path
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
        // Define RocksDB store path
        db_path = generate_unique_db_path();

        // Remove existing RocksDB store directory
        fs::remove_all(db_path);

        // RocksDBStore with empty options and default environment
        std::map<std::string, std::string> kv_options = {
            {"rocksdb.statistics.enable", "true"},
            {"stats_enabled", "true"},
            {"enable_stats", "true"}};

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

// Initialize static members, created only once
boost::intrusive_ptr<CephContext> RocksDBMetricsTest::cct_ptr = nullptr;
CephContext *RocksDBMetricsTest::cct = nullptr;
std::string RocksDBMetricsTest::admin_socket_path = "";

TEST_F(RocksDBMetricsTest, SimpleTest)
{
    AdminSocketClient client(admin_socket_path.c_str());
    std::string message;

    // have to construct a JSON to pass into the do_request or else won't work
    json request_json;
    request_json["prefix"] = "dump_rocksdb_stats";
    request_json["level"] = "telemetry";
    std::string request = request_json.dump(); // convert to string format

    // Debug
    std::cout << "Sending JSON request: " << request << std::endl;

    // JSON command to AdminSocket
    std::string response = client.do_request(request, &message);

    cout << "Received JSON response: " << message << std::endl;

    json response_json;
    try
    {
        response_json = json::parse(message);
    }
    catch (json::parse_error &e)
    {
        FAIL() << "Failed to parse JSON response: " << e.what();
    }

    ASSERT_TRUE(response_json.contains("main")) << "Response JSON in SIMPLETEST does not contain 'main'";
    ASSERT_TRUE(response_json.contains("histogram")) << "Response JSON in SIMPLETEST does not contain 'histogram'";
    ASSERT_TRUE(response_json.contains("columns")) << "Response JSON in SIMPLETEST does not contain 'columns'";

    ASSERT_EQ(response_json["main"]["rocksdb.bytes.read"], 0);
    ASSERT_EQ(response_json["main"]["rocksdb.bytes.written"], 0);
    // ASSERT_EQ(response_json["histogram"]["rocksdb.bytes.read"], -1);
    // ASSERT_EQ(response_json["histogram"]["rocksdb.bytes.written"], -1);
    // ASSERT_EQ(response_json["main"]["rocksdb.bytes.read"], -1);
    // ASSERT_EQ(response_json["main"]["rocksdb.bytes.written"], -1);
}

// Telemetry Stats
TEST_F(RocksDBMetricsTest, DumpTelemetryStats)
{
    AdminSocketClient client(admin_socket_path.c_str());
    string message;

    // Perform write and read operations
    perform_write_operations(store, 100);
    perform_read_operations(store, 50);

    json request_json;
    request_json["prefix"] = "dump_rocksdb_stats";
    request_json["level"] = "telemetry";
    string request = request_json.dump();

    std::cout << "Sending JSON request: " << request << std::endl;

    string response = client.do_request(request, &message);

    cout << "Received JSON response: " << message << std::endl;

    json response_json;
    try
    {
        response_json = json::parse(message);
    }
    catch (json::parse_error &e)
    {
        FAIL() << "Failed to parse JSON response on TEST DumpTelemetryStats: " << e.what();
    }

    // Simple Test
    ASSERT_TRUE(response_json.contains("main")) << "Response JSON in DumpTelemetryStats does not contain 'main'";
    ASSERT_TRUE(response_json.contains("histogram")) << "Response JSON in DumpTelemetryStats does not contain 'histogram'";
    ASSERT_TRUE(response_json.contains("columns")) << "Response JSON in DumpTelemetryStats does not contain 'columns'";

    // Main Metrics
    EXPECT_GT(response_json["main"]["rocksdb.bytes.read"], 0) << "Bytes read should be greater than 0.";
    EXPECT_GT(response_json["main"]["rocksdb.bytes.written"], 0) << "Bytes written should be greater than 0.";
    EXPECT_GE(response_json["main"]["rocksdb.memtable.hit"], 0) << "Memtable hits should be >= 0.";
    EXPECT_GE(response_json["main"]["rocksdb.memtable.miss"], 0) << "Memtable misses should be >= 0.";

    // Histogram Metrics
    ASSERT_TRUE(response_json["histogram"].contains("rocksdb.db.get.micros")) << "Histogram does not contain db.get.micros";
    EXPECT_GT(response_json["histogram"]["rocksdb.db.get.micros"]["count"], 0) << "db.get.micros count should be greater than 0.";
    EXPECT_GT(response_json["histogram"]["rocksdb.db.get.micros"]["avg"], 0) << "Avg db.get.micros should be greater than 0.";

    ASSERT_TRUE(response_json["histogram"].contains("rocksdb.db.write.micros")) << "Histogram does not contain db.write.micros";
    EXPECT_GT(response_json["histogram"]["rocksdb.db.write.micros"]["count"], 0) << "db.write.micros count should be greater than 0.";
    EXPECT_GT(response_json["histogram"]["rocksdb.db.write.micros"]["avg"], 0) << "Avg db.write.micros should be greater than 0.";

    // SST Read Metrics
    ASSERT_TRUE(response_json["histogram"].contains("rocksdb.sst.read.micros")) << "Histogram does not contain sst.read.micros";
    EXPECT_GT(response_json["histogram"]["rocksdb.sst.read.micros"]["count"], 0) << "sst.read.micros count should be greater than 0.";

    // Columns Metrics
    ASSERT_TRUE(response_json["columns"].contains("all_columns")) << "Columns section does not contain 'all_columns'";
    EXPECT_EQ(response_json["columns"]["all_columns"]["sum"]["NumFiles"], 1) << "Number of SST files should be 1.";

    // verify block cache metrics
    EXPECT_GT(response_json["main"]["rocksdb.block.cache.hit"], 0) << "Block cache hits should be greater than 0.";
    EXPECT_GE(response_json["main"]["rocksdb.block.cache.miss"], 0) << "Block cache misses should be >= 0.";
}


// // Test to dump objectstore stats after performing write and read operations
// TEST_F(RocksDBMetricsTest, DumpObjectstoreStats) {
//     AdminSocketClient client(admin_socket_path.c_str());
//     std::string message;

//     // Perform write and read operations to generate metrics
//     perform_write_operations(store, 50);
//     perform_read_operations(store, 50);

//     // Request objectstore stats in JSON format
//     ASSERT_EQ("", client.do_request("{ \"prefix\": \"dump_rocksdb_stats\", \"level\": \"objectstore\", \"format\": \"json\" }", &message));

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
//     // For example, check that bytes written is greater than zero
//     if (j["stats"]["main"].contains("rocksdb.bytes.written")) {
//         EXPECT_GT(j["stats"]["main"]["rocksdb.bytes.written"], 0);
//     }
// }

// // Test to dump debug stats after performing write operations
// TEST_F(RocksDBMetricsTest, DumpDebugStats) {
//     AdminSocketClient client(admin_socket_path.c_str());
//     std::string message;

//     // Perform write operations to generate metrics
//     perform_write_operations(store, 30);

//     // Request debug stats in JSON format
//     ASSERT_EQ("", client.do_request("{ \"prefix\": \"dump_rocksdb_stats\", \"level\": \"debug\", \"format\": \"json\" }", &message));

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
//     // For example, check that compaction counts have been updated
//     if (j["stats"]["main"].contains("rocksdb.compaction.cancelled")) {
//         EXPECT_GE(j["stats"]["main"]["rocksdb.compaction.cancelled"], 0);
//     }
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