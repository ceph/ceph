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

using namespace std;
using json = nlohmann::json;

std::string generate_rand_socket_path() {
    return "/tmp/test_admin_socket_" + to_string(rand()) + ".sock";
}

// Fixture for RocksDBStore tests
class RocksDBMetricsTest : public ::testing::Test {
protected:
    boost::intrusive_ptr<CephContext> cct_ptr;
    CephContext* cct;
    RocksDBStore* store;
    std::string db_path;
    std::string admin_socket_path;

    // Setup before each test
    void SetUp() override {
        // Initialize CephContext with a unique admin socket path
        admin_socket_path = generate_rand_socket_path();
        map<string, string> defaults = {
            { "admin_socket", admin_socket_path }
        };
        vector<const char*> args;
        cct_ptr = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_UTILITY,
                             CINIT_FLAG_NO_DEFAULT_CONFIG_FILE|
                             CINIT_FLAG_NO_CCT_PERF_COUNTERS);
        cct = cct_ptr.get();
        common_init_finish(g_ceph_context);

        // Define the RocksDB store path
        db_path = "/tmp/test_rocksdb_store_" + to_string(rand());

        // Initialize RocksDBStore with empty options and default environment
        map<string, string> kv_options; 
        void* env = nullptr; 

        // Remove any existing RocksDB store directory
        (void)system(("rm -rf " + db_path).c_str());

        // Create RocksDBStore instance
        store = new RocksDBStore(cct, db_path, kv_options, env);
        
        // Open the RocksDBStore with an output stream to capture any messages
        std::ostringstream out;
        int r = store->open(out, ""); 
        ASSERT_EQ(r, 0) << "Failed to open RocksDBStore: " << out.str();

        // Register the stats hook to enable metrics dumping
        r = store->register_stats_hook();
        ASSERT_EQ(r, 0) << "Failed to register stats hook";
    }

    // Teardown after each test
    void TearDown() override {
        if (store) {
            store->unregister_stats_hook();
            store->close();
            delete store;
        }

        // Remove the RocksDB store directory
        (void)system(("rm -rf " + db_path).c_str());

        // Remove the admin socket file
        unlink(admin_socket_path.c_str());
    }

    // Helper function to perform write operations
    void perform_write_operations(RocksDBStore* store, int num_entries) {
        for (int i = 0; i < num_entries; ++i) {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            auto txn = store->get_transaction(); 
            bufferlist bl;
            bl.append(value);
            txn->set("default", key, bl); 
            store->submit_transaction(txn);
        }
        // flushign writes
        store->compact();
    }

    // Helper function to perform read operations
    void perform_read_operations(RocksDBStore* store, int num_entries) {
        for (int i = 0; i < num_entries; ++i) {
            std::string key = "key" + std::to_string(i);
            bufferlist bl;
            int r = store->get("default", key, &bl);
            EXPECT_EQ(r, 0) << "Failed to read key: " << key;
        }
    }
};

// Test to verify that initially, no metrics are present
TEST_F(RocksDBMetricsTest, SimpleTest){
    AdminSocketClient client(admin_socket_path.c_str());
    std::string message;
    
    ASSERT_EQ("", client.do_request("{ \"prefix\": \"dump_rocksdb_stats\" }", &message));
    ASSERT_EQ("{}\n", message); 
}

// // Test to dump telemetry stats after performing write operations
// TEST_F(RocksDBMetricsTest, DumpTelemetryStats) {
//     AdminSocketClient client(admin_socket_path.c_str());
//     std::string message;

//     // Perform write operations to generate metrics
//     perform_write_operations(store, 100);

//     // Request telemetry stats in JSON format
//     ASSERT_EQ("", client.do_request("{ \"prefix\": \"dump_rocksdb_stats\", \"level\": \"telemetry\", \"format\": \"json\" }", &message));

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
//     // For example, check that bytes read is greater than zero
//     if (j["stats"]["main"].contains("rocksdb.bytes.read")) {
//         EXPECT_GT(j["stats"]["main"]["rocksdb.bytes.read"], 0);
//     }
// }

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

int main(int argc, char **argv) {
    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
