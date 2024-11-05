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

// Helper function to wait for AdminSocket readiness
bool wait_for_admin_socket(const std::string &path, int timeout_ms = 1000)
{
    int fd;
    int elapsed = 0;
    int interval = 100; 
    while (elapsed < timeout_ms)
    {
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd >= 0)
        {
            struct sockaddr_un addr;
            memset(&addr, 0, sizeof(addr));
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
            if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0)
            {
                close(fd);
                return true;
            }
            close(fd);
        }
        usleep(interval * 1000); 
        elapsed += interval;
    }
    return false;
}

class RocksDBMetricsTest : public ::testing::Test
{
protected:
    boost::intrusive_ptr<CephContext> cct_ptr;
    CephContext *cct;
    RocksDBStore *store;
    std::string db_path;
    std::string admin_socket_path;

    void SetUp() override
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

        // Define RocksDB store path
        db_path = generate_unique_db_path();

        // Remove existing RocksDB store directory
        fs::remove_all(db_path);

        // RocksDBStore with empty options and default environment
        std::map<std::string, std::string> kv_options; 
        void *env = nullptr;                           

        // Create RocksDBStore instance
        store = new RocksDBStore(cct, db_path, kv_options, env);

        std::ostringstream out;
        int r = store->create_and_open(out); 
        ASSERT_EQ(r, 0) << "Failed to create and open RocksDBStore: " << out.str();

        // Ensure AdminSocket is ready before registering the stats hook
        ASSERT_TRUE(wait_for_admin_socket(admin_socket_path)) << "AdminSocket not ready in time";
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

        // Remove the admin socket file
        fs::remove(admin_socket_path);
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

TEST_F(RocksDBMetricsTest, SimpleTest){
    AdminSocketClient client(admin_socket_path.c_str());
    std::string message;
    
    //have to construct a JSON to pass into the do_request or else won't work
    json request_json;
    request_json["prefix"] = "dump_rocksdb_stats";
    request_json["level"] = "telemetry";
    std::string request = request_json.dump();

    //Debug
    std::cout << "Sending JSON request: " << request << std::endl;
    
    // JSON command to AdminSocket
    std::string response = client.do_request(request, &message);
    
    cout << "Received JSON response: " << message << std::endl;

    json response_json;
    try {
        response_json = json::parse(message);
    } catch (json::parse_error& e) {
        FAIL() << "Failed to parse JSON response: " << e.what();
    }

    ASSERT_TRUE(response_json.contains("main")) << "Response JSON does not contain 'main'";
    ASSERT_TRUE(response_json.contains("histogram")) << "Response JSON does not contain 'histogram'";
    ASSERT_TRUE(response_json.contains("columns")) << "Response JSON does not contain 'columns'";

    // ASSERT_TRUE(response_json["main"].contains("rocksdb.bytes.read")) << "'rocksdb.bytes.read' not found in 'main'";
    // ASSERT_EQ(response_json["main"]["rocksdb.bytes.read"], -1) << "'rocksdb.bytes.read' is negative";

    // ASSERT_EQ(response_json["main"]["rocksdb.bytes.written"], -1) << "'rocksdb.bytes.written' should be zero";
}

// Telemetry Stats
// TEST_F(RocksDBMetricsTest, DumpTelemetryStats) {
//     AdminSocketClient client(admin_socket_path.c_str());
//     std::string message;
    
//     //write a operations on the key/value pair for ROCKSDB
//     perform_write_operations(store, 100);
    
//     json request_json;
//     request_json["prefix"] = "dump_rocksdb_stats";
//     request_json["level"] = "telemetry";
    
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

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
