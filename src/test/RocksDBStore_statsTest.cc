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

// Global Ceph
class CephGlobalEnvironment : public ::testing::Environment
{
public:
    static boost::intrusive_ptr<CephContext> cct_ptr;
    static CephContext *cct;
    static std::string admin_socket_path;

    // Set Up
    void SetUp() override
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
    // Clean up
    void TearDown() override
    {
        if (cct_ptr)
        {
            cct_ptr = nullptr;
            cct = nullptr;
        }
        fs::remove(admin_socket_path);
    }
};
boost::intrusive_ptr<CephContext> CephGlobalEnvironment::cct_ptr = nullptr;
CephContext *CephGlobalEnvironment::cct = nullptr;
std::string CephGlobalEnvironment::admin_socket_path = "";

// -------------------------
// GTEST With PARAMs
// -------------------------
class RocksDBMetricsTest : public ::testing::TestWithParam<RocksDBMetricsTestParams>
{
protected:
    RocksDBStore *store;
    std::string db_path;
    void SetUp() override
    {
        db_path = generate_unique_db_path();
        fs::remove_all(db_path);

        RocksDBMetricsTestParams params = GetParam();
        std::map<std::string, std::string> kv_options = {
            {"rocksdb.statistics.enable", params.perf_counters_enabled ? "true" : "false"}};
        void *env = nullptr;
        store = new RocksDBStore(CephGlobalEnvironment::cct, db_path, kv_options, env);
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
INSTANTIATE_TEST_SUITE_P(
    PerfCounters,
    RocksDBMetricsTest,
    ::testing::Values(
        RocksDBMetricsTestParams{true, "telemetry"},
        RocksDBMetricsTestParams{false, "telemetry"}));

TEST_P(RocksDBMetricsTest, PerfCountersAndModesBehavior)
{
    RocksDBMetricsTestParams params = GetParam();
    AdminSocketClient client(CephGlobalEnvironment::admin_socket_path.c_str());
    std::string message;

    std::cout << "[TEST] Perf Counters "
              << (params.perf_counters_enabled ? "Enabled" : "Disabled")
              << ", Mode: " << params.mode << std::endl;

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

    // Parse response
    json response_json;
    try
    {
        response_json = json::parse(message);
    }
    catch (json::parse_error &e)
    {
        FAIL() << "Failed to parse JSON response: " << e.what();
        return;
    }

    // Assert response structure
    ASSERT_TRUE(response_json.contains("main"));
    ASSERT_TRUE(response_json.contains("histogram"));
    ASSERT_TRUE(response_json.contains("columns"));

    // Validate main metrics
    const auto &main = response_json["main"];

    if (params.perf_counters_enabled)
    {
        EXPECT_EQ(main["rocksdb.bytes.read"].get<int>(), 340);
        EXPECT_EQ(main["rocksdb.bytes.written"].get<int>(), 3480);

        if (params.mode == "telemetry" || params.mode == "all")
        {
            EXPECT_EQ(main["rocksdb.memtable.hit"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.memtable.miss"].get<int>(), 50);
            EXPECT_EQ(main["rocksdb.block.cache.data.hit"].get<int>(), 49);
            EXPECT_EQ(main["rocksdb.block.cache.data.miss"].get<int>(), 1);
            EXPECT_EQ(main["rocksdb.block.cache.filter.add"].get<int>(), 1);
            EXPECT_EQ(main["rocksdb.block.cache.filter.miss"].get<int>(), 1);
            EXPECT_EQ(main["rocksdb.block.cache.hit"].get<int>(), 150);
            EXPECT_EQ(main["rocksdb.block.cache.miss"].get<int>(), 3);
        }
    }
    else
    {
        EXPECT_EQ(main["rocksdb.bytes.read"].get<int>(), -1);
        EXPECT_EQ(main["rocksdb.bytes.written"].get<int>(), -1);
        EXPECT_EQ(main["rocksdb.memtable.hit"].get<int>(), -1);
        EXPECT_EQ(main["rocksdb.memtable.miss"].get<int>(), -1);
        EXPECT_EQ(main["rocksdb.block.cache.data.hit"].get<int>(), -1);
        EXPECT_EQ(main["rocksdb.block.cache.data.miss"].get<int>(), -1);
        EXPECT_EQ(main["rocksdb.block.cache.filter.add"].get<int>(), -1);
        EXPECT_EQ(main["rocksdb.block.cache.filter.miss"].get<int>(), -1);
        EXPECT_EQ(main["rocksdb.block.cache.hit"].get<int>(), -1);
        EXPECT_EQ(main["rocksdb.block.cache.miss"].get<int>(), -1);
    }

    // Validate histogram metrics
    const auto &histogram = response_json["histogram"];

    if (params.perf_counters_enabled)
    {
        ASSERT_TRUE(histogram.contains("rocksdb.db.get.micros"));
        EXPECT_GT(histogram["rocksdb.db.get.micros"]["count"].get<int>(), 0);
        EXPECT_GT(histogram["rocksdb.db.get.micros"]["avg"].get<double>(), 0.0);

        ASSERT_TRUE(histogram.contains("rocksdb.db.write.micros"));
        EXPECT_GT(histogram["rocksdb.db.write.micros"]["count"].get<int>(), 0);
        EXPECT_GT(histogram["rocksdb.db.write.micros"]["avg"].get<double>(), 0.0);

        ASSERT_TRUE(histogram.contains("rocksdb.sst.read.micros"));
        EXPECT_GT(histogram["rocksdb.sst.read.micros"]["count"].get<int>(), 0);
        EXPECT_GT(histogram["rocksdb.sst.read.micros"]["avg"].get<int>(), 0);
    }
    else
    {
        ASSERT_TRUE(histogram.contains("rocksdb.db.get.micros"));
        EXPECT_EQ(histogram["rocksdb.db.get.micros"]["count"].get<int>(), -1);
        EXPECT_EQ(histogram["rocksdb.db.get.micros"]["avg"].get<int>(), -1);

        ASSERT_TRUE(histogram.contains("rocksdb.db.write.micros"));
        EXPECT_EQ(histogram["rocksdb.db.write.micros"]["count"].get<int>(), -1);
        EXPECT_EQ(histogram["rocksdb.db.write.micros"]["avg"].get<int>(), -1);

        ASSERT_TRUE(histogram.contains("rocksdb.sst.read.micros"));
        EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["count"].get<int>(), -1);
        EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["avg"].get<int>(), -1);
    }

    // Validate columns metrics
    const auto &columns = response_json["columns"];

    if (params.perf_counters_enabled && (params.mode == "telemetry" || params.mode == "all"))
    {
        ASSERT_TRUE(columns.contains("all_columns"));
        EXPECT_EQ(columns["all_columns"]["sum"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["all_columns"]["sum"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["sum"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["total_slowdown"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["total_stop"].get<int>(), 0);
    }
    else
    {
        ASSERT_TRUE(columns.contains("all_columns"));
        EXPECT_EQ(columns["all_columns"]["sum"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["all_columns"]["sum"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["sum"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["total_slowdown"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["total_stop"].get<int>(), 0);
    }
}

// -------------------------
// GTEST For Edge Cases
// -------------------------
class RocksDBMetricsEdgeTest : public ::testing::Test
{
protected:
    RocksDBStore *store;
    std::string db_path;
    void SetUp() override
    {
        // RocksDB store path
        db_path = generate_unique_db_path();
        fs::remove_all(db_path);

        std::map<std::string, std::string> kv_options = {
            {"rocksdb.statistics.enable", "true"}};
        void *env = nullptr;
        store = new RocksDBStore(CephGlobalEnvironment::cct, db_path, kv_options, env);
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

TEST_F(RocksDBMetricsEdgeTest, EmptyStoreMetrics)
{
    AdminSocketClient client(CephGlobalEnvironment::admin_socket_path.c_str());
    std::string message;

    std::cout << "[EDGE TEST] Empty Store Metrics" << std::endl;

    // Prepare JSON request
    json request_json;
    request_json["prefix"] = "dump_rocksdb_stats";
    request_json["level"] = "telemetry";
    std::string request = request_json.dump();

    std::cout << "Sending JSON request: " << request << std::endl;

    // Send request and capture response
    std::string response = client.do_request(request, &message);
    std::cout << "Received JSON response: " << message << std::endl;

    // Parse response
    json response_json;
    try
    {
        response_json = json::parse(message);
    }
    catch (json::parse_error &e)
    {
        FAIL() << "Failed to parse JSON response: " << e.what();
        return;
    }

    // Assert response structure
    ASSERT_TRUE(response_json.contains("main"));
    ASSERT_TRUE(response_json.contains("histogram"));
    ASSERT_TRUE(response_json.contains("columns"));

    // Validate main metrics
    const auto &main = response_json["main"];

    // BlobDB metrics
    EXPECT_EQ(main["rocksdb.blobdb.num.get"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.keys.read"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.keys.written"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.multiget"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.next"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.prev"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.put"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.seek"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.write"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.write.blob"].get<int>(), 0);

    // Bytes read/written
    EXPECT_EQ(main["rocksdb.bytes.read"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.bytes.written"].get<int>(), 0);

    // Memtable metrics
    EXPECT_EQ(main["rocksdb.memtable.hit"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.memtable.miss"].get<int>(), 0);

    // Block cache metrics
    EXPECT_EQ(main["rocksdb.block.cache.data.hit"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.block.cache.data.miss"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.block.cache.filter.add"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.block.cache.filter.miss"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.block.cache.hit"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.block.cache.miss"].get<int>(), 0);

    // Validate histogram metrics
    const auto &histogram = response_json["histogram"];

    // rocksdb.compaction.times.cpu_micros
    ASSERT_TRUE(histogram.contains("rocksdb.compaction.times.cpu_micros"));
    EXPECT_EQ(histogram["rocksdb.compaction.times.cpu_micros"]["p50"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.cpu_micros"]["p95"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.cpu_micros"]["p99"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.cpu_micros"]["avg"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.cpu_micros"]["std"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.cpu_micros"]["max"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.cpu_micros"]["count"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.cpu_micros"]["sum"].get<int>(), 0);

    // rocksdb.compaction.times.micros
    ASSERT_TRUE(histogram.contains("rocksdb.compaction.times.micros"));
    EXPECT_EQ(histogram["rocksdb.compaction.times.micros"]["p50"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.micros"]["p95"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.micros"]["p99"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.micros"]["avg"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.micros"]["std"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.micros"]["max"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.micros"]["count"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.compaction.times.micros"]["sum"].get<int>(), 0);

    // rocksdb.db.get.micros
    ASSERT_TRUE(histogram.contains("rocksdb.db.get.micros"));
    EXPECT_EQ(histogram["rocksdb.db.get.micros"]["p50"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.get.micros"]["p95"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.get.micros"]["p99"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.get.micros"]["avg"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.get.micros"]["std"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.get.micros"]["max"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.get.micros"]["count"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.get.micros"]["sum"].get<int>(), 0);

    // rocksdb.db.write.micros
    ASSERT_TRUE(histogram.contains("rocksdb.db.write.micros"));
    EXPECT_EQ(histogram["rocksdb.db.write.micros"]["p50"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.write.micros"]["p95"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.write.micros"]["p99"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.write.micros"]["avg"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.write.micros"]["std"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.write.micros"]["max"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.write.micros"]["count"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.db.write.micros"]["sum"].get<int>(), 0);

    // rocksdb.sst.read.micros
    ASSERT_TRUE(histogram.contains("rocksdb.sst.read.micros"));
    EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["p50"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["p95"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["p99"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["avg"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["std"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["max"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["count"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["sum"].get<int>(), 0);

    // rocksdb.wal.file.sync.micros
    ASSERT_TRUE(histogram.contains("rocksdb.wal.file.sync.micros"));
    EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["p50"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["p95"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["p99"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["avg"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["std"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["max"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["count"].get<int>(), 0);
    EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["sum"].get<int>(), 0);

    // Validate columns metrics
    const auto &columns = response_json["columns"];
    ASSERT_TRUE(columns.contains("all_columns"));
    const auto &all_columns = columns["all_columns"];

    // Sum metrics
    ASSERT_TRUE(all_columns.contains("sum"));
    const auto &sum = all_columns["sum"];
    EXPECT_EQ(sum["KeyDrop"].get<int>(), 0);
    EXPECT_EQ(sum["KeyIn"].get<int>(), 0);
    EXPECT_EQ(sum["NumFiles"].get<int>(), 0);

    // Total metrics
    EXPECT_EQ(all_columns["total_slowdown"].get<int>(), 0);
    EXPECT_EQ(all_columns["total_stop"].get<int>(), 0);
}

TEST_F(RocksDBMetricsEdgeTest, HighVolumeOperations)
{
    AdminSocketClient client(CephGlobalEnvironment::admin_socket_path.c_str());
    std::string message;

    std::cout << "[EDGE TEST] High Volume Operations" << std::endl;

    // Define operation counts
    const int high_write_count = 10000;
    const int high_read_count = 5000;

    // Perform high volume operations
    perform_write_operations(store, high_write_count);
    perform_read_operations(store, high_read_count);

    // Prepare JSON request
    nlohmann::json request_json;
    request_json["prefix"] = "dump_rocksdb_stats";
    request_json["level"] = "telemetry";
    std::string request = request_json.dump();

    std::cout << "Sending JSON request: " << request << std::endl;

    // Send request and capture response
    std::string response = client.do_request(request, &message);

    std::cout << "Received JSON response: " << response << std::endl;

    // Parse response
    nlohmann::json response_json;
    try
    {
        response_json = nlohmann::json::parse(message);
    }
    catch (nlohmann::json::parse_error &e)
    {
        FAIL() << "Failed to parse JSON response: " << e.what();
        return;
    }

    // Assert response structure
    ASSERT_TRUE(response_json.contains("main"));
    ASSERT_TRUE(response_json.contains("histogram"));
    ASSERT_TRUE(response_json.contains("columns"));

    // Validate main metrics
    const auto &main = response_json["main"];

    // BlobDB metrics
    EXPECT_EQ(main["rocksdb.blobdb.num.get"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.keys.read"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.keys.written"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.multiget"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.next"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.prev"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.put"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.seek"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.num.write"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.blobdb.write.blob"].get<int>(), 0);

    // Bytes read/written
    EXPECT_EQ(main["rocksdb.bytes.read"].get<int>(), 43890);
    EXPECT_EQ(main["rocksdb.bytes.written"].get<int>(), 387780);

    // Memtable metrics
    EXPECT_EQ(main["rocksdb.memtable.hit"].get<int>(), 0);
    EXPECT_EQ(main["rocksdb.memtable.miss"].get<int>(), 5000);

    // Block cache metrics
    EXPECT_EQ(main["rocksdb.block.cache.data.hit"].get<int>(), 4945);
    EXPECT_EQ(main["rocksdb.block.cache.data.miss"].get<int>(), 55);
    EXPECT_EQ(main["rocksdb.block.cache.filter.add"].get<int>(), 1);
    EXPECT_EQ(main["rocksdb.block.cache.filter.miss"].get<int>(), 1);
    EXPECT_EQ(main["rocksdb.block.cache.hit"].get<int>(), 14946);
    EXPECT_EQ(main["rocksdb.block.cache.miss"].get<int>(), 57);

    // Validate histogram metrics
    const auto &histogram = response_json["histogram"];

    // rocksdb.db.get.micros
    ASSERT_TRUE(histogram.contains("rocksdb.db.get.micros"));
    EXPECT_EQ(histogram["rocksdb.db.get.micros"]["count"].get<int>(), high_read_count);
    EXPECT_GT(histogram["rocksdb.db.get.micros"]["avg"].get<double>(), 0.0);

    // rocksdb.db.write.micros
    ASSERT_TRUE(histogram.contains("rocksdb.db.write.micros"));
    EXPECT_EQ(histogram["rocksdb.db.write.micros"]["count"].get<int>(), high_write_count);
    EXPECT_GT(histogram["rocksdb.db.write.micros"]["avg"].get<double>(), 0.0);

    // rocksdb.sst.read.micros
    ASSERT_TRUE(histogram.contains("rocksdb.sst.read.micros"));
    EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["count"].get<int>(), 60);
    EXPECT_GT(histogram["rocksdb.sst.read.micros"]["avg"].get<double>(), 0.0);

    // rocksdb.compaction.times.cpu_micros
    ASSERT_TRUE(histogram.contains("rocksdb.compaction.times.cpu_micros"));
    EXPECT_EQ(histogram["rocksdb.compaction.times.cpu_micros"]["count"].get<int>(), 0);

    // rocksdb.compaction.times.micros
    ASSERT_TRUE(histogram.contains("rocksdb.compaction.times.micros"));
    EXPECT_EQ(histogram["rocksdb.compaction.times.micros"]["count"].get<int>(), 0);

    // rocksdb.wal.file.sync.micros
    ASSERT_TRUE(histogram.contains("rocksdb.wal.file.sync.micros"));
    EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["count"].get<int>(), 0);

    // Validate columns metrics
    const auto &columns = response_json["columns"];

    // Verify 'all_columns' metrics
    ASSERT_TRUE(columns.contains("all_columns"));
    const auto &all_columns = columns["all_columns"];
    ASSERT_TRUE(all_columns.contains("sum"));
    const auto &sum = all_columns["sum"];

    EXPECT_EQ(sum["NumFiles"].get<int>(), 1);
    EXPECT_EQ(sum["KeyDrop"].get<int>(), 0);
    EXPECT_EQ(sum["KeyIn"].get<int>(), 0);

    // Validate slowdown and stop metrics
    EXPECT_EQ(all_columns["total_slowdown"].get<int>(), 0);
    EXPECT_EQ(all_columns["total_stop"].get<int>(), 0);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::AddGlobalTestEnvironment(new CephGlobalEnvironment);
    return RUN_ALL_TESTS();
}