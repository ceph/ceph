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
        RocksDBMetricsTestParams{false, "telemetry"},
        RocksDBMetricsTestParams{true, "objectstore"},
        RocksDBMetricsTestParams{false, "objectstore"},
        RocksDBMetricsTestParams{true, "debug"},
        RocksDBMetricsTestParams{false, "debug"},
        RocksDBMetricsTestParams{false, "all"},
        RocksDBMetricsTestParams{true, "all"}
        ));
        

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

        if (params.mode == "telemetry" || params.mode == "all" || params.mode == "objectstore")
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
        else if (params.mode == "debug"){
            // Validate block cache metrics
            EXPECT_EQ(main["rocksdb.block.cache.bytes.read"].get<int>(), 125984);
            EXPECT_EQ(main["rocksdb.block.cache.bytes.write"].get<int>(), 2560);


            // Validate filter cache metrics
            EXPECT_EQ(main["rocksdb.block.cache.filter.add"].get<int>(), 1);
            EXPECT_EQ(main["rocksdb.block.cache.filter.add.redundant"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.block.cache.filter.hit"].get<int>(), 50);
            EXPECT_EQ(main["rocksdb.block.cache.filter.miss"].get<int>(), 1);

            // Validate index cache metrics
            EXPECT_EQ(main["rocksdb.block.cache.index.add"].get<int>(), 1);
            EXPECT_EQ(main["rocksdb.block.cache.index.add.redundant"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.block.cache.index.bytes.evict"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.block.cache.index.bytes.insert"].get<int>(), 112);
            EXPECT_EQ(main["rocksdb.block.cache.index.hit"].get<int>(), 51);
            EXPECT_EQ(main["rocksdb.block.cache.index.miss"].get<int>(), 1);

            // Validate bloom filter metrics
            EXPECT_EQ(main["rocksdb.bloom.filter.full.positive"].get<int>(), 50);
            EXPECT_EQ(main["rocksdb.bloom.filter.full.true.positive"].get<int>(), 50);
            EXPECT_EQ(main["rocksdb.bloom.filter.micros"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.bloom.filter.prefix.checked"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.bloom.filter.prefix.useful"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.bloom.filter.useful"].get<int>(), 0);

            // Validate compaction metrics
            EXPECT_EQ(main["rocksdb.compaction.cancelled"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.compaction.key.drop.new"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.compaction.key.drop.obsolete"].get<int>(), 0);

            // Validate read/write metrics
            EXPECT_EQ(main["rocksdb.bytes.read"].get<int>(), 340);
            EXPECT_EQ(main["rocksdb.bytes.written"].get<int>(), 3480);

            // Validate WAL metrics
            EXPECT_EQ(main["rocksdb.wal.bytes"].get<int>(), 3480);
            EXPECT_EQ(main["rocksdb.wal.synced"].get<int>(), 0);

            // Validate memtable hit/miss
            EXPECT_EQ(main["rocksdb.memtable.hit"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.memtable.miss"].get<int>(), 50);

            // Validate L1 and L2+ hit rates
            EXPECT_EQ(main["rocksdb.l1.hit"].get<int>(), 50);
            EXPECT_EQ(main["rocksdb.l2andup.hit"].get<int>(), 0);

            // Validate file metrics
            EXPECT_EQ(main["rocksdb.no.file.closes"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.no.file.errors"].get<int>(), 0);
            EXPECT_EQ(main["rocksdb.no.file.opens"].get<int>(), 1);
        }
    }
    else
    {
        if (params.mode == "telemetry" || params.mode == "all" || params.mode == "objectstore")
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
        else if (params.mode == "debug"){
            // Validate blobdb metrics
            EXPECT_EQ(main["rocksdb.blobdb.num.get"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.blobdb.num.keys.read"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.blobdb.num.keys.written"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.blobdb.num.multiget"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.blobdb.num.next"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.blobdb.num.prev"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.blobdb.num.put"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.blobdb.num.seek"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.blobdb.num.write"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.blobdb.write.blob"].get<int>(), -1);

            // Validate block cache metrics
            EXPECT_EQ(main["rocksdb.block.cache.bytes.read"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.bytes.write"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.data.add"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.data.add.redundant"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.data.bytes.insert"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.data.hit"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.data.miss"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.filter.add"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.filter.add.redundant"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.filter.bytes.evict"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.filter.bytes.insert"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.filter.hit"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.filter.miss"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.index.add"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.index.add.redundant"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.index.bytes.evict"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.index.bytes.insert"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.index.hit"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.index.miss"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.block.cache.miss"].get<int>(), -1);

            // Validate bloom filter metrics
            EXPECT_EQ(main["rocksdb.bloom.filter.full.positive"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.bloom.filter.full.true.positive"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.bloom.filter.micros"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.bloom.filter.prefix.checked"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.bloom.filter.prefix.useful"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.bloom.filter.useful"].get<int>(), -1);

            // Validate bytes read/write metrics
            EXPECT_EQ(main["rocksdb.bytes.read"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.bytes.written"].get<int>(), -1);

            // Validate compaction metrics
            EXPECT_EQ(main["rocksdb.compaction.cancelled"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compaction.key.drop.new"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compaction.key.drop.obsolete"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compaction.key.drop.range_del"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compaction.key.drop.user"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compaction.optimized.del.drop.obsolete"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compaction.range_del.drop.obsolete"].get<int>(), -1);

            // Validate compact read/write metrics
            EXPECT_EQ(main["rocksdb.compact.read.bytes"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compact.read.marked.bytes"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compact.read.periodic.bytes"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compact.read.ttl.bytes"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compact.write.bytes"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compact.write.marked.bytes"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compact.write.periodic.bytes"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.compact.write.ttl.bytes"].get<int>(), -1);

            // Validate level hits and memtable stats
            EXPECT_EQ(main["rocksdb.l0.hit"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.l0.num.files.stall.micros"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.l0.slowdown.micros"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.l1.hit"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.l2andup.hit"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.memtable.compaction.micros"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.memtable.hit"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.memtable.miss"].get<int>(), -1);

            // Validate merge operation and file stats
            EXPECT_EQ(main["rocksdb.merge.operation.time.nanos"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.no.file.closes"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.no.file.errors"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.no.file.opens"].get<int>(), -1);

            // Validate WAL metrics
            EXPECT_EQ(main["rocksdb.wal.bytes"].get<int>(), -1);
            EXPECT_EQ(main["rocksdb.wal.synced"].get<int>(), -1);
        }
    }

    // Validate histogram metrics
    const auto &histogram = response_json["histogram"];

    if (params.perf_counters_enabled)
    {
        if (params.mode == "telemetry" || params.mode == "all" || params.mode == "objectstore")
        {
            ASSERT_TRUE(histogram.contains("rocksdb.db.get.micros"));
            EXPECT_GT(histogram["rocksdb.db.get.micros"]["count"].get<int>(), 0);
            EXPECT_GT(histogram["rocksdb.db.get.micros"]["avg"].get<double>(), 0.0);

            ASSERT_TRUE(histogram.contains("rocksdb.db.write.micros"));
            EXPECT_GT(histogram["rocksdb.db.write.micros"]["count"].get<int>(), 0);
            EXPECT_GT(histogram["rocksdb.db.write.micros"]["avg"].get<double>(), 0.0);

            ASSERT_TRUE(histogram.contains("rocksdb.sst.read.micros"));
            EXPECT_GT(histogram["rocksdb.sst.read.micros"]["count"].get<int>(), 0);
            EXPECT_GT(histogram["rocksdb.sst.read.micros"]["avg"].get<double>(), 0.0);
        }
        else if (params.mode == "debug"){
            ASSERT_TRUE(histogram.contains("rocksdb.db.get.micros"));
            EXPECT_GT(histogram["rocksdb.db.get.micros"]["count"].get<int>(), 0);
            EXPECT_GT(histogram["rocksdb.db.get.micros"]["avg"].get<double>(), 0.0);

            ASSERT_TRUE(histogram.contains("rocksdb.db.write.micros"));
            EXPECT_GT(histogram["rocksdb.db.write.micros"]["count"].get<int>(), 0);
            EXPECT_GT(histogram["rocksdb.db.write.micros"]["avg"].get<double>(), 0.0);

            ASSERT_TRUE(histogram.contains("rocksdb.db.flush.micros"));
            EXPECT_GT(histogram["rocksdb.db.flush.micros"]["count"].get<int>(), 0);
            EXPECT_GT(histogram["rocksdb.db.flush.micros"]["avg"].get<double>(), 0.0);

            ASSERT_TRUE(histogram.contains("rocksdb.sst.read.micros"));
            EXPECT_GT(histogram["rocksdb.sst.read.micros"]["count"].get<int>(), 0);
            EXPECT_GT(histogram["rocksdb.sst.read.micros"]["avg"].get<double>(), 0.0);

            ASSERT_TRUE(histogram.contains("rocksdb.db.multiget.micros"));
            EXPECT_EQ(histogram["rocksdb.db.multiget.micros"]["count"].get<int>(), 0);

            ASSERT_TRUE(histogram.contains("rocksdb.db.seek.micros"));
            EXPECT_EQ(histogram["rocksdb.db.seek.micros"]["count"].get<int>(), 0);

            ASSERT_TRUE(histogram.contains("rocksdb.db.write.stall"));
            EXPECT_EQ(histogram["rocksdb.db.write.stall"]["count"].get<int>(), 0);

            ASSERT_TRUE(histogram.contains("rocksdb.sst.batch.size"));
            EXPECT_EQ(histogram["rocksdb.sst.batch.size"]["count"].get<int>(), 0);

            ASSERT_TRUE(histogram.contains("rocksdb.wal.file.sync.micros"));
            EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["count"].get<int>(), 0);
        }
    }
    else
    {
        if (params.mode == "telemetry" || params.mode == "all" || params.mode == "objectstore")
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
        else if (params.mode == "debug"){

            ASSERT_TRUE(histogram.contains("rocksdb.db.get.micros"));
            EXPECT_EQ(histogram["rocksdb.db.get.micros"]["count"].get<int>(), -1);
            EXPECT_EQ(histogram["rocksdb.db.get.micros"]["avg"].get<int>(), -1);

            ASSERT_TRUE(histogram.contains("rocksdb.db.write.micros"));
            EXPECT_EQ(histogram["rocksdb.db.write.micros"]["count"].get<int>(), -1);
            EXPECT_EQ(histogram["rocksdb.db.write.micros"]["avg"].get<int>(), -1);

            ASSERT_TRUE(histogram.contains("rocksdb.db.flush.micros"));
            EXPECT_EQ(histogram["rocksdb.db.flush.micros"]["count"].get<int>(), -1);
            EXPECT_EQ(histogram["rocksdb.db.flush.micros"]["avg"].get<int>(), -1);

            ASSERT_TRUE(histogram.contains("rocksdb.db.multiget.micros"));
            EXPECT_EQ(histogram["rocksdb.db.multiget.micros"]["count"].get<int>(), -1);
            EXPECT_EQ(histogram["rocksdb.db.multiget.micros"]["avg"].get<int>(), -1);

            ASSERT_TRUE(histogram.contains("rocksdb.db.seek.micros"));
            EXPECT_EQ(histogram["rocksdb.db.seek.micros"]["count"].get<int>(), -1);
            EXPECT_EQ(histogram["rocksdb.db.seek.micros"]["avg"].get<int>(), -1);

            ASSERT_TRUE(histogram.contains("rocksdb.db.write.stall"));
            EXPECT_EQ(histogram["rocksdb.db.write.stall"]["count"].get<int>(), -1);
            EXPECT_EQ(histogram["rocksdb.db.write.stall"]["avg"].get<int>(), -1);

            ASSERT_TRUE(histogram.contains("rocksdb.sst.batch.size"));
            EXPECT_EQ(histogram["rocksdb.sst.batch.size"]["count"].get<int>(), -1);
            EXPECT_EQ(histogram["rocksdb.sst.batch.size"]["avg"].get<int>(), -1);

            ASSERT_TRUE(histogram.contains("rocksdb.sst.read.micros"));
            EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["count"].get<int>(), -1);
            EXPECT_EQ(histogram["rocksdb.sst.read.micros"]["avg"].get<int>(), -1);

            ASSERT_TRUE(histogram.contains("rocksdb.wal.file.sync.micros"));
            EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["count"].get<int>(), -1);
            EXPECT_EQ(histogram["rocksdb.wal.file.sync.micros"]["avg"].get<int>(), -1);
        }
    }

    // Validate columns metrics
    const auto &columns = response_json["columns"];

    if (params.perf_counters_enabled && (params.mode == "telemetry"))
    {
        ASSERT_TRUE(columns.contains("all_columns"));
        EXPECT_EQ(columns["all_columns"]["sum"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["all_columns"]["sum"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["sum"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["total_slowdown"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["total_stop"].get<int>(), 0);
    }
    else if (params.mode == "telemetry")
    {
        ASSERT_TRUE(columns.contains("all_columns"));
        EXPECT_EQ(columns["all_columns"]["sum"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["all_columns"]["sum"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["sum"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["total_slowdown"].get<int>(), 0);
        EXPECT_EQ(columns["all_columns"]["total_stop"].get<int>(), 0);
    }
    else if (params.perf_counters_enabled && (params.mode == "objectstore")){
        ASSERT_TRUE(columns.contains("default"));
        EXPECT_EQ(columns["default"]["sum"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["sum"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["sum"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["NumFiles"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["l1"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["total_slowdown"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["total_stop"].get<int>(), 0);
    }
    else if (params.mode == "objectstore"){
        ASSERT_TRUE(columns.contains("default"));
        EXPECT_EQ(columns["default"]["sum"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["sum"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["sum"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["NumFiles"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["l1"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["total_slowdown"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["total_stop"].get<int>(), 0);
    }
    else if (params.perf_counters_enabled && (params.mode == "debug")){
        ASSERT_TRUE(columns.contains("default"));
        EXPECT_EQ(columns["default"]["sum"]["CompCount"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["sum"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["sum"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["sum"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["sum"]["ReadGB"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["sum"]["ReadMBps"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["sum"]["WriteAmp"].get<int>(), 1);

        EXPECT_EQ(columns["default"]["l0"]["CompCount"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["l0"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["NumFiles"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["ReadGB"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["ReadMBps"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["WriteAmp"].get<int>(), 1);

        EXPECT_EQ(columns["default"]["l1"]["CompCount"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["CompMergeCPU"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["l1"]["ReadGB"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["ReadMBps"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["WriteAmp"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["WriteGB"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["WriteMBps"].get<double>(), 0);

        EXPECT_EQ(columns["default"]["level0_numfiles"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["level0_numfiles_with_compaction"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["level0_slowdown"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["level0_slowdown_with_compaction"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["slowdown_for_pending_compaction_bytes"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["stop_for_pending_compaction_bytes"].get<int>(), 0);
    }
    else if (params.mode == "debug"){
        ASSERT_TRUE(columns.contains("default"));
        EXPECT_EQ(columns["default"]["sum"]["CompCount"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["sum"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["sum"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["sum"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["sum"]["ReadGB"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["sum"]["ReadMBps"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["sum"]["WriteAmp"].get<int>(), 1);

        EXPECT_EQ(columns["default"]["l0"]["CompCount"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["l0"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["NumFiles"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["ReadGB"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["ReadMBps"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l0"]["WriteAmp"].get<int>(), 1);

        EXPECT_EQ(columns["default"]["l1"]["CompCount"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["CompMergeCPU"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["KeyIn"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["NumFiles"].get<int>(), 1);
        EXPECT_EQ(columns["default"]["l1"]["ReadGB"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["ReadMBps"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["WriteAmp"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["WriteGB"].get<double>(), 0);
        EXPECT_EQ(columns["default"]["l1"]["WriteMBps"].get<double>(), 0);

        EXPECT_EQ(columns["default"]["level0_numfiles"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["level0_numfiles_with_compaction"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["level0_slowdown"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["level0_slowdown_with_compaction"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["slowdown_for_pending_compaction_bytes"].get<int>(), 0);
        EXPECT_EQ(columns["default"]["stop_for_pending_compaction_bytes"].get<int>(), 0);
    }


    if (params.perf_counters_enabled && (params.mode == "all"))
    {
        ASSERT_TRUE(columns.contains("default"));
        const auto& default_columns = columns["default"];

        // Validate "sum" section
        const auto& sum = default_columns["sum"];
        EXPECT_GE(sum["AvgSec"].get<double>(), -1.0);
        EXPECT_EQ(sum["CompCount"].get<int>(), 1);
        EXPECT_GE(sum["CompMergeCPU"].get<double>(), -1.0);
        EXPECT_GE(sum["CompSec"].get<double>(), -1.0);
        EXPECT_EQ(sum["CompactedFiles"].get<int>(), 0);
        EXPECT_EQ(sum["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(sum["KeyIn"].get<int>(), 0);
        EXPECT_GE(sum["MovedGB"].get<double>(), -1.0);
        EXPECT_EQ(sum["NumFiles"].get<int>(), 1);
        EXPECT_DOUBLE_EQ(sum["ReadGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(sum["ReadMBps"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(sum["RnGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(sum["Rnp1GB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(sum["Score"].get<double>(), 0);
        EXPECT_EQ(sum["SizeBytes"].get<int>(), 2239);
        EXPECT_GE(sum["WnewGB"].get<double>(), -1.0);
        EXPECT_DOUBLE_EQ(sum["WriteAmp"].get<double>(), 1);
        EXPECT_GE(sum["WriteGB"].get<double>(), -1.0);
        EXPECT_GE(sum["WriteMBps"].get<double>(), -1.0);

        // Validate "l0" section
        const auto& l0 = default_columns["l0"];
        EXPECT_GE(l0["AvgSec"].get<double>(), -1.0);
        EXPECT_EQ(l0["CompCount"].get<int>(), 1);
        EXPECT_GE(l0["CompMergeCPU"].get<double>(), -1.0);
        EXPECT_GE(l0["CompSec"].get<double>(), -1.0);
        EXPECT_EQ(l0["CompactedFiles"].get<int>(), 0);
        EXPECT_EQ(l0["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(l0["KeyIn"].get<int>(), 0);
        EXPECT_DOUBLE_EQ(l0["MovedGB"].get<double>(), 0);
        EXPECT_EQ(l0["NumFiles"].get<int>(), 0);
        EXPECT_DOUBLE_EQ(l0["ReadGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l0["ReadMBps"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l0["RnGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l0["Rnp1GB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l0["Score"].get<double>(), 0);
        EXPECT_EQ(l0["SizeBytes"].get<int>(), 0);
        EXPECT_GE(l0["WnewGB"].get<double>(), -1.0);
        EXPECT_DOUBLE_EQ(l0["WriteAmp"].get<double>(), 1);
        EXPECT_GE(l0["WriteGB"].get<double>(), -1.0);
        EXPECT_GE(l0["WriteMBps"].get<double>(), -1.0);

        // Validate "l1" section
        const auto& l1 = default_columns["l1"];
        EXPECT_DOUBLE_EQ(l1["AvgSec"].get<double>(), 0);
        EXPECT_EQ(l1["CompCount"].get<int>(), 0);
        EXPECT_DOUBLE_EQ(l1["CompMergeCPU"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["CompSec"].get<double>(), 0);
        EXPECT_EQ(l1["CompactedFiles"].get<int>(), 0);
        EXPECT_EQ(l1["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(l1["KeyIn"].get<int>(), 0);
        EXPECT_GE(l1["MovedGB"].get<double>(), -1.0);
        EXPECT_EQ(l1["NumFiles"].get<int>(), 1);
        EXPECT_DOUBLE_EQ(l1["ReadGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["ReadMBps"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["RnGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["Rnp1GB"].get<double>(), 0);
        EXPECT_GE(l1["Score"].get<double>(), -1.0);
        EXPECT_EQ(l1["SizeBytes"].get<int>(), 2239);
        EXPECT_DOUBLE_EQ(l1["WnewGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["WriteAmp"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["WriteGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["WriteMBps"].get<double>(), 0);

        // Validate other metrics
        EXPECT_EQ(default_columns["level0_numfiles"].get<int>(), 0);
        EXPECT_EQ(default_columns["level0_numfiles_with_compaction"].get<int>(), 0);
        EXPECT_EQ(default_columns["level0_slowdown"].get<int>(), 0);
        EXPECT_EQ(default_columns["level0_slowdown_with_compaction"].get<int>(), 0);
        EXPECT_EQ(default_columns["memtable_compaction"].get<int>(), 0);
        EXPECT_EQ(default_columns["memtable_slowdown"].get<int>(), 0);
        EXPECT_EQ(default_columns["slowdown_for_pending_compaction_bytes"].get<int>(), 0);
        EXPECT_EQ(default_columns["stop_for_pending_compaction_bytes"].get<int>(), 0);
        EXPECT_EQ(default_columns["total_slowdown"].get<int>(), 0);
        EXPECT_EQ(default_columns["total_stop"].get<int>(), 0);
    }
    else if (params.mode == "all")
    {
        ASSERT_TRUE(columns.contains("default"));
        const auto& default_columns = columns["default"];

        // Validate "sum" section
        const auto& sum = default_columns["sum"];
        EXPECT_GE(sum["AvgSec"].get<double>(), -1.0);
        EXPECT_EQ(sum["CompCount"].get<int>(), 1);
        EXPECT_GE(sum["CompMergeCPU"].get<double>(), -1.0);
        EXPECT_GE(sum["CompSec"].get<double>(), -1.0);
        EXPECT_EQ(sum["CompactedFiles"].get<int>(), 0);
        EXPECT_EQ(sum["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(sum["KeyIn"].get<int>(), 0);
        EXPECT_GE(sum["MovedGB"].get<double>(), -1.0);
        EXPECT_EQ(sum["NumFiles"].get<int>(), 1);
        EXPECT_DOUBLE_EQ(sum["ReadGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(sum["ReadMBps"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(sum["RnGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(sum["Rnp1GB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(sum["Score"].get<double>(), 0);
        EXPECT_EQ(sum["SizeBytes"].get<int>(), 2239);
        EXPECT_GE(sum["WnewGB"].get<double>(), -1.0);
        EXPECT_DOUBLE_EQ(sum["WriteAmp"].get<double>(), 1);
        EXPECT_GE(sum["WriteGB"].get<double>(), -1.0);
        EXPECT_GE(sum["WriteMBps"].get<double>(), -1.0);

        // Validate "l0" section
        const auto& l0 = default_columns["l0"];
        EXPECT_GE(l0["AvgSec"].get<double>(), -1.0);
        EXPECT_EQ(l0["CompCount"].get<int>(), 1);
        EXPECT_GE(l0["CompMergeCPU"].get<double>(), -1.0);
        EXPECT_GE(l0["CompSec"].get<double>(), -1.0);
        EXPECT_EQ(l0["CompactedFiles"].get<int>(), 0);
        EXPECT_EQ(l0["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(l0["KeyIn"].get<int>(), 0);
        EXPECT_DOUBLE_EQ(l0["MovedGB"].get<double>(), 0);
        EXPECT_EQ(l0["NumFiles"].get<int>(), 0);
        EXPECT_DOUBLE_EQ(l0["ReadGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l0["ReadMBps"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l0["RnGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l0["Rnp1GB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l0["Score"].get<double>(), 0);
        EXPECT_EQ(l0["SizeBytes"].get<int>(), 0);
        EXPECT_GE(l0["WnewGB"].get<double>(), -1.0);
        EXPECT_DOUBLE_EQ(l0["WriteAmp"].get<double>(), 1);
        EXPECT_GE(l0["WriteGB"].get<double>(), -1.0);
        EXPECT_GE(l0["WriteMBps"].get<double>(), -1.0);

        // Validate "l1" section
        const auto& l1 = default_columns["l1"];
        EXPECT_DOUBLE_EQ(l1["AvgSec"].get<double>(), 0);
        EXPECT_EQ(l1["CompCount"].get<int>(), 0);
        EXPECT_DOUBLE_EQ(l1["CompMergeCPU"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["CompSec"].get<double>(), 0);
        EXPECT_EQ(l1["CompactedFiles"].get<int>(), 0);
        EXPECT_EQ(l1["KeyDrop"].get<int>(), 0);
        EXPECT_EQ(l1["KeyIn"].get<int>(), 0);
        EXPECT_GE(l1["MovedGB"].get<double>(), -1.0);
        EXPECT_EQ(l1["NumFiles"].get<int>(), 1);
        EXPECT_DOUBLE_EQ(l1["ReadGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["ReadMBps"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["RnGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["Rnp1GB"].get<double>(), 0);
        EXPECT_GE(l1["Score"].get<double>(), -1.0);
        EXPECT_EQ(l1["SizeBytes"].get<int>(), 2239);
        EXPECT_DOUBLE_EQ(l1["WnewGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["WriteAmp"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["WriteGB"].get<double>(), 0);
        EXPECT_DOUBLE_EQ(l1["WriteMBps"].get<double>(), 0);

        // Validate other metrics
        EXPECT_EQ(default_columns["level0_numfiles"].get<int>(), 0);
        EXPECT_EQ(default_columns["level0_numfiles_with_compaction"].get<int>(), 0);
        EXPECT_EQ(default_columns["level0_slowdown"].get<int>(), 0);
        EXPECT_EQ(default_columns["level0_slowdown_with_compaction"].get<int>(), 0);
        EXPECT_EQ(default_columns["memtable_compaction"].get<int>(), 0);
        EXPECT_EQ(default_columns["memtable_slowdown"].get<int>(), 0);
        EXPECT_EQ(default_columns["slowdown_for_pending_compaction_bytes"].get<int>(), 0);
        EXPECT_EQ(default_columns["stop_for_pending_compaction_bytes"].get<int>(), 0);
        EXPECT_EQ(default_columns["total_slowdown"].get<int>(), 0);
        EXPECT_EQ(default_columns["total_stop"].get<int>(), 0);
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