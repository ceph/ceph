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
#include "../kv/RocksDBStore.h"
#include "include/buffer.h" 

using namespace std;

int main(int argc, char **argv) {
  map<string,string> defaults = {
    { "admin_socket", get_rand_socket_path() }
  };
  vector<const char*> args;
  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE|
                         CINIT_FLAG_NO_CCT_PERF_COUNTERS);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(RocksDBMetrics, SimpleTest){
  AdminSocketClient client(get_rand_socket_path());
  string message;
  
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"dump_rocksdb_stats\" }", &message));
  ASSERT_EQ("{}\n", message);
}

// // Helper functions
// static RocksDBStore* setup_rocksdb_store(CephContext* cct, const string& db_path) {
//   system(("rm -rf " + db_path).c_str());
//   RocksDBStore* store = new RocksDBStore(cct, db_path, "", "");
//   int r = store->open();
//   EXPECT_EQ(r, 0) << "Failed to open RocksDBStore";

//   r = store->register_stats_hook();
//   EXPECT_EQ(r, 0) << "Failed to register stats hook";

//   return store;
// }

// static void teardown_rocksdb_store(RocksDBStore* store, const string& db_path) {
//   if (store) {
//     store->unregister_stats_hook();
//     store->close();
//     delete store;
//   }
//   system(("rm -rf " + db_path).c_str());
// }

// static void perform_write_operations(RocksDBStore* store, int num_entries) {
//   for (int i = 0; i < num_entries; ++i) {
//     string key = "key" + to_string(i);
//     string value = "value" + to_string(i);
//     auto txn = store->create_transaction();
//     txn->put("default", key, bufferlist().append(value));
//     store->submit_transaction(txn);
//   }
//   store->flush();
// }

// static void perform_read_operations(RocksDBStore* store, int num_entries) {
//   for (int i = 0; i < num_entries; ++i) {
//     string key = "key" + to_string(i);
//     bufferlist bl;
//     int r = store->get("default", key, &bl);
//     EXPECT_EQ(r, 0) << "Failed to read key: " << key;
//   }
// }

// // Tests
// TEST(RocksDBMetrics, DumpTelemetryStats) {
//   AdminSocketClient client(get_rand_socket_path());
//   string db_path = "/tmp/test_rocksdb_store_telemetry";
//   RocksDBStore* store = setup_rocksdb_store(g_ceph_context, db_path);

//   perform_write_operations(store, 100);

//   string message;
//   ASSERT_EQ("", client.do_request("{ \"prefix\": \"dump_rocksdb_stats\", \"level\": \"telemetry\", \"format\": \"json\" }", &message));

//   ASSERT_FALSE(message.empty());
//   "Output should contain Main, Histogram, columns section";
//   ASSERT_NE(message.find("\"main\":"), string::npos);
//   ASSERT_NE(message.find("\"histogram\":"), string::npos); 
//   ASSERT_NE(message.find("\"columns\":"), string::npos);

//   teardown_rocksdb_store(store, db_path);
// }

// TEST(RocksDBMetrics, DumpObjectstoreStats) {
//   AdminSocketClient client(get_rand_socket_path());
//   string db_path = "/tmp/test_rocksdb_store_objectstore";
//   RocksDBStore* store = setup_rocksdb_store(g_ceph_context, db_path);

//   perform_write_operations(store, 50);
//   perform_read_operations(store, 50);

//   string message;
//   ASSERT_EQ("", client.do_request("{ \"prefix\": \"dump_rocksdb_stats\", \"level\": \"objectstore\", \"format\": \"json\" }", &message));

//   ASSERT_FALSE(message.empty());
//   "Output should contain Main, Histogram, columns section";
//   ASSERT_NE(message.find("\"main\":"), string::npos);
//   ASSERT_NE(message.find("\"histogram\":"), string::npos); 
//   ASSERT_NE(message.find("\"columns\":"), string::npos);

//   teardown_rocksdb_store(store, db_path);
// }

// TEST(RocksDBMetrics, DumpDebugStats) {
//   AdminSocketClient client(get_rand_socket_path());
//   string db_path = "/tmp/test_rocksdb_store_debug";
//   RocksDBStore* store = setup_rocksdb_store(g_ceph_context, db_path);

//   perform_write_operations(store, 30);

//   string message;
//   ASSERT_EQ("", client.do_request("{ \"prefix\": \"dump_rocksdb_stats\", \"level\": \"debug\", \"format\": \"json\" }", &message));

//   ASSERT_FALSE(message.empty());
//   "Output should contain Main, Histogram, columns section";
//   ASSERT_NE(message.find("\"main\":"), string::npos);
//   ASSERT_NE(message.find("\"histogram\":"), string::npos); 
//   ASSERT_NE(message.find("\"columns\":"), string::npos);

//   teardown_rocksdb_store(store, db_path);
// }

// TEST(RocksDBMetrics, DumpAllStats) {
//   AdminSocketClient client(get_rand_socket_path());
//   string db_path = "/tmp/test_rocksdb_store_all";
//   RocksDBStore* store = setup_rocksdb_store(g_ceph_context, db_path);

//   perform_write_operations(store, 50);
//   perform_read_operations(store, 50);

//   string message;
//   ASSERT_EQ("", client.do_request("{ \"prefix\": \"dump_rocksdb_stats\", \"level\": \"all\", \"format\": \"json\" }", &message));

//   ASSERT_FALSE(message.empty());
//   "Output should contain Main, Histogram, columns section";
//   ASSERT_NE(message.find("\"main\":"), string::npos);
//   ASSERT_NE(message.find("\"histogram\":"), string::npos); 
//   ASSERT_NE(message.find("\"columns\":"), string::npos);

//   teardown_rocksdb_store(store, db_path);
// }
