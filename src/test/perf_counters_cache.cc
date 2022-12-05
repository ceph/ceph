#include "include/int_types.h"
#include "include/types.h" // FIXME: ordering shouldn't be important, but right 
                           // now, this include has to come before the others.


#include "common/perf_counters_collection.h"
#include "common/perf_counters_cache.h"
#include "common/admin_socket_client.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/safe_io.h"

#include "common/code_environment.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/msgr.h" // for CEPH_ENTITY_TYPE_CLIENT
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
#include <thread>

#include "common/common_init.h"

using namespace std;

int main(int argc, char **argv) {
  map<string,string> defaults = {
    { "admin_socket", get_rand_socket_path() }
  };
  std::vector<const char*> args;
  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE|
			 CINIT_FLAG_NO_CCT_PERF_COUNTERS);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

enum {
  TEST_PERFCOUNTERS1_ELEMENT_FIRST = 200,
  TEST_PERFCOUNTERS_COUNTER,
  TEST_PERFCOUNTERS_TIME,
  TEST_PERFCOUNTERS_TIME_AVG,
  TEST_PERFCOUNTERS1_ELEMENT_LAST,
};

std::string sd(const char *c)
{
  std::string ret(c);
  std::string::size_type sz = ret.size();
  for (std::string::size_type i = 0; i < sz; ++i) {
    if (ret[i] == '\'') {
      ret[i] = '\"';
    }
  }
  return ret;
}

void add_test_counters(PerfCountersBuilder *pcb) {
  pcb->add_u64(TEST_PERFCOUNTERS_COUNTER, "test_counter");
  pcb->add_time(TEST_PERFCOUNTERS_TIME, "test_time");
  pcb->add_time_avg(TEST_PERFCOUNTERS_TIME_AVG, "test_time_avg");
}


static PerfCountersCache* setup_test_perf_counters_cache(CephContext *cct, bool eviction = false, uint64_t target_size = 100)
{
  std::string base_counters_name = "test_base_counters";
  return new PerfCountersCache(cct, eviction, target_size, TEST_PERFCOUNTERS1_ELEMENT_FIRST, TEST_PERFCOUNTERS1_ELEMENT_LAST, add_test_counters, base_counters_name);
}

void cleanup_test(PerfCountersCache *pcc) {
  pcc->clear_cache();
  delete pcc;
}

TEST(PerfCountersCache, NoCacheTest) {
  AdminSocketClient client(get_rand_socket_path());
  std::string message;
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf dump\" }", &message));
  ASSERT_EQ("{}\n", message);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf schema\" }", &message));
  ASSERT_EQ("{}\n", message);
}

TEST(PerfCountersCache, AddLabel) {
  PerfCountersCache *pcc = setup_test_perf_counters_cache(g_ceph_context);
  size_t cache_size = pcc->get_cache_size();
  ASSERT_EQ(cache_size, 0);

  std::string label = "testlabel1";
  pcc->add(label);
  cache_size = pcc->get_cache_size();
  ASSERT_EQ(cache_size, 1);

  std::string label2 = "testlabel2";
  std::string label3 = "testlabel3";
  pcc->add(label2);
  pcc->add(label3);
  cache_size = pcc->get_cache_size();
  ASSERT_EQ(cache_size, 3);
  cleanup_test(pcc);
}

TEST(PerfCountersCache, TestEviction) {
  PerfCountersCache *pcc = setup_test_perf_counters_cache(g_ceph_context, true, 4);
  std::string label1 = "testlabel1";
  std::string label2 = "testlabel2";
  std::string label3 = "testlabel3";
  std::string label4 = "testlabel4";
  std::string label5 = "testlabel5";
  std::string label6 = "testlabel6";

  pcc->add(label1);
  pcc->add(label2);
  pcc->add(label3);
  pcc->add(label4);
  size_t cache_size = pcc->get_cache_size();
  ASSERT_EQ(cache_size, 4);

  AdminSocketClient client(get_rand_socket_path());
  std::string message;
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf dump\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel1\":{},\"testlabel2\":{},\"testlabel3\":{},\"testlabel4\":{}}", message);

  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf schema\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel1\":{},\"testlabel2\":{},\"testlabel3\":{},\"testlabel4\":{}}", message);

  pcc->add(label5);
  pcc->add(label6);
  cache_size = pcc->get_cache_size();
  ASSERT_EQ(cache_size, 4);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf dump\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel3\":{},\"testlabel4\":{},\"testlabel5\":{},\"testlabel6\":{}}", message);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf schema\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel3\":{},\"testlabel4\":{},\"testlabel5\":{},\"testlabel6\":{}}", message);
  cleanup_test(pcc);
}

TEST(PerfCountersCache, TestNoEviction) {
  PerfCountersCache *pcc = setup_test_perf_counters_cache(g_ceph_context, false, 3);
  std::string label1 = "testlabel1";
  std::string label2 = "testlabel2";
  std::string label3 = "testlabel3";
  std::string label4 = "testlabel4";
  std::string label5 = "testlabel5";

  pcc->add(label1);
  pcc->add(label2);
  pcc->add(label3);
  size_t cache_size = pcc->get_cache_size();
  ASSERT_EQ(cache_size, 3);

  AdminSocketClient client(get_rand_socket_path());
  std::string message;
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf dump\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel1\":{},\"testlabel2\":{},\"testlabel3\":{}}", message);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf schema\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel1\":{},\"testlabel2\":{},\"testlabel3\":{}}", message);

  pcc->add(label4);
  pcc->add(label5);
  cache_size = pcc->get_cache_size();
  ASSERT_EQ(cache_size, 5);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf dump\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel1\":{},\"testlabel2\":{},\"testlabel3\":{},\"testlabel4\":{},\"testlabel5\":{}}", message);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf schema\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel1\":{},\"testlabel2\":{},\"testlabel3\":{},\"testlabel4\":{},\"testlabel5\":{}}", message);
  cleanup_test(pcc);
}

TEST(PerfCountersCache, TestCacheCounter) {
  PerfCountersCache *pcc = setup_test_perf_counters_cache(g_ceph_context, false, 3);
  std::string label1 = "testlabel1";
  std::string label2 = "testlabel2";
  std::string label3 = "testlabel3";

  pcc->add(label1);
  pcc->add(label2);

  // test inc()
  pcc->inc(label1, TEST_PERFCOUNTERS_COUNTER, 1);
  pcc->inc(label2, TEST_PERFCOUNTERS_COUNTER, 2);

  AdminSocketClient client(get_rand_socket_path());
  std::string message;
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf dump\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel1\":{\"test_counter\":1},\"testlabel2\":{\"test_counter\":2}}", message);

  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf schema\", \"format\": \"json\"  }", &message));
  ASSERT_EQ("{\"testlabel1\":{\"test_counter\":{\"type\":2,\"metric_type\":\"gauge\",\"value_type\":\"integer\",\"description\":\"\",\"nick\":\"\",\"priority\":0,\"units\":\"none\"}},\"testlabel2\":{\"test_counter\":{\"type\":2,\"metric_type\":\"gauge\",\"value_type\":\"integer\",\"description\":\"\",\"nick\":\"\",\"priority\":0,\"units\":\"none\"}}}", message);

  ASSERT_EQ("", client.do_request("{ \"prefix\": \"perf dump\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"test_base_counters\":{\"test_counter\":3,\"test_time\":0.000000000,\"test_time_avg\":{\"avgcount\":0,\"sum\":0.000000000,\"avgtime\":0.000000000}}}", message);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"perf schema\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"test_base_counters\":{\"test_counter\":{\"type\":2,\"metric_type\":\"gauge\",\"value_type\":\"integer\",\"description\":\"\",\"nick\":\"\",\"priority\":0,\"units\":\"none\"},\"test_time\":{\"type\":1,\"metric_type\":\"gauge\",\"value_type\":\"real\",\"description\":\"\",\"nick\":\"\",\"priority\":0,\"units\":\"none\"},\"test_time_avg\":{\"type\":5,\"metric_type\":\"gauge\",\"value_type\":\"real-integer-pair\",\"description\":\"\",\"nick\":\"\",\"priority\":0,\"units\":\"none\"}}}", message);

  // test dec()
  pcc->dec(label2, TEST_PERFCOUNTERS_COUNTER, 1);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf dump\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel1\":{\"test_counter\":1},\"testlabel2\":{\"test_counter\":1}}", message);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"perf dump\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"test_base_counters\":{\"test_counter\":2,\"test_time\":0.000000000,\"test_time_avg\":{\"avgcount\":0,\"sum\":0.000000000,\"avgtime\":0.000000000}}}", message);

  // test set_counters()
  pcc->add(label3);
  pcc->set_counter(label3, TEST_PERFCOUNTERS_COUNTER, 4);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"labeledperf dump\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"testlabel1\":{\"test_counter\":1},\"testlabel2\":{\"test_counter\":1},\"testlabel3\":{\"test_counter\":4}}", message);
  ASSERT_EQ("", client.do_request("{ \"prefix\": \"perf dump\", \"format\": \"json\" }", &message));
  ASSERT_EQ("{\"test_base_counters\":{\"test_counter\":6,\"test_time\":0.000000000,\"test_time_avg\":{\"avgcount\":0,\"sum\":0.000000000,\"avgtime\":0.000000000}}}", message);

  cleanup_test(pcc);
}
