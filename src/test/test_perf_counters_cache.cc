#include "common/perf_counters_cache.h"
#include "common/perf_counters_key.h"
#include "common/admin_socket_client.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/msgr.h" // for CEPH_ENTITY_TYPE_CLIENT
#include "gtest/gtest.h"

using namespace ceph::perf_counters;

int main(int argc, char **argv) {
  std::map<std::string,std::string> defaults = {
    { "admin_socket", get_rand_socket_path() }
  };
  std::vector<const char*> args;
  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE|
			 CINIT_FLAG_NO_CCT_PERF_COUNTERS);
  common_init_finish(g_ceph_context);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
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

static std::shared_ptr<PerfCounters> create_test_counters(const std::string& name, CephContext *cct) {
  PerfCountersBuilder pcb(cct, name, TEST_PERFCOUNTERS1_ELEMENT_FIRST, TEST_PERFCOUNTERS1_ELEMENT_LAST);
  add_test_counters(&pcb);
  std::shared_ptr<PerfCounters> new_counters(pcb.create_perf_counters());
  cct->get_perfcounters_collection()->add(new_counters.get());
  return new_counters;
}

static PerfCountersCache* setup_test_perf_counters_cache(CephContext *cct, uint64_t target_size = 100)
{
  return new PerfCountersCache(cct, target_size, create_test_counters);
}


void cleanup_test(PerfCountersCache *pcc) {
  delete pcc;
}

TEST(PerfCountersCache, NoCacheTest) {
  AdminSocketClient client(get_rand_socket_path());
  std::string message;
  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump" })", &message));
  ASSERT_EQ("{}\n", message);
  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter schema" })", &message));
  ASSERT_EQ("{}\n", message);
}

TEST(PerfCountersCache, TestEviction) {
  PerfCountersCache *pcc = setup_test_perf_counters_cache(g_ceph_context, 4);
  std::string label1 = key_create("key1", {{"label1", "val1"}});
  std::string label2 = key_create("key2", {{"label2", "val2"}});
  std::string label3 = key_create("key3", {{"label3", "val3"}});
  std::string label4 = key_create("key4", {{"label4", "val4"}});
  std::string label5 = key_create("key5", {{"label5", "val5"}});
  std::string label6 = key_create("key6", {{"label6", "val6"}});

  pcc->set_counter(label1, TEST_PERFCOUNTERS_COUNTER, 0);
  std::shared_ptr<PerfCounters> counter = pcc->get(label2);
  counter->set(TEST_PERFCOUNTERS_COUNTER, 0);
  pcc->set_counter(label3, TEST_PERFCOUNTERS_COUNTER, 0);
  pcc->set_counter(label4, TEST_PERFCOUNTERS_COUNTER, 0);

  AdminSocketClient client(get_rand_socket_path());
  std::string message;
  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "key1": [
        {
            "labels": {
                "label1": "val1"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "key2": [
        {
            "labels": {
                "label2": "val2"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "key3": [
        {
            "labels": {
                "label3": "val3"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "key4": [
        {
            "labels": {
                "label4": "val4"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ]
}
)", message);

  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter schema", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "key1": [
        {
            "labels": {
                "label1": "val1"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ],
    "key2": [
        {
            "labels": {
                "label2": "val2"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ],
    "key3": [
        {
            "labels": {
                "label3": "val3"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ],
    "key4": [
        {
            "labels": {
                "label4": "val4"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ]
}
)", message);

  pcc->set_counter(label5, TEST_PERFCOUNTERS_COUNTER, 0);
  pcc->set_counter(label6, TEST_PERFCOUNTERS_COUNTER, 0);
  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "key3": [
        {
            "labels": {
                "label3": "val3"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "key4": [
        {
            "labels": {
                "label4": "val4"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "key5": [
        {
            "labels": {
                "label5": "val5"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "key6": [
        {
            "labels": {
                "label6": "val6"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ]
}
)", message);


  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter schema", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "key3": [
        {
            "labels": {
                "label3": "val3"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ],
    "key4": [
        {
            "labels": {
                "label4": "val4"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ],
    "key5": [
        {
            "labels": {
                "label5": "val5"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ],
    "key6": [
        {
            "labels": {
                "label6": "val6"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ]
}
)", message);
  cleanup_test(pcc);
}

TEST(PerfCountersCache, TestLabeledCounters) {
  PerfCountersCache *pcc = setup_test_perf_counters_cache(g_ceph_context);
  std::string label1 = key_create("key1", {{"label1", "val1"}});
  std::string label2 = key_create("key2", {{"label2", "val2"}});
  std::string label3 = key_create("key3", {{"label3", "val3"}});

  // test inc()
  pcc->inc(label1, TEST_PERFCOUNTERS_COUNTER, 1);
  pcc->inc(label2, TEST_PERFCOUNTERS_COUNTER, 2);

  AdminSocketClient client(get_rand_socket_path());
  std::string message;
  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "key1": [
        {
            "labels": {
                "label1": "val1"
            },
            "counters": {
                "test_counter": 1,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "key2": [
        {
            "labels": {
                "label2": "val2"
            },
            "counters": {
                "test_counter": 2,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ]
}
)", message);


  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter schema", "format": "raw"  })", &message));
  ASSERT_EQ(R"({
    "key1": [
        {
            "labels": {
                "label1": "val1"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ],
    "key2": [
        {
            "labels": {
                "label2": "val2"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ]
}
)", message);

  // tests to ensure there is no interaction with normal perf counters
  ASSERT_EQ("", client.do_request(R"({ "prefix": "perf dump", "format": "raw" })", &message));
  ASSERT_EQ("{}\n", message);
  ASSERT_EQ("", client.do_request(R"({ "prefix": "perf schema", "format": "raw" })", &message));
  ASSERT_EQ("{}\n", message);

  // test dec()
  pcc->dec(label2, TEST_PERFCOUNTERS_COUNTER, 1);
  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "key1": [
        {
            "labels": {
                "label1": "val1"
            },
            "counters": {
                "test_counter": 1,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "key2": [
        {
            "labels": {
                "label2": "val2"
            },
            "counters": {
                "test_counter": 1,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ]
}
)", message);


  // test set_counters()
  pcc->set_counter(label3, TEST_PERFCOUNTERS_COUNTER, 4);
  uint64_t val = pcc->get_counter(label3, TEST_PERFCOUNTERS_COUNTER);
  ASSERT_EQ(val, 4);
  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "key1": [
        {
            "labels": {
                "label1": "val1"
            },
            "counters": {
                "test_counter": 1,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "key2": [
        {
            "labels": {
                "label2": "val2"
            },
            "counters": {
                "test_counter": 1,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "key3": [
        {
            "labels": {
                "label3": "val3"
            },
            "counters": {
                "test_counter": 4,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ]
}
)", message);

  cleanup_test(pcc);
}

TEST(PerfCountersCache, TestLabeledTimes) {
  PerfCountersCache *pcc = setup_test_perf_counters_cache(g_ceph_context);
  std::string label1 = key_create("key1", {{"label1", "val1"}});
  std::string label2 = key_create("key2", {{"label2", "val2"}});
  std::string label3 = key_create("key3", {{"label3", "val3"}});

  // test inc()
  pcc->tinc(label1, TEST_PERFCOUNTERS_TIME, utime_t(100,0));
  pcc->tinc(label2, TEST_PERFCOUNTERS_TIME, utime_t(200,0));

  //tinc() that takes a ceph_timespan
  ceph::timespan ceph_timespan = std::chrono::seconds(10);
  pcc->tinc(label1, TEST_PERFCOUNTERS_TIME, ceph_timespan);

  pcc->tinc(label1, TEST_PERFCOUNTERS_TIME_AVG, utime_t(200,0));
  pcc->tinc(label1, TEST_PERFCOUNTERS_TIME_AVG, utime_t(400,0));
  pcc->tinc(label2, TEST_PERFCOUNTERS_TIME_AVG, utime_t(100,0));
  pcc->tinc(label2, TEST_PERFCOUNTERS_TIME_AVG, utime_t(200,0));

  AdminSocketClient client(get_rand_socket_path());
  std::string message;
  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "key1": [
        {
            "labels": {
                "label1": "val1"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 110.000000000,
                "test_time_avg": {
                    "avgcount": 2,
                    "sum": 600.000000000,
                    "avgtime": 300.000000000
                }
            }
        }
    ],
    "key2": [
        {
            "labels": {
                "label2": "val2"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 200.000000000,
                "test_time_avg": {
                    "avgcount": 2,
                    "sum": 300.000000000,
                    "avgtime": 150.000000000
                }
            }
        }
    ]
}
)", message);


  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter schema", "format": "raw"  })", &message));
  ASSERT_EQ(R"({
    "key1": [
        {
            "labels": {
                "label1": "val1"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ],
    "key2": [
        {
            "labels": {
                "label2": "val2"
            },
            "counters": {
                "test_counter": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                },
                "test_time_avg": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "",
                    "nick": "",
                    "priority": 0,
                    "units": "none"
                }
            }
        }
    ]
}
)", message);

  // test tset() & tget()
  pcc->tset(label1, TEST_PERFCOUNTERS_TIME, utime_t(500,0));
  utime_t label1_time = pcc->tget(label1, TEST_PERFCOUNTERS_TIME);
  ASSERT_EQ(utime_t(500,0), label1_time);

  cleanup_test(pcc);
}

TEST(PerfCountersCache, TestLabelStrings) {
  AdminSocketClient client(get_rand_socket_path());
  std::string message;
  PerfCountersCache *pcc = setup_test_perf_counters_cache(g_ceph_context);
  std::string empty_key = "";

  // empty string as should not create a labeled entry
  EXPECT_DEATH(pcc->set_counter(empty_key, TEST_PERFCOUNTERS_COUNTER, 1), "");
  EXPECT_DEATH(pcc->get(empty_key), "");
  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ("{}\n", message);

  // key name but no labels at all should not create a labeled entry
  std::string only_key = "only_key";
  // run an op on an invalid key name to make sure nothing happens
  EXPECT_DEATH(pcc->set_counter(only_key, TEST_PERFCOUNTERS_COUNTER, 4), "");
  EXPECT_DEATH(pcc->get(only_key), "");

  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ("{}\n", message);

  // test valid key name with multiple valid label pairs
  std::string label1 = key_create("good_ctrs", {{"label3", "val3"}, {"label2", "val4"}});
  pcc->set_counter(label1, TEST_PERFCOUNTERS_COUNTER, 8);

  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "good_ctrs": [
        {
            "labels": {
                "label2": "val4",
                "label3": "val3"
            },
            "counters": {
                "test_counter": 8,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ]
}
)", message);

  // test empty val in a label pair will get the label pair added into the perf counters cache but empty key will not
  std::string label2 = key_create("bad_ctrs1", {{"label3", "val4"}, {"label1", ""}});
  pcc->set_counter(label2, TEST_PERFCOUNTERS_COUNTER, 2);

  std::string label3 = key_create("bad_ctrs2", {{"", "val4"}, {"label1", "val1"}});
  EXPECT_DEATH(pcc->set_counter(label3, TEST_PERFCOUNTERS_COUNTER, 2), "");

  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "bad_ctrs1": [
        {
            "labels": {
                "label1": "",
                "label3": "val4"
            },
            "counters": {
                "test_counter": 2,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "good_ctrs": [
        {
            "labels": {
                "label2": "val4",
                "label3": "val3"
            },
            "counters": {
                "test_counter": 8,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ]
}
)", message);

  // test empty keys in each of the label pairs will not get the label added into the perf counters cache
  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "bad_ctrs1": [
        {
            "labels": {
                "label1": "",
                "label3": "val4"
            },
            "counters": {
                "test_counter": 2,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "good_ctrs": [
        {
            "labels": {
                "label2": "val4",
                "label3": "val3"
            },
            "counters": {
                "test_counter": 8,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ]
}
)", message);

  // a key with a somehow odd number of entries after the the key name will omit final unfinished label pair
  std::string label5 = "too_many_delimiters";
  label5 += '\0';
  label5 += "label1";
  label5 += '\0';
  label5 += "val1";
  label5 += '\0';
  label5 += "label2";
  label5 += '\0';
  pcc->set_counter(label5, TEST_PERFCOUNTERS_COUNTER, 0);

  ASSERT_EQ("", client.do_request(R"({ "prefix": "counter dump", "format": "raw" })", &message));
  ASSERT_EQ(R"({
    "bad_ctrs1": [
        {
            "labels": {
                "label1": "",
                "label3": "val4"
            },
            "counters": {
                "test_counter": 2,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "good_ctrs": [
        {
            "labels": {
                "label2": "val4",
                "label3": "val3"
            },
            "counters": {
                "test_counter": 8,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ],
    "too_many_delimiters": [
        {
            "labels": {
                "label1": "val1"
            },
            "counters": {
                "test_counter": 0,
                "test_time": 0.000000000,
                "test_time_avg": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ]
}
)", message);

  cleanup_test(pcc);
}
