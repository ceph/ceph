#include "gtest/gtest.h"
#include "rgw_usage_manager.h"
#include "common/perf_counters.h"

class DummyPerfLogger : public PerfCounters {
public:
  DummyPerfLogger() : PerfCounters(nullptr, "dummy", 0, 1) {}
  void set(int id, uint64_t v) override {
    last_id = id;
    last_val = v;
  }
  int last_id = -1;
  uint64_t last_val = 0;
};

class RGWUsageManagerTest : public ::testing::Test {
protected:
  CephContext *cct = nullptr;
  RGWUsageCache *cache = nullptr;
  DummyPerfLogger *logger = nullptr;
  RGWUsageManager *manager = nullptr;

  void SetUp() override {
    cct = (new CephContext(CephContext::get_module_type("test")));
    cache = new RGWUsageCache(cct, "/tmp/rgw_test_mgr");
    ASSERT_EQ(cache->init(), 0);
    logger = new DummyPerfLogger();
    manager = new RGWUsageManager(cct, cache, logger);
  }

  void TearDown() override {
    if (manager) {
      manager->stop();
      delete manager;
    }
    delete logger;
    delete cache;
    delete cct;
    system("rm -rf /tmp/rgw_test_mgr");
  }
};

TEST_F(RGWUsageManagerTest, StartAndStopThread) {
  manager->start();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  manager->stop();
  SUCCEED();
}
