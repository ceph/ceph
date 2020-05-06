#include <pthread.h>
#include <stdlib.h>
#include <iostream>
#include <fmt/format.h>

#include "common/Formatter.h"
#include "common/perf_counters.h"
#include "crimson/common/perf_counters_collection.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>

enum {
  PERFTEST_FIRST = 1000000,
  PERFTEST_INDEX,
  PERFTEST_LAST,
};

static constexpr uint64_t PERF_VAL = 42;

static seastar::future<> test_perfcounters(){
  return crimson::common::sharded_perf_coll().start().then([] {
    return crimson::common::sharded_perf_coll().invoke_on_all([] (auto& s){
      std::string name =fmt::format("seastar-osd::shard-{}",seastar::this_shard_id());
      PerfCountersBuilder plb(NULL, name, PERFTEST_FIRST,PERFTEST_LAST);
      plb.add_u64_counter(PERFTEST_INDEX, "perftest_count", "count perftest");
      auto perf_logger = plb.create_perf_counters();
      perf_logger->inc(PERFTEST_INDEX,PERF_VAL);
      s.get_perf_collection()->add(perf_logger);
    });
  }).then([]{
    return crimson::common::sharded_perf_coll().invoke_on_all([] (auto& s){
      auto pcc = s.get_perf_collection();
      pcc->with_counters([](auto& by_path){
        for (auto& perf_counter : by_path) {
          if (PERF_VAL != perf_counter.second.perf_counters->get(PERFTEST_INDEX)) {
             throw std::runtime_error("perf counter does not match");
           }
        }
      });
    });
  }).finally([] {
     return crimson::common::sharded_perf_coll().stop();
  });

}

int main(int argc, char** argv)
{
  seastar::app_template app;
  return app.run(argc, argv, [&] {
    return test_perfcounters().then([] {
      std::cout << "All tests succeeded" << std::endl;
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });

}


