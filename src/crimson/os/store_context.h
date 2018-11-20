#pragma once

#include "common/perf_counters_collection.h"
#include "ConfigObs.h"

class StoreContext {
  std::unique_ptr<ConfigObs> cobs;
  std::unique_ptr<PerfCountersCollection> perf_counters_collection;
public:
  StoreContext(): cobs(nullptr),perf_counters_collection(nullptr) {
     cobs = std::unique_ptr<ConfigObs>(new ConfigObs);
     perf_counters_collection = std::unique_ptr<PerfCountersCollection>(new PerfCountersCollection(nullptr));
  }
  ~StoreContext(){
   if (perf_counters_collection){
     perf_counters_collection->clear();
   }
  }
  ConfigObs* get_cobs(){ return cobs.get();}
  PerfCountersCollection* get_perfcounters_collection() {
    return perf_counters_collection.get();
  }

};

