#include "common/perf_counters_cache.h"
#include "common/perf_counters_cache_key.h"
//#include "rgw_perfcounters_cache_request.h"

int main() {
  auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);

  size_t target_size = 1;
  PerfCountersCache cache(cct, target_size);

  std::string key1 = ceph::perf_counters::cache_key("rgw", {{"Bucket","Foo"}, {"User", "Sally"}});
  cache.add(key1);
  std::cout << "instance_labels for entry 1: " << key1 << std::endl;


  uint64_t ret = cache.get_counter(key1, l_rgw_metrics_put_b);
  std::cout << "initial l_rgw_put_b is " << ret << std::endl;
  cache.inc(key1, l_rgw_metrics_put_b, 500);
  ret = cache.get_counter(key1, l_rgw_metrics_put_b);
  std::cout << key1 << " l_rgw_metrics_put_b after inc 500 is " << ret << std::endl;
  cache.set_counter(key1, l_rgw_metrics_put_b, 300);
  ret = cache.get_counter(key1, l_rgw_metrics_put_b);
  std::cout << key1 << " l_rgw_metrics_put_b after set 300 is " << ret << std::endl;
  cache.dec(key1, l_rgw_metrics_put_b, 50);
  ret = cache.get_counter(key1, l_rgw_metrics_put_b);
  std::cout << key1 << " l_rgw_metrics_put_b after dec 50 is " << ret << std::endl;
  std::cout << std::endl;

  std::string key2 = ceph::perf_counters::cache_key("rgw", {{"Bucket","Foo"}, {"User", "Harry"}});
  cache.add(key2);
  std::cout << "instance_labels for entry 2: " << key2 << std::endl;

  ret = cache.get_counter(key2, l_rgw_metrics_get_b);
  std::cout << key2 << " initial l_rgw_metrics_get_b is " << ret << std::endl;
  cache.set_counter(key2, l_rgw_metrics_get_b, 400);
  ret = cache.get_counter(key2, l_rgw_metrics_get_b);
  std::cout << key2 << " l_rgw_metrics_get_b after set 400 is " << ret << std::endl;
  cache.inc(key2, l_rgw_metrics_get_b, 1000);
  ret = cache.get_counter(key2, l_rgw_metrics_get_b);
  std::cout << key2 << " l_rgw_metrics_get_b after inc 1000 is " << ret << std::endl;
  cache.dec(key2, l_rgw_metrics_get_b, 500);
  ret = cache.get_counter(key2, l_rgw_metrics_get_b);
  std::cout << key2 << " l_rgw_metrics_get_b after dec 500 is " << ret << std::endl;

  delete cct;

  return 0;
}
