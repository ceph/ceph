// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_perf_counters.h"
#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "common/ceph_context.h"
#include "rgw_sal.h"

using namespace ceph::perf_counters;
using namespace rgw::op_counters;
using namespace rgw::persistent_topic_counters;

PerfCounters *perfcounter = NULL;

void add_rgw_frontend_counters(PerfCountersBuilder *pcb) {
  // RGW emits comparatively few metrics, so let's be generous
  // and mark them all USEFUL to get transmission to ceph-mgr by default.
  pcb->set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  pcb->add_u64_counter(l_rgw_req, "req", "Requests");
  pcb->add_u64_counter(l_rgw_failed_req, "failed_req", "Aborted requests");

  pcb->add_u64(l_rgw_qlen, "qlen", "Queue length");
  pcb->add_u64(l_rgw_qactive, "qactive", "Active requests queue");

  pcb->add_u64_counter(l_rgw_cache_hit, "cache_hit", "Cache hits");
  pcb->add_u64_counter(l_rgw_cache_miss, "cache_miss", "Cache miss");

  pcb->add_u64_counter(l_rgw_keystone_token_cache_hit, "keystone_token_cache_hit", "Keystone token cache hits");
  pcb->add_u64_counter(l_rgw_keystone_token_cache_miss, "keystone_token_cache_miss", "Keystone token cache miss");

  pcb->add_u64_counter(l_rgw_gc_retire, "gc_retire_object", "GC object retires");

  pcb->add_u64_counter(l_rgw_lc_expire_current, "lc_expire_current",
		      "Lifecycle current expiration");
  pcb->add_u64_counter(l_rgw_lc_expire_noncurrent, "lc_expire_noncurrent",
		      "Lifecycle non-current expiration");
  pcb->add_u64_counter(l_rgw_lc_expire_dm, "lc_expire_dm",
		      "Lifecycle delete-marker expiration");
  pcb->add_u64_counter(l_rgw_lc_transition_current, "lc_transition_current",
		      "Lifecycle current transition");
  pcb->add_u64_counter(l_rgw_lc_transition_noncurrent,
		      "lc_transition_noncurrent",
		      "Lifecycle non-current transition");
  pcb->add_u64_counter(l_rgw_lc_abort_mpu, "lc_abort_mpu",
		      "Lifecycle abort multipart upload");

  pcb->add_u64_counter(l_rgw_pubsub_event_triggered, "pubsub_event_triggered", "Pubsub events with at least one topic");
  pcb->add_u64_counter(l_rgw_pubsub_event_lost, "pubsub_event_lost", "Pubsub events lost");
  pcb->add_u64_counter(l_rgw_pubsub_store_ok, "pubsub_store_ok", "Pubsub events successfully stored");
  pcb->add_u64_counter(l_rgw_pubsub_store_fail, "pubsub_store_fail", "Pubsub events failed to be stored");
  pcb->add_u64(l_rgw_pubsub_events, "pubsub_events", "Pubsub events in store");
  pcb->add_u64_counter(l_rgw_pubsub_push_ok, "pubsub_push_ok", "Pubsub events pushed to an endpoint");
  pcb->add_u64_counter(l_rgw_pubsub_push_failed, "pubsub_push_failed", "Pubsub events failed to be pushed to an endpoint");
  pcb->add_u64(l_rgw_pubsub_push_pending, "pubsub_push_pending", "Pubsub events pending reply from endpoint");
  pcb->add_u64_counter(l_rgw_pubsub_missing_conf, "pubsub_missing_conf", "Pubsub events could not be handled because of missing configuration");
  
  pcb->add_u64_counter(l_rgw_lua_script_ok, "lua_script_ok", "Successful executions of Lua scripts");
  pcb->add_u64_counter(l_rgw_lua_script_fail, "lua_script_fail", "Failed executions of Lua scripts");
  pcb->add_u64(l_rgw_lua_current_vms, "lua_current_vms", "Number of Lua VMs currently being executed");
}

void add_rgw_op_counters(PerfCountersBuilder *lpcb) {
  // description must match general rgw counters description above
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  lpcb->add_u64_counter(l_rgw_op_put_obj, "put_obj_ops", "Puts");
  lpcb->add_u64_counter(l_rgw_op_put_obj_b, "put_obj_bytes", "Size of puts");
  lpcb->add_time_avg(l_rgw_op_put_obj_lat, "put_obj_lat", "Put latency");

  lpcb->add_u64_counter(l_rgw_op_get_obj, "get_obj_ops", "Gets");
  lpcb->add_u64_counter(l_rgw_op_get_obj_b, "get_obj_bytes", "Size of gets");
  lpcb->add_time_avg(l_rgw_op_get_obj_lat, "get_obj_lat", "Get latency");

  lpcb->add_u64_counter(l_rgw_op_del_obj, "del_obj_ops", "Delete objects");
  lpcb->add_u64_counter(l_rgw_op_del_obj_b, "del_obj_bytes", "Size of delete objects");
  lpcb->add_time_avg(l_rgw_op_del_obj_lat, "del_obj_lat", "Delete object latency");

  lpcb->add_u64_counter(l_rgw_op_del_bucket, "del_bucket_ops", "Delete Buckets");
  lpcb->add_time_avg(l_rgw_op_del_bucket_lat, "del_bucket_lat", "Delete bucket latency");

  lpcb->add_u64_counter(l_rgw_op_copy_obj, "copy_obj_ops", "Copy objects");
  lpcb->add_u64_counter(l_rgw_op_copy_obj_b, "copy_obj_bytes", "Size of copy objects");
  lpcb->add_time_avg(l_rgw_op_copy_obj_lat, "copy_obj_lat", "Copy object latency");

  lpcb->add_u64_counter(l_rgw_op_list_obj, "list_obj_ops", "List objects");
  lpcb->add_time_avg(l_rgw_op_list_obj_lat, "list_obj_lat", "List objects latency");

  lpcb->add_u64_counter(l_rgw_op_list_buckets, "list_buckets_ops", "List buckets");
  lpcb->add_time_avg(l_rgw_op_list_buckets_lat, "list_buckets_lat", "List buckets latency");
}

void add_rgw_topic_counters(PerfCountersBuilder *lpcb) {
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  lpcb->add_u64(l_rgw_persistent_topic_len, "persistent_topic_len", "Persistent topic queue length");
  lpcb->add_u64(l_rgw_persistent_topic_size, "persistent_topic_size", "Persistent topic queue size");

}

void frontend_counters_init(CephContext *cct) {
  PerfCountersBuilder pcb(cct, "rgw", l_rgw_first, l_rgw_last);
  add_rgw_frontend_counters(&pcb);
  PerfCounters *new_counters = pcb.create_perf_counters();
  cct->get_perfcounters_collection()->add(new_counters);
  perfcounter = new_counters;
}

namespace rgw::op_counters {

ceph::perf_counters::PerfCountersCache *user_counters_cache = NULL;
ceph::perf_counters::PerfCountersCache *bucket_counters_cache = NULL;
PerfCounters *global_op_counters = NULL;
const std::string rgw_global_op_counters_key = "rgw_op";
const std::string rgw_user_op_counters_key = "rgw_op_per_user";
const std::string rgw_bucket_op_counters_key = "rgw_op_per_bucket";

std::shared_ptr<PerfCounters> create_rgw_op_counters(const std::string& name, CephContext *cct) {
  std::string_view key = ceph::perf_counters::key_name(name);
  ceph_assert(rgw_global_op_counters_key == key ||
              rgw_user_op_counters_key == key || rgw_bucket_op_counters_key == key);
  PerfCountersBuilder pcb(cct, name, l_rgw_op_first, l_rgw_op_last);
  add_rgw_op_counters(&pcb);
  std::shared_ptr<PerfCounters> new_counters(pcb.create_perf_counters());
  cct->get_perfcounters_collection()->add(new_counters.get());
  return new_counters;
}

void global_op_counters_init(CephContext *cct) {
  PerfCountersBuilder pcb(cct, rgw_global_op_counters_key, l_rgw_op_first, l_rgw_op_last);
  add_rgw_op_counters(&pcb);
  PerfCounters *new_counters = pcb.create_perf_counters();
  cct->get_perfcounters_collection()->add(new_counters);
  global_op_counters = new_counters;
}

CountersContainer get(req_state *s) {
  CountersContainer counters;
  std::string key;

  if (user_counters_cache && !s->user->get_id().id.empty()) {
    if (s->user->get_tenant().empty()) {
      key = ceph::perf_counters::key_create(rgw_user_op_counters_key, {{"user", s->user->get_id().id}});
    } else {
      key = ceph::perf_counters::key_create(rgw_user_op_counters_key, {{"user", s->user->get_id().id}, {"tenant", s->user->get_tenant()}});
    }
    counters.user_counters = user_counters_cache->get(key);
  }

  if (bucket_counters_cache && !s->bucket_name.empty()) {
    if (s->bucket_tenant.empty()) {
      key = ceph::perf_counters::key_create(rgw_bucket_op_counters_key, {{"bucket", s->bucket_name}});
    } else {
      key = ceph::perf_counters::key_create(rgw_bucket_op_counters_key, {{"bucket", s->bucket_name}, {"tenant", s->bucket_tenant}});
    }
    counters.bucket_counters = bucket_counters_cache->get(key);
  }

  return counters;
}

void inc(const CountersContainer &counters, int idx, uint64_t v) {
  if (counters.user_counters) {
    PerfCounters *user_counters = counters.user_counters.get();
    user_counters->inc(idx, v);
  }
  if (counters.bucket_counters) {
    PerfCounters *bucket_counters = counters.bucket_counters.get();
    bucket_counters->inc(idx, v);
  }
  if (global_op_counters) {
    global_op_counters->inc(idx, v);
  }
}

void tinc(const CountersContainer &counters, int idx, utime_t amt) {
  if (counters.user_counters) {
    PerfCounters *user_counters = counters.user_counters.get();
    user_counters->tinc(idx, amt);
  }
  if (counters.bucket_counters) {
    PerfCounters *bucket_counters = counters.bucket_counters.get();
    bucket_counters->tinc(idx, amt);
  }
  if (global_op_counters) {
    global_op_counters->tinc(idx, amt);
  }
}

void tinc(const CountersContainer &counters, int idx, ceph::timespan amt) {
  if (counters.user_counters) {
    PerfCounters *user_counters = counters.user_counters.get();
    user_counters->tinc(idx, amt);
  }
  if (counters.bucket_counters) {
    PerfCounters *bucket_counters = counters.bucket_counters.get();
    bucket_counters->tinc(idx, amt);
  }
  if (global_op_counters) {
    global_op_counters->tinc(idx, amt);
  }
}

} // namespace rgw::op_counters

namespace rgw::persistent_topic_counters {

const std::string rgw_topic_counters_key = "rgw_topic";

CountersManager::CountersManager(const std::string& topic_name, CephContext *cct)
    : cct(cct)
{
  const std::string topic_key = ceph::perf_counters::key_create(rgw_topic_counters_key, {{"Topic", topic_name}});
  PerfCountersBuilder pcb(cct, topic_key, l_rgw_topic_first, l_rgw_topic_last);
  add_rgw_topic_counters(&pcb);
  topic_counters = std::unique_ptr<PerfCounters>(pcb.create_perf_counters());
  cct->get_perfcounters_collection()->add(topic_counters.get());
}

void CountersManager::set(int idx, uint64_t v) {
  topic_counters->set(idx, v);
}

CountersManager::~CountersManager() {
  cct->get_perfcounters_collection()->remove(topic_counters.get());
}

} // namespace rgw::persistent_topic_counters

int rgw_perf_start(CephContext *cct)
{
  frontend_counters_init(cct);

  bool user_counters_cache_enabled = cct->_conf.get_val<bool>("rgw_user_counters_cache");
  if (user_counters_cache_enabled) {
    uint64_t target_size = cct->_conf.get_val<uint64_t>("rgw_user_counters_cache_size");
    user_counters_cache = new PerfCountersCache(cct, target_size, create_rgw_op_counters);
  }

  bool bucket_counters_cache_enabled = cct->_conf.get_val<bool>("rgw_bucket_counters_cache");
  if (bucket_counters_cache_enabled) {
    uint64_t target_size = cct->_conf.get_val<uint64_t>("rgw_bucket_counters_cache_size");
    bucket_counters_cache = new PerfCountersCache(cct, target_size, create_rgw_op_counters);
  }

  global_op_counters_init(cct);
  return 0;
}

void rgw_perf_stop(CephContext *cct)
{
  ceph_assert(perfcounter);
  cct->get_perfcounters_collection()->remove(perfcounter);
  delete perfcounter;
  ceph_assert(global_op_counters);
  cct->get_perfcounters_collection()->remove(global_op_counters);
  delete global_op_counters;
  delete user_counters_cache;
  delete bucket_counters_cache;
}
