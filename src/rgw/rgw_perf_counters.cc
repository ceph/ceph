// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_perf_counters.h"
#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "common/ceph_context.h"

PerfCounters *perfcounter = NULL;
ceph::perf_counters::PerfCountersCache *perf_counters_cache = NULL;
std::string rgw_op_counters_key = "rgw_op";

static void add_rgw_frontend_counters(PerfCountersBuilder *pcb) {
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
  
  pcb->add_u64_counter(l_rgw_lua_script_ok, "lua_script_ok", "Successfull executions of Lua scripts");
  pcb->add_u64_counter(l_rgw_lua_script_fail, "lua_script_fail", "Failed executions of Lua scripts");
  pcb->add_u64(l_rgw_lua_current_vms, "lua_current_vms", "Number of Lua VMs currently being executed");
}

static void add_rgw_op_counters(PerfCountersBuilder *lpcb) {
  // description must match general rgw counters description above
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  lpcb->add_u64_counter(l_rgw_op_put, "put_ops", "Puts");
  lpcb->add_u64_counter(l_rgw_op_put_b, "put_b", "Size of puts");
  lpcb->add_time_avg(l_rgw_op_put_lat, "put_initial_lat", "Put latency");

  lpcb->add_u64_counter(l_rgw_op_get, "get_ops", "Gets");
  lpcb->add_u64_counter(l_rgw_op_get_b, "get_b", "Size of gets");
  lpcb->add_time_avg(l_rgw_op_get_lat, "get_initial_lat", "Get latency");

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

std::shared_ptr<PerfCounters> create_rgw_counters(const std::string& name, CephContext *cct) {
  std::string_view key = ceph::perf_counters::key_name(name);
  if (rgw_op_counters_key.compare(key) == 0) {
    PerfCountersBuilder pcb(cct, name, l_rgw_op_first, l_rgw_op_last);
    add_rgw_op_counters(&pcb);
    std::shared_ptr<PerfCounters> new_counters(pcb.create_perf_counters());
    cct->get_perfcounters_collection()->add(new_counters.get());
    return new_counters;
  } else {
    PerfCountersBuilder pcb(cct, name, l_rgw_first, l_rgw_last);
    add_rgw_frontend_counters(&pcb);
    std::shared_ptr<PerfCounters> new_counters(pcb.create_perf_counters());
    cct->get_perfcounters_collection()->add(new_counters.get());
    return new_counters;
  }
}

void frontend_counters_init(CephContext *cct) {
  PerfCountersBuilder pcb(cct, "rgw", l_rgw_first, l_rgw_last);
  add_rgw_frontend_counters(&pcb);
  PerfCounters *new_counters = pcb.create_perf_counters();
  cct->get_perfcounters_collection()->add(new_counters);
  perfcounter = new_counters;
}

namespace rgw::op_counters {

PerfCounters *global_op_counters = NULL;

void global_op_counters_init(CephContext *cct) {
  PerfCountersBuilder pcb(cct, rgw_op_counters_key, l_rgw_op_first, l_rgw_op_last);
  add_rgw_op_counters(&pcb);
  PerfCounters *new_counters = pcb.create_perf_counters();
  cct->get_perfcounters_collection()->add(new_counters);
  global_op_counters = new_counters;
}

void inc(std::shared_ptr<PerfCounters> labeled_counters, int idx, uint64_t v) {
  if (labeled_counters) {
    PerfCounters *counter = labeled_counters.get();
    counter->inc(idx, v);
  }
  if (global_op_counters) {
    global_op_counters->inc(idx, v);
  }
}

void tinc(std::shared_ptr<PerfCounters> labeled_counters, int idx, utime_t amt) {
  if (labeled_counters) {
    PerfCounters *counter = labeled_counters.get();
    counter->tinc(idx, amt);
  }
  if (global_op_counters) {
    global_op_counters->tinc(idx, amt);
  }
}

void tinc(std::shared_ptr<PerfCounters> labeled_counters, int idx, ceph::timespan amt) {
  if (labeled_counters) {
    PerfCounters *counter = labeled_counters.get();
    counter->tinc(idx, amt);
  }
  if (global_op_counters) {
    global_op_counters->tinc(idx, amt);
  }
}

} // namespace rgw::op_counters

int rgw_perf_start(CephContext *cct)
{
  frontend_counters_init(cct);

  bool cache_enabled = cct->_conf.get_val<bool>("rgw_perf_counters_cache");
  if (cache_enabled) {
    uint64_t target_size = cct->_conf.get_val<uint64_t>("rgw_perf_counters_cache_size");
    perf_counters_cache = new ceph::perf_counters::PerfCountersCache(cct, target_size, create_rgw_counters); 
  }

  rgw::op_counters::global_op_counters_init(cct);
  return 0;
}

void rgw_perf_stop(CephContext *cct)
{
  ceph_assert(perfcounter);
  cct->get_perfcounters_collection()->remove(perfcounter);
  delete perfcounter;
  delete perf_counters_cache;
}
