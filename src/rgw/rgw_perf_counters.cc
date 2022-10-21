// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_perf_counters.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"

PerfCounters *perfcounter = NULL;
PerfCountersCache *perf_counters_cache = NULL;

int rgw_perf_start(CephContext *cct)
{
  PerfCountersBuilder plb(cct, "rgw", l_rgw_first, l_rgw_last);

  // RGW emits comparatively few metrics, so let's be generous
  // and mark them all USEFUL to get transmission to ceph-mgr by default.
  plb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  plb.add_u64_counter(l_rgw_req, "req", "Requests");
  plb.add_u64_counter(l_rgw_failed_req, "failed_req", "Aborted requests");

  plb.add_u64_counter(l_rgw_get, "get", "Gets");
  plb.add_u64_counter(l_rgw_get_b, "get_b", "Size of gets");
  plb.add_time_avg(l_rgw_get_lat, "get_initial_lat", "Get latency");
  plb.add_u64_counter(l_rgw_put, "put", "Puts");
  plb.add_u64_counter(l_rgw_put_b, "put_b", "Size of puts");
  plb.add_time_avg(l_rgw_put_lat, "put_initial_lat", "Put latency");

  plb.add_u64(l_rgw_qlen, "qlen", "Queue length");
  plb.add_u64(l_rgw_qactive, "qactive", "Active requests queue");

  plb.add_u64_counter(l_rgw_cache_hit, "cache_hit", "Cache hits");
  plb.add_u64_counter(l_rgw_cache_miss, "cache_miss", "Cache miss");

  plb.add_u64_counter(l_rgw_keystone_token_cache_hit, "keystone_token_cache_hit", "Keystone token cache hits");
  plb.add_u64_counter(l_rgw_keystone_token_cache_miss, "keystone_token_cache_miss", "Keystone token cache miss");

  plb.add_u64_counter(l_rgw_gc_retire, "gc_retire_object", "GC object retires");

  plb.add_u64_counter(l_rgw_lc_expire_current, "lc_expire_current",
		      "Lifecycle current expiration");
  plb.add_u64_counter(l_rgw_lc_expire_noncurrent, "lc_expire_noncurrent",
		      "Lifecycle non-current expiration");
  plb.add_u64_counter(l_rgw_lc_expire_dm, "lc_expire_dm",
		      "Lifecycle delete-marker expiration");
  plb.add_u64_counter(l_rgw_lc_transition_current, "lc_transition_current",
		      "Lifecycle current transition");
  plb.add_u64_counter(l_rgw_lc_transition_noncurrent,
		      "lc_transition_noncurrent",
		      "Lifecycle non-current transition");
  plb.add_u64_counter(l_rgw_lc_abort_mpu, "lc_abort_mpu",
		      "Lifecycle abort multipart upload");

  plb.add_u64_counter(l_rgw_pubsub_event_triggered, "pubsub_event_triggered", "Pubsub events with at least one topic");
  plb.add_u64_counter(l_rgw_pubsub_event_lost, "pubsub_event_lost", "Pubsub events lost");
  plb.add_u64_counter(l_rgw_pubsub_store_ok, "pubsub_store_ok", "Pubsub events successfully stored");
  plb.add_u64_counter(l_rgw_pubsub_store_fail, "pubsub_store_fail", "Pubsub events failed to be stored");
  plb.add_u64(l_rgw_pubsub_events, "pubsub_events", "Pubsub events in store");
  plb.add_u64_counter(l_rgw_pubsub_push_ok, "pubsub_push_ok", "Pubsub events pushed to an endpoint");
  plb.add_u64_counter(l_rgw_pubsub_push_failed, "pubsub_push_failed", "Pubsub events failed to be pushed to an endpoint");
  plb.add_u64(l_rgw_pubsub_push_pending, "pubsub_push_pending", "Pubsub events pending reply from endpoint");
  plb.add_u64_counter(l_rgw_pubsub_missing_conf, "pubsub_missing_conf", "Pubsub events could not be handled because of missing configuration");
  
  plb.add_u64_counter(l_rgw_lua_script_ok, "lua_script_ok", "Successfull executions of lua scripts");
  plb.add_u64_counter(l_rgw_lua_script_fail, "lua_script_fail", "Failed executions of lua scripts");
  plb.add_u64(l_rgw_lua_current_vms, "lua_current_vms", "Number of Lua VMs currently being executed");
  
  perfcounter = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perfcounter);

  ceph::common::LabeledPerfCountersBuilder *lplb = new ceph::common::LabeledPerfCountersBuilder(cct, "labeled_rgw", l_rgw_first, l_rgw_last);

  // RGW emits comparatively few metrics, so let's be generous
  // and mark them all USEFUL to get transmission to ceph-mgr by default.
  lplb->set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
  lplb->add_u64_counter(l_rgw_req, "req", "Requests");
  lplb->add_u64_counter(l_rgw_failed_req, "failed_req", "Aborted requests");

  lplb->add_u64_counter(l_rgw_get, "get", "Gets");
  lplb->add_u64_counter(l_rgw_get_b, "get_b", "Size of gets");
  lplb->add_time_avg(l_rgw_get_lat, "get_initial_lat", "Get latency");

  lplb->add_u64_counter(l_rgw_put, "put", "Puts");
  lplb->add_u64_counter(l_rgw_put_b, "put_b", "Size of puts");
  lplb->add_time_avg(l_rgw_put_lat, "put_initial_lat", "Put latency");

  lplb->add_u64(l_rgw_qlen, "qlen", "Queue length");
  lplb->add_u64(l_rgw_qactive, "qactive", "Active requests queue");

  lplb->add_u64_counter(l_rgw_cache_hit, "cache_hit", "Cache hits");
  lplb->add_u64_counter(l_rgw_cache_miss, "cache_miss", "Cache miss");

  lplb->add_u64_counter(l_rgw_keystone_token_cache_hit, "keystone_token_cache_hit", "Keystone token cache hits");
  lplb->add_u64_counter(l_rgw_keystone_token_cache_miss, "keystone_token_cache_miss", "Keystone token cache miss");
  lplb->add_u64_counter(l_rgw_gc_retire, "gc_retire_object", "GC object retires");

  lplb->add_u64_counter(l_rgw_lc_expire_current, "lc_expire_current", "Lifecycle current expiration");
  lplb->add_u64_counter(l_rgw_lc_expire_noncurrent, "lc_expire_noncurrent", "Lifecycle non-current expiration");
  lplb->add_u64_counter(l_rgw_lc_expire_dm, "lc_expire_dm", "Lifecycle delete-marker expiration");
  lplb->add_u64_counter(l_rgw_lc_transition_current, "lc_transition_current", "Lifecycle current transition");
  lplb->add_u64_counter(l_rgw_lc_transition_noncurrent, "lc_transition_noncurrent", "Lifecycle non-current transition");
  lplb->add_u64_counter(l_rgw_lc_abort_mpu, "lc_abort_mpu", "Lifecycle abort multipart upload");

  lplb->add_u64_counter(l_rgw_pubsub_event_triggered, "pubsub_event_triggered", "Pubsub events with at least one topic");
  lplb->add_u64_counter(l_rgw_pubsub_event_lost, "pubsub_event_lost", "Pubsub events lost");
  lplb->add_u64_counter(l_rgw_pubsub_store_ok, "pubsub_store_ok", "Pubsub events successfully stored");
  lplb->add_u64_counter(l_rgw_pubsub_store_fail, "pubsub_store_fail", "Pubsub events failed to be stored");
  lplb->add_u64(l_rgw_pubsub_events, "pubsub_events", "Pubsub events in store");
  lplb->add_u64_counter(l_rgw_pubsub_push_ok, "pubsub_push_ok", "Pubsub events pushed to an endpoint");
  lplb->add_u64_counter(l_rgw_pubsub_push_failed, "pubsub_push_failed", "Pubsub events failed to be pushed to an endpoint");
  lplb->add_u64(l_rgw_pubsub_push_pending, "pubsub_push_pending", "Pubsub events pending reply from endpoint");
  lplb->add_u64_counter(l_rgw_pubsub_missing_conf, "pubsub_missing_conf", "Pubsub events could not be handled because of missing configuration");

  lplb->add_u64(l_rgw_lua_current_vms, "lua_current_vms", "Number of Lua VMs currently being executed");
  lplb->add_u64_counter(l_rgw_lua_script_ok, "lua_script_ok", "Successfull executions of lua scripts");
  lplb->add_u64_counter(l_rgw_lua_script_fail, "lua_script_fail", "Failed executions of lua scripts");

  uint64_t target_size = cct->_conf.get_val<uint64_t>("rgw_labeled_perfcounters_size");
  perf_counters_cache = new PerfCountersCache(cct, target_size, lplb);
  lplb = NULL;
  return 0;
}

void rgw_perf_stop(CephContext *cct)
{
  ceph_assert(perfcounter);
  cct->get_perfcounters_collection()->remove(perfcounter);
  delete perfcounter;
  perf_counters_cache->clear_cache();
  delete perf_counters_cache;
}
