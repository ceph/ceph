// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "include/common_fwd.h"
#include "rgw_common.h"
#include "common/perf_counters_cache.h"
#include "common/perf_counters_key.h"

extern PerfCounters *perfcounter;
extern int rgw_perf_start(CephContext *cct);
extern void rgw_perf_stop(CephContext *cct);

enum {
  l_rgw_first = 15000,
  l_rgw_req,
  l_rgw_failed_req,

  l_rgw_qlen,
  l_rgw_qactive,

  l_rgw_cache_hit,
  l_rgw_cache_miss,

  l_rgw_keystone_token_cache_hit,
  l_rgw_keystone_token_cache_miss,

  l_rgw_gc_retire,

  l_rgw_lc_expire_current,
  l_rgw_lc_expire_noncurrent,
  l_rgw_lc_expire_dm,
  l_rgw_lc_transition_current,
  l_rgw_lc_transition_noncurrent,
  l_rgw_lc_abort_mpu,

  l_rgw_pubsub_event_triggered,
  l_rgw_pubsub_event_lost,
  l_rgw_pubsub_store_ok,
  l_rgw_pubsub_store_fail,
  l_rgw_pubsub_events,
  l_rgw_pubsub_push_ok,
  l_rgw_pubsub_push_failed,
  l_rgw_pubsub_push_pending,
  l_rgw_pubsub_missing_conf,

  l_rgw_lua_current_vms,
  l_rgw_lua_script_ok,
  l_rgw_lua_script_fail,

  l_rgw_last,
};

enum {
  l_rgw_op_first = 16000,

  l_rgw_op_put_obj,
  l_rgw_op_put_obj_b,
  l_rgw_op_put_obj_lat,

  l_rgw_op_get_obj,
  l_rgw_op_get_obj_b,
  l_rgw_op_get_obj_lat,

  l_rgw_op_del_obj,
  l_rgw_op_del_obj_b,
  l_rgw_op_del_obj_lat,

  l_rgw_op_del_bucket,
  l_rgw_op_del_bucket_lat,

  l_rgw_op_copy_obj,
  l_rgw_op_copy_obj_b,
  l_rgw_op_copy_obj_lat,

  l_rgw_op_list_obj,
  l_rgw_op_list_obj_lat,

  l_rgw_op_list_buckets,
  l_rgw_op_list_buckets_lat,

  l_rgw_op_last
};

enum {
  l_rgw_topic_first = 17000,

  l_rgw_persistent_topic_len,
  l_rgw_persistent_topic_size,

  l_rgw_topic_last
};

namespace rgw::op_counters {

struct CountersContainer {
  std::shared_ptr<PerfCounters> user_counters;
  std::shared_ptr<PerfCounters> bucket_counters;
};

CountersContainer get(req_state *s);

void inc(const CountersContainer &counters, int idx, uint64_t v);

void tinc(const CountersContainer &counters, int idx, utime_t);

void tinc(const CountersContainer &counters, int idx, ceph::timespan amt);

} // namespace rgw::op_counters

namespace rgw::persistent_topic_counters {

class CountersManager {
  std::unique_ptr<PerfCounters> topic_counters;
  CephContext *cct;

public:
  CountersManager(const std::string& name, CephContext *cct);

  void set(int idx, uint64_t v);

  ~CountersManager();

};

} // namespace rgw::persistent_topic_counters
