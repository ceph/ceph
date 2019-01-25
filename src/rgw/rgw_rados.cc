// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <boost/algorithm/string.hpp>

#include <boost/container/flat_set.hpp>
#include <boost/format.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>

#include "common/ceph_json.h"
#include "common/utf8.h"

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/Throttle.h"

#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_cache.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h" /* for dumping s3policy in debug log */
#include "rgw_aio_throttle.h"
#include "rgw_bucket.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_putobj_processor.h"

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_const.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "cls/log/cls_log_client.h"
#include "cls/timeindex/cls_timeindex_client.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/user/cls_user_client.h"
#include "cls/otp/cls_otp_client.h"
#include "osd/osd_types.h"

#include "rgw_tools.h"
#include "rgw_coroutine.h"
#include "rgw_compression.h"

#undef fork // fails to compile RGWPeriod::fork() below

#include "common/Clock.h"

using namespace librados;

#include <string>
#include <iostream>
#include <vector>
#include <atomic>
#include <list>
#include <map>
#include "include/random.h"

#include "rgw_gc.h"
#include "rgw_lc.h"

#include "rgw_object_expirer_core.h"
#include "rgw_sync.h"
#include "rgw_sync_trace.h"
#include "rgw_data_sync.h"
#include "rgw_realm_watcher.h"
#include "rgw_reshard.h"

#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"
#include "services/svc_quota.h"
#include "services/svc_sync_modules.h"
#include "services/svc_sys_obj.h"
#include "services/svc_sys_obj_cache.h"

#include "compressor/Compressor.h"

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/rgw_rados.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw


static string shadow_ns = "shadow";
static string dir_oid_prefix = ".dir.";
static string default_bucket_index_pool_suffix = "rgw.buckets.index";
static string default_storage_extra_pool_suffix = "rgw.buckets.non-ec";

static string log_lock_name = "rgw_log_lock";
static RGWObjCategory main_category = RGWObjCategory::Main;
#define RGW_USAGE_OBJ_PREFIX "usage."


#define dout_subsys ceph_subsys_rgw


static bool rgw_get_obj_data_pool(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params,
                                  const rgw_placement_rule& head_placement_rule,
                                  const rgw_obj& obj, rgw_pool *pool)
{
  if (!zone_params.get_head_data_pool(head_placement_rule, obj, pool)) {
    RGWZonePlacementInfo placement;
    if (!zone_params.get_placement(zonegroup.default_placement.name, &placement)) {
      return false;
    }

    if (!obj.in_extra_data) {
      *pool = placement.get_data_pool(zonegroup.default_placement.storage_class);
    } else {
      *pool = placement.get_data_extra_pool();
    }
  }

  return true;
}

static bool rgw_obj_to_raw(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params,
                           const rgw_placement_rule& head_placement_rule,
                           const rgw_obj& obj, rgw_raw_obj *raw_obj)
{
  get_obj_bucket_and_oid_loc(obj, raw_obj->oid, raw_obj->loc);

  return rgw_get_obj_data_pool(zonegroup, zone_params, head_placement_rule, obj, &raw_obj->pool);
}

rgw_raw_obj rgw_obj_select::get_raw_obj(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params) const
{
  if (!is_raw) {
    rgw_raw_obj r;
    rgw_obj_to_raw(zonegroup, zone_params, placement_rule, obj, &r);
    return r;
  }
  return raw_obj;
}

rgw_raw_obj rgw_obj_select::get_raw_obj(RGWRados *store) const
{
  if (!is_raw) {
    rgw_raw_obj r;
    store->obj_to_raw(placement_rule, obj, &r);
    return r;
  }
  return raw_obj;
}

int rgw_init_ioctx(librados::Rados *rados, const rgw_pool& pool, IoCtx& ioctx, bool create)
{
  int r = rados->ioctx_create(pool.name.c_str(), ioctx);
  if (r == -ENOENT && create) {
    r = rados->pool_create(pool.name.c_str());
    if (r == -ERANGE) {
      dout(0)
        << __func__
        << " ERROR: librados::Rados::pool_create returned " << cpp_strerror(-r)
        << " (this can be due to a pool or placement group misconfiguration, e.g."
        << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
        << dendl;
    }
    if (r < 0 && r != -EEXIST) {
      return r;
    }

    r = rados->ioctx_create(pool.name.c_str(), ioctx);
    if (r < 0) {
      return r;
    }

    r = ioctx.application_enable(pg_pool_t::APPLICATION_NAME_RGW, false);
    if (r < 0 && r != -EOPNOTSUPP) {
      return r;
    }
  } else if (r < 0) {
    return r;
  }
  if (!pool.ns.empty()) {
    ioctx.set_namespace(pool.ns);
  }
  return 0;
}

void RGWObjVersionTracker::prepare_op_for_read(ObjectReadOperation *op)
{
  obj_version *check_objv = version_for_check();

  if (check_objv) {
    cls_version_check(*op, *check_objv, VER_COND_EQ);
  }

  cls_version_read(*op, &read_version);
}

void RGWObjVersionTracker::prepare_op_for_write(ObjectWriteOperation *op)
{
  obj_version *check_objv = version_for_check();
  obj_version *modify_version = version_for_write();

  if (check_objv) {
    cls_version_check(*op, *check_objv, VER_COND_EQ);
  }

  if (modify_version) {
    cls_version_set(*op, *modify_version);
  } else {
    cls_version_inc(*op);
  }
}

void RGWObjManifest::obj_iterator::operator++()
{
  if (manifest->explicit_objs) {
    ++explicit_iter;

    update_explicit_pos();

    update_location();
    return;
  }

  uint64_t obj_size = manifest->get_obj_size();
  uint64_t head_size = manifest->get_head_size();

  if (ofs == obj_size) {
    return;
  }

  if (manifest->rules.empty()) {
    return;
  }

  /* are we still pointing at the head? */
  if (ofs < head_size) {
    rule_iter = manifest->rules.begin();
    RGWObjManifestRule *rule = &rule_iter->second;
    ofs = std::min(head_size, obj_size);
    stripe_ofs = ofs;
    cur_stripe = 1;
    stripe_size = std::min(obj_size - ofs, rule->stripe_max_size);
    if (rule->part_size > 0) {
      stripe_size = std::min(stripe_size, rule->part_size);
    }
    update_location();
    return;
  }

  RGWObjManifestRule *rule = &rule_iter->second;

  stripe_ofs += rule->stripe_max_size;
  cur_stripe++;
  dout(20) << "RGWObjManifest::operator++(): rule->part_size=" << rule->part_size << " rules.size()=" << manifest->rules.size() << dendl;

  if (rule->part_size > 0) {
    /* multi part, multi stripes object */

    dout(20) << "RGWObjManifest::operator++(): stripe_ofs=" << stripe_ofs << " part_ofs=" << part_ofs << " rule->part_size=" << rule->part_size << dendl;

    if (stripe_ofs >= part_ofs + rule->part_size) {
      /* moved to the next part */
      cur_stripe = 0;
      part_ofs += rule->part_size;
      stripe_ofs = part_ofs;

      bool last_rule = (next_rule_iter == manifest->rules.end());
      /* move to the next rule? */
      if (!last_rule && stripe_ofs >= next_rule_iter->second.start_ofs) {
        rule_iter = next_rule_iter;
        last_rule = (next_rule_iter == manifest->rules.end());
        if (!last_rule) {
          ++next_rule_iter;
        }
        cur_part_id = rule_iter->second.start_part_num;
      } else {
        cur_part_id++;
      }

      rule = &rule_iter->second;
    }

    stripe_size = std::min(rule->part_size - (stripe_ofs - part_ofs), rule->stripe_max_size);
  }

  cur_override_prefix = rule->override_prefix;

  ofs = stripe_ofs;
  if (ofs > obj_size) {
    ofs = obj_size;
    stripe_ofs = ofs;
    stripe_size = 0;
  }

  dout(20) << "RGWObjManifest::operator++(): result: ofs=" << ofs << " stripe_ofs=" << stripe_ofs << " part_ofs=" << part_ofs << " rule->part_size=" << rule->part_size << dendl;
  update_location();
}

int RGWObjManifest::generator::create_begin(CephContext *cct, RGWObjManifest *_m,
                                            const rgw_placement_rule& head_placement_rule,
                                            const rgw_placement_rule *tail_placement_rule,
                                            const rgw_bucket& _b, const rgw_obj& _obj)
{
  manifest = _m;

  if (!tail_placement_rule) {
    manifest->set_tail_placement(head_placement_rule, _b);
  } else {
    rgw_placement_rule new_tail_rule = *tail_placement_rule;
    new_tail_rule.inherit_from(head_placement_rule);
    manifest->set_tail_placement(new_tail_rule, _b);
  }

  manifest->set_head(head_placement_rule, _obj, 0);
  last_ofs = 0;

  if (manifest->get_prefix().empty()) {
    char buf[33];
    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);

    string oid_prefix = ".";
    oid_prefix.append(buf);
    oid_prefix.append("_");

    manifest->set_prefix(oid_prefix);
  }

  bool found = manifest->get_rule(0, &rule);
  if (!found) {
    derr << "ERROR: manifest->get_rule() could not find rule" << dendl;
    return -EIO;
  }

  uint64_t head_size = manifest->get_head_size();

  if (head_size > 0) {
    cur_stripe_size = head_size;
  } else {
    cur_stripe_size = rule.stripe_max_size;
  }
  
  cur_part_id = rule.start_part_num;

  manifest->get_implicit_location(cur_part_id, cur_stripe, 0, NULL, &cur_obj);

  // Normal object which not generated through copy operation 
  manifest->set_tail_instance(_obj.key.instance);

  manifest->update_iterators();

  return 0;
}

int RGWObjManifest::generator::create_next(uint64_t ofs)
{
  if (ofs < last_ofs) /* only going forward */
    return -EINVAL;

  uint64_t max_head_size = manifest->get_max_head_size();

  if (ofs < max_head_size) {
    manifest->set_head_size(ofs);
  }

  if (ofs >= max_head_size) {
    manifest->set_head_size(max_head_size);
    cur_stripe = (ofs - max_head_size) / rule.stripe_max_size;
    cur_stripe_size = rule.stripe_max_size;

    if (cur_part_id == 0 && max_head_size > 0) {
      cur_stripe++;
    }
  }

  last_ofs = ofs;
  manifest->set_obj_size(ofs);

  manifest->get_implicit_location(cur_part_id, cur_stripe, ofs, NULL, &cur_obj);

  manifest->update_iterators();

  return 0;
}

const RGWObjManifest::obj_iterator& RGWObjManifest::obj_begin()
{
  return begin_iter;
}

const RGWObjManifest::obj_iterator& RGWObjManifest::obj_end()
{
  return end_iter;
}

RGWObjManifest::obj_iterator RGWObjManifest::obj_find(uint64_t ofs)
{
  if (ofs > obj_size) {
    ofs = obj_size;
  }
  RGWObjManifest::obj_iterator iter(this);
  iter.seek(ofs);
  return iter;
}

int RGWObjManifest::append(RGWObjManifest& m, const RGWZoneGroup& zonegroup,
                           const RGWZoneParams& zone_params)
{
  if (explicit_objs || m.explicit_objs) {
    return append_explicit(m, zonegroup, zone_params);
  }

  if (rules.empty()) {
    *this = m;
    return 0;
  }

  string override_prefix;

  if (prefix.empty()) {
    prefix = m.prefix;
  }

  if (prefix != m.prefix) {
    override_prefix = m.prefix;
  }

  map<uint64_t, RGWObjManifestRule>::iterator miter = m.rules.begin();
  if (miter == m.rules.end()) {
    return append_explicit(m, zonegroup, zone_params);
  }

  for (; miter != m.rules.end(); ++miter) {
    map<uint64_t, RGWObjManifestRule>::reverse_iterator last_rule = rules.rbegin();

    RGWObjManifestRule& rule = last_rule->second;

    if (rule.part_size == 0) {
      rule.part_size = obj_size - rule.start_ofs;
    }

    RGWObjManifestRule& next_rule = miter->second;
    if (!next_rule.part_size) {
      next_rule.part_size = m.obj_size - next_rule.start_ofs;
    }

    string rule_prefix = prefix;
    if (!rule.override_prefix.empty()) {
      rule_prefix = rule.override_prefix;
    }

    string next_rule_prefix = m.prefix;
    if (!next_rule.override_prefix.empty()) {
      next_rule_prefix = next_rule.override_prefix;
    }

    if (rule.part_size != next_rule.part_size ||
        rule.stripe_max_size != next_rule.stripe_max_size ||
        rule_prefix != next_rule_prefix) {
      if (next_rule_prefix != prefix) {
        append_rules(m, miter, &next_rule_prefix);
      } else {
        append_rules(m, miter, NULL);
      }
      break;
    }

    uint64_t expected_part_num = rule.start_part_num + 1;
    if (rule.part_size > 0) {
      expected_part_num = rule.start_part_num + (obj_size + next_rule.start_ofs - rule.start_ofs) / rule.part_size;
    }

    if (expected_part_num != next_rule.start_part_num) {
      append_rules(m, miter, NULL);
      break;
    }
  }

  set_obj_size(obj_size + m.obj_size);

  return 0;
}

int RGWObjManifest::append(RGWObjManifest& m, RGWSI_Zone *zone_svc)
{
  return append(m, zone_svc->get_zonegroup(), zone_svc->get_zone_params());
}

void RGWObjManifest::append_rules(RGWObjManifest& m, map<uint64_t, RGWObjManifestRule>::iterator& miter,
                                  string *override_prefix)
{
  for (; miter != m.rules.end(); ++miter) {
    RGWObjManifestRule rule = miter->second;
    rule.start_ofs += obj_size;
    if (override_prefix)
      rule.override_prefix = *override_prefix;
    rules[rule.start_ofs] = rule;
  }
}

void RGWObjManifest::convert_to_explicit(const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params)
{
  if (explicit_objs) {
    return;
  }
  obj_iterator iter = obj_begin();

  while (iter != obj_end()) {
    RGWObjManifestPart& part = objs[iter.get_stripe_ofs()];
    const rgw_obj_select& os = iter.get_location();
    const rgw_raw_obj& raw_loc = os.get_raw_obj(zonegroup, zone_params);
    part.loc_ofs = 0;

    uint64_t ofs = iter.get_stripe_ofs();

    if (ofs == 0) {
      part.loc = obj;
    } else {
      rgw_raw_obj_to_obj(tail_placement.bucket, raw_loc, &part.loc);
    }
    ++iter;
    uint64_t next_ofs = iter.get_stripe_ofs();

    part.size = next_ofs - ofs;
  }

  explicit_objs = true;
  rules.clear();
  prefix.clear();
}

int RGWObjManifest::append_explicit(RGWObjManifest& m, const RGWZoneGroup& zonegroup, const RGWZoneParams& zone_params)
{
  if (!explicit_objs) {
    convert_to_explicit(zonegroup, zone_params);
  }
  if (!m.explicit_objs) {
    m.convert_to_explicit(zonegroup, zone_params);
  }
  map<uint64_t, RGWObjManifestPart>::iterator iter;
  uint64_t base = obj_size;
  for (iter = m.objs.begin(); iter != m.objs.end(); ++iter) {
    RGWObjManifestPart& part = iter->second;
    objs[base + iter->first] = part;
  }
  obj_size += m.obj_size;

  return 0;
}

bool RGWObjManifest::get_rule(uint64_t ofs, RGWObjManifestRule *rule)
{
  if (rules.empty()) {
    return false;
  }

  map<uint64_t, RGWObjManifestRule>::iterator iter = rules.upper_bound(ofs);
  if (iter != rules.begin()) {
    --iter;
  }

  *rule = iter->second;

  return true;
}

void RGWObjVersionTracker::generate_new_write_ver(CephContext *cct)
{
  write_version.ver = 1;
#define TAG_LEN 24

  write_version.tag.clear();
  append_rand_alpha(cct, write_version.tag, write_version.tag, TAG_LEN);
}

class RGWMetaNotifierManager : public RGWCoroutinesManager {
  RGWRados *store;
  RGWHTTPManager http_manager;

public:
  RGWMetaNotifierManager(RGWRados *_store) : RGWCoroutinesManager(_store->ctx(), _store->get_cr_registry()), store(_store),
                                             http_manager(store->ctx(), completion_mgr) {
    http_manager.start();
  }

  int notify_all(map<string, RGWRESTConn *>& conn_map, set<int>& shards) {
    rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                    { "notify", NULL },
                                    { NULL, NULL } };

    list<RGWCoroutinesStack *> stacks;
    for (map<string, RGWRESTConn *>::iterator iter = conn_map.begin(); iter != conn_map.end(); ++iter) {
      RGWRESTConn *conn = iter->second;
      RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
      stack->call(new RGWPostRESTResourceCR<set<int>, int>(store->ctx(), conn, &http_manager, "/admin/log", pairs, shards, NULL));

      stacks.push_back(stack);
    }
    return run(stacks);
  }
};

class RGWDataNotifierManager : public RGWCoroutinesManager {
  RGWRados *store;
  RGWHTTPManager http_manager;

public:
  RGWDataNotifierManager(RGWRados *_store) : RGWCoroutinesManager(_store->ctx(), _store->get_cr_registry()), store(_store),
                                             http_manager(store->ctx(), completion_mgr) {
    http_manager.start();
  }

  int notify_all(map<string, RGWRESTConn *>& conn_map, map<int, set<string> >& shards) {
    rgw_http_param_pair pairs[] = { { "type", "data" },
                                    { "notify", NULL },
                                    { "source-zone", store->svc.zone->get_zone_params().get_id().c_str() },
                                    { NULL, NULL } };

    list<RGWCoroutinesStack *> stacks;
    for (map<string, RGWRESTConn *>::iterator iter = conn_map.begin(); iter != conn_map.end(); ++iter) {
      RGWRESTConn *conn = iter->second;
      RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
      stack->call(new RGWPostRESTResourceCR<map<int, set<string> >, int>(store->ctx(), conn, &http_manager, "/admin/log", pairs, shards, NULL));

      stacks.push_back(stack);
    }
    return run(stacks);
  }
};

/* class RGWRadosThread */

void RGWRadosThread::start()
{
  worker = new Worker(cct, this);
  worker->create(thread_name.c_str());
}

void RGWRadosThread::stop()
{
  down_flag = true;
  stop_process();
  if (worker) {
    worker->signal();
    worker->join();
  }
  delete worker;
  worker = NULL;
}

void *RGWRadosThread::Worker::entry() {
  uint64_t msec = processor->interval_msec();
  utime_t interval = utime_t(msec / 1000, (msec % 1000) * 1000000);

  do {
    utime_t start = ceph_clock_now();
    int r = processor->process();
    if (r < 0) {
      dout(0) << "ERROR: processor->process() returned error r=" << r << dendl;
    }

    if (processor->going_down())
      break;

    utime_t end = ceph_clock_now();
    end -= start;

    uint64_t cur_msec = processor->interval_msec();
    if (cur_msec != msec) { /* was it reconfigured? */
      msec = cur_msec;
      interval = utime_t(msec / 1000, (msec % 1000) * 1000000);
    }

    if (cur_msec > 0) {
      if (interval <= end)
        continue; // next round

      utime_t wait_time = interval;
      wait_time -= end;

      wait_interval(wait_time);
    } else {
      wait();
    }
  } while (!processor->going_down());

  return NULL;
}

class RGWMetaNotifier : public RGWRadosThread {
  RGWMetaNotifierManager notify_mgr;
  RGWMetadataLog *const log;

  uint64_t interval_msec() override {
    return cct->_conf->rgw_md_notify_interval_msec;
  }
  void stop_process() override {
    notify_mgr.stop();
  }
public:
  RGWMetaNotifier(RGWRados *_store, RGWMetadataLog* log)
    : RGWRadosThread(_store, "meta-notifier"), notify_mgr(_store), log(log) {}

  int process() override;
};

int RGWMetaNotifier::process()
{
  set<int> shards;

  log->read_clear_modified(shards);

  if (shards.empty()) {
    return 0;
  }

  for (set<int>::iterator iter = shards.begin(); iter != shards.end(); ++iter) {
    ldout(cct, 20) << __func__ << "(): notifying mdlog change, shard_id=" << *iter << dendl;
  }

  notify_mgr.notify_all(store->svc.zone->get_zone_conn_map(), shards);

  return 0;
}

class RGWDataNotifier : public RGWRadosThread {
  RGWDataNotifierManager notify_mgr;

  uint64_t interval_msec() override {
    return cct->_conf.get_val<int64_t>("rgw_data_notify_interval_msec");
  }
  void stop_process() override {
    notify_mgr.stop();
  }
public:
  RGWDataNotifier(RGWRados *_store) : RGWRadosThread(_store, "data-notifier"), notify_mgr(_store) {}

  int process() override;
};

int RGWDataNotifier::process()
{
  if (!store->data_log) {
    return 0;
  }

  map<int, set<string> > shards;

  store->data_log->read_clear_modified(shards);

  if (shards.empty()) {
    return 0;
  }

  for (map<int, set<string> >::iterator iter = shards.begin(); iter != shards.end(); ++iter) {
    ldout(cct, 20) << __func__ << "(): notifying datalog change, shard_id=" << iter->first << ": " << iter->second << dendl;
  }

  notify_mgr.notify_all(store->svc.zone->get_zone_data_notify_to_map(), shards);

  return 0;
}

class RGWSyncProcessorThread : public RGWRadosThread {
public:
  RGWSyncProcessorThread(RGWRados *_store, const string& thread_name = "radosgw") : RGWRadosThread(_store, thread_name) {}
  RGWSyncProcessorThread(RGWRados *_store) : RGWRadosThread(_store) {}
  ~RGWSyncProcessorThread() override {}
  int init() override = 0 ;
  int process() override = 0;
};

class RGWMetaSyncProcessorThread : public RGWSyncProcessorThread
{
  RGWMetaSyncStatusManager sync;

  uint64_t interval_msec() override {
    return 0; /* no interval associated, it'll run once until stopped */
  }
  void stop_process() override {
    sync.stop();
  }
public:
  RGWMetaSyncProcessorThread(RGWRados *_store, RGWAsyncRadosProcessor *async_rados)
    : RGWSyncProcessorThread(_store, "meta-sync"), sync(_store, async_rados) {}

  void wakeup_sync_shards(set<int>& shard_ids) {
    for (set<int>::iterator iter = shard_ids.begin(); iter != shard_ids.end(); ++iter) {
      sync.wakeup(*iter);
    }
  }
  RGWMetaSyncStatusManager* get_manager() { return &sync; }

  int init() override {
    int ret = sync.init();
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: sync.init() returned " << ret << dendl;
      return ret;
    }
    return 0;
  }

  int process() override {
    sync.run();
    return 0;
  }
};

class RGWDataSyncProcessorThread : public RGWSyncProcessorThread
{
  RGWDataSyncStatusManager sync;
  bool initialized;

  uint64_t interval_msec() override {
    if (initialized) {
      return 0; /* no interval associated, it'll run once until stopped */
    } else {
#define DATA_SYNC_INIT_WAIT_SEC 20
      return DATA_SYNC_INIT_WAIT_SEC * 1000;
    }
  }
  void stop_process() override {
    sync.stop();
  }
public:
  RGWDataSyncProcessorThread(RGWRados *_store, RGWAsyncRadosProcessor *async_rados,
                             const string& _source_zone)
    : RGWSyncProcessorThread(_store, "data-sync"),
      sync(_store, async_rados, _source_zone),
      initialized(false) {}

  void wakeup_sync_shards(map<int, set<string> >& shard_ids) {
    for (map<int, set<string> >::iterator iter = shard_ids.begin(); iter != shard_ids.end(); ++iter) {
      sync.wakeup(iter->first, iter->second);
    }
  }
  RGWDataSyncStatusManager* get_manager() { return &sync; }

  int init() override {
    return 0;
  }

  int process() override {
    while (!initialized) {
      if (going_down()) {
        return 0;
      }
      int ret = sync.init();
      if (ret >= 0) {
        initialized = true;
        break;
      }
      /* we'll be back! */
      return 0;
    }
    sync.run();
    return 0;
  }
};

class RGWSyncLogTrimThread : public RGWSyncProcessorThread, DoutPrefixProvider
{
  RGWCoroutinesManager crs;
  RGWRados *store;
  rgw::BucketTrimManager *bucket_trim;
  RGWHTTPManager http;
  const utime_t trim_interval;

  uint64_t interval_msec() override { return 0; }
  void stop_process() override { crs.stop(); }
public:
  RGWSyncLogTrimThread(RGWRados *store, rgw::BucketTrimManager *bucket_trim,
                       int interval)
    : RGWSyncProcessorThread(store, "sync-log-trim"),
      crs(store->ctx(), store->get_cr_registry()), store(store),
      bucket_trim(bucket_trim),
      http(store->ctx(), crs.get_completion_mgr()),
      trim_interval(interval, 0)
  {}

  int init() override {
    return http.start();
  }
  int process() override {
    list<RGWCoroutinesStack*> stacks;
    auto meta = new RGWCoroutinesStack(store->ctx(), &crs);
    meta->call(create_meta_log_trim_cr(this, store, &http,
                                       cct->_conf->rgw_md_log_max_shards,
                                       trim_interval));
    stacks.push_back(meta);

    auto data = new RGWCoroutinesStack(store->ctx(), &crs);
    data->call(create_data_log_trim_cr(store, &http,
                                       cct->_conf->rgw_data_log_num_shards,
                                       trim_interval));
    stacks.push_back(data);

    auto bucket = new RGWCoroutinesStack(store->ctx(), &crs);
    bucket->call(bucket_trim->create_bucket_trim_cr(&http));
    stacks.push_back(bucket);

    crs.run(stacks);
    return 0;
  }

  // implements DoutPrefixProvider
  CephContext *get_cct() const override { return store->ctx(); }
  unsigned get_subsys() const
  {
    return dout_subsys;
  }

  std::ostream& gen_prefix(std::ostream& out) const
  {
    return out << "sync log trim: ";
  }

};

void RGWRados::wakeup_meta_sync_shards(set<int>& shard_ids)
{
  Mutex::Locker l(meta_sync_thread_lock);
  if (meta_sync_processor_thread) {
    meta_sync_processor_thread->wakeup_sync_shards(shard_ids);
  }
}

void RGWRados::wakeup_data_sync_shards(const string& source_zone, map<int, set<string> >& shard_ids)
{
  ldout(ctx(), 20) << __func__ << ": source_zone=" << source_zone << ", shard_ids=" << shard_ids << dendl;
  Mutex::Locker l(data_sync_thread_lock);
  map<string, RGWDataSyncProcessorThread *>::iterator iter = data_sync_processor_threads.find(source_zone);
  if (iter == data_sync_processor_threads.end()) {
    ldout(ctx(), 10) << __func__ << ": couldn't find sync thread for zone " << source_zone << ", skipping async data sync processing" << dendl;
    return;
  }

  RGWDataSyncProcessorThread *thread = iter->second;
  ceph_assert(thread);
  thread->wakeup_sync_shards(shard_ids);
}

RGWMetaSyncStatusManager* RGWRados::get_meta_sync_manager()
{
  Mutex::Locker l(meta_sync_thread_lock);
  if (meta_sync_processor_thread) {
    return meta_sync_processor_thread->get_manager();
  }
  return nullptr;
}

RGWDataSyncStatusManager* RGWRados::get_data_sync_manager(const std::string& source_zone)
{
  Mutex::Locker l(data_sync_thread_lock);
  auto thread = data_sync_processor_threads.find(source_zone);
  if (thread == data_sync_processor_threads.end()) {
    return nullptr;
  }
  return thread->second->get_manager();
}

int RGWRados::get_required_alignment(const rgw_pool& pool, uint64_t *alignment)
{
  IoCtx ioctx;
  int r = open_pool_ctx(pool, ioctx);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: open_pool_ctx() returned " << r << dendl;
    return r;
  }

  bool requires;
  r = ioctx.pool_requires_alignment2(&requires);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: ioctx.pool_requires_alignment2() returned " 
      << r << dendl;
    return r;
  }

  if (!requires) {
    *alignment = 0;
    return 0;
  }

  uint64_t align;
  r = ioctx.pool_required_alignment2(&align);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: ioctx.pool_required_alignment2() returned " 
      << r << dendl;
    return r;
  }
  if (align != 0) {
    ldout(cct, 20) << "required alignment=" << align << dendl;
  }
  *alignment = align;
  return 0;
}

void RGWRados::get_max_aligned_size(uint64_t size, uint64_t alignment, uint64_t *max_size)
{
  if (alignment == 0) {
    *max_size = size;
    return;
  }

  if (size <= alignment) {
    *max_size = alignment;
    return;
  }

  *max_size = size - (size % alignment);
}

int RGWRados::get_max_chunk_size(const rgw_pool& pool, uint64_t *max_chunk_size, uint64_t *palignment)
{
  uint64_t alignment;
  int r = get_required_alignment(pool, &alignment);
  if (r < 0) {
    return r;
  }

  if (palignment) {
    *palignment = alignment;
  }

  uint64_t config_chunk_size = cct->_conf->rgw_max_chunk_size;

  get_max_aligned_size(config_chunk_size, alignment, max_chunk_size);

  ldout(cct, 20) << "max_chunk_size=" << *max_chunk_size << dendl;

  return 0;
}

int RGWRados::get_max_chunk_size(const rgw_placement_rule& placement_rule, const rgw_obj& obj,
                                 uint64_t *max_chunk_size, uint64_t *palignment)
{
  rgw_pool pool;
  if (!get_obj_data_pool(placement_rule, obj, &pool)) {
    ldout(cct, 0) << "ERROR: failed to get data pool for object " << obj << dendl;
    return -EIO;
  }
  return get_max_chunk_size(pool, max_chunk_size, palignment);
}

class RGWIndexCompletionManager;

struct complete_op_data {
  Mutex lock{"complete_op_data"};
  AioCompletion *rados_completion{nullptr};
  int manager_shard_id{-1};
  RGWIndexCompletionManager *manager{nullptr};
  rgw_obj obj;
  RGWModifyOp op;
  string tag;
  rgw_bucket_entry_ver ver;
  cls_rgw_obj_key key;
  rgw_bucket_dir_entry_meta dir_meta;
  list<cls_rgw_obj_key> remove_objs;
  bool log_op;
  uint16_t bilog_op;
  rgw_zone_set zones_trace;

  bool stopped{false};

  void stop() {
    Mutex::Locker l(lock);
    stopped = true;
  }
};

class RGWIndexCompletionThread : public RGWRadosThread {
  RGWRados *store;

  uint64_t interval_msec() override {
    return 0;
  }

  list<complete_op_data *> completions;

  Mutex completions_lock;
public:
  RGWIndexCompletionThread(RGWRados *_store)
    : RGWRadosThread(_store, "index-complete"), store(_store), completions_lock("RGWIndexCompletionThread::completions_lock") {}

  int process() override;

  void add_completion(complete_op_data *completion) {
    {
      Mutex::Locker l(completions_lock);
      completions.push_back(completion);
    }

    signal();
  }
};

int RGWIndexCompletionThread::process()
{
  list<complete_op_data *> comps;

  {
    Mutex::Locker l(completions_lock);
    completions.swap(comps);
  }

  for (auto c : comps) {
    std::unique_ptr<complete_op_data> up{c};

    if (going_down()) {
      continue;
    }
    ldout(store->ctx(), 20) << __func__ << "(): handling completion for key=" << c->key << dendl;

    RGWRados::BucketShard bs(store);
    RGWBucketInfo bucket_info;

    int r = bs.init(c->obj.bucket, c->obj, &bucket_info);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to initialize BucketShard, obj=" << c->obj << " r=" << r << dendl;
      /* not much to do */
      continue;
    }

    r = store->guard_reshard(&bs, c->obj, bucket_info,
			     [&](RGWRados::BucketShard *bs) -> int {
			       librados::ObjectWriteOperation o;
			       cls_rgw_guard_bucket_resharding(o, -ERR_BUSY_RESHARDING);
			       cls_rgw_bucket_complete_op(o, c->op, c->tag, c->ver, c->key, c->dir_meta, &c->remove_objs,
							  c->log_op, c->bilog_op, &c->zones_trace);
			       return bs->index_ctx.operate(bs->bucket_obj, &o);
                             });
    if (r < 0) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): bucket index completion failed, obj=" << c->obj << " r=" << r << dendl;
      /* ignoring error, can't do anything about it */
      continue;
    }
    r = store->data_log->add_entry(bs.bucket, bs.shard_id);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
    }
  }

  return 0;
}

class RGWIndexCompletionManager {
  RGWRados *store{nullptr};
  vector<Mutex *> locks;
  vector<set<complete_op_data *> > completions;

  RGWIndexCompletionThread *completion_thread{nullptr};

  int num_shards;

  std::atomic<int> cur_shard {0};


public:
  RGWIndexCompletionManager(RGWRados *_store) : store(_store) {
    num_shards = store->ctx()->_conf->rgw_thread_pool_size;

    for (int i = 0; i < num_shards; i++) {
      char buf[64];
      snprintf(buf, sizeof(buf), "RGWIndexCompletionManager::lock::%d", i);
      locks.push_back(new Mutex(buf));
    }

    completions.resize(num_shards);
  }
  ~RGWIndexCompletionManager() {
    stop();

    for (auto l : locks) {
      delete l;
    }
  }

  int next_shard() {
    int result = cur_shard % num_shards;
    cur_shard++;
    return result;
  }

  void create_completion(const rgw_obj& obj,
                         RGWModifyOp op, string& tag,
                         rgw_bucket_entry_ver& ver,
                         const cls_rgw_obj_key& key,
                         rgw_bucket_dir_entry_meta& dir_meta,
                         list<cls_rgw_obj_key> *remove_objs, bool log_op,
                         uint16_t bilog_op,
                         rgw_zone_set *zones_trace,
                         complete_op_data **result);
  bool handle_completion(completion_t cb, complete_op_data *arg);

  int start() {
    completion_thread = new RGWIndexCompletionThread(store);
    int ret = completion_thread->init();
    if (ret < 0) {
      return ret;
    }
    completion_thread->start();
    return 0;
  }
  void stop() {
    if (completion_thread) {
      completion_thread->stop();
      delete completion_thread;
    }

    for (int i = 0; i < num_shards; ++i) {
      Mutex::Locker l(*locks[i]);
      for (auto c : completions[i]) {
        c->stop();
      }
    }
    completions.clear();
  }
};

static void obj_complete_cb(completion_t cb, void *arg)
{
  complete_op_data *completion = (complete_op_data *)arg;
  completion->lock.Lock();
  if (completion->stopped) {
    completion->lock.Unlock(); /* can drop lock, no one else is referencing us */
    delete completion;
    return;
  }
  bool need_delete = completion->manager->handle_completion(cb, completion);
  completion->lock.Unlock();
  if (need_delete) {
    delete completion;
  }
}


void RGWIndexCompletionManager::create_completion(const rgw_obj& obj,
                                                  RGWModifyOp op, string& tag,
                                                  rgw_bucket_entry_ver& ver,
                                                  const cls_rgw_obj_key& key,
                                                  rgw_bucket_dir_entry_meta& dir_meta,
                                                  list<cls_rgw_obj_key> *remove_objs, bool log_op,
                                                  uint16_t bilog_op,
                                                  rgw_zone_set *zones_trace,
                                                  complete_op_data **result)
{
  complete_op_data *entry = new complete_op_data;

  int shard_id = next_shard();

  entry->manager_shard_id = shard_id;
  entry->manager = this;
  entry->obj = obj;
  entry->op = op;
  entry->tag = tag;
  entry->ver = ver;
  entry->key = key;
  entry->dir_meta = dir_meta;
  entry->log_op = log_op;
  entry->bilog_op = bilog_op;

  if (remove_objs) {
    for (auto iter = remove_objs->begin(); iter != remove_objs->end(); ++iter) {
      entry->remove_objs.push_back(*iter);
    }
  }

  if (zones_trace) {
    entry->zones_trace = *zones_trace;
  } else {
    entry->zones_trace.insert(store->svc.zone->get_zone().id);
  }

  *result = entry;

  entry->rados_completion = librados::Rados::aio_create_completion(entry, NULL, obj_complete_cb);

  Mutex::Locker l(*locks[shard_id]);
  completions[shard_id].insert(entry);
}

bool RGWIndexCompletionManager::handle_completion(completion_t cb, complete_op_data *arg)
{
  int shard_id = arg->manager_shard_id;
  {
    Mutex::Locker l(*locks[shard_id]);

    auto& comps = completions[shard_id];

    auto iter = comps.find(arg);
    if (iter == comps.end()) {
      return true;
    }

    comps.erase(iter);
  }

  int r = rados_aio_get_return_value(cb);
  if (r != -ERR_BUSY_RESHARDING) {
    return true;
  }
  completion_thread->add_completion(arg);
  return false;
}

void RGWRados::finalize()
{
  cct->get_admin_socket()->unregister_commands(this);

  if (run_sync_thread) {
    Mutex::Locker l(meta_sync_thread_lock);
    meta_sync_processor_thread->stop();

    Mutex::Locker dl(data_sync_thread_lock);
    for (auto iter : data_sync_processor_threads) {
      RGWDataSyncProcessorThread *thread = iter.second;
      thread->stop();
    }
    if (sync_log_trimmer) {
      sync_log_trimmer->stop();
    }
  }
  if (async_rados) {
    async_rados->stop();
  }
  if (run_sync_thread) {
    delete meta_sync_processor_thread;
    meta_sync_processor_thread = NULL;
    Mutex::Locker dl(data_sync_thread_lock);
    for (auto iter : data_sync_processor_threads) {
      RGWDataSyncProcessorThread *thread = iter.second;
      delete thread;
    }
    data_sync_processor_threads.clear();
    delete sync_log_trimmer;
    sync_log_trimmer = nullptr;
    bucket_trim = boost::none;
  }
  if (meta_notifier) {
    meta_notifier->stop();
    delete meta_notifier;
  }
  if (data_notifier) {
    data_notifier->stop();
    delete data_notifier;
  }
  delete data_log;
  delete sync_tracer;
  if (async_rados) {
    delete async_rados;
  }
  
  delete lc;
  lc = NULL; 

  delete gc;
  gc = NULL;

  delete obj_expirer;
  obj_expirer = NULL;

  RGWQuotaHandler::free_handler(quota_handler);
  if (cr_registry) {
    cr_registry->put();
  }

  svc.shutdown();

  delete meta_mgr;
  delete binfo_cache;
  delete obj_tombstone_cache;

  if (reshard_wait.get()) {
    reshard_wait->stop();
    reshard_wait.reset();
  }

  if (run_reshard_thread) {
    reshard->stop_processor();
  }
  delete reshard;
  delete index_completion_manager;
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_rados()
{
  int ret = 0;
  auto admin_socket = cct->get_admin_socket();
  for (auto cmd : admin_commands) {
    int r = admin_socket->register_command(cmd[0], cmd[1], this,
                                           cmd[2]);
    if (r < 0) {
      lderr(cct) << "ERROR: fail to register admin socket command (r=" << r
                 << ")" << dendl;
      return r;
    }
  }

  auto handles = std::vector<librados::Rados>{static_cast<size_t>(cct->_conf->rgw_num_rados_handles)};

  for (auto& r : handles) {
    ret = r.init_with_context(cct);
    if (ret < 0) {
      return ret;
    }
    ret = r.connect();
    if (ret < 0) {
      return ret;
    }
  }

  auto crs = std::unique_ptr<RGWCoroutinesManagerRegistry>{
    new RGWCoroutinesManagerRegistry(cct)};
  ret = crs->hook_to_admin_command("cr dump");
  if (ret < 0) {
    return ret;
  }

  meta_mgr = new RGWMetadataManager(cct, this);
  data_log = new RGWDataChangesLog(cct, this);
  cr_registry = crs.release();

  std::swap(handles, rados);
  return ret;
}

int RGWRados::register_to_service_map(const string& daemon_type, const map<string, string>& meta)
{
  map<string,string> metadata = meta;
  metadata["num_handles"] = stringify(rados.size());
  metadata["zonegroup_id"] = svc.zone->get_zonegroup().get_id();
  metadata["zonegroup_name"] = svc.zone->get_zonegroup().get_name();
  metadata["zone_name"] = svc.zone->zone_name();
  metadata["zone_id"] = svc.zone->zone_id();
  string name = cct->_conf->name.get_id();
  if (name.compare(0, 4, "rgw.") == 0) {
    name = name.substr(4);
  }
  int ret = rados[0].service_daemon_register(daemon_type, name, metadata);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: service_daemon_register() returned ret=" << ret << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::update_service_map(std::map<std::string, std::string>&& status)
{
  int ret = rados[0].service_daemon_update_status(move(status));
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: service_daemon_update_status() returned ret=" << ret << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_complete()
{
  int ret;

  /* 
   * create sync module instance even if we don't run sync thread, might need it for radosgw-admin
   */
  auto& zone_public_config = svc.zone->get_zone();
  ret = svc.sync_modules->get_manager()->create_instance(cct, zone_public_config.tier_type, svc.zone->get_zone_params().tier_config, &sync_module);
  if (ret < 0) {
    lderr(cct) << "ERROR: failed to init sync module instance, ret=" << ret << dendl;
    if (ret == -ENOENT) {
      lderr(cct) << "ERROR: " << zone_public_config.tier_type 
        << " sync module does not exist. valid sync modules: " 
        << svc.sync_modules->get_manager()->get_registered_module_names()
        << dendl;
    }
    return ret;
  }

  period_puller.reset(new RGWPeriodPuller(this));
  period_history.reset(new RGWPeriodHistory(cct, period_puller.get(),
                                            svc.zone->get_current_period()));

  ret = open_root_pool_ctx();
  if (ret < 0)
    return ret;

  ret = open_gc_pool_ctx();
  if (ret < 0)
    return ret;

  ret = open_lc_pool_ctx();
  if (ret < 0)
    return ret;

  ret = open_objexp_pool_ctx();
  if (ret < 0)
    return ret;

  ret = open_reshard_pool_ctx();
  if (ret < 0)
    return ret;

  pools_initialized = true;

  gc = new RGWGC();
  gc->initialize(cct, this);

  obj_expirer = new RGWObjectExpirer(this);

  if (use_gc_thread) {
    gc->start_processor();
    obj_expirer->start_processor();
  }

  auto& current_period = svc.zone->get_current_period();
  auto& zonegroup = svc.zone->get_zonegroup();
  auto& zone_params = svc.zone->get_zone_params();
  auto& zone = svc.zone->get_zone();

  /* no point of running sync thread if we don't have a master zone configured
    or there is no rest_master_conn */
  if (zonegroup.master_zone.empty() || !svc.zone->get_master_conn()
      || current_period.get_id().empty()) {
    run_sync_thread = false;
  }

  if (run_sync_thread) {
    // initialize the log period history
    meta_mgr->init_oldest_log_period();
  }

  async_rados = new RGWAsyncRadosProcessor(this, cct->_conf->rgw_num_async_rados_threads);
  async_rados->start();

  ret = meta_mgr->init(current_period.get_id());
  if (ret < 0) {
    lderr(cct) << "ERROR: failed to initialize metadata log: "
        << cpp_strerror(-ret) << dendl;
    return ret;
  }

  if (svc.zone->is_meta_master()) {
    auto md_log = meta_mgr->get_log(current_period.get_id());
    meta_notifier = new RGWMetaNotifier(this, md_log);
    meta_notifier->start();
  }

  /* init it anyway, might run sync through radosgw-admin explicitly */
  sync_tracer = new RGWSyncTraceManager(cct, cct->_conf->rgw_sync_trace_history_size);
  sync_tracer->init(this);
  ret = sync_tracer->hook_to_admin_command();
  if (ret < 0) {
    return ret;
  }

  if (run_sync_thread) {
    for (const auto &pt: zonegroup.placement_targets) {
      if (zone_params.placement_pools.find(pt.second.name)
          == zone_params.placement_pools.end()){
        ldout(cct, 0) << "WARNING: This zone does not contain the placement target "
                      << pt.second.name << " present in zonegroup" << dendl;
      }
    }
    Mutex::Locker l(meta_sync_thread_lock);
    meta_sync_processor_thread = new RGWMetaSyncProcessorThread(this, async_rados);
    ret = meta_sync_processor_thread->init();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to initialize meta sync thread" << dendl;
      return ret;
    }
    meta_sync_processor_thread->start();

    // configure the bucket trim manager
    rgw::BucketTrimConfig config;
    rgw::configure_bucket_trim(cct, config);

    bucket_trim.emplace(this, config);
    ret = bucket_trim->init();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to start bucket trim manager" << dendl;
      return ret;
    }
    data_log->set_observer(&*bucket_trim);

    Mutex::Locker dl(data_sync_thread_lock);
    for (auto iter : svc.zone->get_zone_data_sync_from_map()) {
      ldout(cct, 5) << "starting data sync thread for zone " << iter.first << dendl;
      auto *thread = new RGWDataSyncProcessorThread(this, async_rados, iter.first);
      ret = thread->init();
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: failed to initialize data sync thread" << dendl;
        return ret;
      }
      thread->start();
      data_sync_processor_threads[iter.first] = thread;
    }
    auto interval = cct->_conf->rgw_sync_log_trim_interval;
    if (interval > 0) {
      sync_log_trimmer = new RGWSyncLogTrimThread(this, &*bucket_trim, interval);
      ret = sync_log_trimmer->init();
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: failed to initialize sync log trim thread" << dendl;
        return ret;
      }
      sync_log_trimmer->start();
    }
  }
  data_notifier = new RGWDataNotifier(this);
  data_notifier->start();

  lc = new RGWLC();
  lc->initialize(cct, this);

  if (use_lc_thread)
    lc->start_processor();

  quota_handler = RGWQuotaHandler::generate_handler(this, quota_threads);

  bucket_index_max_shards = (cct->_conf->rgw_override_bucket_index_max_shards ? cct->_conf->rgw_override_bucket_index_max_shards :
                             zone.bucket_index_max_shards);
  if (bucket_index_max_shards > get_max_bucket_shards()) {
    bucket_index_max_shards = get_max_bucket_shards();
    ldout(cct, 1) << __func__ << " bucket index max shards is too large, reset to value: "
      << get_max_bucket_shards() << dendl;
  }
  ldout(cct, 20) << __func__ << " bucket index max shards: " << bucket_index_max_shards << dendl;

  binfo_cache = new RGWChainedCacheImpl<bucket_info_entry>;
  binfo_cache->init(svc.cache);

  bool need_tombstone_cache = !svc.zone->get_zone_data_notify_to_map().empty(); /* have zones syncing from us */

  if (need_tombstone_cache) {
    obj_tombstone_cache = new tombstone_cache_t(cct->_conf->rgw_obj_tombstone_cache_size);
  }

  reshard_wait = std::make_shared<RGWReshardWait>();

  reshard = new RGWReshard(this);

  /* only the master zone in the zonegroup reshards buckets */
  run_reshard_thread = run_reshard_thread && (zonegroup.master_zone == zone.id);
  if (run_reshard_thread)  {
    reshard->start_processor();
  }

  index_completion_manager = new RGWIndexCompletionManager(this);
  ret = index_completion_manager->start();

  return ret;
}

int RGWRados::init_svc(bool raw)
{
  if (raw) {
    return svc.init_raw(cct, use_cache);
  }

  return svc.init(cct, use_cache);
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::initialize()
{
  int ret;

  inject_notify_timeout_probability =
    cct->_conf.get_val<double>("rgw_inject_notify_timeout_probability");
  max_notify_retries = cct->_conf.get_val<uint64_t>("rgw_max_notify_retries");

  ret = init_svc(false);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
    return ret;
  }

  host_id = svc.zone_utils->gen_host_id();

  ret = init_rados();
  if (ret < 0)
    return ret;

  return init_complete();
}

/**
 * Open the pool used as root for this gateway
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::open_root_pool_ctx()
{
  return rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().domain_root, root_pool_ctx, true);
}

int RGWRados::open_gc_pool_ctx()
{
  return rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().gc_pool, gc_pool_ctx, true);
}

int RGWRados::open_lc_pool_ctx()
{
  return rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().lc_pool, lc_pool_ctx, true);
}

int RGWRados::open_objexp_pool_ctx()
{
  return rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().log_pool, objexp_pool_ctx, true);
}

int RGWRados::open_reshard_pool_ctx()
{
  return rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().reshard_pool, reshard_pool_ctx, true);
}

int RGWRados::open_pool_ctx(const rgw_pool& pool, librados::IoCtx& io_ctx)
{
  constexpr bool create = true; // create the pool if it doesn't exist
  return rgw_init_ioctx(get_rados_handle(), pool, io_ctx, create);
}

void RGWRados::build_bucket_index_marker(const string& shard_id_str, const string& shard_marker,
      string *marker) {
  if (marker) {
    *marker = shard_id_str;
    marker->append(BucketIndexShardsManager::KEY_VALUE_SEPARATOR);
    marker->append(shard_marker);
  }
}

int RGWRados::open_bucket_index_ctx(const RGWBucketInfo& bucket_info, librados::IoCtx& index_ctx)
{
  const rgw_pool& explicit_pool = bucket_info.bucket.explicit_placement.index_pool;

  if (!explicit_pool.empty()) {
    return open_pool_ctx(explicit_pool, index_ctx);
  }

  auto& zonegroup = svc.zone->get_zonegroup();
  auto& zone_params = svc.zone->get_zone_params();

  const rgw_placement_rule *rule = &bucket_info.placement_rule;
  if (rule->empty()) {
    rule = &zonegroup.default_placement;
  }
  auto iter = zone_params.placement_pools.find(rule->name);
  if (iter == zone_params.placement_pools.end()) {
    ldout(cct, 0) << "could not find placement rule " << *rule << " within zonegroup " << dendl;
    return -EINVAL;
  }

  int r = open_pool_ctx(iter->second.index_pool, index_ctx);
  if (r < 0)
    return r;

  return 0;
}

/**** logs ****/

struct log_list_state {
  string prefix;
  librados::IoCtx io_ctx;
  librados::NObjectIterator obit;
};

int RGWRados::log_list_init(const string& prefix, RGWAccessHandle *handle)
{
  log_list_state *state = new log_list_state;
  int r = rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().log_pool, state->io_ctx);
  if (r < 0) {
    delete state;
    return r;
  }
  state->prefix = prefix;
  state->obit = state->io_ctx.nobjects_begin();
  *handle = (RGWAccessHandle)state;
  return 0;
}

int RGWRados::log_list_next(RGWAccessHandle handle, string *name)
{
  log_list_state *state = static_cast<log_list_state *>(handle);
  while (true) {
    if (state->obit == state->io_ctx.nobjects_end()) {
      delete state;
      return -ENOENT;
    }
    if (state->prefix.length() &&
	state->obit->get_oid().find(state->prefix) != 0) {
      state->obit++;
      continue;
    }
    *name = state->obit->get_oid();
    state->obit++;
    break;
  }
  return 0;
}

int RGWRados::log_remove(const string& name)
{
  librados::IoCtx io_ctx;
  int r = rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().log_pool, io_ctx);
  if (r < 0)
    return r;
  return io_ctx.remove(name);
}

struct log_show_state {
  librados::IoCtx io_ctx;
  bufferlist bl;
  bufferlist::const_iterator p;
  string name;
  uint64_t pos;
  bool eof;
  log_show_state() : pos(0), eof(false) {}
};

int RGWRados::log_show_init(const string& name, RGWAccessHandle *handle)
{
  log_show_state *state = new log_show_state;
  int r = rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().log_pool, state->io_ctx);
  if (r < 0) {
    delete state;
    return r;
  }
  state->name = name;
  *handle = (RGWAccessHandle)state;
  return 0;
}

int RGWRados::log_show_next(RGWAccessHandle handle, rgw_log_entry *entry)
{
  log_show_state *state = static_cast<log_show_state *>(handle);
  off_t off = state->p.get_off();

  ldout(cct, 10) << "log_show_next pos " << state->pos << " bl " << state->bl.length()
	   << " off " << off
	   << " eof " << (int)state->eof
	   << dendl;
  // read some?
  unsigned chunk = 1024*1024;
  if ((state->bl.length() - off) < chunk/2 && !state->eof) {
    bufferlist more;
    int r = state->io_ctx.read(state->name, more, chunk, state->pos);
    if (r < 0)
      return r;
    state->pos += r;
    bufferlist old;
    try {
      old.substr_of(state->bl, off, state->bl.length() - off);
    } catch (buffer::error& err) {
      return -EINVAL;
    }
    state->bl.clear();
    state->bl.claim(old);
    state->bl.claim_append(more);
    state->p = state->bl.cbegin();
    if ((unsigned)r < chunk)
      state->eof = true;
    ldout(cct, 10) << " read " << r << dendl;
  }

  if (state->p.end())
    return 0;  // end of file
  try {
    decode(*entry, state->p);
  }
  catch (const buffer::error &e) {
    return -EINVAL;
  }
  return 1;
}

/**
 * usage_log_hash: get usage log key hash, based on name and index
 *
 * Get the usage object name. Since a user may have more than 1
 * object holding that info (multiple shards), we use index to
 * specify that shard number. Once index exceeds max shards it
 * wraps.
 * If name is not being set, results for all users will be returned
 * and index will wrap only after total shards number.
 *
 * @param cct [in] ceph context
 * @param name [in] user name
 * @param hash [out] hash value
 * @param index [in] shard index number 
 */
static void usage_log_hash(CephContext *cct, const string& name, string& hash, uint32_t index)
{
  uint32_t val = index;

  if (!name.empty()) {
    int max_user_shards = cct->_conf->rgw_usage_max_user_shards;
    val %= max_user_shards;
    val += ceph_str_hash_linux(name.c_str(), name.size());
  }
  char buf[17];
  int max_shards = cct->_conf->rgw_usage_max_shards;
  snprintf(buf, sizeof(buf), RGW_USAGE_OBJ_PREFIX "%u", (unsigned)(val % max_shards));
  hash = buf;
}

int RGWRados::log_usage(map<rgw_user_bucket, RGWUsageBatch>& usage_info)
{
  uint32_t index = 0;

  map<string, rgw_usage_log_info> log_objs;

  string hash;
  string last_user;

  /* restructure usage map, zone by object hash */
  map<rgw_user_bucket, RGWUsageBatch>::iterator iter;
  for (iter = usage_info.begin(); iter != usage_info.end(); ++iter) {
    const rgw_user_bucket& ub = iter->first;
    RGWUsageBatch& info = iter->second;

    if (ub.user.empty()) {
      ldout(cct, 0) << "WARNING: RGWRados::log_usage(): user name empty (bucket=" << ub.bucket << "), skipping" << dendl;
      continue;
    }

    if (ub.user != last_user) {
      /* index *should* be random, but why waste extra cycles
         in most cases max user shards is not going to exceed 1,
         so just incrementing it */
      usage_log_hash(cct, ub.user, hash, index++);
    }
    last_user = ub.user;
    vector<rgw_usage_log_entry>& v = log_objs[hash].entries;

    for (auto miter = info.m.begin(); miter != info.m.end(); ++miter) {
      v.push_back(miter->second);
    }
  }

  map<string, rgw_usage_log_info>::iterator liter;

  for (liter = log_objs.begin(); liter != log_objs.end(); ++liter) {
    int r = cls_obj_usage_log_add(liter->first, liter->second);
    if (r < 0)
      return r;
  }
  return 0;
}

int RGWRados::read_usage(const rgw_user& user, const string& bucket_name, uint64_t start_epoch, uint64_t end_epoch,
                         uint32_t max_entries, bool *is_truncated, RGWUsageIter& usage_iter, map<rgw_user_bucket,
			 rgw_usage_log_entry>& usage)
{
  uint32_t num = max_entries;
  string hash, first_hash;
  string user_str = user.to_str();
  usage_log_hash(cct, user_str, first_hash, 0);

  if (usage_iter.index) {
    usage_log_hash(cct, user_str, hash, usage_iter.index);
  } else {
    hash = first_hash;
  }

  usage.clear();

  do {
    map<rgw_user_bucket, rgw_usage_log_entry> ret_usage;
    map<rgw_user_bucket, rgw_usage_log_entry>::iterator iter;

    int ret =  cls_obj_usage_log_read(hash, user_str, bucket_name, start_epoch, end_epoch, num,
                                    usage_iter.read_iter, ret_usage, is_truncated);
    if (ret == -ENOENT)
      goto next;

    if (ret < 0)
      return ret;

    num -= ret_usage.size();

    for (iter = ret_usage.begin(); iter != ret_usage.end(); ++iter) {
      usage[iter->first].aggregate(iter->second);
    }

next:
    if (!*is_truncated) {
      usage_iter.read_iter.clear();
      usage_log_hash(cct, user_str, hash, ++usage_iter.index);
    }
  } while (num && !*is_truncated && hash != first_hash);
  return 0;
}

int RGWRados::trim_usage(const rgw_user& user, const string& bucket_name, uint64_t start_epoch, uint64_t end_epoch)
{
  uint32_t index = 0;
  string hash, first_hash;
  string user_str = user.to_str();
  usage_log_hash(cct, user_str, first_hash, index);

  hash = first_hash;
  do {
    int ret =  cls_obj_usage_log_trim(hash, user_str, bucket_name, start_epoch, end_epoch);

    if (ret < 0 && ret != -ENOENT)
      return ret;

    usage_log_hash(cct, user_str, hash, ++index);
  } while (hash != first_hash);

  return 0;
}


int RGWRados::clear_usage()
{
  auto max_shards = cct->_conf->rgw_usage_max_shards;
  int ret=0;
  for (unsigned i=0; i < max_shards; i++){
    string oid = RGW_USAGE_OBJ_PREFIX + to_string(i);
    ret = cls_obj_usage_log_clear(oid);
    if (ret < 0){
      ldout(cct,0) << "usage clear on oid="<< oid << "failed with ret=" << ret << dendl;
      return ret;
    }
  }
  return ret;
}

int RGWRados::key_to_shard_id(const string& key, int max_shards)
{
  return rgw_shard_id(key, max_shards);
}

void RGWRados::shard_name(const string& prefix, unsigned max_shards, const string& key, string& name, int *shard_id)
{
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size());
  char buf[16];
  if (shard_id) {
    *shard_id = val % max_shards;
  }
  snprintf(buf, sizeof(buf), "%u", (unsigned)(val % max_shards));
  name = prefix + buf;
}

void RGWRados::shard_name(const string& prefix, unsigned max_shards, const string& section, const string& key, string& name)
{
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size());
  val ^= ceph_str_hash_linux(section.c_str(), section.size());
  char buf[16];
  snprintf(buf, sizeof(buf), "%u", (unsigned)(val % max_shards));
  name = prefix + buf;
}

void RGWRados::shard_name(const string& prefix, unsigned shard_id, string& name)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "%u", shard_id);
  name = prefix + buf;

}

void RGWRados::time_log_prepare_entry(cls_log_entry& entry, const real_time& ut, const string& section, const string& key, bufferlist& bl)
{
  cls_log_add_prepare_entry(entry, utime_t(ut), section, key, bl);
}

int RGWRados::time_log_add_init(librados::IoCtx& io_ctx)
{
  return rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().log_pool, io_ctx, true);

}

int RGWRados::time_log_add(const string& oid, const real_time& ut, const string& section, const string& key, bufferlist& bl)
{
  librados::IoCtx io_ctx;

  int r = time_log_add_init(io_ctx);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;
  utime_t t(ut);
  cls_log_add(op, t, section, key, bl);

  return io_ctx.operate(oid, &op);
}

int RGWRados::time_log_add(const string& oid, list<cls_log_entry>& entries,
			   librados::AioCompletion *completion, bool monotonic_inc)
{
  librados::IoCtx io_ctx;

  int r = time_log_add_init(io_ctx);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;
  cls_log_add(op, entries, monotonic_inc);

  if (!completion) {
    r = io_ctx.operate(oid, &op);
  } else {
    r = io_ctx.aio_operate(oid, completion, &op);
  }
  return r;
}

int RGWRados::time_log_list(const string& oid, const real_time& start_time, const real_time& end_time,
                            int max_entries, list<cls_log_entry>& entries,
			    const string& marker,
			    string *out_marker,
			    bool *truncated)
{
  librados::IoCtx io_ctx;

  int r = rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().log_pool, io_ctx);
  if (r < 0)
    return r;
  librados::ObjectReadOperation op;

  utime_t st(start_time);
  utime_t et(end_time);

  cls_log_list(op, st, et, marker, max_entries, entries,
	       out_marker, truncated);

  bufferlist obl;

  int ret = io_ctx.operate(oid, &op, &obl);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::time_log_info(const string& oid, cls_log_header *header)
{
  librados::IoCtx io_ctx;

  int r = rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().log_pool, io_ctx);
  if (r < 0)
    return r;
  librados::ObjectReadOperation op;

  cls_log_info(op, header);

  bufferlist obl;

  int ret = io_ctx.operate(oid, &op, &obl);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::time_log_info_async(librados::IoCtx& io_ctx, const string& oid, cls_log_header *header, librados::AioCompletion *completion)
{
  int r = rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().log_pool, io_ctx);
  if (r < 0)
    return r;

  librados::ObjectReadOperation op;

  cls_log_info(op, header);

  int ret = io_ctx.aio_operate(oid, completion, &op, NULL);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::time_log_trim(const string& oid, const real_time& start_time, const real_time& end_time,
			    const string& from_marker, const string& to_marker,
                            librados::AioCompletion *completion)
{
  librados::IoCtx io_ctx;

  int r = rgw_init_ioctx(get_rados_handle(), svc.zone->get_zone_params().log_pool, io_ctx);
  if (r < 0)
    return r;

  utime_t st(start_time);
  utime_t et(end_time);

  ObjectWriteOperation op;
  cls_log_trim(op, st, et, from_marker, to_marker);

  if (!completion) {
    r = io_ctx.operate(oid, &op);
  } else {
    r = io_ctx.aio_operate(oid, completion, &op);
  }
  return r;
}

string RGWRados::objexp_hint_get_shardname(int shard_num)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%010u", (unsigned)shard_num);

  string objname("obj_delete_at_hint.");
  return objname + buf;
}

int RGWRados::objexp_key_shard(const rgw_obj_index_key& key)
{
  string obj_key = key.name + key.instance;
  int num_shards = cct->_conf->rgw_objexp_hints_num_shards;
  return rgw_bucket_shard_index(obj_key, num_shards);
}

static string objexp_hint_get_keyext(const string& tenant_name,
                                     const string& bucket_name,
                                     const string& bucket_id,
                                     const rgw_obj_key& obj_key)
{
  return tenant_name + (tenant_name.empty() ? "" : ":") + bucket_name + ":" + bucket_id +
      ":" + obj_key.name + ":" + obj_key.instance;
}

int RGWRados::objexp_hint_add(const ceph::real_time& delete_at,
                              const string& tenant_name,
                              const string& bucket_name,
                              const string& bucket_id,
                              const rgw_obj_index_key& obj_key)
{
  const string keyext = objexp_hint_get_keyext(tenant_name, bucket_name,
          bucket_id, obj_key);
  objexp_hint_entry he = {
      .tenant = tenant_name,
      .bucket_name = bucket_name,
      .bucket_id = bucket_id,
      .obj_key = obj_key,
      .exp_time = delete_at };
  bufferlist hebl;
  encode(he, hebl);
  ObjectWriteOperation op;
  cls_timeindex_add(op, utime_t(delete_at), keyext, hebl);

  string shard_name = objexp_hint_get_shardname(objexp_key_shard(obj_key));
  return objexp_pool_ctx.operate(shard_name, &op);
}

void  RGWRados::objexp_get_shard(int shard_num,
                                 string& shard)                       /* out */
{
  shard = objexp_hint_get_shardname(shard_num);
}

int RGWRados::objexp_hint_list(const string& oid,
                               const ceph::real_time& start_time,
                               const ceph::real_time& end_time,
                               const int max_entries,
                               const string& marker,
                               list<cls_timeindex_entry>& entries, /* out */
                               string *out_marker,                 /* out */
                               bool *truncated)                    /* out */
{
  librados::ObjectReadOperation op;
  cls_timeindex_list(op, utime_t(start_time), utime_t(end_time), marker, max_entries, entries,
        out_marker, truncated);

  bufferlist obl;
  int ret = objexp_pool_ctx.operate(oid, &op, &obl);

  if ((ret < 0 ) && (ret != -ENOENT)) {
    return ret;
  }

  if ((ret == -ENOENT) && truncated) {
    *truncated = false;
  }

  return 0;
}

int RGWRados::objexp_hint_parse(cls_timeindex_entry &ti_entry,  /* in */
                                objexp_hint_entry& hint_entry)  /* out */
{
  try {
    auto iter = ti_entry.value.cbegin();
    decode(hint_entry, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: couldn't decode avail_pools" << dendl;
  }

  return 0;
}

int RGWRados::objexp_hint_trim(const string& oid,
                               const ceph::real_time& start_time,
                               const ceph::real_time& end_time,
                               const string& from_marker,
                               const string& to_marker)
{
  int ret = cls_timeindex_trim(objexp_pool_ctx, oid, utime_t(start_time), utime_t(end_time),
          from_marker, to_marker);
  if ((ret < 0 ) && (ret != -ENOENT)) {
    return ret;
  }

  return 0;
}

int RGWRados::lock_exclusive(const rgw_pool& pool, const string& oid, timespan& duration,
                             string& zone_id, string& owner_id) {
  librados::IoCtx io_ctx;

  int r = rgw_init_ioctx(get_rados_handle(), pool, io_ctx);
  if (r < 0) {
    return r;
  }
  uint64_t msec = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
  utime_t ut(msec / 1000, msec % 1000);
  
  rados::cls::lock::Lock l(log_lock_name);
  l.set_duration(ut);
  l.set_cookie(owner_id);
  l.set_tag(zone_id);
  l.set_may_renew(true);
  
  return l.lock_exclusive(&io_ctx, oid);
}

int RGWRados::unlock(const rgw_pool& pool, const string& oid, string& zone_id, string& owner_id) {
  librados::IoCtx io_ctx;

  int r = rgw_init_ioctx(get_rados_handle(), pool, io_ctx);
  if (r < 0) {
    return r;
  }
  
  rados::cls::lock::Lock l(log_lock_name);
  l.set_tag(zone_id);
  l.set_cookie(owner_id);
  
  return l.unlock(&io_ctx, oid);
}

int RGWRados::decode_policy(bufferlist& bl, ACLOwner *owner)
{
  auto i = bl.cbegin();
  RGWAccessControlPolicy policy(cct);
  try {
    policy.decode_owner(i);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  *owner = policy.get_owner();
  return 0;
}

int rgw_policy_from_attrset(CephContext *cct, map<string, bufferlist>& attrset, RGWAccessControlPolicy *policy)
{
  map<string, bufferlist>::iterator aiter = attrset.find(RGW_ATTR_ACL);
  if (aiter == attrset.end())
    return -EIO;

  bufferlist& bl = aiter->second;
  auto iter = bl.cbegin();
  try {
    policy->decode(iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(policy);
    ldout(cct, 15) << __func__ << " Read AccessControlPolicy";
    s3policy->to_xml(*_dout);
    *_dout << dendl;
  }
  return 0;
}


int RGWRados::Bucket::update_bucket_id(const string& new_bucket_id)
{
  rgw_bucket bucket = bucket_info.bucket;
  bucket.update_bucket_id(new_bucket_id);

  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  int ret = store->get_bucket_instance_info(obj_ctx, bucket, bucket_info, nullptr, nullptr);
  if (ret < 0) {
    return ret;
  }

  return 0;
}


/**
 * Get ordered listing of the objects in a bucket.
 *
 * max: maximum number of results to return
 * bucket: bucket to list contents of
 * prefix: only return results that match this prefix
 * delim: do not include results that match this string.
 *     Any skipped results will have the matching portion of their name
 *     inserted in common_prefixes with a "true" mark.
 * marker: if filled in, begin the listing with this object.
 * end_marker: if filled in, end the listing with this object.
 * result: the objects are put in here.
 * common_prefixes: if delim is filled in, any matching prefixes are placed here.
 * is_truncated: if number of objects in the bucket is bigger than max, then truncated.
 */
int RGWRados::Bucket::List::list_objects_ordered(int64_t max,
						 vector<rgw_bucket_dir_entry> *result,
						 map<string, bool> *common_prefixes,
						 bool *is_truncated)
{
  RGWRados *store = target->get_store();
  CephContext *cct = store->ctx();
  int shard_id = target->get_shard_id();

  int count = 0;
  bool truncated = true;
  int read_ahead = std::max(cct->_conf->rgw_list_bucket_min_readahead,max);

  result->clear();

  rgw_obj_key marker_obj(params.marker.name, params.marker.instance, params.ns);
  rgw_obj_index_key cur_marker;
  marker_obj.get_index_key(&cur_marker);

  rgw_obj_key end_marker_obj(params.end_marker.name, params.end_marker.instance,
                             params.ns);
  rgw_obj_index_key cur_end_marker;
  end_marker_obj.get_index_key(&cur_end_marker);
  const bool cur_end_marker_valid = !params.end_marker.empty();

  rgw_obj_key prefix_obj(params.prefix);
  prefix_obj.ns = params.ns;
  string cur_prefix = prefix_obj.get_index_key_name();

  string bigger_than_delim;

  if (!params.delim.empty()) {
    unsigned long val = decode_utf8((unsigned char *)params.delim.c_str(),
				    params.delim.size());
    char buf[params.delim.size() + 16];
    int r = encode_utf8(val + 1, (unsigned char *)buf);
    if (r < 0) {
      ldout(cct,0) << "ERROR: encode_utf8() failed" << dendl;
      return -EINVAL;
    }
    buf[r] = '\0';

    bigger_than_delim = buf;

    /* if marker points at a common prefix, fast forward it into its upperbound string */
    int delim_pos = cur_marker.name.find(params.delim, cur_prefix.size());
    if (delim_pos >= 0) {
      string s = cur_marker.name.substr(0, delim_pos);
      s.append(bigger_than_delim);
      cur_marker = s;
    }
  }

  string skip_after_delim;
  while (truncated && count <= max) {
    if (skip_after_delim > cur_marker.name) {
      cur_marker = skip_after_delim;
      ldout(cct, 20) << "setting cur_marker=" << cur_marker.name << "[" << cur_marker.instance << "]" << dendl;
    }
    std::map<string, rgw_bucket_dir_entry> ent_map;
    int r = store->cls_bucket_list_ordered(target->get_bucket_info(),
					   shard_id,
					   cur_marker,
					   cur_prefix,
					   read_ahead + 1 - count,
					   params.list_versions,
					   ent_map,
					   &truncated,
					   &cur_marker);
    if (r < 0)
      return r;

    for (auto eiter = ent_map.begin(); eiter != ent_map.end(); ++eiter) {
      rgw_bucket_dir_entry& entry = eiter->second;
      rgw_obj_index_key index_key = entry.key;

      rgw_obj_key obj(index_key);

      /* note that parse_raw_oid() here will not set the correct
       * object's instance, as rgw_obj_index_key encodes that
       * separately. We don't need to set the instance because it's
       * not needed for the checks here and we end up using the raw
       * entry for the return vector
       */
      bool valid = rgw_obj_key::parse_raw_oid(index_key.name, &obj);
      if (!valid) {
        ldout(cct, 0) << "ERROR: could not parse object name: " << obj.name << dendl;
        continue;
      }
      bool check_ns = (obj.ns == params.ns);
      if (!params.list_versions && !entry.is_visible()) {
        continue;
      }

      if (params.enforce_ns && !check_ns) {
        if (!params.ns.empty()) {
          /* we've iterated past the namespace we're searching -- done now */
          truncated = false;
          goto done;
        }

        /* we're not looking at the namespace this object is in, next! */
        continue;
      }

      if (cur_end_marker_valid && cur_end_marker <= index_key) {
        truncated = false;
        goto done;
      }

      if (count < max) {
        params.marker = index_key;
        next_marker = index_key;
      }

      if (params.filter && !params.filter->filter(obj.name, index_key.name))
        continue;

      if (params.prefix.size() &&
	  (obj.name.compare(0, params.prefix.size(), params.prefix) != 0))
        continue;

      if (!params.delim.empty()) {
        int delim_pos = obj.name.find(params.delim, params.prefix.size());

        if (delim_pos >= 0) {
          string prefix_key = obj.name.substr(0, delim_pos + 1);

          if (common_prefixes &&
              common_prefixes->find(prefix_key) == common_prefixes->end()) {
            if (count >= max) {
              truncated = true;
              goto done;
            }
            next_marker = prefix_key;
            (*common_prefixes)[prefix_key] = true;

            int marker_delim_pos = cur_marker.name.find(params.delim, cur_prefix.size());

            skip_after_delim = cur_marker.name.substr(0, marker_delim_pos);
            skip_after_delim.append(bigger_than_delim);

            ldout(cct, 20) << "skip_after_delim=" << skip_after_delim << dendl;

            count++;
          }

          continue;
        }
      }

      if (count >= max) {
        truncated = true;
        goto done;
      }

      result->emplace_back(std::move(entry));
      count++;
    }
  }

done:
  if (is_truncated)
    *is_truncated = truncated;

  return 0;
} // list_objects_ordered


/**
 * Get listing of the objects in a bucket and allow the results to be out
 * of order.
 *
 * Even though there are key differences with the ordered counterpart,
 * the parameters are the same to maintain some compatability.
 *
 * max: maximum number of results to return
 * bucket: bucket to list contents of
 * prefix: only return results that match this prefix
 * delim: should not be set; if it is we should have indicated an error
 * marker: if filled in, begin the listing with this object.
 * end_marker: if filled in, end the listing with this object.
 * result: the objects are put in here.
 * common_prefixes: this is never filled with an unordered list; the param
 *                  is maintained for compatibility
 * is_truncated: if number of objects in the bucket is bigger than max, then
 *               truncated.
 */
int RGWRados::Bucket::List::list_objects_unordered(int64_t max,
						   vector<rgw_bucket_dir_entry> *result,
						   map<string, bool> *common_prefixes,
						   bool *is_truncated)
{
  RGWRados *store = target->get_store();
  CephContext *cct = store->ctx();
  int shard_id = target->get_shard_id();

  int count = 0;
  bool truncated = true;

  // read a few extra in each call to cls_bucket_list_unordered in
  // case some are filtered out due to namespace matching, versioning,
  // filtering, etc.
  const int64_t max_read_ahead = 100;
  const uint32_t read_ahead = uint32_t(max + std::min(max, max_read_ahead));

  result->clear();

  rgw_obj_key marker_obj(params.marker.name, params.marker.instance, params.ns);
  rgw_obj_index_key cur_marker;
  marker_obj.get_index_key(&cur_marker);

  rgw_obj_key end_marker_obj(params.end_marker.name, params.end_marker.instance,
                             params.ns);
  rgw_obj_index_key cur_end_marker;
  end_marker_obj.get_index_key(&cur_end_marker);
  const bool cur_end_marker_valid = !params.end_marker.empty();

  rgw_obj_key prefix_obj(params.prefix);
  prefix_obj.ns = params.ns;
  string cur_prefix = prefix_obj.get_index_key_name();

  while (truncated && count <= max) {
    std::vector<rgw_bucket_dir_entry> ent_list;
    int r = store->cls_bucket_list_unordered(target->get_bucket_info(),
					     shard_id,
					     cur_marker,
					     cur_prefix,
					     read_ahead,
					     params.list_versions,
					     ent_list,
					     &truncated,
					     &cur_marker);
    if (r < 0)
      return r;

    // NB: while regions of ent_list will be sorted, we have no
    // guarantee that all items will be sorted since they can cross
    // shard boundaries

    for (auto& entry : ent_list) {
      rgw_obj_index_key index_key = entry.key;
      rgw_obj_key obj(index_key);

      /* note that parse_raw_oid() here will not set the correct
       * object's instance, as rgw_obj_index_key encodes that
       * separately. We don't need to set the instance because it's
       * not needed for the checks here and we end up using the raw
       * entry for the return vector
       */
      bool valid = rgw_obj_key::parse_raw_oid(index_key.name, &obj);
      if (!valid) {
        ldout(cct, 0) << "ERROR: could not parse object name: " <<
	  obj.name << dendl;
        continue;
      }

      if (!params.list_versions && !entry.is_visible()) {
        continue;
      }

      if (params.enforce_ns && obj.ns != params.ns) {
        continue;
      }

      if (cur_end_marker_valid && cur_end_marker <= index_key) {
	// we're not guaranteed items will come in order, so we have
	// to loop through all
	continue;
      }

      if (count < max) {
        params.marker = index_key;
        next_marker = index_key;
      }

      if (params.filter && !params.filter->filter(obj.name, index_key.name))
        continue;

      if (params.prefix.size() &&
	  (0 != obj.name.compare(0, params.prefix.size(), params.prefix)))
        continue;

      if (count >= max) {
        truncated = true;
        goto done;
      }

      result->emplace_back(std::move(entry));
      count++;
    } // for (auto& entry : ent_list)
  } // while (truncated && count <= max)

done:
  if (is_truncated)
    *is_truncated = truncated;

  return 0;
} // list_objects_unordered


/**
 * create a rados pool, associated meta info
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_pool(const rgw_pool& pool)
{
  librados::IoCtx io_ctx;
  constexpr bool create = true;
  return rgw_init_ioctx(get_rados_handle(), pool, io_ctx, create);
}

int RGWRados::init_bucket_index(RGWBucketInfo& bucket_info, int num_shards)
{
  librados::IoCtx index_ctx;

  string dir_oid = dir_oid_prefix;
  int r = open_bucket_index_ctx(bucket_info, index_ctx);
  if (r < 0) {
    return r;
  }

  dir_oid.append(bucket_info.bucket.bucket_id);

  map<int, string> bucket_objs;
  get_bucket_index_objects(dir_oid, num_shards, bucket_objs);

  return CLSRGWIssueBucketIndexInit(index_ctx,
				    bucket_objs,
				    cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWRados::clean_bucket_index(RGWBucketInfo& bucket_info, int num_shards)
{
  librados::IoCtx index_ctx;

  std::string dir_oid = dir_oid_prefix;
  int r = open_bucket_index_ctx(bucket_info, index_ctx);
  if (r < 0) {
    return r;
  }

  dir_oid.append(bucket_info.bucket.bucket_id);

  std::map<int, std::string> bucket_objs;
  get_bucket_index_objects(dir_oid, num_shards, bucket_objs);

  return CLSRGWIssueBucketIndexClean(index_ctx,
				     bucket_objs,
				     cct->_conf->rgw_bucket_index_max_aio)();
}

void RGWRados::create_bucket_id(string *bucket_id)
{
  uint64_t iid = instance_id();
  uint64_t bid = next_bucket_id();
  char buf[svc.zone->get_zone_params().get_id().size() + 48];
  snprintf(buf, sizeof(buf), "%s.%" PRIu64 ".%" PRIu64,
           svc.zone->get_zone_params().get_id().c_str(), iid, bid);
  *bucket_id = buf;
}

int RGWRados::create_bucket(const RGWUserInfo& owner, rgw_bucket& bucket,
                            const string& zonegroup_id,
                            const rgw_placement_rule& placement_rule,
                            const string& swift_ver_location,
                            const RGWQuotaInfo * pquota_info,
			    map<std::string, bufferlist>& attrs,
                            RGWBucketInfo& info,
                            obj_version *pobjv,
                            obj_version *pep_objv,
                            real_time creation_time,
                            rgw_bucket *pmaster_bucket,
                            uint32_t *pmaster_num_shards,
			    bool exclusive)
{
#define MAX_CREATE_RETRIES 20 /* need to bound retries */
  rgw_placement_rule selected_placement_rule;
  RGWZonePlacementInfo rule_info;

  for (int i = 0; i < MAX_CREATE_RETRIES; i++) {
    int ret = 0;
    ret = svc.zone->select_bucket_placement(owner, zonegroup_id, placement_rule,
                                            &selected_placement_rule, &rule_info);
    if (ret < 0)
      return ret;

    if (!pmaster_bucket) {
      create_bucket_id(&bucket.marker);
      bucket.bucket_id = bucket.marker;
    } else {
      bucket.marker = pmaster_bucket->marker;
      bucket.bucket_id = pmaster_bucket->bucket_id;
    }

    RGWObjVersionTracker& objv_tracker = info.objv_tracker;

    if (pobjv) {
      objv_tracker.write_version = *pobjv;
    } else {
      objv_tracker.generate_new_write_ver(cct);
    }

    info.bucket = bucket;
    info.owner = owner.user_id;
    info.zonegroup = zonegroup_id;
    info.placement_rule = selected_placement_rule;
    info.index_type = rule_info.index_type;
    info.swift_ver_location = swift_ver_location;
    info.swift_versioning = (!swift_ver_location.empty());
    if (pmaster_num_shards) {
      info.num_shards = *pmaster_num_shards;
    } else {
      info.num_shards = bucket_index_max_shards;
    }
    info.bucket_index_shard_hash_type = RGWBucketInfo::MOD;
    info.requester_pays = false;
    if (real_clock::is_zero(creation_time)) {
      info.creation_time = ceph::real_clock::now();
    } else {
      info.creation_time = creation_time;
    }
    if (pquota_info) {
      info.quota = *pquota_info;
    }

    int r = init_bucket_index(info, info.num_shards);
    if (r < 0) {
      return r;
    }

    ret = put_linked_bucket_info(info, exclusive, ceph::real_time(), pep_objv, &attrs, true);
    if (ret == -EEXIST) {
      librados::IoCtx index_ctx;
      map<int, string> bucket_objs;
      int r = open_bucket_index(info, index_ctx, bucket_objs);
      if (r < 0)
        return r;

       /* we need to reread the info and return it, caller will have a use for it */
      RGWObjVersionTracker instance_ver = info.objv_tracker;
      info.objv_tracker.clear();
      auto obj_ctx = svc.sysobj->init_obj_ctx();
      r = get_bucket_info(obj_ctx, bucket.tenant, bucket.name, info, NULL, NULL);
      if (r < 0) {
        if (r == -ENOENT) {
          continue;
        }
        ldout(cct, 0) << "get_bucket_info returned " << r << dendl;
        return r;
      }

      /* only remove it if it's a different bucket instance */
      if (info.bucket.bucket_id != bucket.bucket_id) {
        /* remove bucket meta instance */
        r = rgw_bucket_instance_remove_entry(this,
					     bucket.get_key(),
					     &instance_ver);
        if (r < 0)
          return r;

	/* remove bucket index objects asynchronously by best effort */
	(void) CLSRGWIssueBucketIndexClean(index_ctx,
					   bucket_objs,
					   cct->_conf->rgw_bucket_index_max_aio)();
      }
      /* ret == -ENOENT here */
    }
    return ret;
  }

  /* this is highly unlikely */
  ldout(cct, 0) << "ERROR: could not create bucket, continuously raced with bucket creation and removal" << dendl;
  return -ENOENT;
}

bool RGWRados::get_obj_data_pool(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_pool *pool)
{
  return rgw_get_obj_data_pool(svc.zone->get_zonegroup(), svc.zone->get_zone_params(), placement_rule, obj, pool);
}

bool RGWRados::obj_to_raw(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj *raw_obj)
{
  get_obj_bucket_and_oid_loc(obj, raw_obj->oid, raw_obj->loc);

  return get_obj_data_pool(placement_rule, obj, &raw_obj->pool);
}

int RGWRados::get_obj_head_ioctx(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::IoCtx *ioctx)
{
  string oid, key;
  get_obj_bucket_and_oid_loc(obj, oid, key);

  rgw_pool pool;
  if (!get_obj_data_pool(bucket_info.placement_rule, obj, &pool)) {
    ldout(cct, 0) << "ERROR: cannot get data pool for obj=" << obj << ", probably misconfiguration" << dendl;
    return -EIO;
  }

  int r = open_pool_ctx(pool, *ioctx);
  if (r < 0) {
    return r;
  }

  ioctx->locator_set_key(key);

  return 0;
}

int RGWRados::get_obj_head_ref(const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_rados_ref *ref)
{
  get_obj_bucket_and_oid_loc(obj, ref->obj.oid, ref->obj.loc);

  rgw_pool pool;
  if (!get_obj_data_pool(bucket_info.placement_rule, obj, &pool)) {
    ldout(cct, 0) << "ERROR: cannot get data pool for obj=" << obj << ", probably misconfiguration" << dendl;
    return -EIO;
  }

  int r = open_pool_ctx(pool, ref->ioctx);
  if (r < 0) {
    return r;
  }

  ref->ioctx.locator_set_key(ref->obj.loc);

  return 0;
}

int RGWRados::get_raw_obj_ref(const rgw_raw_obj& obj, rgw_rados_ref *ref)
{
  ref->obj = obj;

  int r;

  if (ref->obj.oid.empty()) {
    ref->obj.oid = obj.pool.to_str();
    ref->obj.pool = svc.zone->get_zone_params().domain_root;
  }
  r = open_pool_ctx(ref->obj.pool, ref->ioctx);
  if (r < 0)
    return r;

  ref->ioctx.locator_set_key(ref->obj.loc);

  return 0;
}

int RGWRados::get_system_obj_ref(const rgw_raw_obj& obj, rgw_rados_ref *ref)
{
  return get_raw_obj_ref(obj, ref);
}

/*
 * fixes an issue where head objects were supposed to have a locator created, but ended
 * up without one
 */
int RGWRados::fix_head_obj_locator(const RGWBucketInfo& bucket_info, bool copy_obj, bool remove_bad, rgw_obj_key& key)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  string oid;
  string locator;

  rgw_obj obj(bucket, key);

  get_obj_bucket_and_oid_loc(obj, oid, locator);

  if (locator.empty()) {
    ldout(cct, 20) << "object does not have a locator, nothing to fix" << dendl;
    return 0;
  }

  librados::IoCtx ioctx;

  int ret = get_obj_head_ioctx(bucket_info, obj, &ioctx);
  if (ret < 0) {
    cerr << "ERROR: get_obj_head_ioctx() returned ret=" << ret << std::endl;
    return ret;
  }
  ioctx.locator_set_key(string()); /* override locator for this object, use empty locator */

  uint64_t size;
  bufferlist data;

  struct timespec mtime_ts;
  map<string, bufferlist> attrs;
  librados::ObjectReadOperation op;
  op.getxattrs(&attrs, NULL);
  op.stat2(&size, &mtime_ts, NULL);
#define HEAD_SIZE 512 * 1024
  op.read(0, HEAD_SIZE, &data, NULL);

  ret = ioctx.operate(oid, &op, NULL);
  if (ret < 0) {
    lderr(cct) << "ERROR: ioctx.operate(oid=" << oid << ") returned ret=" << ret << dendl;
    return ret;
  }

  if (size > HEAD_SIZE) {
    lderr(cct) << "ERROR: returned object size (" << size << ") > HEAD_SIZE (" << HEAD_SIZE << ")" << dendl;
    return -EIO;
  }

  if (size != data.length()) {
    lderr(cct) << "ERROR: returned object size (" << size << ") != data.length() (" << data.length() << ")" << dendl;
    return -EIO;
  }

  if (copy_obj) {
    librados::ObjectWriteOperation wop;

    wop.mtime2(&mtime_ts);

    map<string, bufferlist>::iterator iter;
    for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
      wop.setxattr(iter->first.c_str(), iter->second);
    }

    wop.write(0, data);

    ioctx.locator_set_key(locator);
    ioctx.operate(oid, &wop);
  }

  if (remove_bad) {
    ioctx.locator_set_key(string());

    ret = ioctx.remove(oid);
    if (ret < 0) {
      lderr(cct) << "ERROR: failed to remove original bad object" << dendl;
      return ret;
    }
  }

  return 0;
}

int RGWRados::move_rados_obj(librados::IoCtx& src_ioctx,
			     const string& src_oid, const string& src_locator,
		             librados::IoCtx& dst_ioctx,
			     const string& dst_oid, const string& dst_locator)
{

#define COPY_BUF_SIZE (4 * 1024 * 1024)
  bool done = false;
  uint64_t chunk_size = COPY_BUF_SIZE;
  uint64_t ofs = 0;
  int ret = 0;
  real_time mtime;
  struct timespec mtime_ts;
  uint64_t size;

  if (src_oid == dst_oid && src_locator == dst_locator) {
    return 0;
  }

  src_ioctx.locator_set_key(src_locator);
  dst_ioctx.locator_set_key(dst_locator);

  do {
    bufferlist data;
    ObjectReadOperation rop;
    ObjectWriteOperation wop;

    if (ofs == 0) {
      rop.stat2(&size, &mtime_ts, NULL);
      mtime = real_clock::from_timespec(mtime_ts);
    }
    rop.read(ofs, chunk_size, &data, NULL);
    ret = src_ioctx.operate(src_oid, &rop, NULL);
    if (ret < 0) {
      goto done_err;
    }

    if (data.length() == 0) {
      break;
    }

    if (ofs == 0) {
      wop.create(true); /* make it exclusive */
      wop.mtime2(&mtime_ts);
      mtime = real_clock::from_timespec(mtime_ts);
    }
    wop.write(ofs, data);
    ret = dst_ioctx.operate(dst_oid, &wop);
    if (ret < 0) {
      goto done_err;
    }
    ofs += data.length();
    done = data.length() != chunk_size;
  } while (!done);

  if (ofs != size) {
    lderr(cct) << "ERROR: " << __func__ << ": copying " << src_oid << " -> " << dst_oid
               << ": expected " << size << " bytes to copy, ended up with " << ofs << dendl;
    ret = -EIO;
    goto done_err;
  }

  src_ioctx.remove(src_oid);

  return 0;

done_err:
  // TODO: clean up dst_oid if we created it
  lderr(cct) << "ERROR: failed to copy " << src_oid << " -> " << dst_oid << dendl;
  return ret;
}

/*
 * fixes an issue where head objects were supposed to have a locator created, but ended
 * up without one
 */
int RGWRados::fix_tail_obj_locator(const RGWBucketInfo& bucket_info, rgw_obj_key& key, bool fix, bool *need_fix)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  rgw_obj obj(bucket, key);

  if (need_fix) {
    *need_fix = false;
  }

  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }

  RGWObjState *astate = NULL;
  RGWObjectCtx rctx(this);
  r = get_obj_state(&rctx, bucket_info, obj, &astate, false);
  if (r < 0)
    return r;

  if (astate->has_manifest) {
    RGWObjManifest::obj_iterator miter;
    RGWObjManifest& manifest = astate->manifest;
    for (miter = manifest.obj_begin(); miter != manifest.obj_end(); ++miter) {
      rgw_raw_obj raw_loc = miter.get_location().get_raw_obj(this);
      rgw_obj loc;
      string oid;
      string locator;

      rgw_raw_obj_to_obj(manifest.get_tail_placement().bucket, raw_loc, &loc);

      if (loc.key.ns.empty()) {
	/* continue, we're only interested in tail objects */
	continue;
      }

      get_obj_bucket_and_oid_loc(loc, oid, locator);
      ref.ioctx.locator_set_key(locator);

      ldout(cct, 20) << __func__ << ": key=" << key << " oid=" << oid << " locator=" << locator << dendl;

      r = ref.ioctx.stat(oid, NULL, NULL);
      if (r != -ENOENT) {
	continue;
      }

      string bad_loc;
      prepend_bucket_marker(bucket, loc.key.name, bad_loc);

      /* create a new ioctx with the bad locator */
      librados::IoCtx src_ioctx;
      src_ioctx.dup(ref.ioctx);
      src_ioctx.locator_set_key(bad_loc);

      r = src_ioctx.stat(oid, NULL, NULL);
      if (r != 0) {
	/* cannot find a broken part */
	continue;
      }
      ldout(cct, 20) << __func__ << ": found bad object part: " << loc << dendl;
      if (need_fix) {
        *need_fix = true;
      }
      if (fix) {
        r = move_rados_obj(src_ioctx, oid, bad_loc, ref.ioctx, oid, locator);
        if (r < 0) {
          lderr(cct) << "ERROR: copy_rados_obj() on oid=" << oid << " returned r=" << r << dendl;
        }
      }
    }
  }

  return 0;
}

int RGWRados::BucketShard::init(const rgw_bucket& _bucket,
				const rgw_obj& obj,
				RGWBucketInfo* bucket_info_out)
{
  bucket = _bucket;

  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  RGWBucketInfo bucket_info;
  RGWBucketInfo* bucket_info_p =
    bucket_info_out ? bucket_info_out : &bucket_info;
  
  int ret = store->get_bucket_instance_info(obj_ctx, bucket, *bucket_info_p, NULL, NULL);
  if (ret < 0) {
    return ret;
  }

  ret = store->open_bucket_index_shard(*bucket_info_p, index_ctx, obj.get_hash_object(), &bucket_obj, &shard_id);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: open_bucket_index_shard() returned ret=" << ret << dendl;
    return ret;
  }
  ldout(store->ctx(), 20) << " bucket index object: " << bucket_obj << dendl;

  return 0;
}

int RGWRados::BucketShard::init(const rgw_bucket& _bucket,
				int sid,
				RGWBucketInfo* bucket_info_out)
{
  bucket = _bucket;
  shard_id = sid;

  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  RGWBucketInfo bucket_info;
  RGWBucketInfo* bucket_info_p =
    bucket_info_out ? bucket_info_out : &bucket_info;
  int ret = store->get_bucket_instance_info(obj_ctx, bucket, *bucket_info_p, NULL, NULL);
  if (ret < 0) {
    return ret;
  }

  ret = store->open_bucket_index_shard(*bucket_info_p, index_ctx, shard_id, &bucket_obj);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: open_bucket_index_shard() returned ret=" << ret << dendl;
    return ret;
  }
  ldout(store->ctx(), 20) << " bucket index object: " << bucket_obj << dendl;

  return 0;
}

int RGWRados::BucketShard::init(const RGWBucketInfo& bucket_info,
                                const rgw_obj& obj)
{
  bucket = bucket_info.bucket;

  int ret = store->open_bucket_index_shard(bucket_info, index_ctx,
                                           obj.get_hash_object(), &bucket_obj,
                                           &shard_id);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: open_bucket_index_shard() returned ret=" << ret << dendl;
    return ret;
  }
  ldout(store->ctx(), 20) << " bucket index object: " << bucket_obj << dendl;

  return 0;
}

int RGWRados::BucketShard::init(const RGWBucketInfo& bucket_info, int sid)
{
  bucket = bucket_info.bucket;
  shard_id = sid;

  int ret = store->open_bucket_index_shard(bucket_info, index_ctx, shard_id, &bucket_obj);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: open_bucket_index_shard() returned ret=" << ret << dendl;
    return ret;
  }
  ldout(store->ctx(), 20) << " bucket index object: " << bucket_obj << dendl;

  return 0;
}


/* Execute @handler on last item in bucket listing for bucket specified
 * in @bucket_info. @obj_prefix and @obj_delim narrow down the listing
 * to objects matching these criterias. */
int RGWRados::on_last_entry_in_listing(RGWBucketInfo& bucket_info,
                                       const std::string& obj_prefix,
                                       const std::string& obj_delim,
                                       std::function<int(const rgw_bucket_dir_entry&)> handler)
{
  RGWRados::Bucket target(this, bucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.prefix = obj_prefix;
  list_op.params.delim = obj_delim;

  ldout(cct, 20) << "iterating listing for bucket=" << bucket_info.bucket.name
                 << ", obj_prefix=" << obj_prefix
                 << ", obj_delim=" << obj_delim
                 << dendl;

  bool is_truncated = false;

  boost::optional<rgw_bucket_dir_entry> last_entry;
  /* We need to rewind to the last object in a listing. */
  do {
    /* List bucket entries in chunks. */
    static constexpr int MAX_LIST_OBJS = 100;
    std::vector<rgw_bucket_dir_entry> entries(MAX_LIST_OBJS);

    int ret = list_op.list_objects(MAX_LIST_OBJS, &entries, nullptr,
                                   &is_truncated);
    if (ret < 0) {
      return ret;
    } else if (!entries.empty()) {
      last_entry = entries.back();
    }
  } while (is_truncated);

  if (last_entry) {
    return handler(*last_entry);
  }

  /* Empty listing - no items we can run handler on. */
  return 0;
}


int RGWRados::swift_versioning_copy(RGWObjectCtx& obj_ctx,
                                    const rgw_user& user,
                                    RGWBucketInfo& bucket_info,
                                    rgw_obj& obj)
{
  if (! swift_versioning_enabled(bucket_info)) {
    return 0;
  }

  obj_ctx.set_atomic(obj);

  RGWObjState * state = nullptr;
  int r = get_obj_state(&obj_ctx, bucket_info, obj, &state, false);
  if (r < 0) {
    return r;
  }

  if (!state->exists) {
    return 0;
  }

  const string& src_name = obj.get_oid();
  char buf[src_name.size() + 32];
  struct timespec ts = ceph::real_clock::to_timespec(state->mtime);
  snprintf(buf, sizeof(buf), "%03x%s/%lld.%06ld", (int)src_name.size(),
           src_name.c_str(), (long long)ts.tv_sec, ts.tv_nsec / 1000);

  RGWBucketInfo dest_bucket_info;

  auto sysobj_ctx = svc.sysobj->init_obj_ctx();

  r = get_bucket_info(sysobj_ctx, bucket_info.bucket.tenant, bucket_info.swift_ver_location, dest_bucket_info, NULL, NULL);
  if (r < 0) {
    ldout(cct, 10) << "failed to read dest bucket info: r=" << r << dendl;
    if (r == -ENOENT) {
      return -ERR_PRECONDITION_FAILED;
    }
    return r;
  }

  if (dest_bucket_info.owner != bucket_info.owner) {
    return -ERR_PRECONDITION_FAILED;
  }

  rgw_obj dest_obj(dest_bucket_info.bucket, buf);

  if (dest_bucket_info.versioning_enabled()){
    gen_rand_obj_instance_name(&dest_obj);
  }

  obj_ctx.set_atomic(dest_obj);

  string no_zone;

  r = copy_obj(obj_ctx,
               user,
               NULL, /* req_info *info */
               no_zone,
               dest_obj,
               obj,
               dest_bucket_info,
               bucket_info,
               bucket_info.placement_rule,
               NULL, /* time_t *src_mtime */
               NULL, /* time_t *mtime */
               NULL, /* const time_t *mod_ptr */
               NULL, /* const time_t *unmod_ptr */
               false, /* bool high_precision_time */
               NULL, /* const char *if_match */
               NULL, /* const char *if_nomatch */
               RGWRados::ATTRSMOD_NONE,
               true, /* bool copy_if_newer */
               state->attrset,
               RGWObjCategory::Main,
               0, /* uint64_t olh_epoch */
               real_time(), /* time_t delete_at */
               NULL, /* string *version_id */
               NULL, /* string *ptag */
               NULL, /* string *petag */
               NULL, /* void (*progress_cb)(off_t, void *) */
               NULL); /* void *progress_data */
  if (r == -ECANCELED || r == -ENOENT) {
    /* Has already been overwritten, meaning another rgw process already
     * copied it out */
    return 0;
  }

  return r;
}

int RGWRados::swift_versioning_restore(RGWSysObjectCtx& sysobj_ctx,
                                       RGWObjectCtx& obj_ctx,
                                       const rgw_user& user,
                                       RGWBucketInfo& bucket_info,
                                       rgw_obj& obj,
                                       bool& restored)             /* out */
{
  if (! swift_versioning_enabled(bucket_info)) {
    return 0;
  }

  /* Bucket info of the bucket that stores previous versions of our object. */
  RGWBucketInfo archive_binfo;

  int ret = get_bucket_info(sysobj_ctx, bucket_info.bucket.tenant,
                            bucket_info.swift_ver_location, archive_binfo,
                            nullptr, nullptr);
  if (ret < 0) {
    return ret;
  }

  /* Abort the operation if the bucket storing our archive belongs to someone
   * else. This is a limitation in comparison to Swift as we aren't taking ACLs
   * into consideration. For we can live with that.
   *
   * TODO: delegate this check to un upper layer and compare with ACLs. */
  if (bucket_info.owner != archive_binfo.owner) {
    return -EPERM;
  }

  /* This code will be executed on latest version of the object. */
  const auto handler = [&](const rgw_bucket_dir_entry& entry) -> int {
    std::string no_zone;

    /* We don't support object versioning of Swift API on those buckets that
     * are already versioned using the S3 mechanism. This affects also bucket
     * storing archived objects. Otherwise the delete operation would create
     * a deletion marker. */
    if (archive_binfo.versioned()) {
      restored = false;
      return -ERR_PRECONDITION_FAILED;
    }

    /* We are requesting ATTRSMOD_NONE so the attr attribute is perfectly
     * irrelevant and may be safely skipped. */
    std::map<std::string, ceph::bufferlist> no_attrs;

    rgw_obj archive_obj(archive_binfo.bucket, entry.key);

    if (bucket_info.versioning_enabled()){
      gen_rand_obj_instance_name(&obj);
    }

    obj_ctx.set_atomic(archive_obj);
    obj_ctx.set_atomic(obj);

    int ret = copy_obj(obj_ctx,
                       user,
                       nullptr,       /* req_info *info */
                       no_zone,
                       obj,           /* dest obj */
                       archive_obj,   /* src obj */
                       bucket_info,   /* dest bucket info */
                       archive_binfo, /* src bucket info */
                       bucket_info.placement_rule,  /* placement_rule */
                       nullptr,       /* time_t *src_mtime */
                       nullptr,       /* time_t *mtime */
                       nullptr,       /* const time_t *mod_ptr */
                       nullptr,       /* const time_t *unmod_ptr */
                       false,         /* bool high_precision_time */
                       nullptr,       /* const char *if_match */
                       nullptr,       /* const char *if_nomatch */
                       RGWRados::ATTRSMOD_NONE,
                       true,          /* bool copy_if_newer */
                       no_attrs,
                       RGWObjCategory::Main,
                       0,             /* uint64_t olh_epoch */
                       real_time(),   /* time_t delete_at */
                       nullptr,       /* string *version_id */
                       nullptr,       /* string *ptag */
                       nullptr,       /* string *petag */
                       nullptr,       /* void (*progress_cb)(off_t, void *) */
                       nullptr);      /* void *progress_data */
    if (ret == -ECANCELED || ret == -ENOENT) {
      /* Has already been overwritten, meaning another rgw process already
       * copied it out */
      return 0;
    } else if (ret < 0) {
      return ret;
    } else {
      restored = true;
    }

    /* Need to remove the archived copy. */
    ret = delete_obj(obj_ctx, archive_binfo, archive_obj,
                     archive_binfo.versioning_status());

    return ret;
  };

  const std::string& obj_name = obj.get_oid();
  const auto prefix = boost::str(boost::format("%03x%s") % obj_name.size()
                                                         % obj_name);

  return on_last_entry_in_listing(archive_binfo, prefix, std::string(),
                                  handler);
}

int RGWRados::Object::Write::_do_write_meta(uint64_t size, uint64_t accounted_size,
                                           map<string, bufferlist>& attrs,
                                           bool assume_noent, bool modify_tail,
                                           void *_index_op)
{
  RGWRados::Bucket::UpdateIndex *index_op = static_cast<RGWRados::Bucket::UpdateIndex *>(_index_op);
  RGWRados *store = target->get_store();

  ObjectWriteOperation op;
#ifdef WITH_LTTNG
  const struct req_state* s =  get_req_state();
  string req_id;
  if (!s) {
    // fake req_id
    req_id = store->svc.zone_utils->unique_id(store->get_new_req_id());
  } else {
    req_id = s->req_id;
  }
#endif

  RGWObjState *state;
  int r = target->get_state(&state, false, assume_noent);
  if (r < 0)
    return r;

  rgw_obj& obj = target->get_obj();

  if (obj.get_oid().empty()) {
    ldout(store->ctx(), 0) << "ERROR: " << __func__ << "(): cannot write object with empty name" << dendl;
    return -EIO;
  }

  rgw_rados_ref ref;
  r = store->get_obj_head_ref(target->get_bucket_info(), obj, &ref);
  if (r < 0)
    return r;

  bool is_olh = state->is_olh;

  bool reset_obj = (meta.flags & PUT_OBJ_CREATE) != 0;

  const string *ptag = meta.ptag;
  if (!ptag && !index_op->get_optag()->empty()) {
    ptag = index_op->get_optag();
  }
  r = target->prepare_atomic_modification(op, reset_obj, ptag, meta.if_match, meta.if_nomatch, false, modify_tail);
  if (r < 0)
    return r;

  if (real_clock::is_zero(meta.set_mtime)) {
    meta.set_mtime = real_clock::now();
  }

  if (state->is_olh) {
    op.setxattr(RGW_ATTR_OLH_ID_TAG, state->olh_tag);
  }

  struct timespec mtime_ts = real_clock::to_timespec(meta.set_mtime);
  op.mtime2(&mtime_ts);

  if (meta.data) {
    /* if we want to overwrite the data, we also want to overwrite the
       xattrs, so just remove the object */
    op.write_full(*meta.data);
  }

  string etag;
  string content_type;
  bufferlist acl_bl;
  string storage_class;

  map<string, bufferlist>::iterator iter;
  if (meta.rmattrs) {
    for (iter = meta.rmattrs->begin(); iter != meta.rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op.rmxattr(name.c_str());
    }
  }

  if (meta.manifest) {
    storage_class = meta.manifest->get_tail_placement().placement_rule.storage_class;

    /* remove existing manifest attr */
    iter = attrs.find(RGW_ATTR_MANIFEST);
    if (iter != attrs.end())
      attrs.erase(iter);

    bufferlist bl;
    encode(*meta.manifest, bl);
    op.setxattr(RGW_ATTR_MANIFEST, bl);
  }

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name.c_str(), bl);

    if (name.compare(RGW_ATTR_ETAG) == 0) {
      etag = rgw_bl_str(bl);
    } else if (name.compare(RGW_ATTR_CONTENT_TYPE) == 0) {
      content_type = rgw_bl_str(bl);
    } else if (name.compare(RGW_ATTR_ACL) == 0) {
      acl_bl = bl;
    }
  }
  if (attrs.find(RGW_ATTR_PG_VER) == attrs.end()) {
    cls_rgw_obj_store_pg_ver(op, RGW_ATTR_PG_VER);
  }

  if (attrs.find(RGW_ATTR_SOURCE_ZONE) == attrs.end()) {
    bufferlist bl;
    encode(store->svc.zone->get_zone_short_id(), bl);
    op.setxattr(RGW_ATTR_SOURCE_ZONE, bl);
  }

  if (!storage_class.empty()) {
    bufferlist bl;
    bl.append(storage_class);
    op.setxattr(RGW_ATTR_STORAGE_CLASS, bl);
  }

  if (!op.size())
    return 0;

  uint64_t epoch;
  int64_t poolid;
  bool orig_exists;
  uint64_t orig_size;
  
  if (!reset_obj) {    //Multipart upload, it has immutable head. 
    orig_exists = false;
    orig_size = 0;
  } else {
    orig_exists = state->exists;
    orig_size = state->accounted_size;
  }

  bool versioned_target = (meta.olh_epoch && *meta.olh_epoch > 0) ||
                          !obj.key.instance.empty();

  bool versioned_op = (target->versioning_enabled() || is_olh || versioned_target);

  if (versioned_op) {
    index_op->set_bilog_flags(RGW_BILOG_FLAG_VERSIONED_OP);
  }

  if (!index_op->is_prepared()) {
    tracepoint(rgw_rados, prepare_enter, req_id.c_str());
    r = index_op->prepare(CLS_RGW_OP_ADD, &state->write_tag);
    tracepoint(rgw_rados, prepare_exit, req_id.c_str());
    if (r < 0)
      return r;
  }

  tracepoint(rgw_rados, operate_enter, req_id.c_str());
  r = ref.ioctx.operate(ref.obj.oid, &op);
  tracepoint(rgw_rados, operate_exit, req_id.c_str());
  if (r < 0) { /* we can expect to get -ECANCELED if object was replaced under,
                or -ENOENT if was removed, or -EEXIST if it did not exist
                before and now it does */
    if (r == -EEXIST && assume_noent) {
      target->invalidate_state();
      return r;
    }
    goto done_cancel;
  }

  epoch = ref.ioctx.get_last_version();
  poolid = ref.ioctx.get_id();

  r = target->complete_atomic_modification();
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: complete_atomic_modification returned r=" << r << dendl;
  }

  tracepoint(rgw_rados, complete_enter, req_id.c_str());
  r = index_op->complete(poolid, epoch, size, accounted_size,
                        meta.set_mtime, etag, content_type,
                        storage_class, &acl_bl,
                        meta.category, meta.remove_objs, meta.user_data);
  tracepoint(rgw_rados, complete_exit, req_id.c_str());
  if (r < 0)
    goto done_cancel;

  if (meta.mtime) {
    *meta.mtime = meta.set_mtime;
  }

  /* note that index_op was using state so we couldn't invalidate it earlier */
  target->invalidate_state();
  state = NULL;

  if (versioned_op && meta.olh_epoch) {
    r = store->set_olh(target->get_ctx(), target->get_bucket_info(), obj, false, NULL, *meta.olh_epoch, real_time(), false, meta.zones_trace);
    if (r < 0) {
      return r;
    }
  }

  if (!real_clock::is_zero(meta.delete_at)) {
    rgw_obj_index_key obj_key;
    obj.key.get_index_key(&obj_key);

    r = store->objexp_hint_add(meta.delete_at,
            obj.bucket.tenant, obj.bucket.name, obj.bucket.bucket_id, obj_key);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: objexp_hint_add() returned r=" << r << ", object will not get removed" << dendl;
      /* ignoring error, nothing we can do at this point */
    }
  }
  meta.canceled = false;

  /* update quota cache */
  if (meta.completeMultipart){
  	store->quota_handler->update_stats(meta.owner, obj.bucket, (orig_exists ? 0 : 1),
                                     0, orig_size);
  }
  else {
    store->quota_handler->update_stats(meta.owner, obj.bucket, (orig_exists ? 0 : 1),
                                     accounted_size, orig_size);  
  }
  return 0;

done_cancel:
  int ret = index_op->cancel();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: index_op.cancel()() returned ret=" << ret << dendl;
  }

  meta.canceled = true;

  /* we lost in a race. There are a few options:
   * - existing object was rewritten (ECANCELED)
   * - non existing object was created (EEXIST)
   * - object was removed (ENOENT)
   * should treat it as a success
   */
  if (meta.if_match == NULL && meta.if_nomatch == NULL) {
    if (r == -ECANCELED || r == -ENOENT || r == -EEXIST) {
      r = 0;
    }
  } else {
    if (meta.if_match != NULL) {
      // only overwrite existing object
      if (strcmp(meta.if_match, "*") == 0) {
        if (r == -ENOENT) {
          r = -ERR_PRECONDITION_FAILED;
        } else if (r == -ECANCELED) {
          r = 0;
        }
      }
    }

    if (meta.if_nomatch != NULL) {
      // only create a new object
      if (strcmp(meta.if_nomatch, "*") == 0) {
        if (r == -EEXIST) {
          r = -ERR_PRECONDITION_FAILED;
        } else if (r == -ENOENT) {
          r = 0;
        }
      }
    }
  }

  return r;
}

int RGWRados::Object::Write::write_meta(uint64_t size, uint64_t accounted_size,
                                           map<string, bufferlist>& attrs)
{
  RGWBucketInfo& bucket_info = target->get_bucket_info();

  RGWRados::Bucket bop(target->get_store(), bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, target->get_obj());
  index_op.set_zones_trace(meta.zones_trace);
  
  bool assume_noent = (meta.if_match == NULL && meta.if_nomatch == NULL);
  int r;
  if (assume_noent) {
    r = _do_write_meta(size, accounted_size, attrs, assume_noent, meta.modify_tail, (void *)&index_op);
    if (r == -EEXIST) {
      assume_noent = false;
    }
  }
  if (!assume_noent) {
    r = _do_write_meta(size, accounted_size, attrs, assume_noent, meta.modify_tail, (void *)&index_op);
  }
  return r;
}

class RGWRadosPutObj : public RGWHTTPStreamRWRequest::ReceiveCB
{
  CephContext* cct;
  rgw_obj obj;
  rgw::putobj::DataProcessor *filter;
  boost::optional<RGWPutObj_Compress>& compressor;
  boost::optional<rgw::putobj::ChunkProcessor> buffering;
  CompressorRef& plugin;
  rgw::putobj::ObjectProcessor *processor;
  void (*progress_cb)(off_t, void *);
  void *progress_data;
  bufferlist extra_data_bl;
  uint64_t extra_data_left{0};
  bool need_to_process_attrs{true};
  uint64_t data_len{0};
  map<string, bufferlist> src_attrs;
  uint64_t ofs{0};
  uint64_t lofs{0}; /* logical ofs */
  std::function<int(const map<string, bufferlist>&)> attrs_handler;
public:
  RGWRadosPutObj(CephContext* cct,
                 CompressorRef& plugin,
                 boost::optional<RGWPutObj_Compress>& compressor,
                 rgw::putobj::ObjectProcessor *p,
                 void (*_progress_cb)(off_t, void *),
                 void *_progress_data,
                 std::function<int(const map<string, bufferlist>&)> _attrs_handler) :
                       cct(cct),
                       filter(p),
                       compressor(compressor),
                       plugin(plugin),
                       processor(p),
                       progress_cb(_progress_cb),
                       progress_data(_progress_data),
                       attrs_handler(_attrs_handler) {}

  int process_attrs(void) {
    if (extra_data_bl.length()) {
      JSONParser jp;
      if (!jp.parse(extra_data_bl.c_str(), extra_data_bl.length())) {
        ldout(cct, 0) << "failed to parse response extra data. len=" << extra_data_bl.length() << " data=" << extra_data_bl.c_str() << dendl;
        return -EIO;
      }

      JSONDecoder::decode_json("attrs", src_attrs, &jp);

      src_attrs.erase(RGW_ATTR_COMPRESSION);
      src_attrs.erase(RGW_ATTR_MANIFEST); // not interested in original object layout

      // filter out olh attributes
      auto iter = src_attrs.lower_bound(RGW_ATTR_OLH_PREFIX);
      while (iter != src_attrs.end()) {
        if (!boost::algorithm::starts_with(iter->first, RGW_ATTR_OLH_PREFIX)) {
          break;
        }
        iter = src_attrs.erase(iter);
      }
    }

    int ret = attrs_handler(src_attrs);
    if (ret < 0) {
      return ret;
    }

    if (plugin && src_attrs.find(RGW_ATTR_CRYPT_MODE) == src_attrs.end()) {
      //do not compress if object is encrypted
      compressor = boost::in_place(cct, plugin, filter);
      // add a filter that buffers data so we don't try to compress tiny blocks.
      // libcurl reads in 16k at a time, and we need at least 64k to get a good
      // compression ratio
      constexpr unsigned buffer_size = 512 * 1024;
      buffering = boost::in_place(&*compressor, buffer_size);
      filter = &*buffering;
    }

    need_to_process_attrs = false;

    return 0;
  }

  int handle_data(bufferlist& bl, bool *pause) override {
    if (progress_cb) {
      progress_cb(data_len, progress_data);
    }
    if (extra_data_left) {
      uint64_t extra_len = bl.length();
      if (extra_len > extra_data_left)
        extra_len = extra_data_left;

      bufferlist extra;
      bl.splice(0, extra_len, &extra);
      extra_data_bl.append(extra);

      extra_data_left -= extra_len;
      if (extra_data_left == 0) {
        int res = process_attrs();
        if (res < 0)
          return res;
      }
      ofs += extra_len;
      if (bl.length() == 0) {
        return 0;
      }
    }
    if (need_to_process_attrs) {
      /* need to call process_attrs() even if we don't get any attrs,
       * need it to call attrs_handler().
       */
      int res = process_attrs();
      if (res < 0) {
        return res;
      }
    }

    ceph_assert(uint64_t(ofs) >= extra_data_len);

    uint64_t size = bl.length();
    ofs += size;

    const uint64_t lofs = data_len;
    data_len += size;

    return filter->process(std::move(bl), lofs);
  }

  int flush() {
    return filter->process({}, data_len);
  }

  bufferlist& get_extra_data() { return extra_data_bl; }

  map<string, bufferlist>& get_attrs() { return src_attrs; }

  void set_extra_data_len(uint64_t len) override {
    extra_data_left = len;
    RGWHTTPStreamRWRequest::ReceiveCB::set_extra_data_len(len);
  }

  uint64_t get_data_len() {
    return data_len;
  }
};

/*
 * prepare attrset depending on attrs_mod.
 */
static void set_copy_attrs(map<string, bufferlist>& src_attrs,
                           map<string, bufferlist>& attrs,
                           RGWRados::AttrsMod attrs_mod)
{
  switch (attrs_mod) {
  case RGWRados::ATTRSMOD_NONE:
    attrs = src_attrs;
    break;
  case RGWRados::ATTRSMOD_REPLACE:
    if (!attrs[RGW_ATTR_ETAG].length()) {
      attrs[RGW_ATTR_ETAG] = src_attrs[RGW_ATTR_ETAG];
    }
    if (!attrs[RGW_ATTR_TAIL_TAG].length()) {
      auto ttiter = src_attrs.find(RGW_ATTR_TAIL_TAG);
      if (ttiter != src_attrs.end()) {
        attrs[RGW_ATTR_TAIL_TAG] = src_attrs[RGW_ATTR_TAIL_TAG];
      }
    }
    break;
  case RGWRados::ATTRSMOD_MERGE:
    for (map<string, bufferlist>::iterator it = src_attrs.begin(); it != src_attrs.end(); ++it) {
      if (attrs.find(it->first) == attrs.end()) {
       attrs[it->first] = it->second;
      }
    }
    break;
  }
}

int RGWRados::rewrite_obj(RGWBucketInfo& dest_bucket_info, const rgw_obj& obj)
{
  map<string, bufferlist> attrset;

  real_time mtime;
  uint64_t obj_size;
  RGWObjectCtx rctx(this);

  RGWRados::Object op_target(this, dest_bucket_info, rctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrset;
  read_op.params.lastmod = &mtime;
  read_op.params.obj_size = &obj_size;

  int ret = read_op.prepare();
  if (ret < 0)
    return ret;

  attrset.erase(RGW_ATTR_ID_TAG);
  attrset.erase(RGW_ATTR_TAIL_TAG);

  return copy_obj_data(rctx, dest_bucket_info, dest_bucket_info.placement_rule,
                       read_op, obj_size - 1, obj, NULL, mtime, attrset,
                       0, real_time(), NULL);
}

struct obj_time_weight {
  real_time mtime;
  uint32_t zone_short_id;
  uint64_t pg_ver;
  bool high_precision;

  obj_time_weight() : zone_short_id(0), pg_ver(0), high_precision(false) {}

  bool compare_low_precision(const obj_time_weight& rhs) {
    struct timespec l = ceph::real_clock::to_timespec(mtime);
    struct timespec r = ceph::real_clock::to_timespec(rhs.mtime);
    l.tv_nsec = 0;
    r.tv_nsec = 0;
    if (l > r) {
      return false;
    }
    if (l < r) {
      return true;
    }
    if (!zone_short_id || !rhs.zone_short_id) {
      /* don't compare zone ids, if one wasn't provided */
      return false;
    }
    if (zone_short_id != rhs.zone_short_id) {
      return (zone_short_id < rhs.zone_short_id);
    }
    return (pg_ver < rhs.pg_ver);

  }

  bool operator<(const obj_time_weight& rhs) {
    if (!high_precision || !rhs.high_precision) {
      return compare_low_precision(rhs);
    }
    if (mtime > rhs.mtime) {
      return false;
    }
    if (mtime < rhs.mtime) {
      return true;
    }
    if (!zone_short_id || !rhs.zone_short_id) {
      /* don't compare zone ids, if one wasn't provided */
      return false;
    }
    if (zone_short_id != rhs.zone_short_id) {
      return (zone_short_id < rhs.zone_short_id);
    }
    return (pg_ver < rhs.pg_ver);
  }

  void init(const real_time& _mtime, uint32_t _short_id, uint64_t _pg_ver) {
    mtime = _mtime;
    zone_short_id = _short_id;
    pg_ver = _pg_ver;
  }

  void init(RGWObjState *state) {
    mtime = state->mtime;
    zone_short_id = state->zone_short_id;
    pg_ver = state->pg_ver;
  }
};

inline ostream& operator<<(ostream& out, const obj_time_weight &o) {
  out << o.mtime;

  if (o.zone_short_id != 0 || o.pg_ver != 0) {
    out << "[zid=" << o.zone_short_id << ", pgv=" << o.pg_ver << "]";
  }

  return out;
}

class RGWGetExtraDataCB : public RGWHTTPStreamRWRequest::ReceiveCB {
  bufferlist extra_data;
public:
  RGWGetExtraDataCB() {}
  int handle_data(bufferlist& bl, bool *pause) override {
    int bl_len = (int)bl.length();
    if (extra_data.length() < extra_data_len) {
      off_t max = extra_data_len - extra_data.length();
      if (max > bl_len) {
        max = bl_len;
      }
      bl.splice(0, max, &extra_data);
    }
    return bl_len;
  }

  bufferlist& get_extra_data() {
    return extra_data;
  }
};

int RGWRados::stat_remote_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               req_info *info,
               const string& source_zone,
               rgw_obj& src_obj,
               RGWBucketInfo& src_bucket_info,
               real_time *src_mtime,
               uint64_t *psize,
               const real_time *mod_ptr,
               const real_time *unmod_ptr,
               bool high_precision_time,
               const char *if_match,
               const char *if_nomatch,
               map<string, bufferlist> *pattrs,
               map<string, string> *pheaders,
               string *version_id,
               string *ptag,
               string *petag)
{
  /* source is in a different zonegroup, copy from there */

  RGWRESTStreamRWRequest *in_stream_req;
  string tag;
  map<string, bufferlist> src_attrs;
  append_rand_alpha(cct, tag, tag, 32);
  obj_time_weight set_mtime_weight;
  set_mtime_weight.high_precision = high_precision_time;

  RGWRESTConn *conn;
  if (source_zone.empty()) {
    if (src_bucket_info.zonegroup.empty()) {
      /* source is in the master zonegroup */
      conn = svc.zone->get_master_conn();
    } else {
      auto& zonegroup_conn_map = svc.zone->get_zonegroup_conn_map();
      map<string, RGWRESTConn *>::iterator iter = zonegroup_conn_map.find(src_bucket_info.zonegroup);
      if (iter == zonegroup_conn_map.end()) {
        ldout(cct, 0) << "could not find zonegroup connection to zonegroup: " << source_zone << dendl;
        return -ENOENT;
      }
      conn = iter->second;
    }
  } else {
    auto& zone_conn_map = svc.zone->get_zone_conn_map();
    map<string, RGWRESTConn *>::iterator iter = zone_conn_map.find(source_zone);
    if (iter == zone_conn_map.end()) {
      ldout(cct, 0) << "could not find zone connection to zone: " << source_zone << dendl;
      return -ENOENT;
    }
    conn = iter->second;
  }

  RGWGetExtraDataCB cb;
  map<string, string> req_headers;
  real_time set_mtime;

  const real_time *pmod = mod_ptr;

  obj_time_weight dest_mtime_weight;

  constexpr bool prepend_meta = true;
  constexpr bool get_op = true;
  constexpr bool rgwx_stat = true;
  constexpr bool sync_manifest = true;
  constexpr bool skip_decrypt = true;
  int ret = conn->get_obj(user_id, info, src_obj, pmod, unmod_ptr,
                      dest_mtime_weight.zone_short_id, dest_mtime_weight.pg_ver,
                      prepend_meta, get_op, rgwx_stat,
                      sync_manifest, skip_decrypt,
                      true, &cb, &in_stream_req);
  if (ret < 0) {
    return ret;
  }

  ret = conn->complete_request(in_stream_req, nullptr, &set_mtime, psize, nullptr, pheaders);
  if (ret < 0) {
    return ret;
  }

  bufferlist& extra_data_bl = cb.get_extra_data();
  if (extra_data_bl.length()) {
    JSONParser jp;
    if (!jp.parse(extra_data_bl.c_str(), extra_data_bl.length())) {
      ldout(cct, 0) << "failed to parse response extra data. len=" << extra_data_bl.length() << " data=" << extra_data_bl.c_str() << dendl;
      return -EIO;
    }

    JSONDecoder::decode_json("attrs", src_attrs, &jp);

    src_attrs.erase(RGW_ATTR_MANIFEST); // not interested in original object layout
  }

  if (src_mtime) {
    *src_mtime = set_mtime;
  }

  if (petag) {
    map<string, bufferlist>::iterator iter = src_attrs.find(RGW_ATTR_ETAG);
    if (iter != src_attrs.end()) {
      bufferlist& etagbl = iter->second;
      *petag = etagbl.to_str();
      while (petag->size() > 0 && (*petag)[petag->size() - 1] == '\0') {
        *petag = petag->substr(0, petag->size() - 1);
      }
    }
  }

  if (pattrs) {
    *pattrs = std::move(src_attrs);
  }

  return 0;
}

int RGWRados::fetch_remote_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               req_info *info,
               const string& source_zone,
               const rgw_obj& dest_obj,
               const rgw_obj& src_obj,
               RGWBucketInfo& dest_bucket_info,
               RGWBucketInfo& src_bucket_info,
               std::optional<rgw_placement_rule> dest_placement_rule,
               real_time *src_mtime,
               real_time *mtime,
               const real_time *mod_ptr,
               const real_time *unmod_ptr,
               bool high_precision_time,
               const char *if_match,
               const char *if_nomatch,
               AttrsMod attrs_mod,
               bool copy_if_newer,
               map<string, bufferlist>& attrs,
               RGWObjCategory category,
               std::optional<uint64_t> olh_epoch,
	       real_time delete_at,
               string *ptag,
               string *petag,
               void (*progress_cb)(off_t, void *),
               void *progress_data,
               rgw_zone_set *zones_trace)
{
  /* source is in a different zonegroup, copy from there */

  RGWRESTStreamRWRequest *in_stream_req;
  string tag;
  int i;
  append_rand_alpha(cct, tag, tag, 32);
  obj_time_weight set_mtime_weight;
  set_mtime_weight.high_precision = high_precision_time;
  int ret;

  rgw::AioThrottle aio(cct->_conf->rgw_put_obj_min_window_size);
  using namespace rgw::putobj;
  const rgw_placement_rule *ptail_rule = (dest_placement_rule ? &(*dest_placement_rule) : nullptr);
  AtomicObjectProcessor processor(&aio, this, dest_bucket_info, ptail_rule, user_id,
                                  obj_ctx, dest_obj, olh_epoch, tag);
  RGWRESTConn *conn;
  auto& zone_conn_map = svc.zone->get_zone_conn_map();
  auto& zonegroup_conn_map = svc.zone->get_zonegroup_conn_map();
  if (source_zone.empty()) {
    if (dest_bucket_info.zonegroup.empty()) {
      /* source is in the master zonegroup */
      conn = svc.zone->get_master_conn();
    } else {
      map<string, RGWRESTConn *>::iterator iter = zonegroup_conn_map.find(src_bucket_info.zonegroup);
      if (iter == zonegroup_conn_map.end()) {
        ldout(cct, 0) << "could not find zonegroup connection to zonegroup: " << source_zone << dendl;
        return -ENOENT;
      }
      conn = iter->second;
    }
  } else {
    map<string, RGWRESTConn *>::iterator iter = zone_conn_map.find(source_zone);
    if (iter == zone_conn_map.end()) {
      ldout(cct, 0) << "could not find zone connection to zone: " << source_zone << dendl;
      return -ENOENT;
    }
    conn = iter->second;
  }

  string obj_name = dest_obj.bucket.name + "/" + dest_obj.get_oid();

  boost::optional<RGWPutObj_Compress> compressor;
  CompressorRef plugin;

  rgw_placement_rule dest_rule;
  RGWRadosPutObj cb(cct, plugin, compressor, &processor, progress_cb, progress_data,
                    [&](const map<string, bufferlist>& obj_attrs) {
                      if (!ptail_rule) {
                        auto iter = obj_attrs.find(RGW_ATTR_STORAGE_CLASS);
                        if (iter != obj_attrs.end()) {
                          dest_rule.storage_class = iter->second.to_str();
                          dest_rule.inherit_from(dest_bucket_info.placement_rule);
                          processor.set_tail_placement(std::move(dest_rule));
                          ptail_rule = &dest_rule;
                        } else {
                          ptail_rule = &dest_bucket_info.placement_rule;
                        }
                      }
                      const auto& compression_type = svc.zone->get_zone_params().get_compression_type(*ptail_rule);
                      if (compression_type != "none") {
                        plugin = Compressor::create(cct, compression_type);
                        if (!plugin) {
                          ldout(cct, 1) << "Cannot load plugin for compression type "
                                        << compression_type << dendl;
                        }
                      }

                      int ret = processor.prepare();
                      if (ret < 0) {
                        return ret;
                      }
                      return 0;
                    });

  string etag;
  real_time set_mtime;

  RGWObjState *dest_state = NULL;

  const real_time *pmod = mod_ptr;

  obj_time_weight dest_mtime_weight;

  if (copy_if_newer) {
    /* need to get mtime for destination */
    ret = get_obj_state(&obj_ctx, dest_bucket_info, dest_obj, &dest_state, false);
    if (ret < 0)
      goto set_err_state;

    if (!real_clock::is_zero(dest_state->mtime)) {
      dest_mtime_weight.init(dest_state);
      pmod = &dest_mtime_weight.mtime;
    }
  }

  static constexpr bool prepend_meta = true;
  static constexpr bool get_op = true;
  static constexpr bool rgwx_stat = false;
  static constexpr bool sync_manifest = true;
  static constexpr bool skip_decrypt = true;
  ret = conn->get_obj(user_id, info, src_obj, pmod, unmod_ptr,
                      dest_mtime_weight.zone_short_id, dest_mtime_weight.pg_ver,
                      prepend_meta, get_op, rgwx_stat,
                      sync_manifest, skip_decrypt,
                      true,
                      &cb, &in_stream_req);
  if (ret < 0) {
    goto set_err_state;
  }

  ret = conn->complete_request(in_stream_req, &etag, &set_mtime, nullptr, nullptr, nullptr);
  if (ret < 0) {
    goto set_err_state;
  }
  ret = cb.flush();
  if (ret < 0) {
    goto set_err_state;
  }
  if (compressor && compressor->is_compressed()) {
    bufferlist tmp;
    RGWCompressionInfo cs_info;
    cs_info.compression_type = plugin->get_type_name();
    cs_info.orig_size = cb.get_data_len();
    cs_info.blocks = move(compressor->get_compression_blocks());
    encode(cs_info, tmp);
    cb.get_attrs()[RGW_ATTR_COMPRESSION] = tmp;
  }

  if (source_zone.empty()) { /* need to preserve expiration if copy in the same zonegroup */
    cb.get_attrs().erase(RGW_ATTR_DELETE_AT);
  } else {
    map<string, bufferlist>::iterator iter = cb.get_attrs().find(RGW_ATTR_DELETE_AT);
    if (iter != cb.get_attrs().end()) {
      try {
        decode(delete_at, iter->second);
      } catch (buffer::error& err) {
        ldout(cct, 0) << "ERROR: failed to decode delete_at field in intra zone copy" << dendl;
      }
    }
  }

  if (src_mtime) {
    *src_mtime = set_mtime;
  }

  if (petag) {
    const auto iter = cb.get_attrs().find(RGW_ATTR_ETAG);
    if (iter != cb.get_attrs().end()) {
      *petag = iter->second.to_str();
    }
  }

  if (source_zone.empty()) {
    set_copy_attrs(cb.get_attrs(), attrs, attrs_mod);
  } else {
    attrs = cb.get_attrs();
  }

  if (copy_if_newer) {
    uint64_t pg_ver = 0;
    auto i = attrs.find(RGW_ATTR_PG_VER);
    if (i != attrs.end() && i->second.length() > 0) {
      auto iter = i->second.cbegin();
      try {
        decode(pg_ver, iter);
      } catch (buffer::error& err) {
        ldout(ctx(), 0) << "ERROR: failed to decode pg ver attribute, ignoring" << dendl;
        /* non critical error */
      }
    }
    set_mtime_weight.init(set_mtime, svc.zone->get_zone_short_id(), pg_ver);
  }

#define MAX_COMPLETE_RETRY 100
  for (i = 0; i < MAX_COMPLETE_RETRY; i++) {
    bool canceled = false;
    ret = processor.complete(cb.get_data_len(), etag, mtime, set_mtime,
                             attrs, delete_at, nullptr, nullptr, nullptr,
                             zones_trace, &canceled);
    if (ret < 0) {
      goto set_err_state;
    }
    if (copy_if_newer && canceled) {
      ldout(cct, 20) << "raced with another write of obj: " << dest_obj << dendl;
      obj_ctx.invalidate(dest_obj); /* object was overwritten */
      ret = get_obj_state(&obj_ctx, dest_bucket_info, dest_obj, &dest_state, false);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: " << __func__ << ": get_err_state() returned ret=" << ret << dendl;
        goto set_err_state;
      }
      dest_mtime_weight.init(dest_state);
      dest_mtime_weight.high_precision = high_precision_time;
      if (!dest_state->exists ||
        dest_mtime_weight < set_mtime_weight) {
        ldout(cct, 20) << "retrying writing object mtime=" << set_mtime << " dest_state->mtime=" << dest_state->mtime << " dest_state->exists=" << dest_state->exists << dendl;
        continue;
      } else {
        ldout(cct, 20) << "not retrying writing object mtime=" << set_mtime << " dest_state->mtime=" << dest_state->mtime << " dest_state->exists=" << dest_state->exists << dendl;
      }
    }
    break;
  }

  if (i == MAX_COMPLETE_RETRY) {
    ldout(cct, 0) << "ERROR: retried object completion too many times, something is wrong!" << dendl;
    ret = -EIO;
    goto set_err_state;
  }

  return 0;
set_err_state:
  if (copy_if_newer && ret == -ERR_NOT_MODIFIED) {
    // we may have already fetched during sync of OP_ADD, but were waiting
    // for OP_LINK_OLH to call set_olh() with a real olh_epoch
    if (olh_epoch && *olh_epoch > 0) {
      constexpr bool log_data_change = true;
      ret = set_olh(obj_ctx, dest_bucket_info, dest_obj, false, nullptr,
                    *olh_epoch, real_time(), false, zones_trace, log_data_change);
    } else {
      // we already have the latest copy
      ret = 0;
    }
  }
  return ret;
}


int RGWRados::copy_obj_to_remote_dest(RGWObjState *astate,
                                      map<string, bufferlist>& src_attrs,
                                      RGWRados::Object::Read& read_op,
                                      const rgw_user& user_id,
                                      rgw_obj& dest_obj,
                                      real_time *mtime)
{
  string etag;

  RGWRESTStreamS3PutObj *out_stream_req;

  auto rest_master_conn = svc.zone->get_master_conn();

  int ret = rest_master_conn->put_obj_async(user_id, dest_obj, astate->size, src_attrs, true, &out_stream_req);
  if (ret < 0) {
    return ret;
  }

  ret = read_op.iterate(0, astate->size - 1, out_stream_req->get_out_cb());
  if (ret < 0) {
    delete out_stream_req;
    return ret;
  }

  ret = rest_master_conn->complete_request(out_stream_req, etag, mtime);
  if (ret < 0)
    return ret;

  return 0;
}

/**
 * Copy an object.
 * dest_obj: the object to copy into
 * src_obj: the object to copy from
 * attrs: usage depends on attrs_mod parameter
 * attrs_mod: the modification mode of the attrs, may have the following values:
 *            ATTRSMOD_NONE - the attributes of the source object will be
 *                            copied without modifications, attrs parameter is ignored;
 *            ATTRSMOD_REPLACE - new object will have the attributes provided by attrs
 *                               parameter, source object attributes are not copied;
 *            ATTRSMOD_MERGE - any conflicting meta keys on the source object's attributes
 *                             are overwritten by values contained in attrs parameter.
 * err: stores any errors resulting from the get of the original object
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::copy_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               req_info *info,
               const string& source_zone,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               RGWBucketInfo& dest_bucket_info,
               RGWBucketInfo& src_bucket_info,
               const rgw_placement_rule& dest_placement,
               real_time *src_mtime,
               real_time *mtime,
               const real_time *mod_ptr,
               const real_time *unmod_ptr,
               bool high_precision_time,
               const char *if_match,
               const char *if_nomatch,
               AttrsMod attrs_mod,
               bool copy_if_newer,
               map<string, bufferlist>& attrs,
               RGWObjCategory category,
               uint64_t olh_epoch,
	       real_time delete_at,
               string *version_id,
               string *ptag,
               string *petag,
               void (*progress_cb)(off_t, void *),
               void *progress_data)
{
  int ret;
  uint64_t obj_size;
  rgw_obj shadow_obj = dest_obj;
  string shadow_oid;

  bool remote_src;
  bool remote_dest;

  append_rand_alpha(cct, dest_obj.get_oid(), shadow_oid, 32);
  shadow_obj.init_ns(dest_obj.bucket, shadow_oid, shadow_ns);

  auto& zonegroup = svc.zone->get_zonegroup();

  remote_dest = !zonegroup.equals(dest_bucket_info.zonegroup);
  remote_src = !zonegroup.equals(src_bucket_info.zonegroup);

  if (remote_src && remote_dest) {
    ldout(cct, 0) << "ERROR: can't copy object when both src and dest buckets are remote" << dendl;
    return -EINVAL;
  }

  ldout(cct, 5) << "Copy object " << src_obj.bucket << ":" << src_obj.get_oid() << " => " << dest_obj.bucket << ":" << dest_obj.get_oid() << dendl;

  if (remote_src || !source_zone.empty()) {
    return fetch_remote_obj(obj_ctx, user_id, info, source_zone,
               dest_obj, src_obj, dest_bucket_info, src_bucket_info,
               dest_placement, src_mtime, mtime, mod_ptr,
               unmod_ptr, high_precision_time,
               if_match, if_nomatch, attrs_mod, copy_if_newer, attrs, category,
               olh_epoch, delete_at, ptag, petag, progress_cb, progress_data);
  }

  map<string, bufferlist> src_attrs;
  RGWRados::Object src_op_target(this, src_bucket_info, obj_ctx, src_obj);
  RGWRados::Object::Read read_op(&src_op_target);

  read_op.conds.mod_ptr = mod_ptr;
  read_op.conds.unmod_ptr = unmod_ptr;
  read_op.conds.high_precision_time = high_precision_time;
  read_op.conds.if_match = if_match;
  read_op.conds.if_nomatch = if_nomatch;
  read_op.params.attrs = &src_attrs;
  read_op.params.lastmod = src_mtime;
  read_op.params.obj_size = &obj_size;

  ret = read_op.prepare();
  if (ret < 0) {
    return ret;
  }
  if (src_attrs.count(RGW_ATTR_CRYPT_MODE)) {
    // Current implementation does not follow S3 spec and even
    // may result in data corruption silently when copying
    // multipart objects acorss pools. So reject COPY operations
    //on encrypted objects before it is fully functional.
    ldout(cct, 0) << "ERROR: copy op for encrypted object " << src_obj
                  << " has not been implemented." << dendl;
    return -ERR_NOT_IMPLEMENTED;
  }

  src_attrs[RGW_ATTR_ACL] = attrs[RGW_ATTR_ACL];
  src_attrs.erase(RGW_ATTR_DELETE_AT);

  set_copy_attrs(src_attrs, attrs, attrs_mod);
  attrs.erase(RGW_ATTR_ID_TAG);
  attrs.erase(RGW_ATTR_PG_VER);
  attrs.erase(RGW_ATTR_SOURCE_ZONE);
  map<string, bufferlist>::iterator cmp = src_attrs.find(RGW_ATTR_COMPRESSION);
  if (cmp != src_attrs.end())
    attrs[RGW_ATTR_COMPRESSION] = cmp->second;

  RGWObjManifest manifest;
  RGWObjState *astate = NULL;

  ret = get_obj_state(&obj_ctx, src_bucket_info, src_obj, &astate);
  if (ret < 0) {
    return ret;
  }

  vector<rgw_raw_obj> ref_objs;

  if (remote_dest) {
    /* dest is in a different zonegroup, copy it there */
    return copy_obj_to_remote_dest(astate, attrs, read_op, user_id, dest_obj, mtime);
  }
  uint64_t max_chunk_size;

  ret = get_max_chunk_size(dest_bucket_info.placement_rule, dest_obj, &max_chunk_size);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to get max_chunk_size() for bucket " << dest_obj.bucket << dendl;
    return ret;
  }

  rgw_pool src_pool;
  rgw_pool dest_pool;

  const rgw_placement_rule *src_rule{nullptr};

  if (astate->has_manifest) {
    src_rule = &astate->manifest.get_tail_placement().placement_rule;
    ldout(cct, 20) << __func__ << "(): manifest src_rule=" << src_rule->to_str() << dendl;
  }

  if (!src_rule || src_rule->empty()) {
    src_rule = &src_bucket_info.placement_rule;
  }

  if (!get_obj_data_pool(*src_rule, src_obj, &src_pool)) {
    ldout(cct, 0) << "ERROR: failed to locate data pool for " << src_obj << dendl;
    return -EIO;
  }

  if (!get_obj_data_pool(dest_placement, dest_obj, &dest_pool)) {
    ldout(cct, 0) << "ERROR: failed to locate data pool for " << dest_obj << dendl;
    return -EIO;
  }

  ldout(cct, 20) << __func__ << "(): src_rule=" << src_rule->to_str() << " src_pool=" << src_pool
                             << " dest_rule=" << dest_placement.to_str() << " dest_pool=" << dest_pool << dendl;

  bool copy_data = !astate->has_manifest ||
    (*src_rule != dest_placement) ||
    (src_pool != dest_pool);

  bool copy_first = false;
  if (astate->has_manifest) {
    if (!astate->manifest.has_tail()) {
      copy_data = true;
    } else {
      uint64_t head_size = astate->manifest.get_head_size();

      if (head_size > 0) {
        if (head_size > max_chunk_size) {
          copy_data = true;
        } else {
          copy_first = true;
        }
      }
    }
  }

  if (petag) {
    const auto iter = attrs.find(RGW_ATTR_ETAG);
    if (iter != attrs.end()) {
      *petag = iter->second.to_str();
    }
  }

  if (copy_data) { /* refcounting tail wouldn't work here, just copy the data */
    attrs.erase(RGW_ATTR_TAIL_TAG);
    return copy_obj_data(obj_ctx, dest_bucket_info, dest_placement, read_op, obj_size - 1, dest_obj,
                         mtime, real_time(), attrs, olh_epoch, delete_at, petag);
  }

  RGWObjManifest::obj_iterator miter = astate->manifest.obj_begin();

  if (copy_first) { // we need to copy first chunk, not increase refcount
    ++miter;
  }

  rgw_rados_ref ref;
  ret = get_raw_obj_ref(miter.get_location().get_raw_obj(this), &ref);
  if (ret < 0) {
    return ret;
  }

  bufferlist first_chunk;

  bool copy_itself = (dest_obj == src_obj);
  RGWObjManifest *pmanifest; 
  ldout(cct, 20) << "dest_obj=" << dest_obj << " src_obj=" << src_obj << " copy_itself=" << (int)copy_itself << dendl;

  RGWRados::Object dest_op_target(this, dest_bucket_info, obj_ctx, dest_obj);
  RGWRados::Object::Write write_op(&dest_op_target);

  string tag;

  if (ptag) {
    tag = *ptag;
  }

  if (tag.empty()) {
    append_rand_alpha(cct, tag, tag, 32);
  }

  if (!copy_itself) {
    attrs.erase(RGW_ATTR_TAIL_TAG);
    manifest = astate->manifest;
    const rgw_bucket_placement& tail_placement = manifest.get_tail_placement();
    if (tail_placement.bucket.name.empty()) {
      manifest.set_tail_placement(tail_placement.placement_rule, src_obj.bucket);
    }
    string ref_tag;
    for (; miter != astate->manifest.obj_end(); ++miter) {
      ObjectWriteOperation op;
      ref_tag = tag + '\0';
      cls_refcount_get(op, ref_tag, true);
      const rgw_raw_obj& loc = miter.get_location().get_raw_obj(this);
      ref.ioctx.locator_set_key(loc.loc);

      ret = ref.ioctx.operate(loc.oid, &op);
      if (ret < 0) {
        goto done_ret;
      }

      ref_objs.push_back(loc);
    }

    pmanifest = &manifest;
  } else {
    pmanifest = &astate->manifest;
    /* don't send the object's tail for garbage collection */
    astate->keep_tail = true;
  }

  if (copy_first) {
    ret = read_op.read(0, max_chunk_size, first_chunk);
    if (ret < 0) {
      goto done_ret;
    }

    pmanifest->set_head(dest_bucket_info.placement_rule, dest_obj, first_chunk.length());
  } else {
    pmanifest->set_head(dest_bucket_info.placement_rule, dest_obj, 0);
  }

  write_op.meta.data = &first_chunk;
  write_op.meta.manifest = pmanifest;
  write_op.meta.ptag = &tag;
  write_op.meta.owner = dest_bucket_info.owner;
  write_op.meta.mtime = mtime;
  write_op.meta.flags = PUT_OBJ_CREATE;
  write_op.meta.category = category;
  write_op.meta.olh_epoch = olh_epoch;
  write_op.meta.delete_at = delete_at;
  write_op.meta.modify_tail = !copy_itself;

  ret = write_op.write_meta(obj_size, astate->accounted_size, attrs);
  if (ret < 0) {
    goto done_ret;
  }

  return 0;

done_ret:
  if (!copy_itself) {
    vector<rgw_raw_obj>::iterator riter;

    /* rollback reference */
    for (riter = ref_objs.begin(); riter != ref_objs.end(); ++riter) {
      ObjectWriteOperation op;
      cls_refcount_put(op, tag, true);

      ref.ioctx.locator_set_key(riter->loc);

      int r = ref.ioctx.operate(riter->oid, &op);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: cleanup after error failed to drop reference on obj=" << *riter << dendl;
      }
    }
  }
  return ret;
}


int RGWRados::copy_obj_data(RGWObjectCtx& obj_ctx,
               RGWBucketInfo& dest_bucket_info,
               const rgw_placement_rule& dest_placement,
	       RGWRados::Object::Read& read_op, off_t end,
               const rgw_obj& dest_obj,
	       real_time *mtime,
	       real_time set_mtime,
               map<string, bufferlist>& attrs,
               uint64_t olh_epoch,
	       real_time delete_at,
               string *petag)
{
  string tag;
  append_rand_alpha(cct, tag, tag, 32);

  rgw::AioThrottle aio(cct->_conf->rgw_put_obj_min_window_size);
  using namespace rgw::putobj;
  AtomicObjectProcessor processor(&aio, this, dest_bucket_info, &dest_placement,
                                  dest_bucket_info.owner, obj_ctx,
                                  dest_obj, olh_epoch, tag);
  int ret = processor.prepare();
  if (ret < 0)
    return ret;

  off_t ofs = 0;

  do {
    bufferlist bl;
    ret = read_op.read(ofs, end, bl);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: fail to read object data, ret = " << ret << dendl;
      return ret;
    }

    uint64_t read_len = ret;
    ret = processor.process(std::move(bl), ofs);
    if (ret < 0) {
      return ret;
    }

    ofs += read_len;
  } while (ofs <= end);

  // flush
  ret = processor.process({}, ofs);
  if (ret < 0) {
    return ret;
  }

  string etag;
  auto iter = attrs.find(RGW_ATTR_ETAG);
  if (iter != attrs.end()) {
    bufferlist& bl = iter->second;
    etag = bl.to_str();
    if (petag) {
      *petag = etag;
    }
  }

  uint64_t accounted_size;
  {
    bool compressed{false};
    RGWCompressionInfo cs_info;
    ret = rgw_compression_info_from_attrset(attrs, compressed, cs_info);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to read compression info" << dendl;
      return ret;
    }
    // pass original size if compressed
    accounted_size = compressed ? cs_info.orig_size : ofs;
  }

  return processor.complete(accounted_size, etag, mtime, set_mtime, attrs, delete_at,
                            nullptr, nullptr, nullptr, nullptr, nullptr);
}

int RGWRados::transition_obj(RGWObjectCtx& obj_ctx,
                             RGWBucketInfo& bucket_info,
                             rgw_obj& obj,
                             const rgw_placement_rule& placement_rule,
                             const real_time& mtime,
                             uint64_t olh_epoch)
{
  map<string, bufferlist> attrs;
  real_time read_mtime;
  uint64_t obj_size;

  RGWRados::Object op_target(this, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrs;
  read_op.params.lastmod = &read_mtime;
  read_op.params.obj_size = &obj_size;

  int ret = read_op.prepare();
  if (ret < 0) {
    return ret;
  }

  if (read_mtime != mtime) {
    /* raced */
    return -ECANCELED;
  }

  ret = copy_obj_data(obj_ctx,
                      bucket_info,
                      placement_rule,
                      read_op,
                      obj_size - 1,
                      obj,
                      nullptr /* pmtime */,
                      mtime,
                      attrs,
                      olh_epoch,
                      real_time(),
                      nullptr /* petag */);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWRados::check_bucket_empty(RGWBucketInfo& bucket_info)
{
  std::vector<rgw_bucket_dir_entry> ent_list;
  rgw_obj_index_key marker;
  string prefix;
  bool is_truncated;

  do {
    constexpr uint NUM_ENTRIES = 1000u;
    int r = cls_bucket_list_unordered(bucket_info,
				      RGW_NO_SHARD,
				      marker,
				      prefix,
				      NUM_ENTRIES,
				      true,
				      ent_list,
				      &is_truncated,
				      &marker);
    if (r < 0)
      return r;

    string ns;
    for (auto const& dirent : ent_list) {
      rgw_obj_key obj;

      if (rgw_obj_key::oid_to_key_in_ns(dirent.key.name, &obj, ns))
        return -ENOTEMPTY;
    }
  } while (is_truncated);

  return 0;
}
  
/**
 * Delete a bucket.
 * bucket: the name of the bucket to delete
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_bucket(RGWBucketInfo& bucket_info, RGWObjVersionTracker& objv_tracker, bool check_empty)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;
  int r = open_bucket_index(bucket_info, index_ctx, bucket_objs);
  if (r < 0)
    return r;
  
  if (check_empty) {
    r = check_bucket_empty(bucket_info);
    if (r < 0) {
      return r;
    }
  }
  
  r = rgw_bucket_delete_bucket_obj(this, bucket.tenant, bucket.name, objv_tracker);
  if (r < 0)
    return r;

  /* if the bucket is not synced we can remove the meta file */
  if (!svc.zone->is_syncing_bucket_meta(bucket)) {
    RGWObjVersionTracker objv_tracker;
    r = rgw_bucket_instance_remove_entry(this, bucket.get_key(), &objv_tracker);
    if (r < 0) {
      return r;
    }

   /* remove bucket index objects asynchronously by best effort */
    (void) CLSRGWIssueBucketIndexClean(index_ctx,
				       bucket_objs,
				       cct->_conf->rgw_bucket_index_max_aio)();
  }

  return 0;
}

int RGWRados::set_bucket_owner(rgw_bucket& bucket, ACLOwner& owner)
{
  RGWBucketInfo info;
  map<string, bufferlist> attrs;
  auto obj_ctx = svc.sysobj->init_obj_ctx();
  int r;
  if (bucket.bucket_id.empty()) {
    r = get_bucket_info(obj_ctx, bucket.tenant, bucket.name, info, NULL, &attrs);
  } else {
    r = get_bucket_instance_info(obj_ctx, bucket, info, nullptr, &attrs);
  }
  if (r < 0) {
    ldout(cct, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << dendl;
    return r;
  }

  info.owner = owner.get_id();

  r = put_bucket_instance_info(info, false, real_time(), &attrs);
  if (r < 0) {
    ldout(cct, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << dendl;
    return r;
  }

  return 0;
}


int RGWRados::set_buckets_enabled(vector<rgw_bucket>& buckets, bool enabled)
{
  int ret = 0;

  vector<rgw_bucket>::iterator iter;

  for (iter = buckets.begin(); iter != buckets.end(); ++iter) {
    rgw_bucket& bucket = *iter;
    if (enabled)
      ldout(cct, 20) << "enabling bucket name=" << bucket.name << dendl;
    else
      ldout(cct, 20) << "disabling bucket name=" << bucket.name << dendl;

    RGWBucketInfo info;
    map<string, bufferlist> attrs;
    auto obj_ctx = svc.sysobj->init_obj_ctx();
    int r = get_bucket_info(obj_ctx, bucket.tenant, bucket.name, info, NULL, &attrs);
    if (r < 0) {
      ldout(cct, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
      ret = r;
      continue;
    }
    if (enabled) {
      info.flags &= ~BUCKET_SUSPENDED;
    } else {
      info.flags |= BUCKET_SUSPENDED;
    }

    r = put_bucket_instance_info(info, false, real_time(), &attrs);
    if (r < 0) {
      ldout(cct, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
      ret = r;
      continue;
    }
  }
  return ret;
}

int RGWRados::bucket_suspended(rgw_bucket& bucket, bool *suspended)
{
  RGWBucketInfo bucket_info;
  auto obj_ctx = svc.sysobj->init_obj_ctx();
  int ret = get_bucket_info(obj_ctx, bucket.tenant, bucket.name, bucket_info, NULL);
  if (ret < 0) {
    return ret;
  }

  *suspended = ((bucket_info.flags & BUCKET_SUSPENDED) != 0);
  return 0;
}

int RGWRados::Object::complete_atomic_modification()
{
  if (!state->has_manifest || state->keep_tail)
    return 0;

  cls_rgw_obj_chain chain;
  store->update_gc_chain(obj, state->manifest, &chain);

  if (chain.empty()) {
    return 0;
  }

  string tag = (state->tail_tag.length() > 0 ? state->tail_tag.to_str() : state->obj_tag.to_str());
  return store->gc->send_chain(chain, tag, false);  // do it async
}

void RGWRados::update_gc_chain(rgw_obj& head_obj, RGWObjManifest& manifest, cls_rgw_obj_chain *chain)
{
  RGWObjManifest::obj_iterator iter;
  rgw_raw_obj raw_head;
  obj_to_raw(manifest.get_head_placement_rule(), head_obj, &raw_head);
  for (iter = manifest.obj_begin(); iter != manifest.obj_end(); ++iter) {
    const rgw_raw_obj& mobj = iter.get_location().get_raw_obj(this);
    if (mobj == raw_head)
      continue;
    cls_rgw_obj_key key(mobj.oid);
    chain->push_obj(mobj.pool.to_str(), key, mobj.loc);
  }
}

int RGWRados::send_chain_to_gc(cls_rgw_obj_chain& chain, const string& tag, bool sync)
{
  return gc->send_chain(chain, tag, sync);
}

int RGWRados::open_bucket_index(const RGWBucketInfo& bucket_info,
				librados::IoCtx& index_ctx,
				string& bucket_oid)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  int r = open_bucket_index_ctx(bucket_info, index_ctx);
  if (r < 0)
    return r;

  if (bucket.bucket_id.empty()) {
    ldout(cct, 0) << "ERROR: empty bucket id for bucket operation" << dendl;
    return -EIO;
  }

  bucket_oid = dir_oid_prefix;
  bucket_oid.append(bucket.bucket_id);

  return 0;
}

int RGWRados::open_bucket_index_base(const RGWBucketInfo& bucket_info,
				     librados::IoCtx& index_ctx,
				     string& bucket_oid_base) {
  const rgw_bucket& bucket = bucket_info.bucket;
  int r = open_bucket_index_ctx(bucket_info, index_ctx);
  if (r < 0)
    return r;

  if (bucket.bucket_id.empty()) {
    ldout(cct, 0) << "ERROR: empty bucket_id for bucket operation" << dendl;
    return -EIO;
  }

  bucket_oid_base = dir_oid_prefix;
  bucket_oid_base.append(bucket.bucket_id);

  return 0;

}

int RGWRados::open_bucket_index(const RGWBucketInfo& bucket_info,
				librados::IoCtx& index_ctx,
				map<int, string>& bucket_objs,
				int shard_id,
				map<int, string> *bucket_instance_ids) {
  string bucket_oid_base;
  int ret = open_bucket_index_base(bucket_info, index_ctx, bucket_oid_base);
  if (ret < 0) {
    return ret;
  }

  get_bucket_index_objects(bucket_oid_base, bucket_info.num_shards, bucket_objs, shard_id);
  if (bucket_instance_ids) {
    get_bucket_instance_ids(bucket_info, shard_id, bucket_instance_ids);
  }
  return 0;
}

template<typename T>
int RGWRados::open_bucket_index(const RGWBucketInfo& bucket_info, librados::IoCtx& index_ctx,
                                map<int, string>& oids, map<int, T>& bucket_objs,
                                int shard_id, map<int, string> *bucket_instance_ids)
{
  int ret = open_bucket_index(bucket_info, index_ctx, oids, shard_id, bucket_instance_ids);
  if (ret < 0)
    return ret;

  map<int, string>::const_iterator iter = oids.begin();
  for (; iter != oids.end(); ++iter) {
    bucket_objs[iter->first] = T();
  }
  return 0;
}

int RGWRados::open_bucket_index_shard(const RGWBucketInfo& bucket_info, librados::IoCtx& index_ctx,
    const string& obj_key, string *bucket_obj, int *shard_id)
{
  string bucket_oid_base;
  int ret = open_bucket_index_base(bucket_info, index_ctx, bucket_oid_base);
  if (ret < 0)
    return ret;

  RGWObjectCtx obj_ctx(this);

  ret = get_bucket_index_object(bucket_oid_base, obj_key, bucket_info.num_shards,
        (RGWBucketInfo::BIShardsHashType)bucket_info.bucket_index_shard_hash_type, bucket_obj, shard_id);
  if (ret < 0) {
    ldout(cct, 10) << "get_bucket_index_object() returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWRados::open_bucket_index_shard(const RGWBucketInfo& bucket_info, librados::IoCtx& index_ctx,
                                      int shard_id, string *bucket_obj)
{
  string bucket_oid_base;
  int ret = open_bucket_index_base(bucket_info, index_ctx, bucket_oid_base);
  if (ret < 0)
    return ret;

  RGWObjectCtx obj_ctx(this);

  get_bucket_index_object(bucket_oid_base, bucket_info.num_shards,
                          shard_id, bucket_obj);
  return 0;
}

static void accumulate_raw_stats(const rgw_bucket_dir_header& header,
                                 map<RGWObjCategory, RGWStorageStats>& stats)
{
  for (const auto& pair : header.stats) {
    const RGWObjCategory category = static_cast<RGWObjCategory>(pair.first);
    const rgw_bucket_category_stats& header_stats = pair.second;

    RGWStorageStats& s = stats[category];

    s.category = category;
    s.size += header_stats.total_size;
    s.size_rounded += header_stats.total_size_rounded;
    s.size_utilized += header_stats.actual_size;
    s.num_objects += header_stats.num_entries;
  }
}

int RGWRados::bucket_check_index(RGWBucketInfo& bucket_info,
				 map<RGWObjCategory, RGWStorageStats> *existing_stats,
				 map<RGWObjCategory, RGWStorageStats> *calculated_stats)
{
  librados::IoCtx index_ctx;
  // key - bucket index object id
  // value - bucket index check OP returned result with the given bucket index object (shard)
  map<int, string> oids;
  map<int, struct rgw_cls_check_index_ret> bucket_objs_ret;

  int ret = open_bucket_index(bucket_info, index_ctx, oids, bucket_objs_ret);
  if (ret < 0) {
      return ret;
  }

  ret = CLSRGWIssueBucketCheck(index_ctx, oids, bucket_objs_ret, cct->_conf->rgw_bucket_index_max_aio)();
  if (ret < 0) {
      return ret;
  }

  // Aggregate results (from different shards if there is any)
  map<int, struct rgw_cls_check_index_ret>::iterator iter;
  for (iter = bucket_objs_ret.begin(); iter != bucket_objs_ret.end(); ++iter) {
    accumulate_raw_stats(iter->second.existing_header, *existing_stats);
    accumulate_raw_stats(iter->second.calculated_header, *calculated_stats);
  }

  return 0;
}

int RGWRados::bucket_rebuild_index(RGWBucketInfo& bucket_info)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;

  int r = open_bucket_index(bucket_info, index_ctx, bucket_objs);
  if (r < 0) {
    return r;
  }

  return CLSRGWIssueBucketRebuild(index_ctx, bucket_objs, cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWRados::bucket_set_reshard(const RGWBucketInfo& bucket_info, const cls_rgw_bucket_instance_entry& entry)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;

  int r = open_bucket_index(bucket_info, index_ctx, bucket_objs);
  if (r < 0) {
    return r;
  }

  return CLSRGWIssueSetBucketResharding(index_ctx, bucket_objs, entry, cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWRados::defer_gc(void *ctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj)
{
  RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
  std::string oid, key;
  get_obj_bucket_and_oid_loc(obj, oid, key);
  if (!rctx)
    return 0;

  RGWObjState *state = NULL;

  int r = get_obj_state(rctx, bucket_info, obj, &state, false);
  if (r < 0)
    return r;

  if (!state->is_atomic) {
    ldout(cct, 20) << "state for obj=" << obj << " is not atomic, not deferring gc operation" << dendl;
    return -EINVAL;
  }

  string tag;

  if (state->tail_tag.length() > 0) {
    tag = state->tail_tag.c_str();
  } else if (state->obj_tag.length() > 0) {
    tag = state->obj_tag.c_str();
  } else {
    ldout(cct, 20) << "state->obj_tag is empty, not deferring gc operation" << dendl;
    return -EINVAL;
  }

  ldout(cct, 0) << "defer chain tag=" << tag << dendl;

  return gc->defer_chain(tag, false);
}

void RGWRados::remove_rgw_head_obj(ObjectWriteOperation& op)
{
  list<string> prefixes;
  prefixes.push_back(RGW_ATTR_OLH_PREFIX);
  cls_rgw_remove_obj(op, prefixes);
}

void RGWRados::cls_obj_check_prefix_exist(ObjectOperation& op, const string& prefix, bool fail_if_exist)
{
  cls_rgw_obj_check_attrs_prefix(op, prefix, fail_if_exist);
}

void RGWRados::cls_obj_check_mtime(ObjectOperation& op, const real_time& mtime, bool high_precision_time, RGWCheckMTimeType type)
{
  cls_rgw_obj_check_mtime(op, mtime, high_precision_time, type);
}


/**
 * Delete an object.
 * bucket: name of the bucket storing the object
 * obj: name of the object to delete
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::Object::Delete::delete_obj()
{
  RGWRados *store = target->get_store();
  rgw_obj& src_obj = target->get_obj();
  const string& instance = src_obj.key.instance;
  rgw_obj obj = src_obj;

  if (instance == "null") {
    obj.key.instance.clear();
  }

  bool explicit_marker_version = (!params.marker_version_id.empty());

  if (params.versioning_status & BUCKET_VERSIONED || explicit_marker_version) {
    if (instance.empty() || explicit_marker_version) {
      rgw_obj marker = obj;

      if (!params.marker_version_id.empty()) {
        if (params.marker_version_id != "null") {
          marker.key.set_instance(params.marker_version_id);
        }
      } else if ((params.versioning_status & BUCKET_VERSIONS_SUSPENDED) == 0) {
        store->gen_rand_obj_instance_name(&marker);
      }

      result.version_id = marker.key.instance;
      if (result.version_id.empty())
        result.version_id = "null";
      result.delete_marker = true;

      struct rgw_bucket_dir_entry_meta meta;

      meta.owner = params.obj_owner.get_id().to_str();
      meta.owner_display_name = params.obj_owner.get_display_name();

      if (real_clock::is_zero(params.mtime)) {
        meta.mtime = real_clock::now();
      } else {
        meta.mtime = params.mtime;
      }

      int r = store->set_olh(target->get_ctx(), target->get_bucket_info(), marker, true, &meta, params.olh_epoch, params.unmod_since, params.high_precision_time, params.zones_trace);
      if (r < 0) {
        return r;
      }
    } else {
      rgw_bucket_dir_entry dirent;

      int r = store->bi_get_instance(target->get_bucket_info(), obj, &dirent);
      if (r < 0) {
        return r;
      }
      result.delete_marker = dirent.is_delete_marker();
      r = store->unlink_obj_instance(target->get_ctx(), target->get_bucket_info(), obj, params.olh_epoch, params.zones_trace);
      if (r < 0) {
        return r;
      }
      result.version_id = instance;
    }

    BucketShard *bs;
    int r = target->get_bucket_shard(&bs);
    if (r < 0) {
      ldout(store->ctx(), 5) << "failed to get BucketShard object: r=" << r << dendl;
      return r;
    }

    if (target->bucket_info.datasync_flag_enabled()) {
      r = store->data_log->add_entry(bs->bucket, bs->shard_id);
      if (r < 0) {
        lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
        return r;
      }
    }

    return 0;
  }

  rgw_rados_ref ref;
  int r = store->get_obj_head_ref(target->get_bucket_info(), obj, &ref);
  if (r < 0) {
    return r;
  }

  RGWObjState *state;
  r = target->get_state(&state, false);
  if (r < 0)
    return r;

  ObjectWriteOperation op;

  if (!real_clock::is_zero(params.unmod_since)) {
    struct timespec ctime = ceph::real_clock::to_timespec(state->mtime);
    struct timespec unmod = ceph::real_clock::to_timespec(params.unmod_since);
    if (!params.high_precision_time) {
      ctime.tv_nsec = 0;
      unmod.tv_nsec = 0;
    }

    ldout(store->ctx(), 10) << "If-UnModified-Since: " << params.unmod_since << " Last-Modified: " << ctime << dendl;
    if (ctime > unmod) {
      return -ERR_PRECONDITION_FAILED;
    }

    /* only delete object if mtime is less than or equal to params.unmod_since */
    store->cls_obj_check_mtime(op, params.unmod_since, params.high_precision_time, CLS_RGW_CHECK_TIME_MTIME_LE);
  }
  uint64_t obj_accounted_size = state->accounted_size;

  if (!real_clock::is_zero(params.expiration_time)) {
    bufferlist bl;
    real_time delete_at;

    if (state->get_attr(RGW_ATTR_DELETE_AT, bl)) {
      try {
        auto iter = bl.cbegin();
        decode(delete_at, iter);
      } catch (buffer::error& err) {
        ldout(store->ctx(), 0) << "ERROR: couldn't decode RGW_ATTR_DELETE_AT" << dendl;
	return -EIO;
      }

      if (params.expiration_time != delete_at) {
        return -ERR_PRECONDITION_FAILED;
      }
    } else {
      return -ERR_PRECONDITION_FAILED;
    }
  }

  if (!state->exists) {
    target->invalidate_state();
    return -ENOENT;
  }

  r = target->prepare_atomic_modification(op, false, NULL, NULL, NULL, true, false);
  if (r < 0)
    return r;

  RGWBucketInfo& bucket_info = target->get_bucket_info();

  RGWRados::Bucket bop(store, bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, obj);
  
  index_op.set_zones_trace(params.zones_trace);
  index_op.set_bilog_flags(params.bilog_flags);

  r = index_op.prepare(CLS_RGW_OP_DEL, &state->write_tag);
  if (r < 0)
    return r;

  store->remove_rgw_head_obj(op);
  r = ref.ioctx.operate(ref.obj.oid, &op);

  /* raced with another operation, object state is indeterminate */
  const bool need_invalidate = (r == -ECANCELED);

  int64_t poolid = ref.ioctx.get_id();
  if (r >= 0) {
    tombstone_cache_t *obj_tombstone_cache = store->get_tombstone_cache();
    if (obj_tombstone_cache) {
      tombstone_entry entry{*state};
      obj_tombstone_cache->add(obj, entry);
    }
    r = index_op.complete_del(poolid, ref.ioctx.get_last_version(), state->mtime, params.remove_objs);
    
    int ret = target->complete_atomic_modification();
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: complete_atomic_modification returned ret=" << ret << dendl;
    }
    /* other than that, no need to propagate error */
  } else {
    int ret = index_op.cancel();
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: index_op.cancel() returned ret=" << ret << dendl;
    }
  }

  if (need_invalidate) {
    target->invalidate_state();
  }

  if (r < 0)
    return r;

  /* update quota cache */
  store->quota_handler->update_stats(params.bucket_owner, obj.bucket, -1, 0, obj_accounted_size);

  return 0;
}

int RGWRados::delete_obj(RGWObjectCtx& obj_ctx,
                         const RGWBucketInfo& bucket_info,
                         const rgw_obj& obj,
                         int versioning_status,
                         uint16_t bilog_flags,
                         const real_time& expiration_time,
                         rgw_zone_set *zones_trace)
{
  RGWRados::Object del_target(this, bucket_info, obj_ctx, obj);
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket_info.owner;
  del_op.params.versioning_status = versioning_status;
  del_op.params.bilog_flags = bilog_flags;
  del_op.params.expiration_time = expiration_time;
  del_op.params.zones_trace = zones_trace;

  return del_op.delete_obj();
}

int RGWRados::delete_raw_obj(const rgw_raw_obj& obj)
{
  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;

  op.remove();
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::delete_obj_index(const rgw_obj& obj)
{
  std::string oid, key;
  get_obj_bucket_and_oid_loc(obj, oid, key);

  auto obj_ctx = svc.sysobj->init_obj_ctx();

  RGWBucketInfo bucket_info;
  int ret = get_bucket_instance_info(obj_ctx, obj.bucket, bucket_info, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << "() get_bucket_instance_info(bucket=" << obj.bucket << ") returned ret=" << ret << dendl;
    return ret;
  }

  RGWRados::Bucket bop(this, bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, obj);

  real_time removed_mtime;
  int r = index_op.complete_del(-1 /* pool */, 0, removed_mtime, NULL);

  return r;
}

static void generate_fake_tag(RGWRados *store, map<string, bufferlist>& attrset, RGWObjManifest& manifest, bufferlist& manifest_bl, bufferlist& tag_bl)
{
  string tag;

  RGWObjManifest::obj_iterator mi = manifest.obj_begin();
  if (mi != manifest.obj_end()) {
    if (manifest.has_tail()) // first object usually points at the head, let's skip to a more unique part
      ++mi;
    tag = mi.get_location().get_raw_obj(store).oid;
    tag.append("_");
  }

  unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char md5_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  MD5 hash;
  hash.Update((const unsigned char *)manifest_bl.c_str(), manifest_bl.length());

  map<string, bufferlist>::iterator iter = attrset.find(RGW_ATTR_ETAG);
  if (iter != attrset.end()) {
    bufferlist& bl = iter->second;
    hash.Update((const unsigned char *)bl.c_str(), bl.length());
  }

  hash.Final(md5);
  buf_to_hex(md5, CEPH_CRYPTO_MD5_DIGESTSIZE, md5_str);
  tag.append(md5_str);

  ldout(store->ctx(), 10) << "generate_fake_tag new tag=" << tag << dendl;

  tag_bl.append(tag.c_str(), tag.size() + 1);
}

static bool is_olh(map<string, bufferlist>& attrs)
{
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_OLH_INFO);
  return (iter != attrs.end());
}

static bool has_olh_tag(map<string, bufferlist>& attrs)
{
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_OLH_ID_TAG);
  return (iter != attrs.end());
}

int RGWRados::get_olh_target_state(RGWObjectCtx& obj_ctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                                   RGWObjState *olh_state, RGWObjState **target_state)
{
  ceph_assert(olh_state->is_olh);

  rgw_obj target;
  int r = RGWRados::follow_olh(bucket_info, obj_ctx, olh_state, obj, &target); /* might return -EAGAIN */
  if (r < 0) {
    return r;
  }
  r = get_obj_state(&obj_ctx, bucket_info, target, target_state, false);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWRados::get_obj_state_impl(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                                 RGWObjState **state, bool follow_olh, bool assume_noent)
{
  if (obj.empty()) {
    return -EINVAL;
  }

  bool need_follow_olh = follow_olh && obj.key.instance.empty();

  RGWObjState *s = rctx->get_state(obj);
  ldout(cct, 20) << "get_obj_state: rctx=" << (void *)rctx << " obj=" << obj << " state=" << (void *)s << " s->prefetch_data=" << s->prefetch_data << dendl;
  *state = s;
  if (s->has_attrs) {
    if (s->is_olh && need_follow_olh) {
      return get_olh_target_state(*rctx, bucket_info, obj, s, state);
    }
    return 0;
  }

  s->obj = obj;

  rgw_raw_obj raw_obj;
  obj_to_raw(bucket_info.placement_rule, obj, &raw_obj);

  int r = -ENOENT;

  if (!assume_noent) {
    r = RGWRados::raw_obj_stat(raw_obj, &s->size, &s->mtime, &s->epoch, &s->attrset, (s->prefetch_data ? &s->data : NULL), NULL);
  }

  if (r == -ENOENT) {
    s->exists = false;
    s->has_attrs = true;
    tombstone_entry entry;
    if (obj_tombstone_cache && obj_tombstone_cache->find(obj, entry)) {
      s->mtime = entry.mtime;
      s->zone_short_id = entry.zone_short_id;
      s->pg_ver = entry.pg_ver;
      ldout(cct, 20) << __func__ << "(): found obj in tombstone cache: obj=" << obj
          << " mtime=" << s->mtime << " pgv=" << s->pg_ver << dendl;
    } else {
      s->mtime = real_time();
    }
    return 0;
  }
  if (r < 0)
    return r;

  s->exists = true;
  s->has_attrs = true;
  s->accounted_size = s->size;

  auto iter = s->attrset.find(RGW_ATTR_ETAG);
  if (iter != s->attrset.end()) {
    /* get rid of extra null character at the end of the etag, as we used to store it like that */
    bufferlist& bletag = iter->second;
    if (bletag.length() > 0 && bletag[bletag.length() - 1] == '\0') {
      bufferlist newbl;
      bletag.splice(0, bletag.length() - 1, &newbl);
      bletag.claim(newbl);
    }
  }

  iter = s->attrset.find(RGW_ATTR_COMPRESSION);
  const bool compressed = (iter != s->attrset.end());
  if (compressed) {
    // use uncompressed size for accounted_size
    try {
      RGWCompressionInfo info;
      auto p = iter->second.cbegin();
      decode(info, p);
      s->accounted_size = info.orig_size; 
    } catch (buffer::error&) {
      dout(0) << "ERROR: could not decode compression info for object: " << obj << dendl;
      return -EIO;
    }
  }

  iter = s->attrset.find(RGW_ATTR_SHADOW_OBJ);
  if (iter != s->attrset.end()) {
    bufferlist bl = iter->second;
    bufferlist::iterator it = bl.begin();
    it.copy(bl.length(), s->shadow_obj);
    s->shadow_obj[bl.length()] = '\0';
  }
  s->obj_tag = s->attrset[RGW_ATTR_ID_TAG];
  auto ttiter = s->attrset.find(RGW_ATTR_TAIL_TAG);
  if (ttiter != s->attrset.end()) {
    s->tail_tag = s->attrset[RGW_ATTR_TAIL_TAG];
  }

  bufferlist manifest_bl = s->attrset[RGW_ATTR_MANIFEST];
  if (manifest_bl.length()) {
    auto miter = manifest_bl.cbegin();
    try {
      decode(s->manifest, miter);
      s->has_manifest = true;
      s->manifest.set_head(bucket_info.placement_rule, obj, s->size); /* patch manifest to reflect the head we just read, some manifests might be
                                             broken due to old bugs */
      s->size = s->manifest.get_obj_size();
      if (!compressed)
        s->accounted_size = s->size;
    } catch (buffer::error& err) {
      ldout(cct, 0) << "ERROR: couldn't decode manifest" << dendl;
      return -EIO;
    }
    ldout(cct, 10) << "manifest: total_size = " << s->manifest.get_obj_size() << dendl;
    if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>() && \
	s->manifest.has_explicit_objs()) {
      RGWObjManifest::obj_iterator mi;
      for (mi = s->manifest.obj_begin(); mi != s->manifest.obj_end(); ++mi) {
        ldout(cct, 20) << "manifest: ofs=" << mi.get_ofs() << " loc=" << mi.get_location().get_raw_obj(this) << dendl;
      }
    }

    if (!s->obj_tag.length()) {
      /*
       * Uh oh, something's wrong, object with manifest should have tag. Let's
       * create one out of the manifest, would be unique
       */
      generate_fake_tag(this, s->attrset, s->manifest, manifest_bl, s->obj_tag);
      s->fake_tag = true;
    }
  }
  map<string, bufferlist>::iterator aiter = s->attrset.find(RGW_ATTR_PG_VER);
  if (aiter != s->attrset.end()) {
    bufferlist& pg_ver_bl = aiter->second;
    if (pg_ver_bl.length()) {
      auto pgbl = pg_ver_bl.cbegin();
      try {
        decode(s->pg_ver, pgbl);
      } catch (buffer::error& err) {
        ldout(cct, 0) << "ERROR: couldn't decode pg ver attr for object " << s->obj << ", non-critical error, ignoring" << dendl;
      }
    }
  }
  aiter = s->attrset.find(RGW_ATTR_SOURCE_ZONE);
  if (aiter != s->attrset.end()) {
    bufferlist& zone_short_id_bl = aiter->second;
    if (zone_short_id_bl.length()) {
      auto zbl = zone_short_id_bl.cbegin();
      try {
        decode(s->zone_short_id, zbl);
      } catch (buffer::error& err) {
        ldout(cct, 0) << "ERROR: couldn't decode zone short id attr for object " << s->obj << ", non-critical error, ignoring" << dendl;
      }
    }
  }
  if (s->obj_tag.length())
    ldout(cct, 20) << "get_obj_state: setting s->obj_tag to " << s->obj_tag.c_str() << dendl;
  else
    ldout(cct, 20) << "get_obj_state: s->obj_tag was set empty" << dendl;

  /* an object might not be olh yet, but could have olh id tag, so we should set it anyway if
   * it exist, and not only if is_olh() returns true
   */
  iter = s->attrset.find(RGW_ATTR_OLH_ID_TAG);
  if (iter != s->attrset.end()) {
    s->olh_tag = iter->second;
  }

  if (is_olh(s->attrset)) {
    s->is_olh = true;

    ldout(cct, 20) << __func__ << ": setting s->olh_tag to " << string(s->olh_tag.c_str(), s->olh_tag.length()) << dendl;

    if (need_follow_olh) {
      return get_olh_target_state(*rctx, bucket_info, obj, s, state);
    }
  }

  return 0;
}

int RGWRados::get_obj_state(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWObjState **state,
                            bool follow_olh, bool assume_noent)
{
  int ret;

  do {
    ret = get_obj_state_impl(rctx, bucket_info, obj, state, follow_olh, assume_noent);
  } while (ret == -EAGAIN);

  return ret;
}

int RGWRados::Object::get_manifest(RGWObjManifest **pmanifest)
{
  RGWObjState *astate;
  int r = get_state(&astate, true);
  if (r < 0) {
    return r;
  }

  *pmanifest = &astate->manifest;

  return 0;
}

int RGWRados::Object::Read::get_attr(const char *name, bufferlist& dest)
{
  RGWObjState *state;
  int r = source->get_state(&state, true);
  if (r < 0)
    return r;
  if (!state->exists)
    return -ENOENT;
  if (!state->get_attr(name, dest))
    return -ENODATA;

  return 0;
}


int RGWRados::Object::Stat::stat_async()
{
  RGWObjectCtx& ctx = source->get_ctx();
  rgw_obj& obj = source->get_obj();
  RGWRados *store = source->get_store();

  RGWObjState *s = ctx.get_state(obj); /* calling this one directly because otherwise a sync request will be sent */
  result.obj = obj;
  if (s->has_attrs) {
    state.ret = 0;
    result.size = s->size;
    result.mtime = ceph::real_clock::to_timespec(s->mtime);
    result.attrs = s->attrset;
    result.has_manifest = s->has_manifest;
    result.manifest = s->manifest;
    return 0;
  }

  string oid;
  string loc;
  get_obj_bucket_and_oid_loc(obj, oid, loc);

  int r = store->get_obj_head_ioctx(source->get_bucket_info(), obj, &state.io_ctx);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  op.stat2(&result.size, &result.mtime, NULL);
  op.getxattrs(&result.attrs, NULL);
  state.completion = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  state.io_ctx.locator_set_key(loc);
  r = state.io_ctx.aio_operate(oid, state.completion, &op, NULL);
  if (r < 0) {
    ldout(store->ctx(), 5) << __func__
						   << ": ERROR: aio_operate() returned ret=" << r
						   << dendl;
    return r;
  }

  return 0;
}


int RGWRados::Object::Stat::wait()
{
  if (!state.completion) {
    return state.ret;
  }

  state.completion->wait_for_safe();
  state.ret = state.completion->get_return_value();
  state.completion->release();

  if (state.ret != 0) {
    return state.ret;
  }

  return finish();
}

int RGWRados::Object::Stat::finish()
{
  map<string, bufferlist>::iterator iter = result.attrs.find(RGW_ATTR_MANIFEST);
  if (iter != result.attrs.end()) {
    bufferlist& bl = iter->second;
    auto biter = bl.cbegin();
    try {
      decode(result.manifest, biter);
    } catch (buffer::error& err) {
      RGWRados *store = source->get_store();
      ldout(store->ctx(), 0) << "ERROR: " << __func__ << ": failed to decode manifest"  << dendl;
      return -EIO;
    }
    result.has_manifest = true;
  }

  return 0;
}

int RGWRados::append_atomic_test(RGWObjectCtx *rctx,
                                 const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                                 ObjectOperation& op, RGWObjState **pstate)
{
  if (!rctx)
    return 0;

  int r = get_obj_state(rctx, bucket_info, obj, pstate, false);
  if (r < 0)
    return r;

  return append_atomic_test(*pstate, op);
}

int RGWRados::append_atomic_test(const RGWObjState* state,
                                 librados::ObjectOperation& op)
{
  if (!state->is_atomic) {
    ldout(cct, 20) << "state for obj=" << state->obj << " is not atomic, not appending atomic test" << dendl;
    return 0;
  }

  if (state->obj_tag.length() > 0 && !state->fake_tag) {// check for backward compatibility
    op.cmpxattr(RGW_ATTR_ID_TAG, LIBRADOS_CMPXATTR_OP_EQ, state->obj_tag);
  } else {
    ldout(cct, 20) << "state->obj_tag is empty, not appending atomic test" << dendl;
  }
  return 0;
}

int RGWRados::Object::get_state(RGWObjState **pstate, bool follow_olh, bool assume_noent)
{
  return store->get_obj_state(&ctx, bucket_info, obj, pstate, follow_olh, assume_noent);
}

void RGWRados::Object::invalidate_state()
{
  ctx.invalidate(obj);
}

int RGWRados::Object::prepare_atomic_modification(ObjectWriteOperation& op, bool reset_obj, const string *ptag,
                                                  const char *if_match, const char *if_nomatch, bool removal_op,
                                                  bool modify_tail)
{
  int r = get_state(&state, false);
  if (r < 0)
    return r;

  bool need_guard = (state->has_manifest || (state->obj_tag.length() != 0) ||
                     if_match != NULL || if_nomatch != NULL) &&
                     (!state->fake_tag);

  if (!state->is_atomic) {
    ldout(store->ctx(), 20) << "prepare_atomic_modification: state is not atomic. state=" << (void *)state << dendl;

    if (reset_obj) {
      op.create(false);
      store->remove_rgw_head_obj(op); // we're not dropping reference here, actually removing object
    }

    return 0;
  }

  if (need_guard) {
    /* first verify that the object wasn't replaced under */
    if (if_nomatch == NULL || strcmp(if_nomatch, "*") != 0) {
      op.cmpxattr(RGW_ATTR_ID_TAG, LIBRADOS_CMPXATTR_OP_EQ, state->obj_tag); 
      // FIXME: need to add FAIL_NOTEXIST_OK for racing deletion
    }

    if (if_match) {
      if (strcmp(if_match, "*") == 0) {
        // test the object is existing
        if (!state->exists) {
          return -ERR_PRECONDITION_FAILED;
        }
      } else {
        bufferlist bl;
        if (!state->get_attr(RGW_ATTR_ETAG, bl) ||
            strncmp(if_match, bl.c_str(), bl.length()) != 0) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }

    if (if_nomatch) {
      if (strcmp(if_nomatch, "*") == 0) {
        // test the object is NOT existing
        if (state->exists) {
          return -ERR_PRECONDITION_FAILED;
        }
      } else {
        bufferlist bl;
        if (!state->get_attr(RGW_ATTR_ETAG, bl) ||
            strncmp(if_nomatch, bl.c_str(), bl.length()) == 0) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
  }

  if (reset_obj) {
    if (state->exists) {
      op.create(false);
      store->remove_rgw_head_obj(op);
    } else {
      op.create(true);
    }
  }

  if (removal_op) {
    /* the object is being removed, no need to update its tag */
    return 0;
  }

  if (ptag) {
    state->write_tag = *ptag;
  } else {
    append_rand_alpha(store->ctx(), state->write_tag, state->write_tag, 32);
  }
  bufferlist bl;
  bl.append(state->write_tag.c_str(), state->write_tag.size() + 1);

  ldout(store->ctx(), 10) << "setting object write_tag=" << state->write_tag << dendl;

  op.setxattr(RGW_ATTR_ID_TAG, bl);
  if (modify_tail) {
    op.setxattr(RGW_ATTR_TAIL_TAG, bl);
  }

  return 0;
}

/**
 * Set an attr on an object.
 * bucket: name of the bucket holding the object
 * obj: name of the object to set the attr on
 * name: the attr to set
 * bl: the contents of the attr
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::set_attr(void *ctx, const RGWBucketInfo& bucket_info, rgw_obj& obj, const char *name, bufferlist& bl)
{
  map<string, bufferlist> attrs;
  attrs[name] = bl;
  return set_attrs(ctx, bucket_info, obj, attrs, NULL);
}

int RGWRados::set_attrs(void *ctx, const RGWBucketInfo& bucket_info, rgw_obj& obj,
                        map<string, bufferlist>& attrs,
                        map<string, bufferlist>* rmattrs)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }
  RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);

  ObjectWriteOperation op;
  RGWObjState *state = NULL;

  r = append_atomic_test(rctx, bucket_info, obj, op, &state);
  if (r < 0)
    return r;

  map<string, bufferlist>::iterator iter;
  if (rmattrs) {
    for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op.rmxattr(name.c_str());
    }
  }

  const rgw_bucket& bucket = obj.bucket;

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name.c_str(), bl);

    if (name.compare(RGW_ATTR_DELETE_AT) == 0) {
      real_time ts;
      try {
        decode(ts, bl);

        rgw_obj_index_key obj_key;
        obj.key.get_index_key(&obj_key);

        objexp_hint_add(ts, bucket.tenant, bucket.name, bucket.bucket_id, obj_key);
      } catch (buffer::error& err) {
	ldout(cct, 0) << "ERROR: failed to decode " RGW_ATTR_DELETE_AT << " attr" << dendl;
      }
    }
  }

  if (!op.size())
    return 0;

  RGWObjectCtx obj_ctx(this);

  bufferlist bl;
  RGWRados::Bucket bop(this, bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, obj);

  if (state) {
    string tag;
    append_rand_alpha(cct, tag, tag, 32);
    state->write_tag = tag;
    r = index_op.prepare(CLS_RGW_OP_ADD, &state->write_tag);

    if (r < 0)
      return r;

    bl.append(tag.c_str(), tag.size() + 1);
    op.setxattr(RGW_ATTR_ID_TAG,  bl);
  }


  real_time mtime = real_clock::now();
  struct timespec mtime_ts = real_clock::to_timespec(mtime);
  op.mtime2(&mtime_ts);
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (state) {
    if (r >= 0) {
      bufferlist acl_bl = attrs[RGW_ATTR_ACL];
      bufferlist etag_bl = attrs[RGW_ATTR_ETAG];
      bufferlist content_type_bl = attrs[RGW_ATTR_CONTENT_TYPE];
      string etag = rgw_bl_str(etag_bl);
      string content_type = rgw_bl_str(content_type_bl);
      string storage_class;
      auto iter = attrs.find(RGW_ATTR_STORAGE_CLASS);
      if (iter != attrs.end()) {
        storage_class = rgw_bl_str(iter->second);
      }
      uint64_t epoch = ref.ioctx.get_last_version();
      int64_t poolid = ref.ioctx.get_id();
      r = index_op.complete(poolid, epoch, state->size, state->accounted_size,
                            mtime, etag, content_type, storage_class, &acl_bl,
                            RGWObjCategory::Main, NULL);
    } else {
      int ret = index_op.cancel();
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: complete_update_index_cancel() returned ret=" << ret << dendl;
      }
    }
  }
  if (r < 0)
    return r;

  if (state) {
    state->obj_tag.swap(bl);
    if (rmattrs) {
      for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
        state->attrset.erase(iter->first);
      }
    }
    for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
      state->attrset[iter->first] = iter->second;
    }
  }

  return 0;
}

int RGWRados::Object::Read::prepare()
{
  RGWRados *store = source->get_store();
  CephContext *cct = store->ctx();

  bufferlist etag;

  map<string, bufferlist>::iterator iter;

  RGWObjState *astate;
  int r = source->get_state(&astate, true);
  if (r < 0)
    return r;

  if (!astate->exists) {
    return -ENOENT;
  }

  const RGWBucketInfo& bucket_info = source->get_bucket_info();

  state.obj = astate->obj;
  store->obj_to_raw(bucket_info.placement_rule, state.obj, &state.head_obj);

  state.cur_pool = state.head_obj.pool;
  state.cur_ioctx = &state.io_ctxs[state.cur_pool];

  r = store->get_obj_head_ioctx(bucket_info, state.obj, state.cur_ioctx);
  if (r < 0) {
    return r;
  }
  if (params.attrs) {
    *params.attrs = astate->attrset;
    if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      for (iter = params.attrs->begin(); iter != params.attrs->end(); ++iter) {
        ldout(cct, 20) << "Read xattr: " << iter->first << dendl;
      }
    }
  }

  /* Convert all times go GMT to make them compatible */
  if (conds.mod_ptr || conds.unmod_ptr) {
    obj_time_weight src_weight;
    src_weight.init(astate);
    src_weight.high_precision = conds.high_precision_time;

    obj_time_weight dest_weight;
    dest_weight.high_precision = conds.high_precision_time;

    if (conds.mod_ptr) {
      dest_weight.init(*conds.mod_ptr, conds.mod_zone_id, conds.mod_pg_ver);
      ldout(cct, 10) << "If-Modified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (!(dest_weight < src_weight)) {
        return -ERR_NOT_MODIFIED;
      }
    }

    if (conds.unmod_ptr) {
      dest_weight.init(*conds.unmod_ptr, conds.mod_zone_id, conds.mod_pg_ver);
      ldout(cct, 10) << "If-UnModified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (dest_weight < src_weight) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
  if (conds.if_match || conds.if_nomatch) {
    r = get_attr(RGW_ATTR_ETAG, etag);
    if (r < 0)
      return r;

    

    if (conds.if_match) {
      string if_match_str = rgw_string_unquote(conds.if_match);
      ldout(cct, 10) << "ETag: " << string(etag.c_str(), etag.length()) << " " << " If-Match: " << if_match_str << dendl;
      if (if_match_str.compare(0, etag.length(), etag.c_str(), etag.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }

    if (conds.if_nomatch) {
      string if_nomatch_str = rgw_string_unquote(conds.if_nomatch);
      ldout(cct, 10) << "ETag: " << string(etag.c_str(), etag.length()) << " " << " If-NoMatch: " << if_nomatch_str << dendl;
      if (if_nomatch_str.compare(0, etag.length(), etag.c_str(), etag.length()) == 0) {
        return -ERR_NOT_MODIFIED;
      }
    }
  }

  if (params.obj_size)
    *params.obj_size = astate->size;
  if (params.lastmod)
    *params.lastmod = astate->mtime;

  return 0;
}

int RGWRados::Object::Read::range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end)
{
  if (ofs < 0) {
    ofs += obj_size;
    if (ofs < 0)
      ofs = 0;
    end = obj_size - 1;
  } else if (end < 0) {
    end = obj_size - 1;
  }

  if (obj_size > 0) {
    if (ofs >= (off_t)obj_size) {
      return -ERANGE;
    }
    if (end >= (off_t)obj_size) {
      end = obj_size - 1;
    }
  }
  return 0;
}

int RGWRados::Bucket::UpdateIndex::guard_reshard(BucketShard **pbs, std::function<int(BucketShard *)> call)
{
  RGWRados *store = target->get_store();
  BucketShard *bs;
  int r;

#define NUM_RESHARD_RETRIES 10
  for (int i = 0; i < NUM_RESHARD_RETRIES; ++i) {
    int ret = get_bucket_shard(&bs);
    if (ret < 0) {
      ldout(store->ctx(), 5) << "failed to get BucketShard object: ret=" << ret << dendl;
      return ret;
    }
    r = call(bs);
    if (r != -ERR_BUSY_RESHARDING) {
      break;
    }
    ldout(store->ctx(), 0) << "NOTICE: resharding operation on bucket index detected, blocking" << dendl;
    string new_bucket_id;
    r = store->block_while_resharding(bs, &new_bucket_id,
                                      target->bucket_info, null_yield);
    if (r == -ERR_BUSY_RESHARDING) {
      continue;
    }
    if (r < 0) {
      return r;
    }
    ldout(store->ctx(), 20) << "reshard completion identified, new_bucket_id=" << new_bucket_id << dendl;
    i = 0; /* resharding is finished, make sure we can retry */
    r = target->update_bucket_id(new_bucket_id);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: update_bucket_id() new_bucket_id=" << new_bucket_id << " returned r=" << r << dendl;
      return r;
    }
    invalidate_bs();
  }

  if (r < 0) {
    return r;
  }

  if (pbs) {
    *pbs = bs;
  }

  return 0;
}

int RGWRados::Bucket::UpdateIndex::prepare(RGWModifyOp op, const string *write_tag)
{
  if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();

  if (write_tag && write_tag->length()) {
    optag = string(write_tag->c_str(), write_tag->length());
  } else {
    if (optag.empty()) {
      append_rand_alpha(store->ctx(), optag, optag, 32);
    }
  }

  int r = guard_reshard(nullptr, [&](BucketShard *bs) -> int {
				   return store->cls_obj_prepare_op(*bs, op, optag, obj, bilog_flags, zones_trace);
				 });

  if (r < 0) {
    return r;
  }
  prepared = true;

  return 0;
}

int RGWRados::Bucket::UpdateIndex::complete(int64_t poolid, uint64_t epoch,
                                            uint64_t size, uint64_t accounted_size,
                                            ceph::real_time& ut, const string& etag,
                                            const string& content_type, const string& storage_class,
                                            bufferlist *acl_bl,
                                            RGWObjCategory category,
                                            list<rgw_obj_index_key> *remove_objs, const string *user_data)
{
  if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();
  BucketShard *bs;

  int ret = get_bucket_shard(&bs);
  if (ret < 0) {
    ldout(store->ctx(), 5) << "failed to get BucketShard object: ret=" << ret << dendl;
    return ret;
  }

  rgw_bucket_dir_entry ent;
  obj.key.get_index_key(&ent.key);
  ent.meta.size = size;
  ent.meta.accounted_size = accounted_size;
  ent.meta.mtime = ut;
  ent.meta.etag = etag;
  ent.meta.storage_class = storage_class;
  if (user_data)
    ent.meta.user_data = *user_data;

  ACLOwner owner;
  if (acl_bl && acl_bl->length()) {
    int ret = store->decode_policy(*acl_bl, &owner);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "WARNING: could not decode policy ret=" << ret << dendl;
    }
  }
  ent.meta.owner = owner.get_id().to_str();
  ent.meta.owner_display_name = owner.get_display_name();
  ent.meta.content_type = content_type;

  ret = store->cls_obj_complete_add(*bs, obj, optag, poolid, epoch, ent, category, remove_objs, bilog_flags, zones_trace);

  if (target->bucket_info.datasync_flag_enabled()) {
    int r = store->data_log->add_entry(bs->bucket, bs->shard_id);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
    }
  }

  return ret;
}

int RGWRados::Bucket::UpdateIndex::complete_del(int64_t poolid, uint64_t epoch,
                                                real_time& removed_mtime,
                                                list<rgw_obj_index_key> *remove_objs)
{
  if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();
  BucketShard *bs;

  int ret = get_bucket_shard(&bs);
  if (ret < 0) {
    ldout(store->ctx(), 5) << "failed to get BucketShard object: ret=" << ret << dendl;
    return ret;
  }

  ret = store->cls_obj_complete_del(*bs, optag, poolid, epoch, obj, removed_mtime, remove_objs, bilog_flags, zones_trace);

  if (target->bucket_info.datasync_flag_enabled()) {
    int r = store->data_log->add_entry(bs->bucket, bs->shard_id);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
    }
  }

  return ret;
}


int RGWRados::Bucket::UpdateIndex::cancel()
{
  if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();
  BucketShard *bs;

  int ret = guard_reshard(&bs, [&](BucketShard *bs) -> int {
				 return store->cls_obj_complete_cancel(*bs, optag, obj, bilog_flags, zones_trace);
			       });

  /*
   * need to update data log anyhow, so that whoever follows needs to update its internal markers
   * for following the specific bucket shard log. Otherwise they end up staying behind, and users
   * have no way to tell that they're all caught up
   */
  if (target->bucket_info.datasync_flag_enabled()) {
    int r = store->data_log->add_entry(bs->bucket, bs->shard_id);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
    }
  }

  return ret;
}

int RGWRados::Object::Read::read(int64_t ofs, int64_t end, bufferlist& bl)
{
  RGWRados *store = source->get_store();
  CephContext *cct = store->ctx();

  rgw_raw_obj read_obj;
  uint64_t read_ofs = ofs;
  uint64_t len, read_len;
  bool reading_from_head = true;
  ObjectReadOperation op;

  bool merge_bl = false;
  bufferlist *pbl = &bl;
  bufferlist read_bl;
  uint64_t max_chunk_size;

  RGWObjState *astate;
  int r = source->get_state(&astate, true);
  if (r < 0)
    return r;

  if (astate->size == 0) {
    end = 0;
  } else if (end >= (int64_t)astate->size) {
    end = astate->size - 1;
  }

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (astate->has_manifest && astate->manifest.has_tail()) {
    /* now get the relevant object part */
    RGWObjManifest::obj_iterator iter = astate->manifest.obj_find(ofs);

    uint64_t stripe_ofs = iter.get_stripe_ofs();
    read_obj = iter.get_location().get_raw_obj(store);
    len = std::min(len, iter.get_stripe_size() - (ofs - stripe_ofs));
    read_ofs = iter.location_ofs() + (ofs - stripe_ofs);
    reading_from_head = (read_obj == state.head_obj);
  } else {
    read_obj = state.head_obj;
  }

  r = store->get_max_chunk_size(read_obj.pool, &max_chunk_size);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to get max_chunk_size() for pool " << read_obj.pool << dendl;
    return r;
  }

  if (len > max_chunk_size)
    len = max_chunk_size;


  read_len = len;

  if (reading_from_head) {
    /* only when reading from the head object do we need to do the atomic test */
    r = store->append_atomic_test(&source->get_ctx(), source->get_bucket_info(), state.obj, op, &astate);
    if (r < 0)
      return r;

    if (astate && astate->prefetch_data) {
      if (!ofs && astate->data.length() >= len) {
        bl = astate->data;
        return bl.length();
      }

      if (ofs < astate->data.length()) {
        unsigned copy_len = std::min((uint64_t)astate->data.length() - ofs, len);
        astate->data.copy(ofs, copy_len, bl);
        read_len -= copy_len;
        read_ofs += copy_len;
        if (!read_len)
	  return bl.length();

        merge_bl = true;
        pbl = &read_bl;
      }
    }
  }

  ldout(cct, 20) << "rados->read obj-ofs=" << ofs << " read_ofs=" << read_ofs << " read_len=" << read_len << dendl;
  op.read(read_ofs, read_len, pbl, NULL);

  if (state.cur_pool != read_obj.pool) {
    auto iter = state.io_ctxs.find(read_obj.pool);
    if (iter == state.io_ctxs.end()) {
      state.cur_ioctx = &state.io_ctxs[read_obj.pool];
      r = store->open_pool_ctx(read_obj.pool, *state.cur_ioctx);
      if (r < 0) {
        ldout(cct, 20) << "ERROR: failed to open pool context for pool=" << read_obj.pool << " r=" << r << dendl;
        return r;
      }
    } else {
      state.cur_ioctx = &iter->second;
    }
    state.cur_pool = read_obj.pool;
  }

  state.cur_ioctx->locator_set_key(read_obj.loc);

  r = state.cur_ioctx->operate(read_obj.oid, &op, NULL);
  ldout(cct, 20) << "rados->read r=" << r << " bl.length=" << bl.length() << dendl;

  if (r < 0) {
    return r;
  }

  if (merge_bl) {
    bl.append(read_bl);
  }

  return bl.length();
}

struct get_obj_data {
  RGWRados* store;
  RGWGetDataCB* client_cb;
  rgw::Aio* aio;
  uint64_t offset; // next offset to write to client
  rgw::AioResultList completed; // completed read results, sorted by offset

  get_obj_data(RGWRados* store, RGWGetDataCB* cb, rgw::Aio* aio, uint64_t offset)
    : store(store), client_cb(cb), aio(aio), offset(offset) {}

  int flush(rgw::AioResultList&& results) {
    int r = rgw::check_for_errors(results);
    if (r < 0) {
      return r;
    }

    auto cmp = [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; };
    results.sort(cmp); // merge() requires results to be sorted first
    completed.merge(results, cmp); // merge results in sorted order

    while (!completed.empty() && completed.front().id == offset) {
      auto bl = std::move(completed.front().data);
      completed.pop_front_and_dispose(std::default_delete<rgw::AioResultEntry>{});

      offset += bl.length();
      int r = client_cb->handle_data(bl, 0, bl.length());
      if (r < 0) {
        return r;
      }
    }
    return 0;
  }

  void cancel() {
    // wait for all completions to drain and ignore the results
    aio->drain();
  }

  int drain() {
    auto c = aio->wait();
    while (!c.empty()) {
      int r = flush(std::move(c));
      if (r < 0) {
        cancel();
        return r;
      }
      c = aio->wait();
    }
    return flush(std::move(c));
  }
};

static int _get_obj_iterate_cb(const rgw_raw_obj& read_obj, off_t obj_ofs,
                               off_t read_ofs, off_t len, bool is_head_obj,
                               RGWObjState *astate, void *arg)
{
  struct get_obj_data *d = (struct get_obj_data *)arg;

  return d->store->get_obj_iterate_cb(read_obj, obj_ofs, read_ofs, len,
                                      is_head_obj, astate, arg);
}

int RGWRados::get_obj_iterate_cb(const rgw_raw_obj& read_obj, off_t obj_ofs,
                                 off_t read_ofs, off_t len, bool is_head_obj,
                                 RGWObjState *astate, void *arg)
{
  ObjectReadOperation op;
  struct get_obj_data *d = (struct get_obj_data *)arg;
  string oid, key;

  if (is_head_obj) {
    /* only when reading from the head object do we need to do the atomic test */
    int r = append_atomic_test(astate, op);
    if (r < 0)
      return r;

    if (astate &&
        obj_ofs < astate->data.length()) {
      unsigned chunk_len = std::min((uint64_t)astate->data.length() - obj_ofs, (uint64_t)len);

      r = d->client_cb->handle_data(astate->data, obj_ofs, chunk_len);
      if (r < 0)
        return r;

      len -= chunk_len;
      d->offset += chunk_len;
      read_ofs += chunk_len;
      obj_ofs += chunk_len;
      if (!len)
	  return 0;
    }
  }

  auto obj = d->store->svc.rados->obj(read_obj);
  int r = obj.open();
  if (r < 0) {
    ldout(cct, 4) << "failed to open rados context for " << read_obj << dendl;
    return r;
  }

  ldout(cct, 20) << "rados->get_obj_iterate_cb oid=" << read_obj.oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
  op.read(read_ofs, len, nullptr, nullptr);

  const uint64_t cost = len;
  const uint64_t id = obj_ofs; // use logical object offset for sorting replies

  auto completed = d->aio->submit(obj, &op, cost, id);

  return d->flush(std::move(completed));
}

int RGWRados::Object::Read::iterate(int64_t ofs, int64_t end, RGWGetDataCB *cb)
{
  RGWRados *store = source->get_store();
  CephContext *cct = store->ctx();
  RGWObjectCtx& obj_ctx = source->get_ctx();
  const uint64_t chunk_size = cct->_conf->rgw_get_obj_max_req_size;
  const uint64_t window_size = cct->_conf->rgw_get_obj_window_size;

  rgw::AioThrottle aio(window_size);
  get_obj_data data(store, cb, &aio, ofs);

  int r = store->iterate_obj(obj_ctx, source->get_bucket_info(), state.obj,
                             ofs, end, chunk_size, _get_obj_iterate_cb, &data);
  if (r < 0) {
    ldout(cct, 0) << "iterate_obj() failed with " << r << dendl;
    data.cancel(); // drain completions without writing back to client
    return r;
  }

  return data.drain();
}

int RGWRados::iterate_obj(RGWObjectCtx& obj_ctx,
                          const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                          off_t ofs, off_t end, uint64_t max_chunk_size,
                          iterate_obj_cb cb, void *arg)
{
  rgw_raw_obj head_obj;
  rgw_raw_obj read_obj;
  uint64_t read_ofs = ofs;
  uint64_t len;
  bool reading_from_head = true;
  RGWObjState *astate = NULL;

  obj_to_raw(bucket_info.placement_rule, obj, &head_obj);

  int r = get_obj_state(&obj_ctx, bucket_info, obj, &astate, false);
  if (r < 0) {
    return r;
  }

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (astate->has_manifest) {
    /* now get the relevant object stripe */
    RGWObjManifest::obj_iterator iter = astate->manifest.obj_find(ofs);

    RGWObjManifest::obj_iterator obj_end = astate->manifest.obj_end();

    for (; iter != obj_end && ofs <= end; ++iter) {
      off_t stripe_ofs = iter.get_stripe_ofs();
      off_t next_stripe_ofs = stripe_ofs + iter.get_stripe_size();

      while (ofs < next_stripe_ofs && ofs <= end) {
        read_obj = iter.get_location().get_raw_obj(this);
        uint64_t read_len = std::min(len, iter.get_stripe_size() - (ofs - stripe_ofs));
        read_ofs = iter.location_ofs() + (ofs - stripe_ofs);

        if (read_len > max_chunk_size) {
          read_len = max_chunk_size;
        }

        reading_from_head = (read_obj == head_obj);
        r = cb(read_obj, ofs, read_ofs, read_len, reading_from_head, astate, arg);
	if (r < 0) {
	  return r;
        }

	len -= read_len;
        ofs += read_len;
      }
    }
  } else {
    while (ofs <= end) {
      read_obj = head_obj;
      uint64_t read_len = std::min(len, max_chunk_size);

      r = cb(read_obj, ofs, ofs, read_len, reading_from_head, astate, arg);
      if (r < 0) {
	return r;
      }

      len -= read_len;
      ofs += read_len;
    }
  }

  return 0;
}

int RGWRados::obj_operate(const RGWBucketInfo& bucket_info, const rgw_obj& obj, ObjectWriteOperation *op)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }

  return ref.ioctx.operate(ref.obj.oid, op);
}

int RGWRados::obj_operate(const RGWBucketInfo& bucket_info, const rgw_obj& obj, ObjectReadOperation *op)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }

  bufferlist outbl;

  return ref.ioctx.operate(ref.obj.oid, op, &outbl);
}

int RGWRados::olh_init_modification_impl(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, string *op_tag)
{
  ObjectWriteOperation op;

  ceph_assert(olh_obj.key.instance.empty());

  bool has_tag = (state.exists && has_olh_tag(state.attrset));

  if (!state.exists) {
    op.create(true);
  } else {
    op.assert_exists();
    struct timespec mtime_ts = real_clock::to_timespec(state.mtime);
    op.mtime2(&mtime_ts);
  }

  /*
   * 3 possible cases: olh object doesn't exist, it exists as an olh, it exists as a regular object.
   * If it exists as a regular object we'll need to transform it into an olh. We'll do it in two
   * steps, first change its tag and set the olh pending attrs. Once write is done we'll need to
   * truncate it, remove extra attrs, and send it to the garbage collection. The bucket index olh
   * log will reflect that.
   *
   * Need to generate separate olh and obj tags, as olh can be colocated with object data. obj_tag
   * is used for object data instance, olh_tag for olh instance.
   */
  if (has_tag) {
    /* guard against racing writes */
    bucket_index_guard_olh_op(state, op);
  }

  if (!has_tag) {
    /* obj tag */
    string obj_tag;
    gen_rand_alphanumeric_lower(cct, &obj_tag, 32);

    bufferlist bl;
    bl.append(obj_tag.c_str(), obj_tag.size());
    op.setxattr(RGW_ATTR_ID_TAG, bl);

    state.attrset[RGW_ATTR_ID_TAG] = bl;
    state.obj_tag = bl;

    /* olh tag */
    string olh_tag;
    gen_rand_alphanumeric_lower(cct, &olh_tag, 32);

    bufferlist olh_bl;
    olh_bl.append(olh_tag.c_str(), olh_tag.size());
    op.setxattr(RGW_ATTR_OLH_ID_TAG, olh_bl);

    state.attrset[RGW_ATTR_OLH_ID_TAG] = olh_bl;
    state.olh_tag = olh_bl;
    state.is_olh = true;

    bufferlist verbl;
    op.setxattr(RGW_ATTR_OLH_VER, verbl);
  }

  bufferlist bl;
  RGWOLHPendingInfo pending_info;
  pending_info.time = real_clock::now();
  encode(pending_info, bl);

#define OLH_PENDING_TAG_LEN 32
  /* tag will start with current time epoch, this so that entries are sorted by time */
  char buf[32];
  utime_t ut(pending_info.time);
  snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)ut.sec());
  *op_tag = buf;

  string s;
  gen_rand_alphanumeric_lower(cct, &s, OLH_PENDING_TAG_LEN - op_tag->size());

  op_tag->append(s);

  string attr_name = RGW_ATTR_OLH_PENDING_PREFIX;
  attr_name.append(*op_tag);

  op.setxattr(attr_name.c_str(), bl);

  int ret = obj_operate(bucket_info, olh_obj, &op);
  if (ret < 0) {
    return ret;
  }

  state.exists = true;
  state.attrset[attr_name] = bl;

  return 0;
}

int RGWRados::olh_init_modification(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& obj, string *op_tag)
{
  int ret;

  ret = olh_init_modification_impl(bucket_info, state, obj, op_tag);
  if (ret == -EEXIST) {
    ret = -ECANCELED;
  }

  return ret;
}

int RGWRados::guard_reshard(BucketShard *bs,
			    const rgw_obj& obj_instance,
			    const RGWBucketInfo& bucket_info,
			    std::function<int(BucketShard *)> call)
{
  rgw_obj obj;
  const rgw_obj *pobj = &obj_instance;
  int r;

  for (int i = 0; i < NUM_RESHARD_RETRIES; ++i) {
    r = bs->init(pobj->bucket, *pobj, nullptr /* no RGWBucketInfo */);
    if (r < 0) {
      ldout(cct, 5) << "bs.init() returned ret=" << r << dendl;
      return r;
    }
    r = call(bs);
    if (r != -ERR_BUSY_RESHARDING) {
      break;
    }
    ldout(cct, 0) << "NOTICE: resharding operation on bucket index detected, blocking" << dendl;
    string new_bucket_id;
    r = block_while_resharding(bs, &new_bucket_id, bucket_info, null_yield);
    if (r == -ERR_BUSY_RESHARDING) {
      continue;
    }
    if (r < 0) {
      return r;
    }
    ldout(cct, 20) << "reshard completion identified, new_bucket_id=" << new_bucket_id << dendl;
    i = 0; /* resharding is finished, make sure we can retry */

    obj = *pobj;
    obj.bucket.update_bucket_id(new_bucket_id);
    pobj = &obj;
  }

  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWRados::block_while_resharding(RGWRados::BucketShard *bs,
				     string *new_bucket_id,
                                     const RGWBucketInfo& bucket_info,
                                     optional_yield y)
{
  int ret = 0;
  cls_rgw_bucket_instance_entry entry;

  const int num_retries = 10;
  for (int i=0; i < num_retries; i++) {
    ret = cls_rgw_get_bucket_resharding(bs->index_ctx, bs->bucket_obj, &entry);
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " ERROR: failed to get bucket resharding :"  <<
	cpp_strerror(-ret)<< dendl;
      return ret;
    }
    if (!entry.resharding_in_progress()) {
      *new_bucket_id = entry.new_bucket_instance_id;
      return 0;
    }
    ldout(cct, 20) << "NOTICE: reshard still in progress; " << (i < num_retries - 1 ? "retrying" : "too many retries") << dendl;

    if (i == num_retries - 1) {
      break;
    }

    // If bucket is erroneously marked as resharding (e.g., crash or
    // other error) then fix it. If we can take the bucket reshard
    // lock then it means no other resharding should be taking place,
    // and we're free to clear the flags.
    {
      // since we expect to do this rarely, we'll do our work in a
      // block and erase our work after each try

      RGWObjectCtx obj_ctx(this);
      const rgw_bucket& b = bs->bucket;
      std::string bucket_id = b.get_key();
      RGWBucketReshardLock reshard_lock(this, bucket_info, true);
      ret = reshard_lock.lock();
      if (ret < 0) {
	ldout(cct, 20) << __func__ <<
	  " INFO: failed to take reshard lock for bucket " <<
	  bucket_id << "; expected if resharding underway" << dendl;
      } else {
	ldout(cct, 10) << __func__ <<
	  " INFO: was able to take reshard lock for bucket " <<
	  bucket_id << dendl;
	ret = RGWBucketReshard::clear_resharding(this, bucket_info);
	if (ret < 0) {
	  reshard_lock.unlock();
	  ldout(cct, 0) << __func__ <<
	    " ERROR: failed to clear resharding flags for bucket " <<
	    bucket_id << dendl;
	} else {
	  reshard_lock.unlock();
	  ldout(cct, 5) << __func__ <<
	    " INFO: apparently successfully cleared resharding flags for "
	    "bucket " << bucket_id << dendl;
	  continue; // if we apparently succeed immediately test again
	} // if clear resharding succeeded
      } // if taking of lock succeeded
    } // block to encapsulate recovery from incomplete reshard

    ret = reshard_wait->wait(y);
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " ERROR: bucket is still resharding, please retry" << dendl;
      return ret;
    }
  }
  ldout(cct, 0) << __func__ << " ERROR: bucket is still resharding, please retry" << dendl;
  return -ERR_BUSY_RESHARDING;
}

int RGWRados::bucket_index_link_olh(const RGWBucketInfo& bucket_info, RGWObjState& olh_state, const rgw_obj& obj_instance,
                                    bool delete_marker,
                                    const string& op_tag,
                                    struct rgw_bucket_dir_entry_meta *meta,
                                    uint64_t olh_epoch,
                                    real_time unmod_since, bool high_precision_time,
                                    rgw_zone_set *_zones_trace, bool log_data_change)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, obj_instance, &ref);
  if (r < 0) {
    return r;
  }

  rgw_zone_set zones_trace;
  if (_zones_trace) {
    zones_trace = *_zones_trace;
  }
  zones_trace.insert(svc.zone->get_zone().id);

  BucketShard bs(this);

  cls_rgw_obj_key key(obj_instance.key.get_index_key_name(), obj_instance.key.instance);
  r = guard_reshard(&bs, obj_instance, bucket_info,
		    [&](BucketShard *bs) -> int {
		      librados::ObjectWriteOperation op;
		      cls_rgw_guard_bucket_resharding(op, -ERR_BUSY_RESHARDING);
		      return cls_rgw_bucket_link_olh(bs->index_ctx, op,
						     bs->bucket_obj, key, olh_state.olh_tag, delete_marker, op_tag, meta, olh_epoch,
						     unmod_since, high_precision_time,
						     svc.zone->get_zone().log_data, zones_trace);
                    });
  if (r < 0) {
    ldout(cct, 20) << "cls_rgw_bucket_link_olh() returned r=" << r << dendl;
    return r;
  }

  if (log_data_change && bucket_info.datasync_flag_enabled()) {
    data_log->add_entry(bs.bucket, bs.shard_id);
  }

  return 0;
}

void RGWRados::bucket_index_guard_olh_op(RGWObjState& olh_state, ObjectOperation& op)
{
  ldout(cct, 20) << __func__ << "(): olh_state.olh_tag=" << string(olh_state.olh_tag.c_str(), olh_state.olh_tag.length()) << dendl;
  op.cmpxattr(RGW_ATTR_OLH_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ, olh_state.olh_tag);
}

int RGWRados::bucket_index_unlink_instance(const RGWBucketInfo& bucket_info, const rgw_obj& obj_instance,
                                           const string& op_tag, const string& olh_tag, uint64_t olh_epoch, rgw_zone_set *_zones_trace)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, obj_instance, &ref);
  if (r < 0) {
    return r;
  }

  rgw_zone_set zones_trace;
  if (_zones_trace) {
    zones_trace = *_zones_trace;
  }
  zones_trace.insert(svc.zone->get_zone().id);

  BucketShard bs(this);

  cls_rgw_obj_key key(obj_instance.key.get_index_key_name(), obj_instance.key.instance);
  r = guard_reshard(&bs, obj_instance, bucket_info,
		    [&](BucketShard *bs) -> int {
		      librados::ObjectWriteOperation op;
		      cls_rgw_guard_bucket_resharding(op, -ERR_BUSY_RESHARDING);
		      return cls_rgw_bucket_unlink_instance(bs->index_ctx, op, bs->bucket_obj, key, op_tag,
							    olh_tag, olh_epoch, svc.zone->get_zone().log_data, zones_trace);
                    });
  if (r < 0) {
    ldout(cct, 20) << "cls_rgw_bucket_link_olh() returned r=" << r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::bucket_index_read_olh_log(const RGWBucketInfo& bucket_info, RGWObjState& state,
                                        const rgw_obj& obj_instance, uint64_t ver_marker,
                                        map<uint64_t, vector<rgw_bucket_olh_log_entry> > *log,
                                        bool *is_truncated)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, obj_instance, &ref);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);
  int ret =
    bs.init(obj_instance.bucket, obj_instance, nullptr /* no RGWBucketInfo */);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  string olh_tag(state.olh_tag.c_str(), state.olh_tag.length());

  cls_rgw_obj_key key(obj_instance.key.get_index_key_name(), string());

  ret = guard_reshard(&bs, obj_instance, bucket_info,
		      [&](BucketShard *bs) -> int {
			ObjectReadOperation op;
			cls_rgw_guard_bucket_resharding(op, -ERR_BUSY_RESHARDING);
			return cls_rgw_get_olh_log(bs->index_ctx, bs->bucket_obj, op,
						   key, ver_marker, olh_tag, log, is_truncated);
		      });
  if (ret < 0) {
    ldout(cct, 20) << "cls_rgw_get_olh_log() returned r=" << r << dendl;
    return ret;
  }

  return 0;
}

// a multisite sync bug resulted in the OLH head attributes being overwritten by
// the attributes from another zone, causing link_olh() to fail endlessly due to
// olh_tag mismatch. this attempts to detect this case and reconstruct the OLH
// attributes from the bucket index. see http://tracker.ceph.com/issues/37792
int RGWRados::repair_olh(RGWObjState* state, const RGWBucketInfo& bucket_info,
                         const rgw_obj& obj)
{
  // fetch the current olh entry from the bucket index
  rgw_bucket_olh_entry olh;
  int r = bi_get_olh(bucket_info, obj, &olh);
  if (r < 0) {
    ldout(cct, 0) << "repair_olh failed to read olh entry for " << obj << dendl;
    return r;
  }
  if (olh.tag == rgw_bl_str(state->olh_tag)) { // mismatch already resolved?
    return 0;
  }

  ldout(cct, 4) << "repair_olh setting olh_tag=" << olh.tag
      << " key=" << olh.key << " delete_marker=" << olh.delete_marker << dendl;

  // rewrite OLH_ID_TAG and OLH_INFO from current olh
  ObjectWriteOperation op;
  // assert this is the same olh tag we think we're fixing
  bucket_index_guard_olh_op(*state, op);
  // preserve existing mtime
  struct timespec mtime_ts = ceph::real_clock::to_timespec(state->mtime);
  op.mtime2(&mtime_ts);
  {
    bufferlist bl;
    bl.append(olh.tag.c_str(), olh.tag.size());
    op.setxattr(RGW_ATTR_OLH_ID_TAG, bl);
  }
  {
    RGWOLHInfo info;
    info.target = rgw_obj(bucket_info.bucket, olh.key);
    info.removed = olh.delete_marker;
    bufferlist bl;
    encode(info, bl);
    op.setxattr(RGW_ATTR_OLH_INFO, bl);
  }
  rgw_rados_ref ref;
  r = get_obj_head_ref(bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (r < 0) {
    ldout(cct, 0) << "repair_olh failed to write olh attributes with "
        << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int RGWRados::bucket_index_trim_olh_log(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& obj_instance, uint64_t ver)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, obj_instance, &ref);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);
  int ret =
    bs.init(obj_instance.bucket, obj_instance, nullptr /* no RGWBucketInfo */);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  string olh_tag(state.olh_tag.c_str(), state.olh_tag.length());

  cls_rgw_obj_key key(obj_instance.key.get_index_key_name(), string());

  ret = guard_reshard(&bs, obj_instance, bucket_info,
		      [&](BucketShard *pbs) -> int {
			ObjectWriteOperation op;
			cls_rgw_guard_bucket_resharding(op, -ERR_BUSY_RESHARDING);
			cls_rgw_trim_olh_log(op, key, ver, olh_tag);
			return pbs->index_ctx.operate(pbs->bucket_obj, &op);
                      });
  if (ret < 0) {
    ldout(cct, 20) << "cls_rgw_trim_olh_log() returned r=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::bucket_index_clear_olh(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& obj_instance)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, obj_instance, &ref);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);

  string olh_tag(state.olh_tag.c_str(), state.olh_tag.length());

  cls_rgw_obj_key key(obj_instance.key.get_index_key_name(), string());

  int ret = guard_reshard(&bs, obj_instance, bucket_info,
			  [&](BucketShard *pbs) -> int {
			    ObjectWriteOperation op;
			    cls_rgw_guard_bucket_resharding(op, -ERR_BUSY_RESHARDING);
			    return cls_rgw_clear_olh(pbs->index_ctx, op, pbs->bucket_obj, key, olh_tag);
                          });
  if (ret < 0) {
    ldout(cct, 5) << "cls_rgw_clear_olh() returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::apply_olh_log(RGWObjectCtx& obj_ctx, RGWObjState& state, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                            bufferlist& olh_tag, map<uint64_t, vector<rgw_bucket_olh_log_entry> >& log,
                            uint64_t *plast_ver, rgw_zone_set* zones_trace)
{
  if (log.empty()) {
    return 0;
  }

  librados::ObjectWriteOperation op;

  uint64_t last_ver = log.rbegin()->first;
  *plast_ver = last_ver;

  map<uint64_t, vector<rgw_bucket_olh_log_entry> >::iterator iter = log.begin();

  op.cmpxattr(RGW_ATTR_OLH_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ, olh_tag);
  op.cmpxattr(RGW_ATTR_OLH_VER, CEPH_OSD_CMPXATTR_OP_GT, last_ver);

  struct timespec mtime_ts = real_clock::to_timespec(state.mtime);
  op.mtime2(&mtime_ts);

  bool need_to_link = false;
  cls_rgw_obj_key key;
  bool delete_marker = false;
  list<cls_rgw_obj_key> remove_instances;
  bool need_to_remove = false;

  for (iter = log.begin(); iter != log.end(); ++iter) {
    vector<rgw_bucket_olh_log_entry>::iterator viter = iter->second.begin();
    for (; viter != iter->second.end(); ++viter) {
      rgw_bucket_olh_log_entry& entry = *viter;

      ldout(cct, 20) << "olh_log_entry: op=" << (int)entry.op
                     << " key=" << entry.key.name << "[" << entry.key.instance << "] "
                     << (entry.delete_marker ? "(delete)" : "") << dendl;
      switch (entry.op) {
      case CLS_RGW_OLH_OP_REMOVE_INSTANCE:
        remove_instances.push_back(entry.key);
        break;
      case CLS_RGW_OLH_OP_LINK_OLH:
        need_to_link = true;
        need_to_remove = false;
        key = entry.key;
        delete_marker = entry.delete_marker;
        break;
      case CLS_RGW_OLH_OP_UNLINK_OLH:
        need_to_remove = true;
        need_to_link = false;
        break;
      default:
        ldout(cct, 0) << "ERROR: apply_olh_log: invalid op: " << (int)entry.op << dendl;
        return -EIO;
      }
      string attr_name = RGW_ATTR_OLH_PENDING_PREFIX;
      attr_name.append(entry.op_tag);
      op.rmxattr(attr_name.c_str());
    }
  }

  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }

  const rgw_bucket& bucket = obj.bucket;

  if (need_to_link) {
    rgw_obj target(bucket, key);
    RGWOLHInfo info;
    info.target = target;
    info.removed = delete_marker;
    bufferlist bl;
    encode(info, bl);
    op.setxattr(RGW_ATTR_OLH_INFO, bl);
  }

  /* first remove object instances */
  for (list<cls_rgw_obj_key>::iterator liter = remove_instances.begin();
       liter != remove_instances.end(); ++liter) {
    cls_rgw_obj_key& key = *liter;
    rgw_obj obj_instance(bucket, key);
    int ret = delete_obj(obj_ctx, bucket_info, obj_instance, 0, RGW_BILOG_FLAG_VERSIONED_OP, ceph::real_time(), zones_trace);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "ERROR: delete_obj() returned " << ret << " obj_instance=" << obj_instance << dendl;
      return ret;
    }
  }

  /* update olh object */
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (r == -ECANCELED) {
    r = 0;
  }
  if (r < 0) {
    ldout(cct, 0) << "ERROR: could not apply olh update, r=" << r << dendl;
    return r;
  }

  r = bucket_index_trim_olh_log(bucket_info, state, obj, last_ver);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: could not trim olh log, r=" << r << dendl;
    return r;
  }

  if (need_to_remove) {
    ObjectWriteOperation rm_op;

    rm_op.cmpxattr(RGW_ATTR_OLH_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ, olh_tag);
    rm_op.cmpxattr(RGW_ATTR_OLH_VER, CEPH_OSD_CMPXATTR_OP_GT, last_ver);
    cls_obj_check_prefix_exist(rm_op, RGW_ATTR_OLH_PENDING_PREFIX, true); /* fail if found one of these, pending modification */
    rm_op.remove();

    r = ref.ioctx.operate(ref.obj.oid, &rm_op);
    if (r == -ECANCELED) {
      return 0; /* someone else won this race */
    } else {
      /* 
       * only clear if was successful, otherwise we might clobber pending operations on this object
       */
      r = bucket_index_clear_olh(bucket_info, state, obj);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: could not clear bucket index olh entries r=" << r << dendl;
        return r;
      }
    }
  }

  return 0;
}

/*
 * read olh log and apply it
 */
int RGWRados::update_olh(RGWObjectCtx& obj_ctx, RGWObjState *state, const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_zone_set *zones_trace)
{
  map<uint64_t, vector<rgw_bucket_olh_log_entry> > log;
  bool is_truncated;
  uint64_t ver_marker = 0;

  do {
    int ret = bucket_index_read_olh_log(bucket_info, *state, obj, ver_marker, &log, &is_truncated);
    if (ret < 0) {
      return ret;
    }
    ret = apply_olh_log(obj_ctx, *state, bucket_info, obj, state->olh_tag, log, &ver_marker, zones_trace);
    if (ret < 0) {
      return ret;
    }
  } while (is_truncated);

  return 0;
}

int RGWRados::set_olh(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, const rgw_obj& target_obj, bool delete_marker, rgw_bucket_dir_entry_meta *meta,
                      uint64_t olh_epoch, real_time unmod_since, bool high_precision_time,
                      rgw_zone_set *zones_trace, bool log_data_change)
{
  string op_tag;

  rgw_obj olh_obj = target_obj;
  olh_obj.key.instance.clear();

  RGWObjState *state = NULL;

  int ret = 0;
  int i;
  
#define MAX_ECANCELED_RETRY 100
  for (i = 0; i < MAX_ECANCELED_RETRY; i++) {
    if (ret == -ECANCELED) {
      obj_ctx.invalidate(olh_obj);
    }

    ret = get_obj_state(&obj_ctx, bucket_info, olh_obj, &state, false); /* don't follow olh */
    if (ret < 0) {
      return ret;
    }

    ret = olh_init_modification(bucket_info, *state, olh_obj, &op_tag);
    if (ret < 0) {
      ldout(cct, 20) << "olh_init_modification() target_obj=" << target_obj << " delete_marker=" << (int)delete_marker << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        continue;
      }
      return ret;
    }
    ret = bucket_index_link_olh(bucket_info, *state, target_obj, delete_marker,
                                op_tag, meta, olh_epoch, unmod_since, high_precision_time,
                                zones_trace, log_data_change);
    if (ret < 0) {
      ldout(cct, 20) << "bucket_index_link_olh() target_obj=" << target_obj << " delete_marker=" << (int)delete_marker << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        // the bucket index rejected the link_olh() due to olh tag mismatch;
        // attempt to reconstruct olh head attributes based on the bucket index
        int r2 = repair_olh(state, bucket_info, olh_obj);
        if (r2 < 0 && r2 != -ECANCELED) {
          return r2;
        }
        continue;
      }
      return ret;
    }
    break;
  }

  if (i == MAX_ECANCELED_RETRY) {
    ldout(cct, 0) << "ERROR: exceeded max ECANCELED retries, aborting (EIO)" << dendl;
    return -EIO;
  }

  ret = update_olh(obj_ctx, state, bucket_info, olh_obj);
  if (ret == -ECANCELED) { /* already did what we needed, no need to retry, raced with another user */
    ret = 0;
  }
  if (ret < 0) {
    ldout(cct, 20) << "update_olh() target_obj=" << target_obj << " returned " << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::unlink_obj_instance(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, const rgw_obj& target_obj,
                                  uint64_t olh_epoch, rgw_zone_set *zones_trace)
{
  string op_tag;

  rgw_obj olh_obj = target_obj;
  olh_obj.key.instance.clear();

  RGWObjState *state = NULL;

  int ret = 0;
  int i;

  for (i = 0; i < MAX_ECANCELED_RETRY; i++) {
    if (ret == -ECANCELED) {
      obj_ctx.invalidate(olh_obj);
    }

    ret = get_obj_state(&obj_ctx, bucket_info, olh_obj, &state, false); /* don't follow olh */
    if (ret < 0)
      return ret;

    ret = olh_init_modification(bucket_info, *state, olh_obj, &op_tag);
    if (ret < 0) {
      ldout(cct, 20) << "olh_init_modification() target_obj=" << target_obj << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        continue;
      }
      return ret;
    }

    string olh_tag(state->olh_tag.c_str(), state->olh_tag.length());

    ret = bucket_index_unlink_instance(bucket_info, target_obj, op_tag, olh_tag, olh_epoch, zones_trace);
    if (ret < 0) {
      ldout(cct, 20) << "bucket_index_unlink_instance() target_obj=" << target_obj << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        continue;
      }
      return ret;
    }
    break;
  }

  if (i == MAX_ECANCELED_RETRY) {
    ldout(cct, 0) << "ERROR: exceeded max ECANCELED retries, aborting (EIO)" << dendl;
    return -EIO;
  }

  ret = update_olh(obj_ctx, state, bucket_info, olh_obj, zones_trace);
  if (ret == -ECANCELED) { /* already did what we needed, no need to retry, raced with another user */
    return 0;
  }
  if (ret < 0) {
    ldout(cct, 20) << "update_olh() target_obj=" << target_obj << " returned " << ret << dendl;
    return ret;
  }

  return 0;
}

void RGWRados::gen_rand_obj_instance_name(rgw_obj_key *target_key)
{
#define OBJ_INSTANCE_LEN 32
  char buf[OBJ_INSTANCE_LEN + 1];

  gen_rand_alphanumeric_no_underscore(cct, buf, OBJ_INSTANCE_LEN); /* don't want it to get url escaped,
                                                                      no underscore for instance name due to the way we encode the raw keys */

  target_key->set_instance(buf);
}

void RGWRados::gen_rand_obj_instance_name(rgw_obj *target_obj)
{
  gen_rand_obj_instance_name(&target_obj->key);
}

int RGWRados::get_olh(const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWOLHInfo *olh)
{
  map<string, bufferlist> unfiltered_attrset;

  ObjectReadOperation op;
  op.getxattrs(&unfiltered_attrset, NULL);

  bufferlist outbl;
  int r = obj_operate(bucket_info, obj, &op);

  if (r < 0) {
    return r;
  }
  map<string, bufferlist> attrset;

  rgw_filter_attrset(unfiltered_attrset, RGW_ATTR_OLH_PREFIX, &attrset);

  map<string, bufferlist>::iterator iter = attrset.find(RGW_ATTR_OLH_INFO);
  if (iter == attrset.end()) { /* not an olh */
    return -EINVAL;
  }

  try {
    auto biter = iter->second.cbegin();
    decode(*olh, biter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode olh info" << dendl;
    return -EIO;
  }

  return 0;
}

void RGWRados::check_pending_olh_entries(map<string, bufferlist>& pending_entries, 
                                         map<string, bufferlist> *rm_pending_entries)
{
  map<string, bufferlist>::iterator iter = pending_entries.begin();

  real_time now = real_clock::now();

  while (iter != pending_entries.end()) {
    auto biter = iter->second.cbegin();
    RGWOLHPendingInfo pending_info;
    try {
      decode(pending_info, biter);
    } catch (buffer::error& err) {
      /* skipping bad entry, we could remove it but it might hide a bug */
      ldout(cct, 0) << "ERROR: failed to decode pending entry " << iter->first << dendl;
      ++iter;
      continue;
    }

    map<string, bufferlist>::iterator cur_iter = iter;
    ++iter;
    if (now - pending_info.time >= make_timespan(cct->_conf->rgw_olh_pending_timeout_sec)) {
      (*rm_pending_entries)[cur_iter->first] = cur_iter->second;
      pending_entries.erase(cur_iter);
    } else {
      /* entries names are sorted by time (rounded to a second) */
      break;
    }
  }
}

int RGWRados::remove_olh_pending_entries(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, map<string, bufferlist>& pending_attrs)
{
  ObjectWriteOperation op;

  bucket_index_guard_olh_op(state, op);

  for (map<string, bufferlist>::iterator iter = pending_attrs.begin(); iter != pending_attrs.end(); ++iter) {
    op.rmxattr(iter->first.c_str());
  }

  rgw_rados_ref ref;
  int r = get_obj_head_ref(bucket_info, olh_obj, &ref);
  if (r < 0) {
    return r;
  }

  /* update olh object */
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (r == -ENOENT || r == -ECANCELED) {
    /* raced with some other change, shouldn't sweat about it */
    r = 0;
  }
  if (r < 0) {
    ldout(cct, 0) << "ERROR: could not apply olh update, r=" << r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::follow_olh(const RGWBucketInfo& bucket_info, RGWObjectCtx& obj_ctx, RGWObjState *state, const rgw_obj& olh_obj, rgw_obj *target)
{
  map<string, bufferlist> pending_entries;
  rgw_filter_attrset(state->attrset, RGW_ATTR_OLH_PENDING_PREFIX, &pending_entries);

  map<string, bufferlist> rm_pending_entries;
  check_pending_olh_entries(pending_entries, &rm_pending_entries);

  if (!rm_pending_entries.empty()) {
    int ret = remove_olh_pending_entries(bucket_info, *state, olh_obj, rm_pending_entries);
    if (ret < 0) {
      ldout(cct, 20) << "ERROR: rm_pending_entries returned ret=" << ret << dendl;
      return ret;
    }
  }
  if (!pending_entries.empty()) {
    ldout(cct, 20) << __func__ << "(): found pending entries, need to update_olh() on bucket=" << olh_obj.bucket << dendl;

    int ret = update_olh(obj_ctx, state, bucket_info, olh_obj);
    if (ret < 0) {
      return ret;
    }
  }

  map<string, bufferlist>::iterator iter = state->attrset.find(RGW_ATTR_OLH_INFO);
  ceph_assert(iter != state->attrset.end());
  RGWOLHInfo olh;
  try {
    auto biter = iter->second.cbegin();
    decode(olh, biter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode olh info" << dendl;
    return -EIO;
  }

  if (olh.removed) {
    return -ENOENT;
  }

  *target = olh.target;

  return 0;
}

int RGWRados::raw_obj_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
                           map<string, bufferlist> *attrs, bufferlist *first_chunk,
                           RGWObjVersionTracker *objv_tracker)
{
  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  map<string, bufferlist> unfiltered_attrset;
  uint64_t size = 0;
  struct timespec mtime_ts;

  ObjectReadOperation op;
  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(&op);
  }
  if (attrs) {
    op.getxattrs(&unfiltered_attrset, NULL);
  }
  if (psize || pmtime) {
    op.stat2(&size, &mtime_ts, NULL);
  }
  if (first_chunk) {
    op.read(0, cct->_conf->rgw_max_chunk_size, first_chunk, NULL);
  }
  bufferlist outbl;
  r = ref.ioctx.operate(ref.obj.oid, &op, &outbl);

  if (epoch) {
    *epoch = ref.ioctx.get_last_version();
  }

  if (r < 0)
    return r;

  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = ceph::real_clock::from_timespec(mtime_ts);
  if (attrs) {
    rgw_filter_attrset(unfiltered_attrset, RGW_ATTR_PREFIX, attrs);
  }

  return 0;
}

int RGWRados::get_bucket_stats(RGWBucketInfo& bucket_info, int shard_id, string *bucket_ver, string *master_ver,
    map<RGWObjCategory, RGWStorageStats>& stats, string *max_marker, bool *syncstopped)
{
  vector<rgw_bucket_dir_header> headers;
  map<int, string> bucket_instance_ids;
  int r = cls_bucket_head(bucket_info, shard_id, headers, &bucket_instance_ids);
  if (r < 0) {
    return r;
  }

  ceph_assert(headers.size() == bucket_instance_ids.size());

  auto iter = headers.begin();
  map<int, string>::iterator viter = bucket_instance_ids.begin();
  BucketIndexShardsManager ver_mgr;
  BucketIndexShardsManager master_ver_mgr;
  BucketIndexShardsManager marker_mgr;
  char buf[64];
  for(; iter != headers.end(); ++iter, ++viter) {
    accumulate_raw_stats(*iter, stats);
    snprintf(buf, sizeof(buf), "%lu", (unsigned long)iter->ver);
    ver_mgr.add(viter->first, string(buf));
    snprintf(buf, sizeof(buf), "%lu", (unsigned long)iter->master_ver);
    master_ver_mgr.add(viter->first, string(buf));
    if (shard_id >= 0) {
      *max_marker = iter->max_marker;
    } else {
      marker_mgr.add(viter->first, iter->max_marker);
    }
    if (syncstopped != NULL)
      *syncstopped = iter->syncstopped;
  }
  ver_mgr.to_string(bucket_ver);
  master_ver_mgr.to_string(master_ver);
  if (shard_id < 0) {
    marker_mgr.to_string(max_marker);
  }
  return 0;
}

int RGWRados::get_bi_log_status(RGWBucketInfo& bucket_info, int shard_id,
    map<int, string>& markers)
{
  vector<rgw_bucket_dir_header> headers;
  map<int, string> bucket_instance_ids;
  int r = cls_bucket_head(bucket_info, shard_id, headers, &bucket_instance_ids);
  if (r < 0)
    return r;

  ceph_assert(headers.size() == bucket_instance_ids.size());

  auto iter = headers.begin();
  map<int, string>::iterator viter = bucket_instance_ids.begin();

  for(; iter != headers.end(); ++iter, ++viter) {
    if (shard_id >= 0) {
      markers[shard_id] = iter->max_marker;
    } else {
      markers[viter->first] = iter->max_marker;
    }
  }
  return 0;
}

class RGWGetBucketStatsContext : public RGWGetDirHeader_CB {
  RGWGetBucketStats_CB *cb;
  uint32_t pendings;
  map<RGWObjCategory, RGWStorageStats> stats;
  int ret_code;
  bool should_cb;
  Mutex lock;

public:
  RGWGetBucketStatsContext(RGWGetBucketStats_CB *_cb, uint32_t _pendings)
    : cb(_cb), pendings(_pendings), stats(), ret_code(0), should_cb(true),
    lock("RGWGetBucketStatsContext") {}

  void handle_response(int r, rgw_bucket_dir_header& header) override {
    Mutex::Locker l(lock);
    if (should_cb) {
      if ( r >= 0) {
        accumulate_raw_stats(header, stats);
      } else {
        ret_code = r;
      }

      // Are we all done?
      if (--pendings == 0) {
        if (!ret_code) {
          cb->set_response(&stats);
        }
        cb->handle_response(ret_code);
        cb->put();
      }
    }
  }

  void unset_cb() {
    Mutex::Locker l(lock);
    should_cb = false;
  }
};

int RGWRados::get_bucket_stats_async(RGWBucketInfo& bucket_info, int shard_id, RGWGetBucketStats_CB *ctx)
{
  int num_aio = 0;
  RGWGetBucketStatsContext *get_ctx = new RGWGetBucketStatsContext(ctx, bucket_info.num_shards ? : 1);
  ceph_assert(get_ctx);
  int r = cls_bucket_head_async(bucket_info, shard_id, get_ctx, &num_aio);
  if (r < 0) {
    ctx->put();
    if (num_aio) {
      get_ctx->unset_cb();
    }
  }
  get_ctx->put();
  return r;
}

class RGWGetUserStatsContext : public RGWGetUserHeader_CB {
  RGWGetUserStats_CB *cb;

public:
  explicit RGWGetUserStatsContext(RGWGetUserStats_CB * const cb)
    : cb(cb) {}

  void handle_response(int r, cls_user_header& header) override {
    const cls_user_stats& hs = header.stats;
    if (r >= 0) {
      RGWStorageStats stats;

      stats.size = hs.total_bytes;
      stats.size_rounded = hs.total_bytes_rounded;
      stats.num_objects = hs.total_entries;

      cb->set_response(stats);
    }

    cb->handle_response(r);

    cb->put();
  }
};

int RGWRados::get_user_stats(const rgw_user& user, RGWStorageStats& stats)
{
  string user_str = user.to_str();

  cls_user_header header;
  int r = cls_user_get_header(user_str, &header);
  if (r < 0)
    return r;

  const cls_user_stats& hs = header.stats;

  stats.size = hs.total_bytes;
  stats.size_rounded = hs.total_bytes_rounded;
  stats.num_objects = hs.total_entries;

  return 0;
}

int RGWRados::get_user_stats_async(const rgw_user& user, RGWGetUserStats_CB *ctx)
{
  string user_str = user.to_str();

  RGWGetUserStatsContext *get_ctx = new RGWGetUserStatsContext(ctx);
  int r = cls_user_get_header_async(user_str, get_ctx);
  if (r < 0) {
    ctx->put();
    delete get_ctx;
    return r;
  }

  return 0;
}

void RGWRados::get_bucket_meta_oid(const rgw_bucket& bucket, string& oid)
{
  oid = RGW_BUCKET_INSTANCE_MD_PREFIX + bucket.get_key(':');
}

void RGWRados::get_bucket_instance_obj(const rgw_bucket& bucket, rgw_raw_obj& obj)
{
  if (!bucket.oid.empty()) {
    obj.init(svc.zone->get_zone_params().domain_root, bucket.oid);
  } else {
    string oid;
    get_bucket_meta_oid(bucket, oid);
    obj.init(svc.zone->get_zone_params().domain_root, oid);
  }
}

int RGWRados::get_bucket_instance_info(RGWSysObjectCtx& obj_ctx, const string& meta_key, RGWBucketInfo& info,
                                       real_time *pmtime, map<string, bufferlist> *pattrs)
{
  size_t pos = meta_key.find(':');
  if (pos == string::npos) {
    return -EINVAL;
  }
  string oid = RGW_BUCKET_INSTANCE_MD_PREFIX + meta_key;
  rgw_bucket_instance_key_to_oid(oid);

  return get_bucket_instance_from_oid(obj_ctx, oid, info, pmtime, pattrs);
}

int RGWRados::get_bucket_instance_info(RGWSysObjectCtx& obj_ctx, const rgw_bucket& bucket, RGWBucketInfo& info,
                                       real_time *pmtime, map<string, bufferlist> *pattrs)
{
  string oid;
  if (bucket.oid.empty()) {
    get_bucket_meta_oid(bucket, oid);
  } else {
    oid = bucket.oid;
  }

  return get_bucket_instance_from_oid(obj_ctx, oid, info, pmtime, pattrs);
}

int RGWRados::get_bucket_instance_from_oid(RGWSysObjectCtx& obj_ctx, const string& oid, RGWBucketInfo& info,
                                           real_time *pmtime, map<string, bufferlist> *pattrs,
                                           rgw_cache_entry_info *cache_info,
					   boost::optional<obj_version> refresh_version)
{
  auto& domain_root = svc.zone->get_zone_params().domain_root;

  ldout(cct, 20) << "reading from " << domain_root << ":" << oid << dendl;

  bufferlist epbl;

  int ret = rgw_get_system_obj(this, obj_ctx, domain_root,
			       oid, epbl, &info.objv_tracker, pmtime, pattrs,
			       cache_info, refresh_version);
  if (ret < 0) {
    return ret;
  }

  auto iter = epbl.cbegin();
  try {
    decode(info, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  info.bucket.oid = oid;
  return 0;
}

int RGWRados::get_bucket_entrypoint_info(RGWSysObjectCtx& obj_ctx,
                                         const string& tenant_name,
                                         const string& bucket_name,
                                         RGWBucketEntryPoint& entry_point,
                                         RGWObjVersionTracker *objv_tracker,
                                         real_time *pmtime,
                                         map<string, bufferlist> *pattrs,
                                         rgw_cache_entry_info *cache_info,
					 boost::optional<obj_version> refresh_version)
{
  bufferlist bl;
  string bucket_entry;

  rgw_make_bucket_entry_name(tenant_name, bucket_name, bucket_entry);
  int ret = rgw_get_system_obj(this, obj_ctx, svc.zone->get_zone_params().domain_root,
			       bucket_entry, bl, objv_tracker, pmtime, pattrs,
			       cache_info, refresh_version);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(entry_point, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  return 0;
}

int RGWRados::convert_old_bucket_info(RGWSysObjectCtx& obj_ctx,
                                      const string& tenant_name,
                                      const string& bucket_name)
{
  RGWBucketEntryPoint entry_point;
  real_time ep_mtime;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  RGWBucketInfo info;

  ldout(cct, 10) << "RGWRados::convert_old_bucket_info(): bucket=" << bucket_name << dendl;

  int ret = get_bucket_entrypoint_info(obj_ctx, tenant_name, bucket_name, entry_point, &ot, &ep_mtime, &attrs);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: get_bucket_entrypoint_info() returned " << ret << " bucket=" << bucket_name << dendl;
    return ret;
  }

  if (!entry_point.has_bucket_info) {
    /* already converted! */
    return 0;
  }

  info = entry_point.old_bucket_info;
  info.bucket.oid = bucket_name;
  info.ep_objv = ot.read_version;

  ot.generate_new_write_ver(cct);

  ret = put_linked_bucket_info(info, false, ep_mtime, &ot.write_version, &attrs, true);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to put_linked_bucket_info(): " << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::_get_bucket_info(RGWSysObjectCtx& obj_ctx,
                               const string& tenant,
                               const string& bucket_name,
                               RGWBucketInfo& info,
                               real_time *pmtime,
                               map<string, bufferlist> *pattrs,
                               boost::optional<obj_version> refresh_version)
{
  string bucket_entry;
  rgw_make_bucket_entry_name(tenant, bucket_name, bucket_entry);


  if (auto e = binfo_cache->find(bucket_entry)) {
    if (refresh_version &&
        e->info.objv_tracker.read_version.compare(&(*refresh_version))) {
      lderr(cct) << "WARNING: The bucket info cache is inconsistent. This is "
                 << "a failure that should be debugged. I am a nice machine, "
                 << "so I will try to recover." << dendl;
      binfo_cache->invalidate(bucket_entry);
    } else {
      info = e->info;
      if (pattrs)
	*pattrs = e->attrs;
      if (pmtime)
	*pmtime = e->mtime;
      return 0;
    }
  }

  bucket_info_entry e;
  RGWBucketEntryPoint entry_point;
  real_time ep_mtime;
  RGWObjVersionTracker ot;
  rgw_cache_entry_info entry_cache_info;
  int ret = get_bucket_entrypoint_info(obj_ctx, tenant, bucket_name,
				       entry_point, &ot, &ep_mtime, pattrs,
				       &entry_cache_info, refresh_version);
  if (ret < 0) {
    /* only init these fields */
    info.bucket.tenant = tenant;
    info.bucket.name = bucket_name;
    return ret;
  }

  if (entry_point.has_bucket_info) {
    info = entry_point.old_bucket_info;
    info.bucket.oid = bucket_name;
    info.bucket.tenant = tenant;
    info.ep_objv = ot.read_version;
    ldout(cct, 20) << "rgw_get_bucket_info: old bucket info, bucket=" << info.bucket << " owner " << info.owner << dendl;
    return 0;
  }

  /* data is in the bucket instance object, we need to get attributes from there, clear everything
   * that we got
   */
  if (pattrs) {
    pattrs->clear();
  }

  ldout(cct, 20) << "rgw_get_bucket_info: bucket instance: " << entry_point.bucket << dendl;


  /* read bucket instance info */

  string oid;
  get_bucket_meta_oid(entry_point.bucket, oid);

  rgw_cache_entry_info cache_info;

  ret = get_bucket_instance_from_oid(obj_ctx, oid, e.info, &e.mtime, &e.attrs,
				     &cache_info, refresh_version);
  e.info.ep_objv = ot.read_version;
  info = e.info;
  if (ret < 0) {
    lderr(cct) << "ERROR: get_bucket_instance_from_oid failed: " << ret << dendl;
    info.bucket.tenant = tenant;
    info.bucket.name = bucket_name;
    // XXX and why return anything in case of an error anyway?
    return ret;
  }

  if (pmtime)
    *pmtime = e.mtime;
  if (pattrs)
    *pattrs = e.attrs;

  /* chain to both bucket entry point and bucket instance */
  if (!binfo_cache->put(svc.cache, bucket_entry, &e, {&entry_cache_info, &cache_info})) {
    ldout(cct, 20) << "couldn't put binfo cache entry, might have raced with data changes" << dendl;
  }

  if (refresh_version &&
      refresh_version->compare(&info.objv_tracker.read_version)) {
    lderr(cct) << "WARNING: The OSD has the same version I have. Something may "
               << "have gone squirrelly. An administrator may have forced a "
               << "change; otherwise there is a problem somewhere." << dendl;
  }

  return 0;
}

int RGWRados::get_bucket_info(RGWSysObjectCtx& obj_ctx,
                              const string& tenant, const string& bucket_name,
                              RGWBucketInfo& info,
                              real_time *pmtime, map<string, bufferlist> *pattrs)
{
  return _get_bucket_info(obj_ctx, tenant, bucket_name, info, pmtime,
                          pattrs, boost::none);
}

int RGWRados::try_refresh_bucket_info(RGWBucketInfo& info,
                                      ceph::real_time *pmtime,
                                      map<string, bufferlist> *pattrs)
{
  RGWSysObjectCtx obj_ctx = svc.sysobj->init_obj_ctx();

  return _get_bucket_info(obj_ctx, info.bucket.tenant, info.bucket.name,
                          info, pmtime, pattrs, info.objv_tracker.read_version);
}

int RGWRados::put_bucket_entrypoint_info(const string& tenant_name, const string& bucket_name, RGWBucketEntryPoint& entry_point,
                                         bool exclusive, RGWObjVersionTracker& objv_tracker, real_time mtime,
                                         map<string, bufferlist> *pattrs)
{
  bufferlist epbl;
  encode(entry_point, epbl);
  string bucket_entry;
  rgw_make_bucket_entry_name(tenant_name, bucket_name, bucket_entry);
  return rgw_bucket_store_info(this, bucket_entry, epbl, exclusive, pattrs, &objv_tracker, mtime);
}

int RGWRados::put_bucket_instance_info(RGWBucketInfo& info, bool exclusive,
                              real_time mtime, map<string, bufferlist> *pattrs)
{
  info.has_instance_obj = true;
  bufferlist bl;

  encode(info, bl);

  string key = info.bucket.get_key(); /* when we go through meta api, we don't use oid directly */
  int ret = rgw_bucket_instance_store_info(this, key, bl, exclusive, pattrs, &info.objv_tracker, mtime);
  if (ret == -EEXIST) {
    /* well, if it's exclusive we shouldn't overwrite it, because we might race with another
     * bucket operation on this specific bucket (e.g., being synced from the master), but
     * since bucket instace meta object is unique for this specific bucket instace, we don't
     * need to return an error.
     * A scenario where we'd get -EEXIST here, is in a multi-zone config, we're not on the
     * master, creating a bucket, sending bucket creation to the master, we create the bucket
     * locally, while in the sync thread we sync the new bucket.
     */
    ret = 0;
  }
  return ret;
}

int RGWRados::put_linked_bucket_info(RGWBucketInfo& info, bool exclusive, real_time mtime, obj_version *pep_objv,
                                     map<string, bufferlist> *pattrs, bool create_entry_point)
{
  bool create_head = !info.has_instance_obj || create_entry_point;

  int ret = put_bucket_instance_info(info, exclusive, mtime, pattrs);
  if (ret < 0) {
    return ret;
  }

  if (!create_head)
    return 0; /* done! */

  RGWBucketEntryPoint entry_point;
  entry_point.bucket = info.bucket;
  entry_point.owner = info.owner;
  entry_point.creation_time = info.creation_time;
  entry_point.linked = true;
  RGWObjVersionTracker ot;
  if (pep_objv && !pep_objv->tag.empty()) {
    ot.write_version = *pep_objv;
  } else {
    ot.generate_new_write_ver(cct);
    if (pep_objv) {
      *pep_objv = ot.write_version;
    }
  }
  ret = put_bucket_entrypoint_info(info.bucket.tenant, info.bucket.name, entry_point, exclusive, ot, mtime, NULL); 
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::update_containers_stats(map<string, RGWBucketEnt>& m)
{
  auto obj_ctx = svc.sysobj->init_obj_ctx();

  map<string, RGWBucketEnt>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt& ent = iter->second;
    rgw_bucket& bucket = ent.bucket;
    ent.count = 0;
    ent.size = 0;
    ent.size_rounded = 0;

    vector<rgw_bucket_dir_header> headers;

    RGWBucketInfo bucket_info;
    int ret = get_bucket_instance_info(obj_ctx, bucket, bucket_info, NULL, NULL);
    if (ret < 0) {
      return ret;
    }

    int r = cls_bucket_head(bucket_info, RGW_NO_SHARD, headers);
    if (r < 0)
      return r;

    auto hiter = headers.begin();
    for (; hiter != headers.end(); ++hiter) {
      RGWObjCategory category = main_category;
      auto iter = (hiter->stats).find(category);
      if (iter != hiter->stats.end()) {
        struct rgw_bucket_category_stats& stats = iter->second;
        ent.count += stats.num_entries;
        ent.size += stats.total_size;
        ent.size_rounded += stats.total_size_rounded;
      }
    }

    // fill in placement_rule from the bucket instance for use in swift's
    // per-storage policy statistics
    ent.placement_rule = std::move(bucket_info.placement_rule);
  }

  return m.size();
}

int RGWRados::append_async(rgw_raw_obj& obj, size_t size, bufferlist& bl)
{
  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }
  librados::Rados *rad = get_rados_handle();
  librados::AioCompletion *completion = rad->aio_create_completion(NULL, NULL, NULL);

  r = ref.ioctx.aio_append(ref.obj.oid, completion, bl, size);
  completion->release();
  return r;
}

int RGWRados::pool_iterate_begin(const rgw_pool& pool, RGWPoolIterCtx& ctx)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::NObjectIterator& iter = ctx.iter;

  int r = open_pool_ctx(pool, io_ctx);
  if (r < 0)
    return r;

  iter = io_ctx.nobjects_begin();

  return 0;
}

int RGWRados::pool_iterate_begin(const rgw_pool& pool, const string& cursor, RGWPoolIterCtx& ctx)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::NObjectIterator& iter = ctx.iter;

  int r = open_pool_ctx(pool, io_ctx);
  if (r < 0)
    return r;

  librados::ObjectCursor oc;
  if (!oc.from_str(cursor)) {
    ldout(cct, 10) << "failed to parse cursor: " << cursor << dendl;
    return -EINVAL;
  }

  try {
    iter = io_ctx.nobjects_begin(oc);
    return 0;
  } catch (const std::system_error& e) {
    r = -e.code().value();
    ldout(cct, 10) << "nobjects_begin threw " << e.what()
       << ", returning " << r << dendl;
    return r;
  } catch (const std::exception& e) {
    ldout(cct, 10) << "nobjects_begin threw " << e.what()
       << ", returning -5" << dendl;
    return -EIO;
  }
}

string RGWRados::pool_iterate_get_cursor(RGWPoolIterCtx& ctx)
{
  return ctx.iter.get_cursor().to_str();
}

static int do_pool_iterate(CephContext* cct, RGWPoolIterCtx& ctx, uint32_t num,
                           vector<rgw_bucket_dir_entry>& objs,
                           bool *is_truncated, RGWAccessListFilter *filter)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::NObjectIterator& iter = ctx.iter;

  if (iter == io_ctx.nobjects_end())
    return -ENOENT;

  uint32_t i;

  for (i = 0; i < num && iter != io_ctx.nobjects_end(); ++i, ++iter) {
    rgw_bucket_dir_entry e;

    string oid = iter->get_oid();
    ldout(cct, 20) << "RGWRados::pool_iterate: got " << oid << dendl;

    // fill it in with initial values; we may correct later
    if (filter && !filter->filter(oid, oid))
      continue;

    e.key = oid;
    objs.push_back(e);
  }

  if (is_truncated)
    *is_truncated = (iter != io_ctx.nobjects_end());

  return objs.size();
}

int RGWRados::pool_iterate(RGWPoolIterCtx& ctx, uint32_t num, vector<rgw_bucket_dir_entry>& objs,
                           bool *is_truncated, RGWAccessListFilter *filter)
{
  // catch exceptions from NObjectIterator::operator++()
  try {
    return do_pool_iterate(cct, ctx, num, objs, is_truncated, filter);
  } catch (const std::system_error& e) {
    int r = -e.code().value();
    ldout(cct, 10) << "NObjectIterator threw exception " << e.what()
       << ", returning " << r << dendl;
    return r;
  } catch (const std::exception& e) {
    ldout(cct, 10) << "NObjectIterator threw exception " << e.what()
       << ", returning -5" << dendl;
    return -EIO;
  }
}

int RGWRados::list_raw_objects_init(const rgw_pool& pool, const string& marker, RGWListRawObjsCtx *ctx)
{
  if (!ctx->initialized) {
    int r = pool_iterate_begin(pool, marker, ctx->iter_ctx);
    if (r < 0) {
      ldout(cct, 10) << "failed to list objects pool_iterate_begin() returned r=" << r << dendl;
      return r;
    }
    ctx->initialized = true;
  }
  return 0;
}

int RGWRados::list_raw_objects_next(const string& prefix_filter, int max,
                                    RGWListRawObjsCtx& ctx, list<string>& oids,
                                    bool *is_truncated)
{
  if (!ctx.initialized) {
    return -EINVAL;
  }
  RGWAccessListFilterPrefix filter(prefix_filter);
  vector<rgw_bucket_dir_entry> objs;
  int r = pool_iterate(ctx.iter_ctx, max, objs, is_truncated, &filter);
  if (r < 0) {
    if(r != -ENOENT)
      ldout(cct, 10) << "failed to list objects pool_iterate returned r=" << r << dendl;
    return r;
  }

  vector<rgw_bucket_dir_entry>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    oids.push_back(iter->key.name);
  }

  return oids.size();
}

int RGWRados::list_raw_objects(const rgw_pool& pool, const string& prefix_filter,
			       int max, RGWListRawObjsCtx& ctx, list<string>& oids,
			       bool *is_truncated)
{
  if (!ctx.initialized) {
    int r = list_raw_objects_init(pool, string(), &ctx);
    if (r < 0) {
      return r;
    }
  }

  return list_raw_objects_next(prefix_filter, max, ctx, oids, is_truncated);
}

string RGWRados::list_raw_objs_get_cursor(RGWListRawObjsCtx& ctx)
{
  return pool_iterate_get_cursor(ctx.iter_ctx);
}

int RGWRados::list_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id, string& marker, uint32_t max,
                                  std::list<rgw_bi_log_entry>& result, bool *truncated)
{
  ldout(cct, 20) << __func__ << ": " << bucket_info.bucket << " marker " << marker << " shard_id=" << shard_id << " max " << max << dendl;
  result.clear();

  librados::IoCtx index_ctx;
  map<int, string> oids;
  map<int, cls_rgw_bi_log_list_ret> bi_log_lists;
  int r = open_bucket_index(bucket_info, index_ctx, oids, shard_id);
  if (r < 0)
    return r;

  BucketIndexShardsManager marker_mgr;
  bool has_shards = (oids.size() > 1 || shard_id >= 0);
  // If there are multiple shards for the bucket index object, the marker
  // should have the pattern '{shard_id_1}#{shard_marker_1},{shard_id_2}#
  // {shard_marker_2}...', if there is no sharding, the bi_log_list should
  // only contain one record, and the key is the bucket instance id.
  r = marker_mgr.from_string(marker, shard_id);
  if (r < 0)
    return r;
 
  r = CLSRGWIssueBILogList(index_ctx, marker_mgr, max, oids, bi_log_lists, cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  map<int, list<rgw_bi_log_entry>::iterator> vcurrents;
  map<int, list<rgw_bi_log_entry>::iterator> vends;
  if (truncated) {
    *truncated = false;
  }
  map<int, cls_rgw_bi_log_list_ret>::iterator miter = bi_log_lists.begin();
  for (; miter != bi_log_lists.end(); ++miter) {
    int shard_id = miter->first;
    vcurrents[shard_id] = miter->second.entries.begin();
    vends[shard_id] = miter->second.entries.end();
    if (truncated) {
      *truncated = (*truncated || miter->second.truncated);
    }
  }

  size_t total = 0;
  bool has_more = true;
  map<int, list<rgw_bi_log_entry>::iterator>::iterator viter;
  map<int, list<rgw_bi_log_entry>::iterator>::iterator eiter;
  while (total < max && has_more) {
    has_more = false;

    viter = vcurrents.begin();
    eiter = vends.begin();

    for (; total < max && viter != vcurrents.end(); ++viter, ++eiter) {
      assert (eiter != vends.end());

      int shard_id = viter->first;
      list<rgw_bi_log_entry>::iterator& liter = viter->second;

      if (liter == eiter->second){
        continue;
      }
      rgw_bi_log_entry& entry = *(liter);
      if (has_shards) {
        char buf[16];
        snprintf(buf, sizeof(buf), "%d", shard_id);
        string tmp_id;
        build_bucket_index_marker(buf, entry.id, &tmp_id);
        entry.id.swap(tmp_id);
      }
      marker_mgr.add(shard_id, entry.id);
      result.push_back(entry);
      total++;
      has_more = true;
      ++liter;
    }
  }

  if (truncated) {
    for (viter = vcurrents.begin(), eiter = vends.begin(); viter != vcurrents.end(); ++viter, ++eiter) {
      assert (eiter != vends.end());
      *truncated = (*truncated || (viter->second != eiter->second));
    }
  }

  // Refresh marker, if there are multiple shards, the output will look like
  // '{shard_oid_1}#{shard_marker_1},{shard_oid_2}#{shard_marker_2}...',
  // if there is no sharding, the simply marker (without oid) is returned
  if (has_shards) {
    marker_mgr.to_string(&marker);
  } else {
    if (!result.empty()) {
      marker = result.rbegin()->id;
    }
  }

  return 0;
}

int RGWRados::trim_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id, string& start_marker, string& end_marker)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;

  BucketIndexShardsManager start_marker_mgr;
  BucketIndexShardsManager end_marker_mgr;

  int r = open_bucket_index(bucket_info, index_ctx, bucket_objs, shard_id);
  if (r < 0) {
    return r;
  }

  r = start_marker_mgr.from_string(start_marker, shard_id);
  if (r < 0) {
    return r;
  }

  r = end_marker_mgr.from_string(end_marker, shard_id);
  if (r < 0) {
    return r;
  }

  return CLSRGWIssueBILogTrim(index_ctx, start_marker_mgr, end_marker_mgr, bucket_objs,
			      cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWRados::resync_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;
  int r = open_bucket_index(bucket_info, index_ctx, bucket_objs, shard_id);
  if (r < 0)
    return r;

  return CLSRGWIssueResyncBucketBILog(index_ctx, bucket_objs, cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWRados::stop_bi_log_entries(RGWBucketInfo& bucket_info, int shard_id)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;
  int r = open_bucket_index(bucket_info, index_ctx, bucket_objs, shard_id);
  if (r < 0)
    return r;

  return CLSRGWIssueBucketBILogStop(index_ctx, bucket_objs, cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWRados::bi_get_instance(const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                              rgw_bucket_dir_entry *dirent)
{
  rgw_cls_bi_entry bi_entry;
  int r = bi_get(bucket_info, obj, BIIndexType::Instance, &bi_entry);
  if (r < 0 && r != -ENOENT) {
    ldout(cct, 0) << "ERROR: bi_get() returned r=" << r << dendl;
  }
  if (r < 0) {
    return r;
  }
  auto iter = bi_entry.data.cbegin();
  try {
    decode(*dirent, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode bi_entry()" << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRados::bi_get_olh(const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                         rgw_bucket_olh_entry *olh)
{
  rgw_cls_bi_entry bi_entry;
  int r = bi_get(bucket_info, obj, BIIndexType::OLH, &bi_entry);
  if (r < 0 && r != -ENOENT) {
    ldout(cct, 0) << "ERROR: bi_get() returned r=" << r << dendl;
  }
  if (r < 0) {
    return r;
  }
  auto iter = bi_entry.data.cbegin();
  try {
    decode(*olh, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode bi_entry()" << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRados::bi_get(const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                     BIIndexType index_type, rgw_cls_bi_entry *entry)
{
  BucketShard bs(this);
  int ret = bs.init(bucket_info, obj);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  cls_rgw_obj_key key(obj.key.get_index_key_name(), obj.key.instance);
  
  return cls_rgw_bi_get(bs.index_ctx, bs.bucket_obj, index_type, key, entry);
}

void RGWRados::bi_put(ObjectWriteOperation& op, BucketShard& bs, rgw_cls_bi_entry& entry)
{
  cls_rgw_bi_put(op, bs.bucket_obj, entry);
}

int RGWRados::bi_put(BucketShard& bs, rgw_cls_bi_entry& entry)
{
  int ret = cls_rgw_bi_put(bs.index_ctx, bs.bucket_obj, entry);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::bi_put(rgw_bucket& bucket, rgw_obj& obj, rgw_cls_bi_entry& entry)
{
  BucketShard bs(this);
  int ret = bs.init(bucket, obj, nullptr /* no RGWBucketInfo */);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  return bi_put(bs, entry);
}

int RGWRados::bi_list(rgw_bucket& bucket, const string& obj_name, const string& marker, uint32_t max, list<rgw_cls_bi_entry> *entries, bool *is_truncated)
{
  rgw_obj obj(bucket, obj_name);
  BucketShard bs(this);
  int ret = bs.init(bucket, obj, nullptr /* no RGWBucketInfo */);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  ret = cls_rgw_bi_list(bs.index_ctx, bs.bucket_obj, obj_name, marker, max, entries, is_truncated);
  if (ret == -ENOENT) {
    *is_truncated = false;
  }
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::bi_list(BucketShard& bs, const string& filter_obj, const string& marker, uint32_t max, list<rgw_cls_bi_entry> *entries, bool *is_truncated)
{
  int ret = cls_rgw_bi_list(bs.index_ctx, bs.bucket_obj, filter_obj, marker, max, entries, is_truncated);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::bi_remove(BucketShard& bs)
{
  int ret = bs.index_ctx.remove(bs.bucket_obj);
  if (ret == -ENOENT) {
    ret = 0;
  }
  if (ret < 0) {
    ldout(cct, 5) << "bs.index_ctx.remove(" << bs.bucket_obj << ") returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::bi_list(rgw_bucket& bucket, int shard_id, const string& filter_obj, const string& marker, uint32_t max, list<rgw_cls_bi_entry> *entries, bool *is_truncated)
{
  BucketShard bs(this);
  int ret = bs.init(bucket, shard_id, nullptr /* no RGWBucketInfo */);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  return bi_list(bs, filter_obj, marker, max, entries, is_truncated);
}

int RGWRados::gc_operate(string& oid, librados::ObjectWriteOperation *op)
{
  return gc_pool_ctx.operate(oid, op);
}

int RGWRados::gc_aio_operate(string& oid, librados::ObjectWriteOperation *op, AioCompletion **pc)
{
  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  int r = gc_pool_ctx.aio_operate(oid, c, op);
  if (!pc) {
    c->release();
  } else {
    *pc = c;
  }
  return r;
}

int RGWRados::gc_operate(string& oid, librados::ObjectReadOperation *op, bufferlist *pbl)
{
  return gc_pool_ctx.operate(oid, op, pbl);
}

int RGWRados::list_gc_objs(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated)
{
  return gc->list(index, marker, max, expired_only, result, truncated);
}

int RGWRados::process_gc(bool expired_only)
{
  return gc->process(expired_only);
}

int RGWRados::list_lc_progress(const string& marker, uint32_t max_entries, map<string, int> *progress_map)
{
  return lc->list_lc_progress(marker, max_entries, progress_map);
}

int RGWRados::process_lc()
{
  return lc->process();
}

bool RGWRados::process_expire_objects()
{
  return obj_expirer->inspect_all_shards(utime_t(), ceph_clock_now());
}

int RGWRados::cls_obj_prepare_op(BucketShard& bs, RGWModifyOp op, string& tag,
                                 rgw_obj& obj, uint16_t bilog_flags, rgw_zone_set *_zones_trace)
{
  rgw_zone_set zones_trace;
  if (_zones_trace) {
    zones_trace = *_zones_trace;
  }
  zones_trace.insert(svc.zone->get_zone().id);

  ObjectWriteOperation o;
  cls_rgw_obj_key key(obj.key.get_index_key_name(), obj.key.instance);
  cls_rgw_guard_bucket_resharding(o, -ERR_BUSY_RESHARDING);
  cls_rgw_bucket_prepare_op(o, op, tag, key, obj.key.get_loc(), svc.zone->get_zone().log_data, bilog_flags, zones_trace);
  return bs.index_ctx.operate(bs.bucket_obj, &o);
}

int RGWRados::cls_obj_complete_op(BucketShard& bs, const rgw_obj& obj, RGWModifyOp op, string& tag,
                                  int64_t pool, uint64_t epoch,
                                  rgw_bucket_dir_entry& ent, RGWObjCategory category,
				  list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *_zones_trace)
{
  ObjectWriteOperation o;
  rgw_bucket_dir_entry_meta dir_meta;
  dir_meta = ent.meta;
  dir_meta.category = category;

  rgw_zone_set zones_trace;
  if (_zones_trace) {
    zones_trace = *_zones_trace;
  }
  zones_trace.insert(svc.zone->get_zone().id);

  rgw_bucket_entry_ver ver;
  ver.pool = pool;
  ver.epoch = epoch;
  cls_rgw_obj_key key(ent.key.name, ent.key.instance);
  cls_rgw_guard_bucket_resharding(o, -ERR_BUSY_RESHARDING);
  cls_rgw_bucket_complete_op(o, op, tag, ver, key, dir_meta, remove_objs,
                             svc.zone->get_zone().log_data, bilog_flags, &zones_trace);
  complete_op_data *arg;
  index_completion_manager->create_completion(obj, op, tag, ver, key, dir_meta, remove_objs,
                                              svc.zone->get_zone().log_data, bilog_flags, &zones_trace, &arg);
  librados::AioCompletion *completion = arg->rados_completion;
  int ret = bs.index_ctx.aio_operate(bs.bucket_obj, arg->rados_completion, &o);
  completion->release(); /* can't reference arg here, as it might have already been released */
  return ret;
}

int RGWRados::cls_obj_complete_add(BucketShard& bs, const rgw_obj& obj, string& tag,
                                   int64_t pool, uint64_t epoch,
                                   rgw_bucket_dir_entry& ent, RGWObjCategory category,
                                   list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *zones_trace)
{
  return cls_obj_complete_op(bs, obj, CLS_RGW_OP_ADD, tag, pool, epoch, ent, category, remove_objs, bilog_flags, zones_trace);
}

int RGWRados::cls_obj_complete_del(BucketShard& bs, string& tag,
                                   int64_t pool, uint64_t epoch,
                                   rgw_obj& obj,
                                   real_time& removed_mtime,
                                   list<rgw_obj_index_key> *remove_objs,
                                   uint16_t bilog_flags,
                                   rgw_zone_set *zones_trace)
{
  rgw_bucket_dir_entry ent;
  ent.meta.mtime = removed_mtime;
  obj.key.get_index_key(&ent.key);
  return cls_obj_complete_op(bs, obj, CLS_RGW_OP_DEL, tag, pool, epoch,
			     ent, RGWObjCategory::None, remove_objs,
			     bilog_flags, zones_trace);
}

int RGWRados::cls_obj_complete_cancel(BucketShard& bs, string& tag, rgw_obj& obj, uint16_t bilog_flags, rgw_zone_set *zones_trace)
{
  rgw_bucket_dir_entry ent;
  obj.key.get_index_key(&ent.key);
  return cls_obj_complete_op(bs, obj, CLS_RGW_OP_CANCEL, tag,
			     -1 /* pool id */, 0, ent,
			     RGWObjCategory::None, NULL, bilog_flags,
			     zones_trace);
}

int RGWRados::cls_obj_set_bucket_tag_timeout(RGWBucketInfo& bucket_info, uint64_t timeout)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;
  int r = open_bucket_index(bucket_info, index_ctx, bucket_objs);
  if (r < 0)
    return r;

  return CLSRGWIssueSetTagTimeout(index_ctx, bucket_objs, cct->_conf->rgw_bucket_index_max_aio, timeout)();
}


int RGWRados::cls_bucket_list_ordered(RGWBucketInfo& bucket_info,
				      int shard_id,
				      rgw_obj_index_key& start,
				      const string& prefix,
				      uint32_t num_entries,
				      bool list_versions,
				      map<string, rgw_bucket_dir_entry>& m,
				      bool *is_truncated,
				      rgw_obj_index_key *last_entry,
				      bool (*force_check_filter)(const string& name))
{
  ldout(cct, 10) << "cls_bucket_list_ordered " << bucket_info.bucket <<
    " start " << start.name << "[" << start.instance << "] num_entries " <<
    num_entries << dendl;

  librados::IoCtx index_ctx;
  // key   - oid (for different shards if there is any)
  // value - list result for the corresponding oid (shard), it is filled by
  //         the AIO callback
  map<int, string> oids;
  map<int, struct rgw_cls_list_ret> list_results;
  int r = open_bucket_index(bucket_info, index_ctx, oids, shard_id);
  if (r < 0)
    return r;

  cls_rgw_obj_key start_key(start.name, start.instance);
  r = CLSRGWIssueBucketList(index_ctx, start_key, prefix, num_entries,
			    list_versions, oids, list_results,
			    cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  // Create a list of iterators that are used to iterate each shard
  vector<map<string, struct rgw_bucket_dir_entry>::iterator> vcurrents;
  vector<map<string, struct rgw_bucket_dir_entry>::iterator> vends;
  vector<string> vnames;
  vcurrents.reserve(list_results.size());
  vends.reserve(list_results.size());
  vnames.reserve(list_results.size());
  map<int, struct rgw_cls_list_ret>::iterator iter = list_results.begin();
  *is_truncated = false;
  for (; iter != list_results.end(); ++iter) {
    vcurrents.push_back(iter->second.dir.m.begin());
    vends.push_back(iter->second.dir.m.end());
    vnames.push_back(oids[iter->first]);
    *is_truncated = (*is_truncated || iter->second.is_truncated);
  }

  // Create a map to track the next candidate entry from each shard, if the entry
  // from a specified shard is selected/erased, the next entry from that shard will
  // be inserted for next round selection
  map<string, size_t> candidates;
  for (size_t i = 0; i < vcurrents.size(); ++i) {
    if (vcurrents[i] != vends[i]) {
      candidates[vcurrents[i]->first] = i;
    }
  }

  map<string, bufferlist> updates;
  uint32_t count = 0;
  while (count < num_entries && !candidates.empty()) {
    r = 0;
    // Select the next one
    int pos = candidates.begin()->second;
    const string& name = vcurrents[pos]->first;
    struct rgw_bucket_dir_entry& dirent = vcurrents[pos]->second;

    bool force_check = force_check_filter &&
        force_check_filter(dirent.key.name);
    if ((!dirent.exists && !dirent.is_delete_marker()) ||
        !dirent.pending_map.empty() ||
        force_check) {
      /* there are uncommitted ops. We need to check the current state,
       * and if the tags are old we need to do cleanup as well. */
      librados::IoCtx sub_ctx;
      sub_ctx.dup(index_ctx);
      r = check_disk_state(sub_ctx, bucket_info, dirent, dirent,
			   updates[vnames[pos]]);
      if (r < 0 && r != -ENOENT) {
          return r;
      }
    }
    if (r >= 0) {
      ldout(cct, 10) << "RGWRados::cls_bucket_list_ordered: got " <<
	dirent.key.name << "[" << dirent.key.instance << "]" << dendl;
      m[name] = std::move(dirent);
      ++count;
    }

    // Refresh the candidates map
    candidates.erase(candidates.begin());
    ++vcurrents[pos];
    if (vcurrents[pos] != vends[pos]) {
      candidates[vcurrents[pos]->first] = pos;
    }
  }

  // Suggest updates if there is any
  map<string, bufferlist>::iterator miter = updates.begin();
  for (; miter != updates.end(); ++miter) {
    if (miter->second.length()) {
      ObjectWriteOperation o;
      cls_rgw_suggest_changes(o, miter->second);
      // we don't care if we lose suggested updates, send them off blindly
      AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      index_ctx.aio_operate(miter->first, c, &o);
      c->release();
    }
  }

  // Check if all the returned entries are consumed or not
  for (size_t i = 0; i < vcurrents.size(); ++i) {
    if (vcurrents[i] != vends[i]) {
      *is_truncated = true;
      break;
    }
  }
  if (!m.empty())
    *last_entry = m.rbegin()->first;

  return 0;
}


int RGWRados::cls_bucket_list_unordered(RGWBucketInfo& bucket_info,
					int shard_id,
					rgw_obj_index_key& start,
					const string& prefix,
					uint32_t num_entries,
					bool list_versions,
					std::vector<rgw_bucket_dir_entry>& ent_list,
					bool *is_truncated,
					rgw_obj_index_key *last_entry,
					bool (*force_check_filter)(const string& name)) {
  ldout(cct, 10) << "cls_bucket_list_unordered " << bucket_info.bucket <<
    " start " << start.name << "[" << start.instance <<
    "] num_entries " << num_entries << dendl;

  *is_truncated = false;
  librados::IoCtx index_ctx;

  rgw_obj_index_key my_start = start;

  map<int, string> oids;
  int r = open_bucket_index(bucket_info, index_ctx, oids, shard_id);
  if (r < 0)
    return r;
  const uint32_t num_shards = oids.size();

  uint32_t current_shard;
  if (shard_id >= 0) {
    current_shard = shard_id;
  } else if (my_start.empty()) {
    current_shard = 0u;
  } else {
    current_shard =
      rgw_bucket_shard_index(my_start.name, num_shards);
  }

  uint32_t count = 0u;
  map<string, bufferlist> updates;
  std::string last_added_entry;
  while (count <= num_entries &&
	 ((shard_id >= 0 && current_shard == uint32_t(shard_id)) ||
	  current_shard < num_shards)) {
    // key   - oid (for different shards if there is any)
    // value - list result for the corresponding oid (shard), it is filled by
    //         the AIO callback
    map<int, struct rgw_cls_list_ret> list_results;
    r = CLSRGWIssueBucketList(index_ctx, my_start, prefix, num_entries,
			      list_versions, oids, list_results,
			      cct->_conf->rgw_bucket_index_max_aio)();
    if (r < 0)
      return r;

    const std::string& oid = oids[current_shard];
    ceph_assert(list_results.find(current_shard) != list_results.end());
    auto& result = list_results[current_shard];
    for (auto& entry : result.dir.m) {
      rgw_bucket_dir_entry& dirent = entry.second;

      bool force_check = force_check_filter &&
	force_check_filter(dirent.key.name);
      if ((!dirent.exists && !dirent.is_delete_marker()) ||
	  !dirent.pending_map.empty() ||
	  force_check) {
	/* there are uncommitted ops. We need to check the current state,
	 * and if the tags are old we need to do cleanup as well. */
	librados::IoCtx sub_ctx;
	sub_ctx.dup(index_ctx);
	r = check_disk_state(sub_ctx, bucket_info, dirent, dirent, updates[oid]);
	if (r < 0 && r != -ENOENT) {
	  return r;
	}
      }

      // at this point either r >=0 or r == -ENOENT
      if (r >= 0) { // i.e., if r != -ENOENT
	ldout(cct, 10) << "RGWRados::cls_bucket_list_unordered: got " <<
	  dirent.key.name << "[" << dirent.key.instance << "]" << dendl;

	if (count < num_entries) {
	  last_added_entry = entry.first;
	  my_start = dirent.key;
	  ent_list.emplace_back(std::move(dirent));
	  ++count;
	} else {
	  *is_truncated = true;
	  goto check_updates;
	}
      } else { // r == -ENOENT
	// in the case of -ENOENT, make sure we're advancing marker
	// for possible next call to CLSRGWIssueBucketList
	my_start = dirent.key;
      }
    } // entry for loop

    if (!result.is_truncated) {
      // if we reached the end of the shard read next shard
      ++current_shard;
      my_start = rgw_obj_index_key();
    }
  } // shard loop

check_updates:
  // suggest updates if there is any
  map<string, bufferlist>::iterator miter = updates.begin();
  for (; miter != updates.end(); ++miter) {
    if (miter->second.length()) {
      ObjectWriteOperation o;
      cls_rgw_suggest_changes(o, miter->second);
      // we don't care if we lose suggested updates, send them off blindly
      AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      index_ctx.aio_operate(miter->first, c, &o);
      c->release();
    }
  }

  if (last_entry && !ent_list.empty()) {
    *last_entry = last_added_entry;
  }

  return 0;
}


int RGWRados::cls_obj_usage_log_add(const string& oid,
				    rgw_usage_log_info& info)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().usage_log_pool, oid);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;
  cls_rgw_usage_log_add(op, info);

  r = ref.ioctx.operate(ref.obj.oid, &op);
  return r;
}

int RGWRados::cls_obj_usage_log_read(const string& oid, const string& user, const string& bucket,
                                     uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                                     string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage,
				     bool *is_truncated)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().usage_log_pool, oid);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  *is_truncated = false;

  r = cls_rgw_usage_log_read(ref.ioctx, ref.obj.oid, user, bucket, start_epoch, end_epoch,
			     max_entries, read_iter, usage, is_truncated);

  return r;
}

int RGWRados::cls_obj_usage_log_trim(const string& oid, const string& user, const string& bucket,
                                    uint64_t start_epoch, uint64_t end_epoch)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().usage_log_pool, oid);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  r = cls_rgw_usage_log_trim(ref.ioctx, ref.obj.oid, user, bucket, start_epoch, end_epoch);
  return r;
}

int RGWRados::cls_obj_usage_log_clear(string& oid)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().usage_log_pool, oid);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }
  librados::ObjectWriteOperation op;
  cls_rgw_usage_log_clear(op);
  r = ref.ioctx.operate(ref.obj.oid, &op);
  return r;
}


int RGWRados::remove_objs_from_index(RGWBucketInfo& bucket_info, list<rgw_obj_index_key>& oid_list)
{
  librados::IoCtx index_ctx;
  string dir_oid;

  uint8_t suggest_flag = (svc.zone->get_zone().log_data ? CEPH_RGW_DIR_SUGGEST_LOG_OP : 0);

  int r = open_bucket_index(bucket_info, index_ctx, dir_oid);
  if (r < 0)
    return r;

  bufferlist updates;

  for (auto iter = oid_list.begin(); iter != oid_list.end(); ++iter) {
    rgw_bucket_dir_entry entry;
    entry.key = *iter;
    dout(2) << "RGWRados::remove_objs_from_index bucket=" << bucket_info.bucket << " obj=" << entry.key.name << ":" << entry.key.instance << dendl;
    entry.ver.epoch = (uint64_t)-1; // ULLONG_MAX, needed to that objclass doesn't skip out request
    updates.append(CEPH_RGW_REMOVE | suggest_flag);
    encode(entry, updates);
  }

  bufferlist out;

  r = index_ctx.exec(dir_oid, RGW_CLASS, RGW_DIR_SUGGEST_CHANGES, updates, out);

  return r;
}

int RGWRados::check_disk_state(librados::IoCtx io_ctx,
                               const RGWBucketInfo& bucket_info,
                               rgw_bucket_dir_entry& list_state,
                               rgw_bucket_dir_entry& object,
                               bufferlist& suggested_updates)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  uint8_t suggest_flag = (svc.zone->get_zone().log_data ? CEPH_RGW_DIR_SUGGEST_LOG_OP : 0);

  std::string loc;

  rgw_obj obj(bucket, list_state.key);

  string oid;
  get_obj_bucket_and_oid_loc(obj, oid, loc);

  if (loc != list_state.locator) {
    ldout(cct, 0) << "WARNING: generated locator (" << loc << ") is different from listed locator (" << list_state.locator << ")" << dendl;
  }

  io_ctx.locator_set_key(list_state.locator);

  RGWObjState *astate = NULL;
  RGWObjectCtx rctx(this);
  int r = get_obj_state(&rctx, bucket_info, obj, &astate, false);
  if (r < 0)
    return r;

  list_state.pending_map.clear(); // we don't need this and it inflates size
  if (!astate->exists) {
      /* object doesn't exist right now -- hopefully because it's
       * marked as !exists and got deleted */
    if (list_state.exists) {
      /* FIXME: what should happen now? Work out if there are any
       * non-bad ways this could happen (there probably are, but annoying
       * to handle!) */
    }
    // encode a suggested removal of that key
    list_state.ver.epoch = io_ctx.get_last_version();
    list_state.ver.pool = io_ctx.get_id();
    cls_rgw_encode_suggestion(CEPH_RGW_REMOVE, list_state, suggested_updates);
    return -ENOENT;
  }

  string etag;
  string content_type;
  ACLOwner owner;

  object.meta.size = astate->size;
  object.meta.accounted_size = astate->accounted_size;
  object.meta.mtime = astate->mtime;

  map<string, bufferlist>::iterator iter = astate->attrset.find(RGW_ATTR_ETAG);
  if (iter != astate->attrset.end()) {
    etag = rgw_bl_str(iter->second);
  }
  iter = astate->attrset.find(RGW_ATTR_CONTENT_TYPE);
  if (iter != astate->attrset.end()) {
    content_type = rgw_bl_str(iter->second);
  }
  iter = astate->attrset.find(RGW_ATTR_ACL);
  if (iter != astate->attrset.end()) {
    r = decode_policy(iter->second, &owner);
    if (r < 0) {
      dout(0) << "WARNING: could not decode policy for object: " << obj << dendl;
    }
  }

  if (astate->has_manifest) {
    RGWObjManifest::obj_iterator miter;
    RGWObjManifest& manifest = astate->manifest;
    for (miter = manifest.obj_begin(); miter != manifest.obj_end(); ++miter) {
      const rgw_raw_obj& raw_loc = miter.get_location().get_raw_obj(this);
      rgw_obj loc;
      rgw_raw_obj_to_obj(manifest.get_obj().bucket, raw_loc, &loc);

      if (loc.key.ns == RGW_OBJ_NS_MULTIPART) {
	dout(10) << "check_disk_state(): removing manifest part from index: " << loc << dendl;
	r = delete_obj_index(loc);
	if (r < 0) {
	  dout(0) << "WARNING: delete_obj_index() returned r=" << r << dendl;
	}
      }
    }
  }

  object.meta.etag = etag;
  object.meta.content_type = content_type;
  object.meta.owner = owner.get_id().to_str();
  object.meta.owner_display_name = owner.get_display_name();

  // encode suggested updates
  list_state.ver.pool = io_ctx.get_id();
  list_state.ver.epoch = astate->epoch;
  list_state.meta.size = object.meta.size;
  list_state.meta.accounted_size = object.meta.accounted_size;
  list_state.meta.mtime = object.meta.mtime;
  list_state.meta.category = main_category;
  list_state.meta.etag = etag;
  list_state.meta.content_type = content_type;
  if (astate->obj_tag.length() > 0)
    list_state.tag = astate->obj_tag.c_str();
  list_state.meta.owner = owner.get_id().to_str();
  list_state.meta.owner_display_name = owner.get_display_name();

  list_state.exists = true;
  cls_rgw_encode_suggestion(CEPH_RGW_UPDATE | suggest_flag, list_state, suggested_updates);
  return 0;
}

int RGWRados::cls_bucket_head(const RGWBucketInfo& bucket_info, int shard_id, vector<rgw_bucket_dir_header>& headers, map<int, string> *bucket_instance_ids)
{
  librados::IoCtx index_ctx;
  map<int, string> oids;
  map<int, struct rgw_cls_list_ret> list_results;
  int r = open_bucket_index(bucket_info, index_ctx, oids, list_results, shard_id, bucket_instance_ids);
  if (r < 0)
    return r;

  r = CLSRGWIssueGetDirHeader(index_ctx, oids, list_results, cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  map<int, struct rgw_cls_list_ret>::iterator iter = list_results.begin();
  for(; iter != list_results.end(); ++iter) {
    headers.push_back(std::move(iter->second.dir.header));
  }
  return 0;
}

int RGWRados::cls_bucket_head_async(const RGWBucketInfo& bucket_info, int shard_id, RGWGetDirHeader_CB *ctx, int *num_aio)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;
  int r = open_bucket_index(bucket_info, index_ctx, bucket_objs, shard_id);
  if (r < 0)
    return r;

  map<int, string>::iterator iter = bucket_objs.begin();
  for (; iter != bucket_objs.end(); ++iter) {
    r = cls_rgw_get_dir_header_async(index_ctx, iter->second, static_cast<RGWGetDirHeader_CB*>(ctx->get()));
    if (r < 0) {
      ctx->put();
      break;
    } else {
      (*num_aio)++;
    }
  }
  return r;
}

int RGWRados::cls_user_get_header(const string& user_id, cls_user_header *header)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_raw_obj obj(svc.zone->get_zone_params().user_uid_pool, buckets_obj_id);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  int rc;
  ::cls_user_get_header(op, header, &rc);
  bufferlist ibl;
  r = ref.ioctx.operate(ref.obj.oid, &op, &ibl);
  if (r < 0)
    return r;
  if (rc < 0)
    return rc;

  return 0;
}

int RGWRados::cls_user_reset_stats(const string& user_id)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_raw_obj obj(svc.zone->get_zone_params().user_uid_pool, buckets_obj_id);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_reset_stats(op);
  return ref.ioctx.operate(ref.obj.oid, &op);
}

int RGWRados::cls_user_get_header_async(const string& user_id, RGWGetUserHeader_CB *ctx)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_raw_obj obj(svc.zone->get_zone_params().user_uid_pool, buckets_obj_id);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  r = ::cls_user_get_header_async(ref.ioctx, ref.obj.oid, ctx);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::cls_user_sync_bucket_stats(rgw_raw_obj& user_obj,
					 const RGWBucketInfo& bucket_info)
{
  vector<rgw_bucket_dir_header> headers;
  int r = cls_bucket_head(bucket_info, RGW_NO_SHARD, headers);
  if (r < 0) {
    ldout(cct, 20) << "cls_bucket_header() returned " << r << dendl;
    return r;
  }

  cls_user_bucket_entry entry;

  bucket_info.bucket.convert(&entry.bucket);

  for (const auto& hiter : headers) {
    for (const auto& iter : hiter.stats) {
      if (RGWObjCategory::Main == iter.first ||
	  RGWObjCategory::MultiMeta == iter.first) {
	const struct rgw_bucket_category_stats& header_stats = iter.second;
	entry.size += header_stats.total_size;
	entry.size_rounded += header_stats.total_size_rounded;
	entry.count += header_stats.num_entries;
      }
    }
  }

  list<cls_user_bucket_entry> entries;
  entries.push_back(entry);

  r = cls_user_update_buckets(user_obj, entries, false);
  if (r < 0) {
    ldout(cct, 20) << "cls_user_update_buckets() returned " << r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::cls_user_get_bucket_stats(const rgw_bucket& bucket, cls_user_bucket_entry& entry)
{
  vector<rgw_bucket_dir_header> headers;
  RGWBucketInfo bucket_info;
  auto obj_ctx = svc.sysobj->init_obj_ctx();
  int ret = get_bucket_instance_info(obj_ctx, bucket, bucket_info, NULL, NULL);
  if (ret < 0) {
    return ret;
  }

  ret = cls_bucket_head(bucket_info, RGW_NO_SHARD, headers);
  if (ret < 0) {
    ldout(cct, 20) << "cls_bucket_header() returned " << ret << dendl;
    return ret;
  }

  bucket.convert(&entry.bucket);

  for (const auto& hiter : headers) {
    for (const auto& iter : hiter.stats) {
      const struct rgw_bucket_category_stats& header_stats = iter.second;
      entry.size += header_stats.total_size;
      entry.size_rounded += header_stats.total_size_rounded;
      entry.count += header_stats.num_entries;
    }
  }

  return 0;
}

int RGWRados::cls_user_list_buckets(rgw_raw_obj& obj,
                                    const string& in_marker,
                                    const string& end_marker,
                                    const int max_entries,
                                    list<cls_user_bucket_entry>& entries,
                                    string * const out_marker,
                                    bool * const truncated)
{
  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  int rc;

  cls_user_bucket_list(op, in_marker, end_marker, max_entries, entries, out_marker, truncated, &rc);
  bufferlist ibl;
  r = ref.ioctx.operate(ref.obj.oid, &op, &ibl);
  if (r < 0)
    return r;
  if (rc < 0)
    return rc;

  return 0;
}

int RGWRados::cls_user_update_buckets(rgw_raw_obj& obj, list<cls_user_bucket_entry>& entries, bool add)
{
  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  cls_user_set_buckets(op, entries, add);
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::complete_sync_user_stats(const rgw_user& user_id)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_raw_obj obj(svc.zone->get_zone_params().user_uid_pool, buckets_obj_id);
  return cls_user_complete_stats_sync(obj);
}

int RGWRados::cls_user_complete_stats_sync(rgw_raw_obj& obj)
{
  rgw_rados_ref ref;
  int r = get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_complete_stats_sync(op);
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::cls_user_add_bucket(rgw_raw_obj& obj, const cls_user_bucket_entry& entry)
{
  list<cls_user_bucket_entry> l;
  l.push_back(entry);

  return cls_user_update_buckets(obj, l, true);
}

int RGWRados::cls_user_remove_bucket(rgw_raw_obj& obj, const cls_user_bucket& bucket)
{
  rgw_rados_ref ref;
  int r = get_system_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_remove_bucket(op, bucket);
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::check_bucket_shards(const RGWBucketInfo& bucket_info, const rgw_bucket& bucket,
				  RGWQuotaInfo& bucket_quota)
{
  if (! cct->_conf.get_val<bool>("rgw_dynamic_resharding")) {
      return 0;
  }

  bool need_resharding = false;
  int num_source_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);
  uint32_t suggested_num_shards;

  const uint64_t max_objs_per_shard =
    cct->_conf.get_val<uint64_t>("rgw_max_objs_per_shard");
  int ret =
    quota_handler->check_bucket_shards(max_objs_per_shard, num_source_shards,
				       bucket_info.owner, bucket, bucket_quota,
				       1, need_resharding, &suggested_num_shards);
  if (ret < 0) {
    return ret;
  }

  if (need_resharding) {
    ldout(cct, 20) << __func__ << " bucket " << bucket.name << " need resharding " <<
      " old num shards " << bucket_info.num_shards << " new num shards " << suggested_num_shards <<
      dendl;
    return add_bucket_to_reshard(bucket_info, suggested_num_shards);
  }

  return ret;
}

int RGWRados::add_bucket_to_reshard(const RGWBucketInfo& bucket_info, uint32_t new_num_shards)
{
  RGWReshard reshard(this);

  uint32_t num_source_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

  new_num_shards = std::min(new_num_shards, get_max_bucket_shards());
  if (new_num_shards <= num_source_shards) {
    ldout(cct, 20) << "not resharding bucket name=" << bucket_info.bucket.name << ", orig_num=" << num_source_shards << ", new_num_shards=" << new_num_shards << dendl;
    return 0;
  }

  cls_rgw_reshard_entry entry;
  entry.time = real_clock::now();
  entry.tenant = bucket_info.owner.tenant;
  entry.bucket_name = bucket_info.bucket.name;
  entry.bucket_id = bucket_info.bucket.bucket_id;
  entry.old_num_shards = num_source_shards;
  entry.new_num_shards = new_num_shards;

  return reshard.add(entry);
}

int RGWRados::check_quota(const rgw_user& bucket_owner, rgw_bucket& bucket,
                          RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, bool check_size_only)
{
  // if we only check size, then num_objs will set to 0
  if(check_size_only)
    return quota_handler->check_quota(bucket_owner, bucket, user_quota, bucket_quota, 0, obj_size);

  return quota_handler->check_quota(bucket_owner, bucket, user_quota, bucket_quota, 1, obj_size);
}

void RGWRados::get_bucket_index_objects(const string& bucket_oid_base,
					uint32_t num_shards,
					map<int, string>& bucket_objects,
					int shard_id) {
  if (!num_shards) {
    bucket_objects[0] = bucket_oid_base;
  } else {
    char buf[bucket_oid_base.size() + 32];
    if (shard_id < 0) {
      for (uint32_t i = 0; i < num_shards; ++i) {
        snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), i);
        bucket_objects[i] = buf;
      }
    } else {
      if ((uint32_t)shard_id > num_shards) {
        return;
      }
      snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), shard_id);
      bucket_objects[shard_id] = buf;
    }
  }
}

void RGWRados::get_bucket_instance_ids(const RGWBucketInfo& bucket_info, int shard_id, map<int, string> *result)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  string plain_id = bucket.name + ":" + bucket.bucket_id;
  if (!bucket_info.num_shards) {
    (*result)[0] = plain_id;
  } else {
    char buf[16];
    if (shard_id < 0) {
      for (uint32_t i = 0; i < bucket_info.num_shards; ++i) {
        snprintf(buf, sizeof(buf), ":%d", i);
        (*result)[i] = plain_id + buf;
      }
    } else {
      if ((uint32_t)shard_id > bucket_info.num_shards) {
        return;
      }
      snprintf(buf, sizeof(buf), ":%d", shard_id);
      (*result)[shard_id] = plain_id + buf;
    }
  }
}

int RGWRados::get_target_shard_id(const RGWBucketInfo& bucket_info, const string& obj_key,
                                  int *shard_id)
{
  int r = 0;
  switch (bucket_info.bucket_index_shard_hash_type) {
    case RGWBucketInfo::MOD:
      if (!bucket_info.num_shards) {
        if (shard_id) {
          *shard_id = -1;
        }
      } else {
        uint32_t sid = rgw_bucket_shard_index(obj_key, bucket_info.num_shards);
        if (shard_id) {
          *shard_id = (int)sid;
        }
      }
      break;
    default:
      r = -ENOTSUP;
  }
  return r;
}

void RGWRados::get_bucket_index_object(const string& bucket_oid_base, uint32_t num_shards,
                                      int shard_id, string *bucket_obj)
{
  if (!num_shards) {
    // By default with no sharding, we use the bucket oid as itself
    (*bucket_obj) = bucket_oid_base;
  } else {
    char buf[bucket_oid_base.size() + 32];
    snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), shard_id);
    (*bucket_obj) = buf;
  }
}

int RGWRados::get_bucket_index_object(const string& bucket_oid_base, const string& obj_key,
    uint32_t num_shards, RGWBucketInfo::BIShardsHashType hash_type, string *bucket_obj, int *shard_id)
{
  int r = 0;
  switch (hash_type) {
    case RGWBucketInfo::MOD:
      if (!num_shards) {
        // By default with no sharding, we use the bucket oid as itself
        (*bucket_obj) = bucket_oid_base;
        if (shard_id) {
          *shard_id = -1;
        }
      } else {
        uint32_t sid = rgw_bucket_shard_index(obj_key, num_shards);
        char buf[bucket_oid_base.size() + 32];
        snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), sid);
        (*bucket_obj) = buf;
        if (shard_id) {
          *shard_id = (int)sid;
        }
      }
      break;
    default:
      r = -ENOTSUP;
  }
  return r;
}

uint64_t RGWRados::instance_id()
{
  return get_rados_handle()->get_instance_id();
}

uint64_t RGWRados::next_bucket_id()
{
  Mutex::Locker l(bucket_id_lock);
  return ++max_bucket_id;
}

RGWRados *RGWStoreManager::init_storage_provider(CephContext *cct, bool use_gc_thread, bool use_lc_thread,
						 bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_cache)
{
  RGWRados *store = new RGWRados;

  if ((*store).set_use_cache(use_cache)
              .set_run_gc_thread(use_gc_thread)
              .set_run_lc_thread(use_lc_thread)
              .set_run_quota_threads(quota_threads)
              .set_run_sync_thread(run_sync_thread)
              .set_run_reshard_thread(run_reshard_thread)
              .initialize(cct) < 0) {
    delete store;
    return NULL;
  }

  return store;
}

RGWRados *RGWStoreManager::init_raw_storage_provider(CephContext *cct)
{
  RGWRados *store = NULL;
  store = new RGWRados;

  store->set_context(cct);

  int ret = store->init_svc(true);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
    return nullptr;
  }

  if (store->init_rados() < 0) {
    delete store;
    return nullptr;
  }

  return store;
}

void RGWStoreManager::close_storage(RGWRados *store)
{
  if (!store)
    return;

  store->finalize();

  delete store;
}

librados::Rados* RGWRados::get_rados_handle()
{
  if (rados.size() == 1) {
    return &rados[0];
  } else {
    handle_lock.get_read();
    pthread_t id = pthread_self();
    std::map<pthread_t, int>:: iterator it = rados_map.find(id);

    if (it != rados_map.end()) {
      handle_lock.put_read();
      return &rados[it->second];
    } else {
      handle_lock.put_read();
      handle_lock.get_write();
      const uint32_t handle = next_rados_handle;
      rados_map[id] = handle;
      if (++next_rados_handle == rados.size()) {
        next_rados_handle = 0;
      }
      handle_lock.put_write();
      return &rados[handle];
    }
  }
}

int RGWRados::delete_raw_obj_aio(const rgw_raw_obj& obj, list<librados::AioCompletion *>& handles)
{
  rgw_rados_ref ref;
  int ret = get_raw_obj_ref(obj, &ref);
  if (ret < 0) {
    lderr(cct) << "ERROR: failed to get obj ref with ret=" << ret << dendl;
    return ret;
  }

  ObjectWriteOperation op;
  list<string> prefixes;
  cls_rgw_remove_obj(op, prefixes);

  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  ret = ref.ioctx.aio_operate(ref.obj.oid, c, &op);
  if (ret < 0) {
    lderr(cct) << "ERROR: AioOperate failed with ret=" << ret << dendl;
    c->release();
    return ret;
  }

  handles.push_back(c);

  return 0;
}

int RGWRados::delete_obj_aio(const rgw_obj& obj,
                             RGWBucketInfo& bucket_info, RGWObjState *astate,
                             list<librados::AioCompletion *>& handles, bool keep_index_consistent)
{
  rgw_rados_ref ref;
  int ret = get_obj_head_ref(bucket_info, obj, &ref);
  if (ret < 0) {
    lderr(cct) << "ERROR: failed to get obj ref with ret=" << ret << dendl;
    return ret;
  }

  if (keep_index_consistent) {
    RGWRados::Bucket bop(this, bucket_info);
    RGWRados::Bucket::UpdateIndex index_op(&bop, obj);

    ret = index_op.prepare(CLS_RGW_OP_DEL, &astate->write_tag);
    if (ret < 0) {
      lderr(cct) << "ERROR: failed to prepare index op with ret=" << ret << dendl;
      return ret;
    }
  }

  ObjectWriteOperation op;
  list<string> prefixes;
  cls_rgw_remove_obj(op, prefixes);

  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  ret = ref.ioctx.aio_operate(ref.obj.oid, c, &op);
  if (ret < 0) {
    lderr(cct) << "ERROR: AioOperate failed with ret=" << ret << dendl;
    c->release();
    return ret;
  }

  handles.push_back(c);

  if (keep_index_consistent) {
    ret = delete_obj_index(obj);
    if (ret < 0) {
      lderr(cct) << "ERROR: failed to delete obj index with ret=" << ret << dendl;
      return ret;
    }
  }
  return ret;
}

int rgw_compression_info_from_attrset(map<string, bufferlist>& attrs, bool& need_decompress, RGWCompressionInfo& cs_info) {
  map<string, bufferlist>::iterator value = attrs.find(RGW_ATTR_COMPRESSION);
  if (value != attrs.end()) {
    auto bliter = value->second.cbegin();
    try {
      decode(cs_info, bliter);
    } catch (buffer::error& err) {
      return -EIO;
    }
    if (cs_info.blocks.size() == 0) {
      return -EIO;
    }
    if (cs_info.compression_type != "none")
      need_decompress = true;
    else
      need_decompress = false;
    return 0;
  } else {
    need_decompress = false;
    return 0;
  }
}

bool RGWRados::call(std::string_view command, const cmdmap_t& cmdmap,
		    std::string_view format, bufferlist& out)
{
  if (command == "cache list"sv) {
    std::optional<std::string> filter;
    if (auto i = cmdmap.find("filter"); i != cmdmap.cend()) {
      filter = boost::get<std::string>(i->second);
    }
    std::unique_ptr<Formatter> f(ceph::Formatter::create(format, "table"));
    if (f) {
      f->open_array_section("cache_entries");
      call_list(filter, f.get());
      f->close_section();
      f->flush(out);
      return true;
    } else {
      out.append("Unable to create Formatter.\n");
      return false;
    }
  } else if (command == "cache inspect"sv) {
    std::unique_ptr<Formatter> f(ceph::Formatter::create(format, "json-pretty"));
    if (f) {
      const auto& target = boost::get<std::string>(cmdmap.at("target"));
      if (call_inspect(target, f.get())) {
        f->flush(out);
        return true;
      } else {
        out.append("Unable to find entry "s + target + ".\n");
        return false;
      }
    } else {
      out.append("Unable to create Formatter.\n");
      return false;
    }
  } else if (command == "cache erase"sv) {
    const auto& target = boost::get<std::string>(cmdmap.at("target"));
    if (call_erase(target)) {
      return true;
    } else {
      out.append("Unable to find entry "s + target + ".\n");
      return false;
    }
  } else if (command == "cache zap"sv) {
    call_zap();
    return true;
  }
  return false;
}

void RGWRados::call_list(const std::optional<std::string>& s,
                         ceph::Formatter *f)
{
  if (!svc.cache) {
    return;
  }
  svc.cache->call_list(s, f);
}

bool RGWRados::call_inspect(const std::string& s, Formatter *f)
{
  if (!svc.cache) {
    return false;
  }
  return svc.cache->call_inspect(s, f);
}

bool RGWRados::call_erase(const std::string& s) {
  if (!svc.cache) {
    return false;
  }
  return svc.cache->call_erase(s);
}

void RGWRados::call_zap() {
  if (svc.cache) {
    return;
  }
  svc.cache->call_zap();
}

string RGWRados::get_mfa_oid(const rgw_user& user)
{
  return string("user:") + user.to_str();
}

int RGWRados::get_mfa_ref(const rgw_user& user, rgw_rados_ref *ref)
{
  string oid = get_mfa_oid(user);
  rgw_raw_obj obj(svc.zone->get_zone_params().otp_pool, oid);
  return get_system_obj_ref(obj, ref);
}

int RGWRados::check_mfa(const rgw_user& user, const string& otp_id, const string& pin)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(user, &ref);
  if (r < 0) {
    return r;
  }

  rados::cls::otp::otp_check_t result;

  r = rados::cls::otp::OTP::check(cct, ref.ioctx, ref.obj.oid, otp_id, pin, &result);
  if (r < 0)
    return r;

  ldout(cct, 20) << "OTP check, otp_id=" << otp_id << " result=" << (int)result.result << dendl;

  return (result.result == rados::cls::otp::OTP_CHECK_SUCCESS ? 0 : -EACCES);
}

void RGWRados::prepare_mfa_write(librados::ObjectWriteOperation *op,
                                 RGWObjVersionTracker *objv_tracker,
                                 const ceph::real_time& mtime)
{
  RGWObjVersionTracker ot;

  if (objv_tracker) {
    ot = *objv_tracker;
  }

  if (ot.write_version.tag.empty()) {
    if (ot.read_version.tag.empty()) {
      ot.generate_new_write_ver(cct);
    } else {
      ot.write_version = ot.read_version;
      ot.write_version.ver++;
    }
  }

  ot.prepare_op_for_write(op);
  struct timespec mtime_ts = real_clock::to_timespec(mtime);
  op->mtime2(&mtime_ts);
}

int RGWRados::create_mfa(const rgw_user& user, const rados::cls::otp::otp_info_t& config,
                         RGWObjVersionTracker *objv_tracker, const ceph::real_time& mtime)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(user, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  prepare_mfa_write(&op, objv_tracker, mtime);
  rados::cls::otp::OTP::create(&op, config);
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (r < 0) {
    ldout(cct, 20) << "OTP create, otp_id=" << config.id << " result=" << (int)r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::remove_mfa(const rgw_user& user, const string& id,
                         RGWObjVersionTracker *objv_tracker,
                         const ceph::real_time& mtime)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(user, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  prepare_mfa_write(&op, objv_tracker, mtime);
  rados::cls::otp::OTP::remove(&op, id);
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (r < 0) {
    ldout(cct, 20) << "OTP remove, otp_id=" << id << " result=" << (int)r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::get_mfa(const rgw_user& user, const string& id, rados::cls::otp::otp_info_t *result)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(user, &ref);
  if (r < 0) {
    return r;
  }

  r = rados::cls::otp::OTP::get(nullptr, ref.ioctx, ref.obj.oid, id, result);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWRados::list_mfa(const rgw_user& user, list<rados::cls::otp::otp_info_t> *result)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(user, &ref);
  if (r < 0) {
    return r;
  }

  r = rados::cls::otp::OTP::get_all(nullptr, ref.ioctx, ref.obj.oid, result);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWRados::otp_get_current_time(const rgw_user& user, ceph::real_time *result)
{
  rgw_rados_ref ref;

  int r = get_mfa_ref(user, &ref);
  if (r < 0) {
    return r;
  }

  r = rados::cls::otp::OTP::get_current_time(ref.ioctx, ref.obj.oid, result);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWRados::set_mfa(const string& oid, const list<rados::cls::otp::otp_info_t>& entries,
                      bool reset_obj, RGWObjVersionTracker *objv_tracker,
                      const real_time& mtime)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().otp_pool, oid);
  rgw_rados_ref ref;
  int r = get_system_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  if (reset_obj) {
    op.remove();
    op.set_op_flags2(LIBRADOS_OP_FLAG_FAILOK);
    op.create(false);
  }
  prepare_mfa_write(&op, objv_tracker, mtime);
  rados::cls::otp::OTP::set(&op, entries);
  r = ref.ioctx.operate(ref.obj.oid, &op);
  if (r < 0) {
    ldout(cct, 20) << "OTP set entries.size()=" << entries.size() << " result=" << (int)r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::list_mfa(const string& oid, list<rados::cls::otp::otp_info_t> *result,
                       RGWObjVersionTracker *objv_tracker, ceph::real_time *pmtime)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().otp_pool, oid);
  rgw_rados_ref ref;
  int r = get_system_obj_ref(obj, &ref);
  if (r < 0) {
    return r;
  }
  librados::ObjectReadOperation op;
  struct timespec mtime_ts;
  if (pmtime) {
    op.stat2(nullptr, &mtime_ts, nullptr);
  }
  objv_tracker->prepare_op_for_read(&op);
  r = rados::cls::otp::OTP::get_all(&op, ref.ioctx, ref.obj.oid, result);
  if (r < 0) {
    return r;
  }
  if (pmtime) {
    *pmtime = ceph::real_clock::from_timespec(mtime_ts);
  }

  return 0;
}

