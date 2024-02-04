// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "include/compat.h"
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sstream>

#include <boost/algorithm/string.hpp>
#include <string_view>

#include <boost/container/flat_set.hpp>
#include <boost/format.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>

#include "common/ceph_json.h"

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/Throttle.h"
#include "common/BackTrace.h"

#include "rgw_sal.h"
#include "rgw_zone.h"
#include "rgw_cache.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h" /* for dumping s3policy in debug log */
#include "rgw_aio_throttle.h"
#include "driver/rados/rgw_bucket.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_datalog.h"
#include "rgw_putobj_processor.h"

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_const.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "osd/osd_types.h"

#include "rgw_tools.h"
#include "rgw_coroutine.h"
#include "rgw_compression.h"
#include "rgw_crypt.h"
#include "rgw_etag_verifier.h"
#include "rgw_worker.h"
#include "rgw_notify.h"
#include "rgw_http_errors.h"

#undef fork // fails to compile RGWPeriod::fork() below

#include "common/Clock.h"

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
#include "rgw_sync_counters.h"
#include "rgw_sync_trace.h"
#include "rgw_trim_datalog.h"
#include "rgw_trim_mdlog.h"
#include "rgw_data_sync.h"
#include "rgw_realm_watcher.h"
#include "rgw_reshard.h"
#include "rgw_cr_rados.h"

#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"
#include "services/svc_quota.h"
#include "services/svc_sync_modules.h"
#include "services/svc_sys_obj.h"
#include "services/svc_sys_obj_cache.h"
#include "services/svc_bucket.h"
#include "services/svc_mdlog.h"

#include "compressor/Compressor.h"

#include "rgw_d3n_datacache.h"

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

using namespace std;
using namespace librados;

#define ldout_bitx(_bitx, _dpp, _level) if(_bitx) { ldpp_dout(_dpp, 0) << "BITX: "
#define ldout_bitx_c(_bitx, _ctx, _level) if(_bitx) { ldout(_ctx, 0) << "BITX: "
#define dendl_bitx                      dendl ; }

static string shadow_ns = "shadow";
static string default_bucket_index_pool_suffix = "rgw.buckets.index";
static string default_storage_extra_pool_suffix = "rgw.buckets.non-ec";

static RGWObjCategory main_category = RGWObjCategory::Main;
#define RGW_USAGE_OBJ_PREFIX "usage."

// reads attribute as std::string
static inline void read_attr(std::map<std::string, bufferlist>& attrs,
			     const std::string& attr_name,
			     std::string& dest,
			     bool* found = nullptr) {
  auto i = attrs.find(attr_name);
  if (i != attrs.end()) {
    dest = rgw_bl_str(i->second);
  }
  if (found) *found = i != attrs.end();
}

// reads attribute as bufferlist
static inline void read_attr(std::map<std::string, bufferlist>& attrs,
			     const std::string& attr_name,
			     bufferlist& dest,
			     bool* found = nullptr) {
  auto i = attrs.find(attr_name);
  if (i != attrs.cend()) {
    dest = i->second; // copy
  }
  if (found) {
    *found = i != attrs.end();
  }
}

rgw_raw_obj rgw_obj_select::get_raw_obj(RGWRados* store) const
{
  if (!is_raw) {
    rgw_raw_obj r;
    store->obj_to_raw(placement_rule, obj, &r);
    return r;
  }
  return raw_obj;
}

void RGWObjVersionTracker::prepare_op_for_read(ObjectReadOperation* op)
{
  obj_version* check_objv = version_for_check();

  if (check_objv) {
    cls_version_check(*op, *check_objv, VER_COND_EQ);
  }

  cls_version_read(*op, &read_version);
}

void RGWObjVersionTracker::prepare_op_for_write(ObjectWriteOperation *op)
{
  obj_version* check_objv = version_for_check();
  obj_version* modify_version = version_for_write();

  if (check_objv) {
    cls_version_check(*op, *check_objv, VER_COND_EQ);
  }

  if (modify_version) {
    cls_version_set(*op, *modify_version);
  } else {
    cls_version_inc(*op);
  }
}

void RGWObjVersionTracker::apply_write()
{
  const bool checked = (read_version.ver != 0);
  const bool incremented = (write_version.ver == 0);

  if (checked && incremented) {
    // apply cls_version_inc() so our next operation can recheck it
    ++read_version.ver;
  } else {
    read_version = write_version;
  }
  write_version = obj_version();
}

RGWObjStateManifest *RGWObjectCtx::get_state(const rgw_obj& obj) {
  RGWObjStateManifest *result;
  typename std::map<rgw_obj, RGWObjStateManifest>::iterator iter;
  lock.lock_shared();
  assert (!obj.empty());
  iter = objs_state.find(obj);
  if (iter != objs_state.end()) {
    result = &iter->second;
    lock.unlock_shared();
  } else {
    lock.unlock_shared();
    lock.lock();
    result = &objs_state[obj];
    lock.unlock();
  }
  return result;
}

void RGWObjectCtx::set_compressed(const rgw_obj& obj) {
  std::unique_lock wl{lock};
  assert (!obj.empty());
  objs_state[obj].state.compressed = true;
}

void RGWObjectCtx::set_atomic(const rgw_obj& obj) {
  std::unique_lock wl{lock};
  assert (!obj.empty());
  objs_state[obj].state.is_atomic = true;
}
void RGWObjectCtx::set_prefetch_data(const rgw_obj& obj) {
  std::unique_lock wl{lock};
  assert (!obj.empty());
  objs_state[obj].state.prefetch_data = true;
}

void RGWObjectCtx::invalidate(const rgw_obj& obj) {
  std::unique_lock wl{lock};
  auto iter = objs_state.find(obj);
  if (iter == objs_state.end()) {
    return;
  }
  bool is_atomic = iter->second.state.is_atomic;
  bool prefetch_data = iter->second.state.prefetch_data;
  bool compressed = iter->second.state.compressed;

  objs_state.erase(iter);

  if (is_atomic || prefetch_data) {
    auto& sm = objs_state[obj];
    sm.state.is_atomic = is_atomic;
    sm.state.prefetch_data = prefetch_data;
    sm.state.compressed = compressed;
  }
}

class RGWMetaNotifierManager : public RGWCoroutinesManager {
  RGWRados* store;
  RGWHTTPManager http_manager;

public:
  RGWMetaNotifierManager(RGWRados *_driver) : RGWCoroutinesManager(_driver->ctx(), _driver->get_cr_registry()), store(_driver),
                                             http_manager(store->ctx(), completion_mgr) {
    http_manager.start();
  }

  int notify_all(const DoutPrefixProvider *dpp, map<rgw_zone_id, RGWRESTConn *>& conn_map, set<int>& shards) {
    rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                    { "notify", NULL },
                                    { NULL, NULL } };

    list<RGWCoroutinesStack *> stacks;
    for (auto iter = conn_map.begin(); iter != conn_map.end(); ++iter) {
      RGWRESTConn *conn = iter->second;
      RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
      stack->call(new RGWPostRESTResourceCR<set<int>, int>(store->ctx(), conn, &http_manager, "/admin/log", pairs, shards, NULL));

      stacks.push_back(stack);
    }
    return run(dpp, stacks);
  }
};

class RGWDataNotifierManager : public RGWCoroutinesManager {
  RGWRados* store;
  RGWHTTPManager http_manager;

public:
  RGWDataNotifierManager(RGWRados *_driver) : RGWCoroutinesManager(_driver->ctx(), _driver->get_cr_registry()), store(_driver),
                                             http_manager(store->ctx(), completion_mgr) {
    http_manager.start();
  }

  int notify_all(const DoutPrefixProvider *dpp, map<rgw_zone_id, RGWRESTConn *>& conn_map,
		bc::flat_map<int, bc::flat_set<rgw_data_notify_entry> >& shards) {

    list<RGWCoroutinesStack *> stacks;
    const char *source_zone = store->svc.zone->get_zone_params().get_id().c_str();
    for (auto iter = conn_map.begin(); iter != conn_map.end(); ++iter) {
      RGWRESTConn *conn = iter->second;
      RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
      stack->call(new RGWDataPostNotifyCR(store, http_manager, shards, source_zone, conn));
      stacks.push_back(stack);
    }

    return run(dpp, stacks);
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
  auto interval = std::chrono::milliseconds(msec);

  do {
    auto start = ceph::real_clock::now();
    int r = processor->process(this);
    if (r < 0) {
      ldpp_dout(this, 0) << "ERROR: processor->process() returned error r=" << r << dendl;
    }

    if (processor->going_down())
      break;

    auto end = ceph::real_clock::now() - start;

    uint64_t cur_msec = processor->interval_msec();
    if (cur_msec != msec) { /* was it reconfigured? */
      msec = cur_msec;
      interval = std::chrono::milliseconds(msec);
    }

    if (cur_msec > 0) {
      if (interval <= end)
        continue; // next round

      auto wait_time = interval - end;
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
  RGWMetaNotifier(RGWRados *_driver, RGWMetadataLog* log)
    : RGWRadosThread(_driver, "meta-notifier"), notify_mgr(_driver), log(log) {}

  int process(const DoutPrefixProvider *dpp) override;
};

int RGWMetaNotifier::process(const DoutPrefixProvider *dpp)
{
  set<int> shards;

  log->read_clear_modified(shards);

  if (shards.empty()) {
    return 0;
  }

  for (set<int>::iterator iter = shards.begin(); iter != shards.end(); ++iter) {
    ldpp_dout(dpp, 20) << __func__ << "(): notifying mdlog change, shard_id=" << *iter << dendl;
  }

  notify_mgr.notify_all(dpp, store->svc.zone->get_zone_conn_map(), shards);

  return 0;
}

class RGWDataNotifier : public RGWRadosThread {
  RGWDataNotifierManager notify_mgr;
  bc::flat_set<rgw_data_notify_entry> entry;

  uint64_t interval_msec() override {
    return cct->_conf.get_val<int64_t>("rgw_data_notify_interval_msec");
  }
  void stop_process() override {
    notify_mgr.stop();
  }
public:
  RGWDataNotifier(RGWRados *_driver) : RGWRadosThread(_driver, "data-notifier"), notify_mgr(_driver) {}

  int process(const DoutPrefixProvider *dpp) override;
};

int RGWDataNotifier::process(const DoutPrefixProvider *dpp)
{
  auto data_log = store->svc.datalog_rados;
  if (!data_log) {
    return 0;
  }

  auto shards = data_log->read_clear_modified();

  if (shards.empty()) {
    return 0;
  }

  for (const auto& [shard_id, entries] : shards) {
    bc::flat_set<rgw_data_notify_entry>::iterator it;
    for (const auto& entry : entries) {
      ldpp_dout(dpp, 20) << __func__ << "(): notifying datalog change, shard_id="
        << shard_id << ":" << entry.gen << ":" << entry.key << dendl;
    }
  }

  notify_mgr.notify_all(dpp, store->svc.zone->get_zone_data_notify_to_map(), shards);

  return 0;
}

class RGWSyncProcessorThread : public RGWRadosThread {
public:
  RGWSyncProcessorThread(RGWRados *_driver, const string& thread_name = "radosgw") : RGWRadosThread(_driver, thread_name) {}
  RGWSyncProcessorThread(RGWRados *_driver) : RGWRadosThread(_driver) {}
  ~RGWSyncProcessorThread() override {}
  int init(const DoutPrefixProvider *dpp) override = 0 ;
  int process(const DoutPrefixProvider *dpp) override = 0;
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
  RGWMetaSyncProcessorThread(rgw::sal::RadosStore* _driver, RGWAsyncRadosProcessor *async_rados)
    : RGWSyncProcessorThread(_driver->getRados(), "meta-sync"), sync(_driver, async_rados) {}

  void wakeup_sync_shards(set<int>& shard_ids) {
    for (set<int>::iterator iter = shard_ids.begin(); iter != shard_ids.end(); ++iter) {
      sync.wakeup(*iter);
    }
  }
  RGWMetaSyncStatusManager* get_manager() { return &sync; }

  int init(const DoutPrefixProvider *dpp) override {
    int ret = sync.init(dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: sync.init() returned " << ret << dendl;
      return ret;
    }
    return 0;
  }

  int process(const DoutPrefixProvider *dpp) override {
    sync.run(dpp, null_yield);
    return 0;
  }
};

class RGWDataSyncProcessorThread : public RGWSyncProcessorThread
{
  PerfCountersRef counters;
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
  RGWDataSyncProcessorThread(rgw::sal::RadosStore* _driver, RGWAsyncRadosProcessor *async_rados,
                             const RGWZone* source_zone)
    : RGWSyncProcessorThread(_driver->getRados(), "data-sync"),
      counters(sync_counters::build(store->ctx(), std::string("data-sync-from-") + source_zone->name)),
      sync(_driver, async_rados, source_zone->id, counters.get()),
      initialized(false) {}

  void wakeup_sync_shards(bc::flat_map<int, bc::flat_set<rgw_data_notify_entry> >& entries) {
    for (bc::flat_map<int, bc::flat_set<rgw_data_notify_entry> >::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
      sync.wakeup(iter->first, iter->second);
    }
  }

  RGWDataSyncStatusManager* get_manager() { return &sync; }

  int init(const DoutPrefixProvider *dpp) override {
    return 0;
  }

  int process(const DoutPrefixProvider *dpp) override {
    while (!initialized) {
      if (going_down()) {
        return 0;
      }
      int ret = sync.init(dpp);
      if (ret >= 0) {
        initialized = true;
        break;
      }
      /* we'll be back! */
      return 0;
    }
    sync.run(dpp);
    return 0;
  }
};

class RGWSyncLogTrimThread : public RGWSyncProcessorThread, DoutPrefixProvider
{
  RGWCoroutinesManager crs;
  rgw::sal::RadosStore* store;
  rgw::BucketTrimManager *bucket_trim;
  RGWHTTPManager http;
  const utime_t trim_interval;

  uint64_t interval_msec() override { return 0; }
  void stop_process() override { crs.stop(); }
public:
  RGWSyncLogTrimThread(rgw::sal::RadosStore* store, rgw::BucketTrimManager *bucket_trim,
                       int interval)
    : RGWSyncProcessorThread(store->getRados(), "sync-log-trim"),
      crs(store->ctx(), store->getRados()->get_cr_registry()), store(store),
      bucket_trim(bucket_trim),
      http(store->ctx(), crs.get_completion_mgr()),
      trim_interval(interval, 0)
  {}

  int init(const DoutPrefixProvider *dpp) override {
    return http.start();
  }
  int process(const DoutPrefixProvider *dpp) override {
    list<RGWCoroutinesStack*> stacks;
    auto metatrimcr = create_meta_log_trim_cr(this, static_cast<rgw::sal::RadosStore*>(store), &http,
					      cct->_conf->rgw_md_log_max_shards,
					      trim_interval);
    if (!metatrimcr) {
      ldpp_dout(dpp, -1) << "Bailing out of trim thread!" << dendl;
      return -EINVAL;
    }
    auto meta = new RGWCoroutinesStack(store->ctx(), &crs);
    meta->call(metatrimcr);

    stacks.push_back(meta);

    if (store->svc()->zone->sync_module_exports_data()) {
      auto data = new RGWCoroutinesStack(store->ctx(), &crs);
      data->call(create_data_log_trim_cr(dpp, static_cast<rgw::sal::RadosStore*>(store), &http,
                                         cct->_conf->rgw_data_log_num_shards,
                                         trim_interval));
      stacks.push_back(data);

      auto bucket = new RGWCoroutinesStack(store->ctx(), &crs);
      bucket->call(bucket_trim->create_bucket_trim_cr(&http));
      stacks.push_back(bucket);
    }

    crs.run(dpp, stacks);
    return 0;
  }

  // implements DoutPrefixProvider
  CephContext *get_cct() const override { return store->ctx(); }
  unsigned get_subsys() const override
  {
    return dout_subsys;
  }

  std::ostream& gen_prefix(std::ostream& out) const override
  {
    return out << "sync log trim: ";
  }

};

void RGWRados::wakeup_meta_sync_shards(set<int>& shard_ids)
{
  std::lock_guard l{meta_sync_thread_lock};
  if (meta_sync_processor_thread) {
    meta_sync_processor_thread->wakeup_sync_shards(shard_ids);
  }
}

void RGWRados::wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, bc::flat_map<int, bc::flat_set<rgw_data_notify_entry> >& entries)
{
  ldpp_dout(dpp, 20) << __func__ << ": source_zone=" << source_zone << ", entries=" << entries << dendl;
  for (bc::flat_map<int, bc::flat_set<rgw_data_notify_entry> >::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
    ldpp_dout(dpp, 20) << __func__ << "(): updated shard=" << iter->first << dendl;
    bc::flat_set<rgw_data_notify_entry>& entries = iter->second;
    for (const auto& [key, gen] : entries) {
      ldpp_dout(dpp, 20) << __func__ << ": source_zone=" << source_zone << ", key=" << key
			 << ", gen=" << gen << dendl;
    }
  }

  std::lock_guard l{data_sync_thread_lock};
  auto iter = data_sync_processor_threads.find(source_zone);
  if (iter == data_sync_processor_threads.end()) {
    ldpp_dout(dpp, 10) << __func__ << ": couldn't find sync thread for zone " << source_zone << ", skipping async data sync processing" << dendl;
    return;
  }

  RGWDataSyncProcessorThread *thread = iter->second;
  ceph_assert(thread);
  thread->wakeup_sync_shards(entries);
}

RGWMetaSyncStatusManager* RGWRados::get_meta_sync_manager()
{
  std::lock_guard l{meta_sync_thread_lock};
  if (meta_sync_processor_thread) {
    return meta_sync_processor_thread->get_manager();
  }
  return nullptr;
}

RGWDataSyncStatusManager* RGWRados::get_data_sync_manager(const rgw_zone_id& source_zone)
{
  std::lock_guard l{data_sync_thread_lock};
  auto thread = data_sync_processor_threads.find(source_zone);
  if (thread == data_sync_processor_threads.end()) {
    return nullptr;
  }
  return thread->second->get_manager();
}

int RGWRados::get_required_alignment(const DoutPrefixProvider *dpp, const rgw_pool& pool, uint64_t *alignment)
{
  IoCtx ioctx;
  int r = open_pool_ctx(dpp, pool, ioctx, false, true);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: open_pool_ctx() returned " << r << dendl;
    return r;
  }

  bool req;
  r = ioctx.pool_requires_alignment2(&req);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: ioctx.pool_requires_alignment2() returned "
      << r << dendl;
    return r;
  }

  if (!req) {
    *alignment = 0;
    return 0;
  }

  uint64_t align;
  r = ioctx.pool_required_alignment2(&align);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: ioctx.pool_required_alignment2() returned "
      << r << dendl;
    return r;
  }
  if (align != 0) {
    ldpp_dout(dpp, 20) << "required alignment=" << align << dendl;
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

int RGWRados::get_max_chunk_size(const rgw_pool& pool, uint64_t *max_chunk_size, const DoutPrefixProvider *dpp, uint64_t *palignment)
{
  uint64_t alignment;
  int r = get_required_alignment(dpp, pool, &alignment);
  if (r < 0) {
    return r;
  }

  if (palignment) {
    *palignment = alignment;
  }

  uint64_t config_chunk_size = cct->_conf->rgw_max_chunk_size;

  get_max_aligned_size(config_chunk_size, alignment, max_chunk_size);

  ldpp_dout(dpp, 20) << "max_chunk_size=" << *max_chunk_size << dendl;

  return 0;
}

int RGWRados::get_max_chunk_size(const rgw_placement_rule& placement_rule, const rgw_obj& obj,
                                 uint64_t *max_chunk_size, const DoutPrefixProvider *dpp, uint64_t *palignment)
{
  rgw_pool pool;
  if (!get_obj_data_pool(placement_rule, obj, &pool)) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get data pool for object " << obj << dendl;
    return -EIO;
  }
  return get_max_chunk_size(pool, max_chunk_size, dpp, palignment);
}

void add_datalog_entry(const DoutPrefixProvider* dpp,
                       RGWDataChangesLog* datalog,
                       const RGWBucketInfo& bucket_info,
                       uint32_t shard_id, optional_yield y)
{
  const auto& logs = bucket_info.layout.logs;
  if (logs.empty()) {
    return;
  }
  int r = datalog->add_entry(dpp, bucket_info, logs.back(), shard_id, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed writing data log" << dendl;
  } // datalog error is not fatal
}

class RGWIndexCompletionManager;

struct complete_op_data {
  ceph::mutex lock = ceph::make_mutex("complete_op_data");
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
    std::lock_guard l{lock};
    stopped = true;
  }
};

class RGWIndexCompletionManager {
  RGWRados* const store;
  const uint32_t num_shards;
  ceph::containers::tiny_vector<ceph::mutex> locks;
  std::vector<set<complete_op_data*>> completions;
  std::vector<complete_op_data*> retry_completions;

  std::condition_variable cond;
  std::mutex retry_completions_lock;
  bool _stop{false};
  std::thread retry_thread;

  // used to distribute the completions and the locks they use across
  // their respective vectors; it will get incremented and can wrap
  // around back to 0 without issue
  std::atomic<uint32_t> cur_shard {0};

  void process();
  
  void add_completion(complete_op_data *completion);
  
  void stop() {
    if (retry_thread.joinable()) {
      _stop = true;
      cond.notify_all();
      retry_thread.join();
    }

    for (uint32_t i = 0; i < num_shards; ++i) {
      std::lock_guard l{locks[i]};
      for (auto c : completions[i]) {
        c->stop();
      }
    }
    completions.clear();
  }
  
  uint32_t next_shard() {
    return cur_shard++ % num_shards;
  }

public:
  RGWIndexCompletionManager(RGWRados *_driver) :
    store(_driver),
    num_shards(store->ctx()->_conf->rgw_thread_pool_size),
    locks{ceph::make_lock_container<ceph::mutex>(
      num_shards,
      [](const size_t i) {
        return ceph::make_mutex("RGWIndexCompletionManager::lock::" +
				std::to_string(i));
      })},
    completions(num_shards),
    retry_thread(&RGWIndexCompletionManager::process, this)
    {}

  ~RGWIndexCompletionManager() {
    stop();
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

  CephContext* ctx() {
    return store->ctx();
  }
};

static void obj_complete_cb(completion_t cb, void *arg)
{
  complete_op_data *completion = reinterpret_cast<complete_op_data*>(arg);
  completion->lock.lock();
  if (completion->stopped) {
    completion->lock.unlock(); /* can drop lock, no one else is referencing us */
    delete completion;
    return;
  }
  bool need_delete = completion->manager->handle_completion(cb, completion);
  completion->lock.unlock();
  if (need_delete) {
    delete completion;
  }
}

void RGWIndexCompletionManager::process()
{
  DoutPrefix dpp(store->ctx(), dout_subsys, "rgw index completion thread: ");
  while(!_stop) {
    std::vector<complete_op_data*> comps;

    {
      std::unique_lock l{retry_completions_lock};
      cond.wait(l, [this](){return _stop || !retry_completions.empty();});
      if (_stop) {
        return;
      }
      retry_completions.swap(comps);
    }

    for (auto c : comps) {
      std::unique_ptr<complete_op_data> up{c};

      ldpp_dout(&dpp, 20) << __func__ << "(): handling completion for key=" << c->key << dendl;

      RGWRados::BucketShard bs(store);
      RGWBucketInfo bucket_info;

      int r = bs.init(c->obj.bucket, c->obj, &bucket_info, &dpp, null_yield);
      if (r < 0) {
        ldpp_dout(&dpp, 0) << "ERROR: " << __func__ << "(): failed to initialize BucketShard, obj=" << c->obj << " r=" << r << dendl;
        /* not much to do */
        continue;
      }

      r = store->guard_reshard(&dpp, &bs, c->obj, bucket_info,
			     [&](RGWRados::BucketShard *bs) -> int {
			       const bool bitx = ctx()->_conf->rgw_bucket_index_transaction_instrumentation;
			       ldout_bitx(bitx, &dpp, 10) <<
				 "ENTERING " << __func__ << ": bucket-shard=" << bs <<
				 " obj=" << c->obj << " tag=" << c->tag <<
				 " op=" << c->op << ", remove_objs=" << c->remove_objs << dendl_bitx;
			       ldout_bitx(bitx, &dpp, 25) <<
				 "BACKTRACE: " << __func__ << ": " << ClibBackTrace(1) << dendl_bitx;

			       librados::ObjectWriteOperation o;
			       o.assert_exists();
			       cls_rgw_guard_bucket_resharding(o, -ERR_BUSY_RESHARDING);
			       cls_rgw_bucket_complete_op(o, c->op, c->tag, c->ver, c->key, c->dir_meta, &c->remove_objs,
							  c->log_op, c->bilog_op, &c->zones_trace);
			       int ret = bs->bucket_obj.operate(&dpp, &o, null_yield);
			       ldout_bitx(bitx, &dpp, 10) <<
				 "EXITING " << __func__ << ": ret=" << dendl_bitx;
			       return ret;
                             }, null_yield);
      if (r < 0) {
        ldpp_dout(&dpp, 0) << "ERROR: " << __func__ << "(): bucket index completion failed, obj=" << c->obj << " r=" << r << dendl;
        /* ignoring error, can't do anything about it */
        continue;
      }

      if (c->log_op) {
        // This null_yield can stay, for now, since we're in our own thread
        add_datalog_entry(&dpp, store->svc.datalog_rados, bucket_info,
                          bs.shard_id, null_yield);
      }
    }
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
    entry->zones_trace.insert(store->svc.zone->get_zone().id, obj.bucket.get_key());
  }

  *result = entry;

  entry->rados_completion = librados::Rados::aio_create_completion(entry, obj_complete_cb);

  std::lock_guard l{locks[shard_id]};
  const auto ok = completions[shard_id].insert(entry).second;
  ceph_assert(ok);
}

void RGWIndexCompletionManager::add_completion(complete_op_data *completion) {
  {
    std::lock_guard l{retry_completions_lock};
    retry_completions.push_back(completion);
  }
  cond.notify_all();
}

bool RGWIndexCompletionManager::handle_completion(completion_t cb, complete_op_data *arg)
{
  int shard_id = arg->manager_shard_id;
  {
    std::lock_guard l{locks[shard_id]};

    auto& comps = completions[shard_id];

    auto iter = comps.find(arg);
    if (iter == comps.end()) {
      ldout(arg->manager->ctx(), 0) << __func__ << "(): cannot find completion for obj=" << arg->key << dendl;
      return true;
    }

    comps.erase(iter);
  }

  int r = rados_aio_get_return_value(cb);
  if (r != -ERR_BUSY_RESHARDING) {
    ldout(arg->manager->ctx(), 20) << __func__ << "(): completion " << 
      (r == 0 ? "ok" : "failed with " + to_string(r)) << 
      " for obj=" << arg->key << dendl;
    return true;
  }
  add_completion(arg);
  ldout(arg->manager->ctx(), 20) << __func__ << "(): async completion added for obj=" << arg->key << dendl;
  return false;
}

void RGWRados::finalize()
{
  /* Before joining any sync threads, drain outstanding requests &
   * mark the async_processor as going_down() */
  if (svc.async_processor) {
    svc.async_processor->stop();
  }

  if (run_sync_thread) {
    std::lock_guard l{meta_sync_thread_lock};
    meta_sync_processor_thread->stop();

    std::lock_guard dl{data_sync_thread_lock};
    for (auto iter : data_sync_processor_threads) {
      RGWDataSyncProcessorThread *thread = iter.second;
      thread->stop();
    }
    if (sync_log_trimmer) {
      sync_log_trimmer->stop();
    }
  }
  if (run_sync_thread) {
    // https://github.com/ceph/ceph/commit/9106e8ad3121a503bf49a7334367849756bd1291
    // coverity[missing_lock:SUPPRESS]
    delete meta_sync_processor_thread;
    meta_sync_processor_thread = NULL;
    std::lock_guard dl{data_sync_thread_lock};
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
  delete sync_tracer;
  
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

  delete binfo_cache;
  delete obj_tombstone_cache;
  delete topic_cache;
  if (d3n_data_cache)
    delete d3n_data_cache;

  if (reshard_wait.get()) {
    reshard_wait->stop();
    reshard_wait.reset();
  }

  if (run_reshard_thread) {
    reshard->stop_processor();
  }
  delete reshard;
  delete index_completion_manager;

  if (run_notification_thread) {
    rgw::notify::shutdown();
  }
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_rados()
{
  int ret = 0;

  ret = rados.init_with_context(cct);
  if (ret < 0) {
    return ret;
  }
  ret = rados.connect();
  if (ret < 0) {
    return ret;
  }

  auto crs = std::unique_ptr<RGWCoroutinesManagerRegistry>{
    new RGWCoroutinesManagerRegistry(cct)};
  ret = crs->hook_to_admin_command("cr dump");
  if (ret < 0) {
    return ret;
  }

  cr_registry = crs.release();

  if (use_datacache) {
    d3n_data_cache = new D3nDataCache();
    d3n_data_cache->init(cct);
  }

  return ret;
}

int RGWRados::register_to_service_map(const DoutPrefixProvider *dpp, const string& daemon_type, const map<string, string>& meta)
{
  string name = cct->_conf->name.get_id();
  if (name.compare(0, 4, "rgw.") == 0) {
    name = name.substr(4);
  }
  map<string,string> metadata = meta;
  metadata["num_handles"] = "1"s;
  metadata["zonegroup_id"] = svc.zone->get_zonegroup().get_id();
  metadata["zonegroup_name"] = svc.zone->get_zonegroup().get_name();
  metadata["zone_name"] = svc.zone->zone_name();
  metadata["zone_id"] = svc.zone->zone_id().id;
  metadata["realm_name"] = svc.zone->get_realm().get_name();
  metadata["realm_id"] = svc.zone->get_realm().get_id();
  metadata["id"] = name;
  int ret = rados.service_daemon_register(
    daemon_type,
    stringify(rados.get_instance_id()),
    metadata);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: service_daemon_register() returned ret=" << ret << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::update_service_map(const DoutPrefixProvider *dpp, std::map<std::string, std::string>&& status)
{
  int ret = rados.service_daemon_update_status(std::move(status));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: service_daemon_update_status() returned ret=" << ret << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

/**
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_complete(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret;

  /*
   * create sync module instance even if we don't run sync thread, might need it for radosgw-admin
   */
  sync_module = svc.sync_modules->get_sync_module();

  ret = open_root_pool_ctx(dpp);
  if (ret < 0)
    return ret;

  ret = open_gc_pool_ctx(dpp);
  if (ret < 0)
    return ret;

  ret = open_lc_pool_ctx(dpp);
  if (ret < 0)
    return ret;

  ret = open_objexp_pool_ctx(dpp);
  if (ret < 0)
    return ret;

  ret = open_reshard_pool_ctx(dpp);
  if (ret < 0)
    return ret;

  ret = open_notif_pool_ctx(dpp);
  if (ret < 0)
    return ret;

  ret = open_topics_pool_ctx(dpp);
  if (ret < 0)
    return ret;

  pools_initialized = true;

  if (use_gc) {
    gc = new RGWGC();
    gc->initialize(cct, this, y);
  } else {
    ldpp_dout(dpp, 5) << "note: GC not initialized" << dendl;
  }

  obj_expirer = new RGWObjectExpirer(this->driver);

  if (use_gc_thread && use_gc) {
    gc->start_processor();
    obj_expirer->start_processor();
  }

  auto& current_period = svc.zone->get_current_period();
  auto& zonegroup = svc.zone->get_zonegroup();
  auto& zone_params = svc.zone->get_zone_params();
  auto& zone = svc.zone->get_zone();

  /* no point of running sync thread if we don't have a master zone configured
    or there is no rest_master_conn */
  if (!svc.zone->need_to_sync()) {
    run_sync_thread = false;
  }

  if (svc.zone->is_meta_master()) {
    auto md_log = svc.mdlog->get_log(current_period.get_id());
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
        ldpp_dout(dpp, 0) << "WARNING: This zone does not contain the placement target "
                      << pt.second.name << " present in zonegroup" << dendl;
      }
    }
    auto async_processor = svc.async_processor;
    std::lock_guard l{meta_sync_thread_lock};
    meta_sync_processor_thread = new RGWMetaSyncProcessorThread(this->driver, async_processor);
    ret = meta_sync_processor_thread->init(dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to initialize meta sync thread" << dendl;
      return ret;
    }
    meta_sync_processor_thread->start();

    // configure the bucket trim manager
    rgw::BucketTrimConfig config;
    rgw::configure_bucket_trim(cct, config);

    bucket_trim.emplace(this->driver, config);
    ret = bucket_trim->init();
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to start bucket trim manager" << dendl;
      return ret;
    }
    svc.datalog_rados->set_observer(&*bucket_trim);

    std::lock_guard dl{data_sync_thread_lock};
    for (auto source_zone : svc.zone->get_data_sync_source_zones()) {
      ldpp_dout(dpp, 5) << "starting data sync thread for zone " << source_zone->name << dendl;
      auto *thread = new RGWDataSyncProcessorThread(this->driver, svc.async_processor, source_zone);
      ret = thread->init(dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to initialize data sync thread" << dendl;
        return ret;
      }
      thread->start();
      data_sync_processor_threads[rgw_zone_id(source_zone->id)] = thread;
    }
    auto interval = cct->_conf->rgw_sync_log_trim_interval;
    if (interval > 0) {
      sync_log_trimmer = new RGWSyncLogTrimThread(this->driver, &*bucket_trim, interval);
      ret = sync_log_trimmer->init(dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to initialize sync log trim thread" << dendl;
        return ret;
      }
      sync_log_trimmer->start();
    }
  }
  if (cct->_conf->rgw_data_notify_interval_msec) {
    data_notifier = new RGWDataNotifier(this);
    data_notifier->start();
  }

  binfo_cache = new RGWChainedCacheImpl<bucket_info_entry>;
  binfo_cache->init(svc.cache);

  topic_cache = new RGWChainedCacheImpl<pubsub_bucket_topics_entry>;
  topic_cache->init(svc.cache);

  lc = new RGWLC();
  lc->initialize(cct, this->driver);

  if (use_lc_thread)
    lc->start_processor();

  quota_handler = RGWQuotaHandler::generate_handler(dpp, this->driver, quota_threads);

  bucket_index_max_shards = (cct->_conf->rgw_override_bucket_index_max_shards ? cct->_conf->rgw_override_bucket_index_max_shards :
                             zone.bucket_index_max_shards);
  if (bucket_index_max_shards > get_max_bucket_shards()) {
    bucket_index_max_shards = get_max_bucket_shards();
    ldpp_dout(dpp, 1) << __func__ << " bucket index max shards is too large, reset to value: "
      << get_max_bucket_shards() << dendl;
  }
  ldpp_dout(dpp, 20) << __func__ << " bucket index max shards: " << bucket_index_max_shards << dendl;

  bool need_tombstone_cache = !svc.zone->get_zone_data_notify_to_map().empty(); /* have zones syncing from us */

  if (need_tombstone_cache) {
    obj_tombstone_cache = new tombstone_cache_t(cct->_conf->rgw_obj_tombstone_cache_size);
  }

  reshard_wait = std::make_shared<RGWReshardWait>();

  reshard = new RGWReshard(this->driver);

  // disable reshard thread based on zone/zonegroup support
  run_reshard_thread = run_reshard_thread && svc.zone->can_reshard();

  if (run_reshard_thread)  {
    reshard->start_processor();
  }

  index_completion_manager = new RGWIndexCompletionManager(this);

  if (run_notification_thread) {
    ret = rgw::notify::init(cct, driver, *svc.site, dpp);
    if (ret < 0 ) {
      ldpp_dout(dpp, 1) << "ERROR: failed to initialize notification manager" << dendl;
    }
  }

  return ret;
}

int RGWRados::init_svc(bool raw, const DoutPrefixProvider *dpp,
                       const rgw::SiteConfig& site)
{
  if (raw) {
    return svc.init_raw(cct, driver, use_cache, null_yield, dpp, site);
  }

  return svc.init(cct, driver, use_cache, run_sync_thread, null_yield, dpp, site);
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_begin(CephContext* _cct, const DoutPrefixProvider *dpp,
                         const rgw::SiteConfig& site)
{
  set_context(_cct);
  int ret = driver->init_neorados(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize neorados (ret=" << cpp_strerror(-ret) << ")" << dendl;
    return ret;
  }
  ret = init_rados();
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize librados (ret=" << cpp_strerror(-ret) << ")" << dendl;
    return ret;
  }

  ret = init_svc(false, dpp, site);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
    return ret;
  }

  ret = ctl.init(&svc, driver, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to init ctls (ret=" << cpp_strerror(-ret) << ")" << dendl;
    return ret;
  }

  host_id = svc.zone_utils->gen_host_id();

  return 0;
}

/**
 * Open the pool used as root for this gateway
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::open_root_pool_ctx(const DoutPrefixProvider *dpp)
{
  return rgw_init_ioctx(dpp, get_rados_handle(), svc.zone->get_zone_params().domain_root, root_pool_ctx, true, true);
}

int RGWRados::open_gc_pool_ctx(const DoutPrefixProvider *dpp)
{
  return rgw_init_ioctx(dpp, get_rados_handle(), svc.zone->get_zone_params().gc_pool, gc_pool_ctx, true, true);
}

int RGWRados::open_lc_pool_ctx(const DoutPrefixProvider *dpp)
{
  return rgw_init_ioctx(dpp, get_rados_handle(), svc.zone->get_zone_params().lc_pool, lc_pool_ctx, true, true);
}

int RGWRados::open_objexp_pool_ctx(const DoutPrefixProvider *dpp)
{
  return rgw_init_ioctx(dpp, get_rados_handle(), svc.zone->get_zone_params().log_pool, objexp_pool_ctx, true, true);
}

int RGWRados::open_reshard_pool_ctx(const DoutPrefixProvider *dpp)
{
  return rgw_init_ioctx(dpp, get_rados_handle(), svc.zone->get_zone_params().reshard_pool, reshard_pool_ctx, true, true);
}

int RGWRados::open_notif_pool_ctx(const DoutPrefixProvider *dpp)
{
  return rgw_init_ioctx(dpp, get_rados_handle(), svc.zone->get_zone_params().notif_pool, notif_pool_ctx, true, true);
}

int RGWRados::open_topics_pool_ctx(const DoutPrefixProvider* dpp) {
  return rgw_init_ioctx(dpp, get_rados_handle(),
                        svc.zone->get_zone_params().topics_pool,
                        topics_pool_ctx, true, true);
}

int RGWRados::open_pool_ctx(const DoutPrefixProvider *dpp, const rgw_pool& pool, librados::IoCtx& io_ctx,
			    bool mostly_omap, bool bulk)
{
  constexpr bool create = true; // create the pool if it doesn't exist
  return rgw_init_ioctx(dpp, get_rados_handle(), pool, io_ctx, create, mostly_omap, bulk);
}

/**** logs ****/

struct log_list_state {
  string prefix;
  librados::IoCtx io_ctx;
  librados::NObjectIterator obit;
};

int RGWRados::log_list_init(const DoutPrefixProvider *dpp, const string& prefix, RGWAccessHandle *handle)
{
  log_list_state *state = new log_list_state;
  int r = rgw_init_ioctx(dpp, get_rados_handle(), svc.zone->get_zone_params().log_pool, state->io_ctx);
  if (r < 0) {
    delete state;
    return r;
  }
  try {
    state->prefix = prefix;
    state->obit = state->io_ctx.nobjects_begin();
    *handle = (RGWAccessHandle)state;
    return 0;
  } catch (const std::system_error& e) {
    r = -e.code().value();
    ldpp_dout(dpp, 10) << "nobjects_begin threw " << e.what()
       << ", returning " << r << dendl;
    return r;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 10) << "nobjects_begin threw " << e.what()
       << ", returning -5" << dendl;
    return -EIO;
  }
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

int RGWRados::log_remove(const DoutPrefixProvider *dpp, const string& name)
{
  librados::IoCtx io_ctx;
  int r = rgw_init_ioctx(dpp, get_rados_handle(), svc.zone->get_zone_params().log_pool, io_ctx);
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

int RGWRados::log_show_init(const DoutPrefixProvider *dpp, const string& name, RGWAccessHandle *handle)
{
  log_show_state *state = new log_show_state;
  int r = rgw_init_ioctx(dpp, get_rados_handle(), svc.zone->get_zone_params().log_pool, state->io_ctx);
  if (r < 0) {
    delete state;
    return r;
  }
  state->name = name;
  *handle = (RGWAccessHandle)state;
  return 0;
}

int RGWRados::log_show_next(const DoutPrefixProvider *dpp, RGWAccessHandle handle, rgw_log_entry *entry)
{
  log_show_state *state = static_cast<log_show_state *>(handle);
  off_t off = state->p.get_off();

  ldpp_dout(dpp, 10) << "log_show_next pos " << state->pos << " bl " << state->bl.length()
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
    state->bl = std::move(old);
    state->bl.claim_append(more);
    state->p = state->bl.cbegin();
    if ((unsigned)r < chunk)
      state->eof = true;
    ldpp_dout(dpp, 10) << " read " << r << dendl;
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

int RGWRados::log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info, optional_yield y)
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
      ldpp_dout(dpp, 0) << "WARNING: RGWRados::log_usage(): user name empty (bucket=" << ub.bucket << "), skipping" << dendl;
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
    int r = cls_obj_usage_log_add(dpp, liter->first, liter->second, y);
    if (r < 0)
      return r;
  }
  return 0;
}

int RGWRados::read_usage(const DoutPrefixProvider *dpp, const rgw_user& user, const string& bucket_name, uint64_t start_epoch, uint64_t end_epoch,
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

    int ret =  cls_obj_usage_log_read(dpp, hash, user_str, bucket_name, start_epoch, end_epoch, num,
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

int RGWRados::trim_usage(const DoutPrefixProvider *dpp, const rgw_user& user, const string& bucket_name, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
{
  uint32_t index = 0;
  string hash, first_hash;
  string user_str = user.to_str();
  usage_log_hash(cct, user_str, first_hash, index);

  hash = first_hash;
  do {
    int ret =  cls_obj_usage_log_trim(dpp, hash, user_str, bucket_name, start_epoch, end_epoch, y);

    if (ret < 0 && ret != -ENOENT)
      return ret;

    usage_log_hash(cct, user_str, hash, ++index);
  } while (hash != first_hash);

  return 0;
}


int RGWRados::clear_usage(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto max_shards = cct->_conf->rgw_usage_max_shards;
  int ret=0;
  for (unsigned i=0; i < max_shards; i++){
    string oid = RGW_USAGE_OBJ_PREFIX + to_string(i);
    ret = cls_obj_usage_log_clear(dpp, oid, y);
    if (ret < 0){
      ldpp_dout(dpp,0) << "usage clear on oid="<< oid << "failed with ret=" << ret << dendl;
      return ret;
    }
  }
  return ret;
}

int RGWRados::decode_policy(const DoutPrefixProvider *dpp,
			    ceph::buffer::list& bl,
			    ACLOwner *owner)
{
  auto i = bl.cbegin();
  RGWAccessControlPolicy policy;
  try {
    policy.decode_owner(i);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  *owner = policy.get_owner();
  return 0;
}

int RGWRados::Bucket::update_bucket_id(const string& new_bucket_id, const DoutPrefixProvider *dpp, optional_yield y)
{
  rgw_bucket bucket = bucket_info.bucket;
  bucket.update_bucket_id(new_bucket_id);

  bucket_info.objv_tracker.clear();
  int ret = store->get_bucket_instance_info(bucket, bucket_info, nullptr, nullptr, y, dpp);
  if (ret < 0) {
    return ret;
  }

  return 0;
}


/**
 * Get ordered listing of the objects in a bucket.
 *
 * max_p: maximum number of results to return
 * bucket: bucket to list contents of
 * prefix: only return results that match this prefix
 * delim: do not include results that match this string.
 *     Any skipped results will have the matching portion of their name
 *     inserted in common_prefixes with a "true" mark.
 * marker: if filled in, begin the listing with this object.
 * end_marker: if filled in, end the listing with this object.
 * result: the objects are put in here.
 * common_prefixes: if delim is filled in, any matching prefixes are
 * placed here.
 * is_truncated: if number of objects in the bucket is bigger than
 * max, then truncated.
 */
int RGWRados::Bucket::List::list_objects_ordered(
  const DoutPrefixProvider *dpp,
  int64_t max_p,
  std::vector<rgw_bucket_dir_entry> *result,
  std::map<std::string, bool> *common_prefixes,
  bool *is_truncated,
  optional_yield y)
{
  RGWRados *store = target->get_store();
  CephContext *cct = store->ctx();
  int shard_id = target->get_shard_id();
  const auto& current_index = target->get_bucket_info().layout.current_index;

  int count = 0;
  bool truncated = true;
  bool cls_filtered = false;
  const int64_t max = // protect against memory issues and negative vals
    std::min(bucket_list_objects_absolute_max, std::max(int64_t(0), max_p));
  int read_ahead = std::max(cct->_conf->rgw_list_bucket_min_readahead, max);

  result->clear();

  // use a local marker; either the marker will have a previous entry
  // or it will be empty; either way it's OK to copy
  rgw_obj_key marker_obj(params.marker.name,
			 params.marker.instance,
			 params.ns.empty() ? params.marker.ns : params.ns);
  rgw_obj_index_key cur_marker;
  marker_obj.get_index_key(&cur_marker);

  rgw_obj_key end_marker_obj(params.end_marker.name,
			     params.end_marker.instance,
			     params.ns.empty() ? params.end_marker.ns : params.ns);
  rgw_obj_index_key cur_end_marker;
  end_marker_obj.get_index_key(&cur_end_marker);
  const bool cur_end_marker_valid = !params.end_marker.empty();

  rgw_obj_key prefix_obj(params.prefix);
  prefix_obj.set_ns(params.ns);
  std::string cur_prefix = prefix_obj.get_index_key_name();
  std::string after_delim_s; /* needed in !params.delim.empty() AND later */

  if (!params.delim.empty()) {
    after_delim_s = cls_rgw_after_delim(params.delim);
    /* if marker points at a common prefix, fast forward it into its
     * upper bound string */
    int delim_pos = cur_marker.name.find(params.delim, cur_prefix.size());
    if (delim_pos >= 0) {
      string s = cur_marker.name.substr(0, delim_pos);
      s.append(after_delim_s);
      cur_marker = s;
    }
  }

  // we'll stop after this many attempts as long we return at least
  // one entry; but we will also go beyond this number of attempts
  // until we return at least one entry
  constexpr uint16_t SOFT_MAX_ATTEMPTS = 8;

  rgw_obj_index_key prev_marker;
  for (uint16_t attempt = 1; /* empty */; ++attempt) {
    ldpp_dout(dpp, 20) << __func__ <<
      ": starting attempt " << attempt << dendl;

    if (attempt > 1 && !(prev_marker < cur_marker)) {
      // we've failed to make forward progress
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	" marker failed to make forward progress; attempt=" << attempt <<
	", prev_marker=" << prev_marker <<
	", cur_marker=" << cur_marker << dendl;
      break;
    }
    prev_marker = cur_marker;

    ent_map_t ent_map;
    ent_map.reserve(read_ahead);
    int r = store->cls_bucket_list_ordered(dpp,
                                           target->get_bucket_info(),
                                           current_index,
                                           shard_id,
					   cur_marker,
					   cur_prefix,
					   params.delim,
					   read_ahead + 1 - count,
					   params.list_versions,
					   attempt,
					   ent_map,
					   &truncated,
					   &cls_filtered,
					   &cur_marker,
                                           y,
					   params.force_check_filter);
    if (r < 0) {
      return r;
    }

    for (auto eiter = ent_map.begin(); eiter != ent_map.end(); ++eiter) {
      rgw_bucket_dir_entry& entry = eiter->second;
      rgw_obj_index_key index_key = entry.key;
      rgw_obj_key obj(index_key);

      ldpp_dout(dpp, 20) << __func__ <<
	": considering entry " << entry.key << dendl;

      /* note that parse_raw_oid() here will not set the correct
       * object's instance, as rgw_obj_index_key encodes that
       * separately. We don't need to set the instance because it's
       * not needed for the checks here and we end up using the raw
       * entry for the return vector
       */
      bool valid = rgw_obj_key::parse_raw_oid(index_key.name, &obj);
      if (!valid) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	  " could not parse object name: " << obj.name << dendl;
        continue;
      }

      bool matched_ns = (obj.ns == params.ns);
      if (!params.list_versions && !entry.is_visible()) {
        ldpp_dout(dpp, 10) << __func__ <<
	  ": skipping not visible entry \"" << entry.key << "\"" << dendl;
        continue;
      }

      if (params.enforce_ns && !matched_ns) {
        if (!params.ns.empty()) {
          /* we've iterated past the namespace we're searching -- done now */
          truncated = false;
	  ldpp_dout(dpp, 10) << __func__ <<
	    ": finished due to getting past requested namespace \"" <<
	    params.ns << "\"" << dendl;
          goto done;
        }

        /* we're skipping past namespaced objects */
	ldpp_dout(dpp, 20) << __func__ <<
	  ": skipping past namespaced objects, including \"" << entry.key <<
	  "\"" << dendl;
        continue;
      }

      if (cur_end_marker_valid && cur_end_marker <= index_key) {
        truncated = false;
	ldpp_dout(dpp, 10) << __func__ <<
	  ": finished due to getting end marker of \"" << cur_end_marker <<
	  "\" with \"" << entry.key << "\"" << dendl;
        goto done;
      }

      if (count < max) {
	params.marker = index_key;
	next_marker = index_key;
      }

      if (params.access_list_filter &&
	  !params.access_list_filter(obj.name, index_key.name)) {
	ldpp_dout(dpp, 20) << __func__ <<
	  ": skipping past namespaced objects, including \"" << entry.key <<
	  "\"" << dendl;
        continue;
      }

      if (params.prefix.size() &&
	  0 != obj.name.compare(0, params.prefix.size(), params.prefix)) {
	ldpp_dout(dpp, 20) << __func__ <<
	  ": skipping object \"" << entry.key <<
	  "\" that doesn't match prefix \"" << params.prefix << "\"" << dendl;
        continue;
      }

      if (!params.delim.empty()) {
	const int delim_pos = obj.name.find(params.delim, params.prefix.size());
	if (delim_pos >= 0) {
	  // run either the code where delimiter filtering is done a)
	  // in the OSD/CLS or b) here.
	  if (cls_filtered) {
	    // NOTE: this condition is for the newer versions of the
	    // OSD that does filtering on the CLS side should only
	    // find one delimiter at the end if it finds any after the
	    // prefix
	    if (delim_pos !=
		int(obj.name.length() - params.delim.length())) {
	      ldpp_dout(dpp, 0) << "WARNING: " << __func__ <<
		" found delimiter in place other than the end of "
		"the prefix; obj.name=" << obj.name <<
		", prefix=" << params.prefix << dendl;
	    }
	    if (common_prefixes) {
	      if (count >= max) {
		truncated = true;
		ldpp_dout(dpp, 10) << __func__ <<
		  ": stopping early with common prefix \"" << entry.key <<
		  "\" because requested number (" << max <<
		  ") reached (cls filtered)" << dendl;
		goto done;
	      }

	      (*common_prefixes)[obj.name] = true;
	      count++;
	    }

	    ldpp_dout(dpp, 20) << __func__ <<
	      ": finished entry with common prefix \"" << entry.key <<
	      "\" so continuing loop (cls filtered)" << dendl;
	    continue;
	  } else {
	    // NOTE: this condition is for older versions of the OSD
	    // that do not filter on the CLS side, so the following code
	    // must do the filtering; once we reach version 16 of ceph,
	    // this code can be removed along with the conditional that
	    // can lead this way

	    /* extract key -with trailing delimiter- for CommonPrefix */
	    string prefix_key =
	      obj.name.substr(0, delim_pos + params.delim.length());

	    if (common_prefixes &&
		common_prefixes->find(prefix_key) == common_prefixes->end()) {
	      if (count >= max) {
		truncated = true;
		ldpp_dout(dpp, 10) << __func__ <<
		  ": stopping early with common prefix \"" << entry.key <<
		  "\" because requested number (" << max <<
		  ") reached (not cls filtered)" << dendl;
		goto done;
	      }
	      next_marker = prefix_key;
	      (*common_prefixes)[prefix_key] = true;

	      count++;
	    }

	    ldpp_dout(dpp, 20) << __func__ <<
	      ": finished entry with common prefix \"" << entry.key <<
	      "\" so continuing loop (not cls filtered)" << dendl;
	    continue;
	  } // if we're running an older OSD version
	} // if a delimiter was found after prefix
      } // if a delimiter was passed in

      if (count >= max) {
        truncated = true;
	ldpp_dout(dpp, 10) << __func__ <<
	  ": stopping early with entry \"" << entry.key <<
	  "\" because requested number (" << max <<
	  ") reached" << dendl;
        goto done;
      }

      ldpp_dout(dpp, 20) << __func__ <<
	": adding entry " << entry.key << " to result" << dendl;

      result->emplace_back(std::move(entry));
      count++;
    } // eiter for loop

    // NOTE: the following conditional is needed by older versions of
    // the OSD that don't do delimiter filtering on the CLS side; once
    // we reach version 16 of ceph, the following conditional and the
    // code within can be removed
    if (!cls_filtered && !params.delim.empty()) {
      int marker_delim_pos =
	cur_marker.name.find(params.delim, cur_prefix.size());
      if (marker_delim_pos >= 0) {
	std::string skip_after_delim =
	  cur_marker.name.substr(0, marker_delim_pos);
        skip_after_delim.append(after_delim_s);

        ldpp_dout(dpp, 20) << __func__ <<
	  ": skip_after_delim=" << skip_after_delim << dendl;

        if (skip_after_delim > cur_marker.name) {
          cur_marker = skip_after_delim;
          ldpp_dout(dpp, 20) << __func__ <<
	    ": setting cur_marker=" << cur_marker.name <<
	    "[" << cur_marker.instance << "]" << dendl;
        }
      }
    } // if older osd didn't do delimiter filtering

    ldpp_dout(dpp, 10) << __func__ <<
      ": end of outer loop, truncated=" << truncated <<
      ", count=" << count << ", attempt=" << attempt << dendl;

    if (!truncated || count >= (max + 1) / 2) {
      // if we finished listing, or if we're returning at least half the
      // requested entries, that's enough; S3 and swift protocols allow
      // returning fewer than max entries
      ldpp_dout(dpp, 10) << __func__ <<
	": exiting attempt loop because we reached end (" << truncated <<
	") or we're returning half the requested entries (" << count <<
	" of " << max << ")" << dendl;
      break;
    } else if (attempt > SOFT_MAX_ATTEMPTS && count >= 1) {
      // if we've made at least 8 attempts and we have some, but very
      // few, results, return with what we have
      ldpp_dout(dpp, 10) << __func__ <<
	": exiting attempt loop because we made " << attempt <<
	" attempts and we're returning " << count << " entries" << dendl;
      break;
    }
  } // for (uint16_t attempt...

done:

  if (is_truncated) {
    *is_truncated = truncated;
  }

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
int RGWRados::Bucket::List::list_objects_unordered(const DoutPrefixProvider *dpp,
                                                   int64_t max_p,
						   std::vector<rgw_bucket_dir_entry>* result,
						   std::map<std::string, bool>* common_prefixes,
						   bool* is_truncated,
                                                   optional_yield y)
{
  RGWRados *store = target->get_store();
  int shard_id = target->get_shard_id();
  const auto& current_index = target->get_bucket_info().layout.current_index;

  int count = 0;
  bool truncated = true;

  const int64_t max = // protect against memory issues and negative vals
    std::min(bucket_list_objects_absolute_max, std::max(int64_t(0), max_p));

  // read a few extra in each call to cls_bucket_list_unordered in
  // case some are filtered out due to namespace matching, versioning,
  // filtering, etc.
  const int64_t max_read_ahead = 100;
  const uint32_t read_ahead = uint32_t(max + std::min(max, max_read_ahead));

  result->clear();

  // use a local marker; either the marker will have a previous entry
  // or it will be empty; either way it's OK to copy
  rgw_obj_key marker_obj(params.marker.name,
			 params.marker.instance,
			 params.ns.empty() ? params.marker.ns : params.ns);
  rgw_obj_index_key cur_marker;
  marker_obj.get_index_key(&cur_marker);

  rgw_obj_key end_marker_obj(params.end_marker.name,
			     params.end_marker.instance,
			     params.ns.empty() ? params.end_marker.ns : params.ns);
  rgw_obj_index_key cur_end_marker;
  end_marker_obj.get_index_key(&cur_end_marker);
  const bool cur_end_marker_valid = !params.end_marker.empty();

  rgw_obj_key prefix_obj(params.prefix);
  prefix_obj.set_ns(params.ns);
  std::string cur_prefix = prefix_obj.get_index_key_name();

  while (truncated && count <= max) {
    std::vector<rgw_bucket_dir_entry> ent_list;
    ent_list.reserve(read_ahead);

    int r = store->cls_bucket_list_unordered(dpp,
                                             target->get_bucket_info(),
                                             current_index,
                                             shard_id,
					     cur_marker,
					     cur_prefix,
					     read_ahead,
					     params.list_versions,
					     ent_list,
					     &truncated,
					     &cur_marker,
                                             y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	" cls_bucket_list_unordered returned " << r << " for " <<
	target->get_bucket_info().bucket << dendl;
      return r;
    }

    // NB: while regions of ent_list will be sorted, we have no
    // guarantee that all items will be sorted since they can cross
    // shard boundaries

    for (auto& entry : ent_list) {
      rgw_obj_index_key index_key = entry.key;
      rgw_obj_key obj(index_key);

      if (count < max) {
	params.marker.set(index_key);
	next_marker.set(index_key);
      }

      /* note that parse_raw_oid() here will not set the correct
       * object's instance, as rgw_obj_index_key encodes that
       * separately. We don't need to set the instance because it's
       * not needed for the checks here and we end up using the raw
       * entry for the return vector
       */
      bool valid = rgw_obj_key::parse_raw_oid(index_key.name, &obj);
      if (!valid) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	  " could not parse object name: " << obj.name << dendl;
        continue;
      }

      if (!params.list_versions && !entry.is_visible()) {
        ldpp_dout(dpp, 20) << __func__ <<
	  ": skipping \"" << index_key <<
	  "\" because not listing versions and entry not visible" << dendl;
        continue;
      }

      if (params.enforce_ns && obj.ns != params.ns) {
        ldpp_dout(dpp, 20) << __func__ <<
	  ": skipping \"" << index_key <<
	  "\" because namespace does not match" << dendl;
        continue;
      }

      if (cur_end_marker_valid && cur_end_marker <= index_key) {
	// we're not guaranteed items will come in order, so we have
	// to loop through all
        ldpp_dout(dpp, 20) << __func__ <<
	  ": skipping \"" << index_key <<
	  "\" because after end_marker" << dendl;
	continue;
      }

      if (params.access_list_filter &&
	  !params.access_list_filter(obj.name, index_key.name)) {
        ldpp_dout(dpp, 20) << __func__ <<
	  ": skipping \"" << index_key <<
	  "\" because doesn't match filter" << dendl;
        continue;
      }

      if (params.prefix.size() &&
	  (0 != obj.name.compare(0, params.prefix.size(), params.prefix))) {
        ldpp_dout(dpp, 20) << __func__ <<
	  ": skipping \"" << index_key <<
	  "\" because doesn't match prefix" << dendl;
	continue;
      }

      if (count >= max) {
        truncated = true;
        goto done;
      }

      result->emplace_back(std::move(entry));
      count++;
    } // for (auto& entry : ent_list)
  } // while (truncated && count <= max)

done:

  if (is_truncated) {
    *is_truncated = truncated;
  }

  return 0;
} // list_objects_unordered


/**
 * create a rados pool, associated meta info
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_pool(const DoutPrefixProvider *dpp, const rgw_pool& pool)
{
  librados::IoCtx io_ctx;
  constexpr bool create = true;
  return rgw_init_ioctx(dpp, get_rados_handle(), pool, io_ctx, create);
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

int RGWRados::create_bucket(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            const rgw_bucket& bucket,
                            const rgw_user& owner,
                            const std::string& zonegroup_id,
                            const rgw_placement_rule& placement_rule,
                            const RGWZonePlacementInfo* zone_placement,
                            const std::map<std::string, bufferlist>& attrs,
                            bool obj_lock_enabled,
                            const std::optional<std::string>& swift_ver_location,
                            const std::optional<RGWQuotaInfo>& quota,
                            std::optional<ceph::real_time> creation_time,
                            obj_version* pep_objv,
                            RGWBucketInfo& info)
{
  int ret = 0;

#define MAX_CREATE_RETRIES 20 /* need to bound retries */
  for (int i = 0; i < MAX_CREATE_RETRIES; i++) {
    RGWObjVersionTracker& objv_tracker = info.objv_tracker;
    objv_tracker.read_version.clear();
    objv_tracker.generate_new_write_ver(cct);

    if (bucket.marker.empty()) {
      create_bucket_id(&info.bucket.marker);
      info.bucket.bucket_id = info.bucket.marker;
    } else {
      info.bucket = bucket;
    }

    info.owner = owner;
    info.zonegroup = zonegroup_id;
    info.placement_rule = placement_rule;
    info.swift_versioning = swift_ver_location.has_value();
    if (swift_ver_location) {
      info.swift_ver_location = *swift_ver_location;
    }
    if (obj_lock_enabled) {
      info.flags |= BUCKET_VERSIONED | BUCKET_OBJ_LOCK_ENABLED;
    }

    if (zone_placement) {
      init_default_bucket_layout(cct, info.layout, svc.zone->get_zone(),
                                 zone_placement->index_type);
    }

    info.requester_pays = false;
    if (creation_time) {
      info.creation_time = *creation_time;
    } else {
      info.creation_time = ceph::real_clock::now();
    }
    if (quota) {
      info.quota = *quota;
    }

    if (zone_placement) {
      ret = svc.bi->init_index(dpp, info, info.layout.current_index);
      if (ret < 0) {
        return ret;
      }
    }

    constexpr bool exclusive = true;
    ret = put_linked_bucket_info(info, exclusive, ceph::real_time(), pep_objv, &attrs, true, dpp, y);
    if (ret == -ECANCELED) {
      ret = -EEXIST;
    }
    if (ret == -EEXIST) {
       /* we need to reread the info and return it, caller will have a use for it */
      RGWBucketInfo orig_info;
      int r = get_bucket_info(&svc, bucket.tenant, bucket.name, orig_info, NULL, y, NULL);
      if (r < 0) {
        if (r == -ENOENT) {
          continue;
        }
        ldpp_dout(dpp, 0) << "get_bucket_info returned " << r << dendl;
        return r;
      }

      /* only remove it if it's a different bucket instance */
      if (orig_info.bucket.bucket_id != bucket.bucket_id) {
        if (zone_placement) {
          r = svc.bi->clean_index(dpp, info, info.layout.current_index);
          if (r < 0) {
            ldpp_dout(dpp, 0) << "WARNING: could not remove bucket index (r=" << r << ")" << dendl;
          }
        }
        r = ctl.bucket->remove_bucket_instance_info(info.bucket, info, y, dpp);
        if (r < 0) {
          ldpp_dout(dpp, 0) << "WARNING: " << __func__ << "(): failed to remove bucket instance info: bucket instance=" << info.bucket.get_key() << ": r=" << r << dendl;
          /* continue anyway */
        }
      }

      info = std::move(orig_info);
      /* ret == -EEXIST here */
    }
    return ret;
  }

  /* this is highly unlikely */
  ldpp_dout(dpp, 0) << "ERROR: could not create bucket, continuously raced with bucket creation and removal" << dendl;
  return -ENOENT;
}

bool RGWRados::obj_to_raw(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj *raw_obj)
{
  get_obj_bucket_and_oid_loc(obj, raw_obj->oid, raw_obj->loc);

  return get_obj_data_pool(placement_rule, obj, &raw_obj->pool);
}

std::string RGWRados::get_cluster_fsid(const DoutPrefixProvider *dpp, optional_yield y)
{
  std::string s;
  rados.cluster_fsid(&s);
  return s;
}

int RGWRados::get_obj_head_ioctx(const DoutPrefixProvider *dpp,
				 const RGWBucketInfo& bucket_info,
				 const rgw_obj& obj,
				 librados::IoCtx *ioctx)
{
  std::string oid, key;
  get_obj_bucket_and_oid_loc(obj, oid, key);

  rgw_pool pool;
  if (!get_obj_data_pool(bucket_info.placement_rule, obj, &pool)) {
    ldpp_dout(dpp, 0) << "ERROR: cannot get data pool for obj=" << obj <<
      ", probably misconfiguration" << dendl;
    return -EIO;
  }

  int r = open_pool_ctx(dpp, pool, *ioctx, false, true);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: unable to open data-pool=" << pool.to_str() <<
      " for obj=" << obj << " with error-code=" << r << dendl;
    return r;
  }

  ioctx->locator_set_key(key);

  return 0;
}

int RGWRados::get_obj_head_ref(const DoutPrefixProvider *dpp,
                               const rgw_placement_rule& target_placement_rule,
                               const rgw_obj& obj,
                               rgw_rados_ref *ref)
{
  rgw_raw_obj raw;
  get_obj_bucket_and_oid_loc(obj, raw.oid, raw.loc);
  if (!get_obj_data_pool(target_placement_rule, obj, &raw.pool)) {
    ldpp_dout(dpp, 0) << "ERROR: cannot get data pool for obj=" << obj
		      << ", probably misconfiguration" << dendl;
    return -EIO;
  }
  auto r = rgw_get_rados_ref(dpp, get_rados_handle(), raw, ref);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed opening data pool (pool="
		      << raw.pool << "); r=" << r << dendl;
    return r;
  }
  return 0;
}

int RGWRados::get_obj_head_ref(const DoutPrefixProvider *dpp,
                               const RGWBucketInfo& bucket_info,
                               const rgw_obj& obj,
                               rgw_rados_ref *ref)
{
  return get_obj_head_ref(dpp, bucket_info.placement_rule, obj, ref);
}

int RGWRados::get_raw_obj_ref(const DoutPrefixProvider *dpp, rgw_raw_obj obj, rgw_rados_ref* ref)
{
  if (obj.oid.empty()) {
    obj.oid = obj.pool.to_str();
    obj.pool = svc.zone->get_zone_params().domain_root;
  }
  int r = rgw_get_rados_ref(dpp, get_rados_handle(), std::move(obj), ref);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed opening pool (pool=" << obj.pool << "); r=" << r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::get_system_obj_ref(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, rgw_rados_ref *ref)
{
  return get_raw_obj_ref(dpp, obj, ref);
}

/*
 * fixes an issue where head objects were supposed to have a locator created, but ended
 * up without one
 */
int RGWRados::fix_head_obj_locator(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, bool copy_obj, bool remove_bad, rgw_obj_key& key, optional_yield y)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  string oid;
  string locator;

  rgw_obj obj(bucket, key);

  get_obj_bucket_and_oid_loc(obj, oid, locator);

  if (locator.empty()) {
    ldpp_dout(dpp, 20) << "object does not have a locator, nothing to fix" << dendl;
    return 0;
  }

  librados::IoCtx ioctx;

  int ret = get_obj_head_ioctx(dpp, bucket_info, obj, &ioctx);
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

  ret = rgw_rados_operate(dpp, ioctx, oid, &op, &data, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: rgw_rados_operate(oid=" << oid << ") returned ret=" << ret << dendl;
    return ret;
  }

  if (size > HEAD_SIZE) {
    ldpp_dout(dpp, -1) << "ERROR: returned object size (" << size << ") > HEAD_SIZE (" << HEAD_SIZE << ")" << dendl;
    return -EIO;
  }

  if (size != data.length()) {
    ldpp_dout(dpp, -1) << "ERROR: returned object size (" << size << ") != data.length() (" << data.length() << ")" << dendl;
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
    rgw_rados_operate(dpp, ioctx, oid, &wop, y);
  }

  if (remove_bad) {
    ioctx.locator_set_key(string());

    ret = ioctx.remove(oid);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed to remove original bad object" << dendl;
      return ret;
    }
  }

  return 0;
}

int RGWRados::move_rados_obj(const DoutPrefixProvider *dpp,
                             librados::IoCtx& src_ioctx,
			     const string& src_oid, const string& src_locator,
		             librados::IoCtx& dst_ioctx,
			     const string& dst_oid, const string& dst_locator, optional_yield y)
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
    ret = rgw_rados_operate(dpp, src_ioctx, src_oid, &rop, &data, y);
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
    ret = rgw_rados_operate(dpp, dst_ioctx, dst_oid, &wop, y);
    if (ret < 0) {
      goto done_err;
    }
    ofs += data.length();
    done = data.length() != chunk_size;
  } while (!done);

  if (ofs != size) {
    ldpp_dout(dpp, -1) << "ERROR: " << __func__ << ": copying " << src_oid << " -> " << dst_oid
               << ": expected " << size << " bytes to copy, ended up with " << ofs << dendl;
    ret = -EIO;
    goto done_err;
  }

  src_ioctx.remove(src_oid);

  return 0;

done_err:
  // TODO: clean up dst_oid if we created it
  ldpp_dout(dpp, -1) << "ERROR: failed to copy " << src_oid << " -> " << dst_oid << dendl;
  return ret;
}

/*
 * fixes an issue where head objects were supposed to have a locator created, but ended
 * up without one
 */
int RGWRados::fix_tail_obj_locator(const DoutPrefixProvider *dpp,
                                   RGWBucketInfo& bucket_info, rgw_obj_key& key,
                                   bool fix, bool *need_fix, optional_yield y)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  rgw_obj obj(bucket, key);

  if (need_fix) {
    *need_fix = false;
  }

  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }

  RGWObjState *astate = nullptr;
  RGWObjManifest* manifest = nullptr;
  RGWObjectCtx octx(this->driver);
  r = get_obj_state(dpp, &octx, bucket_info, obj, &astate, &manifest, false, y);
  if (r < 0)
    return r;

  if (manifest) {
    RGWObjManifest::obj_iterator miter;
    for (miter = manifest->obj_begin(dpp); miter != manifest->obj_end(dpp); ++miter) {
      rgw_raw_obj raw_loc = miter.get_location().get_raw_obj(this);
      rgw_obj loc;
      string oid;
      string locator;

      RGWSI_Tier_RADOS::raw_obj_to_obj(manifest->get_tail_placement().bucket, raw_loc, &loc);

      if (loc.key.ns.empty()) {
	/* continue, we're only interested in tail objects */
	continue;
      }

      auto& ioctx = ref.ioctx;

      get_obj_bucket_and_oid_loc(loc, oid, locator);
      ref.ioctx.locator_set_key(locator);

      ldpp_dout(dpp, 20) << __func__ << ": key=" << key << " oid=" << oid << " locator=" << locator << dendl;

      r = ioctx.stat(oid, NULL, NULL);
      if (r != -ENOENT) {
	continue;
      }

      string bad_loc;
      prepend_bucket_marker(bucket, loc.key.name, bad_loc);

      /* create a new ioctx with the bad locator */
      librados::IoCtx src_ioctx;
      src_ioctx.dup(ioctx);
      src_ioctx.locator_set_key(bad_loc);

      r = src_ioctx.stat(oid, NULL, NULL);
      if (r != 0) {
	/* cannot find a broken part */
	continue;
      }
      ldpp_dout(dpp, 20) << __func__ << ": found bad object part: " << loc << dendl;
      if (need_fix) {
        *need_fix = true;
      }
      if (fix) {
        r = move_rados_obj(dpp, src_ioctx, oid, bad_loc, ioctx, oid, locator, y);
        if (r < 0) {
          ldpp_dout(dpp, -1) << "ERROR: copy_rados_obj() on oid=" << oid << " returned r=" << r << dendl;
        }
      }
    }
  }

  return 0;
}

int RGWRados::BucketShard::init(const rgw_bucket& _bucket,
				const rgw_obj& obj,
				RGWBucketInfo* bucket_info_out,
                                const DoutPrefixProvider *dpp, optional_yield y)
{
  bucket = _bucket;

  RGWBucketInfo bucket_info;
  RGWBucketInfo* bucket_info_p =
    bucket_info_out ? bucket_info_out : &bucket_info;

  int ret = store->get_bucket_instance_info(bucket, *bucket_info_p, NULL, NULL, y, dpp);
  if (ret < 0) {
    return ret;
  }

  string oid;

  ret = store->svc.bi_rados->open_bucket_index_shard(dpp, *bucket_info_p, obj.get_hash_object(), &bucket_obj, &shard_id);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: open_bucket_index_shard() returned ret=" << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << " bucket index object: " << bucket_obj << dendl;

  return 0;
}

int RGWRados::BucketShard::init(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info,
                                const rgw_obj& obj, optional_yield y)
{
  bucket = bucket_info.bucket;

  int ret = store->svc.bi_rados->open_bucket_index_shard(dpp, bucket_info,
							 obj.get_hash_object(),
							 &bucket_obj,
							 &shard_id);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: open_bucket_index_shard() returned ret=" << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << " bucket index object: " << bucket_obj << dendl;

  return 0;
}

int RGWRados::BucketShard::init(const DoutPrefixProvider *dpp,
				const RGWBucketInfo& bucket_info,
                                const rgw::bucket_index_layout_generation& index,
                                int sid, optional_yield y)
{
  bucket = bucket_info.bucket;
  shard_id = sid;

  int ret = store->svc.bi_rados->open_bucket_index_shard(dpp, bucket_info, index,
                                                         shard_id, &bucket_obj);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: open_bucket_index_shard() returned ret=" << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << " bucket index object: " << bucket_obj << dendl;

  return 0;
}


/* Execute @handler on last item in bucket listing for bucket specified
 * in @bucket_info. @obj_prefix and @obj_delim narrow down the listing
 * to objects matching these criterias. */
int RGWRados::on_last_entry_in_listing(const DoutPrefixProvider *dpp,
                                       RGWBucketInfo& bucket_info,
                                       const std::string& obj_prefix,
                                       const std::string& obj_delim,
                                       std::function<int(const rgw_bucket_dir_entry&)> handler, optional_yield y)
{
  RGWRados::Bucket target(this, bucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.prefix = obj_prefix;
  list_op.params.delim = obj_delim;

  ldpp_dout(dpp, 20) << "iterating listing for bucket=" << bucket_info.bucket.name
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

    int ret = list_op.list_objects(dpp, MAX_LIST_OBJS, &entries, nullptr,
                                   &is_truncated, y);
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

bool RGWRados::swift_versioning_enabled(const RGWBucketInfo& bucket_info) const
{
  return bucket_info.has_swift_versioning() &&
    bucket_info.swift_ver_location.size();
}

int RGWRados::swift_versioning_copy(RGWObjectCtx& obj_ctx,
                                    const rgw_user& user,
                                    RGWBucketInfo& bucket_info,
                                    const rgw_obj& obj,
                                    const DoutPrefixProvider *dpp,
                                    optional_yield y)
{
  if (! swift_versioning_enabled(bucket_info)) {
    return 0;
  }

  obj_ctx.set_atomic(obj);

  RGWObjState * state = nullptr;
  RGWObjManifest *manifest = nullptr;
  int r = get_obj_state(dpp, &obj_ctx, bucket_info, obj, &state, &manifest, false, y);
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

  r = get_bucket_info(&svc, bucket_info.bucket.tenant, bucket_info.swift_ver_location, dest_bucket_info, NULL, y, NULL);
  if (r < 0) {
    ldpp_dout(dpp, 10) << "failed to read dest bucket info: r=" << r << dendl;
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

  rgw_zone_id no_zone;

  jspan_context no_trace{false, false};

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
               NULL, /* void *progress_data */
               dpp,
               y,
               no_trace);
  if (r == -ECANCELED || r == -ENOENT) {
    /* Has already been overwritten, meaning another rgw process already
     * copied it out */
    return 0;
  }

  return r;
}

int RGWRados::swift_versioning_restore(RGWObjectCtx& obj_ctx,
                                       const rgw_user& user,
                                       RGWBucketInfo& bucket_info,
                                       rgw_obj& obj,
                                       bool& restored,
                                       const DoutPrefixProvider *dpp, optional_yield y)
{
  if (! swift_versioning_enabled(bucket_info)) {
    return 0;
  }

  /* Bucket info of the bucket that stores previous versions of our object. */
  RGWBucketInfo archive_binfo;

  int ret = get_bucket_info(&svc, bucket_info.bucket.tenant,
                            bucket_info.swift_ver_location,
			    archive_binfo, nullptr, y, nullptr);
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
    rgw_zone_id no_zone;

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

    jspan_context no_trace{false, false};

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
                       nullptr,       /* void *progress_data */
                       dpp,
                       y,
                       no_trace);
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
    ret = delete_obj(dpp, obj_ctx, archive_binfo, archive_obj,
                     archive_binfo.versioning_status(), y);

    return ret;
  };

  const std::string& obj_name = obj.get_oid();
  const auto prefix = boost::str(boost::format("%03x%s") % obj_name.size()
                                                         % obj_name);

  return on_last_entry_in_listing(dpp, archive_binfo, prefix, std::string(),
                                  handler, y);
}

int RGWRados::Object::Write::_do_write_meta(uint64_t size, uint64_t accounted_size,
                                           map<string, bufferlist>& attrs,
                                           bool assume_noent, bool modify_tail,
                                           void *_index_op, const req_context& rctx,
                                           jspan_context& trace, bool log_op)
{
  RGWRados::Bucket::UpdateIndex *index_op = static_cast<RGWRados::Bucket::UpdateIndex *>(_index_op);
  RGWRados *store = target->get_store();

  ObjectWriteOperation op;
#ifdef WITH_LTTNG
  const req_state* s =  get_req_state();
  string req_id;
  if (!s) {
    // fake req_id
    req_id = store->svc.zone_utils->unique_id(store->driver->get_new_req_id());
  } else {
    req_id = s->req_id;
  }
#endif

  RGWObjState *state;
  RGWObjManifest *manifest = nullptr;
  int r = target->get_state(rctx.dpp, &state, &manifest, false, rctx.y, assume_noent);
  if (r < 0)
    return r;

  rgw_obj& obj = target->get_obj();

  if (obj.get_oid().empty()) {
    ldpp_dout(rctx.dpp, 0) << "ERROR: " << __func__ << "(): cannot write object with empty name" << dendl;
    return -EIO;
  }

  rgw_rados_ref ref;
  r = store->get_obj_head_ref(rctx.dpp, target->get_meta_placement_rule(), obj, &ref);
  if (r < 0)
    return r;

  bool is_olh = state->is_olh;

  bool reset_obj = (meta.flags & PUT_OBJ_CREATE) != 0;

  const string *ptag = meta.ptag;
  if (!ptag && !index_op->get_optag()->empty()) {
    ptag = index_op->get_optag();
  }
  r = target->prepare_atomic_modification(rctx.dpp, op, reset_obj, ptag, meta.if_match, meta.if_nomatch, false, modify_tail, rctx.y);
  if (r < 0)
    return r;

  if (real_clock::is_zero(meta.set_mtime)) {
    meta.set_mtime = real_clock::now();
  }

  if (target->get_bucket_info().obj_lock_enabled() && target->get_bucket_info().obj_lock.has_rule() && meta.flags == PUT_OBJ_CREATE) {
    auto iter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
    if (iter == attrs.end()) {
      real_time lock_until_date = target->get_bucket_info().obj_lock.get_lock_until_date(meta.set_mtime);
      string mode = target->get_bucket_info().obj_lock.get_mode();
      RGWObjectRetention obj_retention(mode, lock_until_date);
      bufferlist bl;
      obj_retention.encode(bl);
      op.setxattr(RGW_ATTR_OBJECT_RETENTION, bl);
    }
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
    if (state->compressed) {
      uint32_t alloc_hint_flags = librados::ALLOC_HINT_FLAG_INCOMPRESSIBLE;
      op.set_alloc_hint2(0, 0, alloc_hint_flags);
    }
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
    r = index_op->prepare(rctx.dpp, CLS_RGW_OP_ADD, &state->write_tag, rctx.y, log_op);
    tracepoint(rgw_rados, prepare_exit, req_id.c_str());
    if (r < 0)
      return r;
  }

  auto& ioctx = ref.ioctx;

  tracepoint(rgw_rados, operate_enter, req_id.c_str());
  r = rgw_rados_operate(rctx.dpp, ref.ioctx, ref.obj.oid, &op, rctx.y, 0, &trace);
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

  epoch = ioctx.get_last_version();
  poolid = ioctx.get_id();

  r = target->complete_atomic_modification(rctx.dpp, rctx.y);
  if (r < 0) {
    ldpp_dout(rctx.dpp, 0) << "ERROR: complete_atomic_modification returned r=" << r << dendl;
  }

  tracepoint(rgw_rados, complete_enter, req_id.c_str());
  r = index_op->complete(rctx.dpp, poolid, epoch, size, accounted_size,
                        meta.set_mtime, etag, content_type,
                        storage_class, &acl_bl,
			 meta.category, meta.remove_objs, rctx.y,
			 meta.user_data, meta.appendable, log_op);
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
    bool add_log = log_op && store->svc.zone->need_to_log_data();
    r = store->set_olh(rctx.dpp, target->get_ctx(), target->get_bucket_info(), obj, false, NULL, *meta.olh_epoch, real_time(), false, rctx.y, meta.zones_trace, add_log);
    if (r < 0) {
      return r;
    }
  }

  if (!real_clock::is_zero(meta.delete_at)) {
    rgw_obj_index_key obj_key;
    obj.key.get_index_key(&obj_key);

    r = store->obj_expirer->hint_add(rctx.dpp, meta.delete_at, obj.bucket.tenant, obj.bucket.name,
                                     obj.bucket.bucket_id, obj_key);
    if (r < 0) {
      ldpp_dout(rctx.dpp, 0) << "ERROR: objexp_hint_add() returned r=" << r << ", object will not get removed" << dendl;
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
  int ret = index_op->cancel(rctx.dpp, meta.remove_objs, rctx.y, log_op);
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 0) << "ERROR: index_op.cancel() returned ret=" << ret << dendl;
  }

  meta.canceled = true;

  /* we lost in a race. There are a few options:
   * - existing object was rewritten (ECANCELED)
   * - nonexistent object was created (EEXIST)
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
                                        map<string, bufferlist>& attrs, const req_context& rctx,
                                        jspan_context& trace, bool log_op)
{
  RGWBucketInfo& bucket_info = target->get_bucket_info();

  RGWRados::Bucket bop(target->get_store(), bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, target->get_obj());
  index_op.set_zones_trace(meta.zones_trace);
  
  bool assume_noent = (meta.if_match == NULL && meta.if_nomatch == NULL);
  int r;
  if (assume_noent) {
    r = _do_write_meta(size, accounted_size, attrs, assume_noent, meta.modify_tail, (void *)&index_op, rctx, trace, log_op);
    if (r == -EEXIST) {
      assume_noent = false;
    }
  }
  if (!assume_noent) {
    r = _do_write_meta(size, accounted_size, attrs, assume_noent, meta.modify_tail, (void *)&index_op, rctx, trace, log_op);
  }
  return r;
}

class RGWRadosPutObj : public RGWHTTPStreamRWRequest::ReceiveCB
{
  const DoutPrefixProvider *dpp;
  CephContext* cct;
  rgw_obj obj;
  rgw::sal::DataProcessor *filter;
  boost::optional<RGWPutObj_Compress>& compressor;
  bool try_etag_verify;
  rgw::putobj::etag_verifier_ptr etag_verifier;
  boost::optional<rgw::putobj::ChunkProcessor> buffering;
  CompressorRef& plugin;
  rgw::sal::ObjectProcessor *processor;
  void (*progress_cb)(off_t, void *);
  void *progress_data;
  bufferlist extra_data_bl, manifest_bl;
  std::optional<RGWCompressionInfo> compression_info;
  uint64_t extra_data_left{0};
  bool need_to_process_attrs{true};
  uint64_t data_len{0};
  map<string, bufferlist> src_attrs;
  uint64_t ofs{0};
  uint64_t lofs{0}; /* logical ofs */
  std::function<int(map<string, bufferlist>&)> attrs_handler;

public:
  RGWRadosPutObj(const DoutPrefixProvider *dpp,
                 CephContext* cct,
                 CompressorRef& plugin,
                 boost::optional<RGWPutObj_Compress>& compressor,
                 rgw::sal::ObjectProcessor *p,
                 void (*_progress_cb)(off_t, void *),
                 void *_progress_data,
                 std::function<int(map<string, bufferlist>&)> _attrs_handler) :
                       dpp(dpp),
                       cct(cct),
                       filter(p),
                       compressor(compressor),
                       try_etag_verify(cct->_conf->rgw_sync_obj_etag_verify),
                       plugin(plugin),
                       processor(p),
                       progress_cb(_progress_cb),
                       progress_data(_progress_data),
                       attrs_handler(_attrs_handler) {}


  int process_attrs(void) {
    bool encrypted = false;
    if (extra_data_bl.length()) {
      JSONParser jp;
      if (!jp.parse(extra_data_bl.c_str(), extra_data_bl.length())) {
        ldpp_dout(dpp, 0) << "failed to parse response extra data. len=" << extra_data_bl.length() << " data=" << extra_data_bl.c_str() << dendl;
        return -EIO;
      }

      JSONDecoder::decode_json("attrs", src_attrs, &jp);

      encrypted = src_attrs.count(RGW_ATTR_CRYPT_MODE);
      if (encrypted) {
        // we won't have access to the decrypted data for checksumming
        try_etag_verify = false;
      }

      // if the object is both compressed and encrypted, it was transferred
      // in its encrypted+compressed form. we need to preserve the original
      // RGW_ATTR_COMPRESSION instead of falling back to default compression
      // settings
      auto iter = src_attrs.find(RGW_ATTR_COMPRESSION);
      if (iter != src_attrs.end() && !encrypted) {
        const bufferlist bl = std::move(iter->second);
        src_attrs.erase(iter); // don't preserve source compression info

        if (try_etag_verify) {
          // if we're trying to verify etags, we need to convert compressed
          // ranges in the manifest back into logical multipart part offsets
          RGWCompressionInfo info;
          bool compressed = false;
          int r = rgw_compression_info_from_attr(bl, compressed, info);
          if (r < 0) {
            ldpp_dout(dpp, 4) << "failed to decode compression info, "
                "disabling etag verification" << dendl;
            try_etag_verify = false;
          } else if (compressed) {
            compression_info = std::move(info);
          }
        }
      }

      /* We need the manifest to recompute the ETag for verification */
      iter = src_attrs.find(RGW_ATTR_MANIFEST);
      if (iter != src_attrs.end()) {
        manifest_bl = std::move(iter->second);
        src_attrs.erase(iter);

        // if the source object was encrypted, preserve the part lengths from
        // the original object's manifest in RGW_ATTR_CRYPT_PARTS. if the object
        // already replicated and has the RGW_ATTR_CRYPT_PARTS attr, preserve it
        if (src_attrs.count(RGW_ATTR_CRYPT_MODE) &&
            !src_attrs.count(RGW_ATTR_CRYPT_PARTS)) {
          std::vector<size_t> parts_len;
          int r = RGWGetObj_BlockDecrypt::read_manifest_parts(dpp, manifest_bl,
                                                              parts_len);
          if (r < 0) {
            ldpp_dout(dpp, 4) << "failed to read part lengths from the manifest" << dendl;
          } else {
            // store the encoded part lenghts in RGW_ATTR_CRYPT_PARTS
            bufferlist parts_bl;
            encode(parts_len, parts_bl);
            src_attrs[RGW_ATTR_CRYPT_PARTS] = std::move(parts_bl);
          }
        }
      }

      // filter out olh attributes
      iter = src_attrs.lower_bound(RGW_ATTR_OLH_PREFIX);
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

    // do not compress if object is encrypted
    if (plugin && !encrypted) {
      compressor = boost::in_place(cct, plugin, filter);
      // add a filter that buffers data so we don't try to compress tiny blocks.
      // libcurl reads in 16k at a time, and we need at least 64k to get a good
      // compression ratio
      constexpr unsigned buffer_size = 512 * 1024;
      buffering = boost::in_place(&*compressor, buffer_size);
      filter = &*buffering;
    }

    if (try_etag_verify) {
      ret = rgw::putobj::create_etag_verifier(dpp, cct, filter, manifest_bl,
                                              compression_info,
                                              etag_verifier);
      if (ret < 0) {
        ldpp_dout(dpp, 4) << "failed to initial etag verifier, "
            "disabling etag verification" << dendl;
      } else {
        filter = etag_verifier.get();
      }
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

  std::string get_verifier_etag() {
    if (etag_verifier) {
      etag_verifier->calculate_etag();
      return etag_verifier->get_calculated_etag();
    } else {
      return "";
    }
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

int RGWRados::rewrite_obj(RGWBucketInfo& dest_bucket_info, const rgw_obj& obj, const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWObjectCtx octx(this->driver);
  rgw::sal::Attrs attrset;
  uint64_t obj_size;
  ceph::real_time mtime;
  RGWRados::Object op_target(this, dest_bucket_info, octx, obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrset;
  read_op.params.obj_size = &obj_size;
  read_op.params.lastmod = &mtime;

  int ret = read_op.prepare(y, dpp);
  if (ret < 0)
    return ret;

  attrset.erase(RGW_ATTR_ID_TAG);
  attrset.erase(RGW_ATTR_TAIL_TAG);
  attrset.erase(RGW_ATTR_STORAGE_CLASS);

  return copy_obj_data(octx, dest_bucket_info, dest_bucket_info.placement_rule,
                       read_op, obj_size - 1, obj, NULL, mtime,
                       attrset, 0, real_time(), NULL, dpp, y);
}


int RGWRados::reindex_obj(rgw::sal::Driver* driver,
			  RGWBucketInfo& bucket_info,
			  const rgw_obj& head_obj,
			  const DoutPrefixProvider* dpp,
			  optional_yield y)
{
  // used for trimming pending entries; max value means all versions trimmed
  const uint64_t max_ver = std::numeric_limits<uint64_t>::max();
  // used for linking an olh
  const std::string empty_op_tag = "";

  int ret;
  RGWObjectCtx obj_ctx(driver);

  // aids in printing out name of bucket/object
  auto p = [](const rgw_obj& o) -> std::string {
    std::stringstream ss;
    ss << o.bucket.name << ':' << o.key;
    return ss.str();
  };

  // since the code for linking a versioned object and adding a delete
  // marker is so similar, we bring the common OLH-handling code into
  // this lambda
  auto link_helper = [&](const bool is_delete_marker,
			 rgw_bucket_dir_entry_meta& meta,
			 const std::string& log_tag) -> int {
    int ret = 0;

    // convert the head object name into the OLH object by removing
    // the instance info
    rgw_obj olh_obj = head_obj;
    olh_obj.key.instance.clear();

    RGWObjState* olh_state { nullptr };
    RGWObjManifest* olh_manifest { nullptr }; // we don't use, but must send in
    ret = get_obj_state(dpp, &obj_ctx, bucket_info, olh_obj,
			&olh_state, &olh_manifest,
			false, // don't follow olh
			y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	": during " << log_tag << " get_obj_state on OLH object " <<
	olh_obj.key << " returned: " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    // In order to update the data in the OLH object we're calling
    // bucket_index_link_olh followed by bucket_index_trim_olh_log
    // since that churns metadata less than a call to set_olh
    // would. bucket_index_link_olh does leave entries in the OLH
    // object's pending log since normally OLH updates are paired with
    // other ops, but we remove such entries below.
    ret = bucket_index_link_olh(dpp,
				bucket_info,
				*olh_state,
				head_obj,
				is_delete_marker,
				empty_op_tag,
				&meta,
				0, // zero olh_epoch means calculated in CLS
				ceph::real_clock::zero(), // unmod_since
				true, // high_precision_time
				y,
				nullptr, // zones trace
				false); // log data change
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	": during " << log_tag << " set_index_link_olh returned: " <<
	cpp_strerror(-ret) << dendl;
      return ret;
    }

    // bucket_)index_link_olh leaves a pending_log entry in the OLH;
    // this trims it out
    ret = bucket_index_trim_olh_log(dpp,
				    bucket_info,
				    *olh_state,
				    head_obj,
				    max_ver,
				    y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	": during " << log_tag <<
	" bucket_index_trim_olh_log returned: " <<
	cpp_strerror(-ret) << dendl;
      return ret;
    }

    return 0;
  }; // link_helper lambda

  librados::IoCtx head_obj_ctx;
  ret = get_obj_head_ioctx(dpp, bucket_info, head_obj, &head_obj_ctx);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
      ": get_obj_head_ioctx for " << p(head_obj) << " returned: " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }

  const int64_t pool_id = head_obj_ctx.get_id();
  const bool is_versioned = bucket_info.versioned();
  const bool has_instance = ! head_obj.key.instance.empty();

  ldpp_dout(dpp, 20) << "INFO: " << __func__ << ": reindexing " <<
    p(head_obj) << dendl;

  RGWObjState *head_state { nullptr };
  RGWObjManifest *head_manifest { nullptr };

  // if head_obj does not exist does not return -ENOENT but instead
  // sets head_state->exists to false
  ret = get_obj_state(dpp, &obj_ctx, bucket_info, head_obj,
		      &head_state, &head_manifest,
		      false, // don't follow olh
		      y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
      ": get_obj_state on " << p(head_obj) << " returned: " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }

  if (! head_state->exists && is_versioned && has_instance) {
    // head object does not exist if it's a delete marker; handle here
    // and return
    ldpp_dout(dpp, 20) << "INFO: " << __func__ << ": indexing " <<
      p(head_obj) << " as delete marker" << dendl;

    // empty metadata object is fine for delete marker
    rgw_bucket_dir_entry_meta meta;

    return link_helper(true, meta, "set delete marker");
  } else if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
      ": unable to complete stat of " << p(head_obj) << "; returned: " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }

  // data we'll pull from head object xattrs
  std::string etag;
  std::string content_type;
  std::string storage_class;
  bufferlist acl_bl;
  bool found_olh_info { false };
  bufferlist olh_info_bl;
  bool appendable { false };
  bufferlist part_num_bl;

  rgw::sal::Attrs& attr_set = head_state->attrset;
  read_attr(attr_set, RGW_ATTR_ETAG, etag);
  read_attr(attr_set, RGW_ATTR_CONTENT_TYPE, content_type);
  read_attr(attr_set, RGW_ATTR_STORAGE_CLASS, storage_class);
  read_attr(attr_set, RGW_ATTR_ACL, acl_bl);
  read_attr(attr_set, RGW_ATTR_OLH_INFO, olh_info_bl, &found_olh_info);
  read_attr(attr_set, RGW_ATTR_APPEND_PART_NUM, part_num_bl, &appendable);

  // check for a pure OLH object and if so exit early
  if (found_olh_info) {
    try {
      auto iter = olh_info_bl.cbegin();
      RGWOLHInfo info;
      decode(info, iter);
      if (! info.target.key.instance.empty()) {
	// since there is a listed instance this appears to be a pure
	// OLH (i.e., no data); we won't index as we index actual
	// objects with data and set the OLH then
	ldpp_dout(dpp, 20) << "INFO: " << __func__ << ": " <<
	  p(head_obj) << " appears to be a pure OLH object; ignoring" << dendl;
	return 0;
      }
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	": unable to decode OLH info for " << p(head_obj) << dendl;
      return -EIO;
    }
  }

  Bucket bkt(this, bucket_info);
  RGWRados::Bucket::UpdateIndex update_idx(&bkt, head_obj);

  // note: we can skip calling prepare() since there's no transaction
  // and we don't specify a write tag (i.e., transaction tag)
  ret = update_idx.complete(dpp,
			    pool_id,
			    0, // bucket index epoch
			    head_state->size,
			    head_state->accounted_size,
			    head_state->mtime,
			    etag,
			    content_type,
			    storage_class,
			    &acl_bl,
			    RGWObjCategory::Main, // RGWObjCategory category,
			    nullptr, // remove_objs list
			    y,
			    nullptr, // user data string
			    appendable);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
      ": update index complete for " << p(head_obj) << " returned: " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }

  if (bucket_info.versioned()) {
    ldpp_dout(dpp, 20) << "INFO: " << __func__ << ": since " <<
      bucket_info.bucket << " appears to be versioned, setting OLH for " <<
      p(head_obj) << dendl;

    // write OLH and instance entries
    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::Main;
    meta.mtime = head_state->mtime;
    meta.size = head_state->size;
    meta.accounted_size = head_state->accounted_size;
    meta.etag = etag;
    meta.content_type = content_type;
    meta.appendable = appendable;

    ret = link_helper(false, meta, "linking version");
  } // if bucket is versioned

  return ret;
} // RGWRados::reindex_obj


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

int RGWRados::stat_remote_obj(const DoutPrefixProvider *dpp,
               RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               req_info *info,
               const rgw_zone_id& source_zone,
               const rgw_obj& src_obj,
               const RGWBucketInfo *src_bucket_info,
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
               string *petag, optional_yield y)
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
    if (!src_bucket_info || src_bucket_info->zonegroup.empty()) {
      /* source is in the master zonegroup */
      conn = svc.zone->get_master_conn();
    } else {
      auto& zonegroup_conn_map = svc.zone->get_zonegroup_conn_map();
      map<string, RGWRESTConn *>::iterator iter = zonegroup_conn_map.find(src_bucket_info->zonegroup);
      if (iter == zonegroup_conn_map.end()) {
        ldpp_dout(dpp, 0) << "could not find zonegroup connection to zonegroup: " << source_zone << dendl;
        return -ENOENT;
      }
      conn = iter->second;
    }
  } else {
    auto& zone_conn_map = svc.zone->get_zone_conn_map();
    auto iter = zone_conn_map.find(source_zone);
    if (iter == zone_conn_map.end()) {
      ldpp_dout(dpp, 0) << "could not find zone connection to zone: " << source_zone << dendl;
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
  constexpr bool sync_cloudtiered = true;

  static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
  for (int tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
    int ret = conn->get_obj(dpp, user_id, info, src_obj, pmod, unmod_ptr,
                        dest_mtime_weight.zone_short_id, dest_mtime_weight.pg_ver,
                        prepend_meta, get_op, rgwx_stat,
                        sync_manifest, skip_decrypt, nullptr, sync_cloudtiered,
                        true, &cb, &in_stream_req);
    if (ret < 0) {
      return ret;
    }

    ret = conn->complete_request(in_stream_req, nullptr, &set_mtime, psize,
                                 nullptr, pheaders, y);
    if (ret < 0) {
      if (ret == -EIO && tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
        ldpp_dout(dpp, 20) << __func__  << "(): failed to fetch object from remote. retries=" << tries << dendl;
        continue;
      }
      return ret;
    }
    break;
  }

  bufferlist& extra_data_bl = cb.get_extra_data();
  if (extra_data_bl.length()) {
    JSONParser jp;
    if (!jp.parse(extra_data_bl.c_str(), extra_data_bl.length())) {
      ldpp_dout(dpp, 0) << "failed to parse response extra data. len=" << extra_data_bl.length() << " data=" << extra_data_bl.c_str() << dendl;
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

int RGWFetchObjFilter_Default::filter(CephContext *cct,
                                      const rgw_obj_key& source_key,
                                      const RGWBucketInfo& dest_bucket_info,
                                      std::optional<rgw_placement_rule> dest_placement_rule,
                                      const map<string, bufferlist>& obj_attrs,
				      std::optional<rgw_user> *poverride_owner,
                                      const rgw_placement_rule **prule)
{
  const rgw_placement_rule *ptail_rule = (dest_placement_rule ? &(*dest_placement_rule) : nullptr);
  if (!ptail_rule) {
    auto iter = obj_attrs.find(RGW_ATTR_STORAGE_CLASS);
    if (iter != obj_attrs.end()) {
      dest_rule.storage_class = iter->second.to_str();
      dest_rule.inherit_from(dest_bucket_info.placement_rule);
      ptail_rule = &dest_rule;
    } else {
      ptail_rule = &dest_bucket_info.placement_rule;
    }
  }
  *prule = ptail_rule;
  return 0;
}

int RGWRados::fetch_remote_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               req_info *info,
               const rgw_zone_id& source_zone,
               const rgw_obj& dest_obj,
               const rgw_obj& src_obj,
               RGWBucketInfo& dest_bucket_info,
               RGWBucketInfo *src_bucket_info,
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
               rgw::sal::Attrs& attrs,
               RGWObjCategory category,
               std::optional<uint64_t> olh_epoch,
	       real_time delete_at,
               string *ptag,
               string *petag,
               void (*progress_cb)(off_t, void *),
               void *progress_data,
               const req_context& rctx,
               RGWFetchObjFilter *filter,
               bool stat_follow_olh,
               const rgw_obj& stat_dest_obj,
               const rgw_zone_set_entry& source_trace_entry,
               rgw_zone_set *zones_trace,
               std::optional<uint64_t>* bytes_transferred)
{
  /* source is in a different zonegroup, copy from there */

  RGWRESTStreamRWRequest *in_stream_req;
  string tag;
  int i;
  append_rand_alpha(cct, tag, tag, 32);
  obj_time_weight set_mtime_weight;
  set_mtime_weight.high_precision = high_precision_time;
  int ret;

  rgw::BlockingAioThrottle aio(cct->_conf->rgw_put_obj_min_window_size);
  using namespace rgw::putobj;
  jspan_context no_trace{false, false};
  AtomicObjectProcessor processor(&aio, this, dest_bucket_info, nullptr,
                                  user_id, obj_ctx, dest_obj, olh_epoch,
				  tag, rctx.dpp, rctx.y, no_trace);
  RGWRESTConn *conn;
  auto& zone_conn_map = svc.zone->get_zone_conn_map();
  auto& zonegroup_conn_map = svc.zone->get_zonegroup_conn_map();
  if (source_zone.empty()) {
    if (!src_bucket_info || src_bucket_info->zonegroup.empty()) {
      /* source is in the master zonegroup */
      conn = svc.zone->get_master_conn();
    } else {
      map<string, RGWRESTConn *>::iterator iter = zonegroup_conn_map.find(src_bucket_info->zonegroup);
      if (iter == zonegroup_conn_map.end()) {
        ldpp_dout(rctx.dpp, 0) << "could not find zonegroup connection to zonegroup: " << source_zone << dendl;
        return -ENOENT;
      }
      conn = iter->second;
    }
  } else {
    auto iter = zone_conn_map.find(source_zone);
    if (iter == zone_conn_map.end()) {
      ldpp_dout(rctx.dpp, 0) << "could not find zone connection to zone: " << source_zone << dendl;
      return -ENOENT;
    }
    conn = iter->second;
  }

  boost::optional<RGWPutObj_Compress> compressor;
  CompressorRef plugin;

  RGWFetchObjFilter_Default source_filter;
  if (!filter) {
    filter = &source_filter;
  }

  std::optional<rgw_user> override_owner;

  RGWRadosPutObj cb(rctx.dpp, cct, plugin, compressor, &processor, progress_cb, progress_data,
                    [&](map<string, bufferlist>& obj_attrs) {
                      const rgw_placement_rule *ptail_rule;

                      int ret = filter->filter(cct,
                                               src_obj.key,
                                               dest_bucket_info,
                                               dest_placement_rule,
                                               obj_attrs,
					       &override_owner,
                                               &ptail_rule);
                      if (ret < 0) {
                        ldpp_dout(rctx.dpp, 5) << "Aborting fetch: source object filter returned ret=" << ret << dendl;
                        return ret;
                      }

                      processor.set_tail_placement(*ptail_rule);

                      const auto& compression_type = svc.zone->get_zone_params().get_compression_type(*ptail_rule);
                      if (compression_type != "none") {
                        plugin = Compressor::create(cct, compression_type);
                        if (!plugin) {
                          ldpp_dout(rctx.dpp, 1) << "Cannot load plugin for compression type "
                                        << compression_type << dendl;
                        }
                      }

                      ret = processor.prepare(rctx.y);
                      if (ret < 0) {
                        return ret;
                      }
                      return 0;
                    });

  string etag;
  real_time set_mtime;
  uint64_t accounted_size = 0;

  RGWObjState *dest_state = NULL;
  RGWObjManifest *manifest = nullptr;

  const real_time *pmod = mod_ptr;

  obj_time_weight dest_mtime_weight;
  rgw_zone_set_entry dst_zone_trace(svc.zone->get_zone().id, dest_bucket_info.bucket.get_key());

  if (copy_if_newer) {
    /* need to get mtime for destination */
    ret = get_obj_state(rctx.dpp, &obj_ctx, dest_bucket_info, stat_dest_obj, &dest_state, &manifest, stat_follow_olh, rctx.y);
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
  static constexpr bool sync_cloudtiered = true;

  static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
  for (int tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
    ret = conn->get_obj(rctx.dpp, user_id, info, src_obj, pmod, unmod_ptr,
                        dest_mtime_weight.zone_short_id, dest_mtime_weight.pg_ver,
                        prepend_meta, get_op, rgwx_stat,
                        sync_manifest, skip_decrypt, &dst_zone_trace,
                        sync_cloudtiered, true,
                        &cb, &in_stream_req);
    if (ret < 0) {
      goto set_err_state;
    }

    ret = conn->complete_request(in_stream_req, &etag, &set_mtime,
                                 &accounted_size, nullptr, nullptr, rctx.y);
    if (ret < 0) {
      if (ret == -EIO && tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
        ldpp_dout(rctx.dpp, 20) << __func__  << "(): failed to fetch object from remote. retries=" << tries << dendl;
        continue;
      }
      goto set_err_state;
    }
    break;
  }
  ret = cb.flush();
  if (ret < 0) {
    goto set_err_state;
  }
  if (cb.get_data_len() != accounted_size) {
    ret = -EIO;
    ldpp_dout(rctx.dpp, 0) << "ERROR: object truncated during fetching, expected "
        << accounted_size << " bytes but received " << cb.get_data_len() << dendl;
    goto set_err_state;
  }

  if (compressor && compressor->is_compressed()) {
    bufferlist tmp;
    RGWCompressionInfo cs_info;
    cs_info.compression_type = plugin->get_type_name();
    cs_info.orig_size = accounted_size;
    cs_info.compressor_message = compressor->get_compressor_message();
    cs_info.blocks = std::move(compressor->get_compression_blocks());
    encode(cs_info, tmp);
    cb.get_attrs()[RGW_ATTR_COMPRESSION] = tmp;
  } else if (auto c = cb.get_attrs().find(RGW_ATTR_COMPRESSION);
             c != cb.get_attrs().end()) {
    // if the object was transferred in its compressed+encrypted form, use its
    // original uncompressed size
    try {
      RGWCompressionInfo info;
      auto p = c->second.cbegin();
      decode(info, p);
      accounted_size = info.orig_size;
    } catch (const buffer::error&) {
      ldpp_dout(rctx.dpp, 0) << "ERROR: could not decode compression attr for "
          "replicated object " << dest_obj << dendl;
      // decode error isn't fatal, but we might put the wrong size in the index
    }
  }

  if (override_owner) {
    processor.set_owner(*override_owner);

    auto& obj_attrs = cb.get_attrs();

    RGWUserInfo owner_info;
    if (ctl.user->get_info_by_uid(rctx.dpp, *override_owner, &owner_info, rctx.y) < 0) {
      ldpp_dout(rctx.dpp, 10) << "owner info does not exist" << dendl;
      return -EINVAL;
    }

    RGWAccessControlPolicy acl;

    auto aiter = obj_attrs.find(RGW_ATTR_ACL);
    if (aiter == obj_attrs.end()) {
      ldpp_dout(rctx.dpp, 0) << "WARNING: " << __func__ << "(): object doesn't have ACL attribute, setting default ACLs" << dendl;
      acl.create_default(owner_info.user_id, owner_info.display_name);
    } else {
      auto iter = aiter->second.cbegin();
      try {
	acl.decode(iter);
      } catch (buffer::error& err) {
	ldpp_dout(rctx.dpp, 0) << "ERROR: " << __func__ << "(): could not decode policy, caught buffer::error" << dendl;
	return -EIO;
      }
    }

    ACLOwner new_owner;
    new_owner.id = *override_owner;
    new_owner.display_name = owner_info.display_name;

    acl.set_owner(new_owner);

    bufferlist bl;
    acl.encode(bl);
    obj_attrs[RGW_ATTR_ACL] = std::move(bl);
  }

  if (source_zone.empty()) { /* need to preserve expiration if copy in the same zonegroup */
    cb.get_attrs().erase(RGW_ATTR_DELETE_AT);
  } else {
    map<string, bufferlist>::iterator iter = cb.get_attrs().find(RGW_ATTR_DELETE_AT);
    if (iter != cb.get_attrs().end()) {
      try {
        decode(delete_at, iter->second);
      } catch (buffer::error& err) {
        ldpp_dout(rctx.dpp, 0) << "ERROR: failed to decode delete_at field in intra zone copy" << dendl;
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

  //erase the append attr
  cb.get_attrs().erase(RGW_ATTR_APPEND_PART_NUM);

  { // add x-amz-replication-status=REPLICA
    auto& bl = cb.get_attrs()[RGW_ATTR_OBJ_REPLICATION_STATUS];
    bl.clear(); // overwrite source's status
    bl.append("REPLICA");
  }
  { // update replication trace
    std::vector<rgw_zone_set_entry> trace;
    if (auto i = cb.get_attrs().find(RGW_ATTR_OBJ_REPLICATION_TRACE);
        i != cb.get_attrs().end()) {
      try {
        decode(trace, i->second);
      } catch (const buffer::error&) {}
    }
    // add the source entry to the end
    trace.push_back(source_trace_entry);

    bufferlist bl;
    encode(trace, bl);
    cb.get_attrs()[RGW_ATTR_OBJ_REPLICATION_TRACE] = std::move(bl);
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
        ldpp_dout(rctx.dpp, 0) << "ERROR: failed to decode pg ver attribute, ignoring" << dendl;
        /* non critical error */
      }
    }
    set_mtime_weight.init(set_mtime, svc.zone->get_zone_short_id(), pg_ver);
  }

  /* Perform ETag verification is we have computed the object's MD5 sum at our end */
  if (const auto& verifier_etag = cb.get_verifier_etag();
      !verifier_etag.empty()) {
    string trimmed_etag = etag;

    /* Remove the leading and trailing double quotes from etag */
    trimmed_etag.erase(std::remove(trimmed_etag.begin(), trimmed_etag.end(),'\"'),
      trimmed_etag.end());

    if (verifier_etag != trimmed_etag) {
      ret = -EIO;
      ldpp_dout(rctx.dpp, 0) << "ERROR: source and destination objects don't match. Expected etag:"
        << trimmed_etag << " Computed etag:" << verifier_etag << dendl;
      goto set_err_state;
    }
  }

#define MAX_COMPLETE_RETRY 100
  for (i = 0; i < MAX_COMPLETE_RETRY; i++) {
    bool canceled = false;
    ret = processor.complete(accounted_size, etag, mtime, set_mtime,
                             attrs, delete_at, nullptr, nullptr, nullptr,
                             zones_trace, &canceled, rctx, rgw::sal::FLAG_LOG_OP);
    if (ret < 0) {
      goto set_err_state;
    }

    if (copy_if_newer && canceled) {
      ldpp_dout(rctx.dpp, 20) << "raced with another write of obj: " << dest_obj << dendl;
      obj_ctx.invalidate(dest_obj); /* object was overwritten */
      ret = get_obj_state(rctx.dpp, &obj_ctx, dest_bucket_info, stat_dest_obj, &dest_state, &manifest, stat_follow_olh, rctx.y);
      if (ret < 0) {
        ldpp_dout(rctx.dpp, 0) << "ERROR: " << __func__ << ": get_err_state() returned ret=" << ret << dendl;
        goto set_err_state;
      }
      dest_mtime_weight.init(dest_state);
      dest_mtime_weight.high_precision = high_precision_time;
      if (!dest_state->exists ||
        dest_mtime_weight < set_mtime_weight) {
        ldpp_dout(rctx.dpp, 20) << "retrying writing object mtime=" << set_mtime << " dest_state->mtime=" << dest_state->mtime << " dest_state->exists=" << dest_state->exists << dendl;
        continue;
      } else {
        ldpp_dout(rctx.dpp, 20) << "not retrying writing object mtime=" << set_mtime << " dest_state->mtime=" << dest_state->mtime << " dest_state->exists=" << dest_state->exists << dendl;
      }
    }
    break;
  }

  if (i == MAX_COMPLETE_RETRY) {
    ldpp_dout(rctx.dpp, 0) << "ERROR: retried object completion too many times, something is wrong!" << dendl;
    ret = -EIO;
    goto set_err_state;
  }

  if (bytes_transferred) {
    *bytes_transferred = cb.get_data_len();
  }
  return 0;
set_err_state:
  if (copy_if_newer && ret == -ERR_NOT_MODIFIED) {
    // we may have already fetched during sync of OP_ADD, but were waiting
    // for OP_LINK_OLH to call set_olh() with a real olh_epoch
    if (olh_epoch && *olh_epoch > 0) {
      constexpr bool log_data_change = true;
      ret = set_olh(rctx.dpp, obj_ctx, dest_bucket_info, dest_obj, false, nullptr,
                    *olh_epoch, real_time(), false, rctx.y, zones_trace, log_data_change);
    } else {
      // we already have the latest copy
      ret = 0;
    }
  }
  return ret;
}


int RGWRados::copy_obj_to_remote_dest(const DoutPrefixProvider *dpp,
                                      RGWObjState *astate,
                                      map<string, bufferlist>& src_attrs,
                                      RGWRados::Object::Read& read_op,
                                      const rgw_user& user_id,
                                      const rgw_obj& dest_obj,
                                      real_time *mtime, optional_yield y)
{
  string etag;

  RGWRESTStreamS3PutObj *out_stream_req;

  auto rest_master_conn = svc.zone->get_master_conn();

  static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
  for (int tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
    int ret = rest_master_conn->put_obj_async_init(dpp, user_id, dest_obj, src_attrs, &out_stream_req);
    if (ret < 0) {
      return ret;
    }

    out_stream_req->set_send_length(astate->size);

    ret = RGWHTTP::send(out_stream_req);
    if (ret < 0) {
      delete out_stream_req;
      return ret;
    }

    ret = read_op.iterate(dpp, 0, astate->size - 1, out_stream_req->get_out_cb(), y);
    if (ret < 0) {
      delete out_stream_req;
      return ret;
    }

    ret = rest_master_conn->complete_request(out_stream_req, etag, mtime, y);
    if (ret < 0) {
      if (ret == -EIO && tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
        ldpp_dout(dpp, 20) << __func__  << "(): failed to put_obj_async_init. retries=" << tries << dendl;
        continue;
      }
      return ret;
    }
    break;
  }

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
               const rgw_zone_id& source_zone,
               const rgw_obj& dest_obj,
               const rgw_obj& src_obj,
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
               rgw::sal::Attrs& attrs,
               RGWObjCategory category,
               uint64_t olh_epoch,
	       real_time delete_at,
               string *version_id,
               string *ptag,
               string *petag,
               void (*progress_cb)(off_t, void *),
               void *progress_data,
               const DoutPrefixProvider *dpp,
               optional_yield y,
               jspan_context& trace)
{
  int ret;
  uint64_t obj_size;
  rgw_obj shadow_obj = dest_obj;
  string shadow_oid;

  bool remote_src;
  bool remote_dest;

  bool stat_follow_olh = false;
  rgw_obj stat_dest_obj = dest_obj;

  append_rand_alpha(cct, dest_obj.get_oid(), shadow_oid, 32);
  shadow_obj.init_ns(dest_obj.bucket, shadow_oid, shadow_ns);

  auto& zonegroup = svc.zone->get_zonegroup();

  remote_dest = !zonegroup.equals(dest_bucket_info.zonegroup);
  remote_src = !zonegroup.equals(src_bucket_info.zonegroup);

  if (remote_src && remote_dest) {
    ldpp_dout(dpp, 0) << "ERROR: can't copy object when both src and dest buckets are remote" << dendl;
    return -EINVAL;
  }

  ldpp_dout(dpp, 5) << "Copy object " << src_obj.bucket << ":" << src_obj.get_oid() << " => " << dest_obj.bucket << ":" << dest_obj.get_oid() << dendl;

  if (remote_src || !source_zone.empty()) {
    rgw_zone_set_entry source_trace_entry{source_zone.id, std::nullopt};
    const req_context rctx{dpp, y, nullptr};
    return fetch_remote_obj(obj_ctx, user_id, info, source_zone,
               dest_obj, src_obj, dest_bucket_info, &src_bucket_info,
               dest_placement, src_mtime, mtime, mod_ptr,
               unmod_ptr, high_precision_time,
               if_match, if_nomatch, attrs_mod, copy_if_newer, attrs, category,
               olh_epoch, delete_at, ptag, petag, progress_cb, progress_data, rctx,
               nullptr /* filter */, stat_follow_olh, stat_dest_obj, source_trace_entry);
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

  ret = read_op.prepare(y, dpp);
  if (ret < 0) {
    return ret;
  }
  if (src_attrs.count(RGW_ATTR_CRYPT_MODE)) {
    // Current implementation does not follow S3 spec and even
    // may result in data corruption silently when copying
    // multipart objects across pools. So reject COPY operations
    //on encrypted objects before it is fully functional.
    ldpp_dout(dpp, 0) << "ERROR: copy op for encrypted object " << src_obj
                  << " has not been implemented." << dendl;
    return -ERR_NOT_IMPLEMENTED;
  }

  src_attrs[RGW_ATTR_ACL] = attrs[RGW_ATTR_ACL];
  src_attrs.erase(RGW_ATTR_DELETE_AT);

  src_attrs.erase(RGW_ATTR_OBJECT_RETENTION);
  src_attrs.erase(RGW_ATTR_OBJECT_LEGAL_HOLD);
  map<string, bufferlist>::iterator rt = attrs.find(RGW_ATTR_OBJECT_RETENTION);
  if (rt != attrs.end())
    src_attrs[RGW_ATTR_OBJECT_RETENTION] = rt->second;
  map<string, bufferlist>::iterator lh = attrs.find(RGW_ATTR_OBJECT_LEGAL_HOLD);
  if (lh != attrs.end())
    src_attrs[RGW_ATTR_OBJECT_LEGAL_HOLD] = lh->second;

  if (dest_bucket_info.flags & BUCKET_VERSIONS_SUSPENDED) {
    src_attrs.erase(RGW_ATTR_OLH_ID_TAG);
    src_attrs.erase(RGW_ATTR_OLH_INFO);
    src_attrs.erase(RGW_ATTR_OLH_VER);
  }

  set_copy_attrs(src_attrs, attrs, attrs_mod);
  attrs.erase(RGW_ATTR_ID_TAG);
  attrs.erase(RGW_ATTR_PG_VER);
  attrs.erase(RGW_ATTR_SOURCE_ZONE);
  map<string, bufferlist>::iterator cmp = src_attrs.find(RGW_ATTR_COMPRESSION);
  if (cmp != src_attrs.end())
    attrs[RGW_ATTR_COMPRESSION] = cmp->second;

  RGWObjManifest manifest;
  RGWObjState *astate = NULL;
  RGWObjManifest *amanifest = nullptr;

  constexpr bool follow_olh = true;
  ret = get_obj_state(dpp, &obj_ctx, src_bucket_info, src_obj,
                      &astate, &amanifest, follow_olh, y);
  if (ret < 0) {
    return ret;
  }

  vector<rgw_raw_obj> ref_objs;

  if (remote_dest) {
    /* dest is in a different zonegroup, copy it there */
    return copy_obj_to_remote_dest(dpp, astate, attrs, read_op, user_id, dest_obj, mtime, y);
  }
  uint64_t max_chunk_size;

  ret = get_max_chunk_size(dest_bucket_info.placement_rule, dest_obj, &max_chunk_size, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get max_chunk_size() for bucket " << dest_obj.bucket << dendl;
    return ret;
  }

  rgw_pool src_pool;
  rgw_pool dest_pool;

  const rgw_placement_rule *src_rule{nullptr};

  if (amanifest) {
    src_rule = &amanifest->get_tail_placement().placement_rule;
    ldpp_dout(dpp, 20) << __func__ << "(): manifest src_rule=" << src_rule->to_str() << dendl;
  }

  if (!src_rule || src_rule->empty()) {
    src_rule = &src_bucket_info.placement_rule;
  }

  if (!get_obj_data_pool(*src_rule, src_obj, &src_pool)) {
    ldpp_dout(dpp, 0) << "ERROR: failed to locate data pool for " << src_obj << dendl;
    return -EIO;
  }

  if (!get_obj_data_pool(dest_placement, dest_obj, &dest_pool)) {
    ldpp_dout(dpp, 0) << "ERROR: failed to locate data pool for " << dest_obj << dendl;
    return -EIO;
  }

  ldpp_dout(dpp, 20) << __func__ << "(): src_rule=" << src_rule->to_str() << " src_pool=" << src_pool
                             << " dest_rule=" << dest_placement.to_str() << " dest_pool=" << dest_pool << dendl;

  bool copy_data = (!amanifest) ||
    (*src_rule != dest_placement) ||
    (src_pool != dest_pool);

  bool copy_first = false;
  if (amanifest) {
    if (!amanifest->has_tail()) {
      copy_data = true;
    } else {
      uint64_t head_size = amanifest->get_head_size();

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
                         mtime, real_time(), attrs, olh_epoch, delete_at, petag, dpp, y);
  }

  /* This has been in for 2 years, so we can safely assume amanifest is not NULL */
  RGWObjManifest::obj_iterator miter = amanifest->obj_begin(dpp);

  if (copy_first) { // we need to copy first chunk, not increase refcount
    ++miter;
  }

  bufferlist first_chunk;

  const bool copy_itself = (dest_obj == src_obj);
  RGWObjManifest *pmanifest; 
  ldpp_dout(dpp, 20) << "dest_obj=" << dest_obj << " src_obj=" << src_obj << " copy_itself=" << (int)copy_itself << dendl;

  RGWRados::Object dest_op_target(this, dest_bucket_info, obj_ctx, dest_obj);
  RGWRados::Object::Write write_op(&dest_op_target);

  string tag;

  if (ptag) {
    tag = *ptag;
  }

  if (tag.empty()) {
    append_rand_alpha(cct, tag, tag, 32);
  }

  const req_context rctx{dpp, y, nullptr};
  std::unique_ptr<rgw::Aio> aio;
  rgw::AioResultList all_results;
  if (!copy_itself) {
    aio = rgw::make_throttle(cct->_conf->rgw_max_copy_obj_concurrent_io, y);
    attrs.erase(RGW_ATTR_TAIL_TAG);
    manifest = *amanifest;
    const rgw_bucket_placement& tail_placement = manifest.get_tail_placement();
    if (tail_placement.bucket.name.empty()) {
      manifest.set_tail_placement(tail_placement.placement_rule, src_obj.bucket);
    }
    string ref_tag;
    for (; miter != amanifest->obj_end(dpp); ++miter) {
      ObjectWriteOperation op;
      ref_tag = tag + '\0';
      cls_refcount_get(op, ref_tag, true);

      rgw_rados_ref obj;
      ret = rgw_get_rados_ref(dpp, driver->getRados()->get_rados_handle(),
			      miter.get_location().get_raw_obj(this),
			      &obj);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "failed to open rados context for " << obj << dendl;
        goto done_ret;
      }

      static constexpr uint64_t cost = 1; // 1 throttle unit per request
      static constexpr uint64_t id = 0; // ids unused
      rgw::AioResultList completed =
	aio->get(obj.obj, rgw::Aio::librados_op(obj.ioctx, std::move(op), y),
		 cost, id);
      ret = rgw::check_for_errors(completed);
      all_results.splice(all_results.end(), completed);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to copy obj=" << obj << ", the error code = " << ret << dendl;
        goto done_ret;
      }
    }

    rgw::AioResultList completed = aio->drain();
    ret = rgw::check_for_errors(completed);
    all_results.splice(all_results.end(), completed);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to drain ios, the error code = " << ret <<dendl;
      goto done_ret;
    }

    pmanifest = &manifest;
  } else {
    pmanifest = amanifest;
    /* don't send the object's tail for garbage collection */
    astate->keep_tail = true;
  }

  if (copy_first) {
    ret = read_op.read(0, max_chunk_size, first_chunk, y, dpp);
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

  ret = write_op.write_meta(obj_size, astate->accounted_size, attrs, rctx, trace);
  if (ret < 0) {
    goto done_ret;
  }

  return 0;

done_ret:
  if (!copy_itself) {

    /* wait all pending op done */
    rgw::AioResultList completed = aio->drain();
    all_results.splice(all_results.end(), completed);

    /* rollback reference */
    string ref_tag = tag + '\0';
    int ret2 = 0;
    for (auto& r : all_results) {
      if (r.result < 0) {
        continue; // skip errors
      }
      rgw_rados_ref obj;
      ret2 = rgw_get_rados_ref(dpp, get_rados_handle(), r.obj, &obj);
      if (ret2 < 0) {
        continue;
      }

      ObjectWriteOperation op;
      cls_refcount_put(op, ref_tag, true);

      static constexpr uint64_t cost = 1; // 1 throttle unit per request
      static constexpr uint64_t id = 0; // ids unused
      rgw::AioResultList completed =
	aio->get(obj.obj, rgw::Aio::librados_op(obj.ioctx, std::move(op), y),
		 cost, id);
      ret2 = rgw::check_for_errors(completed);
      if (ret2 < 0) {
        ldpp_dout(dpp, 0) << "ERROR: cleanup after error failed to drop reference on obj=" << r.obj << dendl;
      }
    }
    completed = aio->drain();
    ret2 = rgw::check_for_errors(completed);
    if (ret2 < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to drain rollback ios, the error code = " << ret2 <<dendl;
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
               rgw::sal::Attrs& attrs,
               uint64_t olh_epoch,
	       real_time delete_at,
               string *petag,
               const DoutPrefixProvider *dpp,
               optional_yield y,
               bool log_op)
{
  string tag;
  append_rand_alpha(cct, tag, tag, 32);

  auto aio = rgw::make_throttle(cct->_conf->rgw_put_obj_min_window_size, y);
  using namespace rgw::putobj;
  jspan_context no_trace{false, false};
  AtomicObjectProcessor processor(aio.get(), this, dest_bucket_info,
                                  &dest_placement, dest_bucket_info.owner,
                                  obj_ctx, dest_obj, olh_epoch, tag, dpp, y, no_trace);
  int ret = processor.prepare(y);
  if (ret < 0)
    return ret;

  off_t ofs = 0;

  do {
    bufferlist bl;
    ret = read_op.read(ofs, end, bl, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: fail to read object data, ret = " << ret << dendl;
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
      ldpp_dout(dpp, 0) << "ERROR: failed to read compression info" << dendl;
      return ret;
    }
    // pass original size if compressed
    accounted_size = compressed ? cs_info.orig_size : ofs;
  }

  const req_context rctx{dpp, y, nullptr};
  return processor.complete(accounted_size, etag, mtime, set_mtime, attrs, delete_at,
                            nullptr, nullptr, nullptr, nullptr, nullptr, rctx,
                            log_op ? rgw::sal::FLAG_LOG_OP : 0);
}

int RGWRados::transition_obj(RGWObjectCtx& obj_ctx,
                             RGWBucketInfo& bucket_info,
                             const rgw_obj& obj,
                             const rgw_placement_rule& placement_rule,
                             const real_time& mtime,
                             uint64_t olh_epoch,
                             const DoutPrefixProvider *dpp,
                             optional_yield y,
                             bool log_op)
{
  rgw::sal::Attrs attrs;
  real_time read_mtime;
  uint64_t obj_size;

  obj_ctx.set_atomic(obj);
  RGWRados::Object op_target(this, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrs;
  read_op.params.lastmod = &read_mtime;
  read_op.params.obj_size = &obj_size;

  int ret = read_op.prepare(y, dpp);
  if (ret < 0) {
    return ret;
  }

  if (read_mtime != mtime) {
    /* raced */
    ldpp_dout(dpp, 0) << __func__ << " ERROR: failed to transition obj(" << obj.key << ") read_mtime = " << read_mtime << " doesn't match mtime = " << mtime << dendl;
    return -ECANCELED;
  }

  attrs.erase(RGW_ATTR_ID_TAG);
  attrs.erase(RGW_ATTR_TAIL_TAG);

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
                      nullptr /* petag */,
                      dpp,
                      y,
                      log_op);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWRados::check_bucket_empty(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, optional_yield y)
{
  constexpr uint NUM_ENTRIES = 1000u;

  rgw_obj_index_key marker;
  string prefix;
  bool is_truncated;

  do {
    std::vector<rgw_bucket_dir_entry> ent_list;
    ent_list.reserve(NUM_ENTRIES);

    int r = cls_bucket_list_unordered(dpp,
                                      bucket_info,
                                      bucket_info.layout.current_index,
                                      RGW_NO_SHARD,
				      marker,
				      prefix,
				      NUM_ENTRIES,
				      true,
				      ent_list,
				      &is_truncated,
				      &marker,
                                      y);
    if (r < 0) {
      return r;
    }

    string ns;
    for (auto const& dirent : ent_list) {
      rgw_obj_key obj;

      if (rgw_obj_key::oid_to_key_in_ns(dirent.key.name, &obj, ns)) {
        return -ENOTEMPTY;
      }
    }
  } while (is_truncated);

  return 0;
}
  
/**
 * Delete a bucket.
 * bucket: the name of the bucket to delete
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_bucket(RGWBucketInfo& bucket_info, RGWObjVersionTracker& objv_tracker, optional_yield y, const DoutPrefixProvider *dpp, bool check_empty)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;
  int r = svc.bi_rados->open_bucket_index(dpp, bucket_info, std::nullopt, bucket_info.layout.current_index, &index_pool, &bucket_objs, nullptr);
  if (r < 0)
    return r;
  
  if (check_empty) {
    r = check_bucket_empty(dpp, bucket_info, y);
    if (r < 0) {
      return r;
    }
  }

  bool remove_ep = true;

  if (objv_tracker.read_version.empty()) {
    RGWBucketEntryPoint ep;
    r = ctl.bucket->read_bucket_entrypoint_info(bucket_info.bucket,
                                                &ep,
						null_yield,
                                                dpp,
                                                RGWBucketCtl::Bucket::GetParams()
                                                .set_objv_tracker(&objv_tracker));
    if (r < 0 ||
        (!bucket_info.bucket.bucket_id.empty() &&
         ep.bucket.bucket_id != bucket_info.bucket.bucket_id)) {
      if (r != -ENOENT) {
        ldpp_dout(dpp, 0) << "ERROR: read_bucket_entrypoint_info() bucket=" << bucket_info.bucket << " returned error: r=" << r << dendl;
        /* we have no idea what caused the error, will not try to remove it */
      }
      /* 
       * either failed to read bucket entrypoint, or it points to a different bucket instance than
       * requested
       */
      remove_ep = false;
    }
  }
 
  if (remove_ep) {
    r = ctl.bucket->remove_bucket_entrypoint_info(bucket_info.bucket, y, dpp,
                                                  RGWBucketCtl::Bucket::RemoveParams()
                                                  .set_objv_tracker(&objv_tracker));
    if (r < 0)
      return r;
  }

  /* if the bucket is not synced we can remove the meta file */
  if (!svc.zone->is_syncing_bucket_meta(bucket)) {
    RGWObjVersionTracker objv_tracker;
    r = ctl.bucket->remove_bucket_instance_info(bucket, bucket_info, y, dpp);
    if (r < 0) {
      return r;
    }

   /* remove bucket index objects asynchronously by best effort */
    (void) CLSRGWIssueBucketIndexClean(index_pool,
				       bucket_objs,
				       cct->_conf->rgw_bucket_index_max_aio)();
  }

  return 0;
}

int RGWRados::set_bucket_owner(rgw_bucket& bucket, ACLOwner& owner, const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWBucketInfo info;
  map<string, bufferlist> attrs;
  int r;

  if (bucket.bucket_id.empty()) {
    r = get_bucket_info(&svc, bucket.tenant, bucket.name, info, NULL, y, dpp, &attrs);
  } else {
    r = get_bucket_instance_info(bucket, info, nullptr, &attrs, y, dpp);
  }
  if (r < 0) {
    ldpp_dout(dpp, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << dendl;
    return r;
  }

  info.owner = owner.id;

  r = put_bucket_instance_info(info, false, real_time(), &attrs, dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << dendl;
    return r;
  }

  return 0;
}


int RGWRados::set_buckets_enabled(vector<rgw_bucket>& buckets, bool enabled, const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = 0;

  vector<rgw_bucket>::iterator iter;

  for (iter = buckets.begin(); iter != buckets.end(); ++iter) {
    rgw_bucket& bucket = *iter;
    if (enabled) {
      ldpp_dout(dpp, 20) << "enabling bucket name=" << bucket.name << dendl;
    } else {
      ldpp_dout(dpp, 20) << "disabling bucket name=" << bucket.name << dendl;
    }

    RGWBucketInfo info;
    map<string, bufferlist> attrs;
    int r = get_bucket_info(&svc, bucket.tenant, bucket.name, info, NULL, y, dpp, &attrs);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
      ret = r;
      continue;
    }
    if (enabled) {
      info.flags &= ~BUCKET_SUSPENDED;
    } else {
      info.flags |= BUCKET_SUSPENDED;
    }

    r = put_bucket_instance_info(info, false, real_time(), &attrs, dpp, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
      ret = r;
      continue;
    }
  }
  return ret;
}

int RGWRados::bucket_suspended(const DoutPrefixProvider *dpp, rgw_bucket& bucket, bool *suspended, optional_yield y)
{
  RGWBucketInfo bucket_info;
  int ret = get_bucket_info(&svc, bucket.tenant, bucket.name, bucket_info, NULL, y, dpp);
  if (ret < 0) {
    return ret;
  }

  *suspended = ((bucket_info.flags & BUCKET_SUSPENDED) != 0);
  return 0;
}

int RGWRados::Object::complete_atomic_modification(const DoutPrefixProvider *dpp, optional_yield y)
{
  if ((!manifest)|| state->keep_tail)
    return 0;

  cls_rgw_obj_chain chain;
  store->update_gc_chain(dpp, obj, *manifest, &chain);

  if (chain.empty()) {
    return 0;
  }

  string tag = (state->tail_tag.length() > 0 ? state->tail_tag.to_str() : state->obj_tag.to_str());
  if (store->gc == nullptr) {
    ldpp_dout(dpp, 0) << "deleting objects inline since gc isn't initialized" << dendl;
    //Delete objects inline just in case gc hasn't been initialised, prevents crashes
    store->delete_objs_inline(dpp, chain, tag);
  } else {
    auto [ret, leftover_chain] = store->gc->send_split_chain(chain, tag, y); // do it synchronously
    if (ret < 0 && leftover_chain) {
      //Delete objects inline if send chain to gc fails
      store->delete_objs_inline(dpp, *leftover_chain, tag);
    }
  }
  return 0;
}

void RGWRados::update_gc_chain(const DoutPrefixProvider *dpp, rgw_obj head_obj, RGWObjManifest& manifest, cls_rgw_obj_chain *chain)
{
  RGWObjManifest::obj_iterator iter;
  rgw_raw_obj raw_head;
  obj_to_raw(manifest.get_head_placement_rule(), head_obj, &raw_head);
  for (iter = manifest.obj_begin(dpp); iter != manifest.obj_end(dpp); ++iter) {
    const rgw_raw_obj& mobj = iter.get_location().get_raw_obj(this);
    if (mobj == raw_head)
      continue;
    cls_rgw_obj_key key(mobj.oid);
    chain->push_obj(mobj.pool.to_str(), key, mobj.loc);
  }
}

std::tuple<int, std::optional<cls_rgw_obj_chain>> RGWRados::send_chain_to_gc(cls_rgw_obj_chain& chain, const string& tag, optional_yield y)
{
  if (chain.empty()) {
    return {0, std::nullopt};
  }

  return gc->send_split_chain(chain, tag, y);
}

void RGWRados::delete_objs_inline(const DoutPrefixProvider *dpp, cls_rgw_obj_chain& chain, const string& tag)
{
  string last_pool;
  std::unique_ptr<IoCtx> ctx(new IoCtx);
  int ret = 0;
  for (auto liter = chain.objs.begin(); liter != chain.objs.end(); ++liter) {
    cls_rgw_obj& obj = *liter;
    if (obj.pool != last_pool) {
      ctx.reset(new IoCtx);
      ret = rgw_init_ioctx(dpp, get_rados_handle(), obj.pool, *ctx);
      if (ret < 0) {
        last_pool = "";
        ldpp_dout(dpp, 0) << "ERROR: failed to create ioctx pool=" <<
        obj.pool << dendl;
        continue;
      }
      last_pool = obj.pool;
    }
    ctx->locator_set_key(obj.loc);
    const string& oid = obj.key.name; /* just stored raw oid there */
    ldpp_dout(dpp, 5) << "delete_objs_inline: removing " << obj.pool <<
    ":" << obj.key.name << dendl;
    ObjectWriteOperation op;
    cls_refcount_put(op, tag, true);
    ret = ctx->operate(oid, &op);
    if (ret < 0) {
      ldpp_dout(dpp, 5) << "delete_objs_inline: refcount put returned error " << ret << dendl;
    }
  }
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

int RGWRados::bucket_check_index(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info,
				 map<RGWObjCategory, RGWStorageStats> *existing_stats,
				 map<RGWObjCategory, RGWStorageStats> *calculated_stats)
{
  librados::IoCtx index_pool;

  // key - bucket index object id
  // value - bucket index check OP returned result with the given bucket index object (shard)
  map<int, string> oids;

  int ret = svc.bi_rados->open_bucket_index(dpp, bucket_info, std::nullopt, bucket_info.layout.current_index, &index_pool, &oids, nullptr);
  if (ret < 0) {
    return ret;
  }

  // declare and pre-populate
  map<int, struct rgw_cls_check_index_ret> bucket_objs_ret;
  for (auto& iter : oids) {
    bucket_objs_ret.emplace(iter.first, rgw_cls_check_index_ret());
  }

  ret = CLSRGWIssueBucketCheck(index_pool, oids, bucket_objs_ret, cct->_conf->rgw_bucket_index_max_aio)();
  if (ret < 0) {
    return ret;
  }

  // aggregate results (from different shards if there are any)
  for (const auto& iter : bucket_objs_ret) {
    accumulate_raw_stats(iter.second.existing_header, *existing_stats);
    accumulate_raw_stats(iter.second.calculated_header, *calculated_stats);
  }

  return 0;
}

int RGWRados::bucket_rebuild_index(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;

  int r = svc.bi_rados->open_bucket_index(dpp, bucket_info, std::nullopt, bucket_info.layout.current_index, &index_pool, &bucket_objs, nullptr);
  if (r < 0) {
    return r;
  }

  return CLSRGWIssueBucketRebuild(index_pool, bucket_objs, cct->_conf->rgw_bucket_index_max_aio)();
}

static int resync_encrypted_multipart(const DoutPrefixProvider* dpp,
                                      optional_yield y, RGWRados* store,
                                      RGWBucketInfo& bucket_info,
                                      RGWObjectCtx& obj_ctx,
                                      const RGWObjState& state)
{
  // only overwrite if the tag hasn't changed
  obj_ctx.set_atomic(state.obj);

  // make a tiny adjustment to the existing mtime so that fetch_remote_obj()
  // won't return ERR_NOT_MODIFIED when resyncing the object
  const auto set_mtime = state.mtime + std::chrono::nanoseconds(1);

  // use set_attrs() to update the mtime in a bucket index transaction so the
  // change is recorded in bilog and datalog entries. this will cause any peer
  // zones to resync the object
  auto add_attrs = std::map<std::string, bufferlist>{
    { RGW_ATTR_PREFIX "resync-encrypted-multipart", bufferlist{} },
  };

  return store->set_attrs(dpp, &obj_ctx, bucket_info, state.obj,
                          add_attrs, nullptr, y, set_mtime);
}

static void try_resync_encrypted_multipart(const DoutPrefixProvider* dpp,
                                           optional_yield y, RGWRados* store,
                                           RGWBucketInfo& bucket_info,
                                           RGWObjectCtx& obj_ctx,
                                           const rgw_bucket_dir_entry& dirent,
                                           Formatter* f)
{
  const auto obj = rgw_obj{bucket_info.bucket, dirent.key};

  RGWObjState* astate = nullptr;
  RGWObjManifest* manifest = nullptr;
  constexpr bool follow_olh = false; // dirent will have version ids
  int ret = store->get_obj_state(dpp, &obj_ctx, bucket_info, obj,
                                 &astate, &manifest, follow_olh, y);
  if (ret < 0) {
    ldpp_dout(dpp, 4) << obj << " does not exist" << dendl;
    return;
  }

  // check whether the object is encrypted
  if (auto i = astate->attrset.find(RGW_ATTR_CRYPT_MODE);
      i == astate->attrset.end()) {
    ldpp_dout(dpp, 4) << obj << " is not encrypted" << dendl;
    return;
  }

  // check whether the object is multipart
  if (!manifest) {
    ldpp_dout(dpp, 4) << obj << " has no manifest so is not multipart" << dendl;
    return;
  }
  const RGWObjManifest::obj_iterator end = manifest->obj_end(dpp);
  if (end.get_cur_part_id() == 0) {
    ldpp_dout(dpp, 4) << obj << " manifest is not multipart" << dendl;
    return;
  }

  ret = resync_encrypted_multipart(dpp, y, store, bucket_info,
                                   obj_ctx, *astate);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to update " << obj
        << ": " << cpp_strerror(ret) << dendl;
    return;
  }

  f->open_object_section("object");
  encode_json("name", obj.key.name, f);
  if (!obj.key.instance.empty()) {
    encode_json("version", obj.key.instance, f);
  }
  encode_json("mtime", astate->mtime, f);
  f->close_section(); // "object"
}

int RGWRados::bucket_resync_encrypted_multipart(const DoutPrefixProvider* dpp,
                                                optional_yield y,
                                                rgw::sal::RadosStore* driver,
                                                RGWBucketInfo& bucket_info,
                                                const std::string& marker,
                                                RGWFormatterFlusher& flusher)
{
  RGWRados::Bucket target(this, bucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.marker.name = marker;
  list_op.params.enforce_ns = true; // only empty ns
  list_op.params.list_versions = true;
  list_op.params.allow_unordered = true;

  /* List bucket entries in chunks. */
  static constexpr int MAX_LIST_OBJS = 100;
  std::vector<rgw_bucket_dir_entry> entries;
  entries.reserve(MAX_LIST_OBJS);

  int processed = 0;
  bool is_truncated = true;

  Formatter* f = flusher.get_formatter();
  f->open_array_section("progress");

  do {
    int ret = list_op.list_objects(dpp, MAX_LIST_OBJS, &entries, nullptr,
                                   &is_truncated, y);
    if (ret < 0) {
      return ret;
    }

    f->open_object_section("batch");
    f->open_array_section("modified");

    for (const auto& dirent : entries) {
      RGWObjectCtx obj_ctx{driver};
      try_resync_encrypted_multipart(dpp, y, this, bucket_info,
                                     obj_ctx, dirent, f);
    }

    f->close_section(); // "modified"

    processed += entries.size();
    encode_json("total processed", processed, f);
    encode_json("marker", list_op.get_next_marker().name, f);
    f->close_section(); // "batch"

    flusher.flush(); // flush after each 'chunk'
  } while (is_truncated);

  f->close_section(); // "progress" array
  return 0;
}

int RGWRados::bucket_set_reshard(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const cls_rgw_bucket_instance_entry& entry)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;

  int r = svc.bi_rados->open_bucket_index(dpp, bucket_info, std::nullopt, bucket_info.layout.current_index, &index_pool, &bucket_objs, nullptr);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
      ": unable to open bucket index, r=" << r << " (" <<
      cpp_strerror(-r) << ")" << dendl;
    return r;
  }

  r = CLSRGWIssueSetBucketResharding(index_pool, bucket_objs, entry, cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
      ": unable to issue set bucket resharding, r=" << r << " (" <<
      cpp_strerror(-r) << ")" << dendl;
  }
  return r;
}

int RGWRados::defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx* octx, RGWBucketInfo& bucket_info, const rgw_obj& obj, optional_yield y)
{
  std::string oid, key;
  get_obj_bucket_and_oid_loc(obj, oid, key);
  if (!octx)
    return 0;

  RGWObjState *state = NULL;
  RGWObjManifest *manifest = nullptr;

  int r = get_obj_state(dpp, octx, bucket_info, obj, &state, &manifest, false, y);
  if (r < 0)
    return r;

  if (!state->is_atomic) {
    ldpp_dout(dpp, 20) << "state for obj=" << obj << " is not atomic, not deferring gc operation" << dendl;
    return -EINVAL;
  }

  string tag;

  if (state->tail_tag.length() > 0) {
    tag = state->tail_tag.c_str();
  } else if (state->obj_tag.length() > 0) {
    tag = state->obj_tag.c_str();
  } else {
    ldpp_dout(dpp, 20) << "state->obj_tag is empty, not deferring gc operation" << dendl;
    return -EINVAL;
  }

  ldpp_dout(dpp, 0) << "defer chain tag=" << tag << dendl;

  cls_rgw_obj_chain chain;
  update_gc_chain(dpp, state->obj, *manifest, &chain);
  return gc->async_defer_chain(tag, chain);
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

struct tombstone_entry {
  ceph::real_time mtime;
  uint32_t zone_short_id;
  uint64_t pg_ver;

  tombstone_entry() = default;
  explicit tombstone_entry(const RGWObjState& state)
    : mtime(state.mtime), zone_short_id(state.zone_short_id),
      pg_ver(state.pg_ver) {}
};

/**
 * Delete an object.
 * bucket: name of the bucket storing the object
 * obj: name of the object to delete
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::Object::Delete::delete_obj(optional_yield y, const DoutPrefixProvider *dpp, bool log_op)
{
  RGWRados *store = target->get_store();
  const rgw_obj& src_obj = target->get_obj();
  const string& instance = src_obj.key.instance;
  rgw_obj obj = target->get_obj();

  if (instance == "null") {
    obj.key.instance.clear();
  }

  bool explicit_marker_version = (!params.marker_version_id.empty());

  if (params.versioning_status & BUCKET_VERSIONED || explicit_marker_version) {
    bool add_log = log_op && store->svc.zone->need_to_log_data();

    if (instance.empty() || explicit_marker_version) {
      rgw_obj marker = obj;
      marker.key.instance.clear();

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

      meta.owner = params.obj_owner.id.to_str();
      meta.owner_display_name = params.obj_owner.display_name;

      if (real_clock::is_zero(params.mtime)) {
        meta.mtime = real_clock::now();
      } else {
        meta.mtime = params.mtime;
      }

      int r = store->set_olh(dpp, target->get_ctx(), target->get_bucket_info(), marker, true,
                             &meta, params.olh_epoch, params.unmod_since, params.high_precision_time,
                             y, params.zones_trace, add_log);
      if (r < 0) {
        return r;
      }
    } else {
      rgw_bucket_dir_entry dirent;

      int r = store->bi_get_instance(dpp, target->get_bucket_info(), obj, &dirent, y);
      if (r < 0) {
        return r;
      }
      result.delete_marker = dirent.is_delete_marker();
      r = store->unlink_obj_instance(dpp, target->get_ctx(), target->get_bucket_info(), obj, params.olh_epoch,
                                     y, params.zones_trace, add_log);
      if (r < 0) {
        return r;
      }
      result.version_id = instance;
    }

    BucketShard *bs = nullptr;
    int r = target->get_bucket_shard(&bs, dpp, y);
    if (r < 0) {
      ldpp_dout(dpp, 5) << "failed to get BucketShard object: r=" << r << dendl;
      return r;
    }

    if (add_log) {
      add_datalog_entry(dpp, store->svc.datalog_rados,
                        target->get_bucket_info(), bs->shard_id, y);
    }

    return 0;
  }

  rgw_rados_ref ref;
  int r = store->get_obj_head_ref(dpp, target->get_bucket_info(), obj, &ref);
  if (r < 0) {
    return r;
  }

  RGWObjState *state;
  RGWObjManifest *manifest = nullptr;
  r = target->get_state(dpp, &state, &manifest, false, y);
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

    ldpp_dout(dpp, 10) << "If-UnModified-Since: " << params.unmod_since << " Last-Modified: " << ctime << dendl;
    if (ctime > unmod) {
      return -ERR_PRECONDITION_FAILED;
    }

    /* only delete object if mtime is less than or equal to params.unmod_since */
    store->cls_obj_check_mtime(op, params.unmod_since, params.high_precision_time, CLS_RGW_CHECK_TIME_MTIME_LE);
  }
  uint64_t obj_accounted_size = state->accounted_size;

  if(params.abortmp) {
    obj_accounted_size = params.parts_accounted_size;
  }

  if (!real_clock::is_zero(params.expiration_time)) {
    bufferlist bl;
    real_time delete_at;

    if (state->get_attr(RGW_ATTR_DELETE_AT, bl)) {
      try {
        auto iter = bl.cbegin();
        decode(delete_at, iter);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 0) << "ERROR: couldn't decode RGW_ATTR_DELETE_AT" << dendl;
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

  r = target->prepare_atomic_modification(dpp, op, false, NULL, NULL, NULL, true, false, y);
  if (r < 0)
    return r;

  RGWBucketInfo& bucket_info = target->get_bucket_info();

  RGWRados::Bucket bop(store, bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, obj);

  index_op.set_zones_trace(params.zones_trace);
  index_op.set_bilog_flags(params.bilog_flags);

  r = index_op.prepare(dpp, CLS_RGW_OP_DEL, &state->write_tag, y, log_op);
  if (r < 0)
    return r;

  store->remove_rgw_head_obj(op);

  auto& ioctx = ref.ioctx;
  r = rgw_rados_operate(dpp, ioctx, ref.obj.oid, &op, y);

  /* raced with another operation, object state is indeterminate */
  const bool need_invalidate = (r == -ECANCELED);

  int64_t poolid = ioctx.get_id();
  if (r >= 0) {
    tombstone_cache_t *obj_tombstone_cache = store->get_tombstone_cache();
    if (obj_tombstone_cache) {
      tombstone_entry entry{*state};
      obj_tombstone_cache->add(obj, entry);
    }
    r = index_op.complete_del(dpp, poolid, ioctx.get_last_version(), state->mtime, params.remove_objs, y, log_op);

    int ret = target->complete_atomic_modification(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: complete_atomic_modification returned ret=" << ret << dendl;
    }
    /* other than that, no need to propagate error */
  } else {
    int ret = index_op.cancel(dpp, params.remove_objs, y, log_op);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: index_op.cancel() returned ret=" << ret << dendl;
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

int RGWRados::delete_obj(const DoutPrefixProvider *dpp,
                         RGWObjectCtx& obj_ctx,
                         const RGWBucketInfo& bucket_info,
                         const rgw_obj& obj,
                         int versioning_status, optional_yield y,// versioning flags defined in enum RGWBucketFlags
                         uint16_t bilog_flags,
                         const real_time& expiration_time,
                         rgw_zone_set *zones_trace,
                         bool log_op)
{
  RGWRados::Object del_target(this, bucket_info, obj_ctx, obj);
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket_info.owner;
  del_op.params.versioning_status = versioning_status;
  del_op.params.bilog_flags = bilog_flags;
  del_op.params.expiration_time = expiration_time;
  del_op.params.zones_trace = zones_trace;

  return del_op.delete_obj(y, dpp, log_op ? rgw::sal::FLAG_LOG_OP : 0);
}

int RGWRados::delete_raw_obj(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, optional_yield y)
{
  rgw_rados_ref ref;
  int r = get_raw_obj_ref(dpp, obj, &ref);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;

  op.remove();
  r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::delete_obj_index(const rgw_obj& obj, ceph::real_time mtime,
			       const DoutPrefixProvider *dpp, optional_yield y)
{
  std::string oid, key;
  get_obj_bucket_and_oid_loc(obj, oid, key);

  RGWBucketInfo bucket_info;
  int ret = get_bucket_instance_info(obj.bucket, bucket_info, NULL, NULL, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "() get_bucket_instance_info(bucket=" << obj.bucket << ") returned ret=" << ret << dendl;
    return ret;
  }

  RGWRados::Bucket bop(this, bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, obj);

  return index_op.complete_del(dpp, -1 /* pool */, 0, mtime, nullptr, y);
}

static void generate_fake_tag(const DoutPrefixProvider *dpp, RGWRados* store, map<string, bufferlist>& attrset, RGWObjManifest& manifest, bufferlist& manifest_bl, bufferlist& tag_bl)
{
  string tag;

  RGWObjManifest::obj_iterator mi = manifest.obj_begin(dpp);
  if (mi != manifest.obj_end(dpp)) {
    if (manifest.has_tail()) // first object usually points at the head, let's skip to a more unique part
      ++mi;
    tag = mi.get_location().get_raw_obj(store).oid;
    tag.append("_");
  }

  unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char md5_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  hash.Update((const unsigned char *)manifest_bl.c_str(), manifest_bl.length());

  map<string, bufferlist>::iterator iter = attrset.find(RGW_ATTR_ETAG);
  if (iter != attrset.end()) {
    bufferlist& bl = iter->second;
    hash.Update((const unsigned char *)bl.c_str(), bl.length());
  }

  hash.Final(md5);
  buf_to_hex(md5, CEPH_CRYPTO_MD5_DIGESTSIZE, md5_str);
  tag.append(md5_str);

  ldpp_dout(dpp, 10) << "generate_fake_tag new tag=" << tag << dendl;

  tag_bl.append(tag.c_str(), tag.size() + 1);
}

static bool is_olh(map<string, bufferlist>& attrs)
{
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_OLH_VER);
  return (iter != attrs.end());
}

static bool has_olh_tag(map<string, bufferlist>& attrs)
{
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_OLH_ID_TAG);
  return (iter != attrs.end());
}

int RGWRados::get_olh_target_state(const DoutPrefixProvider *dpp, RGWObjectCtx&
				   obj_ctx, RGWBucketInfo& bucket_info,
				   const rgw_obj& obj, RGWObjState *olh_state,
				   RGWObjStateManifest **psm, optional_yield y)
{
  ceph_assert(olh_state->is_olh);

  rgw_obj target;
  int r = RGWRados::follow_olh(dpp, bucket_info, obj_ctx, olh_state,
                               obj, &target, y); /* might return -EAGAIN */
  if (r < 0) {
    return r;
  }

  return get_obj_state(dpp, &obj_ctx, bucket_info, target, psm, false, y);
}

int RGWRados::get_obj_state_impl(const DoutPrefixProvider *dpp, RGWObjectCtx *octx,
                                 RGWBucketInfo& bucket_info, const rgw_obj& obj,
                                 RGWObjStateManifest** psm, bool follow_olh,
                                 optional_yield y, bool assume_noent)
{
  if (obj.empty()) {
    return -EINVAL;
  }

  bool need_follow_olh = follow_olh && obj.key.instance.empty();

  RGWObjStateManifest *sm = octx->get_state(obj);
  RGWObjState *s = &(sm->state);
  ldpp_dout(dpp, 20) << "get_obj_state: octx=" << (void *)octx << " obj=" << obj << " state=" << (void *)s << " s->prefetch_data=" << s->prefetch_data << dendl;
  *psm = sm;
  if (s->has_attrs) {
    if (s->is_olh && need_follow_olh) {
      return get_olh_target_state(dpp, *octx, bucket_info, obj, s, psm, y);
    }
    return 0;
  }

  s->obj = obj;

  rgw_raw_obj raw_obj;
  obj_to_raw(bucket_info.placement_rule, obj, &raw_obj);

  int r = -ENOENT;

  if (!assume_noent) {
    r = RGWRados::raw_obj_stat(dpp, raw_obj, &s->size, &s->mtime, &s->epoch, &s->attrset, (s->prefetch_data ? &s->data : NULL), NULL, y);
  }

  if (r == -ENOENT) {
    s->exists = false;
    s->has_attrs = true;
    tombstone_entry entry;
    if (obj_tombstone_cache && obj_tombstone_cache->find(obj, entry)) {
      s->mtime = entry.mtime;
      s->zone_short_id = entry.zone_short_id;
      s->pg_ver = entry.pg_ver;
      ldpp_dout(dpp, 20) << __func__ << "(): found obj in tombstone cache: obj=" << obj
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
      bletag = std::move(newbl);
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
      ldpp_dout(dpp, 0) << "ERROR: could not decode compression info for object: " << obj << dendl;
      return -EIO;
    }
  }

  if (iter = s->attrset.find(RGW_ATTR_SHADOW_OBJ); iter != s->attrset.end()) {
    const bufferlist& bl = iter->second;
    auto it = bl.begin();
    it.copy(bl.length(), s->shadow_obj);
    s->shadow_obj[bl.length()] = '\0';
  }
  if (iter = s->attrset.find(RGW_ATTR_ID_TAG); iter != s->attrset.end()) {
    s->obj_tag = iter->second;
  }
  if (iter = s->attrset.find(RGW_ATTR_TAIL_TAG); iter != s->attrset.end()) {
    s->tail_tag = iter->second;
  }

  if (iter = s->attrset.find(RGW_ATTR_MANIFEST); iter != s->attrset.end()) {
    bufferlist manifest_bl = iter->second;
    auto miter = manifest_bl.cbegin();
    try {
      sm->manifest.emplace();
      decode(*sm->manifest, miter);
      sm->manifest->set_head(bucket_info.placement_rule, obj, s->size); /* patch manifest to reflect the head we just read, some manifests might be
                                             broken due to old bugs */
      s->size = sm->manifest->get_obj_size();
      if (!compressed)
        s->accounted_size = s->size;
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: couldn't decode manifest" << dendl;
      return -EIO;
    }
    ldpp_dout(dpp, 10) << "manifest: total_size = " << sm->manifest->get_obj_size() << dendl;
    if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>() && \
	sm->manifest->has_explicit_objs()) {
      RGWObjManifest::obj_iterator mi;
      for (mi = sm->manifest->obj_begin(dpp); mi != sm->manifest->obj_end(dpp); ++mi) {
        ldpp_dout(dpp, 20) << "manifest: ofs=" << mi.get_ofs() << " loc=" << mi.get_location().get_raw_obj(this) << dendl;
      }
    }

    if (!s->obj_tag.length()) {
      /*
       * Uh oh, something's wrong, object with manifest should have tag. Let's
       * create one out of the manifest, would be unique
       */
      generate_fake_tag(dpp, this, s->attrset, *sm->manifest, manifest_bl, s->obj_tag);
      s->fake_tag = true;
    }
  }
  if (iter = s->attrset.find(RGW_ATTR_PG_VER); iter != s->attrset.end()) {
    const bufferlist& pg_ver_bl = iter->second;
    if (pg_ver_bl.length()) {
      auto pgbl = pg_ver_bl.cbegin();
      try {
        decode(s->pg_ver, pgbl);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 0) << "ERROR: couldn't decode pg ver attr for object " << s->obj << ", non-critical error, ignoring" << dendl;
      }
    }
  }
  if (iter = s->attrset.find(RGW_ATTR_SOURCE_ZONE); iter != s->attrset.end()) {
    const bufferlist& zone_short_id_bl = iter->second;
    if (zone_short_id_bl.length()) {
      auto zbl = zone_short_id_bl.cbegin();
      try {
        decode(s->zone_short_id, zbl);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 0) << "ERROR: couldn't decode zone short id attr for object " << s->obj << ", non-critical error, ignoring" << dendl;
      }
    }
  }
  if (s->obj_tag.length()) {
    ldpp_dout(dpp, 20) << "get_obj_state: setting s->obj_tag to " << s->obj_tag.c_str() << dendl;
  } else {
    ldpp_dout(dpp, 20) << "get_obj_state: s->obj_tag was set empty" << dendl;
  }

  /* an object might not be olh yet, but could have olh id tag, so we should set it anyway if
   * it exist, and not only if is_olh() returns true
   */
  if (iter = s->attrset.find(RGW_ATTR_OLH_ID_TAG); iter != s->attrset.end()) {
    s->olh_tag = iter->second;
  }

  if (is_olh(s->attrset)) {
    s->is_olh = true;

    ldpp_dout(dpp, 20) << __func__ << ": setting s->olh_tag to " << string(s->olh_tag.c_str(), s->olh_tag.length()) << dendl;

    if (need_follow_olh) {
      return get_olh_target_state(dpp, *octx, bucket_info, obj, s, psm, y);
    } else if (obj.key.have_null_instance() && !sm->manifest) {
      // read null version, and the head object only have olh info
      s->exists = false;
      return -ENOENT;
    }
  }

  return 0;
}

int RGWRados::get_obj_state(const DoutPrefixProvider *dpp, RGWObjectCtx *octx,
                            RGWBucketInfo& bucket_info, const rgw_obj& obj,
                            RGWObjStateManifest** psm, bool follow_olh,
                            optional_yield y, bool assume_noent)
{
  int ret;

  do {
    ret = get_obj_state_impl(dpp, octx, bucket_info, obj, psm,
                             follow_olh, y, assume_noent);
  } while (ret == -EAGAIN);

  return ret;
}

int RGWRados::get_obj_state(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx,
                            RGWBucketInfo& bucket_info, const rgw_obj& obj,
                            RGWObjState** pstate, RGWObjManifest** pmanifest,
                            bool follow_olh, optional_yield y, bool assume_noent)
{
  RGWObjStateManifest* sm = nullptr;
  int r = get_obj_state(dpp, rctx, bucket_info, obj, &sm,
                        follow_olh, y, assume_noent);
  if (r < 0) {
    return r;
  }
  if (pstate) {
    *pstate = &sm->state;
  }
  if (pmanifest) {
    if (sm->manifest) {
      *pmanifest = &(*sm->manifest);
    } else {
      *pmanifest = nullptr;
    }
  }
  return 0;
}

int RGWRados::Object::get_manifest(const DoutPrefixProvider *dpp, RGWObjManifest **pmanifest, optional_yield y)
{
  RGWObjState *astate;
  int r = get_state(dpp, &astate, pmanifest, true, y);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWRados::Object::Read::get_attr(const DoutPrefixProvider *dpp, const char *name, bufferlist& dest, optional_yield y)
{
  RGWObjState *state;
  RGWObjManifest *manifest = nullptr;
  int r = source->get_state(dpp, &state, &manifest, true, y);
  if (r < 0)
    return r;
  if (!state->exists)
    return -ENOENT;
  if (!state->get_attr(name, dest))
    return -ENODATA;

  return 0;
}

int RGWRados::Object::Stat::stat_async(const DoutPrefixProvider *dpp)
{
  RGWObjectCtx& ctx = source->get_ctx();
  rgw_obj& obj = source->get_obj();
  RGWRados *store = source->get_store();

  RGWObjStateManifest *sm = ctx.get_state(obj);
  result.obj = obj;
  if (sm->state.has_attrs) {
    state.ret = 0;
    result.size = sm->state.size;
    result.mtime = ceph::real_clock::to_timespec(sm->state.mtime);
    result.attrs = sm->state.attrset;
    result.manifest = sm->manifest;
    return 0;
  }

  string oid;
  string loc;
  get_obj_bucket_and_oid_loc(obj, oid, loc);

  int r = store->get_obj_head_ioctx(dpp, source->get_bucket_info(), obj, &state.io_ctx);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  op.stat2(&result.size, &result.mtime, NULL);
  op.getxattrs(&result.attrs, NULL);
  state.completion = librados::Rados::aio_create_completion(nullptr, nullptr);
  state.io_ctx.locator_set_key(loc);
  r = state.io_ctx.aio_operate(oid, state.completion, &op, NULL);
  if (r < 0) {
    ldpp_dout(dpp, 5) << __func__
						   << ": ERROR: aio_operate() returned ret=" << r
						   << dendl;
    return r;
  }

  return 0;
}


int RGWRados::Object::Stat::wait(const DoutPrefixProvider *dpp)
{
  if (!state.completion) {
    return state.ret;
  }

  state.completion->wait_for_complete();
  state.ret = state.completion->get_return_value();
  state.completion->release();

  if (state.ret != 0) {
    return state.ret;
  }

  return finish(dpp);
}

int RGWRados::Object::Stat::finish(const DoutPrefixProvider *dpp)
{
  map<string, bufferlist>::iterator iter = result.attrs.find(RGW_ATTR_MANIFEST);
  if (iter != result.attrs.end()) {
    bufferlist& bl = iter->second;
    auto biter = bl.cbegin();
    try {
      result.manifest.emplace();
      decode(*result.manifest, biter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << ": failed to decode manifest"  << dendl;
      return -EIO;
    }
  }

  return 0;
}

int RGWRados::append_atomic_test(const DoutPrefixProvider *dpp, RGWObjectCtx* octx,
                                 RGWBucketInfo& bucket_info, const rgw_obj& obj,
                                 ObjectOperation& op, RGWObjState **pstate,
				 RGWObjManifest** pmanifest, optional_yield y)
{
  if (!octx)
    return 0;

  int r = get_obj_state(dpp, octx, bucket_info, obj, pstate, pmanifest, false, y);
  if (r < 0)
    return r;

  return append_atomic_test(dpp, *pstate, op);
}

int RGWRados::append_atomic_test(const DoutPrefixProvider *dpp,
                                 const RGWObjState* state,
                                 librados::ObjectOperation& op)
{
  if (!state->is_atomic) {
    ldpp_dout(dpp, 20) << "state for obj=" << state->obj << " is not atomic, not appending atomic test" << dendl;
    return 0;
  }

  if (state->obj_tag.length() > 0 && !state->fake_tag) {// check for backward compatibility
    op.cmpxattr(RGW_ATTR_ID_TAG, LIBRADOS_CMPXATTR_OP_EQ, state->obj_tag);
  } else {
    ldpp_dout(dpp, 20) << "state->obj_tag is empty, not appending atomic test" << dendl;
  }
  return 0;
}

int RGWRados::Object::get_state(const DoutPrefixProvider *dpp, RGWObjState **pstate, RGWObjManifest **pmanifest, bool follow_olh, optional_yield y, bool assume_noent)
{
  return store->get_obj_state(dpp, &ctx, bucket_info, obj, pstate, pmanifest, follow_olh, y, assume_noent);
}

void RGWRados::Object::invalidate_state()
{
  ctx.invalidate(obj);
}

int RGWRados::Object::prepare_atomic_modification(const DoutPrefixProvider *dpp,
                                                  ObjectWriteOperation& op, bool reset_obj, const string *ptag,
                                                  const char *if_match, const char *if_nomatch, bool removal_op,
                                                  bool modify_tail, optional_yield y)
{
  int r = get_state(dpp, &state, &manifest, false, y);
  if (r < 0)
    return r;

  bool need_guard = ((manifest) || (state->obj_tag.length() != 0) ||
                     if_match != NULL || if_nomatch != NULL) &&
                     (!state->fake_tag);

  if (!state->is_atomic) {
    ldpp_dout(dpp, 20) << "prepare_atomic_modification: state is not atomic. state=" << (void *)state << dendl;

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

  ldpp_dout(dpp, 10) << "setting object write_tag=" << state->write_tag << dendl;

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
int RGWRados::set_attr(const DoutPrefixProvider *dpp, RGWObjectCtx* octx, RGWBucketInfo& bucket_info, const rgw_obj& obj, const char *name, bufferlist& bl, optional_yield y)
{
  map<string, bufferlist> attrs;
  attrs[name] = bl;
  return set_attrs(dpp, octx, bucket_info, obj, attrs, NULL, y);
}

int RGWRados::set_attrs(const DoutPrefixProvider *dpp, RGWObjectCtx* octx, RGWBucketInfo& bucket_info, const rgw_obj& src_obj,
                        map<string, bufferlist>& attrs,
                        map<string, bufferlist>* rmattrs,
                        optional_yield y,
                        ceph::real_time set_mtime /* = zero() */)
{
  rgw_obj obj = src_obj;
  if (obj.key.instance == "null") {
    obj.key.instance.clear();
  }

  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;
  RGWObjState *state = NULL;
  RGWObjManifest *manifest = nullptr;

  r = append_atomic_test(dpp, octx, bucket_info, obj, op, &state, &manifest, y);
  if (r < 0)
    return r;

  // ensure null version object exist
  if (src_obj.key.instance == "null" && !manifest) {
    return -ENOENT;
  }

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

        obj_expirer->hint_add(dpp, ts, bucket.tenant, bucket.name, bucket.bucket_id, obj_key);
      } catch (buffer::error& err) {
	ldpp_dout(dpp, 0) << "ERROR: failed to decode " RGW_ATTR_DELETE_AT << " attr" << dendl;
      }
    }
  }

  if (!op.size())
    return 0;

  bufferlist bl;
  RGWRados::Bucket bop(this, bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, obj);

  if (state) {
    string tag;
    append_rand_alpha(cct, tag, tag, 32);
    state->write_tag = tag;
    r = index_op.prepare(dpp, CLS_RGW_OP_ADD, &state->write_tag, y);

    if (r < 0)
      return r;

    bl.append(tag.c_str(), tag.size() + 1);
    op.setxattr(RGW_ATTR_ID_TAG,  bl);
  }


  /* As per https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html, 
   * the only way for users to modify object metadata is to make a copy of the object and
   * set the metadata.
   * Hence do not update mtime for any other attr changes */
  real_time mtime = state->mtime;
  if (set_mtime != ceph::real_clock::zero()) {
    mtime = set_mtime;
  }
  struct timespec mtime_ts = real_clock::to_timespec(mtime);
  op.mtime2(&mtime_ts);
  auto& ioctx = ref.ioctx;
  r = rgw_rados_operate(dpp, ioctx, ref.obj.oid, &op, y);
  if (state) {
    if (r >= 0) {
      bufferlist acl_bl;
      if (iter = attrs.find(RGW_ATTR_ACL); iter != attrs.end()) {
        acl_bl = iter->second;
      }
      std::string etag;
      if (iter = attrs.find(RGW_ATTR_ETAG); iter != attrs.end()) {
        etag = rgw_bl_str(iter->second);
      }
      std::string content_type;
      if (iter = attrs.find(RGW_ATTR_CONTENT_TYPE); iter != attrs.end()) {
        content_type = rgw_bl_str(iter->second);
      }
      string storage_class;
      if (iter = attrs.find(RGW_ATTR_STORAGE_CLASS); iter != attrs.end()) {
        storage_class = rgw_bl_str(iter->second);
      }
      uint64_t epoch = ioctx.get_last_version();
      int64_t poolid = ioctx.get_id();
      r = index_op.complete(dpp, poolid, epoch, state->size, state->accounted_size,
                            mtime, etag, content_type, storage_class, &acl_bl,
                            RGWObjCategory::Main, nullptr, y);
    } else {
      int ret = index_op.cancel(dpp, nullptr, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: complete_update_index_cancel() returned ret=" << ret << dendl;
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

    auto iter = state->attrset.find(RGW_ATTR_ID_TAG);
    if (iter != state->attrset.end()) {
      iter->second = state->obj_tag;
    }

    state->mtime = mtime;
  }

  return 0;
}

static int get_part_obj_state(const DoutPrefixProvider* dpp, optional_yield y,
                              RGWRados* store, RGWBucketInfo& bucket_info,
                              RGWObjectCtx* rctx, RGWObjManifest* manifest,
                              int part_num, int* parts_count, bool prefetch,
                              RGWObjState** pstate, RGWObjManifest** pmanifest)
{
  if (!manifest) {
    return -ERR_INVALID_PART;
  }
  // navigate to the requested part in the manifest
  RGWObjManifest::obj_iterator end = manifest->obj_end(dpp);
  if (end.get_cur_part_id() == 0) { // not multipart
    ldpp_dout(dpp, 20) << "object does not have a multipart manifest" << dendl;
    return -ERR_INVALID_PART;
  }
  if (parts_count) {
    *parts_count = end.get_cur_part_id() - 1;
  }
  ldpp_dout(dpp, 20) << "seeking to part #" << part_num
      << " in the object manifest" << dendl;
  RGWObjManifest::obj_iterator iter = manifest->obj_find_part(dpp, part_num);
  if (iter == end) { // part number not found
    ldpp_dout(dpp, 20) << "failed to find part #" << part_num
        << " in the object manifest" << dendl;
    return -ERR_INVALID_PART;
  }
  auto head_obj = iter.get_location().get_head_obj();
  if (!head_obj) { // iterator points to a tail object
    ldpp_dout(dpp, 20) << "object manifest for part #" << part_num
        << " points to a tail object" << dendl;
    return -ERR_INVALID_PART;
  }
  const auto part_offset = iter.get_ofs();

  // read the part's head object
  if (prefetch) {
    rctx->set_prefetch_data(*head_obj);
  }
  RGWObjStateManifest* sm = nullptr;
  constexpr bool follow_olh = false; // parts aren't versioned
  int r = store->get_obj_state(dpp, rctx, bucket_info, *head_obj,
                               &sm, follow_olh, y);
  if (r < 0) {
    return r;
  }
  *pstate = &sm->state;

  // if the part has its own manifest, use it directly
  if (sm->manifest) {
    *pmanifest = &*sm->manifest;
    return 0;
  }

  // create a new manifest for just this part
  sm->manifest.emplace();
  RGWObjManifest& part_manifest = *sm->manifest;
  part_manifest.set_multipart_part_rule(iter.get_stripe_size(), part_num);

  if (auto& prefix = iter.get_cur_override_prefix(); !prefix.empty()) {
    // the part was reuploaded with a different prefix
    part_manifest.set_prefix(prefix);
  } else {
    part_manifest.set_prefix(manifest->get_prefix());
  }

  RGWObjManifest::generator gen;
  gen.create_begin(store->ctx(), &part_manifest,
                   manifest->get_head_placement_rule(),
                   &manifest->get_tail_placement().placement_rule,
                   head_obj->bucket, *head_obj);

  // copy each of the part's stripes into the new manifest. the final call to
  // create_next() uses the starting offset of the next part
  do {
    ++iter;
    gen.create_next(iter.get_ofs() - part_offset);
  } while (iter.get_cur_part_id() == part_num);

  // update the object size
  sm->state.size = part_manifest.get_obj_size();

  *pmanifest = &part_manifest;
  return 0;
}

int RGWRados::Object::Read::prepare(optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWRados *store = source->get_store();
  CephContext *cct = store->ctx();
  RGWObjectCtx& obj_ctx = source->get_ctx();

  bufferlist etag;

  map<string, bufferlist>::iterator iter;

  bool part_prefetch = false;
  if (params.part_num) {
    // prefetch from the part's head object instead of the multipart head
    auto sm = obj_ctx.get_state(source->get_obj());
    part_prefetch = std::exchange(sm->state.prefetch_data, false);
  }

  RGWObjState *astate;
  RGWObjManifest *manifest = nullptr;
  int r = source->get_state(dpp, &astate, &manifest, true, y);
  if (r < 0)
    return r;

  if (!astate->exists) {
    return -ENOENT;
  }

  RGWBucketInfo& bucket_info = source->get_bucket_info();

  if (params.part_num) {
    int parts_count = 0;
    // use the manifest to redirect to the requested part number
    r = get_part_obj_state(dpp, y, store, bucket_info, &source->get_ctx(),
                           manifest, *params.part_num, &parts_count,
                           part_prefetch, &astate, &manifest);
    if (r == -ERR_INVALID_PART && *params.part_num == 1) {
      // for non-multipart uploads, treat requests for the first part as a
      // request for the entire range. this behavior is expected by the java
      // sdk's TransferManager.download()
      ldpp_dout(dpp, 4) << "requested part #" << *params.part_num
          << ": " << cpp_strerror(r) << dendl;
    } else if (r < 0) {
      ldpp_dout(dpp, 4) << "failed to read part #" << *params.part_num
          << ": " << cpp_strerror(r) << dendl;
      return -ERR_INVALID_PART;
    } else if (!astate->exists) {
      ldpp_dout(dpp, 4) << "part #" << *params.part_num
          << " does not exist" << dendl;
      return -ERR_INVALID_PART;
    } else {
      params.parts_count = parts_count;
    }
  }

  state.obj = astate->obj;
  store->obj_to_raw(bucket_info.placement_rule, state.obj, &state.head_obj);

  state.cur_pool = state.head_obj.pool;
  state.cur_ioctx = &state.io_ctxs[state.cur_pool];

  r = store->get_obj_head_ioctx(dpp, bucket_info, state.obj, state.cur_ioctx);
  if (r < 0) {
    return r;
  }
  if (params.target_obj) {
    *params.target_obj = state.obj;
  }
  if (params.attrs) {
    *params.attrs = astate->attrset;
    if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      for (iter = params.attrs->begin(); iter != params.attrs->end(); ++iter) {
        ldpp_dout(dpp, 20) << "Read xattr rgw_rados: " << iter->first << dendl;
      }
    }
  }

  if (params.lastmod) {
    *params.lastmod = astate->mtime;
  }

  /* Convert all times go GMT to make them compatible */
  if (conds.mod_ptr || conds.unmod_ptr) {
    obj_time_weight src_weight;
    src_weight.init(astate);
    src_weight.high_precision = conds.high_precision_time;

    obj_time_weight dest_weight;
    dest_weight.high_precision = conds.high_precision_time;

    if (conds.mod_ptr && !conds.if_nomatch) {
      dest_weight.init(*conds.mod_ptr, conds.mod_zone_id, conds.mod_pg_ver);
      ldpp_dout(dpp, 10) << "If-Modified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (!(dest_weight < src_weight)) {
        return -ERR_NOT_MODIFIED;
      }
    }

    if (conds.unmod_ptr && !conds.if_match) {
      dest_weight.init(*conds.unmod_ptr, conds.mod_zone_id, conds.mod_pg_ver);
      ldpp_dout(dpp, 10) << "If-UnModified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (dest_weight < src_weight) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
  if (conds.if_match || conds.if_nomatch) {
    r = get_attr(dpp, RGW_ATTR_ETAG, etag, y);
    if (r < 0)
      return r;

    if (conds.if_match) {
      string if_match_str = rgw_string_unquote(conds.if_match);
      ldpp_dout(dpp, 10) << "ETag: " << string(etag.c_str(), etag.length()) << " " << " If-Match: " << if_match_str << dendl;
      if (if_match_str.compare(0, etag.length(), etag.c_str(), etag.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }

    if (conds.if_nomatch) {
      string if_nomatch_str = rgw_string_unquote(conds.if_nomatch);
      ldpp_dout(dpp, 10) << "ETag: " << string(etag.c_str(), etag.length()) << " " << " If-NoMatch: " << if_nomatch_str << dendl;
      if (if_nomatch_str.compare(0, etag.length(), etag.c_str(), etag.length()) == 0) {
        return -ERR_NOT_MODIFIED;
      }
    }
  }

  if (params.obj_size) {
    *params.obj_size = astate->size;
  }
  if (params.epoch) {
    *params.epoch = astate->epoch;
  }

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

int RGWRados::Bucket::UpdateIndex::guard_reshard(const DoutPrefixProvider *dpp, const rgw_obj& obj_instance, BucketShard **pbs, std::function<int(BucketShard *)> call, optional_yield y)
{
  RGWRados *store = target->get_store();
  BucketShard *bs = nullptr;
  int r;

#define NUM_RESHARD_RETRIES 10
  for (int i = 0; i < NUM_RESHARD_RETRIES; ++i) {
    int ret = get_bucket_shard(&bs, dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to get BucketShard object. obj=" << 
        obj_instance.key << ". ret=" << ret << dendl;
      return ret;
    }

    r = call(bs);
    if (r != -ERR_BUSY_RESHARDING) {
      break;
    }

    ldpp_dout(dpp, 10) <<
      "NOTICE: resharding operation on bucket index detected, blocking. obj=" << 
      obj_instance.key << dendl;

    r = store->block_while_resharding(bs, obj_instance, target->bucket_info, y, dpp);
    if (r == -ERR_BUSY_RESHARDING) {
      ldpp_dout(dpp, 10) << __func__ <<
	" NOTICE: block_while_resharding() still busy. obj=" <<
        obj_instance.key << dendl;
      continue;
    } else if (r < 0) {
      ldpp_dout(dpp, 0) << __func__ <<
	" ERROR: block_while_resharding() failed. obj=" <<
        obj_instance.key << ". ret=" << cpp_strerror(-r) << dendl;
      return r;
    }

    ldpp_dout(dpp, 20) << "reshard completion identified. obj=" << obj_instance.key << dendl;
    i = 0; /* resharding is finished, make sure we can retry */
    invalidate_bs();
  } // for loop

  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: bucket shard callback failed. obj=" << 
      obj_instance.key << ". ret=" << cpp_strerror(-r) << dendl;
    return r;
  }

  if (pbs) {
    *pbs = bs;
  }

  return 0;
}

int RGWRados::Bucket::UpdateIndex::prepare(const DoutPrefixProvider *dpp, RGWModifyOp op, const string *write_tag,
                                           optional_yield y, bool log_op)
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

  bool add_log = log_op && store->svc.zone->need_to_log_data();

  int r = guard_reshard(dpp, obj, nullptr, [&](BucketShard *bs) -> int {
				   return store->cls_obj_prepare_op(dpp, *bs, op, optag, obj, bilog_flags, y, zones_trace, add_log);
				 }, y);
  if (r < 0) {
    return r;
  }

  prepared = true;
  return 0;
}

int RGWRados::Bucket::UpdateIndex::complete(const DoutPrefixProvider *dpp, int64_t poolid, uint64_t epoch,
                                            uint64_t size, uint64_t accounted_size,
                                            const ceph::real_time& ut, const string& etag,
                                            const string& content_type, const string& storage_class,
                                            bufferlist *acl_bl,
                                            RGWObjCategory category,
                                            list<rgw_obj_index_key> *remove_objs,
					    optional_yield y,
					    const string *user_data,
                                            bool appendable,
                                            bool log_op)
{
  if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();
  BucketShard *bs = nullptr;

  int ret = get_bucket_shard(&bs, dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "failed to get BucketShard object: ret=" << ret << dendl;
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
    int ret = store->decode_policy(dpp, *acl_bl, &owner);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: could not decode policy ret=" << ret << dendl;
    }
  }
  ent.meta.owner = owner.id.to_str();
  ent.meta.owner_display_name = owner.display_name;
  ent.meta.content_type = content_type;
  ent.meta.appendable = appendable;

  bool add_log = log_op && store->svc.zone->need_to_log_data();

  ret = store->cls_obj_complete_add(*bs, obj, optag, poolid, epoch, ent, category, remove_objs, bilog_flags, zones_trace, add_log);
  if (add_log) {
    add_datalog_entry(dpp, store->svc.datalog_rados,
                      target->bucket_info, bs->shard_id, y);
  }

  return ret;
}

int RGWRados::Bucket::UpdateIndex::complete_del(const DoutPrefixProvider *dpp,
                                                int64_t poolid, uint64_t epoch,
                                                real_time& removed_mtime,
                                                list<rgw_obj_index_key> *remove_objs,
                                                optional_yield y,
                                                bool log_op)
{
  if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();
  BucketShard *bs = nullptr;

  int ret = get_bucket_shard(&bs, dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "failed to get BucketShard object: ret=" << ret << dendl;
    return ret;
  }

  bool add_log = log_op && store->svc.zone->need_to_log_data();

  ret = store->cls_obj_complete_del(*bs, optag, poolid, epoch, obj, removed_mtime, remove_objs, bilog_flags, zones_trace, add_log);

  if (add_log) {
    add_datalog_entry(dpp, store->svc.datalog_rados,
                      target->bucket_info, bs->shard_id, y);
  }

  return ret;
}


int RGWRados::Bucket::UpdateIndex::cancel(const DoutPrefixProvider *dpp,
                                          list<rgw_obj_index_key> *remove_objs,
                                          optional_yield y,
                                          bool log_op)
{
    if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();
  BucketShard *bs;

  bool add_log = log_op && store->svc.zone->need_to_log_data();

  int ret = guard_reshard(dpp, obj, &bs, [&](BucketShard *bs) -> int {
				 return store->cls_obj_complete_cancel(*bs, optag, obj, remove_objs, bilog_flags, zones_trace, add_log);
			       }, y);

  if (add_log) {
    /*
     * need to update data log anyhow, so that whoever follows needs to update its internal markers
     * for following the specific bucket shard log. Otherwise they end up staying behind, and users
     * have no way to tell that they're all caught up
     */
    add_datalog_entry(dpp, store->svc.datalog_rados,
                      target->bucket_info, bs->shard_id, y);
  }

  return ret;
}

/*
 * Read up through index `end` inclusive. Number of bytes read is up
 * to `end - ofs + 1`.
 */
int RGWRados::Object::Read::read(int64_t ofs, int64_t end,
				 bufferlist& bl, optional_yield y,
				 const DoutPrefixProvider *dpp)
{
  RGWRados *store = source->get_store();

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
  RGWObjManifest *manifest = nullptr;
  int r = source->get_state(dpp, &astate, &manifest, true, y);
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

  if (manifest && manifest->has_tail()) {
    /* now get the relevant object part */
    RGWObjManifest::obj_iterator iter = manifest->obj_find(dpp, ofs);

    uint64_t stripe_ofs = iter.get_stripe_ofs();
    read_obj = iter.get_location().get_raw_obj(store);
    len = std::min(len, iter.get_stripe_size() - (ofs - stripe_ofs));
    read_ofs = iter.location_ofs() + (ofs - stripe_ofs);
    reading_from_head = (read_obj == state.head_obj);
  } else {
    read_obj = state.head_obj;
  }

  r = store->get_max_chunk_size(read_obj.pool, &max_chunk_size, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get max_chunk_size() for pool " << read_obj.pool << dendl;
    return r;
  }

  if (len > max_chunk_size)
    len = max_chunk_size;


  read_len = len;

  if (reading_from_head) {
    /* only when reading from the head object do we need to do the atomic test */
    r = store->append_atomic_test(dpp, &source->get_ctx(), source->get_bucket_info(), state.obj, op, &astate, &manifest, y);
    if (r < 0)
      return r;

    // astate can be modified by append_atomic_test
    // coverity[check_after_deref:SUPPRESS]
    if (astate && astate->prefetch_data) {
      if (!ofs && astate->data.length() >= len) {
        bl = astate->data;
        return bl.length();
      }

      if (ofs < astate->data.length()) {
        unsigned copy_len = std::min((uint64_t)astate->data.length() - ofs, len);
        astate->data.begin(ofs).copy(copy_len, bl);
        read_len -= copy_len;
        read_ofs += copy_len;
        if (!read_len)
	  return bl.length();

        merge_bl = true;
        pbl = &read_bl;
      }
    }
  }

  ldpp_dout(dpp, 20) << "rados->read obj-ofs=" << ofs << " read_ofs=" << read_ofs << " read_len=" << read_len << dendl;
  op.read(read_ofs, read_len, pbl, NULL);

  if (state.cur_pool != read_obj.pool) {
    auto iter = state.io_ctxs.find(read_obj.pool);
    if (iter == state.io_ctxs.end()) {
      state.cur_ioctx = &state.io_ctxs[read_obj.pool];
      r = store->open_pool_ctx(dpp, read_obj.pool, *state.cur_ioctx, false, true);
      if (r < 0) {
        ldpp_dout(dpp, 20) << "ERROR: failed to open pool context for pool=" << read_obj.pool << " r=" << r << dendl;
        return r;
      }
    } else {
      state.cur_ioctx = &iter->second;
    }
    state.cur_pool = read_obj.pool;
  }

  state.cur_ioctx->locator_set_key(read_obj.loc);

  r = state.cur_ioctx->operate(read_obj.oid, &op, NULL);
  ldpp_dout(dpp, 20) << "rados->read r=" << r << " bl.length=" << bl.length() << dendl;

  if (r < 0) {
    return r;
  }

  if (merge_bl) {
    bl.append(read_bl);
  }

  return bl.length();
}

int get_obj_data::flush(rgw::AioResultList&& results) {
  int r = rgw::check_for_errors(results);
  if (r < 0) {
    return r;
  }
  std::list<bufferlist> bl_list;

  auto cmp = [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; };
  results.sort(cmp); // merge() requires results to be sorted first
  completed.merge(results, cmp); // merge results in sorted order

  while (!completed.empty() && completed.front().id == offset) {
    auto bl = std::move(completed.front().data);

    bl_list.push_back(bl);
    offset += bl.length();
    int r = client_cb->handle_data(bl, 0, bl.length());
    if (r < 0) {
      return r;
    }

    if (rgwrados->get_use_datacache()) {
      const std::lock_guard l(d3n_get_data.d3n_lock);
      auto oid = completed.front().obj.oid;
      if (bl.length() <= g_conf()->rgw_get_obj_max_req_size && !d3n_bypass_cache_write) {
        lsubdout(g_ceph_context, rgw_datacache, 10) << "D3nDataCache: " << __func__ << "(): bl.length <= rgw_get_obj_max_req_size (default 4MB) - write to datacache, bl.length=" << bl.length() << dendl;
        rgwrados->d3n_data_cache->put(bl, bl.length(), oid);
      } else {
        lsubdout(g_ceph_context, rgw_datacache, 10) << "D3nDataCache: " << __func__ << "(): not writing to datacache - bl.length > rgw_get_obj_max_req_size (default 4MB), bl.length=" << bl.length() << " or d3n_bypass_cache_write=" << d3n_bypass_cache_write << dendl;
      }
    }
    completed.pop_front_and_dispose(std::default_delete<rgw::AioResultEntry>{});
  }
  return 0;
}

static int _get_obj_iterate_cb(const DoutPrefixProvider *dpp,
                               const rgw_raw_obj& read_obj, off_t obj_ofs,
                               off_t read_ofs, off_t len, bool is_head_obj,
                               RGWObjState *astate, void *arg)
{
  struct get_obj_data* d = static_cast<struct get_obj_data*>(arg);
  return d->rgwrados->get_obj_iterate_cb(dpp, read_obj, obj_ofs, read_ofs, len,
                                      is_head_obj, astate, arg);
}

int RGWRados::get_obj_iterate_cb(const DoutPrefixProvider *dpp,
                                 const rgw_raw_obj& read_obj, off_t obj_ofs,
                                 off_t read_ofs, off_t len, bool is_head_obj,
                                 RGWObjState *astate, void *arg)
{
  ObjectReadOperation op;
  struct get_obj_data* d = static_cast<struct get_obj_data*>(arg);
  string oid, key;

  if (is_head_obj) {
    /* only when reading from the head object do we need to do the atomic test */
    int r = append_atomic_test(dpp, astate, op);
    if (r < 0)
      return r;

    // astate can be modified by append_atomic_test
    // coverity[check_after_deref:SUPPRESS]
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

  rgw_rados_ref obj;
  int r = rgw_get_rados_ref(dpp, d->rgwrados->get_rados_handle(), read_obj,
			    &obj);
  if (r < 0) {
    ldpp_dout(dpp, 4) << "failed to open rados context for " << read_obj << dendl;
    return r;
  }

  ldpp_dout(dpp, 20) << "rados->get_obj_iterate_cb oid=" << read_obj.oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
  op.read(read_ofs, len, nullptr, nullptr);

  const uint64_t cost = len;
  const uint64_t id = obj_ofs; // use logical object offset for sorting replies

  auto completed = d->aio->get(obj.obj, rgw::Aio::librados_op(obj.ioctx, std::move(op), d->yield), cost, id);

  return d->flush(std::move(completed));
}

int RGWRados::Object::Read::iterate(const DoutPrefixProvider *dpp, int64_t ofs, int64_t end, RGWGetDataCB *cb,
                                    optional_yield y)
{
  RGWRados *store = source->get_store();
  CephContext *cct = store->ctx();
  const uint64_t chunk_size = cct->_conf->rgw_get_obj_max_req_size;
  const uint64_t window_size = cct->_conf->rgw_get_obj_window_size;

  auto aio = rgw::make_throttle(window_size, y);
  get_obj_data data(store, cb, &*aio, ofs, y);

  int r = store->iterate_obj(dpp, source->get_ctx(), source->get_bucket_info(), state.obj,
                             ofs, end, chunk_size, _get_obj_iterate_cb, &data, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "iterate_obj() failed with " << r << dendl;
    data.cancel(); // drain completions without writing back to client
    return r;
  }

  return data.drain();
}

int RGWRados::iterate_obj(const DoutPrefixProvider *dpp, RGWObjectCtx& obj_ctx,
                          RGWBucketInfo& bucket_info, const rgw_obj& obj,
                          off_t ofs, off_t end, uint64_t max_chunk_size,
                          iterate_obj_cb cb, void *arg, optional_yield y)
{
  rgw_raw_obj head_obj;
  rgw_raw_obj read_obj;
  uint64_t read_ofs = ofs;
  uint64_t len;
  bool reading_from_head = true;
  RGWObjState *astate = NULL;
  RGWObjManifest *manifest = nullptr;

  obj_to_raw(bucket_info.placement_rule, obj, &head_obj);

  int r = get_obj_state(dpp, &obj_ctx, bucket_info, obj, &astate, &manifest, false, y);
  if (r < 0) {
    return r;
  }

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (manifest) {
    /* now get the relevant object stripe */
    RGWObjManifest::obj_iterator iter = manifest->obj_find(dpp, ofs);

    RGWObjManifest::obj_iterator obj_end = manifest->obj_end(dpp);

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
        r = cb(dpp, read_obj, ofs, read_ofs, read_len, reading_from_head, astate, arg);
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

      r = cb(dpp, read_obj, ofs, ofs, read_len, reading_from_head, astate, arg);
      if (r < 0) {
	return r;
      }

      len -= read_len;
      ofs += read_len;
    }
  }

  return 0;
}

int RGWRados::obj_operate(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, ObjectWriteOperation *op, optional_yield y)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }

  return rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, op, y);
}

int RGWRados::obj_operate(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, ObjectReadOperation *op, optional_yield y)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }

  bufferlist outbl;

  return rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, op, &outbl, y);
}

void RGWRados::olh_cancel_modification(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info,
                                       RGWObjState& state, const rgw_obj& olh_obj,
                                       const std::string& op_tag, optional_yield y)
{
  if (cct->_conf->rgw_debug_inject_olh_cancel_modification_err) {
    // simulate the scenario where we fail to remove the pending xattr
    return;
  }

  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, olh_obj, &ref);
  if (r < 0) {
    ldpp_dout(dpp, 0) << __func__ << " target_obj=" << olh_obj << " get_obj_head_ref() returned " << r << dendl;
    return;
  }
  string attr_name = RGW_ATTR_OLH_PENDING_PREFIX;
  attr_name.append(op_tag);

  // first remove the relevant pending prefix
  ObjectWriteOperation op;
  bucket_index_guard_olh_op(dpp, state, op);
  op.rmxattr(attr_name.c_str());
  r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
  if (r < 0) {
    if (r != -ENOENT && r != -ECANCELED) {
      ldpp_dout(dpp, 0) << __func__ << " target_obj=" << olh_obj << " rmxattr rgw_rados_operate() returned " << r << dendl;
    }
    return;
  }
    
  if (auto iter = state.attrset.find(RGW_ATTR_OLH_INFO); iter == state.attrset.end()) {
    // attempt to remove the OLH object if there are no pending ops,
    // its olh info attr is empty, and its tag hasn't changed
    ObjectWriteOperation rm_op;
    bucket_index_guard_olh_op(dpp, state, rm_op);
    rm_op.cmpxattr(RGW_ATTR_OLH_INFO, CEPH_OSD_CMPXATTR_OP_EQ, bufferlist());
    cls_obj_check_prefix_exist(rm_op, RGW_ATTR_OLH_PENDING_PREFIX, true);
    rm_op.remove();
    r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &rm_op, y);
  }
  if (r < 0 && (r != -ENOENT && r != -ECANCELED)) {
    ldpp_dout(dpp, 0) << __func__ << " target_obj=" << olh_obj << " olh rm rgw_rados_operate() returned " << r << dendl;
  }
}

int RGWRados::olh_init_modification_impl(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, string *op_tag, optional_yield y)
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
    bucket_index_guard_olh_op(dpp, state, op);
  } else if (state.exists) {
    // This is the case where a null versioned object already exists for this key
    // but it hasn't been initialized as an OLH object yet. We immediately add
    // the RGW_ATTR_OLH_INFO attr so that the OLH points back to itself and
    // therefore effectively makes this an unobservable modification.
    op.cmpxattr(RGW_ATTR_OLH_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ, bufferlist());
    RGWOLHInfo info;
    info.target = olh_obj;
    info.removed = false;
    bufferlist bl;
    encode(info, bl);
    op.setxattr(RGW_ATTR_OLH_INFO, bl);
  }

  if (!has_tag) {
    /* obj tag */
    string obj_tag = gen_rand_alphanumeric_lower(cct, 32);

    bufferlist bl;
    bl.append(obj_tag.c_str(), obj_tag.size());
    op.setxattr(RGW_ATTR_ID_TAG, bl);

    state.attrset[RGW_ATTR_ID_TAG] = bl;
    state.obj_tag = bl;

    /* olh tag */
    string olh_tag = gen_rand_alphanumeric_lower(cct, 32);

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

  string s = gen_rand_alphanumeric_lower(cct, OLH_PENDING_TAG_LEN - op_tag->size());

  op_tag->append(s);

  string attr_name = RGW_ATTR_OLH_PENDING_PREFIX;
  attr_name.append(*op_tag);

  op.setxattr(attr_name.c_str(), bl);

  int ret = obj_operate(dpp, bucket_info, olh_obj, &op, y);
  if (ret < 0) {
    return ret;
  }

  state.exists = true;
  state.attrset[attr_name] = bl;

  return 0;
}

int RGWRados::olh_init_modification(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& obj, string *op_tag, optional_yield y)
{
  int ret;

  ret = olh_init_modification_impl(dpp, bucket_info, state, obj, op_tag, y);
  if (ret == -EEXIST) {
    ret = -ECANCELED;
  }

  return ret;
}

int RGWRados::guard_reshard(const DoutPrefixProvider *dpp,
                            BucketShard *bs,
			    const rgw_obj& obj_instance,
			    RGWBucketInfo& bucket_info,
			    std::function<int(BucketShard *)> call, optional_yield y)
{
  rgw_obj obj;
  const rgw_obj *pobj = &obj_instance;
  int r;

  for (int i = 0; i < NUM_RESHARD_RETRIES; ++i) {
    r = bs->init(pobj->bucket, *pobj, nullptr /* no RGWBucketInfo */, dpp, y);
    if (r < 0) {
      ldpp_dout(dpp, 5) << "bs.init() returned ret=" << r << dendl;
      return r;
    }

    r = call(bs);
    if (r != -ERR_BUSY_RESHARDING) {
      break;
    }

    ldpp_dout(dpp, 10) <<
      "NOTICE: resharding operation on bucket index detected, blocking. obj=" <<
      obj_instance.key << dendl;

    r = block_while_resharding(bs, obj_instance, bucket_info, y, dpp);
    if (r == -ERR_BUSY_RESHARDING) {
      ldpp_dout(dpp, 10) << __func__ <<
	" NOTICE: block_while_resharding() still busy. obj=" <<
        obj_instance.key << dendl;
      continue;
    } else if (r < 0) {
      ldpp_dout(dpp, 0) << __func__ <<
	" ERROR: block_while_resharding() failed. obj=" <<
        obj_instance.key << ". ret=" << cpp_strerror(-r) << dendl;
      return r;
    }

    ldpp_dout(dpp, 20) << "reshard completion identified" << dendl;
    i = 0; /* resharding is finished, make sure we can retry */
  } // for loop

  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: bucket shard callback failed. obj=" << 
      obj_instance.key << ". ret=" << cpp_strerror(-r) << dendl;
    return r;
  }

  return 0;
}


int RGWRados::block_while_resharding(RGWRados::BucketShard *bs,
                                     const rgw_obj& obj_instance,
                                     RGWBucketInfo& bucket_info,
                                     optional_yield y,
                                     const DoutPrefixProvider *dpp)
{
  int ret = 0;
  cls_rgw_bucket_instance_entry entry;

  // gets loaded by fetch_new_bucket_info; can be used by
  // clear_resharding
  std::map<std::string, bufferlist> bucket_attrs;

  // since we want to run this recovery code from two distinct places,
  // let's just put it in a lambda so we can easily re-use; if the
  // lambda successfully fetches a new bucket id, it sets
  // new_bucket_id and returns 0, otherwise it returns a negative
  // error code
  auto fetch_new_bucket_info =
    [this, bs, &obj_instance, &bucket_info, &bucket_attrs, &y, dpp](const std::string& log_tag) -> int {
    int ret = get_bucket_info(&svc, bs->bucket.tenant, bs->bucket.name,
			      bucket_info, nullptr, y, dpp, &bucket_attrs);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ <<
	" ERROR: failed to refresh bucket info after reshard at " <<
	log_tag << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    ret = bs->init(dpp, bucket_info, obj_instance, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ <<
	" ERROR: failed to refresh bucket shard generation after reshard at " <<
	log_tag << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    const auto gen = bucket_info.layout.logs.empty() ? -1 : bucket_info.layout.logs.back().gen;
    ldpp_dout(dpp, 20) << __func__ <<
      " INFO: refreshed bucket info after reshard at " <<
      log_tag << ". new shard_id=" << bs->shard_id << ". gen=" << gen << dendl;

    return 0;
  }; // lambda fetch_new_bucket_info

  constexpr int num_retries = 10;
  for (int i = 1; i <= num_retries; i++) { // nb: 1-based for loop
    auto& ref = bs->bucket_obj;
    ret = cls_rgw_get_bucket_resharding(ref.ioctx, ref.obj.oid, &entry);
    if (ret == -ENOENT) {
      ret = fetch_new_bucket_info("get_bucket_resharding_failed");
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	  " failed to refresh bucket info after reshard when get bucket "
	  "resharding failed, error: " << cpp_strerror(-ret) << dendl;
	return ret;
      }
    } else if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ <<
	" ERROR: failed to get bucket resharding : " << cpp_strerror(-ret) <<
	dendl;
      return ret;
    }

    if (!entry.resharding_in_progress()) {
      ret = fetch_new_bucket_info("get_bucket_resharding_succeeded");
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
          " failed to refresh bucket info after reshard when get bucket "
          "resharding succeeded, error: " << cpp_strerror(-ret) << dendl;
      }
      return ret;
    }

    ldpp_dout(dpp, 20) << __func__ << " NOTICE: reshard still in progress; " <<
      (i < num_retries ? "retrying" : "too many retries") << dendl;

    if (i == num_retries) {
      break;
    }

    // If bucket is erroneously marked as resharding (e.g., crash or
    // other error) then fix it. If we can take the bucket reshard
    // lock then it means no other resharding should be taking place,
    // and we're free to clear the flags.
    {
      // since we expect to do this rarely, we'll do our work in a
      // block and erase our work after each try

      RGWObjectCtx obj_ctx(this->driver);
      const rgw_bucket& b = bs->bucket;
      std::string bucket_id = b.get_key();
      RGWBucketReshardLock reshard_lock(this->driver, bucket_info, true);
      ret = reshard_lock.lock(dpp);
      if (ret == -ENOENT) {
	continue;
      } else if (ret < 0) {
	ldpp_dout(dpp, 20) << __func__ <<
	  " ERROR: failed to take reshard lock for bucket " <<
	  bucket_id << "; expected if resharding underway" << dendl;

        // the reshard may have finished, so refresh bucket_obj to avoid
        // acquiring reshard lock conflicts
        ret = fetch_new_bucket_info("trying_to_refresh_bucket_info");
        if (ret < 0) {
          ldpp_dout(dpp, 0) << __func__ <<
            " ERROR: failed to refresh bucket_obj for bucket " <<
            bs->bucket.name << dendl;
          return ret;
        }
      } else {
	ldpp_dout(dpp, 10) << __func__ <<
	  " INFO: was able to take reshard lock for bucket " <<
	  bucket_id << dendl;
        // the reshard may have finished, so call clear_resharding()
        // with its current bucket info; ALSO this will load
        // bucket_attrs for call to clear_resharding below
        ret = fetch_new_bucket_info("trying_to_clear_resharding");
        if (ret < 0) {
	  reshard_lock.unlock();
	  ldpp_dout(dpp, 0) << __func__ <<
	    " ERROR: failed to update bucket info before clear resharding for bucket " <<
	    bucket_id << dendl;
          continue; // try again
        }

	ret = RGWBucketReshard::clear_resharding(this->driver, bucket_info, bucket_attrs, dpp, y);
	reshard_lock.unlock();
	if (ret == -ENOENT) {
	  ldpp_dout(dpp, 5) << __func__ <<
	    " INFO: no need to reset reshard flags; old shards apparently"
	    " removed after successful resharding of bucket " <<
	    bucket_id << dendl;
	  continue; // immediately test again
	} else if (ret < 0) {
	  ldpp_dout(dpp, 0) << __func__ <<
	    " ERROR: failed to clear resharding flags for bucket " <<
	    bucket_id << ", " << cpp_strerror(-ret) << dendl;
	  // wait and then test again
	} else {
	  ldpp_dout(dpp, 5) << __func__ <<
	    " INFO: apparently successfully cleared resharding flags for "
	    "bucket " << bucket_id << dendl;
	  continue; // if we apparently succeed immediately test again
	} // if clear resharding succeeded
      } // if taking of lock succeeded
    } // block to encapsulate recovery from incomplete reshard

    ret = reshard_wait->wait(y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ <<
	" ERROR: bucket is still resharding, please retry" << dendl;
      return ret;
    }
  } // for loop

  ldpp_dout(dpp, 0) << __func__ <<
    " ERROR: bucket is still resharding, please retry" << dendl;
  return -ERR_BUSY_RESHARDING;
}

int RGWRados::bucket_index_link_olh(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info,
                                    RGWObjState& olh_state, const rgw_obj& obj_instance,
                                    bool delete_marker, const string& op_tag,
                                    struct rgw_bucket_dir_entry_meta *meta,
                                    uint64_t olh_epoch,
                                    real_time unmod_since, bool high_precision_time,
				    optional_yield y,
                                    rgw_zone_set *_zones_trace, bool log_data_change)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj_instance, &ref);
  if (r < 0) {
    return r;
  }

  rgw_zone_set zones_trace;
  if (_zones_trace) {
    zones_trace = *_zones_trace;
  }
  zones_trace.insert(svc.zone->get_zone().id, bucket_info.bucket.get_key());

  BucketShard bs(this);

  r = guard_reshard(dpp, &bs, obj_instance, bucket_info,
		    [&](BucketShard *bs) -> int {
		      cls_rgw_obj_key key(obj_instance.key.get_index_key_name(), obj_instance.key.instance);
		      auto& ref = bs->bucket_obj;
		      librados::ObjectWriteOperation op;
		      op.assert_exists(); // bucket index shard must exist
		      cls_rgw_guard_bucket_resharding(op, -ERR_BUSY_RESHARDING);
		      cls_rgw_bucket_link_olh(op, key, olh_state.olh_tag,
                                              delete_marker, op_tag, meta, olh_epoch,
					      unmod_since, high_precision_time,
					      log_data_change, zones_trace);
                      return rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
                    }, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "rgw_rados_operate() after cls_rgw_bucket_link_olh() returned r=" << r << dendl;
    return r;
  }

  if (log_data_change) {
    add_datalog_entry(dpp, svc.datalog_rados, bucket_info, bs.shard_id, y);
  }

  return 0;
}

void RGWRados::bucket_index_guard_olh_op(const DoutPrefixProvider *dpp, RGWObjState& olh_state, ObjectOperation& op)
{
  ldpp_dout(dpp, 20) << __func__ << "(): olh_state.olh_tag=" << string(olh_state.olh_tag.c_str(), olh_state.olh_tag.length()) << dendl;
  op.cmpxattr(RGW_ATTR_OLH_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ, olh_state.olh_tag);
}

int RGWRados::bucket_index_unlink_instance(const DoutPrefixProvider *dpp,
                                           RGWBucketInfo& bucket_info,
                                           const rgw_obj& obj_instance,
                                           const string& op_tag, const string& olh_tag,
                                           uint64_t olh_epoch, optional_yield y,
                                           rgw_zone_set *_zones_trace, bool log_op)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj_instance, &ref);
  if (r < 0) {
    return r;
  }

  rgw_zone_set zones_trace;
  if (_zones_trace) {
    zones_trace = *_zones_trace;
  }
  zones_trace.insert(svc.zone->get_zone().id, bucket_info.bucket.get_key());

  BucketShard bs(this);

  cls_rgw_obj_key key(obj_instance.key.get_index_key_name(), obj_instance.key.instance);
  r = guard_reshard(dpp, &bs, obj_instance, bucket_info,
		    [&](BucketShard *bs) -> int {
		      auto& ref = bs->bucket_obj;
		      librados::ObjectWriteOperation op;
		      op.assert_exists(); // bucket index shard must exist
		      cls_rgw_guard_bucket_resharding(op, -ERR_BUSY_RESHARDING);
		      cls_rgw_bucket_unlink_instance(op, key, op_tag,
						     olh_tag, olh_epoch, log_op, zones_trace);
                      return rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
                    }, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "rgw_rados_operate() after cls_rgw_bucket_link_instance() returned r=" << r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::bucket_index_read_olh_log(const DoutPrefixProvider *dpp,
                                        RGWBucketInfo& bucket_info, RGWObjState& state,
                                        const rgw_obj& obj_instance, uint64_t ver_marker,
                                        std::map<uint64_t, std::vector<rgw_bucket_olh_log_entry> > *log,
                                        bool *is_truncated, optional_yield y)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj_instance, &ref);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);
  int ret =
    bs.init(obj_instance.bucket, obj_instance, nullptr /* no RGWBucketInfo */, dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  string olh_tag(state.olh_tag.c_str(), state.olh_tag.length());

  cls_rgw_obj_key key(obj_instance.key.get_index_key_name(), string());

  auto& shard_ref = bs.bucket_obj;
  ObjectReadOperation op;

  rgw_cls_read_olh_log_ret log_ret;
  int op_ret = 0;
  cls_rgw_get_olh_log(op, key, ver_marker, olh_tag, log_ret, op_ret);
  bufferlist outbl;
  r =  rgw_rados_operate(dpp, shard_ref.ioctx, shard_ref.obj.oid, &op, &outbl, y);
  if (r < 0) {
    return r;
  }
  if (op_ret < 0) {
    ldpp_dout(dpp, 20) << "cls_rgw_get_olh_log() returned op_ret=" << op_ret << dendl;
    return op_ret;
  }

  *log = std::move(log_ret.log);
  *is_truncated = log_ret.is_truncated;

  return 0;
}

// a multisite sync bug resulted in the OLH head attributes being overwritten by
// the attributes from another zone, causing link_olh() to fail endlessly due to
// olh_tag mismatch. this attempts to detect this case and reconstruct the OLH
// attributes from the bucket index. see http://tracker.ceph.com/issues/37792
int RGWRados::repair_olh(const DoutPrefixProvider *dpp, RGWObjState* state, const RGWBucketInfo& bucket_info,
                         const rgw_obj& obj, optional_yield y)
{
  // fetch the current olh entry from the bucket index
  rgw_bucket_olh_entry olh;
  int r = bi_get_olh(dpp, bucket_info, obj, &olh, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "repair_olh failed to read olh entry for " << obj << dendl;
    return r;
  }
  if (olh.tag == rgw_bl_str(state->olh_tag)) { // mismatch already resolved?
    return 0;
  }

  ldpp_dout(dpp, 4) << "repair_olh setting olh_tag=" << olh.tag
      << " key=" << olh.key << " delete_marker=" << olh.delete_marker << dendl;

  // rewrite OLH_ID_TAG and OLH_INFO from current olh
  ObjectWriteOperation op;
  // assert this is the same olh tag we think we're fixing
  bucket_index_guard_olh_op(dpp, *state, op);
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
  r = get_obj_head_ref(dpp, bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }
  r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "repair_olh failed to write olh attributes with "
        << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
} // RGWRados::repair_olh

int RGWRados::bucket_index_trim_olh_log(const DoutPrefixProvider *dpp,
                                        RGWBucketInfo& bucket_info,
                                        RGWObjState& state,
                                        const rgw_obj& obj_instance, uint64_t ver, optional_yield y)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj_instance, &ref);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);
  int ret =
    bs.init(obj_instance.bucket, obj_instance, nullptr /* no RGWBucketInfo */, dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  string olh_tag(state.olh_tag.c_str(), state.olh_tag.length());

  cls_rgw_obj_key key(obj_instance.key.get_index_key_name(), string());

  ret = guard_reshard(dpp, &bs, obj_instance, bucket_info,
		      [&](BucketShard *pbs) -> int {
			ObjectWriteOperation op;
			op.assert_exists(); // bucket index shard must exist
			cls_rgw_guard_bucket_resharding(op, -ERR_BUSY_RESHARDING);
			cls_rgw_trim_olh_log(op, key, ver, olh_tag);
                        return pbs->bucket_obj.operate(dpp, &op, y);
                      }, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "cls_rgw_trim_olh_log() returned r=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::bucket_index_clear_olh(const DoutPrefixProvider *dpp,
                                     RGWBucketInfo& bucket_info,
                                     const std::string& olh_tag,
                                     const rgw_obj& obj_instance, optional_yield y)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj_instance, &ref);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);

  cls_rgw_obj_key key(obj_instance.key.get_index_key_name(), string());

  int ret = guard_reshard(dpp, &bs, obj_instance, bucket_info,
			  [&](BucketShard *pbs) -> int {
			    ObjectWriteOperation op;
			    op.assert_exists(); // bucket index shard must exist
			    auto& ref = bs.bucket_obj;
			    cls_rgw_guard_bucket_resharding(op, -ERR_BUSY_RESHARDING);
			    cls_rgw_clear_olh(op, key, olh_tag);
                            return rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
                          }, y);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "rgw_rados_operate() after cls_rgw_clear_olh() returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

static int decode_olh_info(const DoutPrefixProvider *dpp, CephContext* cct, const bufferlist& bl, RGWOLHInfo *olh)
{
  try {
    auto biter = bl.cbegin();
    decode(*olh, biter);
    return 0;
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode olh info" << dendl;
    return -EIO;
  }
}

int RGWRados::apply_olh_log(const DoutPrefixProvider *dpp,
			    RGWObjectCtx& obj_ctx,
			    RGWObjState& state,
			    RGWBucketInfo& bucket_info,
			    const rgw_obj& obj,
			    bufferlist& olh_tag,
			    std::map<uint64_t, std::vector<rgw_bucket_olh_log_entry> >& log,
			    uint64_t *plast_ver,
			    optional_yield y, rgw_zone_set* zones_trace,
                            bool log_op)
{
  if (log.empty()) {
    return 0;
  }

  librados::ObjectWriteOperation op;

  uint64_t last_ver = log.rbegin()->first;
  *plast_ver = last_ver;

  map<uint64_t, vector<rgw_bucket_olh_log_entry> >::iterator iter = log.begin();

  op.cmpxattr(RGW_ATTR_OLH_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ, olh_tag);
  op.cmpxattr(RGW_ATTR_OLH_VER, CEPH_OSD_CMPXATTR_OP_GTE, last_ver);

  bufferlist ver_bl;
  string last_ver_s = to_string(last_ver);
  ver_bl.append(last_ver_s.c_str(), last_ver_s.size());
  op.setxattr(RGW_ATTR_OLH_VER, ver_bl);

  struct timespec mtime_ts = real_clock::to_timespec(state.mtime);
  op.mtime2(&mtime_ts);

  bool need_to_link = false;
  uint64_t link_epoch = 0;
  cls_rgw_obj_key key;
  bool delete_marker = false;
  list<cls_rgw_obj_key> remove_instances;
  bool need_to_remove = false;

  // decode current epoch and instance
  auto olh_ver = state.attrset.find(RGW_ATTR_OLH_VER);
  if (olh_ver != state.attrset.end()) {
    std::string str = olh_ver->second.to_str();
    std::string err;
    link_epoch = strict_strtoll(str.c_str(), 10, &err);
  }
  auto olh_info = state.attrset.find(RGW_ATTR_OLH_INFO);
  if (olh_info != state.attrset.end()) {
    RGWOLHInfo info;
    int r = decode_olh_info(dpp, cct, olh_info->second, &info);
    if (r < 0) {
      return r;
    }
    info.target.key.get_index_key(&key);
    delete_marker = info.removed;
  }

  for (iter = log.begin(); iter != log.end(); ++iter) {
    vector<rgw_bucket_olh_log_entry>::iterator viter = iter->second.begin();
    for (; viter != iter->second.end(); ++viter) {
      rgw_bucket_olh_log_entry& entry = *viter;

      ldpp_dout(dpp, 20) << "olh_log_entry: epoch=" << iter->first << " op=" << (int)entry.op
                     << " key=" << entry.key.name << "[" << entry.key.instance << "] "
                     << (entry.delete_marker ? "(delete)" : "") << dendl;
      switch (entry.op) {
      case CLS_RGW_OLH_OP_REMOVE_INSTANCE:
        remove_instances.push_back(entry.key);
        break;
      case CLS_RGW_OLH_OP_LINK_OLH:
        // only overwrite a link of the same epoch if its key sorts before
        if (link_epoch < iter->first || key.instance.empty() ||
            key.instance > entry.key.instance) {
          ldpp_dout(dpp, 20) << "apply_olh_log applying key=" << entry.key << " epoch=" << iter->first << " delete_marker=" << entry.delete_marker
              << " over current=" << key << " epoch=" << link_epoch << " delete_marker=" << delete_marker << dendl;
          need_to_link = true;
          need_to_remove = false;
          key = entry.key;
          delete_marker = entry.delete_marker;
        } else {
          ldpp_dout(dpp, 20) << "apply_olh skipping key=" << entry.key<< " epoch=" << iter->first << " delete_marker=" << entry.delete_marker
              << " before current=" << key << " epoch=" << link_epoch << " delete_marker=" << delete_marker << dendl;
        }
        break;
      case CLS_RGW_OLH_OP_UNLINK_OLH:
        need_to_remove = true;
        need_to_link = false;
        break;
      default:
        ldpp_dout(dpp, 0) << "ERROR: apply_olh_log: invalid op: " << (int)entry.op << dendl;
        return -EIO;
      }
      string attr_name = RGW_ATTR_OLH_PENDING_PREFIX;
      attr_name.append(entry.op_tag);
      op.rmxattr(attr_name.c_str());
    }
  }

  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj, &ref);
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
    int ret = delete_obj(dpp, obj_ctx, bucket_info, obj_instance, 0, y, RGW_BILOG_FLAG_VERSIONED_OP, ceph::real_time(), zones_trace, log_op);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: delete_obj() returned " << ret << " obj_instance=" << obj_instance << dendl;
      return ret;
    }
  }

  /* update olh object */
  r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << ": could not apply olh update to oid \"" << ref.obj.oid << "\", r=" << r << dendl;
    return r;
  }


  if (need_to_remove) {
    string olh_tag(state.olh_tag.c_str(), state.olh_tag.length());
    r = clear_olh(dpp, obj_ctx, obj, bucket_info, ref, olh_tag, last_ver, y);
    if (r < 0 && r != -ECANCELED) {
      ldpp_dout(dpp, 0) << "ERROR: could not clear olh, r=" << r << dendl;
      return r;
    }
  } else {
    r = bucket_index_trim_olh_log(dpp, bucket_info, state, obj, last_ver, y);
    if (r < 0 && r != -ECANCELED) {
      ldpp_dout(dpp, 0) << "ERROR: could not trim olh log, r=" << r << dendl;
      return r;
    }
  }

  return 0;
}

int RGWRados::clear_olh(const DoutPrefixProvider *dpp,
                        RGWObjectCtx& obj_ctx,
                        const rgw_obj& obj,
                        RGWBucketInfo& bucket_info,
                        const std::string& tag,
                        const uint64_t ver,
                        optional_yield y) {
  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, obj, &ref);
  if (r < 0) {
    return r;
  }
  return clear_olh(dpp, obj_ctx, obj, bucket_info, ref, tag, ver, y);
}

int RGWRados::clear_olh(const DoutPrefixProvider *dpp,
                        RGWObjectCtx& obj_ctx,
                        const rgw_obj& obj,
                        RGWBucketInfo& bucket_info,
                        rgw_rados_ref& ref,
                        const std::string& tag,
                        const uint64_t ver,
                        optional_yield y) {
  ObjectWriteOperation rm_op;

  RGWObjManifest *manifest = nullptr;
  RGWObjState *s = nullptr;

  int r = get_obj_state(dpp, &obj_ctx, bucket_info, obj, &s, &manifest, false, y);
  if (r < 0) {
    return r;
  }
  map<string, bufferlist> pending_entries;
  rgw_filter_attrset(s->attrset, RGW_ATTR_OLH_PENDING_PREFIX, &pending_entries);

  map<string, bufferlist> rm_pending_entries;
  check_pending_olh_entries(dpp, pending_entries, &rm_pending_entries);

  if (!rm_pending_entries.empty()) {
    r = remove_olh_pending_entries(dpp, bucket_info, *s, obj, rm_pending_entries, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: rm_pending_entries returned ret=" << r << dendl;
      return r;
    }
  }

  bufferlist tag_bl;
  tag_bl.append(tag.c_str(), tag.length());
  rm_op.cmpxattr(RGW_ATTR_OLH_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ, tag_bl);
  rm_op.cmpxattr(RGW_ATTR_OLH_VER, CEPH_OSD_CMPXATTR_OP_EQ, ver);
  cls_obj_check_prefix_exist(rm_op, RGW_ATTR_OLH_PENDING_PREFIX, true); /* fail if found one of these, pending modification */
  rm_op.remove();

  r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &rm_op, y);
  if (r == -ECANCELED) {
    return r; /* someone else made a modification in the meantime */
  }
  /* 
   * only clear if was successful, otherwise we might clobber pending operations on this object
   */
  r = bucket_index_clear_olh(dpp, bucket_info, tag, obj, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not clear bucket index olh entries r=" << r << dendl;
    return r;
  }
  return 0;
}

/*
 * read olh log and apply it
 */
int RGWRados::update_olh(const DoutPrefixProvider *dpp, RGWObjectCtx& obj_ctx, RGWObjState *state, RGWBucketInfo& bucket_info, const rgw_obj& obj, optional_yield y, rgw_zone_set *zones_trace, bool log_op)
{
  map<uint64_t, vector<rgw_bucket_olh_log_entry> > log;
  bool is_truncated;
  uint64_t ver_marker = 0;

  do {
    int ret = bucket_index_read_olh_log(dpp, bucket_info, *state, obj, ver_marker, &log, &is_truncated, y);
    if (ret < 0) {
      return ret;
    }
    ret = apply_olh_log(dpp, obj_ctx, *state, bucket_info, obj, state->olh_tag, log, &ver_marker, y, zones_trace, log_op);
    if (ret < 0) {
      return ret;
    }
  } while (is_truncated);

  return 0;
}

int RGWRados::set_olh(const DoutPrefixProvider *dpp, RGWObjectCtx& obj_ctx,
		      RGWBucketInfo& bucket_info,
		      const rgw_obj& target_obj, bool delete_marker,
		      rgw_bucket_dir_entry_meta *meta,
                      uint64_t olh_epoch, real_time unmod_since, bool high_precision_time,
                      optional_yield y, rgw_zone_set *zones_trace, bool log_data_change,
		      bool skip_olh_obj_update)
{
  string op_tag;

  rgw_obj olh_obj = target_obj;
  olh_obj.key.instance.clear();

  RGWObjState *state = NULL;
  RGWObjManifest *manifest = nullptr;

  int ret = 0;
  int i;

#define MAX_ECANCELED_RETRY 100
  for (i = 0; i < MAX_ECANCELED_RETRY; i++) {
    if (ret == -ECANCELED) {
      obj_ctx.invalidate(olh_obj);
    }

    ret = get_obj_state(dpp, &obj_ctx, bucket_info, olh_obj, &state, &manifest, false, y); /* don't follow olh */
    if (ret < 0) {
      return ret;
    }

    ret = olh_init_modification(dpp, bucket_info, *state, olh_obj, &op_tag, y);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "olh_init_modification() target_obj=" << target_obj << " delete_marker=" << (int)delete_marker << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        continue;
      }
      return ret;
    }
    if (cct->_conf->rgw_debug_inject_set_olh_err) {
      // fail here to simulate the scenario of an unlinked object instance
      ret = -cct->_conf->rgw_debug_inject_set_olh_err;
    } else {
      ret = bucket_index_link_olh(dpp, bucket_info, *state, target_obj,
		                              delete_marker, op_tag, meta, olh_epoch, unmod_since,
		                              high_precision_time, y, zones_trace, log_data_change);
    }
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "bucket_index_link_olh() target_obj=" << target_obj << " delete_marker=" << (int)delete_marker << " returned " << ret << dendl;
      olh_cancel_modification(dpp, bucket_info, *state, olh_obj, op_tag, y);
      if (ret == -ECANCELED) {
        // the bucket index rejected the link_olh() due to olh tag mismatch;
        // attempt to reconstruct olh head attributes based on the bucket index
        int r2 = repair_olh(dpp, state, bucket_info, olh_obj, y);
        if (r2 < 0 && r2 != -ECANCELED) {
          return r2;
        }
        continue;
      }
      // it's possible that the pending xattr from this op prevented the olh
      // object from being cleaned by another thread that was deleting the last
      // existing version. We invoke a best-effort update_olh here to handle this case.
      int r = update_olh(dpp, obj_ctx, state, bucket_info, olh_obj, y, zones_trace, log_data_change);
      if (r < 0 && r != -ECANCELED) {
        ldpp_dout(dpp, 20) << "update_olh() target_obj=" << olh_obj << " returned " << r << dendl;
      }
      return ret;
    }
    break;
  }

  if (i == MAX_ECANCELED_RETRY) {
    ldpp_dout(dpp, 0) << "ERROR: exceeded max ECANCELED retries, aborting (EIO)" << dendl;
    return -EIO;
  }

  // exit early if we're skipping the olh update and just updating the index
  if (skip_olh_obj_update) {
    return 0;
  }

  ret = update_olh(dpp, obj_ctx, state, bucket_info, olh_obj, y, zones_trace, log_data_change);
  if (ret == -ECANCELED) { /* already did what we needed, no need to retry, raced with another user */
    ret = 0;
  }
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "update_olh() target_obj=" << target_obj << " returned " << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::unlink_obj_instance(const DoutPrefixProvider *dpp, RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, const rgw_obj& target_obj,
                                  uint64_t olh_epoch, optional_yield y, rgw_zone_set *zones_trace, bool log_op)
{
  string op_tag;

  rgw_obj olh_obj = target_obj;
  olh_obj.key.instance.clear();

  RGWObjState *state = NULL;
  RGWObjManifest *manifest = NULL;

  int ret = 0;
  int i;

  for (i = 0; i < MAX_ECANCELED_RETRY; i++) {
    if (ret == -ECANCELED) {
      obj_ctx.invalidate(olh_obj);
    }

    ret = get_obj_state(dpp, &obj_ctx, bucket_info, olh_obj, &state, &manifest, false, y); /* don't follow olh */
    if (ret < 0)
      return ret;

    ret = olh_init_modification(dpp, bucket_info, *state, olh_obj, &op_tag, y);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "olh_init_modification() target_obj=" << target_obj << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        continue;
      }
      return ret;
    }

    string olh_tag(state->olh_tag.c_str(), state->olh_tag.length());
    
    if (cct->_conf->rgw_debug_inject_latency_bi_unlink) {
      // simulates queue latency for unlink ops to validate behavior with
      // concurrent delete requests for the same object version instance
      std::this_thread::sleep_for(cct->_conf->rgw_debug_inject_latency_bi_unlink * std::chrono::seconds{1});
    }

    ret = bucket_index_unlink_instance(dpp, bucket_info, target_obj, op_tag, olh_tag, olh_epoch, y, zones_trace, log_op);
    if (ret < 0) {
      olh_cancel_modification(dpp, bucket_info, *state, olh_obj, op_tag, y);
      ldpp_dout(dpp, 20) << "bucket_index_unlink_instance() target_obj=" << target_obj << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        continue;
      }
      // it's possible that the pending xattr from this op prevented the olh
      // object from being cleaned by another thread that was deleting the last
      // existing version. We invoke a best-effort update_olh here to handle this case.
      int r = update_olh(dpp, obj_ctx, state, bucket_info, olh_obj, y, zones_trace, log_op);
      if (r < 0 && r != -ECANCELED) {
        ldpp_dout(dpp, 20) << "update_olh() target_obj=" << olh_obj << " returned " << r << dendl;
      }
      return ret;
    }
    break;
  }

  if (i == MAX_ECANCELED_RETRY) {
    ldpp_dout(dpp, 0) << "ERROR: exceeded max ECANCELED retries, aborting (EIO)" << dendl;
    return -EIO;
  }

  ret = update_olh(dpp, obj_ctx, state, bucket_info, olh_obj, y, zones_trace, log_op);
  if (ret == -ECANCELED) { /* already did what we needed, no need to retry, raced with another user */
    return 0;
  }
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "update_olh() target_obj=" << target_obj << " returned " << ret << dendl;
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

int RGWRados::get_olh(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWOLHInfo *olh, optional_yield y)
{
  map<string, bufferlist> attrset;

  ObjectReadOperation op;
  op.getxattrs(&attrset, NULL);

  int r = obj_operate(dpp, bucket_info, obj, &op, y);
  if (r < 0) {
    return r;
  }

  auto iter = attrset.find(RGW_ATTR_OLH_VER);
  if (iter == attrset.end()) { /* not an olh */
    return -EINVAL;
  }

  return decode_olh_info(dpp, cct, iter->second, olh);
}

void RGWRados::check_pending_olh_entries(const DoutPrefixProvider *dpp,
					 map<string, bufferlist>& pending_entries,
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
      ldpp_dout(dpp, 0) << "ERROR: failed to decode pending entry " << iter->first << dendl;
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

int RGWRados::remove_olh_pending_entries(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, map<string, bufferlist>& pending_attrs, optional_yield y)
{
  rgw_rados_ref ref;
  int r = get_obj_head_ref(dpp, bucket_info, olh_obj, &ref);
  if (r < 0) {
    return r;
  }

  // trim no more than 1000 entries per osd op
  constexpr int max_entries = 1000;

  auto i = pending_attrs.begin();
  while (i != pending_attrs.end()) {
    ObjectWriteOperation op;
    bucket_index_guard_olh_op(dpp, state, op);

    for (int n = 0; n < max_entries && i != pending_attrs.end(); ++n, ++i) {
      op.rmxattr(i->first.c_str());
    }

    r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
    if (r == -ENOENT || r == -ECANCELED) {
      /* raced with some other change, shouldn't sweat about it */
      return 0;
    }
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << ": could not apply olh update to oid \"" << ref.obj.oid << "\", r=" << r << dendl;
      return r;
    }
  }
  return 0;
}

int RGWRados::follow_olh(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, RGWObjectCtx& obj_ctx, RGWObjState *state, const rgw_obj& olh_obj, rgw_obj *target, optional_yield y)
{
  map<string, bufferlist> pending_entries;
  rgw_filter_attrset(state->attrset, RGW_ATTR_OLH_PENDING_PREFIX, &pending_entries);

  map<string, bufferlist> rm_pending_entries;
  check_pending_olh_entries(dpp, pending_entries, &rm_pending_entries);

  if (!rm_pending_entries.empty()) {
    int ret = remove_olh_pending_entries(dpp, bucket_info, *state, olh_obj, rm_pending_entries, y);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "ERROR: rm_pending_entries returned ret=" << ret << dendl;
      return ret;
    }
  }
  if (!pending_entries.empty()) {
    ldpp_dout(dpp, 20) << __func__ << "(): found pending entries, need to update_olh() on bucket=" << olh_obj.bucket << dendl;

    int ret = update_olh(dpp, obj_ctx, state, bucket_info, olh_obj, y);
    if (ret < 0) {
      if (ret == -ECANCELED) {
        // In this context, ECANCELED means that the OLH tag changed in either the bucket index entry or the OLH object.
        // If the OLH tag changed, it indicates that a previous OLH entry was removed since this request started. We
        // return ENOENT to indicate that the OLH object was removed.
        ret = -ENOENT;
      }
      return ret;
    }
  }

  auto iter = state->attrset.find(RGW_ATTR_OLH_VER);
  if (iter == state->attrset.end()) {
    return -EINVAL;
  }
  iter = state->attrset.find(RGW_ATTR_OLH_INFO);
  if (iter == state->attrset.end()) {
    return -ENOENT;
  }

  RGWOLHInfo olh;
  int ret = decode_olh_info(dpp, cct, iter->second, &olh);
  if (ret < 0) {
    return ret;
  }

  if (olh.removed) {
    return -ENOENT;
  }

  *target = olh.target;

  return 0;
}

int RGWRados::raw_obj_stat(const DoutPrefixProvider *dpp,
                           rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
                           map<string, bufferlist> *attrs, bufferlist *first_chunk,
                           RGWObjVersionTracker *objv_tracker, optional_yield y)
{
  rgw_rados_ref ref;
  int r = get_raw_obj_ref(dpp, obj, &ref);
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
  r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, &outbl, y);

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

int RGWRados::get_bucket_stats(const DoutPrefixProvider *dpp,
			       RGWBucketInfo& bucket_info,
			       const rgw::bucket_index_layout_generation& idx_layout,
			       int shard_id, string *bucket_ver, string *master_ver,
			       map<RGWObjCategory, RGWStorageStats>& stats,
			       string *max_marker, bool *syncstopped)
{
  vector<rgw_bucket_dir_header> headers;
  map<int, string> bucket_instance_ids;
  int r = cls_bucket_head(dpp, bucket_info, idx_layout, shard_id, headers, &bucket_instance_ids);
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

class RGWGetBucketStatsContext : public RGWGetDirHeader_CB {
  boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb;
  uint32_t pendings;
  RGWStorageStats stats;
  int ret_code;
  bool should_cb;
  ceph::mutex lock = ceph::make_mutex("RGWGetBucketStatsContext");

public:
  RGWGetBucketStatsContext(boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb, uint32_t _pendings)
    : cb(std::move(cb)), pendings(_pendings), stats(), ret_code(0), should_cb(true)
  {}

  void handle_response(int r, const rgw_bucket_dir_header& header) override {
    std::lock_guard l{lock};
    if (should_cb) {
      if (r >= 0) {
        for (const auto& [c, s] : header.stats) {
          stats.size += s.total_size;
          stats.size_rounded += s.total_size_rounded;
          stats.size_utilized += s.actual_size;
          stats.num_objects += s.num_entries;
        }
      } else {
        ret_code = r;
      }

      // Are we all done?
      if (--pendings == 0) {
        cb->handle_response(ret_code, stats);
        cb.reset();
      }
    }
  }

  void unset_cb() {
    std::lock_guard l{lock};
    should_cb = false;
  }
};

int RGWRados::get_bucket_stats_async(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, const rgw::bucket_index_layout_generation& idx_layout, int shard_id, boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb)
{
  int num_aio = 0;
  boost::intrusive_ptr headercb = new RGWGetBucketStatsContext(std::move(cb), bucket_info.layout.current_index.layout.normal.num_shards ? : 1);
  int r = cls_bucket_head_async(dpp, bucket_info, idx_layout, shard_id, headercb, &num_aio);
  if (r < 0) {
    if (num_aio) {
      headercb->unset_cb();
    }
  }
  return r;
}

int RGWRados::get_bucket_instance_info(const string& meta_key,
				       RGWBucketInfo& info,
                                       real_time *pmtime,
				       map<string, bufferlist> *pattrs,
				       optional_yield y,
                                       const DoutPrefixProvider *dpp)
{
  rgw_bucket bucket;
  rgw_bucket_parse_bucket_key(cct, meta_key, &bucket, nullptr);

  return get_bucket_instance_info(bucket, info, pmtime, pattrs, y, dpp);
}

int RGWRados::get_bucket_instance_info(const rgw_bucket& bucket, RGWBucketInfo& info,
                                       real_time *pmtime, map<string, bufferlist> *pattrs, optional_yield y,
                                       const DoutPrefixProvider *dpp)
{
  return ctl.bucket->read_bucket_instance_info(bucket, &info,
					       y,
                                               dpp,
					       RGWBucketCtl::BucketInstance::GetParams()
					       .set_mtime(pmtime)
					       .set_attrs(pattrs));
}

int RGWRados::get_bucket_info(RGWServices *svc,
                              const string& tenant, const string& bucket_name,
                              RGWBucketInfo& info,
                              real_time *pmtime,
                              optional_yield y,
                              const DoutPrefixProvider *dpp, map<string, bufferlist> *pattrs)
{
  rgw_bucket bucket;
  bucket.tenant = tenant;
  bucket.name = bucket_name;
  return ctl.bucket->read_bucket_info(bucket, &info, y, dpp,
				      RGWBucketCtl::BucketInstance::GetParams()
				      .set_mtime(pmtime)
				      .set_attrs(pattrs));
}

int RGWRados::try_refresh_bucket_info(RGWBucketInfo& info,
                                      ceph::real_time *pmtime,
                                      const DoutPrefixProvider *dpp, optional_yield y,
                                      map<string, bufferlist> *pattrs)
{
  rgw_bucket bucket = info.bucket;
  bucket.bucket_id.clear();

  auto rv = info.objv_tracker.read_version;

  return ctl.bucket->read_bucket_info(bucket, &info, y, dpp,
				      RGWBucketCtl::BucketInstance::GetParams()
				      .set_mtime(pmtime)
				      .set_attrs(pattrs)
				      .set_refresh_version(rv));
}

int RGWRados::put_bucket_instance_info(RGWBucketInfo& info, bool exclusive,
                              real_time mtime, const map<string, bufferlist> *pattrs,
                              const DoutPrefixProvider *dpp, optional_yield y)
{
  return ctl.bucket->store_bucket_instance_info(info.bucket, info, y, dpp,
						RGWBucketCtl::BucketInstance::PutParams()
						.set_exclusive(exclusive)
						.set_mtime(mtime)
						.set_attrs(pattrs));
}

int RGWRados::put_linked_bucket_info(RGWBucketInfo& info, bool exclusive, real_time mtime, obj_version *pep_objv,
                                     const map<string, bufferlist> *pattrs, bool create_entry_point,
                                     const DoutPrefixProvider *dpp, optional_yield y)
{
  bool create_head = !info.has_instance_obj || create_entry_point;

  int ret = put_bucket_instance_info(info, exclusive, mtime, pattrs, dpp, y);
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
  ret = ctl.bucket->store_bucket_entrypoint_info(info.bucket, entry_point, y, dpp, RGWBucketCtl::Bucket::PutParams()
						                          .set_exclusive(exclusive)
									  .set_objv_tracker(&ot)
									  .set_mtime(mtime));
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::append_async(const DoutPrefixProvider *dpp, rgw_raw_obj& obj, size_t size, bufferlist& bl)
{
  rgw_rados_ref ref;
  int r = get_raw_obj_ref(dpp, obj, &ref);
  if (r < 0) {
    return r;
  }
  librados::Rados *rad = get_rados_handle();
  librados::AioCompletion *completion = rad->aio_create_completion(nullptr, nullptr);

  r = ref.ioctx.aio_append(ref.obj.oid, completion, bl, size);
  completion->release();
  return r;
}

int RGWRados::pool_iterate_begin(const DoutPrefixProvider *dpp, const rgw_pool& pool, RGWPoolIterCtx& ctx)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::NObjectIterator& iter = ctx.iter;

  int r = open_pool_ctx(dpp, pool, io_ctx, false, false);
  if (r < 0)
    return r;

  iter = io_ctx.nobjects_begin();

  return 0;
}

int RGWRados::pool_iterate_begin(const DoutPrefixProvider *dpp, const rgw_pool& pool, const string& cursor, RGWPoolIterCtx& ctx)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::NObjectIterator& iter = ctx.iter;

  int r = open_pool_ctx(dpp, pool, io_ctx, false, false);
  if (r < 0)
    return r;

  librados::ObjectCursor oc;
  if (!oc.from_str(cursor)) {
    ldpp_dout(dpp, 10) << "failed to parse cursor: " << cursor << dendl;
    return -EINVAL;
  }

  try {
    iter = io_ctx.nobjects_begin(oc);
    return 0;
  } catch (const std::system_error& e) {
    r = -e.code().value();
    ldpp_dout(dpp, 10) << "nobjects_begin threw " << e.what()
       << ", returning " << r << dendl;
    return r;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 10) << "nobjects_begin threw " << e.what()
       << ", returning -5" << dendl;
    return -EIO;
  }
}

string RGWRados::pool_iterate_get_cursor(RGWPoolIterCtx& ctx)
{
  return ctx.iter.get_cursor().to_str();
}

static int do_pool_iterate(const DoutPrefixProvider *dpp, CephContext* cct, RGWPoolIterCtx& ctx, uint32_t num,
                           vector<rgw_bucket_dir_entry>& objs,
                           bool *is_truncated, const rgw::AccessListFilter& filter)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::NObjectIterator& iter = ctx.iter;

  if (iter == io_ctx.nobjects_end())
    return -ENOENT;

  uint32_t i;

  for (i = 0; i < num && iter != io_ctx.nobjects_end(); ++i, ++iter) {
    rgw_bucket_dir_entry e;

    string oid = iter->get_oid();
    ldpp_dout(dpp, 20) << "RGWRados::pool_iterate: got " << oid << dendl;

    // fill it in with initial values; we may correct later
    if (filter && !filter(oid, oid))
      continue;

    e.key = oid;
    objs.push_back(e);
  }

  if (is_truncated)
    *is_truncated = (iter != io_ctx.nobjects_end());

  return objs.size();
}

int RGWRados::pool_iterate(const DoutPrefixProvider *dpp, RGWPoolIterCtx& ctx, uint32_t num, vector<rgw_bucket_dir_entry>& objs,
                           bool *is_truncated, const rgw::AccessListFilter& filter)
{
  // catch exceptions from NObjectIterator::operator++()
  try {
    return do_pool_iterate(dpp, cct, ctx, num, objs, is_truncated, filter);
  } catch (const std::system_error& e) {
    int r = -e.code().value();
    ldpp_dout(dpp, 10) << "NObjectIterator threw exception " << e.what()
       << ", returning " << r << dendl;
    return r;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 10) << "NObjectIterator threw exception " << e.what()
       << ", returning -5" << dendl;
    return -EIO;
  }
}

int RGWRados::list_raw_objects_init(const DoutPrefixProvider *dpp, const rgw_pool& pool, const string& marker, RGWListRawObjsCtx *ctx)
{
  if (!ctx->initialized) {
    int r = pool_iterate_begin(dpp, pool, marker, ctx->iter_ctx);
    if (r < 0) {
      ldpp_dout(dpp, 10) << "failed to list objects pool_iterate_begin() returned r=" << r << dendl;
      return r;
    }
    ctx->initialized = true;
  }
  return 0;
}

int RGWRados::list_raw_objects_next(const DoutPrefixProvider *dpp, const string& prefix_filter, int max,
                                    RGWListRawObjsCtx& ctx, list<string>& oids,
                                    bool *is_truncated)
{
  if (!ctx.initialized) {
    return -EINVAL;
  }
  auto filter = rgw::AccessListFilterPrefix(prefix_filter);
  vector<rgw_bucket_dir_entry> objs;
  int r = pool_iterate(dpp, ctx.iter_ctx, max, objs, is_truncated, filter);
  if (r < 0) {
    if(r != -ENOENT)
      ldpp_dout(dpp, 10) << "failed to list objects pool_iterate returned r=" << r << dendl;
    return r;
  }

  vector<rgw_bucket_dir_entry>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    oids.push_back(iter->key.name);
  }

  return oids.size();
}

int RGWRados::list_raw_objects(const DoutPrefixProvider *dpp, const rgw_pool& pool, const string& prefix_filter,
			       int max, RGWListRawObjsCtx& ctx, list<string>& oids,
			       bool *is_truncated)
{
  if (!ctx.initialized) {
    int r = list_raw_objects_init(dpp, pool, string(), &ctx);
    if (r < 0) {
      return r;
    }
  }

  return list_raw_objects_next(dpp, prefix_filter, max, ctx, oids, is_truncated);
}

string RGWRados::list_raw_objs_get_cursor(RGWListRawObjsCtx& ctx)
{
  return pool_iterate_get_cursor(ctx.iter_ctx);
}

int RGWRados::bi_get_instance(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                              rgw_bucket_dir_entry *dirent, optional_yield y)
{
  rgw_cls_bi_entry bi_entry;
  int r = bi_get(dpp, bucket_info, obj, BIIndexType::Instance, &bi_entry, y);
  if (r < 0 && r != -ENOENT) {
    ldpp_dout(dpp, 0) << "ERROR: bi_get() returned r=" << r << dendl;
  }
  if (r < 0) {
    return r;
  }
  auto iter = bi_entry.data.cbegin();
  try {
    decode(*dirent, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode bi_entry()" << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRados::bi_get_olh(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                         rgw_bucket_olh_entry *olh, optional_yield y)
{
  rgw_cls_bi_entry bi_entry;
  int r = bi_get(dpp, bucket_info, obj, BIIndexType::OLH, &bi_entry, y);
  if (r < 0 && r != -ENOENT) {
    ldpp_dout(dpp, 0) << "ERROR: bi_get() returned r=" << r << dendl;
  }
  if (r < 0) {
    return r;
  }
  auto iter = bi_entry.data.cbegin();
  try {
    decode(*olh, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode bi_entry()" << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRados::bi_get(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                     BIIndexType index_type, rgw_cls_bi_entry *entry, optional_yield y)
{
  BucketShard bs(this);
  int ret = bs.init(dpp, bucket_info, obj, y);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  cls_rgw_obj_key key(obj.key.get_index_key_name(), obj.key.instance);

  auto& ref = bs.bucket_obj;

  return cls_rgw_bi_get(ref.ioctx, ref.obj.oid, index_type, key, entry);
}

void RGWRados::bi_put(ObjectWriteOperation& op, BucketShard& bs, rgw_cls_bi_entry& entry, optional_yield y)
{
  auto& ref = bs.bucket_obj;
  cls_rgw_bi_put(op, ref.obj.oid, entry);
}

int RGWRados::bi_put(BucketShard& bs, rgw_cls_bi_entry& entry, optional_yield y)
{
  auto& ref = bs.bucket_obj;
  int ret = cls_rgw_bi_put(ref.ioctx, ref.obj.oid, entry);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::bi_put(const DoutPrefixProvider *dpp, rgw_bucket& bucket, rgw_obj& obj, rgw_cls_bi_entry& entry, optional_yield y)
{
  // make sure incomplete multipart uploads are hashed correctly
  if (obj.key.ns == RGW_OBJ_NS_MULTIPART) {
    RGWMPObj mp;
    mp.from_meta(obj.key.name);
    obj.index_hash_source = mp.get_key();
  }
  BucketShard bs(this);

  int ret = bs.init(bucket, obj, nullptr /* no RGWBucketInfo */, dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  return bi_put(bs, entry, y);
}

int RGWRados::bi_list(const DoutPrefixProvider *dpp, rgw_bucket& bucket,
		      const string& obj_name_filter, const string& marker, uint32_t max,
		      list<rgw_cls_bi_entry> *entries, bool *is_truncated, optional_yield y)
{
  rgw_obj obj(bucket, obj_name_filter);
  BucketShard bs(this);
  int ret = bs.init(bucket, obj, nullptr /* no RGWBucketInfo */, dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  auto& ref = bs.bucket_obj;
  ret = cls_rgw_bi_list(ref.ioctx, ref.obj.oid, obj_name_filter, marker, max, entries, is_truncated);
  if (ret == -ENOENT) {
    *is_truncated = false;
  }
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::bi_list(BucketShard& bs, const string& obj_name_filter, const string& marker, uint32_t max,
		      list<rgw_cls_bi_entry> *entries, bool *is_truncated, optional_yield y)
{
  auto& ref = bs.bucket_obj;
  int ret = cls_rgw_bi_list(ref.ioctx, ref.obj.oid, obj_name_filter, marker, max, entries, is_truncated);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::bi_list(const DoutPrefixProvider *dpp,
		      const RGWBucketInfo& bucket_info, int shard_id, const string& obj_name_filter, const string& marker, uint32_t max,
		      list<rgw_cls_bi_entry> *entries, bool *is_truncated, optional_yield y)
{
  BucketShard bs(this);
  int ret = bs.init(dpp, bucket_info,
		    bucket_info.layout.current_index,
		    shard_id, y);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  return bi_list(bs, obj_name_filter, marker, max, entries, is_truncated, y);
}

int RGWRados::bi_remove(const DoutPrefixProvider *dpp, BucketShard& bs)
{
  auto& ref = bs.bucket_obj;
  int ret = ref.ioctx.remove(ref.obj.oid);
  if (ret == -ENOENT) {
    ret = 0;
  }
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "bs.index_ctx.remove(" << bs.bucket_obj << ") returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::gc_operate(const DoutPrefixProvider *dpp, string& oid, librados::ObjectWriteOperation *op, optional_yield y)
{
  return rgw_rados_operate(dpp, gc_pool_ctx, oid, op, y);
}

int RGWRados::gc_aio_operate(const string& oid, librados::AioCompletion *c,
                             librados::ObjectWriteOperation *op)
{
  return gc_pool_ctx.aio_operate(oid, c, op);
}

int RGWRados::gc_operate(const DoutPrefixProvider *dpp, string& oid, librados::ObjectReadOperation *op, bufferlist *pbl, optional_yield y)
{
  return rgw_rados_operate(dpp, gc_pool_ctx, oid, op, pbl, y);
}

int RGWRados::list_gc_objs(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated, bool& processing_queue)
{
  return gc->list(index, marker, max, expired_only, result, truncated, processing_queue);
}

int RGWRados::process_gc(bool expired_only, optional_yield y)
{
  return gc->process(expired_only, y);
}

int RGWRados::list_lc_progress(string& marker, uint32_t max_entries,
			       vector<std::unique_ptr<rgw::sal::Lifecycle::LCEntry>>& progress_map,
			       int& index)
{
  return lc->list_lc_progress(marker, max_entries, progress_map, index);
}

int RGWRados::process_lc(const std::unique_ptr<rgw::sal::Bucket>& optional_bucket)
{
  RGWLC lc;
  lc.initialize(cct, this->driver);
  RGWLC::LCWorker worker(&lc, cct, &lc, 0);
  auto ret = lc.process(&worker, optional_bucket, true /* once */);
  lc.stop_processor(); // sets down_flag, but returns immediately
  return ret;
}

bool RGWRados::process_expire_objects(const DoutPrefixProvider *dpp, optional_yield y)
{
  return obj_expirer->inspect_all_shards(dpp, utime_t(), ceph_clock_now(), y);
}

int RGWRados::cls_obj_prepare_op(const DoutPrefixProvider *dpp, BucketShard& bs, RGWModifyOp op, string& tag,
                                 rgw_obj& obj, uint16_t bilog_flags, optional_yield y, rgw_zone_set *_zones_trace,
                                 bool log_op)
{
  const bool bitx = cct->_conf->rgw_bucket_index_transaction_instrumentation;
  ldout_bitx(bitx, dpp, 10) << "ENTERING " << __func__ << ": bucket-shard=" << bs << " obj=" << obj << " tag=" << tag << " op=" << op << dendl_bitx;
  ldout_bitx(bitx, dpp, 25) << "BACKTRACE: " << __func__ << ": " << ClibBackTrace(0) << dendl_bitx;

  rgw_zone_set zones_trace;
  if (_zones_trace) {
    zones_trace = *_zones_trace;
  }
  zones_trace.insert(svc.zone->get_zone().id, bs.bucket.get_key());

  ObjectWriteOperation o;
  o.assert_exists(); // bucket index shard must exist

  cls_rgw_obj_key key(obj.key.get_index_key_name(), obj.key.instance);
  cls_rgw_guard_bucket_resharding(o, -ERR_BUSY_RESHARDING);
  cls_rgw_bucket_prepare_op(o, op, tag, key, obj.key.get_loc(), log_op, bilog_flags, zones_trace);
  int ret = bs.bucket_obj.operate(dpp, &o, y);
  ldout_bitx(bitx, dpp, 10) << "EXITING " << __func__ << ": ret=" << ret << dendl_bitx;
  return ret;
}

int RGWRados::cls_obj_complete_op(BucketShard& bs, const rgw_obj& obj, RGWModifyOp op, string& tag,
                                  int64_t pool, uint64_t epoch,
                                  rgw_bucket_dir_entry& ent, RGWObjCategory category,
                                  list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags,
                                  rgw_zone_set *_zones_trace, bool log_op)
{
  const bool bitx = cct->_conf->rgw_bucket_index_transaction_instrumentation;
  ldout_bitx_c(bitx, cct, 10) << "ENTERING " << __func__ << ": bucket-shard=" << bs <<
    " obj=" << obj << " tag=" << tag << " op=" << op <<
    ", remove_objs=" << (remove_objs ? *remove_objs : std::list<rgw_obj_index_key>()) <<
    ", log_op=" << log_op << dendl_bitx;
  ldout_bitx_c(bitx, cct, 25) << "BACKTRACE: " << __func__ << ": " << ClibBackTrace(0) << dendl_bitx;

  ObjectWriteOperation o;
  o.assert_exists(); // bucket index shard must exist

  rgw_bucket_dir_entry_meta dir_meta;
  dir_meta = ent.meta;
  dir_meta.category = category;

  rgw_zone_set zones_trace;
  if (_zones_trace) {
    zones_trace = *_zones_trace;
  }
  zones_trace.insert(svc.zone->get_zone().id, bs.bucket.get_key());

  rgw_bucket_entry_ver ver;
  ver.pool = pool;
  ver.epoch = epoch;
  cls_rgw_obj_key key(ent.key.name, ent.key.instance);
  cls_rgw_guard_bucket_resharding(o, -ERR_BUSY_RESHARDING);
  cls_rgw_bucket_complete_op(o, op, tag, ver, key, dir_meta, remove_objs,
                             log_op, bilog_flags, &zones_trace, obj.key.get_loc());
  complete_op_data *arg;
  index_completion_manager->create_completion(obj, op, tag, ver, key, dir_meta, remove_objs,
                                              log_op, bilog_flags, &zones_trace, &arg);
  librados::AioCompletion *completion = arg->rados_completion;
  int ret = bs.bucket_obj.aio_operate(arg->rados_completion, &o);
  completion->release(); /* can't reference arg here, as it might have already been released */

  ldout_bitx_c(bitx, cct, 10) << "EXITING " << __func__ << ": ret=" << ret << dendl_bitx;
  return ret;
}

int RGWRados::cls_obj_complete_add(BucketShard& bs, const rgw_obj& obj, string& tag,
                                   int64_t pool, uint64_t epoch,
                                   rgw_bucket_dir_entry& ent, RGWObjCategory category,
                                   list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags,
                                   rgw_zone_set *zones_trace, bool log_op)
{
  return cls_obj_complete_op(bs, obj, CLS_RGW_OP_ADD, tag, pool, epoch,
                             ent, category, remove_objs, bilog_flags,
                             zones_trace, log_op);
}

int RGWRados::cls_obj_complete_del(BucketShard& bs, string& tag,
                                   int64_t pool, uint64_t epoch,
                                   rgw_obj& obj,
                                   real_time& removed_mtime,
                                   list<rgw_obj_index_key> *remove_objs,
                                   uint16_t bilog_flags,
                                   rgw_zone_set *zones_trace,
                                   bool log_op)
{
  rgw_bucket_dir_entry ent;
  ent.meta.mtime = removed_mtime;
  obj.key.get_index_key(&ent.key);
  return cls_obj_complete_op(bs, obj, CLS_RGW_OP_DEL, tag, pool, epoch,
			     ent, RGWObjCategory::None, remove_objs,
			     bilog_flags, zones_trace, log_op);
}

int RGWRados::cls_obj_complete_cancel(BucketShard& bs, string& tag, rgw_obj& obj,
                                      list<rgw_obj_index_key> *remove_objs,
                                      uint16_t bilog_flags, rgw_zone_set *zones_trace, bool log_op)
{
  rgw_bucket_dir_entry ent;
  obj.key.get_index_key(&ent.key);
  return cls_obj_complete_op(bs, obj, CLS_RGW_OP_CANCEL, tag,
			     -1 /* pool id */, 0, ent,
			     RGWObjCategory::None, remove_objs, bilog_flags,
			     zones_trace, log_op);
}

int RGWRados::cls_obj_set_bucket_tag_timeout(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, uint64_t timeout)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;
  int r = svc.bi_rados->open_bucket_index(dpp, bucket_info, std::nullopt, bucket_info.layout.current_index, &index_pool, &bucket_objs, nullptr);
  if (r < 0)
    return r;

  return CLSRGWIssueSetTagTimeout(index_pool, bucket_objs, cct->_conf->rgw_bucket_index_max_aio, timeout)();
}


// returns 0 if there is an error in calculation
uint32_t RGWRados::calc_ordered_bucket_list_per_shard(uint32_t num_entries,
						      uint32_t num_shards)
{
  if (num_shards == 0) {
    // we'll get a floating point exception since we divide by
    // num_shards
    return 0;
  }

  // We want to minimize the chances that when num_shards >>
  // num_entries that we return much fewer than num_entries to the
  // client. Given all the overhead of making a cls call to the osd,
  // returning a few entries is not much more work than returning one
  // entry. This minimum might be better tuned based on future
  // experiments where num_shards >> num_entries. (Note: ">>" should
  // be interpreted as "much greater than".)
  constexpr uint32_t min_read = 8;

  // The following is based on _"Balls into Bins" -- A Simple and
  // Tight Analysis_ by Raab and Steger. We add 1 as a way to handle
  // cases when num_shards >> num_entries (it almost serves as a
  // ceiling calculation). We also assume alpha is 1.0 and extract it
  // from the calculation. Future work could involve memoizing some of
  // the transcendental functions to minimize repeatedly re-calling
  // them with the same parameters, which we expect to be the case the
  // majority of the time.
  uint32_t calc_read =
    1 +
    static_cast<uint32_t>((num_entries / num_shards) +
			  sqrt((2 * num_entries) *
			       log(num_shards) / num_shards));

  return std::max(min_read, calc_read);
}


int RGWRados::cls_bucket_list_ordered(const DoutPrefixProvider *dpp,
                                      RGWBucketInfo& bucket_info,
                                      const rgw::bucket_index_layout_generation& idx_layout,
                                      const int shard_id,
				      const rgw_obj_index_key& start_after,
				      const std::string& prefix,
				      const std::string& delimiter,
				      const uint32_t num_entries,
				      const bool list_versions,
				      const uint16_t expansion_factor,
				      ent_map_t& m,
				      bool* is_truncated,
				      bool* cls_filtered,
				      rgw_obj_index_key* last_entry,
                                      optional_yield y,
				      RGWBucketListNameFilter force_check_filter)
{
  const bool bitx = cct->_conf->rgw_bucket_index_transaction_instrumentation;

  /* expansion_factor allows the number of entries to read to grow
   * exponentially; this is used when earlier reads are producing too
   * few results, perhaps due to filtering or to a series of
   * namespaced entries */

  ldout_bitx(bitx, dpp, 10) << "ENTERING " << __func__ << ": " << bucket_info.bucket <<
    " start_after=\"" << start_after.to_string() <<
    "\", prefix=\"" << prefix <<
    ", delimiter=\"" << delimiter <<
    "\", shard_id=" << shard_id <<
    "\", num_entries=" << num_entries <<
    ", shard_id=" << shard_id <<
    ", list_versions=" << list_versions <<
    ", expansion_factor=" << expansion_factor <<
    ", force_check_filter is " <<
    (force_check_filter ? "set" : "unset") << dendl_bitx;
  ldout_bitx(bitx, dpp, 25) << "BACKTRACE: " << __func__ << ": " << ClibBackTrace(0) << dendl_bitx;

  m.clear();

  librados::IoCtx index_pool;
  // key   - oid (for different shards if there is any)
  // value - list result for the corresponding oid (shard), it is filled by
  //         the AIO callback
  std::map<int, std::string> shard_oids;
  int r = svc.bi_rados->open_bucket_index(dpp, bucket_info, shard_id, idx_layout,
					  &index_pool, &shard_oids,
					  nullptr);
  if (r < 0) {
    ldpp_dout(dpp, 0) << __func__ <<
      ": open_bucket_index for " << bucket_info.bucket << " failed" << dendl;
    return r;
  }

  const uint32_t shard_count = shard_oids.size();
  if (shard_count == 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
      ": the bucket index shard count appears to be 0, "
      "which is an illegal value" << dendl;
    return -ERR_INVALID_BUCKET_STATE;
  }

  uint32_t num_entries_per_shard;
  if (expansion_factor == 0) {
    num_entries_per_shard =
      calc_ordered_bucket_list_per_shard(num_entries, shard_count);
  } else if (expansion_factor <= 11) {
    // we'll max out the exponential multiplication factor at 1024 (2<<10)
    num_entries_per_shard =
      std::min(num_entries,
	       (uint32_t(1 << (expansion_factor - 1)) *
		calc_ordered_bucket_list_per_shard(num_entries, shard_count)));
  } else {
    num_entries_per_shard = num_entries;
  }

  if (num_entries_per_shard == 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
      ": unable to calculate the number of entries to read from each "
      "bucket index shard" << dendl;
    return -ERR_INVALID_BUCKET_STATE;
  }

  ldpp_dout(dpp, 10) << __func__ <<
    ": request from each of " << shard_count <<
    " shard(s) for " << num_entries_per_shard << " entries to get " <<
    num_entries << " total entries" << dendl;

  auto& ioctx = index_pool;
  std::map<int, rgw_cls_list_ret> shard_list_results;
  cls_rgw_obj_key start_after_key(start_after.name, start_after.instance);
  r = CLSRGWIssueBucketList(ioctx, start_after_key, prefix, delimiter,
			    num_entries_per_shard,
			    list_versions, shard_oids, shard_list_results,
			    cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0) {
    ldpp_dout(dpp, 0) << __func__ <<
      ": CLSRGWIssueBucketList for " << bucket_info.bucket <<
      " failed" << dendl;
    return r;
  }

  // to manage the iterators through each shard's list results
  struct ShardTracker {
    const size_t shard_idx;
    rgw_cls_list_ret& result;
    const std::string& oid_name;
    RGWRados::ent_map_t::iterator cursor;
    RGWRados::ent_map_t::iterator end;

    // manages an iterator through a shard and provides other
    // accessors
    ShardTracker(size_t _shard_idx,
		 rgw_cls_list_ret& _result,
		 const std::string& _oid_name):
      shard_idx(_shard_idx),
      result(_result),
      oid_name(_oid_name),
      cursor(_result.dir.m.begin()),
      end(_result.dir.m.end())
    {}

    inline const std::string& entry_name() const {
      return cursor->first;
    }
    rgw_bucket_dir_entry& dir_entry() const {
      return cursor->second;
    }
    inline bool is_truncated() const {
      return result.is_truncated;
    }
    inline ShardTracker& advance() {
      ++cursor;
      // return a self-reference to allow for chaining of calls, such
      // as x.advance().at_end()
      return *this;
    }
    inline bool at_end() const {
      return cursor == end;
    }
  }; // ShardTracker

  // add the next unique candidate, or return false if we reach the end
  auto next_candidate = [] (CephContext *cct, ShardTracker& t,
                            std::multimap<std::string, size_t>& candidates,
                            size_t tracker_idx) {
    if (!t.at_end()) {
      candidates.emplace(t.entry_name(), tracker_idx);
    }
    return;
  };

  // one tracker per shard requested (may not be all shards)
  std::vector<ShardTracker> results_trackers;
  results_trackers.reserve(shard_list_results.size());
  for (auto& r : shard_list_results) {
    results_trackers.emplace_back(r.first, r.second, shard_oids[r.first]);

    // if any *one* shard's result is truncated, the entire result is
    // truncated
    *is_truncated = *is_truncated || r.second.is_truncated;

    // unless *all* are shards are cls_filtered, the entire result is
    // not filtered
    *cls_filtered = *cls_filtered && r.second.cls_filtered;
  }

  // create a map to track the next candidate entry from ShardTracker
  // (key=candidate, value=index into results_trackers); as we consume
  // entries from shards, we replace them with the next entries in the
  // shards until we run out
  std::multimap<std::string, size_t> candidates;
  size_t tracker_idx = 0;
  std::vector<size_t> vidx;
  vidx.reserve(shard_list_results.size());
  for (auto& t : results_trackers) {
    // it's important that the values in the map refer to the index
    // into the results_trackers vector, which may not be the same
    // as the shard number (i.e., when not all shards are requested)
    next_candidate(cct, t, candidates, tracker_idx);
    ++tracker_idx;
  }

  rgw_bucket_dir_entry*
    last_entry_visited = nullptr; // to set last_entry (marker)
  std::map<std::string, bufferlist> updates;
  uint32_t count = 0;
  while (count < num_entries && !candidates.empty()) {
    r = 0;
    // select the next entry in lexical order (first key in map);
    // again tracker_idx is not necessarily shard number, but is index
    // into results_trackers vector
    tracker_idx = candidates.begin()->second;
    auto& tracker = results_trackers.at(tracker_idx);

    const std::string& name = tracker.entry_name();
    rgw_bucket_dir_entry& dirent = tracker.dir_entry();

    ldpp_dout(dpp, 20) << __func__ << ": currently processing " <<
      dirent.key << " from shard " << tracker.shard_idx << dendl;

    const bool force_check =
      force_check_filter && force_check_filter(dirent.key.name);

    if ((!dirent.exists &&
	 !dirent.is_delete_marker() &&
	 !dirent.is_common_prefix()) ||
        !dirent.pending_map.empty() ||
        force_check) {
      /* there are uncommitted ops. We need to check the current
       * state, and if the tags are old we need to do clean-up as
       * well. */
      librados::IoCtx sub_ctx;
      sub_ctx.dup(ioctx);
      ldout_bitx(bitx, dpp, 20) << "INFO: " << __func__ <<
	" calling check_disk_state bucket=" << bucket_info.bucket <<
	" entry=" << dirent.key << dendl_bitx;
      r = check_disk_state(dpp, sub_ctx, bucket_info, dirent, dirent,
			   updates[tracker.oid_name], y);
      if (r < 0 && r != -ENOENT) {
	ldpp_dout(dpp, 0) << __func__ <<
	  ": check_disk_state for \"" << dirent.key <<
	  "\" failed with r=" << r << dendl;
	return r;
      }
    } else {
      r = 0;
    }

    const cls_rgw_obj_key dirent_key = dirent.key;

    // at this point either r >= 0 or r == -ENOENT
    if (r >= 0) { // i.e., if r != -ENOENT
      ldpp_dout(dpp, 10) << __func__ << ": got " <<
	dirent_key << dendl;

      auto [it, inserted] = m.insert_or_assign(name, std::move(dirent));
      last_entry_visited = &it->second;
      if (inserted) {
	++count;
      } else {
	ldpp_dout(dpp, 0) << "WARNING: " << __func__ <<
	  " reassigned map value at \"" << name <<
	  "\", which should not happen" << dendl;
      }
    } else {
      ldpp_dout(dpp, 10) << __func__ << ": skipping " <<
	dirent.key.name << "[" << dirent.key.instance << "]" << dendl;
      last_entry_visited = &tracker.dir_entry();
    }

    // refresh the candidates map
    vidx.clear();
    bool need_to_stop = false;
    auto range = candidates.equal_range(name);
    for (auto i = range.first; i != range.second; ++i) {
      vidx.push_back(i->second);
    } 
    candidates.erase(range.first, range.second);
    for (auto idx : vidx) {
      auto& tracker_match = results_trackers.at(idx);
      tracker_match.advance();
      next_candidate(cct, tracker_match, candidates, idx);
      if (tracker_match.at_end() && tracker_match.is_truncated()) {
        need_to_stop = true;
        break;
      }
    }
    if (need_to_stop) {
      // once we exhaust one shard that is truncated, we need to stop,
      // as we cannot be certain that one of the next entries needs to
      // come from that shard; S3 and swift protocols allow returning
      // fewer than what was requested
      ldpp_dout(dpp, 10) << __func__ <<
	": stopped accumulating results at count=" << count <<
	", dirent=\"" << dirent_key <<
	"\", because its shard is truncated and exhausted" << dendl;
      break;
    }
  } // while we haven't provided requested # of result entries

  // suggest updates if there are any
  for (auto& miter : updates) {
    if (miter.second.length()) {
      ObjectWriteOperation o;
      cls_rgw_suggest_changes(o, miter.second);
      // we don't care if we lose suggested updates, send them off blindly
      AioCompletion *c =
	librados::Rados::aio_create_completion(nullptr, nullptr);

      ldout_bitx(bitx, dpp, 10) << "INFO: " << __func__ <<
	": doing dir_suggest on " << miter.first << dendl_bitx;
      ioctx.aio_operate(miter.first, c, &o);
      c->release();
    }
  } // updates loop

  // determine truncation by checking if all the returned entries are
  // consumed or not
  *is_truncated = false;
  for (const auto& t : results_trackers) {
    if (!t.at_end() || t.is_truncated()) {
      *is_truncated = true;
      break;
    }
  }

  ldpp_dout(dpp, 20) << __func__ <<
    ": returning, count=" << count << ", is_truncated=" << *is_truncated <<
    dendl;

  if (*is_truncated && count < num_entries) {
    ldpp_dout(dpp, 10) << __func__ <<
      ": requested " << num_entries << " entries but returning " <<
      count << ", which is truncated" << dendl;
  }

  if (last_entry_visited != nullptr && last_entry) {
    *last_entry = last_entry_visited->key;
    ldpp_dout(dpp, 20) << __func__ <<
      ": returning, last_entry=" << *last_entry << dendl;
  } else {
    ldpp_dout(dpp, 20) << __func__ <<
      ": returning, last_entry NOT SET" << dendl;
  }

  ldout_bitx(bitx, dpp, 10) << "EXITING " << __func__ << dendl_bitx;
  return 0;
} // RGWRados::cls_bucket_list_ordered


// A helper function to retrieve the hash source from an incomplete
// multipart entry by removing everything from the second to last
// period on.
static int parse_index_hash_source(const std::string& oid_wo_ns, std::string *index_hash_source) {
  std::size_t found = oid_wo_ns.rfind('.');
  if (found == std::string::npos || found < 1) {
    return -EINVAL;
  }
  found = oid_wo_ns.rfind('.', found - 1);
  if (found == std::string::npos || found < 1) {
    return -EINVAL;
  }
  *index_hash_source = oid_wo_ns.substr(0, found);
  return 0;
}


int RGWRados::cls_bucket_list_unordered(const DoutPrefixProvider *dpp,
                                        RGWBucketInfo& bucket_info,
                                        const rgw::bucket_index_layout_generation& idx_layout,
                                        int shard_id,
					const rgw_obj_index_key& start_after,
					const std::string& prefix,
					uint32_t num_entries,
					bool list_versions,
					std::vector<rgw_bucket_dir_entry>& ent_list,
					bool *is_truncated,
					rgw_obj_index_key *last_entry,
                                        optional_yield y,
					RGWBucketListNameFilter force_check_filter) {
  const bool bitx = cct->_conf->rgw_bucket_index_transaction_instrumentation;

  ldout_bitx(bitx, dpp, 10) << "ENTERING " << __func__ << ": " << bucket_info.bucket <<
    " start_after=\"" << start_after <<
    "\", prefix=\"" << prefix <<
    "\", shard_id=" << shard_id <<
    "\", num_entries=" << num_entries <<
    ", list_versions=" << list_versions <<
    (force_check_filter ? "set" : "unset") << dendl_bitx;
  ldout_bitx(bitx, dpp, 25) << "BACKTRACE: " << __func__ << ": " << ClibBackTrace(0) << dendl_bitx;

  ent_list.clear();

  *is_truncated = false;
  librados::IoCtx index_pool;

  std::map<int, std::string> oids;
  int r = svc.bi_rados->open_bucket_index(dpp, bucket_info, shard_id, idx_layout, &index_pool, &oids, nullptr);
  if (r < 0) {
    return r;
  }

  auto& ioctx = index_pool;

  const uint32_t num_shards = oids.size();

  rgw_obj_index_key marker = start_after;
  uint32_t current_shard;
  if (shard_id >= 0) {
    current_shard = shard_id;
  } else if (start_after.empty()) {
    current_shard = 0u;
  } else {
    // at this point we have a marker (start_after) that has something
    // in it, so we need to get to the bucket shard index, so we can
    // start reading from there


    // now convert the key (oid) to an rgw_obj_key since that will
    // separate out the namespace, name, and instance
    rgw_obj_key obj_key;
    bool parsed = rgw_obj_key::parse_raw_oid(start_after.name, &obj_key);
    if (!parsed) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	" received an invalid start marker: \"" << start_after << "\"" <<
	dendl;
      return -EINVAL;
    } else if (obj_key.name.empty()) {
      // if the name is empty that means the object name came in with
      // a namespace only, and therefore we need to start our scan at
      // the first bucket index shard
      current_shard = 0u;
    } else {
      // so now we have the key used to compute the bucket index shard
      // and can extract the specific shard from it
      if (obj_key.ns == RGW_OBJ_NS_MULTIPART) {
        // Use obj_key.ns == RGW_OBJ_NS_MULTIPART instead of
        // the implementation relying on MultipartMetaFilter
        // because MultipartMetaFilter only checks .meta suffix, which may
        // exclude data multiparts but include some regular objects with .meta suffix
        // by mistake.
        string index_hash_source;
        r = parse_index_hash_source(obj_key.name, &index_hash_source);
        if (r < 0) {
	  ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	    " parse_index_hash_source unable to parse \"" << obj_key.name <<
	    "\", r=" << r << dendl;
          return r;
        }
        current_shard = svc.bi_rados->bucket_shard_index(index_hash_source, num_shards);
      } else {
        current_shard = svc.bi_rados->bucket_shard_index(obj_key.name, num_shards);
      }
    }
  }

  uint32_t count = 0u;
  std::map<std::string, bufferlist> updates;
  rgw_obj_index_key last_added_entry;
  while (count <= num_entries &&
	 ((shard_id >= 0 && current_shard == uint32_t(shard_id)) ||
	  current_shard < num_shards)) {
    const std::string& oid = oids[current_shard];
    rgw_cls_list_ret result;

    librados::ObjectReadOperation op;
    const std::string empty_delimiter;
    cls_rgw_bucket_list_op(op, marker, prefix, empty_delimiter,
			   num_entries,
                           list_versions, &result);
    r = rgw_rados_operate(dpp, ioctx, oid, &op, nullptr, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	": error in rgw_rados_operate (bucket list op), r=" << r << dendl;
      return r;
    }

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
	sub_ctx.dup(ioctx);
	ldout_bitx(bitx, dpp, 20) << "INFO: " << __func__ <<
	  ": calling check_disk_state bucket=" << bucket_info.bucket <<
	  " entry=" << dirent.key << dendl_bitx;
	r = check_disk_state(dpp, sub_ctx, bucket_info, dirent, dirent, updates[oid], y);
	if (r < 0 && r != -ENOENT) {
	  ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	    ": error in check_disk_state, r=" << r << dendl;
	  return r;
	}
      } else {
        r = 0;
      }

      // at this point either r >= 0 or r == -ENOENT
      if (r >= 0) { // i.e., if r != -ENOENT
	ldpp_dout(dpp, 10) << __func__ << ": got " <<
	  dirent.key << dendl;

	if (count < num_entries) {
	  marker = last_added_entry = dirent.key; // double assign
	  ent_list.emplace_back(std::move(dirent));
	  ++count;
	} else {
	  *is_truncated = true;
	  ldpp_dout(dpp, 10) << "INFO: " << __func__ <<
	    ": reached max entries (" << num_entries << ") to return at \"" <<
	    dirent.key << "\"" << dendl;
	  goto check_updates;
	}
      } else { // r == -ENOENT
	// in the case of -ENOENT, make sure we're advancing marker
	// for possible next call to CLSRGWIssueBucketList
	marker = dirent.key;
      }
    } // entry for loop

    if (!result.is_truncated) {
      // if we reached the end of the shard read next shard
      ++current_shard;
      marker = rgw_obj_index_key();
    }
  } // shard loop

check_updates:

  // suggest updates if there is any
  std::map<std::string, bufferlist>::iterator miter = updates.begin();
  for (; miter != updates.end(); ++miter) {
    if (miter->second.length()) {
      ObjectWriteOperation o;
      cls_rgw_suggest_changes(o, miter->second);
      // we don't care if we lose suggested updates, send them off blindly
      AioCompletion *c = librados::Rados::aio_create_completion(nullptr, nullptr);

      ldout_bitx(bitx, dpp, 10) << "INFO: " << __func__ <<
	" doing dir_suggest on " << miter->first << dendl_bitx;
      ioctx.aio_operate(miter->first, c, &o);
      c->release();
    }
  }

  if (last_entry && !ent_list.empty()) {
    *last_entry = last_added_entry;
  }

  ldout_bitx(bitx, dpp, 10) << "EXITING " << __func__ << dendl_bitx;
  return 0;
} // RGWRados::cls_bucket_list_unordered


int RGWRados::cls_obj_usage_log_add(const DoutPrefixProvider *dpp, const string& oid,
				    rgw_usage_log_info& info, optional_yield y)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().usage_log_pool, oid);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(dpp, obj, &ref);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;
  cls_rgw_usage_log_add(op, info);

  r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
  return r;
}

int RGWRados::cls_obj_usage_log_read(const DoutPrefixProvider *dpp, const string& oid, const string& user, const string& bucket,
                                     uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                                     string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage,
				     bool *is_truncated)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().usage_log_pool, oid);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(dpp, obj, &ref);
  if (r < 0) {
    return r;
  }

  *is_truncated = false;

  r = cls_rgw_usage_log_read(ref.ioctx, ref.obj.oid, user, bucket, start_epoch, end_epoch,
			     max_entries, read_iter, usage, is_truncated);

  return r;
}

static int cls_rgw_usage_log_trim_repeat(const DoutPrefixProvider *dpp, rgw_rados_ref ref, const string& user, const string& bucket, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
{
  bool done = false;
  do {
    librados::ObjectWriteOperation op;
    cls_rgw_usage_log_trim(op, user, bucket, start_epoch, end_epoch);
    int r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
    if (r == -ENODATA)
      done = true;
    else if (r < 0)
      return r;
  } while (!done);

  return 0;
}

int RGWRados::cls_obj_usage_log_trim(const DoutPrefixProvider *dpp, const string& oid, const string& user, const string& bucket,
                                    uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().usage_log_pool, oid);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(dpp, obj, &ref);
  if (r < 0) {
    return r;
  }

  r = cls_rgw_usage_log_trim_repeat(dpp, ref, user, bucket, start_epoch, end_epoch, y);
  return r;
}

int RGWRados::cls_obj_usage_log_clear(const DoutPrefixProvider *dpp, string& oid, optional_yield y)
{
  rgw_raw_obj obj(svc.zone->get_zone_params().usage_log_pool, oid);

  rgw_rados_ref ref;
  int r = get_raw_obj_ref(dpp, obj, &ref);
  if (r < 0) {
    return r;
  }
  librados::ObjectWriteOperation op;
  cls_rgw_usage_log_clear(op);
  r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
  return r;
}


// note: this removes entries from the rados bucket index objects
// without going through CLS; this is known to be called from
// "radosgw-admin unlink" and "radosgw-admin bucket check --fix"
int RGWRados::remove_objs_from_index(const DoutPrefixProvider *dpp,
				     RGWBucketInfo& bucket_info,
				     const std::list<rgw_obj_index_key>& entry_key_list)
{
  const bool bitx = cct->_conf->rgw_bucket_index_transaction_instrumentation;
  ldout_bitx(bitx, dpp, 10) << "ENTERING " << __func__ << ": bucket=" << bucket_info.bucket <<
    " entry_key_list.size()=" << entry_key_list.size() << dendl_bitx;
  ldout_bitx(bitx, dpp, 25) << "BACKTRACE: " << __func__ << ": " << ClibBackTrace(0) << dendl_bitx;

  const auto& current_index = bucket_info.get_current_index();
  if (is_layout_indexless(current_index)) {
    return -EINVAL;
  }
  const uint32_t num_shards = current_index.layout.normal.num_shards;

  librados::IoCtx index_pool;
  std::map<int, std::string> index_oids;
  int r = svc.bi_rados->open_bucket_index(dpp, bucket_info, std::nullopt,
					  bucket_info.layout.current_index,
					  &index_pool, &index_oids, nullptr);
  if (r < 0) {
    ldout_bitx(bitx, dpp, 0) << "ERROR: " << __func__ <<
      " open_bucket_index returned " << r << dendl_bitx;
    return r;
  }

  // split up removals by shard
  std::map<int, std::set<std::string>> sharded_removals;
  for (const auto& entry_key : entry_key_list) {
    const rgw_obj_key obj_key(entry_key);
    const uint32_t shard =
      RGWSI_BucketIndex_RADOS::bucket_shard_index(obj_key, num_shards);

    // entry_key already combines namespace and name, so we first have
    // to break that apart before we can then combine with instance
    std::string name;
    std::string ns; // namespace
    rgw_obj_key::parse_index_key(entry_key.name, &name, &ns);
    rgw_obj_key full_key(name, entry_key.instance, ns);
    std::string combined_key = full_key.get_oid();

    sharded_removals[shard].insert(combined_key);

    ldout_bitx(bitx, dpp, 20) << "INFO: " << __func__ <<
      ": removal from bucket index, bucket=" << bucket_info.bucket <<
      " key=" << combined_key << " designated for shard " << shard <<
      dendl_bitx;
  }

  for (const auto& removals : sharded_removals) {
    const int shard = removals.first;
    const std::string& oid = index_oids[shard];

    ldout_bitx(bitx, dpp, 10) << "INFO: " << __func__ <<
      ": removal from bucket index, bucket=" << bucket_info.bucket <<
      ", shard=" << shard << ", oid=" << oid << ", num_keys=" <<
      removals.second.size() << dendl_bitx;

    r = index_pool.omap_rm_keys(oid, removals.second);
    if (r < 0) {
      ldout_bitx(bitx, dpp, 0) << "ERROR: " << __func__ <<
	": omap_rm_keys returned ret=" << r <<
	dendl_bitx;
      return r;
    }
  }

  ldout_bitx(bitx, dpp, 5) <<
    "EXITING " << __func__ << " and returning " << r << dendl_bitx;

  return r;
}

int RGWRados::check_disk_state(const DoutPrefixProvider *dpp,
                               librados::IoCtx io_ctx,
                               RGWBucketInfo& bucket_info,
                               rgw_bucket_dir_entry& list_state,
                               rgw_bucket_dir_entry& object,
                               bufferlist& suggested_updates,
                               optional_yield y)
{
  const bool bitx = cct->_conf->rgw_bucket_index_transaction_instrumentation;
  ldout_bitx(bitx, dpp, 10) << "ENTERING " << __func__ << ": bucket=" <<
    bucket_info.bucket << " dir_entry=" << list_state.key << dendl_bitx;

  uint8_t suggest_flag = (svc.zone->need_to_log_data() ? CEPH_RGW_DIR_SUGGEST_LOG_OP : 0);

  std::string loc;

  rgw_obj obj(bucket_info.bucket, list_state.key);

  string temp_key;
  if (MultipartMetaFilter(list_state.key.name, temp_key)) {
    obj.in_extra_data = true;
  }

  string oid;
  get_obj_bucket_and_oid_loc(obj, oid, loc);

  if (loc != list_state.locator) {
    ldpp_dout(dpp, 0) << "WARNING: generated locator (" << loc << ") is different from listed locator (" << list_state.locator << ")" << dendl;
  }

  io_ctx.locator_set_key(list_state.locator);

  RGWObjState *astate = NULL;
  RGWObjManifest *manifest = nullptr;
  RGWObjectCtx octx(this->driver);
  int r = get_obj_state(dpp, &octx, bucket_info, obj, &astate, &manifest, false, y);
  if (r < 0)
    return r;

  list_state.pending_map.clear(); // we don't need this and it inflates size
  if (!list_state.is_delete_marker() && !astate->exists) {
    ldout_bitx(bitx, dpp, 10) << "INFO: " << __func__ << ": disk state exists" << dendl_bitx;
      /* object doesn't exist right now -- hopefully because it's
       * marked as !exists and got deleted */
    if (list_state.exists) {
      ldout_bitx(bitx, dpp, 10) << "INFO: " << __func__ << ": index list state exists" << dendl_bitx;
      /* FIXME: what should happen now? Work out if there are any
       * non-bad ways this could happen (there probably are, but annoying
       * to handle!) */
    }

    // encode a suggested removal of that key
    list_state.ver.epoch = io_ctx.get_last_version();
    list_state.ver.pool = io_ctx.get_id();
    ldout_bitx(bitx, dpp, 10) << "INFO: " << __func__ << ": encoding remove of " << list_state.key << " on suggested_updates" << dendl_bitx;
    cls_rgw_encode_suggestion(CEPH_RGW_REMOVE | suggest_flag, list_state, suggested_updates);
    return -ENOENT;
  }

  string etag;
  string content_type;
  string storage_class;
  ACLOwner owner;
  bool appendable = false;

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
  iter = astate->attrset.find(RGW_ATTR_STORAGE_CLASS);
  if (iter != astate->attrset.end()) {
    storage_class = rgw_bl_str(iter->second);
  }
  iter = astate->attrset.find(RGW_ATTR_ACL);
  if (iter != astate->attrset.end()) {
    r = decode_policy(dpp, iter->second, &owner);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "WARNING: could not decode policy for object: " << obj << dendl;
    }
  }
  iter = astate->attrset.find(RGW_ATTR_APPEND_PART_NUM);
  if (iter != astate->attrset.end()) {
    appendable = true;
  }

  if (manifest) {
    RGWObjManifest::obj_iterator miter;
    for (miter = manifest->obj_begin(dpp); miter != manifest->obj_end(dpp); ++miter) {
      const rgw_raw_obj& raw_loc = miter.get_location().get_raw_obj(this);
      rgw_obj loc;
      RGWSI_Tier_RADOS::raw_obj_to_obj(manifest->get_obj().bucket, raw_loc, &loc);

      if (loc.key.ns == RGW_OBJ_NS_MULTIPART) {
	ldout_bitx(bitx, dpp, 10) << "INFO: " << __func__ << " removing manifest part from index loc=" << loc << dendl_bitx;
	r = delete_obj_index(loc, astate->mtime, dpp, y);
	if (r < 0) {
	  ldout_bitx(bitx, dpp, 0) <<
	    "WARNING: " << __func__ << ": delete_obj_index returned r=" << r << dendl_bitx;
	}
      }
    }
  }

  object.meta.etag = etag;
  object.meta.content_type = content_type;
  object.meta.storage_class = storage_class;
  object.meta.owner = owner.id.to_str();
  object.meta.owner_display_name = owner.display_name;
  object.meta.appendable = appendable;

  // encode suggested updates

  list_state.meta.size = object.meta.size;
  list_state.meta.accounted_size = object.meta.accounted_size;
  list_state.meta.mtime = object.meta.mtime;
  list_state.meta.category = main_category;
  list_state.meta.etag = etag;
  list_state.meta.appendable = appendable;
  list_state.meta.content_type = content_type;
  list_state.meta.storage_class = storage_class;

  librados::IoCtx head_obj_ctx; // initialize to data pool so we can get pool id
  r = get_obj_head_ioctx(dpp, bucket_info, obj, &head_obj_ctx);
  if (r < 0) {
    ldpp_dout(dpp, 0) << __func__ <<
      " WARNING: unable to find head object data pool for \"" <<
      obj << "\", not updating version pool/epoch" << dendl;
  } else {
    list_state.ver.pool = head_obj_ctx.get_id();
    list_state.ver.epoch = astate->epoch;
  }

  if (astate->obj_tag.length() > 0) {
    list_state.tag = astate->obj_tag.c_str();
  }

  list_state.meta.owner = owner.id.to_str();
  list_state.meta.owner_display_name = owner.display_name;

  list_state.exists = true;

  ldout_bitx(bitx, dpp, 10) << "INFO: " << __func__ <<
    ": encoding update of " << list_state.key << " on suggested_updates" << dendl_bitx;
  cls_rgw_encode_suggestion(CEPH_RGW_UPDATE | suggest_flag, list_state, suggested_updates);

  ldout_bitx(bitx, dpp, 10) << "EXITING " << __func__ << dendl_bitx;
  return 0;
} // RGWRados::check_disk_state

int RGWRados::cls_bucket_head(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw::bucket_index_layout_generation& idx_layout, int shard_id, vector<rgw_bucket_dir_header>& headers, map<int, string> *bucket_instance_ids)
{
  librados::IoCtx index_pool;
  map<int, string> oids;
  map<int, struct rgw_cls_list_ret> list_results;
  int r = svc.bi_rados->open_bucket_index(dpp, bucket_info, shard_id, idx_layout, &index_pool, &oids, bucket_instance_ids);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "cls_bucket_head: open_bucket_index() returned "
                   << r << dendl;
    return r;
  }

  r = CLSRGWIssueGetDirHeader(index_pool, oids, list_results, cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0) {
    ldpp_dout(dpp, 20) << "cls_bucket_head: CLSRGWIssueGetDirHeader() returned "
                   << r << dendl;
    return r;
  }

  map<int, struct rgw_cls_list_ret>::iterator iter = list_results.begin();
  for(; iter != list_results.end(); ++iter) {
    headers.push_back(std::move(iter->second.dir.header));
  }
  return 0;
}

int RGWRados::cls_bucket_head_async(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info,
                                    const rgw::bucket_index_layout_generation& idx_layout, int shard_id,
                                    boost::intrusive_ptr<RGWGetDirHeader_CB> cb, int *num_aio)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;
  int r = svc.bi_rados->open_bucket_index(dpp, bucket_info, shard_id, idx_layout, &index_pool, &bucket_objs, nullptr);
  if (r < 0)
    return r;

  for (auto& pair : bucket_objs) {
    r = cls_rgw_get_dir_header_async(index_pool, pair.second, cb);
    if (r < 0) {
      return r;
    }
    (*num_aio)++;
  }
  return 0;
}

int RGWRados::check_bucket_shards(const RGWBucketInfo& bucket_info,
				  uint64_t num_objs,
                                  const DoutPrefixProvider *dpp, optional_yield y)
{
  if (! cct->_conf.get_val<bool>("rgw_dynamic_resharding")) {
      return 0;
  }

  if (! is_layout_reshardable(bucket_info.layout)) {
    return 0;
  }

  bool need_resharding = false;
  uint32_t num_source_shards = rgw::current_num_shards(bucket_info.layout);
  const uint32_t max_dynamic_shards =
    uint32_t(cct->_conf.get_val<uint64_t>("rgw_max_dynamic_shards"));

  if (num_source_shards >= max_dynamic_shards) {
    return 0;
  }

  uint32_t suggested_num_shards = 0;
  const uint64_t max_objs_per_shard =
    cct->_conf.get_val<uint64_t>("rgw_max_objs_per_shard");

  // TODO: consider per-bucket sync policy here?
  const bool is_multisite = svc.zone->need_to_log_data();

  quota_handler->check_bucket_shards(dpp, max_objs_per_shard, num_source_shards,
				     num_objs, is_multisite, need_resharding,
				     &suggested_num_shards);
  if (! need_resharding) {
    return 0;
  }

  const uint32_t final_num_shards =
    RGWBucketReshard::get_preferred_shards(suggested_num_shards,
					   max_dynamic_shards);
  // final verification, so we don't reduce number of shards
  if (final_num_shards <= num_source_shards) {
    return 0;
  }

  ldpp_dout(dpp, 1) << "RGWRados::" << __func__ << " bucket " << bucket_info.bucket.name <<
    " needs resharding; current num shards " << bucket_info.layout.current_index.layout.normal.num_shards <<
    "; new num shards " << final_num_shards << " (suggested " <<
    suggested_num_shards << ")" << dendl;

  return add_bucket_to_reshard(dpp, bucket_info, final_num_shards, y);
}

int RGWRados::add_bucket_to_reshard(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, uint32_t new_num_shards, optional_yield y)
{
  RGWReshard reshard(this->driver, dpp);

  uint32_t num_source_shards = rgw::current_num_shards(bucket_info.layout);

  new_num_shards = std::min(new_num_shards, get_max_bucket_shards());
  if (new_num_shards <= num_source_shards) {
    ldpp_dout(dpp, 20) << "not resharding bucket name=" << bucket_info.bucket.name << ", orig_num=" << num_source_shards << ", new_num_shards=" << new_num_shards << dendl;
    return 0;
  }

  cls_rgw_reshard_entry entry;
  entry.time = real_clock::now();
  entry.tenant = bucket_info.owner.tenant;
  entry.bucket_name = bucket_info.bucket.name;
  entry.bucket_id = bucket_info.bucket.bucket_id;
  entry.old_num_shards = num_source_shards;
  entry.new_num_shards = new_num_shards;

  return reshard.add(dpp, entry, y);
}

int RGWRados::check_quota(const DoutPrefixProvider *dpp, const rgw_user& bucket_owner, rgw_bucket& bucket,
                          RGWQuota& quota,
			  uint64_t obj_size, optional_yield y,
			  bool check_size_only)
{
  // if we only check size, then num_objs will set to 0
  if(check_size_only)
    return quota_handler->check_quota(dpp, bucket_owner, bucket, quota, 0, obj_size, y);

  return quota_handler->check_quota(dpp, bucket_owner, bucket, quota, 1, obj_size, y);
}

int RGWRados::get_target_shard_id(const rgw::bucket_index_normal_layout& layout, const string& obj_key,
                                  int *shard_id)
{
  int r = 0;
  switch (layout.hash_type) {
    case rgw::BucketHashType::Mod:
      if (!layout.num_shards) {
        if (shard_id) {
          *shard_id = -1;
        }
      } else {
        uint32_t sid = svc.bi_rados->bucket_shard_index(obj_key, layout.num_shards);
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
  std::lock_guard l{bucket_id_lock};
  return ++max_bucket_id;
}

librados::Rados* RGWRados::get_rados_handle()
{
  return &rados;
}

int RGWRados::delete_raw_obj_aio(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, list<librados::AioCompletion *>& handles)
{
  rgw_rados_ref ref;
  int ret = get_raw_obj_ref(dpp, obj, &ref);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to get obj ref with ret=" << ret << dendl;
    return ret;
  }

  ObjectWriteOperation op;
  list<string> prefixes;
  cls_rgw_remove_obj(op, prefixes);

  AioCompletion *c = librados::Rados::aio_create_completion(nullptr, nullptr);
  ret = ref.ioctx.aio_operate(ref.obj.oid, c, &op);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: AioOperate failed with ret=" << ret << dendl;
    c->release();
    return ret;
  }

  handles.push_back(c);

  return 0;
}

int RGWRados::delete_obj_aio(const DoutPrefixProvider *dpp, const rgw_obj& obj,
                             RGWBucketInfo& bucket_info, RGWObjState *astate,
                             list<librados::AioCompletion *>& handles, bool keep_index_consistent,
                             optional_yield y)
{
  rgw_rados_ref ref;
  int ret = get_obj_head_ref(dpp, bucket_info, obj, &ref);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to get obj ref with ret=" << ret << dendl;
    return ret;
  }

  if (keep_index_consistent) {
    RGWRados::Bucket bop(this, bucket_info);
    RGWRados::Bucket::UpdateIndex index_op(&bop, obj);

    ret = index_op.prepare(dpp, CLS_RGW_OP_DEL, &astate->write_tag, y);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed to prepare index op with ret=" << ret << dendl;
      return ret;
    }
  }

  ObjectWriteOperation op;
  list<string> prefixes;
  cls_rgw_remove_obj(op, prefixes);

  AioCompletion *c = librados::Rados::aio_create_completion(nullptr, nullptr);
  ret = ref.ioctx.aio_operate(ref.obj.oid, c, &op);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: AioOperate failed with ret=" << ret << dendl;
    c->release();
    return ret;
  }

  handles.push_back(c);

  if (keep_index_consistent) {
    ret = delete_obj_index(obj, astate->mtime, dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed to delete obj index with ret=" << ret << dendl;
      return ret;
    }
  }
  return ret;
}

void objexp_hint_entry::generate_test_instances(list<objexp_hint_entry*>& o)
{
  auto it = new objexp_hint_entry;
  it->tenant = "tenant1";
  it->bucket_name = "bucket1";
  it->bucket_id = "1234";
  it->obj_key = rgw_obj_key("obj");
  o.push_back(it);
  o.push_back(new objexp_hint_entry);
}

void objexp_hint_entry::dump(Formatter *f) const
{
  f->open_object_section("objexp_hint_entry");
  encode_json("tenant", tenant, f);
  encode_json("bucket_name", bucket_name, f);
  encode_json("bucket_id", bucket_id, f);
  encode_json("rgw_obj_key", obj_key, f);
  utime_t ut(exp_time);
  encode_json("exp_time", ut, f);
  f->close_section();
}

void RGWOLHInfo::generate_test_instances(list<RGWOLHInfo*> &o)
{
  RGWOLHInfo *olh = new RGWOLHInfo;
  olh->removed = false;
  o.push_back(olh);
  o.push_back(new RGWOLHInfo);
}

void RGWOLHInfo::dump(Formatter *f) const
{
  encode_json("target", target, f);
  encode_json("removed", removed, f);
}

void RGWOLHPendingInfo::dump(Formatter *f) const
{
  utime_t ut(time);
  encode_json("time", ut, f);
}

void RGWOLHPendingInfo::generate_test_instances(list<RGWOLHPendingInfo*>& o)
{
  auto it = new RGWOLHPendingInfo;
  it->time = ceph::real_clock::zero();
  o.push_back(it);
}
