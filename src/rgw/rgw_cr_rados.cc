// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "include/compat.h"
#include "rgw_sal.h"
#include "rgw_zone.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sync_counters.h"
#include "rgw_bucket.h"

#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"
#include "services/svc_sys_obj.h"
#include "services/svc_cls.h"

#include "cls/lock/cls_lock_client.h"
#include "cls/rgw/cls_rgw_client.h"

#include <boost/asio/yield.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

bool RGWAsyncRadosProcessor::RGWWQ::_enqueue(RGWAsyncRadosRequest *req) {
  if (processor->is_going_down()) {
    return false;
  }
  req->get();
  processor->m_req_queue.push_back(req);
  dout(20) << "enqueued request req=" << hex << req << dec << dendl;
  _dump_queue();
  return true;
}

bool RGWAsyncRadosProcessor::RGWWQ::_empty() {
  return processor->m_req_queue.empty();
}

RGWAsyncRadosRequest *RGWAsyncRadosProcessor::RGWWQ::_dequeue() {
  if (processor->m_req_queue.empty())
    return NULL;
  RGWAsyncRadosRequest *req = processor->m_req_queue.front();
  processor->m_req_queue.pop_front();
  dout(20) << "dequeued request req=" << hex << req << dec << dendl;
  _dump_queue();
  return req;
}

void RGWAsyncRadosProcessor::RGWWQ::_process(RGWAsyncRadosRequest *req, ThreadPool::TPHandle& handle) {
  processor->handle_request(req);
  processor->req_throttle.put(1);
}

void RGWAsyncRadosProcessor::RGWWQ::_dump_queue() {
  if (!g_conf()->subsys.should_gather<ceph_subsys_rgw, 20>()) {
    return;
  }
  deque<RGWAsyncRadosRequest *>::iterator iter;
  if (processor->m_req_queue.empty()) {
    dout(20) << "RGWWQ: empty" << dendl;
    return;
  }
  dout(20) << "RGWWQ:" << dendl;
  for (iter = processor->m_req_queue.begin(); iter != processor->m_req_queue.end(); ++iter) {
    dout(20) << "req: " << hex << *iter << dec << dendl;
  }
}

RGWAsyncRadosProcessor::RGWAsyncRadosProcessor(CephContext *_cct, int num_threads)
  : cct(_cct), m_tp(cct, "RGWAsyncRadosProcessor::m_tp", "rados_async", num_threads),
    req_throttle(_cct, "rgw_async_rados_ops", num_threads * 2),
    req_wq(this, g_conf()->rgw_op_thread_timeout,
    g_conf()->rgw_op_thread_suicide_timeout, &m_tp) {
}

void RGWAsyncRadosProcessor::start() {
  m_tp.start();
}

void RGWAsyncRadosProcessor::stop() {
  going_down = true;
  m_tp.drain(&req_wq);
  m_tp.stop();
  for (auto iter = m_req_queue.begin(); iter != m_req_queue.end(); ++iter) {
    (*iter)->put();
  }
}

void RGWAsyncRadosProcessor::handle_request(RGWAsyncRadosRequest *req) {
  req->send_request();
  req->put();
}

void RGWAsyncRadosProcessor::queue(RGWAsyncRadosRequest *req) {
  req_throttle.get(1);
  req_wq.queue(req);
}

int RGWAsyncGetSystemObj::_send_request()
{
  map<string, bufferlist> *pattrs = want_attrs ? &attrs : nullptr;

  auto sysobj = obj_ctx.get_obj(obj);
  return sysobj.rop()
               .set_objv_tracker(&objv_tracker)
               .set_attrs(pattrs)
	       .set_raw_attrs(raw_attrs)
               .read(&bl, null_yield);
}

RGWAsyncGetSystemObj::RGWAsyncGetSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWSI_SysObj *_svc,
                       RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
                       bool want_attrs, bool raw_attrs)
  : RGWAsyncRadosRequest(caller, cn), obj_ctx(_svc),
    obj(_obj), want_attrs(want_attrs), raw_attrs(raw_attrs)
{
  if (_objv_tracker) {
    objv_tracker = *_objv_tracker;
  }
}

int RGWSimpleRadosReadAttrsCR::send_request()
{
  req = new RGWAsyncGetSystemObj(this, stack->create_completion_notifier(),
			         svc, objv_tracker, obj, true, raw_attrs);
  async_rados->queue(req);
  return 0;
}

int RGWSimpleRadosReadAttrsCR::request_complete()
{
  if (pattrs) {
    *pattrs = std::move(req->attrs);
  }
  if (objv_tracker) {
    *objv_tracker = req->objv_tracker;
  }
  return req->get_ret_status();
}

int RGWAsyncPutSystemObj::_send_request()
{
  auto obj_ctx = svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);
  return sysobj.wop()
               .set_objv_tracker(&objv_tracker)
               .set_exclusive(exclusive)
               .write_data(bl, null_yield);
}

RGWAsyncPutSystemObj::RGWAsyncPutSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn,
                     RGWSI_SysObj *_svc,
                     RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
                     bool _exclusive, bufferlist _bl)
  : RGWAsyncRadosRequest(caller, cn), svc(_svc),
    obj(_obj), exclusive(_exclusive), bl(std::move(_bl))
{
  if (_objv_tracker) {
    objv_tracker = *_objv_tracker;
  }
}

int RGWAsyncPutSystemObjAttrs::_send_request()
{
  auto obj_ctx = svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);
  return sysobj.wop()
               .set_objv_tracker(&objv_tracker)
               .set_exclusive(false)
               .set_attrs(attrs)
               .write_attrs(null_yield);
}

RGWAsyncPutSystemObjAttrs::RGWAsyncPutSystemObjAttrs(RGWCoroutine *caller, RGWAioCompletionNotifier *cn,
                     RGWSI_SysObj *_svc,
                     RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
                     map<string, bufferlist> _attrs)
  : RGWAsyncRadosRequest(caller, cn), svc(_svc),
    obj(_obj), attrs(std::move(_attrs))
{
  if (_objv_tracker) {
    objv_tracker = *_objv_tracker;
  }
}


RGWOmapAppend::RGWOmapAppend(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store, const rgw_raw_obj& _obj,
                             uint64_t _window_size)
                      : RGWConsumerCR<string>(_store->ctx()), async_rados(_async_rados),
                        store(_store), obj(_obj), going_down(false), num_pending_entries(0), window_size(_window_size), total_entries(0)
{
}

int RGWAsyncLockSystemObj::_send_request()
{
  rgw_rados_ref ref;
  int r = store->getRados()->get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to get ref for (" << obj << ") ret=" << r << dendl;
    return r;
  }

  rados::cls::lock::Lock l(lock_name);
  utime_t duration(duration_secs, 0);
  l.set_duration(duration);
  l.set_cookie(cookie);
  l.set_may_renew(true);

  return l.lock_exclusive(&ref.pool.ioctx(), ref.obj.oid);
}

RGWAsyncLockSystemObj::RGWAsyncLockSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RGWRadosStore *_store,
                      RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
                       const string& _name, const string& _cookie, uint32_t _duration_secs) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                                              obj(_obj),
                                                              lock_name(_name),
                                                              cookie(_cookie),
                                                              duration_secs(_duration_secs)
{
}

int RGWAsyncUnlockSystemObj::_send_request()
{
  rgw_rados_ref ref;
  int r = store->getRados()->get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to get ref for (" << obj << ") ret=" << r << dendl;
    return r;
  }

  rados::cls::lock::Lock l(lock_name);

  l.set_cookie(cookie);

  return l.unlock(&ref.pool.ioctx(), ref.obj.oid);
}

RGWAsyncUnlockSystemObj::RGWAsyncUnlockSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RGWRadosStore *_store,
                                                 RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
                                                 const string& _name, const string& _cookie) : RGWAsyncRadosRequest(caller, cn), store(_store),
  obj(_obj),
  lock_name(_name), cookie(_cookie)
{
}

RGWRadosSetOmapKeysCR::RGWRadosSetOmapKeysCR(rgw::sal::RGWRadosStore *_store,
                      const rgw_raw_obj& _obj,
                      map<string, bufferlist>& _entries) : RGWSimpleCoroutine(_store->ctx()),
                                                store(_store),
                                                entries(_entries),
                                                obj(_obj), cn(NULL)
{
  stringstream& s = set_description();
  s << "set omap keys dest=" << obj << " keys=[" << s.str() << "]";
  for (auto i = entries.begin(); i != entries.end(); ++i) {
    if (i != entries.begin()) {
      s << ", ";
    }
    s << i->first;
  }
  s << "]";
}

int RGWRadosSetOmapKeysCR::send_request()
{
  int r = store->getRados()->get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to get ref for (" << obj << ") ret=" << r << dendl;
    return r;
  }

  set_status() << "sending request";

  librados::ObjectWriteOperation op;
  op.omap_set(entries);

  cn = stack->create_completion_notifier();
  return ref.pool.ioctx().aio_operate(ref.obj.oid, cn->completion(), &op);
}

int RGWRadosSetOmapKeysCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}

RGWRadosGetOmapKeysCR::RGWRadosGetOmapKeysCR(rgw::sal::RGWRadosStore *_store,
                      const rgw_raw_obj& _obj,
                      const string& _marker,
                      int _max_entries,
                      ResultPtr _result)
  : RGWSimpleCoroutine(_store->ctx()), store(_store), obj(_obj),
    marker(_marker), max_entries(_max_entries),
    result(std::move(_result))
{
  ceph_assert(result); // must be allocated
  set_description() << "get omap keys dest=" << obj << " marker=" << marker;
}

int RGWRadosGetOmapKeysCR::send_request() {
  int r = store->getRados()->get_raw_obj_ref(obj, &result->ref);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to get ref for (" << obj << ") ret=" << r << dendl;
    return r;
  }

  set_status() << "send request";

  librados::ObjectReadOperation op;
  op.omap_get_keys2(marker, max_entries, &result->entries, &result->more, nullptr);

  cn = stack->create_completion_notifier(result);
  return result->ref.pool.ioctx().aio_operate(result->ref.obj.oid, cn->completion(), &op, NULL);
}

int RGWRadosGetOmapKeysCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}

RGWRadosGetOmapValsCR::RGWRadosGetOmapValsCR(rgw::sal::RGWRadosStore *_store,
                      const rgw_raw_obj& _obj,
                      const string& _marker,
                      int _max_entries,
                      ResultPtr _result)
  : RGWSimpleCoroutine(_store->ctx()), store(_store), obj(_obj),
    marker(_marker), max_entries(_max_entries),
    result(std::move(_result))
{
  ceph_assert(result); // must be allocated
  set_description() << "get omap keys dest=" << obj << " marker=" << marker;
}

int RGWRadosGetOmapValsCR::send_request() {
  int r = store->getRados()->get_raw_obj_ref(obj, &result->ref);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to get ref for (" << obj << ") ret=" << r << dendl;
    return r;
  }

  set_status() << "send request";

  librados::ObjectReadOperation op;
  op.omap_get_vals2(marker, max_entries, &result->entries, &result->more, nullptr);

  cn = stack->create_completion_notifier(result);
  return result->ref.pool.ioctx().aio_operate(result->ref.obj.oid, cn->completion(), &op, NULL);
}

int RGWRadosGetOmapValsCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}

RGWRadosRemoveOmapKeysCR::RGWRadosRemoveOmapKeysCR(rgw::sal::RGWRadosStore *_store,
                      const rgw_raw_obj& _obj,
                      const set<string>& _keys) : RGWSimpleCoroutine(_store->ctx()),
                                                store(_store),
                                                keys(_keys),
                                                obj(_obj), cn(NULL)
{
  set_description() << "remove omap keys dest=" << obj << " keys=" << keys;
}

int RGWRadosRemoveOmapKeysCR::send_request() {
  int r = store->getRados()->get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to get ref for (" << obj << ") ret=" << r << dendl;
    return r;
  }

  set_status() << "send request";

  librados::ObjectWriteOperation op;
  op.omap_rm_keys(keys);

  cn = stack->create_completion_notifier();
  return ref.pool.ioctx().aio_operate(ref.obj.oid, cn->completion(), &op);
}

int RGWRadosRemoveOmapKeysCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}

RGWRadosRemoveCR::RGWRadosRemoveCR(rgw::sal::RGWRadosStore *store, const rgw_raw_obj& obj,
                                   RGWObjVersionTracker* objv_tracker)
  : RGWSimpleCoroutine(store->ctx()),
    store(store), obj(obj), objv_tracker(objv_tracker)
{
  set_description() << "remove dest=" << obj;
}

int RGWRadosRemoveCR::send_request()
{
  auto rados = store->getRados()->get_rados_handle();
  int r = rados->ioctx_create(obj.pool.name.c_str(), ioctx);
  if (r < 0) {
    lderr(cct) << "ERROR: failed to open pool (" << obj.pool.name << ") ret=" << r << dendl;
    return r;
  }
  ioctx.locator_set_key(obj.loc);

  set_status() << "send request";

  librados::ObjectWriteOperation op;
  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(&op);
  }
  op.remove();

  cn = stack->create_completion_notifier();
  return ioctx.aio_operate(obj.oid, cn->completion(), &op);
}

int RGWRadosRemoveCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}

RGWSimpleRadosLockCR::RGWSimpleRadosLockCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store,
                      const rgw_raw_obj& _obj,
                      const string& _lock_name,
                      const string& _cookie,
                      uint32_t _duration) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
                                                store(_store),
                                                lock_name(_lock_name),
                                                cookie(_cookie),
                                                duration(_duration),
                                                obj(_obj),
                                                req(NULL)
{
  set_description() << "rados lock dest=" << obj << " lock=" << lock_name << " cookie=" << cookie << " duration=" << duration;
}

void RGWSimpleRadosLockCR::request_cleanup()
{
  if (req) {
    req->finish();
    req = NULL;
  }
}

int RGWSimpleRadosLockCR::send_request()
{
  set_status() << "sending request";
  req = new RGWAsyncLockSystemObj(this, stack->create_completion_notifier(),
                                 store, NULL, obj, lock_name, cookie, duration);
  async_rados->queue(req);
  return 0;
}

int RGWSimpleRadosLockCR::request_complete()
{
  set_status() << "request complete; ret=" << req->get_ret_status();
  return req->get_ret_status();
}

RGWSimpleRadosUnlockCR::RGWSimpleRadosUnlockCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store,
                      const rgw_raw_obj& _obj,
                      const string& _lock_name,
                      const string& _cookie) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
                                                store(_store),
                                                lock_name(_lock_name),
                                                cookie(_cookie),
                                                obj(_obj),
                                                req(NULL)
{
  set_description() << "rados unlock dest=" << obj << " lock=" << lock_name << " cookie=" << cookie;
}

void RGWSimpleRadosUnlockCR::request_cleanup()
{
  if (req) {
    req->finish();
    req = NULL;
  }
}

int RGWSimpleRadosUnlockCR::send_request()
{
  set_status() << "sending request";

  req = new RGWAsyncUnlockSystemObj(this, stack->create_completion_notifier(),
                                 store, NULL, obj, lock_name, cookie);
  async_rados->queue(req);
  return 0;
}

int RGWSimpleRadosUnlockCR::request_complete()
{
  set_status() << "request complete; ret=" << req->get_ret_status();
  return req->get_ret_status();
}

int RGWOmapAppend::operate() {
  reenter(this) {
    for (;;) {
      if (!has_product() && going_down) {
        set_status() << "going down";
        break;
      }
      set_status() << "waiting for product";
      yield wait_for_product();
      yield {
        string entry;
        while (consume(&entry)) {
          set_status() << "adding entry: " << entry;
          entries[entry] = bufferlist();
          if (entries.size() >= window_size) {
            break;
          }
        }
        if (entries.size() >= window_size || going_down) {
          set_status() << "flushing to omap";
          call(new RGWRadosSetOmapKeysCR(store, obj, entries));
          entries.clear();
        }
      }
      if (get_ret_status() < 0) {
        ldout(cct, 0) << "ERROR: failed to store entries in omap" << dendl;
        return set_state(RGWCoroutine_Error);
      }
    }
    /* done with coroutine */
    return set_state(RGWCoroutine_Done);
  }
  return 0;
}

void RGWOmapAppend::flush_pending() {
  receive(pending_entries);
  num_pending_entries = 0;
}

bool RGWOmapAppend::append(const string& s) {
  if (is_done()) {
    return false;
  }
  ++total_entries;
  pending_entries.push_back(s);
  if (++num_pending_entries >= (int)window_size) {
    flush_pending();
  }
  return true;
}

bool RGWOmapAppend::finish() {
  going_down = true;
  flush_pending();
  set_sleeping(false);
  return (!is_done());
}

int RGWAsyncGetBucketInstanceInfo::_send_request()
{
  int r;
  if (!bucket.bucket_id.empty()) {
    RGWSysObjectCtx obj_ctx = store->svc()->sysobj->init_obj_ctx();
    r = store->getRados()->get_bucket_instance_info(obj_ctx, bucket, bucket_info, nullptr, &attrs, null_yield);
  } else {
    r = store->ctl()->bucket->read_bucket_info(bucket, &bucket_info, null_yield,
                                               RGWBucketCtl::BucketInstance::GetParams().set_attrs(&attrs));
  }
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to get bucket instance info for "
        << bucket << dendl;
    return r;
  }

  return 0;
}

RGWRadosBILogTrimCR::RGWRadosBILogTrimCR(rgw::sal::RGWRadosStore *store,
                                         const RGWBucketInfo& bucket_info,
                                         int shard_id,
                                         const std::string& start_marker,
                                         const std::string& end_marker)
  : RGWSimpleCoroutine(store->ctx()), bs(store->getRados()),
    start_marker(BucketIndexShardsManager::get_shard_marker(start_marker)),
    end_marker(BucketIndexShardsManager::get_shard_marker(end_marker))
{
  bs.init(bucket_info, bucket_info.layout.current_index, shard_id);
}

int RGWRadosBILogTrimCR::send_request()
{
  bufferlist in;
  cls_rgw_bi_log_trim_op call;
  call.start_marker = std::move(start_marker);
  call.end_marker = std::move(end_marker);
  encode(call, in);

  librados::ObjectWriteOperation op;
  op.exec(RGW_CLASS, RGW_BI_LOG_TRIM, in);

  cn = stack->create_completion_notifier();
  return bs.bucket_obj.aio_operate(cn->completion(), &op);
}

int RGWRadosBILogTrimCR::request_complete()
{
  int r = cn->completion()->get_return_value();
  set_status() << "request complete; ret=" << r;
  return r;
}

int RGWAsyncFetchRemoteObj::_send_request()
{
  RGWObjectCtx obj_ctx(store);

  char buf[16];
  snprintf(buf, sizeof(buf), ".%lld", (long long)store->getRados()->instance_id());
  map<string, bufferlist> attrs;

  rgw::sal::RGWRadosBucket bucket(store, src_bucket);
  rgw::sal::RGWRadosObject src_obj(store, key, &bucket);
  rgw::sal::RGWRadosBucket dest_bucket(store, dest_bucket_info);
  rgw::sal::RGWRadosObject dest_obj(store, dest_key.value_or(key), &dest_bucket);

  std::optional<uint64_t> bytes_transferred;
  int r = store->getRados()->fetch_remote_obj(obj_ctx,
                       user_id.value_or(rgw_user()),
                       NULL, /* req_info */
                       source_zone,
                       &dest_obj,
                       &src_obj,
                       &dest_bucket, /* dest */
                       nullptr, /* source */
		       dest_placement_rule,
                       NULL, /* real_time* src_mtime, */
                       NULL, /* real_time* mtime, */
                       NULL, /* const real_time* mod_ptr, */
                       NULL, /* const real_time* unmod_ptr, */
                       false, /* high precision time */
                       NULL, /* const char *if_match, */
                       NULL, /* const char *if_nomatch, */
                       RGWRados::ATTRSMOD_NONE,
                       copy_if_newer,
                       attrs,
                       RGWObjCategory::Main,
                       versioned_epoch,
                       real_time(), /* delete_at */
                       NULL, /* string *ptag, */
                       NULL, /* string *petag, */
                       NULL, /* void (*progress_cb)(off_t, void *), */
                       NULL, /* void *progress_data*); */
                       dpp,
                       filter.get(),
                       &zones_trace,
                       &bytes_transferred);

  if (r < 0) {
    ldout(store->ctx(), 0) << "store->fetch_remote_obj() returned r=" << r << dendl;
    if (counters) {
      counters->inc(sync_counters::l_fetch_err, 1);
    }
  } else if (counters) {
    if (bytes_transferred) {
      counters->inc(sync_counters::l_fetch, *bytes_transferred);
    } else {
      counters->inc(sync_counters::l_fetch_not_modified);
    }
  }
  return r;
}

int RGWAsyncStatRemoteObj::_send_request()
{
  RGWObjectCtx obj_ctx(store);

  string user_id;
  char buf[16];
  snprintf(buf, sizeof(buf), ".%lld", (long long)store->getRados()->instance_id());

  rgw::sal::RGWRadosBucket bucket(store, src_bucket);
  rgw::sal::RGWRadosObject src_obj(store, key, &bucket);

  int r = store->getRados()->stat_remote_obj(obj_ctx,
                       rgw_user(user_id),
                       nullptr, /* req_info */
                       source_zone,
                       &src_obj,
                       nullptr, /* source */
                       pmtime, /* real_time* src_mtime, */
                       psize, /* uint64_t * */
                       nullptr, /* const real_time* mod_ptr, */
                       nullptr, /* const real_time* unmod_ptr, */
                       true, /* high precision time */
                       nullptr, /* const char *if_match, */
                       nullptr, /* const char *if_nomatch, */
                       pattrs,
                       pheaders,
                       nullptr,
                       nullptr, /* string *ptag, */
                       petag); /* string *petag, */

  if (r < 0) {
    ldout(store->ctx(), 0) << "store->fetch_remote_obj() returned r=" << r << dendl;
  }
  return r;
}


int RGWAsyncRemoveObj::_send_request()
{
  RGWObjectCtx obj_ctx(store);

  rgw_obj obj(bucket_info.bucket, key);

  ldout(store->ctx(), 0) << __func__ << "(): deleting obj=" << obj << dendl;

  obj_ctx.set_atomic(obj);

  RGWObjState *state;

  int ret = store->getRados()->get_obj_state(&obj_ctx, bucket_info, obj, &state, null_yield);
  if (ret < 0) {
    ldout(store->ctx(), 20) << __func__ << "(): get_obj_state() obj=" << obj << " returned ret=" << ret << dendl;
    return ret;
  }

  /* has there been any racing object write? */
  if (del_if_older && (state->mtime > timestamp)) {
    ldout(store->ctx(), 20) << __func__ << "(): skipping object removal obj=" << obj << " (obj mtime=" << state->mtime << ", request timestamp=" << timestamp << ")" << dendl;
    return 0;
  }

  RGWAccessControlPolicy policy;

  /* decode policy */
  map<string, bufferlist>::iterator iter = state->attrset.find(RGW_ATTR_ACL);
  if (iter != state->attrset.end()) {
    auto bliter = iter->second.cbegin();
    try {
      policy.decode(bliter);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
      return -EIO;
    }
  }

  RGWRados::Object del_target(store->getRados(), bucket_info, obj_ctx, obj);
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket_info.owner;
  del_op.params.obj_owner = policy.get_owner();
  if (del_if_older) {
    del_op.params.unmod_since = timestamp;
  }
  if (versioned) {
    del_op.params.versioning_status = BUCKET_VERSIONED;
  }
  del_op.params.olh_epoch = versioned_epoch;
  del_op.params.marker_version_id = marker_version_id;
  del_op.params.obj_owner.set_id(rgw_user(owner));
  del_op.params.obj_owner.set_name(owner_display_name);
  del_op.params.mtime = timestamp;
  del_op.params.high_precision_time = true;
  del_op.params.zones_trace = &zones_trace;

  ret = del_op.delete_obj(null_yield);
  if (ret < 0) {
    ldout(store->ctx(), 20) << __func__ << "(): delete_obj() obj=" << obj << " returned ret=" << ret << dendl;
  }
  return ret;
}

int RGWContinuousLeaseCR::operate()
{
  if (aborted) {
    caller->set_sleeping(false);
    return set_cr_done();
  }
  reenter(this) {
    while (!going_down) {
      yield call(new RGWSimpleRadosLockCR(async_rados, store, obj, lock_name, cookie, interval));

      caller->set_sleeping(false); /* will only be relevant when we return, that's why we can do it early */
      if (retcode < 0) {
        set_locked(false);
        ldout(store->ctx(), 20) << *this << ": couldn't lock " << obj << ":" << lock_name << ": retcode=" << retcode << dendl;
        return set_state(RGWCoroutine_Error, retcode);
      }
      set_locked(true);
      yield wait(utime_t(interval / 2, 0));
    }
    set_locked(false); /* moot at this point anyway */
    yield call(new RGWSimpleRadosUnlockCR(async_rados, store, obj, lock_name, cookie));
    return set_state(RGWCoroutine_Done);
  }
  return 0;
}

RGWRadosTimelogAddCR::RGWRadosTimelogAddCR(rgw::sal::RGWRadosStore *_store, const string& _oid,
                      const cls_log_entry& entry) : RGWSimpleCoroutine(_store->ctx()),
                                                store(_store),
                                                oid(_oid), cn(NULL)
{
  stringstream& s = set_description();
  s << "timelog add entry oid=" <<  oid << "entry={id=" << entry.id << ", section=" << entry.section << ", name=" << entry.name << "}";
  entries.push_back(entry);
}

int RGWRadosTimelogAddCR::send_request()
{
  set_status() << "sending request";

  cn = stack->create_completion_notifier();
  return store->svc()->cls->timelog.add(oid, entries, cn->completion(), true, null_yield);
}

int RGWRadosTimelogAddCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}

RGWRadosTimelogTrimCR::RGWRadosTimelogTrimCR(rgw::sal::RGWRadosStore *store,
                                             const std::string& oid,
                                             const real_time& start_time,
                                             const real_time& end_time,
                                             const std::string& from_marker,
                                             const std::string& to_marker)
  : RGWSimpleCoroutine(store->ctx()), store(store), oid(oid),
    start_time(start_time), end_time(end_time),
    from_marker(from_marker), to_marker(to_marker)
{
  set_description() << "timelog trim oid=" <<  oid
      << " start_time=" << start_time << " end_time=" << end_time
      << " from_marker=" << from_marker << " to_marker=" << to_marker;
}

int RGWRadosTimelogTrimCR::send_request()
{
  set_status() << "sending request";

  cn = stack->create_completion_notifier();
  return store->svc()->cls->timelog.trim(oid, start_time, end_time, from_marker,
                                      to_marker, cn->completion(),
                                      null_yield);
}

int RGWRadosTimelogTrimCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}


RGWSyncLogTrimCR::RGWSyncLogTrimCR(rgw::sal::RGWRadosStore *store, const std::string& oid,
                                   const std::string& to_marker,
                                   std::string *last_trim_marker)
  : RGWRadosTimelogTrimCR(store, oid, real_time{}, real_time{},
                          std::string{}, to_marker),
    cct(store->ctx()), last_trim_marker(last_trim_marker)
{
}

int RGWSyncLogTrimCR::request_complete()
{
  int r = RGWRadosTimelogTrimCR::request_complete();
  if (r != -ENODATA) {
    return r;
  }
  // nothing left to trim, update last_trim_marker
  if (*last_trim_marker < to_marker && to_marker != max_marker) {
    *last_trim_marker = to_marker;
  }
  return 0;
}


int RGWAsyncStatObj::_send_request()
{
  rgw_raw_obj raw_obj;
  store->getRados()->obj_to_raw(bucket_info.placement_rule, obj, &raw_obj);
  return store->getRados()->raw_obj_stat(raw_obj, psize, pmtime, pepoch,
                             nullptr, nullptr, objv_tracker, null_yield);
}

RGWStatObjCR::RGWStatObjCR(RGWAsyncRadosProcessor *async_rados, rgw::sal::RGWRadosStore *store,
                           const RGWBucketInfo& _bucket_info, const rgw_obj& obj, uint64_t *psize,
                           real_time* pmtime, uint64_t *pepoch,
                           RGWObjVersionTracker *objv_tracker)
  : RGWSimpleCoroutine(store->ctx()), store(store), async_rados(async_rados),
    bucket_info(_bucket_info), obj(obj), psize(psize), pmtime(pmtime), pepoch(pepoch),
    objv_tracker(objv_tracker)
{
}

void RGWStatObjCR::request_cleanup()
{
  if (req) {
    req->finish();
    req = NULL;
  }
}

int RGWStatObjCR::send_request()
{
  req = new RGWAsyncStatObj(this, stack->create_completion_notifier(),
                            store, bucket_info, obj, psize, pmtime, pepoch, objv_tracker);
  async_rados->queue(req);
  return 0;
}

int RGWStatObjCR::request_complete()
{
  return req->get_ret_status();
}

RGWRadosNotifyCR::RGWRadosNotifyCR(rgw::sal::RGWRadosStore *store, const rgw_raw_obj& obj,
                                   bufferlist& request, uint64_t timeout_ms,
                                   bufferlist *response)
  : RGWSimpleCoroutine(store->ctx()), store(store), obj(obj),
    request(request), timeout_ms(timeout_ms), response(response)
{
  set_description() << "notify dest=" << obj;
}

int RGWRadosNotifyCR::send_request()
{
  int r = store->getRados()->get_raw_obj_ref(obj, &ref);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to get ref for (" << obj << ") ret=" << r << dendl;
    return r;
  }

  set_status() << "sending request";

  cn = stack->create_completion_notifier();
  return ref.pool.ioctx().aio_notify(ref.obj.oid, cn->completion(), request,
                              timeout_ms, response);
}

int RGWRadosNotifyCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}
