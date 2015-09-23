#include "rgw_rados.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"

#include "cls/lock/cls_lock_client.h"

#include <boost/asio/coroutine.hpp>
#include <boost/asio/yield.hpp>


#define dout_subsys ceph_subsys_rgw

bool RGWAsyncRadosProcessor::RGWWQ::_enqueue(RGWAsyncRadosRequest *req) {
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

void RGWAsyncRadosProcessor::RGWWQ::_process(RGWAsyncRadosRequest *req) {
  processor->handle_request(req);
  processor->req_throttle.put(1);
}

void RGWAsyncRadosProcessor::RGWWQ::_dump_queue() {
  if (!g_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
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

RGWAsyncRadosProcessor::RGWAsyncRadosProcessor(RGWRados *_store, int num_threads)
  : store(_store), m_tp(store->ctx(), "RGWAsyncRadosProcessor::m_tp", num_threads),
  req_throttle(store->ctx(), "rgw_async_rados_ops", num_threads * 2),
  req_wq(this, g_conf->rgw_op_thread_timeout,
         g_conf->rgw_op_thread_suicide_timeout, &m_tp) {
}

void RGWAsyncRadosProcessor::start() {
  m_tp.start();
}

void RGWAsyncRadosProcessor::stop() {
  m_tp.drain(&req_wq);
  m_tp.stop();
}

void RGWAsyncRadosProcessor::handle_request(RGWAsyncRadosRequest *req) {
  req->send_request();
}

void RGWAsyncRadosProcessor::queue(RGWAsyncRadosRequest *req) {
  req_throttle.get(1);
  req_wq.queue(req);
}

int RGWAsyncGetSystemObj::_send_request()
{
  return store->get_system_obj(*obj_ctx, read_state, objv_tracker, obj, *pbl, ofs, end, NULL, NULL);
}

RGWAsyncGetSystemObj::RGWAsyncGetSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store, RGWObjectCtx *_obj_ctx,
                       RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                       bufferlist *_pbl, off_t _ofs, off_t _end) : RGWAsyncRadosRequest(cn), store(_store), obj_ctx(_obj_ctx),
                                                                   objv_tracker(_objv_tracker), obj(_obj), pbl(_pbl),
                                                                  ofs(_ofs), end(_end)
{
}


int RGWAsyncPutSystemObj::_send_request()
{
  return store->put_system_obj(NULL, obj, bl.c_str(), bl.length(), exclusive,
                               NULL, attrs, objv_tracker, mtime);
}

RGWAsyncPutSystemObj::RGWAsyncPutSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store,
                     RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj, bool _exclusive,
                     bufferlist& _bl, time_t _mtime) : RGWAsyncRadosRequest(cn), store(_store),
                                                       objv_tracker(_objv_tracker), obj(_obj), exclusive(_exclusive),
                                                       bl(_bl), mtime(_mtime)
{
}


RGWOmapAppend::RGWOmapAppend(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, rgw_bucket& _pool, const string& _oid)
                      : RGWConsumerCR<string>(_store->ctx()), async_rados(_async_rados),
                        store(_store), pool(_pool), oid(_oid), going_down(false), num_pending_entries(0)
{
}

int RGWAsyncLockSystemObj::_send_request()
{
  librados::IoCtx ioctx;
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(obj.bucket.name.c_str(), ioctx); /* system object only! */
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open pool (" << obj.bucket.name << ") ret=" << r << dendl;
    return r;
  }

  rados::cls::lock::Lock l(lock_name);
  utime_t duration(duration_secs, 0);
  l.set_duration(duration);
  l.set_cookie(cookie);

  return l.lock_exclusive(&ioctx, obj.get_object());
}

RGWAsyncLockSystemObj::RGWAsyncLockSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store,
                      RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                       const string& _name, const string& _cookie, uint32_t _duration_secs) : RGWAsyncRadosRequest(cn), store(_store),
                                                              obj(_obj),
                                                              lock_name(_name),
                                                              cookie(_cookie),
                                                              duration_secs(_duration_secs)
{
}

int RGWAsyncUnlockSystemObj::_send_request()
{
  librados::IoCtx ioctx;
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(obj.bucket.name.c_str(), ioctx); /* system object only! */
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open pool (" << obj.bucket.name << ") ret=" << r << dendl;
    return r;
  }

  rados::cls::lock::Lock l(lock_name);

  l.set_cookie(cookie);

  return l.unlock(&ioctx, obj.get_object());
}

RGWAsyncUnlockSystemObj::RGWAsyncUnlockSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store,
                                                 RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                                                 const string& _name, const string& _cookie) : RGWAsyncRadosRequest(cn), store(_store),
  obj(_obj),
  lock_name(_name), cookie(_cookie)
{
}


RGWRadosSetOmapKeysCR::RGWRadosSetOmapKeysCR(RGWRados *_store,
                      rgw_bucket& _pool, const string& _oid,
                      map<string, bufferlist>& _entries) : RGWSimpleCoroutine(_store->ctx()),
                                                store(_store),
                                                entries(_entries),
                                                pool(_pool), oid(_oid), cn(NULL)
{
}

RGWRadosSetOmapKeysCR::~RGWRadosSetOmapKeysCR()
{
  cn->put();
}

int RGWRadosSetOmapKeysCR::send_request()
{
  librados::IoCtx ioctx;
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(pool.name.c_str(), ioctx); /* system object only! */
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open pool (" << pool.name << ") ret=" << r << dendl;
    return r;
  }

  librados::ObjectWriteOperation op;
  op.omap_set(entries);

  cn = stack->create_completion_notifier();
  cn->get();
  return ioctx.aio_operate(oid, cn->completion(), &op);
}

int RGWRadosSetOmapKeysCR::request_complete()
{
  return cn->completion()->get_return_value();
}

RGWRadosGetOmapKeysCR::RGWRadosGetOmapKeysCR(RGWRados *_store,
                      rgw_bucket& _pool, const string& _oid,
                      const string& _marker,
                      map<string, bufferlist> *_entries, int _max_entries) : RGWSimpleCoroutine(_store->ctx()),
                                                store(_store),
                                                marker(_marker),
                                                entries(_entries), max_entries(_max_entries), rval(0),
                                                pool(_pool), oid(_oid), cn(NULL)
{
}

RGWRadosGetOmapKeysCR::~RGWRadosGetOmapKeysCR()
{
}

int RGWRadosGetOmapKeysCR::send_request() {
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(pool.name.c_str(), ioctx); /* system object only! */
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open pool (" << pool.name << ") ret=" << r << dendl;
    return r;
  }

  librados::ObjectReadOperation op;
  op.omap_get_vals(marker, max_entries, entries, &rval);

  cn = stack->create_completion_notifier();
  return ioctx.aio_operate(oid, cn->completion(), &op, NULL);
}

RGWSimpleRadosLockCR::RGWSimpleRadosLockCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                      rgw_bucket& _pool, const string& _oid, const string& _lock_name,
                      const string& _cookie,
                      uint32_t _duration) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
                                                store(_store),
                                                lock_name(_lock_name),
                                                cookie(_cookie),
                                                duration(_duration),
                                                pool(_pool), oid(_oid),
                                                req(NULL)
{
}

RGWSimpleRadosLockCR::~RGWSimpleRadosLockCR()
{
  delete req;
}

int RGWSimpleRadosLockCR::send_request()
{
  rgw_obj obj = rgw_obj(pool, oid);
  req = new RGWAsyncLockSystemObj(stack->create_completion_notifier(),
                                 store, NULL, obj, lock_name, cookie, duration);
  async_rados->queue(req);
  return 0;
}

int RGWSimpleRadosLockCR::request_complete()
{
  return req->get_ret_status();
}

RGWSimpleRadosUnlockCR::RGWSimpleRadosUnlockCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                      rgw_bucket& _pool, const string& _oid, const string& _lock_name,
                      const string& _cookie) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
                                                store(_store),
                                                lock_name(_lock_name),
                                                cookie(_cookie),
                                                pool(_pool), oid(_oid),
                                                req(NULL)
{
}

RGWSimpleRadosUnlockCR::~RGWSimpleRadosUnlockCR()
{
  delete req;
}

int RGWSimpleRadosUnlockCR::send_request()
{
  rgw_obj obj = rgw_obj(pool, oid);
  req = new RGWAsyncUnlockSystemObj(stack->create_completion_notifier(),
                                 store, NULL, obj, lock_name, cookie);
  async_rados->queue(req);
  return 0;
}

int RGWSimpleRadosUnlockCR::request_complete()
{
  return req->get_ret_status();
}


#define OMAP_APPEND_MAX_ENTRIES 100
int RGWOmapAppend::operate() {
  reenter(this) {
    for (;;) {
      if (!has_product() && going_down) {
        break;
      }
      yield wait_for_product();
      yield {
        string entry;
        while (consume(&entry)) {
          entries[entry] = bufferlist();
          if (entries.size() >= OMAP_APPEND_MAX_ENTRIES) {
            break;
          }
        }
        if (entries.size() >= OMAP_APPEND_MAX_ENTRIES || going_down) {
          call(new RGWRadosSetOmapKeysCR(store, pool, oid, entries));
          entries.clear();
        }
      }
      if (get_ret_status() < 0) {
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

void RGWOmapAppend::append(const string& s) {
  pending_entries.push_back(s);
  if (++num_pending_entries >= OMAP_APPEND_MAX_ENTRIES) {
    flush_pending();
  }
}

void RGWOmapAppend::finish() {
  going_down = true;
  flush_pending();
  set_sleeping(false);
}

int RGWAsyncGetBucketInstanceInfo::_send_request()
{
  string id = bucket_name + ":" + bucket_id;
  RGWObjectCtx obj_ctx(store);

  int r = store->get_bucket_instance_info(obj_ctx, id, *bucket_info, NULL, NULL);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to get bucket instance info for bucket id=" << id << dendl;
    return r;
  }

  return 0;
}

int RGWAsyncFetchRemoteObj::_send_request()
{
  RGWObjectCtx obj_ctx(store);

  string user_id;
  char buf[16];
  snprintf(buf, sizeof(buf), ".%lld", (long long)store->instance_id());
  string client_id = store->zone_id() + buf;
  string op_id = store->unique_id(store->get_new_req_id());
  map<string, bufferlist> attrs;

  rgw_obj src_obj(bucket_info.bucket, key.name);
  src_obj.set_instance(key.instance);

  rgw_obj dest_obj(src_obj);

  int r = store->fetch_remote_obj(obj_ctx,
                       user_id,
                       client_id,
                       op_id,
                       NULL, /* req_info */
                       source_zone,
                       dest_obj,
                       src_obj,
                       bucket_info, /* dest */
                       bucket_info, /* source */
                       NULL, /* time_t *src_mtime, */
                       NULL, /* time_t *mtime, */
                       NULL, /* const time_t *mod_ptr, */
                       NULL, /* const time_t *unmod_ptr, */
                       NULL, /* const char *if_match, */
                       NULL, /* const char *if_nomatch, */
                       RGWRados::ATTRSMOD_NONE,
                       copy_if_newer,
                       attrs,
                       RGW_OBJ_CATEGORY_MAIN,
                       versioned_epoch,
                       0, /* delete_at */
                       NULL, /* string *version_id, */
                       NULL, /* string *ptag, */
                       NULL, /* string *petag, */
                       NULL, /* struct rgw_err *err, */
                       NULL, /* void (*progress_cb)(off_t, void *), */
                       NULL); /* void *progress_data*); */

  if (r < 0) {
    ldout(store->ctx(), 0) << "store->fetch_remote_obj() returned r=" << r << dendl;
  }
  return r;
}

