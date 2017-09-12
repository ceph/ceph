#include "rgw_rados.h"
#include "rgw_coroutine.h"
#include "rgw_boost_asio_yield.h"
#include "rgw_cr_rados.h"

#include "cls/lock/cls_lock_client.h"

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
  : store(_store), m_tp(store->ctx(), "RGWAsyncRadosProcessor::m_tp", "rados_async", num_threads),
    req_throttle(store->ctx(), "rgw_async_rados_ops", num_threads * 2),
    req_wq(this, g_conf->rgw_op_thread_timeout,
    g_conf->rgw_op_thread_suicide_timeout, &m_tp) {
}

void RGWAsyncRadosProcessor::start() {
  m_tp.start();
}

void RGWAsyncRadosProcessor::stop() {
  going_down.set(1);
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
  return store->get_system_obj(*obj_ctx, read_state, objv_tracker, obj, *pbl, ofs, end, pattrs, NULL);
}

RGWAsyncGetSystemObj::RGWAsyncGetSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store, RGWObjectCtx *_obj_ctx,
                       RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                       bufferlist *_pbl, off_t _ofs, off_t _end) : RGWAsyncRadosRequest(caller, cn), store(_store), obj_ctx(_obj_ctx),
                                                                   objv_tracker(_objv_tracker), obj(_obj), pbl(_pbl), pattrs(NULL),
                                                                  ofs(_ofs), end(_end)
{
}

int RGWSimpleRadosReadAttrsCR::send_request()
{
  rgw_obj obj = rgw_obj(pool, oid);
  req = new RGWAsyncGetSystemObj(this, stack->create_completion_notifier(),
			         store, &obj_ctx, NULL,
				 obj,
				 &bl, 0, -1);
  if (pattrs) {
    req->set_read_attrs(pattrs);
  }
  async_rados->queue(req);
  return 0;
}

int RGWSimpleRadosReadAttrsCR::request_complete()
{
  return req->get_ret_status();
}

int RGWAsyncPutSystemObj::_send_request()
{
  return store->put_system_obj_data(NULL, obj, bl, -1, exclusive);
}

RGWAsyncPutSystemObj::RGWAsyncPutSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                     rgw_obj& _obj, bool _exclusive,
                     bufferlist& _bl) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                                       obj(_obj), exclusive(_exclusive),
                                                       bl(_bl)
{
}

int RGWAsyncPutSystemObjAttrs::_send_request()
{
  return store->system_obj_set_attrs(NULL, obj, *attrs, NULL, objv_tracker);
}

RGWAsyncPutSystemObjAttrs::RGWAsyncPutSystemObjAttrs(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                     RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                     map<string, bufferlist> *_attrs) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                                       objv_tracker(_objv_tracker), obj(_obj),
                                                       attrs(_attrs)
{
}


RGWOmapAppend::RGWOmapAppend(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, rgw_bucket& _pool, const string& _oid,
                             uint64_t _window_size)
                      : RGWConsumerCR<string>(_store->ctx()), async_rados(_async_rados),
                        store(_store), pool(_pool), oid(_oid), going_down(false), num_pending_entries(0), window_size(_window_size), total_entries(0)
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
  l.set_renew(true);

  return l.lock_exclusive(&ioctx, obj.get_object());
}

RGWAsyncLockSystemObj::RGWAsyncLockSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                      RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                       const string& _name, const string& _cookie, uint32_t _duration_secs) : RGWAsyncRadosRequest(caller, cn), store(_store),
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

RGWAsyncUnlockSystemObj::RGWAsyncUnlockSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                                                 RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                                                 const string& _name, const string& _cookie) : RGWAsyncRadosRequest(caller, cn), store(_store),
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
  stringstream& s = set_description();
  s << "set omap keys dest=" << pool.name << "/" << oid << " keys=[" << s.str() << "]";
  for (auto i = entries.begin(); i != entries.end(); ++i) {
    if (i != entries.begin()) {
      s << ", ";
    }
    s << i->first;
  }
  s << "]";
}

RGWRadosSetOmapKeysCR::~RGWRadosSetOmapKeysCR()
{
  if (cn) {
    cn->put();
  }
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

  set_status() << "sending request";

  librados::ObjectWriteOperation op;
  op.omap_set(entries);

  cn = stack->create_completion_notifier();
  cn->get();
  return ioctx.aio_operate(oid, cn->completion(), &op);
}

int RGWRadosSetOmapKeysCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}

RGWRadosGetOmapKeysCR::RGWRadosGetOmapKeysCR(RGWRados *_store,
                      const rgw_bucket& _pool, const string& _oid,
                      const string& _marker,
                      map<string, bufferlist> *_entries, int _max_entries) : RGWSimpleCoroutine(_store->ctx()),
                                                store(_store),
                                                marker(_marker),
                                                entries(_entries), max_entries(_max_entries), rval(0),
                                                pool(_pool), oid(_oid), cn(NULL)
{
  set_description() << "set omap keys dest=" << pool.name << "/" << oid << " marker=" << marker;
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

  set_status() << "send request";

  librados::ObjectReadOperation op;
  op.omap_get_vals(marker, max_entries, entries, &rval);

  cn = stack->create_completion_notifier();
  return ioctx.aio_operate(oid, cn->completion(), &op, NULL);
}

RGWRadosRemoveOmapKeysCR::RGWRadosRemoveOmapKeysCR(RGWRados *_store,
                      const rgw_bucket& _pool, const string& _oid,
                      const set<string>& _keys) : RGWSimpleCoroutine(_store->ctx()),
                                                store(_store),
                                                keys(_keys),
                                                pool(_pool), oid(_oid), cn(NULL)
{
  set_description() << "remove omap keys dest=" << pool.name << "/" << oid << " keys=" << keys;
}

RGWRadosRemoveOmapKeysCR::~RGWRadosRemoveOmapKeysCR()
{
}

int RGWRadosRemoveOmapKeysCR::send_request() {
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(pool.name.c_str(), ioctx); /* system object only! */
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open pool (" << pool.name << ") ret=" << r << dendl;
    return r;
  }

  set_status() << "send request";

  librados::ObjectWriteOperation op;
  op.omap_rm_keys(keys);

  cn = stack->create_completion_notifier();
  return ioctx.aio_operate(oid, cn->completion(), &op);
}

int RGWRadosRemoveOmapKeysCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}

RGWSimpleRadosLockCR::RGWSimpleRadosLockCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                      const rgw_bucket& _pool, const string& _oid, const string& _lock_name,
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
  stringstream s;
  s << "rados lock dest=" << pool << "/" << oid << " lock=" << lock_name << " cookie=" << cookie << " duration=" << duration;
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
  rgw_obj obj = rgw_obj(pool, oid);
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

RGWSimpleRadosUnlockCR::RGWSimpleRadosUnlockCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                      const rgw_bucket& _pool, const string& _oid, const string& _lock_name,
                      const string& _cookie) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
                                                store(_store),
                                                lock_name(_lock_name),
                                                cookie(_cookie),
                                                pool(_pool), oid(_oid),
                                                req(NULL)
{
  set_description() << "rados unlock dest=" << pool << "/" << oid << " lock=" << lock_name << " cookie=" << cookie;
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

  rgw_obj obj = rgw_obj(pool, oid);
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
          call(new RGWRadosSetOmapKeysCR(store, pool, oid, entries));
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
  RGWObjectCtx obj_ctx(store);
  int r = store->get_bucket_instance_info(obj_ctx, bucket, *bucket_info, NULL, NULL);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to get bucket instance info for "
        << bucket << dendl;
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
                       false, /* don't record op state in ops log */
                       NULL, /* req_info */
                       source_zone,
                       dest_obj,
                       src_obj,
                       bucket_info, /* dest */
                       bucket_info, /* source */
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
                       RGW_OBJ_CATEGORY_MAIN,
                       versioned_epoch,
                       real_time(), /* delete_at */
                       &key.instance, /* string *version_id, */
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


int RGWAsyncRemoveObj::_send_request()
{
  RGWObjectCtx obj_ctx(store);

  rgw_obj obj(bucket_info.bucket, key.name);
  obj.set_instance(key.instance);

  ldout(store->ctx(), 0) << __func__ << "(): deleting obj=" << obj << dendl;

  obj_ctx.set_atomic(obj);

  RGWObjState *state;

  int ret = store->get_obj_state(&obj_ctx, obj, &state);
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
    bufferlist::iterator bliter = iter->second.begin();
    try {
      policy.decode(bliter);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
      return -EIO;
    }
  }

  RGWRados::Object del_target(store, bucket_info, obj_ctx, obj);
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
  del_op.params.obj_owner.set_id(owner);
  del_op.params.obj_owner.set_name(owner_display_name);
  del_op.params.mtime = timestamp;
  del_op.params.high_precision_time = true;

  ret = del_op.delete_obj();
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
    while (!going_down.read()) {
      yield call(new RGWSimpleRadosLockCR(async_rados, store, pool, oid, lock_name, cookie, interval));

      caller->set_sleeping(false); /* will only be relevant when we return, that's why we can do it early */
      if (retcode < 0) {
        set_locked(false);
        ldout(store->ctx(), 20) << *this << ": couldn't lock " << pool.name << ":" << oid << ":" << lock_name << ": retcode=" << retcode << dendl;
        return set_state(RGWCoroutine_Error, retcode);
      }
      set_locked(true);
      yield wait(utime_t(interval / 2, 0));
    }
    set_locked(false); /* moot at this point anyway */
    yield call(new RGWSimpleRadosUnlockCR(async_rados, store, pool, oid, lock_name, cookie));
    return set_state(RGWCoroutine_Done);
  }
  return 0;
}

RGWRadosTimelogAddCR::RGWRadosTimelogAddCR(RGWRados *_store, const string& _oid,
                      const cls_log_entry& entry) : RGWSimpleCoroutine(_store->ctx()),
                                                store(_store),
                                                oid(_oid), cn(NULL)
{
  stringstream& s = set_description();
  s << "timelog add entry oid=" <<  oid << "entry={id=" << entry.id << ", section=" << entry.section << ", name=" << entry.name << "}";
  entries.push_back(entry);
}

RGWRadosTimelogAddCR::~RGWRadosTimelogAddCR()
{
  if (cn) {
    cn->put();
  }
}

int RGWRadosTimelogAddCR::send_request()
{
  set_status() << "sending request";

  cn = stack->create_completion_notifier();
  cn->get();
  return store->time_log_add(oid, entries, cn->completion(), true);
}

int RGWRadosTimelogAddCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  return r;
}

int RGWAsyncStatObj::_send_request()
{
  return store->raw_obj_stat(obj, psize, pmtime, pepoch,
                             nullptr, nullptr, objv_tracker);
}

RGWStatObjCR::RGWStatObjCR(RGWAsyncRadosProcessor *async_rados, RGWRados *store,
                           const rgw_obj& obj, uint64_t *psize,
                           real_time* pmtime, uint64_t *pepoch,
                           RGWObjVersionTracker *objv_tracker)
  : RGWSimpleCoroutine(store->ctx()), store(store), async_rados(async_rados),
    obj(obj), psize(psize), pmtime(pmtime), pepoch(pepoch),
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
                            store, obj, psize, pmtime, pepoch, objv_tracker);
  async_rados->queue(req);
  return 0;
}

int RGWStatObjCR::request_complete()
{
  return req->get_ret_status();
}
