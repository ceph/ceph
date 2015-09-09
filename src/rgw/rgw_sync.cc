#include "common/ceph_json.h"
#include "common/RWLock.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_sync.h"
#include "rgw_metadata.h"
#include "rgw_rest_conn.h"
#include "rgw_tools.h"

#include "cls/lock/cls_lock_client.h"

#include <boost/asio/coroutine.hpp>
#include <boost/asio/yield.hpp>


#define dout_subsys ceph_subsys_rgw

static string mdlog_sync_status_oid = "mdlog.sync-status";
static string mdlog_sync_status_shard_prefix = "mdlog.sync-status.shard";
static string mdlog_sync_full_sync_index_prefix = "meta.full-sync.index";

void rgw_mdlog_info::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("num_objects", num_shards, obj);
}

struct rgw_mdlog_entry {
  string id;
  string section;
  string name;
  utime_t timestamp;
  RGWMetadataLogData log_data;

  void decode_json(JSONObj *obj);
};

struct rgw_mdlog_shard_data {
  string marker;
  bool truncated;
  vector<rgw_mdlog_entry> entries;

  void decode_json(JSONObj *obj);
};


void rgw_mdlog_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("section", section, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("timestamp", timestamp, obj);
  JSONDecoder::decode_json("data", log_data, obj);
}

void rgw_mdlog_shard_data::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("truncated", truncated, obj);
  JSONDecoder::decode_json("entries", entries, obj);
};

class RGWAsyncRadosRequest {
  RGWAioCompletionNotifier *notifier;

  void *user_info;
  int retcode;

protected:
  virtual int _send_request() = 0;
public:
  RGWAsyncRadosRequest(RGWAioCompletionNotifier *_cn) : notifier(_cn) {}
  virtual ~RGWAsyncRadosRequest() {}

  void send_request() {
    retcode = _send_request();
    notifier->cb();
  }

  int get_ret_status() { return retcode; }
};


class RGWAsyncGetSystemObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  RGWObjectCtx *obj_ctx;
  RGWRados::SystemObject::Read::GetObjState read_state;
  RGWObjVersionTracker *objv_tracker;
  rgw_obj obj;
  bufferlist *pbl;
  off_t ofs;
  off_t end;
protected:
  int _send_request() {
    return store->get_system_obj(*obj_ctx, read_state, objv_tracker, obj, *pbl, ofs, end, NULL);
  }
public:
  RGWAsyncGetSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store, RGWObjectCtx *_obj_ctx,
                       RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                       bufferlist *_pbl, off_t _ofs, off_t _end) : RGWAsyncRadosRequest(cn), store(_store), obj_ctx(_obj_ctx),
                                                                   objv_tracker(_objv_tracker), obj(_obj), pbl(_pbl),
								  ofs(_ofs), end(_end) {
  }
};

class RGWAsyncPutSystemObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  RGWObjVersionTracker *objv_tracker;
  rgw_obj obj;
  bool exclusive;
  bufferlist bl;
  map<string, bufferlist> attrs;
  time_t mtime;

protected:
  int _send_request() {
    return store->put_system_obj(NULL, obj, bl.c_str(), bl.length(), exclusive,
                                 NULL, attrs, objv_tracker, mtime);
  }
public:
  RGWAsyncPutSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store,
                       RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj, bool _exclusive,
                       bufferlist& _bl, time_t _mtime = 0) : RGWAsyncRadosRequest(cn), store(_store),
                                                                   objv_tracker(_objv_tracker), obj(_obj), exclusive(_exclusive),
								   bl(_bl), mtime(_mtime) {}
};

class RGWAsyncLockSystemObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  rgw_obj obj;
  string lock_name;
  string cookie;
  uint32_t duration_secs;

protected:
  int _send_request() {
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
public:
  RGWAsyncLockSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store,
                        RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
		        const string& _name, const string& _cookie, uint32_t _duration_secs) : RGWAsyncRadosRequest(cn), store(_store),
                                                                obj(_obj),
                                                                lock_name(_name),
                                                                cookie(_cookie),
							        duration_secs(_duration_secs) {}
};

class RGWAsyncUnlockSystemObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  rgw_obj obj;
  string lock_name;
  string cookie;

protected:
  int _send_request() {
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
public:
  RGWAsyncUnlockSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store,
                        RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
		        const string& _name, const string& _cookie) : RGWAsyncRadosRequest(cn), store(_store),
                                               obj(_obj),
                                               lock_name(_name), cookie(_cookie) {}
};


class RGWAsyncRadosProcessor {
  deque<RGWAsyncRadosRequest *> m_req_queue;
protected:
  RGWRados *store;
  ThreadPool m_tp;
  Throttle req_throttle;

  struct RGWWQ : public ThreadPool::WorkQueue<RGWAsyncRadosRequest> {
    RGWAsyncRadosProcessor *processor;
    RGWWQ(RGWAsyncRadosProcessor *p, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<RGWAsyncRadosRequest>("RGWWQ", timeout, suicide_timeout, tp), processor(p) {}

    bool _enqueue(RGWAsyncRadosRequest *req) {
      processor->m_req_queue.push_back(req);
      dout(20) << "enqueued request req=" << hex << req << dec << dendl;
      _dump_queue();
      return true;
    }
    void _dequeue(RGWAsyncRadosRequest *req) {
      assert(0);
    }
    bool _empty() {
      return processor->m_req_queue.empty();
    }
    RGWAsyncRadosRequest *_dequeue() {
      if (processor->m_req_queue.empty())
	return NULL;
      RGWAsyncRadosRequest *req = processor->m_req_queue.front();
      processor->m_req_queue.pop_front();
      dout(20) << "dequeued request req=" << hex << req << dec << dendl;
      _dump_queue();
      return req;
    }
    using ThreadPool::WorkQueue<RGWAsyncRadosRequest>::_process;
    void _process(RGWAsyncRadosRequest *req) {
      processor->handle_request(req);
      processor->req_throttle.put(1);
    }
    void _dump_queue() {
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
    void _clear() {
      assert(processor->m_req_queue.empty());
    }
  } req_wq;

public:
  RGWAsyncRadosProcessor(RGWRados *_store, int num_threads)
    : store(_store), m_tp(store->ctx(), "RGWAsyncRadosProcessor::m_tp", num_threads),
      req_throttle(store->ctx(), "rgw_async_rados_ops", num_threads * 2),
      req_wq(this, g_conf->rgw_op_thread_timeout,
	     g_conf->rgw_op_thread_suicide_timeout, &m_tp) {}
  ~RGWAsyncRadosProcessor() {}
  void start() {
    m_tp.start();
  }
  void stop() {
    m_tp.drain(&req_wq);
    m_tp.stop();
  }
  void handle_request(RGWAsyncRadosRequest *req) {
    req->send_request();
  }

  void queue(RGWAsyncRadosRequest *req) {
    req_throttle.get(1);
    req_wq.queue(req);
  }
};

int RGWRemoteMetaLog::read_log_info(rgw_mdlog_info *log_info)
{
  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { NULL, NULL } };

  int ret = conn->get_json_resource("/admin/log", pairs, *log_info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog info" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote mdlog, num_shards=" << log_info->num_shards << dendl;

  return 0;
}

int RGWRemoteMetaLog::init()
{
  CephContext *cct = store->ctx();
  async_rados = new RGWAsyncRadosProcessor(store, cct->_conf->rgw_num_async_rados_threads);
  async_rados->start();

  conn = store->rest_master_conn;

  int ret = http_manager.set_threaded();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "failed in http_manager.set_threaded() ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

void RGWRemoteMetaLog::finish()
{
  async_rados->stop();
  delete async_rados;
}

int RGWRemoteMetaLog::list_shards(int num_shards)
{
  for (int i = 0; i < (int)num_shards; i++) {
    int ret = list_shard(i);
    if (ret < 0) {
      ldout(store->ctx(), 10) << "failed to list shard: ret=" << ret << dendl;
    }
  }

  return 0;
}

int RGWRemoteMetaLog::list_shard(int shard_id)
{
  conn = store->rest_master_conn;

  char buf[32];
  snprintf(buf, sizeof(buf), "%d", shard_id);

  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { "id", buf },
                                  { NULL, NULL } };

  rgw_mdlog_shard_data data;
  int ret = conn->get_json_resource("/admin/log", pairs, data);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog data" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote mdlog, shard_id=" << shard_id << " num of shard entries: " << data.entries.size() << dendl;

  vector<rgw_mdlog_entry>::iterator iter;
  for (iter = data.entries.begin(); iter != data.entries.end(); ++iter) {
    rgw_mdlog_entry& entry = *iter;
    ldout(store->ctx(), 20) << "entry: name=" << entry.name << dendl;
  }

  return 0;
}

int RGWRemoteMetaLog::get_shard_info(int shard_id)
{
  conn = store->rest_master_conn;

  char buf[32];
  snprintf(buf, sizeof(buf), "%d", shard_id);

  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { "id", buf },
                                  { "info", NULL },
                                  { NULL, NULL } };

  RGWMetadataLogInfo info;
  int ret = conn->get_json_resource("/admin/log", pairs, info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog info" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote mdlog, shard_id=" << shard_id << " marker=" << info.marker << dendl;

  return 0;
}

#define CLONE_MAX_ENTRIES 100

int RGWMetaSyncStatusManager::init()
{
  if (store->is_meta_master()) {
    return 0;
  }

  if (!store->rest_master_conn) {
    lderr(store->ctx()) << "no REST connection to master zone" << dendl;
    return -EIO;
  }

  const char *log_pool = store->get_zone_params().log_pool.name.c_str();
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(log_pool, ioctx);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open log pool (" << store->get_zone_params().log_pool.name << " ret=" << r << dendl;
    return r;
  }

  global_status_obj = rgw_obj(store->get_zone_params().log_pool, mdlog_sync_status_oid);

  r = master_log.init();
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to init remote log, r=" << r << dendl;
    return r;
  }

  rgw_mdlog_info mdlog_info;
  r = master_log.read_log_info(&mdlog_info);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: master.read_log_info() returned r=" << r << dendl;
    return r;
  }

  num_shards = mdlog_info.num_shards;

  for (int i = 0; i < num_shards; i++) {
    shard_objs[i] = rgw_obj(store->get_zone_params().log_pool, shard_obj_name(i));
  }

  RWLock::WLocker wl(ts_to_shard_lock);
  for (int i = 0; i < num_shards; i++) {
    clone_markers.push_back(string());
    utime_shard ut;
    ut.shard_id = i;
    ts_to_shard[ut] = i;
  }

  return 0;
}

string RGWMetaSyncStatusManager::shard_obj_name(int shard_id)
{
  char buf[mdlog_sync_status_shard_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", mdlog_sync_status_shard_prefix.c_str(), shard_id);

  return string(buf);
}

template <class T>
class RGWSimpleRadosReadCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWObjectCtx& obj_ctx;
  bufferlist bl;

  rgw_bucket pool;
  string oid;

  T *result;

  RGWAsyncGetSystemObj *req;

public:
  RGWSimpleRadosReadCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      RGWObjectCtx& _obj_ctx,
		      rgw_bucket& _pool, const string& _oid,
		      T *_result) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados), store(_store),
                                                obj_ctx(_obj_ctx),
						pool(_pool), oid(_oid),
						result(_result),
                                                req(NULL) { }
                                                         
  ~RGWSimpleRadosReadCR() {
    delete req;
  }

  int send_request();
  int request_complete();

  virtual int handle_data(T& data) {
    return 0;
  }
};

template <class T>
int RGWSimpleRadosReadCR<T>::send_request()
{
  rgw_obj obj = rgw_obj(pool, oid);
  req = new RGWAsyncGetSystemObj(stack->create_completion_notifier(),
			         store, &obj_ctx, NULL,
				 obj,
				 &bl, 0, -1);
  async_rados->queue(req);
  return 0;
}

template <class T>
int RGWSimpleRadosReadCR<T>::request_complete()
{
  int ret = req->get_ret_status();
  retcode = ret;
  if (ret != -ENOENT) {
    if (ret < 0) {
      return ret;
    }
    bufferlist::iterator iter = bl.begin();
    try {
      ::decode(*result, iter);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0) << "ERROR: failed to decode global mdlog status" << dendl;
    }
  } else {
    *result = T();
  }

  return handle_data(*result);
}

template <class T>
class RGWSimpleRadosWriteCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  bufferlist bl;

  rgw_bucket pool;
  string oid;

  RGWAsyncPutSystemObj *req;

public:
  RGWSimpleRadosWriteCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      rgw_bucket& _pool, const string& _oid,
		      const T& _data) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
						store(_store),
						pool(_pool), oid(_oid),
                                                req(NULL) {
    ::encode(_data, bl);
  }

  ~RGWSimpleRadosWriteCR() {
    delete req;
  }

  int send_request() {
    rgw_obj obj = rgw_obj(pool, oid);
    req = new RGWAsyncPutSystemObj(stack->create_completion_notifier(),
			           store, NULL, obj, false, bl);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWRadosSetOmapKeysCR : public RGWSimpleCoroutine {
  RGWRados *store;
  map<string, bufferlist> entries;

  rgw_bucket pool;
  string oid;

  RGWAioCompletionNotifier *cn;

public:
  RGWRadosSetOmapKeysCR(RGWRados *_store,
		      rgw_bucket& _pool, const string& _oid,
		      map<string, bufferlist>& _entries) : RGWSimpleCoroutine(_store->ctx()),
						store(_store),
						entries(_entries),
						pool(_pool), oid(_oid), cn(NULL) {
  }

  ~RGWRadosSetOmapKeysCR() {
    cn->put();
  }

  int send_request() {
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

  int request_complete() {
    return cn->completion()->get_return_value();

  }
};

class RGWRadosGetOmapKeysCR : public RGWSimpleCoroutine {
  RGWRados *store;

  string marker;
  map<string, bufferlist> *entries;
  int max_entries;

  int rval;
  librados::IoCtx ioctx;

  rgw_bucket pool;
  string oid;

  RGWAioCompletionNotifier *cn;

public:
  RGWRadosGetOmapKeysCR(RGWRados *_store,
		      rgw_bucket& _pool, const string& _oid,
		      const string& _marker,
		      map<string, bufferlist> *_entries, int _max_entries) : RGWSimpleCoroutine(_store->ctx()),
						store(_store),
						marker(_marker),
						entries(_entries), max_entries(_max_entries), rval(0),
						pool(_pool), oid(_oid), cn(NULL) {
  }

  ~RGWRadosGetOmapKeysCR() {
  }

  int send_request() {
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

  int request_complete() {
    return rval;
  }
};

class RGWSimpleRadosLockCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  string lock_name;
  string cookie;
  uint32_t duration;

  rgw_bucket pool;
  string oid;

  RGWAsyncLockSystemObj *req;

public:
  RGWSimpleRadosLockCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      rgw_bucket& _pool, const string& _oid, const string& _lock_name,
		      const string& _cookie,
		      uint32_t _duration) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
						store(_store),
						lock_name(_lock_name),
						cookie(_cookie),
						duration(_duration),
						pool(_pool), oid(_oid),
                                                req(NULL) {
  }

  ~RGWSimpleRadosLockCR() {
    delete req;
  }

  int send_request() {
    rgw_obj obj = rgw_obj(pool, oid);
    req = new RGWAsyncLockSystemObj(stack->create_completion_notifier(),
			           store, NULL, obj, lock_name, cookie, duration);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWSimpleRadosUnlockCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  string lock_name;
  string cookie;

  rgw_bucket pool;
  string oid;

  RGWAsyncUnlockSystemObj *req;

public:
  RGWSimpleRadosUnlockCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      rgw_bucket& _pool, const string& _oid, const string& _lock_name,
		      const string& _cookie) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
						store(_store),
						lock_name(_lock_name),
						cookie(_cookie),
						pool(_pool), oid(_oid),
                                                req(NULL) {
  }

  ~RGWSimpleRadosUnlockCR() {
    delete req;
  }

  int send_request() {
    rgw_obj obj = rgw_obj(pool, oid);
    req = new RGWAsyncUnlockSystemObj(stack->create_completion_notifier(),
			           store, NULL, obj, lock_name, cookie);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWReadMDLogShardInfo : public RGWSimpleCoroutine {
  RGWRados *store;
  RGWMetadataLog *mdlog;
  int req_ret;

  int shard_id;
  RGWMetadataLogInfo *shard_info;
public:
  RGWReadMDLogShardInfo(RGWRados *_store, int _shard_id, RGWMetadataLogInfo *_shard_info) : RGWSimpleCoroutine(_store->ctx()),
                                                store(_store), mdlog(store->meta_mgr->get_log()),
                                                req_ret(0), shard_id(_shard_id), shard_info(_shard_info) {
  }

  int send_request() {
    int ret = mdlog->get_info_async(shard_id, shard_info, stack->get_completion_mgr(), (void *)stack, &req_ret);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: mdlog->get_info_async() returned ret=" << ret << dendl;
      return set_state(RGWCoroutine_Error, ret);
    }

    return 0;
  }

  int request_complete() {
    return req_ret;
  }
};

class RGWAsyncWait : public RGWAsyncRadosRequest {
  CephContext *cct;
  Mutex *lock;
  Cond *cond;
  utime_t interval;
protected:
  int _send_request() {
    Mutex::Locker l(*lock);
    return cond->WaitInterval(cct, *lock, interval);
  }
public:
  RGWAsyncWait(RGWAioCompletionNotifier *cn, CephContext *_cct, Mutex *_lock, Cond *_cond, int _secs) : RGWAsyncRadosRequest(cn),
                                       cct(_cct),
                                       lock(_lock), cond(_cond), interval(_secs, 0) {}

  void wakeup() {
    Mutex::Locker l(*lock);
    cond->Signal();
  }
};

class RGWWaitCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  Mutex *lock;
  Cond *cond;
  int secs;

  RGWAsyncWait *req;

public:
  RGWWaitCR(RGWAsyncRadosProcessor *_async_rados, CephContext *_cct,
	    Mutex *_lock, Cond *_cond,
            int _secs) : RGWSimpleCoroutine(cct), cct(_cct),
                         async_rados(_async_rados), lock(_lock), cond(_cond), secs(_secs) {
  }

  ~RGWWaitCR() {
    wakeup();
    delete req;
  }

  int send_request() {
    req = new RGWAsyncWait(stack->create_completion_notifier(), cct,  lock, cond, secs);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }

  void wakeup() {
    req->wakeup();
  }
};

class RGWAsyncReadMDLogEntries : public RGWAsyncRadosRequest {
  RGWRados *store;
  RGWMetadataLog *mdlog;
  int shard_id;
  string *marker;
  int max_entries;
  list<cls_log_entry> *entries;
  bool *truncated;

  void *handle;
protected:
  int _send_request() {
    utime_t from_time;
    utime_t end_time;

    mdlog->init_list_entries(shard_id, from_time, end_time, *marker, &handle);

    return mdlog->list_entries(handle, max_entries, *entries, marker, truncated);
  }
public:
  RGWAsyncReadMDLogEntries(RGWAioCompletionNotifier *cn, RGWRados *_store,
			   int _shard_id, string* _marker, int _max_entries,
			   list<cls_log_entry> *_entries, bool *_truncated) : RGWAsyncRadosRequest(cn), store(_store), mdlog(store->meta_mgr->get_log()),
                                                                   shard_id(_shard_id), marker(_marker), max_entries(_max_entries),
								   entries(_entries), truncated(_truncated) {}
};

class RGWReadMDLogEntriesCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  int shard_id;
  string marker;
  string *pmarker;
  int max_entries;
  list<cls_log_entry> *entries;
  bool *truncated;

  RGWAsyncReadMDLogEntries *req;

public:
  RGWReadMDLogEntriesCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		       int _shard_id, string*_marker, int _max_entries,
		       list<cls_log_entry> *_entries, bool *_truncated) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
						store(_store),
                                                shard_id(_shard_id), pmarker(_marker), max_entries(_max_entries),
						entries(_entries), truncated(_truncated) {
						}

  ~RGWReadMDLogEntriesCR() {
    delete req;
  }

  int send_request() {
    marker = *pmarker;
    req = new RGWAsyncReadMDLogEntries(stack->create_completion_notifier(),
			           store, shard_id, &marker, max_entries, entries, truncated);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    int ret = req->get_ret_status();
    if (ret >= 0 && !entries->empty()) {
     *pmarker = marker;
    }
    return req->get_ret_status();
  }
};


template <class T>
class RGWReadRESTResourceCR : public RGWSimpleCoroutine {
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string path;
  rgw_http_param_pair *params;
  T *result;

  RGWRESTReadResource *http_op;

public:
  RGWReadRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn, RGWHTTPManager *_http_manager,
			const string& _path, rgw_http_param_pair *_params,
			T *_result) : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
                                      path(_path), params(_params), result(_result), http_op(NULL) {}

  int send_request() {
    http_op = new RGWRESTReadResource(conn, path, params, NULL, http_manager);

    http_op->set_user_info((void *)stack);

    int ret = http_op->aio_read();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to fetch metadata" << dendl;
      log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
      http_op->put();
      return ret;
    }
    return 0;
  }

  int request_complete() {
    int ret = http_op->wait(result);
    http_op->put();
    if (ret < 0) {
      error_stream << "http operation failed: " << http_op->to_str() << " status=" << http_op->get_http_status() << std::endl;
      ldout(cct, 0) << "ERROR: failed to wait for op, ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }
};

class RGWReadRemoteMDLogShardInfoCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  RGWRESTReadResource *http_op;

  int shard_id;
  RGWMetadataLogInfo *shard_info;

public:
  RGWReadRemoteMDLogShardInfoCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                                                      int _shard_id, RGWMetadataLogInfo *_shard_info) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
                                                      http_op(NULL),
                                                      shard_id(_shard_id),
                                                      shard_info(_shard_info) {
  }

  int operate() {
    RGWRESTConn *conn = store->rest_master_conn;
    reenter(this) {
      yield {
	char buf[16];
	snprintf(buf, sizeof(buf), "%d", shard_id);
        rgw_http_param_pair pairs[] = { { "type" , "metadata" },
	                                { "id", buf },
					{ "info" , NULL },
	                                { NULL, NULL } };

        string p = "/admin/log/";

        http_op = new RGWRESTReadResource(conn, p, pairs, NULL, http_manager);

        http_op->set_user_info((void *)stack);

        int ret = http_op->aio_read();
        if (ret < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to read from " << p << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          http_op->put();
          return set_state(RGWCoroutine_Error, ret);
        }

        return io_block(0);
      }
      yield {
        int ret = http_op->wait(shard_info);
        if (ret < 0) {
          return set_state(RGWCoroutine_Error, ret);
        }
        return set_state(RGWCoroutine_Done, 0);
      }
    }
    return 0;
  }
};

class RGWInitSyncStatusCoroutine : public RGWCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWObjectCtx& obj_ctx;

  string lock_name;
  string cookie;
  rgw_meta_sync_info status;
  map<int, RGWMetadataLogInfo> shards_info;
public:
  RGWInitSyncStatusCoroutine(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, RGWHTTPManager *_http_mgr,
		      RGWObjectCtx& _obj_ctx, uint32_t _num_shards) : RGWCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
                                                http_manager(_http_mgr),
                                                obj_ctx(_obj_ctx) {
    lock_name = "sync_lock";
    status.num_shards = _num_shards;

#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];

    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    string cookie = buf;
  }

  int operate() {
    int ret;
    reenter(this) {
      yield {
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(async_rados, store, store->get_zone_params().log_pool, mdlog_sync_status_oid,
			             lock_name, cookie, lock_duration));
	if (retcode < 0) {
	  ldout(cct, 0) << "ERROR: failed to take a lock on " << mdlog_sync_status_oid << dendl;
	  return set_state(RGWCoroutine_Error, retcode);
	}
      }
      yield {
        call(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(async_rados, store, store->get_zone_params().log_pool,
				 mdlog_sync_status_oid, status));
      }
      yield { /* take lock again, we just recreated the object */
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(async_rados, store, store->get_zone_params().log_pool, mdlog_sync_status_oid,
			             lock_name, cookie, lock_duration));
	if (retcode < 0) {
	  ldout(cct, 0) << "ERROR: failed to take a lock on " << mdlog_sync_status_oid << dendl;
	  return set_state(RGWCoroutine_Error, retcode);
	}
      }
      /* fetch current position in logs */
      yield {
        for (int i = 0; i < (int)status.num_shards; i++) {
          spawn(new RGWReadRemoteMDLogShardInfoCR(store, http_manager, async_rados, i, &shards_info[i]), false);
	}
      }
      while (collect(&ret)) {
	if (ret < 0) {
	  return set_state(RGWCoroutine_Error);
	}
        yield;
      }
      yield {
        for (int i = 0; i < (int)status.num_shards; i++) {
	  rgw_meta_sync_marker marker;
	  marker.next_step_marker = shards_info[i].marker;
          spawn(new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(async_rados, store, store->get_zone_params().log_pool,
				                          RGWMetaSyncStatusManager::shard_obj_name(i), marker), true);
        }
      }
      yield {
	status.state = rgw_meta_sync_info::StateBuildingFullSyncMaps;
        call(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(async_rados, store, store->get_zone_params().log_pool,
				 mdlog_sync_status_oid, status));
      }
      yield { /* unlock */
	call(new RGWSimpleRadosUnlockCR(async_rados, store, store->get_zone_params().log_pool, mdlog_sync_status_oid,
			             lock_name, cookie));
      }
      while (collect(&ret)) {
	if (ret < 0) {
	  return set_state(RGWCoroutine_Error);
	}
        yield;
      }
      return set_state(RGWCoroutine_Done);
    }
    return 0;
  }
};

class RGWReadSyncStatusCoroutine : public RGWSimpleRadosReadCR<rgw_meta_sync_info> {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWObjectCtx& obj_ctx;

  rgw_meta_sync_status *sync_status;

public:
  RGWReadSyncStatusCoroutine(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      RGWObjectCtx& _obj_ctx,
		      rgw_meta_sync_status *_status) : RGWSimpleRadosReadCR(_async_rados, _store, _obj_ctx,
									    _store->get_zone_params().log_pool,
									    mdlog_sync_status_oid,
									    &_status->sync_info),
                                                                            async_rados(_async_rados), store(_store),
                                                                            obj_ctx(_obj_ctx),
									    sync_status(_status) {}

  int handle_data(rgw_meta_sync_info& data);
};

int RGWReadSyncStatusCoroutine::handle_data(rgw_meta_sync_info& data)
{
  if (retcode == -ENOENT) {
    return retcode;
  }

  map<uint32_t, rgw_meta_sync_marker>& markers = sync_status->sync_markers;
  for (int i = 0; i < (int)data.num_shards; i++) {
    spawn(new RGWSimpleRadosReadCR<rgw_meta_sync_marker>(async_rados, store, obj_ctx, store->get_zone_params().log_pool,
				                    RGWMetaSyncStatusManager::shard_obj_name(i), &markers[i]), true);
  }
  return 0;
}

class RGWOmapAppend : public RGWConsumerCR<string> {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;

  rgw_bucket pool;
  string oid;

  bool going_down;

  int num_pending_entries;
  list<string> pending_entries;

  map<string, bufferlist> entries;
public:

  RGWOmapAppend(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, rgw_bucket& _pool, const string& _oid)
                      : RGWConsumerCR<string>(_store->ctx()), async_rados(_async_rados),
		        store(_store), pool(_pool), oid(_oid), going_down(false), num_pending_entries(0) {}

#define OMAP_APPEND_MAX_ENTRIES 100
  int operate() {
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

  void flush_pending() {
    receive(pending_entries);
    num_pending_entries = 0;
  }

  void append(const string& s) {
    pending_entries.push_back(s);
    if (++num_pending_entries >= OMAP_APPEND_MAX_ENTRIES) {
      flush_pending();
    }
  }

  void finish() {
    going_down = true;
    flush_pending();
    set_sleeping(false);
  }
};

class RGWShardedOmapCRManager {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWCoroutine *op;

  int num_shards;

  vector<RGWOmapAppend *> shards;
public:
  RGWShardedOmapCRManager(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, RGWCoroutine *_op, int _num_shards, rgw_bucket& pool, const string& oid_prefix)
                      : async_rados(_async_rados),
		        store(_store), op(_op), num_shards(_num_shards) {
    shards.reserve(num_shards);
    for (int i = 0; i < num_shards; ++i) {
      char buf[oid_prefix.size() + 16];
      snprintf(buf, sizeof(buf), "%s.%d", oid_prefix.c_str(), i);
      RGWOmapAppend *shard = new RGWOmapAppend(async_rados, store, pool, buf);
      shards.push_back(shard);
      op->spawn(shard, false);
    }
  }
  void append(const string& entry) {
    int shard_id = store->key_to_shard_id(entry, shards.size());
    shards[shard_id]->append(entry);
  }
  void finish() {
    for (vector<RGWOmapAppend *>::iterator iter = shards.begin(); iter != shards.end(); ++iter) {
      (*iter)->finish();
    }
  }
};

class RGWFetchAllMetaCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  int num_shards;


  int req_ret;

  list<string> sections;
  list<string>::iterator sections_iter;
  list<string> result;

  RGWShardedOmapCRManager *entries_index;

public:
  RGWFetchAllMetaCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados, int _num_shards) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
						      num_shards(_num_shards),
						      req_ret(0), entries_index(NULL) {
  }

  int operate() {
    RGWRESTConn *conn = store->rest_master_conn;

    reenter(this) {
      entries_index = new RGWShardedOmapCRManager(async_rados, store, this, num_shards,
						  store->get_zone_params().log_pool, mdlog_sync_full_sync_index_prefix);
      yield {
	call(new RGWReadRESTResourceCR<list<string> >(store->ctx(), conn, http_manager,
				       "/admin/metadata", NULL, &sections));
      }
      if (get_ret_status() < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to fetch metadata sections" << dendl;
	return set_state(RGWCoroutine_Error);
      }
      sections_iter = sections.begin();
      for (; sections_iter != sections.end(); ++sections_iter) {
        yield {
	  string entrypoint = string("/admin/metadata/") + *sections_iter;
#warning need a better scaling solution here, requires streaming output
	  call(new RGWReadRESTResourceCR<list<string> >(store->ctx(), conn, http_manager,
				       entrypoint, NULL, &result));
	}
	yield {
	  if (get_ret_status() < 0) {
            ldout(store->ctx(), 0) << "ERROR: failed to fetch metadata section: " << *sections_iter << dendl;
	    return set_state(RGWCoroutine_Error);
	  }
	  for (list<string>::iterator iter = result.begin(); iter != result.end(); ++iter) {
	    ldout(store->ctx(), 20) << "list metadata: section=" << *sections_iter << " key=" << *iter << dendl;
	    string s = *sections_iter + ":" + *iter;
	    entries_index->append(s);
#warning error handling of shards
	  }
	}
      }
      yield entries_index->finish();
      int ret;
      while (collect(&ret)) {
	if (ret < 0) {
	  return set_state(RGWCoroutine_Error);
	}
        yield;
      }
      yield return set_state(RGWCoroutine_Done);
    }
    return 0;
  }
};

static string full_sync_index_shard_oid(int shard_id)
{
  char buf[mdlog_sync_full_sync_index_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", mdlog_sync_full_sync_index_prefix.c_str(), shard_id);
  return string(buf);
}

class RGWReadRemoteMetadataCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  RGWRESTReadResource *http_op;

  string section;
  string key;

  bufferlist *pbl;

public:
  RGWReadRemoteMetadataCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                                                      const string& _section, const string& _key, bufferlist *_pbl) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
                                                      http_op(NULL),
                                                      section(_section),
                                                      key(_key),
						      pbl(_pbl) {
  }

  int operate() {
    RGWRESTConn *conn = store->rest_master_conn;
    reenter(this) {
      yield {
        rgw_http_param_pair pairs[] = { { "key" , key.c_str()},
	                                { NULL, NULL } };

        string p = string("/admin/metadata/") + section + "/" + key;

        http_op = new RGWRESTReadResource(conn, p, pairs, NULL, http_manager);

        http_op->set_user_info((void *)stack);

        int ret = http_op->aio_read();
        if (ret < 0) {
          ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog data" << dendl;
          log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
          http_op->put();
          return set_state(RGWCoroutine_Error, ret);
        }

        return io_block(0);
      }
      yield {
        int ret = http_op->wait_bl(pbl);
        if (ret < 0) {
          return set_state(RGWCoroutine_Error, ret);
        }
        return set_state(RGWCoroutine_Done, 0);
      }
    }
    return 0;
  }
};

class RGWAsyncMetaStoreEntry : public RGWAsyncRadosRequest {
  RGWRados *store;
  string raw_key;
  bufferlist bl;
protected:
  int _send_request() {
    int ret = store->meta_mgr->put(raw_key, bl, RGWMetadataHandler::APPLY_ALWAYS);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: can't put key: ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }
public:
  RGWAsyncMetaStoreEntry(RGWAioCompletionNotifier *cn, RGWRados *_store,
                       const string& _raw_key,
                       bufferlist& _bl) : RGWAsyncRadosRequest(cn), store(_store),
                                          raw_key(_raw_key), bl(_bl) {}
};


class RGWMetaStoreEntryCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  string raw_key;
  bufferlist bl;

  RGWAsyncMetaStoreEntry *req;

public:
  RGWMetaStoreEntryCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                       const string& _raw_key,
                       bufferlist& _bl) : RGWSimpleCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
                                          raw_key(_raw_key), bl(_bl), req(NULL) {
  }

  ~RGWMetaStoreEntryCR() {
    delete req;
  }

  int send_request() {
    req = new RGWAsyncMetaStoreEntry(stack->create_completion_notifier(),
			           store, raw_key, bl);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

#define META_SYNC_UPDATE_MARKER_WINDOW 10

class RGWMetaSyncShardMarkerTrack {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;
  RGWCoroutine *cr;

  map<string, bool> pending;

  string high_marker;

  string marker_oid;
  rgw_meta_sync_marker sync_marker;

  int updates_since_flush;

public:
  RGWMetaSyncShardMarkerTrack(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
                         RGWCoroutine *_cr,
                         const string& _marker_oid,
                         const rgw_meta_sync_marker& _marker) : store(_store), http_manager(_mgr),
                                                                async_rados(_async_rados), cr(_cr),
                                                                marker_oid(_marker_oid),
                                                                sync_marker(_marker), updates_since_flush(0) {}

  void start(const string& pos) {
    pending[pos] = true;
  }

  RGWCoroutine *finish(const string& pos) {
    assert(!pending.empty());

    map<string, bool>::iterator iter = pending.begin();
    const string& first_pos = iter->first;

    if (pos > high_marker) {
      high_marker = pos;
    }

    pending.erase(pos);

    updates_since_flush++;

    if (pos == first_pos && (updates_since_flush >= META_SYNC_UPDATE_MARKER_WINDOW || pending.empty())) {
      return update_marker(high_marker);
    }
    return NULL;
  }

  RGWCoroutine *update_marker(const string& new_marker) {
    sync_marker.marker = new_marker;

    updates_since_flush = 0;

    ldout(store->ctx(), 20) << __func__ << "(): updating marker marker_oid=" << marker_oid << " marker=" << new_marker << dendl;
    return new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(async_rados, store, store->get_zone_params().log_pool,
				 marker_oid, sync_marker);
  }
};

class RGWMetaSyncSingleEntryCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  string raw_key;
  string entry_marker;

  ssize_t pos;
  string section;
  string key;

  int sync_status;

  bufferlist md_bl;

  RGWMetaSyncShardMarkerTrack *marker_tracker;

public:
  RGWMetaSyncSingleEntryCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
		           const string& _raw_key, const string& _entry_marker, RGWMetaSyncShardMarkerTrack *_marker_tracker) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
						      raw_key(_raw_key), entry_marker(_entry_marker),
                                                      pos(0), sync_status(0),
                                                      marker_tracker(_marker_tracker) {
  }

  int operate() {
    reenter(this) {
      yield {
        pos = raw_key.find(':');
        section = raw_key.substr(0, pos);
        key = raw_key.substr(pos + 1);
        call(new RGWReadRemoteMetadataCR(store, http_manager, async_rados, section, key, &md_bl));
      }

      if (sync_status < 0) {
#warning failed syncing, a metadata entry, need to log
#warning also need to handle errors differently here, depending on error (transient / permanent)
        return set_state(RGWCoroutine_Error, sync_status);
      }

      yield call(new RGWMetaStoreEntryCR(async_rados, store, raw_key, md_bl));

      sync_status = retcode;
      yield {
        /* update marker */
        int ret = call(marker_tracker->finish(entry_marker));
        if (ret < 0) {
          ldout(store->ctx(), 0) << "ERROR: marker_tracker->finish(" << entry_marker << ") returned ret=" << ret << dendl;
          return set_state(RGWCoroutine_Error, sync_status);
        }
      }
      if (sync_status == 0) {
        sync_status = retcode;
      }
      if (sync_status < 0) {
        return set_state(RGWCoroutine_Error, retcode);
      }
      return set_state(RGWCoroutine_Done, 0);
    }
    return 0;
  }
};

class RGWCloneMetaLogCoroutine : public RGWCoroutine {
  RGWRados *store;
  RGWMetadataLog *mdlog;
  RGWHTTPManager *http_manager;

  int shard_id;
  string marker;
  bool truncated;
  string *new_marker;

  int max_entries;

  RGWRESTReadResource *http_op;

  RGWAioCompletionNotifier *md_op_notifier;

  int req_ret;
  RGWMetadataLogInfo shard_info;
  rgw_mdlog_shard_data data;

public:
  RGWCloneMetaLogCoroutine(RGWRados *_store, RGWHTTPManager *_mgr,
		    int _id, const string& _marker, string *_new_marker) : RGWCoroutine(_store->ctx()), store(_store),
                                                      mdlog(store->meta_mgr->get_log()),
                                                      http_manager(_mgr), shard_id(_id),
                                                      marker(_marker), truncated(false), new_marker(_new_marker),
                                                      max_entries(CLONE_MAX_ENTRIES),
						      http_op(NULL), md_op_notifier(NULL),
						      req_ret(0) {
    if (new_marker) {
      *new_marker = marker;
    }
  }

  int operate();

  int state_init();
  int state_read_shard_status();
  int state_read_shard_status_complete();
  int state_send_rest_request();
  int state_receive_rest_response();
  int state_store_mdlog_entries();
  int state_store_mdlog_entries_complete();
};

class RGWMetaSyncShardCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  rgw_bucket pool;

  uint32_t shard_id;
  rgw_meta_sync_marker sync_marker;

  map<string, bufferlist> entries;
  map<string, bufferlist>::iterator iter;

  string oid;

  RGWMetaSyncShardMarkerTrack *marker_tracker;

  list<cls_log_entry> log_entries;
  list<cls_log_entry>::iterator log_iter;
  bool truncated;

  string mdlog_marker;
  string raw_key;

  Mutex inc_lock;
  Cond inc_cond;

  boost::asio::coroutine incremental_cr;
  boost::asio::coroutine full_cr;


public:
  RGWMetaSyncShardCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados,
		     rgw_bucket& _pool,
		     uint32_t _shard_id, rgw_meta_sync_marker& _marker) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
						      pool(_pool),
						      shard_id(_shard_id),
						      sync_marker(_marker),
                                                      marker_tracker(NULL), truncated(false), inc_lock("RGWMetaSyncShardCR::inc_lock") {
  }

  ~RGWMetaSyncShardCR() {
    delete marker_tracker;
  }

  void set_marker_tracker(RGWMetaSyncShardMarkerTrack *mt) {
    delete marker_tracker;
    marker_tracker = mt;
  }

  int operate() {
    while (true) {
      switch (sync_marker.state) {
      case rgw_meta_sync_marker::FullSync:
        return full_sync();
      case rgw_meta_sync_marker::IncrementalSync:
        return incremental_sync();
        break;
      default:
        return set_state(RGWCoroutine_Error, -EIO);
      }
    }
    return 0;
  }

  int full_sync() {
    int ret;

#define OMAP_GET_MAX_ENTRIES 100
    int max_entries = OMAP_GET_MAX_ENTRIES;
    reenter(&full_cr) {
      oid = full_sync_index_shard_oid(shard_id);
      set_marker_tracker(new RGWMetaSyncShardMarkerTrack(store, http_manager, async_rados, this,
                                                         RGWMetaSyncStatusManager::shard_obj_name(shard_id),
                                                         sync_marker));
      do {
        yield return call(new RGWRadosGetOmapKeysCR(store, pool, oid, sync_marker.marker, &entries, max_entries));
        if (retcode < 0) {
          ldout(store->ctx(), 0) << "ERROR: " << __func__ << "(): RGWRadosGetOmapKeysCR() returned ret=" << retcode << dendl;
          return set_state(RGWCoroutine_Error, retcode);
        }
        iter = entries.begin();
        for (; iter != entries.end(); ++iter) {
          ldout(store->ctx(), 20) << __func__ << ": full sync: " << iter->first << dendl;
          marker_tracker->start(iter->first);
            // fetch remote and write locally
          yield spawn(new RGWMetaSyncSingleEntryCR(store, http_manager, async_rados, iter->first, iter->first, marker_tracker), false);
          if (retcode < 0) {
            return set_state(RGWCoroutine_Error, retcode);
          }
          sync_marker.marker = iter->first;
        }
      } while ((int)entries.size() == max_entries);

      /* wait for all operations to complete */
      while (collect(&ret)) {
        if (ret < 0) {
          ldout(store->ctx(), 0) << "ERROR: a sync operation returned error" << dendl;
          /* we should have reported this error */
#warning deal with error
        }
        yield;
      }

      yield {
        /* update marker to reflect we're done with full sync */
        sync_marker.state = rgw_meta_sync_marker::IncrementalSync;
        sync_marker.marker = sync_marker.next_step_marker;
        sync_marker.next_step_marker.clear();
        call(new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(async_rados, store, store->get_zone_params().log_pool,
                                                                   RGWMetaSyncStatusManager::shard_obj_name(shard_id), sync_marker));
      }
      if (retcode < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to set sync marker: retcode=" << retcode << dendl;
        return set_state(RGWCoroutine_Error, retcode);
      }
    }
    return 0;
  }
    

  int incremental_sync() {
    reenter(&incremental_cr) {
      mdlog_marker = sync_marker.marker;
      set_marker_tracker(new RGWMetaSyncShardMarkerTrack(store, http_manager, async_rados, this,
                                                         RGWMetaSyncStatusManager::shard_obj_name(shard_id),
                                                         sync_marker));
      do {
#define INCREMENTAL_MAX_ENTRIES 100
	ldout(store->ctx(), 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " mdlog_marker=" << mdlog_marker << " sync_marker.marker=" << sync_marker.marker << dendl;
	if (mdlog_marker <= sync_marker.marker) {
	  /* we're at the tip, try to bring more entries */
          ldout(store->ctx(), 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " syncing mdlog for shard_id=" << shard_id << dendl;
	  yield call(new RGWCloneMetaLogCoroutine(store, http_manager, shard_id, mdlog_marker, &mdlog_marker));
	}
	ldout(store->ctx(), 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " mdlog_marker=" << mdlog_marker << " sync_marker.marker=" << sync_marker.marker << dendl;
	if (mdlog_marker > sync_marker.marker) {
          yield call(new RGWReadMDLogEntriesCR(async_rados, store, shard_id, &sync_marker.marker, INCREMENTAL_MAX_ENTRIES, &log_entries, &truncated));
          for (log_iter = log_entries.begin(); log_iter != log_entries.end(); ++log_iter) {
            ldout(store->ctx(), 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " log_entry: " << log_iter->id << ":" << log_iter->section << ":" << log_iter->name << ":" << log_iter->timestamp << dendl;
            marker_tracker->start(log_iter->id);
            raw_key = log_iter->section + ":" + log_iter->name;
            yield spawn(new RGWMetaSyncSingleEntryCR(store, http_manager, async_rados, raw_key, log_iter->id, marker_tracker), false);
            if (retcode < 0) {
              return set_state(RGWCoroutine_Error, retcode);
          }
	  }
	}
	ldout(store->ctx(), 20) << __func__ << ":" << __LINE__ << ": shard_id=" << shard_id << " mdlog_marker=" << mdlog_marker << " sync_marker.marker=" << sync_marker.marker << dendl;
	if (mdlog_marker == sync_marker.marker) {
#define INCREMENTAL_INTERVAL 20
	  yield call(new RGWWaitCR(async_rados, store->ctx(), &inc_lock, &inc_cond, INCREMENTAL_INTERVAL));
	}
      } while (true);
    }
    /* TODO */
    return 0;
  }

  void wakeup() {
    Mutex::Locker l(inc_lock);
    inc_cond.Signal();
  }
};

class RGWMetaSyncCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager *http_manager;
  RGWAsyncRadosProcessor *async_rados;

  rgw_meta_sync_status sync_status;

  map<int, RGWMetaSyncShardCR *> shard_crs;


public:
  RGWMetaSyncCR(RGWRados *_store, RGWHTTPManager *_mgr, RGWAsyncRadosProcessor *_async_rados, rgw_meta_sync_status& _sync_status) : RGWCoroutine(_store->ctx()), store(_store),
                                                      http_manager(_mgr),
						      async_rados(_async_rados),
						      sync_status(_sync_status) {
  }

  int operate() {
    reenter(this) {
      yield {
	map<uint32_t, rgw_meta_sync_marker>::iterator iter = sync_status.sync_markers.begin();
	for (; iter != sync_status.sync_markers.end(); ++iter) {
	  uint32_t shard_id = iter->first;
	  rgw_meta_sync_marker marker;

	  RGWMetaSyncShardCR *shard_cr = new RGWMetaSyncShardCR(store, http_manager, async_rados, store->get_zone_params().log_pool,
				       shard_id,
				       sync_status.sync_markers[shard_id]);


	  shard_crs[shard_id] = shard_cr;
          spawn(shard_cr, true);
        }
      }
      yield return set_state(RGWCoroutine_Done);
    }
    return 0;
  }

  void wakeup(int shard_id) {
    map<int, RGWMetaSyncShardCR *>::iterator iter = shard_crs.find(shard_id);
    if (iter == shard_crs.end()) {
      return;
    }
    iter->second->wakeup();
  }
};

int RGWRemoteMetaLog::clone_shards(int num_shards, vector<string>& clone_markers)
{
  list<RGWCoroutinesStack *> stacks;
  for (int i = 0; i < (int)num_shards; i++) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
    int r = stack->call(new RGWCloneMetaLogCoroutine(store, &http_manager, i, clone_markers[i], NULL));
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: stack->call() returned r=" << r << dendl;
      return r;
    }

    stacks.push_back(stack);
  }

  return run(stacks);
}

int RGWRemoteMetaLog::fetch(int num_shards, vector<string>& clone_markers)
{
  list<RGWCoroutinesStack *> stacks;
  for (int i = 0; i < (int)num_shards; i++) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
    int r = stack->call(new RGWCloneMetaLogCoroutine(store, &http_manager, i, clone_markers[i], NULL));
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: stack->call() returned r=" << r << dendl;
      return r;
    }

    stacks.push_back(stack);
  }

  return run(stacks);
}

int RGWRemoteMetaLog::read_sync_status(rgw_meta_sync_status *sync_status)
{
  RGWObjectCtx obj_ctx(store, NULL);
  return run(new RGWReadSyncStatusCoroutine(async_rados, store, obj_ctx, sync_status));
}

int RGWRemoteMetaLog::init_sync_status(int num_shards)
{
  RGWObjectCtx obj_ctx(store, NULL);
  return run(new RGWInitSyncStatusCoroutine(async_rados, store, &http_manager, obj_ctx, num_shards));
}

int RGWRemoteMetaLog::set_sync_info(const rgw_meta_sync_info& sync_info)
{
  return run(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(async_rados, store, store->get_zone_params().log_pool,
				 mdlog_sync_status_oid, sync_info));
}

int RGWRemoteMetaLog::run_sync(int num_shards, rgw_meta_sync_status& sync_status)
{
  if (store->is_meta_master()) {
    return 0;
  }

  RGWObjectCtx obj_ctx(store, NULL);

  int r = run(new RGWReadSyncStatusCoroutine(async_rados, store, obj_ctx, &sync_status));
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch sync status" << dendl;
    return r;
  }

  switch ((rgw_meta_sync_info::SyncState)sync_status.sync_info.state) {
    case rgw_meta_sync_info::StateInit:
      ldout(store->ctx(), 20) << __func__ << "(): init" << dendl;
      r = run(new RGWInitSyncStatusCoroutine(async_rados, store, &http_manager, obj_ctx, num_shards));
      /* fall through */
    case rgw_meta_sync_info::StateBuildingFullSyncMaps:
      ldout(store->ctx(), 20) << __func__ << "(): building full sync maps" << dendl;
      r = run(new RGWFetchAllMetaCR(store, &http_manager, async_rados, num_shards));
      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to fetch all metadata keys" << dendl;
        return r;
      }

      sync_status.sync_info.state = rgw_meta_sync_info::StateSync;
      r = set_sync_info(sync_status.sync_info);
      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to update sync status" << dendl;
        return r;
      }
      /* fall through */
    case rgw_meta_sync_info::StateSync:
      ldout(store->ctx(), 20) << __func__ << "(): sync" << dendl;
      meta_sync_cr = new RGWMetaSyncCR(store, &http_manager, async_rados, sync_status);
      r = run(meta_sync_cr);
      if (r < 0) {
        ldout(store->ctx(), 0) << "ERROR: failed to fetch all metadata keys" << dendl;
        return r;
      }
      break;
    default:
      ldout(store->ctx(), 0) << "ERROR: bad sync state!" << dendl;
      return -EIO;
  }

  return 0;
}

void RGWRemoteMetaLog::wakeup(int shard_id)
{
  if (!meta_sync_cr) {
    return;
  }
  meta_sync_cr->wakeup(shard_id);
}

int RGWCloneMetaLogCoroutine::operate()
{
  reenter(this) {
    do {
      yield {
        ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": init request" << dendl;
        return state_init();
      }
      yield {
        ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": reading shard status" << dendl;
        return state_read_shard_status();
      }
      yield {
        ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": reading shard status complete" << dendl;
        return state_read_shard_status_complete();
      }
      yield {
        ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": sending rest request" << dendl;
        return state_send_rest_request();
      }
      yield {
        ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": receiving rest response" << dendl;
        return state_receive_rest_response();
      }
      yield {
        ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": storing mdlog entries" << dendl;
        return state_store_mdlog_entries();
      }
    } while (truncated);
    yield {
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": storing mdlog entries complete" << dendl;
      return state_store_mdlog_entries_complete();
    }
  }

  return 0;
}

int RGWCloneMetaLogCoroutine::state_init()
{
  data = rgw_mdlog_shard_data();

  return 0;
}

int RGWCloneMetaLogCoroutine::state_read_shard_status()
{
  int ret = mdlog->get_info_async(shard_id, &shard_info, stack->get_completion_mgr(), (void *)stack, &req_ret);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: mdlog->get_info_async() returned ret=" << ret << dendl;
    return set_state(RGWCoroutine_Error, ret);
  }

  return io_block(0);
}

int RGWCloneMetaLogCoroutine::state_read_shard_status_complete()
{
  ldout(store->ctx(), 20) << "shard_id=" << shard_id << " marker=" << shard_info.marker << " last_update=" << shard_info.last_update << dendl;

  marker = shard_info.marker;

  return 0;
}

int RGWCloneMetaLogCoroutine::state_send_rest_request()
{
  RGWRESTConn *conn = store->rest_master_conn;

  char buf[32];
  snprintf(buf, sizeof(buf), "%d", shard_id);

  char max_entries_buf[32];
  snprintf(max_entries_buf, sizeof(max_entries_buf), "%d", max_entries);

  const char *marker_key = (marker.empty() ? "" : "marker");

  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { "id", buf },
                                  { "max-entries", max_entries_buf },
                                  { marker_key, marker.c_str() },
                                  { NULL, NULL } };

  http_op = new RGWRESTReadResource(conn, "/admin/log", pairs, NULL, http_manager);

  http_op->set_user_info((void *)stack);

  int ret = http_op->aio_read();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog data" << dendl;
    log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
    http_op->put();
    return ret;
  }

  return io_block(0);
}

int RGWCloneMetaLogCoroutine::state_receive_rest_response()
{
  int ret = http_op->wait(&data);
  if (ret < 0) {
    error_stream << "http operation failed: " << http_op->to_str() << " status=" << http_op->get_http_status() << std::endl;
    ldout(store->ctx(), 0) << "ERROR: failed to wait for op, ret=" << ret << dendl;
    http_op->put();
    return set_state(RGWCoroutine_Error, ret);
  }
  http_op->put();

  ldout(store->ctx(), 20) << "remote mdlog, shard_id=" << shard_id << " num of shard entries: " << data.entries.size() << dendl;

  truncated = ((int)data.entries.size() == max_entries);

  if (data.entries.empty()) {
    return set_state(RGWCoroutine_Done);
  }

  if (new_marker) {
    *new_marker = data.entries.back().id;
  }

  return 0;
}


int RGWCloneMetaLogCoroutine::state_store_mdlog_entries()
{
  list<cls_log_entry> dest_entries;

  vector<rgw_mdlog_entry>::iterator iter;
  for (iter = data.entries.begin(); iter != data.entries.end(); ++iter) {
    rgw_mdlog_entry& entry = *iter;
    ldout(store->ctx(), 20) << "entry: name=" << entry.name << dendl;

    cls_log_entry dest_entry;
    dest_entry.id = entry.id;
    dest_entry.section = entry.section;
    dest_entry.name = entry.name;
    dest_entry.timestamp = entry.timestamp;
  
    ::encode(entry.log_data, dest_entry.data);

    dest_entries.push_back(dest_entry);

    marker = entry.id;
  }

  RGWAioCompletionNotifier *cn = stack->create_completion_notifier();

  int ret = store->meta_mgr->store_md_log_entries(dest_entries, shard_id, cn->completion());
  if (ret < 0) {
    cn->put();
    ldout(store->ctx(), 10) << "failed to store md log entries shard_id=" << shard_id << " ret=" << ret << dendl;
    return set_state(RGWCoroutine_Error, ret);
  }
  return io_block(0);
}

int RGWCloneMetaLogCoroutine::state_store_mdlog_entries_complete()
{
  return set_state(RGWCoroutine_Done);
}


