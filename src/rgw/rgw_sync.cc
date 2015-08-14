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

class RGWAsyncWriteOmapKeys : public RGWAsyncRadosRequest {
  RGWRados *store;
  rgw_obj obj;
  map<string, bufferlist> entries;

protected:
  int _send_request() {
    librados::IoCtx ioctx;
    librados::Rados *rados = store->get_rados_handle();
    int r = rados->ioctx_create(obj.bucket.name.c_str(), ioctx); /* system object only! */
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed to open pool (" << obj.bucket.name << ") ret=" << r << dendl;
      return r;
    }

    return ioctx.omap_set(obj.get_object(), entries);
  }
public:
  RGWAsyncWriteOmapKeys(RGWAioCompletionNotifier *cn, RGWRados *_store, rgw_obj& _obj,
		     map<string, bufferlist>& _entries) : RGWAsyncRadosRequest(cn), store(_store),
                                                                obj(_obj), entries(_entries) {}
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
#define CLONE_OPS_WINDOW 16


int RGWMetaSyncStatusManager::init()
{
  if (store->is_meta_master()) {
    return -EINVAL;
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
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  map<string, bufferlist> entries;

  rgw_bucket pool;
  string oid;

  RGWAsyncWriteOmapKeys *req;

public:
  RGWRadosSetOmapKeysCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      rgw_bucket& _pool, const string& _oid,
		      map<string, bufferlist>& _entries) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
						store(_store),
						entries(_entries),
						pool(_pool), oid(_oid),
                                                req(NULL) {
  }

  ~RGWRadosSetOmapKeysCR() {
    delete req;
  }

  int send_request() {
    rgw_obj obj = rgw_obj(pool, oid);
    req = new RGWAsyncWriteOmapKeys(stack->create_completion_notifier(), store, obj, entries);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
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

class RGWInitSyncStatusCoroutine : public RGWCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWObjectCtx& obj_ctx;

  string lock_name;
  string cookie;
  rgw_meta_sync_info status;

public:
  RGWInitSyncStatusCoroutine(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      RGWObjectCtx& _obj_ctx, uint32_t _num_shards) : RGWCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
                                                obj_ctx(_obj_ctx) {
    lock_name = "sync_lock";
    status.num_shards = _num_shards;

#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];

    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    string cookie = buf;
  }

  int operate() {
    reenter(this) {
      yield {
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(async_rados, store, store->get_zone_params().log_pool, mdlog_sync_status_oid,
			             lock_name, cookie, lock_duration));
      }
      yield {
        call(new RGWSimpleRadosWriteCR<rgw_meta_sync_info>(async_rados, store, store->get_zone_params().log_pool,
				 mdlog_sync_status_oid, status));
      }
      yield { /* take lock again, we just recreated the object */
	uint32_t lock_duration = 30;
	call(new RGWSimpleRadosLockCR(async_rados, store, store->get_zone_params().log_pool, mdlog_sync_status_oid,
			             lock_name, cookie, lock_duration));
      }
      yield {
        for (int i = 0; i < (int)status.num_shards; i++) {
	  rgw_meta_sync_marker marker;
          spawn(new RGWSimpleRadosWriteCR<rgw_meta_sync_marker>(async_rados, store, store->get_zone_params().log_pool,
				                          RGWMetaSyncStatusManager::shard_obj_name(i), marker), true);
        }
      }
      yield { /* unlock */
	call(new RGWSimpleRadosUnlockCR(async_rados, store, store->get_zone_params().log_pool, mdlog_sync_status_oid,
			             lock_name, cookie));
      }
      yield {
	int ret = stack->complete_spawned();
	if (ret < 0) {
	  return set_state(RGWCoroutine_Error);
	}
	return set_state(RGWCoroutine_Done);
      }
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
  int finish();
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

int RGWReadSyncStatusCoroutine::finish()
{
  return stack->complete_spawned();
}

class RGWOmapAppend : public RGWConsumerCR<string> {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWCoroutinesEnv *env;

  rgw_bucket pool;
  string oid;

  map<string, bufferlist> entries;
public:

  RGWOmapAppend(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, RGWCoroutinesEnv *_env, rgw_bucket& _pool, const string& _oid)
                      : RGWConsumerCR<string>(_store->ctx()), async_rados(_async_rados),
		        store(_store), env(_env), pool(_pool), oid(_oid) {}

#define OMAP_APPEND_MAX_ENTRIES 100
  int operate() {
    reenter(this) {
      for (;;) {
        yield wait_for_product();
        yield {
	  string entry;
	  while (consume(&entry)) {
	    entries[entry] = bufferlist();
	    if (entries.size() >= OMAP_APPEND_MAX_ENTRIES) {
	      break;
	    }
	  }
	  call(new RGWRadosSetOmapKeysCR(async_rados, store, pool, oid, entries));
	}
        if (get_ret_status() < 0) {
	  return set_state(RGWCoroutine_Error);
        }
      }
      return set_state(RGWCoroutine_Done);
    }
    return 0;
  }
};

class RGWShardedOmapCRManager {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWCoroutinesEnv *env;

  int num_shards;

  vector<RGWOmapAppend *> shards;
public:
  RGWShardedOmapCRManager(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, RGWCoroutinesEnv *_env, int _num_shards, rgw_bucket& pool, const string& oid_prefix)
                      : async_rados(_async_rados),
		        store(_store), env(_env), num_shards(_num_shards) {
    shards.reserve(num_shards);
    for (int i = 0; i < num_shards; ++i) {
      char buf[oid_prefix.size() + 16];
      snprintf(buf, sizeof(buf), "%s.%d", oid_prefix.c_str(), i);
      RGWOmapAppend *shard = new RGWOmapAppend(async_rados, store, env, pool, buf);
      shards.push_back(shard);
      env->stack->spawn(shard, false);
    }
  }
  void append(const string& entry) {
    static int counter = 0;
    shards[++counter % shards.size()]->receive(entry);
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
      entries_index = new RGWShardedOmapCRManager(async_rados, store, stack->get_env(), num_shards,
						  store->get_zone_params().log_pool, "meta.full-sync.index");
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
      yield return set_state(RGWCoroutine_Done);
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

  int max_entries;

  RGWRESTReadResource *http_op;

  RGWAioCompletionNotifier *md_op_notifier;

  int req_ret;
  RGWMetadataLogInfo shard_info;
  rgw_mdlog_shard_data data;

public:
  RGWCloneMetaLogCoroutine(RGWRados *_store, RGWHTTPManager *_mgr,
		    int _id, const string& _marker) : RGWCoroutine(_store->ctx()), store(_store),
                                                      mdlog(store->meta_mgr->get_log()),
                                                      http_manager(_mgr), shard_id(_id),
                                                      marker(_marker), truncated(false), max_entries(CLONE_MAX_ENTRIES),
						      http_op(NULL), md_op_notifier(NULL),
						      req_ret(0) {}

  int operate();

  int state_init();
  int state_read_shard_status();
  int state_read_shard_status_complete();
  int state_send_rest_request();
  int state_receive_rest_response();
  int state_store_mdlog_entries();
  int state_store_mdlog_entries_complete();
};

int RGWRemoteMetaLog::clone_shards(int num_shards, vector<string>& clone_markers)
{
  list<RGWCoroutinesStack *> stacks;
  for (int i = 0; i < (int)num_shards; i++) {
    RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
    int r = stack->call(new RGWCloneMetaLogCoroutine(store, &http_manager, i, clone_markers[i]));
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
    int r = stack->call(new RGWCloneMetaLogCoroutine(store, &http_manager, i, clone_markers[i]));
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
  return run(new RGWInitSyncStatusCoroutine(async_rados, store, obj_ctx, num_shards));
}

int RGWRemoteMetaLog::run_sync(int num_shards, rgw_meta_sync_status& sync_status)
{
  RGWObjectCtx obj_ctx(store, NULL);
  return run(new RGWFetchAllMetaCR(store, &http_manager, async_rados, num_shards));
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


