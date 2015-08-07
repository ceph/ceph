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


#define dout_subsys ceph_subsys_rgw

static string mdlog_sync_status_oid = "mdlog.sync-status";

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


int RGWRemoteMetaLog::init()
{
  CephContext *cct = store->ctx();
  async_rados = new RGWAsyncRadosProcessor(store, cct->_conf->rgw_num_async_rados_threads);
  async_rados->start();

  conn = store->rest_master_conn;

  rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                  { NULL, NULL } };

  int ret = conn->get_json_resource("/admin/log", pairs, log_info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog info" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote mdlog, num_shards=" << log_info.num_shards << dendl;

  RWLock::WLocker wl(ts_to_shard_lock);
  for (int i = 0; i < (int)log_info.num_shards; i++) {
    clone_markers.push_back(string());
    utime_shard ut;
    ut.shard_id = i;
    ts_to_shard[ut] = i;
  }

  ret = http_manager.set_threaded();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "failed in http_manager.set_threaded() ret=" << ret << dendl;
    return ret;
  }

  ret = status_manager.init(log_info.num_shards);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "failed in status_manager.init() ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

void RGWRemoteMetaLog::finish()
{
  async_rados->stop();
  delete async_rados;
}

int RGWRemoteMetaLog::list_shards()
{
  for (int i = 0; i < (int)log_info.num_shards; i++) {
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


int RGWMetaSyncStatusManager::init(int _num_shards)
{
  num_shards = _num_shards;

  const char *log_pool = store->get_zone_params().log_pool.name.c_str();
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(log_pool, ioctx);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: failed to open log pool (" << store->get_zone_params().log_pool.name << " ret=" << r << dendl;
    return r;
  }

  global_status_oid = "mdlog.state.global";
  shard_status_oid_prefix = "mdlog.state.shard";
  global_status_obj = rgw_obj(store->get_zone_params().log_pool, global_status_oid);

  for (int i = 0; i < num_shards; i++) {
    shard_objs[i] = rgw_obj(store->get_zone_params().log_pool, shard_obj_name(i));
  }

  return 0;
}

int RGWMetaSyncStatusManager::read_global_status()
{
  RGWObjectCtx obj_ctx(store, NULL);

  RGWRados::SystemObject src(store, obj_ctx, global_status_obj);
  RGWRados::SystemObject::Read rop(&src);

  bufferlist bl;

  int ret = rop.read(0, -1, bl, NULL);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }

  if (ret != -ENOENT) {
    bufferlist::iterator iter = bl.begin();
    try {
      ::decode(global_status, iter);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0) << "ERROR: failed to decode global mdlog status" << dendl;
    }
  }
  return 0;
}

string RGWMetaSyncStatusManager::shard_obj_name(int shard_id)
{
  char buf[shard_status_oid_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", shard_status_oid_prefix.c_str(), shard_id);

  return string(buf);
}

int RGWMetaSyncStatusManager::read_shard_status(int shard_id)
{
  RGWObjectCtx obj_ctx(store, NULL);

  RGWRados::SystemObject src(store, obj_ctx, shard_objs[shard_id]);
  RGWRados::SystemObject::Read rop(&src);

  bufferlist bl;

  int ret = rop.read(0, -1, bl, NULL);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }

  if (ret != -ENOENT) {
    bufferlist::iterator iter = bl.begin();
    try {
      ::decode(shard_markers[shard_id], iter);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0) << "ERROR: failed to decode global mdlog status" << dendl;
    }
  }
  return 0;
}

int RGWMetaSyncStatusManager::set_state(RGWMetaSyncGlobalStatus::SyncState state)
{
  global_status.state = state;

  bufferlist bl;
  ::encode(global_status, bl);

  int ret = rgw_put_system_obj(store, store->get_zone_params().log_pool, global_status_oid, bl.c_str(), bl.length(), false, NULL, 0, NULL);
  if (ret < 0) {
    return ret;
  }

  return 0;
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
  req = new RGWAsyncGetSystemObj(env->stack->create_completion_notifier(),
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
    req = new RGWAsyncPutSystemObj(env->stack->create_completion_notifier(),
			           store, NULL, obj, false, bl);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWReadSyncStatusCoroutine : public RGWSimpleRadosReadCR<RGWMetaSyncGlobalStatus> {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWObjectCtx& obj_ctx;

  RGWMetaSyncGlobalStatus *global_status;
  rgw_sync_marker sync_marker;

public:
  RGWReadSyncStatusCoroutine(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      RGWObjectCtx& _obj_ctx,
		      RGWMetaSyncGlobalStatus *_gs) : RGWSimpleRadosReadCR(_async_rados, _store, _obj_ctx,
									    _store->get_zone_params().log_pool,
									    "mdlog.state.global",
									    _gs),
                                                      async_rados(_async_rados), store(_store),
                                                      obj_ctx(_obj_ctx), global_status(_gs) {}

  int handle_data(RGWMetaSyncGlobalStatus& data);
};

int RGWReadSyncStatusCoroutine::handle_data(RGWMetaSyncGlobalStatus& data)
{
  if (retcode == -ENOENT) {
    return retcode;
  }
  spawn(new RGWSimpleRadosReadCR<rgw_sync_marker>(async_rados, store, obj_ctx, store->get_zone_params().log_pool,
				 "mdlog.state.0", &sync_marker));
  spawn(new RGWSimpleRadosReadCR<rgw_sync_marker>(async_rados, store, obj_ctx, store->get_zone_params().log_pool,
				 "mdlog.state.1", &sync_marker));
  return 0;
}

class RGWMetaSyncCoroutine : public RGWCoroutine {
  RGWRados *store;
  RGWMetadataLog *mdlog;
  RGWHTTPManager *http_manager;

  int shard_id;
  string marker;

  int max_entries;

  RGWRESTReadResource *http_op;

  enum State {
    Init                      = 0,
    ReadSyncStatus            = 1,
    ReadSyncStatusComplete    = 2,
    Done                      = 100,
    Error                     = 200,
  } state;

  int set_state(State s, int ret = 0) {
    state = s;
    return ret;
  }
public:
  RGWMetaSyncCoroutine(RGWRados *_store, RGWHTTPManager *_mgr, int _id) : RGWCoroutine(), store(_store),
                           mdlog(store->meta_mgr->get_log()),
                           http_manager(_mgr),
			   shard_id(_id),
                           max_entries(CLONE_MAX_ENTRIES),
			   http_op(NULL),
                           state(RGWMetaSyncCoroutine::Init) {}

  int operate();

  int state_init();
  int state_read_sync_status();
  int state_read_sync_status_complete();

  bool is_done() { return (state == Done || state == Error); }
  bool is_error() { return (state == Error); }
};

int RGWMetaSyncCoroutine::operate()
{
  switch (state) {
    case Init:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": init request" << dendl;
      return state_init();
    case ReadSyncStatus:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": reading shard status" << dendl;
      return state_read_sync_status();
    case ReadSyncStatusComplete:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": reading shard status complete" << dendl;
      return state_read_sync_status_complete();
    case Done:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": done" << dendl;
      break;
    case Error:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": error" << dendl;
      break;
  }

  return 0;
}

int RGWMetaSyncCoroutine::state_init()
{
  return 0;
}

int RGWMetaSyncCoroutine::state_read_sync_status()
{
  return 0;
}

int RGWMetaSyncCoroutine::state_read_sync_status_complete()
{
  return 0;
}

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

  enum State {
    Init = 0,
    ReadShardStatus           = 1,
    ReadShardStatusComplete   = 2,
    SendRESTRequest           = 3,
    ReceiveRESTResponse       = 4,
    StoreMDLogEntries         = 5,
    StoreMDLogEntriesComplete = 6,
    Done                      = 100,
    Error                     = 200,
  } state;

  int set_state(State s, int ret = 0) {
    state = s;
    return ret;
  }
public:
  RGWCloneMetaLogCoroutine(RGWRados *_store, RGWHTTPManager *_mgr,
		    int _id, const string& _marker) : RGWCoroutine(), store(_store),
                                                      mdlog(store->meta_mgr->get_log()),
                                                      http_manager(_mgr), shard_id(_id),
                                                      marker(_marker), truncated(false), max_entries(CLONE_MAX_ENTRIES),
						      http_op(NULL), md_op_notifier(NULL),
						      req_ret(0),
                                                      state(RGWCloneMetaLogCoroutine::Init) {}

  int operate();

  int state_init();
  int state_read_shard_status();
  int state_read_shard_status_complete();
  int state_send_rest_request();
  int state_receive_rest_response();
  int state_store_mdlog_entries();
  int state_store_mdlog_entries_complete();

  bool is_done() { return (state == Done || state == Error); }
  bool is_error() { return (state == Error); }
};

int RGWRemoteMetaLog::clone_shards()
{
  list<RGWCoroutinesStack *> stacks;
  for (int i = 0; i < (int)log_info.num_shards; i++) {
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

int RGWRemoteMetaLog::fetch()
{
  list<RGWCoroutinesStack *> stacks;
  for (int i = 0; i < (int)log_info.num_shards; i++) {
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

int RGWRemoteMetaLog::get_sync_status(RGWMetaSyncGlobalStatus *sync_status)
{
  RGWObjectCtx obj_ctx(store, NULL);
  return run(new RGWReadSyncStatusCoroutine(async_rados, store, obj_ctx, sync_status));
}

int RGWRemoteMetaLog::get_shard_sync_marker(int shard_id, rgw_sync_marker *shard_status)
{
  int ret = status_manager.read_shard_status(shard_id);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: status_manager.read_global_status() returned ret=" << ret << dendl;
    return ret;
  }

  *shard_status = status_manager.get_shard_status(shard_id);

  return 0;
}

int RGWCloneMetaLogCoroutine::operate()
{
  switch (state) {
    case Init:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": init request" << dendl;
      return state_init();
    case ReadShardStatus:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": reading shard status" << dendl;
      return state_read_shard_status();
    case ReadShardStatusComplete:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": reading shard status complete" << dendl;
      return state_read_shard_status_complete();
    case SendRESTRequest:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": sending rest request" << dendl;
      return state_send_rest_request();
    case ReceiveRESTResponse:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": receiving rest response" << dendl;
      return state_receive_rest_response();
    case StoreMDLogEntries:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": storing mdlog entries" << dendl;
      return state_store_mdlog_entries();
    case StoreMDLogEntriesComplete:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": storing mdlog entries complete" << dendl;
      return state_store_mdlog_entries_complete();
    case Done:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": done" << dendl;
      break;
    case Error:
      ldout(store->ctx(), 20) << __func__ << ": shard_id=" << shard_id << ": error" << dendl;
      break;
  }

  return 0;
}

int RGWCloneMetaLogCoroutine::state_init()
{
  data = rgw_mdlog_shard_data();

  return set_state(ReadShardStatus);
}

int RGWCloneMetaLogCoroutine::state_read_shard_status()
{
  int ret = mdlog->get_info_async(shard_id, &shard_info, env->stack->get_completion_mgr(), (void *)env->stack, &req_ret);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: mdlog->get_info_async() returned ret=" << ret << dendl;
    return set_state(Error, ret);
  }

  return yield(set_state(ReadShardStatusComplete));
}

int RGWCloneMetaLogCoroutine::state_read_shard_status_complete()
{
  ldout(store->ctx(), 20) << "shard_id=" << shard_id << " marker=" << shard_info.marker << " last_update=" << shard_info.last_update << dendl;

  marker = shard_info.marker;

  return set_state(SendRESTRequest);
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

  http_op->set_user_info((void *)env->stack);

  int ret = http_op->aio_read();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog data" << dendl;
    log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
    http_op->put();
    return ret;
  }

  return yield(set_state(ReceiveRESTResponse));
}

int RGWCloneMetaLogCoroutine::state_receive_rest_response()
{
  int ret = http_op->wait(&data);
  if (ret < 0) {
    error_stream << "http operation failed: " << http_op->to_str() << " status=" << http_op->get_http_status() << std::endl;
    ldout(store->ctx(), 0) << "ERROR: failed to wait for op, ret=" << ret << dendl;
    http_op->put();
    return set_state(Error, ret);
  }
  http_op->put();

  ldout(store->ctx(), 20) << "remote mdlog, shard_id=" << shard_id << " num of shard entries: " << data.entries.size() << dendl;

  truncated = ((int)data.entries.size() == max_entries);

  if (data.entries.empty()) {
    return set_state(Done);
  }

  return set_state(StoreMDLogEntries);
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

  RGWAioCompletionNotifier *cn = env->stack->create_completion_notifier();

  int ret = store->meta_mgr->store_md_log_entries(dest_entries, shard_id, cn->completion());
  if (ret < 0) {
    cn->put();
    ldout(store->ctx(), 10) << "failed to store md log entries shard_id=" << shard_id << " ret=" << ret << dendl;
    return set_state(Error, ret);
  }
  return yield(set_state(StoreMDLogEntriesComplete));
}

int RGWCloneMetaLogCoroutine::state_store_mdlog_entries_complete()
{
  if (truncated) {
    return state_init();
  }
  return set_state(Done);
}


int RGWMetadataSync::init()
{
  if (store->is_meta_master()) {
    return -EINVAL;
  }

  if (!store->rest_master_conn) {
    lderr(store->ctx()) << "no REST connection to master zone" << dendl;
    return -EIO;
  }

  int ret = master_log.init();
  if (ret < 0) {
    return ret;
  }

  return 0;
}



