#ifndef CEPH_RGW_CR_RADOS_H
#define CEPH_RGW_CR_RADOS_H

#include "rgw_coroutine.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"

class RGWAsyncRadosRequest : public RefCountedObject {
  RGWCoroutine *caller;
  RGWAioCompletionNotifier *notifier;

  int retcode;

  Mutex lock;

protected:
  virtual int _send_request() = 0;
public:
  RGWAsyncRadosRequest(RGWCoroutine *_caller, RGWAioCompletionNotifier *_cn) : caller(_caller), notifier(_cn), retcode(0),
                                                                               lock("RGWAsyncRadosRequest::lock") {
  }
  virtual ~RGWAsyncRadosRequest() {
    if (notifier) {
      notifier->put();
    }
  }

  void send_request() {
    get();
    retcode = _send_request();
    {
      Mutex::Locker l(lock);
      if (notifier) {
        notifier->cb(); // drops its own ref
        notifier = nullptr;
      }
    }
    put();
  }

  int get_ret_status() { return retcode; }

  void finish() {
    {
      Mutex::Locker l(lock);
      if (notifier) {
        // we won't call notifier->cb() to drop its ref, so drop it here
        notifier->put();
        notifier = nullptr;
      }
    }
    put();
  }
};


class RGWAsyncRadosProcessor {
  deque<RGWAsyncRadosRequest *> m_req_queue;
  atomic_t going_down;
protected:
  RGWRados *store;
  ThreadPool m_tp;
  Throttle req_throttle;

  struct RGWWQ : public ThreadPool::WorkQueue<RGWAsyncRadosRequest> {
    RGWAsyncRadosProcessor *processor;
    RGWWQ(RGWAsyncRadosProcessor *p, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<RGWAsyncRadosRequest>("RGWWQ", timeout, suicide_timeout, tp), processor(p) {}

    bool _enqueue(RGWAsyncRadosRequest *req);
    void _dequeue(RGWAsyncRadosRequest *req) {
      assert(0);
    }
    bool _empty();
    RGWAsyncRadosRequest *_dequeue();
    using ThreadPool::WorkQueue<RGWAsyncRadosRequest>::_process;
    void _process(RGWAsyncRadosRequest *req, ThreadPool::TPHandle& handle);
    void _dump_queue();
    void _clear() {
      assert(processor->m_req_queue.empty());
    }
  } req_wq;

public:
  RGWAsyncRadosProcessor(RGWRados *_store, int num_threads);
  ~RGWAsyncRadosProcessor() {}
  void start();
  void stop();
  void handle_request(RGWAsyncRadosRequest *req);
  void queue(RGWAsyncRadosRequest *req);

  bool is_going_down() {
    return (going_down.read() != 0);
  }
};


class RGWAsyncGetSystemObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  RGWObjectCtx *obj_ctx;
  RGWRados::SystemObject::Read::GetObjState read_state;
  RGWObjVersionTracker *objv_tracker;
  rgw_obj obj;
  bufferlist *pbl;
  map<string, bufferlist> *pattrs;
  off_t ofs;
  off_t end;
protected:
  int _send_request();
public:
  RGWAsyncGetSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store, RGWObjectCtx *_obj_ctx,
                       RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                       bufferlist *_pbl, off_t _ofs, off_t _end);
  void set_read_attrs(map<string, bufferlist> *_pattrs) { pattrs = _pattrs; }
};

class RGWAsyncPutSystemObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  rgw_obj obj;
  bool exclusive;
  bufferlist bl;

protected:
  int _send_request();
public:
  RGWAsyncPutSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                       rgw_obj& _obj, bool _exclusive,
                       bufferlist& _bl);
};

class RGWAsyncPutSystemObjAttrs : public RGWAsyncRadosRequest {
  RGWRados *store;
  RGWObjVersionTracker *objv_tracker;
  rgw_obj obj;
  map<string, bufferlist> *attrs;

protected:
  int _send_request();
public:
  RGWAsyncPutSystemObjAttrs(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                       RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                       map<string, bufferlist> *_attrs);
};

class RGWAsyncLockSystemObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  rgw_obj obj;
  string lock_name;
  string cookie;
  uint32_t duration_secs;

protected:
  int _send_request();
public:
  RGWAsyncLockSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                        RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
		        const string& _name, const string& _cookie, uint32_t _duration_secs);
};

class RGWAsyncUnlockSystemObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  rgw_obj obj;
  string lock_name;
  string cookie;

protected:
  int _send_request();
public:
  RGWAsyncUnlockSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                        RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
		        const string& _name, const string& _cookie);
};


template <class T>
class RGWSimpleRadosReadCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWObjectCtx obj_ctx;
  bufferlist bl;

  rgw_bucket pool;
  string oid;

  map<string, bufferlist> *pattrs{nullptr};

  T *result;
  /// on ENOENT, call handle_data() with an empty object instead of failing
  const bool empty_on_enoent;

  RGWAsyncGetSystemObj *req{nullptr};

public:
  RGWSimpleRadosReadCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      const rgw_bucket& _pool, const string& _oid,
		      T *_result, bool empty_on_enoent = true)
    : RGWSimpleCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
      obj_ctx(store), pool(_pool), oid(_oid), result(_result),
      empty_on_enoent(empty_on_enoent) {}
  ~RGWSimpleRadosReadCR() {
    request_cleanup();
  }

  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
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

template <class T>
int RGWSimpleRadosReadCR<T>::request_complete()
{
  int ret = req->get_ret_status();
  retcode = ret;
  if (ret == -ENOENT && empty_on_enoent) {
    *result = T();
  } else {
    if (ret < 0) {
      return ret;
    }
    try {
      bufferlist::iterator iter = bl.begin();
      if (iter.end()) {
        // allow successful reads with empty buffers. ReadSyncStatus coroutines
        // depend on this to be able to read without locking, because the
        // cls lock from InitSyncStatus will create an empty object if it didnt
        // exist
        *result = T();
      } else {
        ::decode(*result, iter);
      }
    } catch (buffer::error& err) {
      return -EIO;
    }
  }

  return handle_data(*result);
}

class RGWSimpleRadosReadAttrsCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  RGWObjectCtx obj_ctx;
  bufferlist bl;

  rgw_bucket pool;
  string oid;

  map<string, bufferlist> *pattrs;

  RGWAsyncGetSystemObj *req;

public:
  RGWSimpleRadosReadAttrsCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      rgw_bucket& _pool, const string& _oid,
		      map<string, bufferlist> *_pattrs) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados), store(_store),
                                                obj_ctx(store),
						pool(_pool), oid(_oid),
                                                pattrs(_pattrs),
                                                req(NULL) { }
  ~RGWSimpleRadosReadAttrsCR() {
    request_cleanup();
  }
                                                         
  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request();
  int request_complete();
};

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
		      const rgw_bucket& _pool, const string& _oid,
		      const T& _data) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
						store(_store),
						pool(_pool), oid(_oid),
                                                req(NULL) {
    ::encode(_data, bl);
  }

  ~RGWSimpleRadosWriteCR() {
    request_cleanup();
  }

  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request() {
    rgw_obj obj = rgw_obj(pool, oid);
    req = new RGWAsyncPutSystemObj(this, stack->create_completion_notifier(),
			           store, obj, false, bl);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWSimpleRadosWriteAttrsCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;

  rgw_bucket pool;
  string oid;

  map<string, bufferlist> attrs;

  RGWAsyncPutSystemObjAttrs *req;

public:
  RGWSimpleRadosWriteAttrsCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
		      rgw_bucket& _pool, const string& _oid,
		      map<string, bufferlist>& _attrs) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
						store(_store),
						pool(_pool), oid(_oid),
                                                attrs(_attrs), req(NULL) {
  }
  ~RGWSimpleRadosWriteAttrsCR() {
    request_cleanup();
  }

  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request() {
    rgw_obj obj = rgw_obj(pool, oid);
    req = new RGWAsyncPutSystemObjAttrs(this, stack->create_completion_notifier(),
			           store, NULL, obj, &attrs);
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
		      map<string, bufferlist>& _entries);

  ~RGWRadosSetOmapKeysCR();

  int send_request();
  int request_complete();
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
		      const rgw_bucket& _pool, const string& _oid,
		      const string& _marker,
		      map<string, bufferlist> *_entries, int _max_entries);

  ~RGWRadosGetOmapKeysCR();

  int send_request();

  int request_complete() {
    return rval;
  }
};

class RGWRadosRemoveOmapKeysCR : public RGWSimpleCoroutine {
  RGWRados *store;

  librados::IoCtx ioctx;

  set<string> keys;

  rgw_bucket pool;
  string oid;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosRemoveOmapKeysCR(RGWRados *_store,
		      const rgw_bucket& _pool, const string& _oid,
		      const set<string>& _keys);

  ~RGWRadosRemoveOmapKeysCR();

  int send_request();

  int request_complete();
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
		      const rgw_bucket& _pool, const string& _oid, const string& _lock_name,
		      const string& _cookie,
		      uint32_t _duration);
  ~RGWSimpleRadosLockCR() {
    request_cleanup();
  }
  void request_cleanup();

  int send_request();
  int request_complete();
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
		      const rgw_bucket& _pool, const string& _oid, const string& _lock_name,
		      const string& _cookie);
  ~RGWSimpleRadosUnlockCR() {
    request_cleanup();
  }
  void request_cleanup();

  int send_request();
  int request_complete();
};

#define OMAP_APPEND_MAX_ENTRIES_DEFAULT 100

class RGWOmapAppend : public RGWConsumerCR<string> {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;

  rgw_bucket pool;
  string oid;

  bool going_down;

  int num_pending_entries;
  list<string> pending_entries;

  map<string, bufferlist> entries;

  uint64_t window_size;
  uint64_t total_entries;
public:
  RGWOmapAppend(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, rgw_bucket& _pool, const string& _oid,
                uint64_t _window_size = OMAP_APPEND_MAX_ENTRIES_DEFAULT);
  int operate();
  void flush_pending();
  bool append(const string& s);
  bool finish();

  uint64_t get_total_entries() {
    return total_entries;
  }

  const rgw_bucket& get_pool() {
    return pool;
  }

  const string& get_oid() {
    return oid;
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
  RGWAsyncWait(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, CephContext *_cct,
               Mutex *_lock, Cond *_cond, int _secs) : RGWAsyncRadosRequest(caller, cn),
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
                         async_rados(_async_rados), lock(_lock), cond(_cond), secs(_secs), req(NULL) {
  }
  ~RGWWaitCR() {
    request_cleanup();
  }

  void request_cleanup() {
    if (req) {
      wakeup();
      req->finish();
      req = NULL;
    }
  }

  int send_request() {
    req = new RGWAsyncWait(this, stack->create_completion_notifier(), cct,  lock, cond, secs);
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
      shard->get();
      shards.push_back(shard);
      op->spawn(shard, false);
    }
  }

  ~RGWShardedOmapCRManager() {
    for (auto shard : shards) {
      shard->put();
    }
  }

  bool append(const string& entry, int shard_id) {
    return shards[shard_id]->append(entry);
  }
  bool finish() {
    bool success = true;
    for (vector<RGWOmapAppend *>::iterator iter = shards.begin(); iter != shards.end(); ++iter) {
      success &= ((*iter)->finish() && (!(*iter)->is_error()));
    }
    return success;
  }

  uint64_t get_total_entries(int shard_id) {
    return shards[shard_id]->get_total_entries();
  }
};

class RGWAsyncGetBucketInstanceInfo : public RGWAsyncRadosRequest {
  RGWRados *store;
  rgw_bucket bucket;
  RGWBucketInfo *bucket_info;

protected:
  int _send_request();
public:
  RGWAsyncGetBucketInstanceInfo(RGWCoroutine *caller, RGWAioCompletionNotifier *cn,
                                RGWRados *_store, const rgw_bucket& bucket,
                                RGWBucketInfo *_bucket_info)
    : RGWAsyncRadosRequest(caller, cn), store(_store),
      bucket(bucket), bucket_info(_bucket_info) {}
};

class RGWGetBucketInstanceInfoCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  rgw_bucket bucket;
  RGWBucketInfo *bucket_info;

  RGWAsyncGetBucketInstanceInfo *req;
  
public:
  RGWGetBucketInstanceInfoCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                             const rgw_bucket& bucket, RGWBucketInfo *_bucket_info)
    : RGWSimpleCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
      bucket(bucket), bucket_info(_bucket_info), req(NULL) {}
  ~RGWGetBucketInstanceInfoCR() {
    request_cleanup();
  }
  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request() {
    req = new RGWAsyncGetBucketInstanceInfo(this, stack->create_completion_notifier(), store, bucket, bucket_info);
    async_rados->queue(req);
    return 0;
  }
  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWAsyncFetchRemoteObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  string source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;
  uint64_t versioned_epoch;

  real_time src_mtime;

  bool copy_if_newer;

protected:
  int _send_request();
public:
  RGWAsyncFetchRemoteObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                         const string& _source_zone,
                         RGWBucketInfo& _bucket_info,
                         const rgw_obj_key& _key,
                         uint64_t _versioned_epoch,
                         bool _if_newer) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                                      source_zone(_source_zone),
                                                      bucket_info(_bucket_info),
                                                      key(_key),
                                                      versioned_epoch(_versioned_epoch),
                                                      copy_if_newer(_if_newer) {}
};

class RGWFetchRemoteObjCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  string source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;
  uint64_t versioned_epoch;

  real_time src_mtime;

  bool copy_if_newer;

  RGWAsyncFetchRemoteObj *req;

public:
  RGWFetchRemoteObjCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                      const string& _source_zone,
                      RGWBucketInfo& _bucket_info,
                      const rgw_obj_key& _key,
                      uint64_t _versioned_epoch,
                      bool _if_newer) : RGWSimpleCoroutine(_store->ctx()), cct(_store->ctx()),
                                       async_rados(_async_rados), store(_store),
                                       source_zone(_source_zone),
                                       bucket_info(_bucket_info),
                                       key(_key),
                                       versioned_epoch(_versioned_epoch),
                                       copy_if_newer(_if_newer), req(NULL) {}


  ~RGWFetchRemoteObjCR() {
    request_cleanup();
  }

  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request() {
    req = new RGWAsyncFetchRemoteObj(this, stack->create_completion_notifier(), store, source_zone, bucket_info,
                                     key, versioned_epoch, copy_if_newer);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWAsyncRemoveObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  string source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;
  string owner;
  string owner_display_name;
  bool versioned;
  uint64_t versioned_epoch;
  string marker_version_id;

  bool del_if_older;
  ceph::real_time timestamp;

protected:
  int _send_request();
public:
  RGWAsyncRemoveObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                         const string& _source_zone,
                         RGWBucketInfo& _bucket_info,
                         const rgw_obj_key& _key,
                         const string& _owner,
                         const string& _owner_display_name,
                         bool _versioned,
                         uint64_t _versioned_epoch,
                         bool _delete_marker,
                         bool _if_older,
                         real_time& _timestamp) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                                      source_zone(_source_zone),
                                                      bucket_info(_bucket_info),
                                                      key(_key),
                                                      owner(_owner),
                                                      owner_display_name(_owner_display_name),
                                                      versioned(_versioned),
                                                      versioned_epoch(_versioned_epoch),
                                                      del_if_older(_if_older),
                                                      timestamp(_timestamp) {
    if (_delete_marker) {
      marker_version_id = key.instance;
    }
  }
};

class RGWRemoveObjCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  string source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;
  bool versioned;
  uint64_t versioned_epoch;
  bool delete_marker;
  string owner;
  string owner_display_name;

  bool del_if_older;
  real_time timestamp;

  RGWAsyncRemoveObj *req;

public:
  RGWRemoveObjCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                      const string& _source_zone,
                      RGWBucketInfo& _bucket_info,
                      const rgw_obj_key& _key,
                      bool _versioned,
                      uint64_t _versioned_epoch,
                      string *_owner,
                      string *_owner_display_name,
                      bool _delete_marker,
                      real_time *_timestamp) : RGWSimpleCoroutine(_store->ctx()), cct(_store->ctx()),
                                       async_rados(_async_rados), store(_store),
                                       source_zone(_source_zone),
                                       bucket_info(_bucket_info),
                                       key(_key),
                                       versioned(_versioned),
                                       versioned_epoch(_versioned_epoch),
                                       delete_marker(_delete_marker), req(NULL) {
    del_if_older = (_timestamp != NULL);
    if (_timestamp) {
      timestamp = *_timestamp;
    }

    if (_owner) {
      owner = *_owner;
    }

    if (_owner_display_name) {
      owner_display_name = *_owner_display_name;
    }
  }
  ~RGWRemoveObjCR() {
    request_cleanup();
  }

  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request() {
    req = new RGWAsyncRemoveObj(this, stack->create_completion_notifier(), store, source_zone, bucket_info,
                                key, owner, owner_display_name, versioned, versioned_epoch,
                                delete_marker, del_if_older, timestamp);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWContinuousLeaseCR : public RGWCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;

  const rgw_bucket& pool;
  string oid;

  string lock_name;
  string cookie;

  int interval;

  Mutex lock;
  atomic_t going_down;
  bool locked;

  RGWCoroutine *caller;

  bool aborted;

public:
  RGWContinuousLeaseCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                       const rgw_bucket& _pool, const string& _oid,
                       const string& _lock_name, int _interval, RGWCoroutine *_caller) : RGWCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
                                        pool(_pool), oid(_oid), lock_name(_lock_name), interval(_interval),
                                        lock("RGWContimuousLeaseCR"), locked(false), caller(_caller), aborted(false) {
#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];

    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    cookie = buf;
  }

  int operate();

  bool is_locked() {
    Mutex::Locker l(lock);
    return locked;
  }

  void set_locked(bool status) {
    Mutex::Locker l(lock);
    locked = status;
  }

  void go_down() {
    going_down.set(1);
    wakeup();
  }

  void abort() {
    aborted = true;
  }
};

class RGWRadosTimelogAddCR : public RGWSimpleCoroutine {
  RGWRados *store;
  list<cls_log_entry> entries;

  string oid;

  RGWAioCompletionNotifier *cn;

public:
  RGWRadosTimelogAddCR(RGWRados *_store, const string& _oid,
		        const cls_log_entry& entry);
  ~RGWRadosTimelogAddCR();

  int send_request();
  int request_complete();
};

class RGWAsyncStatObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  rgw_obj obj;
  uint64_t *psize;
  real_time *pmtime;
  uint64_t *pepoch;
  RGWObjVersionTracker *objv_tracker;
protected:
  int _send_request() override;
public:
  RGWAsyncStatObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *store,
                  const rgw_obj& obj, uint64_t *psize = nullptr,
                  real_time *pmtime = nullptr, uint64_t *pepoch = nullptr,
                  RGWObjVersionTracker *objv_tracker = nullptr)
	  : RGWAsyncRadosRequest(caller, cn), store(store), obj(obj), psize(psize),
	  pmtime(pmtime), pepoch(pepoch), objv_tracker(objv_tracker) {}
};

class RGWStatObjCR : public RGWSimpleCoroutine {
  RGWRados *store;
  RGWAsyncRadosProcessor *async_rados;
  rgw_obj obj;
  uint64_t *psize;
  real_time *pmtime;
  uint64_t *pepoch;
  RGWObjVersionTracker *objv_tracker;
  RGWAsyncStatObj *req = nullptr;
 public:
  RGWStatObjCR(RGWAsyncRadosProcessor *async_rados, RGWRados *store,
	  const rgw_obj& obj, uint64_t *psize = nullptr,
	  real_time* pmtime = nullptr, uint64_t *pepoch = nullptr,
	  RGWObjVersionTracker *objv_tracker = nullptr);
  ~RGWStatObjCR() {
    request_cleanup();
  }
  void request_cleanup();

  int send_request() override;
  int request_complete() override;
};

#endif
