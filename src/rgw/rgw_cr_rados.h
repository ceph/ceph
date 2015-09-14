#ifndef CEPH_RGW_CR_RADOS_H
#define CEPH_RGW_CR_RADOS_H

#include "rgw_coroutine.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"

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

    bool _enqueue(RGWAsyncRadosRequest *req);
    void _dequeue(RGWAsyncRadosRequest *req) {
      assert(0);
    }
    bool _empty();
    RGWAsyncRadosRequest *_dequeue();
    using ThreadPool::WorkQueue<RGWAsyncRadosRequest>::_process;
    void _process(RGWAsyncRadosRequest *req);
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
  int _send_request();
public:
  RGWAsyncGetSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store, RGWObjectCtx *_obj_ctx,
                       RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
                       bufferlist *_pbl, off_t _ofs, off_t _end);
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
  int _send_request();
public:
  RGWAsyncPutSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store,
                       RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj, bool _exclusive,
                       bufferlist& _bl, time_t _mtime = 0);
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
  RGWAsyncLockSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store,
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
  RGWAsyncUnlockSystemObj(RGWAioCompletionNotifier *cn, RGWRados *_store,
                        RGWObjVersionTracker *_objv_tracker, rgw_obj& _obj,
		        const string& _name, const string& _cookie);
};


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
      return -EIO;
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
		      rgw_bucket& _pool, const string& _oid,
		      const string& _marker,
		      map<string, bufferlist> *_entries, int _max_entries);
  ~RGWRadosGetOmapKeysCR();

  int send_request();

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
		      uint32_t _duration);
  ~RGWSimpleRadosLockCR();

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
		      rgw_bucket& _pool, const string& _oid, const string& _lock_name,
		      const string& _cookie);
  ~RGWSimpleRadosUnlockCR();

  int send_request();
  int request_complete();
};

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
  RGWOmapAppend(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store, rgw_bucket& _pool, const string& _oid);
  int operate();
  void flush_pending();
  void append(const string& s);
  void finish();
};

#endif
