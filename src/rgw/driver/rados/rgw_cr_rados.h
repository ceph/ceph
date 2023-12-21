// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <boost/intrusive_ptr.hpp>
#include "include/ceph_assert.h"
#include "rgw_coroutine.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"

#include <atomic>
#include "common/ceph_time.h"

#include "services/svc_sys_obj.h"
#include "services/svc_bucket.h"

struct rgw_http_param_pair;
class RGWRESTConn;

class RGWAsyncRadosRequest : public RefCountedObject {
  RGWCoroutine *caller;
  RGWAioCompletionNotifier *notifier;

  int retcode;

  ceph::mutex lock = ceph::make_mutex("RGWAsyncRadosRequest::lock");

protected:
  virtual int _send_request(const DoutPrefixProvider *dpp) = 0;
public:
  RGWAsyncRadosRequest(RGWCoroutine *_caller, RGWAioCompletionNotifier *_cn)
    : caller(_caller), notifier(_cn), retcode(0) {
  }
  ~RGWAsyncRadosRequest() override {
    if (notifier) {
      notifier->put();
    }
  }

  void send_request(const DoutPrefixProvider *dpp) {
    get();
    retcode = _send_request(dpp);
    {
      std::lock_guard l{lock};
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
      std::lock_guard l{lock};
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
  std::deque<RGWAsyncRadosRequest *> m_req_queue;
  std::atomic<bool> going_down = { false };
protected:
  CephContext *cct;
  ThreadPool m_tp;
  Throttle req_throttle;

  struct RGWWQ : public DoutPrefixProvider, public ThreadPool::WorkQueue<RGWAsyncRadosRequest> {
    RGWAsyncRadosProcessor *processor;
    RGWWQ(RGWAsyncRadosProcessor *p,
	  ceph::timespan timeout, ceph::timespan suicide_timeout,
	  ThreadPool *tp)
      : ThreadPool::WorkQueue<RGWAsyncRadosRequest>("RGWWQ", timeout, suicide_timeout, tp), processor(p) {}

    bool _enqueue(RGWAsyncRadosRequest *req) override;
    void _dequeue(RGWAsyncRadosRequest *req) override {
      ceph_abort();
    }
    bool _empty() override;
    RGWAsyncRadosRequest *_dequeue() override;
    using ThreadPool::WorkQueue<RGWAsyncRadosRequest>::_process;
    void _process(RGWAsyncRadosRequest *req, ThreadPool::TPHandle& handle) override;
    void _dump_queue();
    void _clear() override {
      ceph_assert(processor->m_req_queue.empty());
    }

  CephContext *get_cct() const { return processor->cct; }
  unsigned get_subsys() const { return ceph_subsys_rgw; }
  std::ostream& gen_prefix(std::ostream& out) const { return out << "rgw async rados processor: ";}

  } req_wq;

public:
  RGWAsyncRadosProcessor(CephContext *_cct, int num_threads);
  ~RGWAsyncRadosProcessor() {}
  void start();
  void stop();
  void handle_request(const DoutPrefixProvider *dpp, RGWAsyncRadosRequest *req);
  void queue(RGWAsyncRadosRequest *req);

  bool is_going_down() {
    return going_down;
  }

};

template <class P>
class RGWSimpleWriteOnlyAsyncCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;

  P params;
  const DoutPrefixProvider *dpp;

  class Request : public RGWAsyncRadosRequest {
    rgw::sal::RadosStore* store;
    P params;
    const DoutPrefixProvider *dpp;
  protected:
    int _send_request(const DoutPrefixProvider *dpp) override;
  public:
    Request(RGWCoroutine *caller,
            RGWAioCompletionNotifier *cn,
            rgw::sal::RadosStore* store,
            const P& _params,
            const DoutPrefixProvider *dpp) : RGWAsyncRadosRequest(caller, cn),
                                store(store),
                                params(_params),
                                dpp(dpp) {}
  } *req{nullptr};

 public:
  RGWSimpleWriteOnlyAsyncCR(RGWAsyncRadosProcessor *_async_rados,
			    rgw::sal::RadosStore* _store,
			    const P& _params,
                            const DoutPrefixProvider *_dpp) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
                                                store(_store),
				                params(_params),
                                                dpp(_dpp) {}

  ~RGWSimpleWriteOnlyAsyncCR() override {
    request_cleanup();
  }
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new Request(this,
                      stack->create_completion_notifier(),
                      store,
                      params,
                      dpp);

    async_rados->queue(req);
    return 0;
  }
  int request_complete() override {
    return req->get_ret_status();
  }
};


template <class P, class R>
class RGWSimpleAsyncCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;

  P params;
  std::shared_ptr<R> result;
  const DoutPrefixProvider *dpp;

  class Request : public RGWAsyncRadosRequest {
    rgw::sal::RadosStore* store;
    P params;
    std::shared_ptr<R> result;
    const DoutPrefixProvider *dpp;
  protected:
    int _send_request(const DoutPrefixProvider *dpp) override;
  public:
    Request(const DoutPrefixProvider *dpp,
            RGWCoroutine *caller,
            RGWAioCompletionNotifier *cn,
            rgw::sal::RadosStore* _store,
            const P& _params,
            std::shared_ptr<R>& _result,
            const DoutPrefixProvider *_dpp) : RGWAsyncRadosRequest(caller, cn),
                                           store(_store),
                                           params(_params),
                                           result(_result),
                                           dpp(_dpp) {}
  } *req{nullptr};

 public:
  RGWSimpleAsyncCR(RGWAsyncRadosProcessor *_async_rados,
                   rgw::sal::RadosStore* _store,
                   const P& _params,
                   std::shared_ptr<R>& _result,
                   const DoutPrefixProvider *_dpp) : RGWSimpleCoroutine(_store->ctx()),
                                                  async_rados(_async_rados),
                                                  store(_store),
                                                  params(_params),
                                                  result(_result),
                                                  dpp(_dpp) {}

  ~RGWSimpleAsyncCR() override {
    request_cleanup();
  }
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new Request(dpp,
                      this,
                      stack->create_completion_notifier(),
                      store,
                      params,
                      result,
                      dpp);

    async_rados->queue(req);
    return 0;
  }
  int request_complete() override {
    return req->get_ret_status();
  }
};

class RGWGenericAsyncCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;


public:
  class Action {
  public:
    virtual ~Action() {}
    virtual int operate() = 0;
  };

private:
  std::shared_ptr<Action> action;

  class Request : public RGWAsyncRadosRequest {
    std::shared_ptr<Action> action;
  protected:
    int _send_request(const DoutPrefixProvider *dpp) override {
      if (!action) {
	return 0;
      }
      return action->operate();
    }
  public:
    Request(const DoutPrefixProvider *dpp,
            RGWCoroutine *caller,
            RGWAioCompletionNotifier *cn,
            std::shared_ptr<Action>& _action) : RGWAsyncRadosRequest(caller, cn),
                                           action(_action) {}
  } *req{nullptr};

 public:
  RGWGenericAsyncCR(CephContext *_cct,
		    RGWAsyncRadosProcessor *_async_rados,
		    std::shared_ptr<Action>& _action) : RGWSimpleCoroutine(_cct),
                                                  async_rados(_async_rados),
                                                  action(_action) {}
  template<typename T>
  RGWGenericAsyncCR(CephContext *_cct,
		    RGWAsyncRadosProcessor *_async_rados,
		    std::shared_ptr<T>& _action) : RGWSimpleCoroutine(_cct),
                                                  async_rados(_async_rados),
                                                  action(std::static_pointer_cast<Action>(_action)) {}

  ~RGWGenericAsyncCR() override {
    request_cleanup();
  }
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new Request(dpp, this,
                      stack->create_completion_notifier(),
                      action);

    async_rados->queue(req);
    return 0;
  }
  int request_complete() override {
    return req->get_ret_status();
  }
};


class RGWAsyncGetSystemObj : public RGWAsyncRadosRequest {
  const DoutPrefixProvider *dpp;
  RGWSI_SysObj* svc_sysobj;
  rgw_raw_obj obj;
  const bool want_attrs;
  const bool raw_attrs;
protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncGetSystemObj(const DoutPrefixProvider *dpp, 
                       RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWSI_SysObj *_svc,
                       RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
                       bool want_attrs, bool raw_attrs);

  bufferlist bl;
  std::map<std::string, bufferlist> attrs;
  RGWObjVersionTracker objv_tracker;
};

class RGWAsyncPutSystemObj : public RGWAsyncRadosRequest {
  const DoutPrefixProvider *dpp;
  RGWSI_SysObj *svc;
  rgw_raw_obj obj;
  bool exclusive;
  bufferlist bl;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncPutSystemObj(const DoutPrefixProvider *dpp, RGWCoroutine *caller, 
                       RGWAioCompletionNotifier *cn, RGWSI_SysObj *_svc,
                       RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
                       bool _exclusive, bufferlist _bl);

  RGWObjVersionTracker objv_tracker;
};

class RGWAsyncPutSystemObjAttrs : public RGWAsyncRadosRequest {
  const DoutPrefixProvider *dpp;
  RGWSI_SysObj *svc;
  rgw_raw_obj obj;
  std::map<std::string, bufferlist> attrs;
  bool exclusive;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncPutSystemObjAttrs(const DoutPrefixProvider *dpp, RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWSI_SysObj *_svc,
			    RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
			    std::map<std::string, bufferlist> _attrs, bool exclusive);

  RGWObjVersionTracker objv_tracker;
};

class RGWAsyncLockSystemObj : public RGWAsyncRadosRequest {
  rgw::sal::RadosStore* store;
  rgw_raw_obj obj;
  std::string lock_name;
  std::string cookie;
  uint32_t duration_secs;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncLockSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RadosStore* _store,
                        RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
		        const std::string& _name, const std::string& _cookie, uint32_t _duration_secs);
};

class RGWAsyncUnlockSystemObj : public RGWAsyncRadosRequest {
  rgw::sal::RadosStore* store;
  rgw_raw_obj obj;
  std::string lock_name;
  std::string cookie;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncUnlockSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RadosStore* _store,
                        RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
		        const std::string& _name, const std::string& _cookie);
};

template <class T>
class RGWSimpleRadosReadCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider* dpp;
  rgw::sal::RadosStore* store;
  rgw_raw_obj obj;
  T* result;
  /// on ENOENT, call handle_data() with an empty object instead of failing
  const bool empty_on_enoent;
  RGWObjVersionTracker* objv_tracker;

  T val;
  rgw_rados_ref ref;
  ceph::buffer::list bl;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWSimpleRadosReadCR(const DoutPrefixProvider* dpp,
		       rgw::sal::RadosStore* store,
		       const rgw_raw_obj& obj,
		       T* result, bool empty_on_enoent = true,
		       RGWObjVersionTracker* objv_tracker = nullptr)
    : RGWSimpleCoroutine(store->ctx()), dpp(dpp), store(store),
      obj(obj), result(result), empty_on_enoent(empty_on_enoent),
      objv_tracker(objv_tracker) {
    if (!result) {
      result = &val;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) {
    int r = store->getRados()->get_raw_obj_ref(dpp, obj, &ref);
    if (r < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed to get ref for (" << obj << ") ret="
			 << r << dendl;
      return r;
    }

    set_status() << "sending request";

    librados::ObjectReadOperation op;
    if (objv_tracker) {
      objv_tracker->prepare_op_for_read(&op);
    }

    op.read(0, -1, &bl, nullptr);

    cn = stack->create_completion_notifier();
    return ref.ioctx.aio_operate(ref.obj.oid, cn->completion(), &op, nullptr);
  }

  int request_complete() {
    int ret = cn->completion()->get_return_value();
    set_status() << "request complete; ret=" << ret;

    if (ret == -ENOENT && empty_on_enoent) {
      *result = T();
    } else {
      if (ret < 0) {
	return ret;
      }
      try {
	auto iter = bl.cbegin();
	if (iter.end()) {
	  // allow successful reads with empty buffers. ReadSyncStatus coroutines
	  // depend on this to be able to read without locking, because the
	  // cls lock from InitSyncStatus will create an empty object if it didn't
	  // exist
	  *result = T();
	} else {
	  decode(*result, iter);
	}
      } catch (buffer::error& err) {
	return -EIO;
      }
    }

    return handle_data(*result);
  }

  virtual int handle_data(T& data) {
    return 0;
  }
};

class RGWSimpleRadosReadAttrsCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider* dpp;
  rgw::sal::RadosStore* const store;

  const rgw_raw_obj obj;
  std::map<std::string, bufferlist>* const pattrs;
  const bool raw_attrs;
  RGWObjVersionTracker* const objv_tracker;

  rgw_rados_ref ref;
  std::map<std::string, bufferlist> unfiltered_attrs;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWSimpleRadosReadAttrsCR(const DoutPrefixProvider* dpp,
			    rgw::sal::RadosStore* store,
                            rgw_raw_obj obj,
			    std::map<std::string, bufferlist>* pattrs,
                            bool raw_attrs,
			    RGWObjVersionTracker* objv_tracker = nullptr)
    : RGWSimpleCoroutine(store->ctx()), dpp(dpp), store(store),
      obj(std::move(obj)), pattrs(pattrs), raw_attrs(raw_attrs),
      objv_tracker(objv_tracker) {}

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

template <class T>
class RGWSimpleRadosWriteCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider* dpp;
  rgw::sal::RadosStore* const store;
  rgw_raw_obj obj;
  RGWObjVersionTracker* objv_tracker;
  bool exclusive;

  bufferlist bl;
  rgw_rados_ref ref;
  std::map<std::string, bufferlist> unfiltered_attrs;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;


public:
  RGWSimpleRadosWriteCR(const DoutPrefixProvider* dpp,
			rgw::sal::RadosStore* const store,
			rgw_raw_obj obj, const T& data,
			RGWObjVersionTracker* objv_tracker = nullptr,
			bool exclusive = false)
    : RGWSimpleCoroutine(store->ctx()), dpp(dpp), store(store),
      obj(std::move(obj)), objv_tracker(objv_tracker), exclusive(exclusive) {
    encode(data, bl);
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    int r = store->getRados()->get_raw_obj_ref(dpp, obj, &ref);
    if (r < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed to get ref for (" << obj << ") ret="
			 << r << dendl;
      return r;
    }

    set_status() << "sending request";

    librados::ObjectWriteOperation op;
    if (exclusive) {
      op.create(true);
    }
    if (objv_tracker) {
      objv_tracker->prepare_op_for_write(&op);
    }
    op.write_full(bl);

    cn = stack->create_completion_notifier();
    return ref.ioctx.aio_operate(ref.obj.oid, cn->completion(), &op);
  }

  int request_complete() override {
    int ret = cn->completion()->get_return_value();
    set_status() << "request complete; ret=" << ret;
    if (ret >= 0 && objv_tracker) {
      objv_tracker->apply_write();
    }
    return ret;
  }
};

class RGWSimpleRadosWriteAttrsCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider* dpp;
  rgw::sal::RadosStore* const store;
  RGWObjVersionTracker* objv_tracker;
  rgw_raw_obj obj;
  std::map<std::string, bufferlist> attrs;
  bool exclusive;

  rgw_rados_ref ref;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;


public:
  RGWSimpleRadosWriteAttrsCR(const DoutPrefixProvider* dpp,
			     rgw::sal::RadosStore* const store,
                             rgw_raw_obj obj,
                             std::map<std::string, bufferlist> attrs,
                             RGWObjVersionTracker* objv_tracker = nullptr,
                             bool exclusive = false)
			     : RGWSimpleCoroutine(store->ctx()), dpp(dpp),
			       store(store), objv_tracker(objv_tracker),
			       obj(std::move(obj)), attrs(std::move(attrs)),
			       exclusive(exclusive) {}

  int send_request(const DoutPrefixProvider *dpp) override {
    int r = store->getRados()->get_raw_obj_ref(dpp, obj, &ref);
    if (r < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed to get ref for (" << obj << ") ret="
			 << r << dendl;
      return r;
    }

    set_status() << "sending request";

    librados::ObjectWriteOperation op;
    if (exclusive) {
      op.create(true);
    }
    if (objv_tracker) {
      objv_tracker->prepare_op_for_write(&op);
    }

    for (const auto& [name, bl] : attrs) {
      if (!bl.length())
	continue;
      op.setxattr(name.c_str(), bl);
    }

    cn = stack->create_completion_notifier();
    if (!op.size()) {
      cn->cb();
      return 0;
    }

    return ref.ioctx.aio_operate(ref.obj.oid, cn->completion(), &op);
  }

  int request_complete() override {
    int ret = cn->completion()->get_return_value();
    set_status() << "request complete; ret=" << ret;
    if (ret >= 0 && objv_tracker) {
      objv_tracker->apply_write();
    }
    return ret;
  }
};

class RGWRadosSetOmapKeysCR : public RGWSimpleCoroutine {
  rgw::sal::RadosStore* store;
  std::map<std::string, bufferlist> entries;

  rgw_rados_ref ref;

  rgw_raw_obj obj;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosSetOmapKeysCR(rgw::sal::RadosStore* _store,
		      const rgw_raw_obj& _obj,
		      std::map<std::string, bufferlist>& _entries);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

class RGWRadosGetOmapKeysCR : public RGWSimpleCoroutine {
 public:
  struct Result {
    rgw_rados_ref ref;
    std::set<std::string> entries;
    bool more = false;
  };
  using ResultPtr = std::shared_ptr<Result>;

  RGWRadosGetOmapKeysCR(rgw::sal::RadosStore* _store, const rgw_raw_obj& _obj,
                        const std::string& _marker, int _max_entries,
                        ResultPtr result);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;

 private:
  rgw::sal::RadosStore* store;
  rgw_raw_obj obj;
  std::string marker;
  int max_entries;
  ResultPtr result;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
};

class RGWRadosGetOmapValsCR : public RGWSimpleCoroutine {
 public:
  struct Result {
    rgw_rados_ref ref;
    std::map<std::string, bufferlist> entries;
    bool more = false;
  };
  using ResultPtr = std::shared_ptr<Result>;

  RGWRadosGetOmapValsCR(rgw::sal::RadosStore* _store, const rgw_raw_obj& _obj,
                        const std::string& _marker, int _max_entries,
                        ResultPtr result);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;

 private:
  rgw::sal::RadosStore* store;
  rgw_raw_obj obj;
  std::string marker;
  int max_entries;
  ResultPtr result;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
};

class RGWRadosRemoveOmapKeysCR : public RGWSimpleCoroutine {
  rgw::sal::RadosStore* store;

  rgw_rados_ref ref;

  std::set<std::string> keys;

  rgw_raw_obj obj;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosRemoveOmapKeysCR(rgw::sal::RadosStore* _store,
		      const rgw_raw_obj& _obj,
		      const std::set<std::string>& _keys);

  int send_request(const DoutPrefixProvider *dpp) override;

  int request_complete() override;
};

class RGWRadosRemoveCR : public RGWSimpleCoroutine {
  rgw::sal::RadosStore* store;
  librados::IoCtx ioctx;
  const rgw_raw_obj obj;
  RGWObjVersionTracker* objv_tracker;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosRemoveCR(rgw::sal::RadosStore* store, const rgw_raw_obj& obj,
                   RGWObjVersionTracker* objv_tracker = nullptr);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

class RGWRadosRemoveOidCR : public RGWSimpleCoroutine {
  librados::IoCtx ioctx;
  const std::string oid;
  RGWObjVersionTracker* objv_tracker;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosRemoveOidCR(rgw::sal::RadosStore* store,
		      librados::IoCtx&& ioctx, std::string_view oid,
		      RGWObjVersionTracker* objv_tracker = nullptr);

  RGWRadosRemoveOidCR(rgw::sal::RadosStore* store,
		      rgw_rados_ref obj,
		      RGWObjVersionTracker* objv_tracker = nullptr);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

class RGWSimpleRadosLockCR : public RGWSimpleCoroutine {
    RGWAsyncRadosProcessor *async_rados;
    rgw::sal::RadosStore* store;
    std::string lock_name;
    std::string cookie;
    uint32_t duration;

    rgw_raw_obj obj;

    RGWAsyncLockSystemObj *req;

public:
  RGWSimpleRadosLockCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RadosStore* _store,
		      const rgw_raw_obj& _obj,
          const std::string& _lock_name,
		      const std::string& _cookie,
		      uint32_t _duration);
  ~RGWSimpleRadosLockCR() override {
    request_cleanup();
  }
  void request_cleanup() override;

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;

  static std::string gen_random_cookie(CephContext* cct) {
    static constexpr std::size_t COOKIE_LEN = 16;
    char buf[COOKIE_LEN + 1];
    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    return buf;
  }
};

class RGWSimpleRadosUnlockCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;
  std::string lock_name;
  std::string cookie;

  rgw_raw_obj obj;

  RGWAsyncUnlockSystemObj *req;

public:
  RGWSimpleRadosUnlockCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RadosStore* _store,
		      const rgw_raw_obj& _obj, 
                      const std::string& _lock_name,
		      const std::string& _cookie);
  ~RGWSimpleRadosUnlockCR() override {
    request_cleanup();
  }
  void request_cleanup() override;

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

#define OMAP_APPEND_MAX_ENTRIES_DEFAULT 100

class RGWOmapAppend : public RGWConsumerCR<std::string> {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;

  rgw_raw_obj obj;

  bool going_down;

  int num_pending_entries;
  std::list<std::string> pending_entries;

  std::map<std::string, bufferlist> entries;

  uint64_t window_size;
  uint64_t total_entries;
public:
  RGWOmapAppend(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RadosStore* _store,
                const rgw_raw_obj& _obj,
                uint64_t _window_size = OMAP_APPEND_MAX_ENTRIES_DEFAULT);
  int operate(const DoutPrefixProvider *dpp) override;
  void flush_pending();
  bool append(const std::string& s);
  bool finish();

  uint64_t get_total_entries() {
    return total_entries;
  }

  const rgw_raw_obj& get_obj() {
    return obj;
  }
};

class RGWShardedOmapCRManager {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;
  RGWCoroutine *op;

  int num_shards;

  std::vector<RGWOmapAppend *> shards;
public:
  RGWShardedOmapCRManager(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RadosStore* _store, RGWCoroutine *_op, int _num_shards, const rgw_pool& pool, const std::string& oid_prefix)
                      : async_rados(_async_rados),
		        store(_store), op(_op), num_shards(_num_shards) {
    shards.reserve(num_shards);
    for (int i = 0; i < num_shards; ++i) {
      char buf[oid_prefix.size() + 16];
      snprintf(buf, sizeof(buf), "%s.%d", oid_prefix.c_str(), i);
      RGWOmapAppend *shard = new RGWOmapAppend(async_rados, store, rgw_raw_obj(pool, buf));
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

  bool append(const std::string& entry, int shard_id) {
    return shards[shard_id]->append(entry);
  }
  bool finish() {
    bool success = true;
    for (auto& append_op : shards) {
      success &= (append_op->finish() && (!append_op->is_error()));
    }
    return success;
  }

  uint64_t get_total_entries(int shard_id) {
    return shards[shard_id]->get_total_entries();
  }
};

class RGWAsyncGetBucketInstanceInfo : public RGWAsyncRadosRequest {
  rgw::sal::RadosStore* store;
  rgw_bucket bucket;
  const DoutPrefixProvider *dpp;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncGetBucketInstanceInfo(RGWCoroutine *caller, RGWAioCompletionNotifier *cn,
                                rgw::sal::RadosStore* _store, const rgw_bucket& bucket,
                                const DoutPrefixProvider *dpp)
    : RGWAsyncRadosRequest(caller, cn), store(_store), bucket(bucket), dpp(dpp) {}

  RGWBucketInfo bucket_info;
  std::map<std::string, bufferlist> attrs;
};

class RGWAsyncPutBucketInstanceInfo : public RGWAsyncRadosRequest {
  rgw::sal::RadosStore* store;
  RGWBucketInfo& bucket_info;
  bool exclusive;
  real_time mtime;
  std::map<std::string, ceph::bufferlist>* attrs;
  const DoutPrefixProvider *dpp;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncPutBucketInstanceInfo(RGWCoroutine* caller,
				RGWAioCompletionNotifier* cn,
                                rgw::sal::RadosStore* store,
				RGWBucketInfo& bucket_info,
				bool exclusive,
				real_time mtime,
				std::map<std::string, ceph::bufferlist>* attrs,
                                const DoutPrefixProvider* dpp)
    : RGWAsyncRadosRequest(caller, cn), store(store), bucket_info(bucket_info),
      exclusive(exclusive), mtime(mtime), attrs(attrs), dpp(dpp) {}
};

class RGWGetBucketInstanceInfoCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;
  rgw_bucket bucket;
  RGWBucketInfo *bucket_info;
  std::map<std::string, bufferlist> *pattrs;
  const DoutPrefixProvider *dpp;

  RGWAsyncGetBucketInstanceInfo *req{nullptr};

public:
  // rgw_bucket constructor
  RGWGetBucketInstanceInfoCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RadosStore* _store,
                             const rgw_bucket& _bucket, RGWBucketInfo *_bucket_info,
                             std::map<std::string, bufferlist> *_pattrs, const DoutPrefixProvider *dpp)
    : RGWSimpleCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
      bucket(_bucket), bucket_info(_bucket_info), pattrs(_pattrs), dpp(dpp) {}
  ~RGWGetBucketInstanceInfoCR() override {
    request_cleanup();
  }
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncGetBucketInstanceInfo(this, stack->create_completion_notifier(), store, bucket, dpp);
    async_rados->queue(req);
    return 0;
  }
  int request_complete() override {
    if (bucket_info) {
      *bucket_info = std::move(req->bucket_info);
    }
    if (pattrs) {
      *pattrs = std::move(req->attrs);
    }
    return req->get_ret_status();
  }
};

class RGWPutBucketInstanceInfoCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;
  RGWBucketInfo& bucket_info;
  bool exclusive;
  real_time mtime;
  std::map<std::string, ceph::bufferlist>* attrs;
  const DoutPrefixProvider *dpp;

  RGWAsyncPutBucketInstanceInfo* req = nullptr;

public:
  // rgw_bucket constructor
  RGWPutBucketInstanceInfoCR(RGWAsyncRadosProcessor *async_rados,
			     rgw::sal::RadosStore* store,
			     RGWBucketInfo& bucket_info,
			     bool exclusive,
			     real_time mtime,
			     std::map<std::string, ceph::bufferlist>* attrs,
                             const DoutPrefixProvider *dpp)
    : RGWSimpleCoroutine(store->ctx()), async_rados(async_rados), store(store),
      bucket_info(bucket_info), exclusive(exclusive),
      mtime(mtime), attrs(attrs), dpp(dpp) {}
  ~RGWPutBucketInstanceInfoCR() override {
    request_cleanup();
  }
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = nullptr;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncPutBucketInstanceInfo(this,
					    stack->create_completion_notifier(),
					    store, bucket_info, exclusive,
					    mtime, attrs, dpp);
    async_rados->queue(req);
    return 0;
  }
  int request_complete() override {
    return req->get_ret_status();
  }
};

class RGWRadosBILogTrimCR : public RGWSimpleCoroutine {
  const RGWBucketInfo& bucket_info;
  int shard_id;
  const rgw::bucket_index_layout_generation generation;
  RGWRados::BucketShard bs;
  std::string start_marker;
  std::string end_marker;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
 public:
  RGWRadosBILogTrimCR(const DoutPrefixProvider *dpp,
                      rgw::sal::RadosStore* store, const RGWBucketInfo& bucket_info,
                      int shard_id,
		      const rgw::bucket_index_layout_generation& generation,
		      const std::string& start_marker,
                      const std::string& end_marker);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

class RGWAsyncFetchRemoteObj : public RGWAsyncRadosRequest {
  rgw::sal::RadosStore* store;
  rgw_zone_id source_zone;

  std::optional<rgw_user> user_id;

  rgw_bucket src_bucket;
  std::optional<rgw_placement_rule> dest_placement_rule;
  RGWBucketInfo dest_bucket_info;

  rgw_obj_key key;
  std::optional<rgw_obj_key> dest_key;
  std::optional<uint64_t> versioned_epoch;

  real_time src_mtime;

  bool copy_if_newer;
  std::shared_ptr<RGWFetchObjFilter> filter;
  bool stat_follow_olh;
  rgw_zone_set_entry source_trace_entry;
  rgw_zone_set zones_trace;
  PerfCounters* counters;
  const DoutPrefixProvider *dpp;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncFetchRemoteObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RadosStore* _store,
                         const rgw_zone_id& _source_zone,
                         std::optional<rgw_user>& _user_id,
                         const rgw_bucket& _src_bucket,
			 std::optional<rgw_placement_rule> _dest_placement_rule,
                         const RGWBucketInfo& _dest_bucket_info,
                         const rgw_obj_key& _key,
                         const std::optional<rgw_obj_key>& _dest_key,
                         std::optional<uint64_t> _versioned_epoch,
                         bool _if_newer,
                         std::shared_ptr<RGWFetchObjFilter> _filter,
                         bool _stat_follow_olh,
                         const rgw_zone_set_entry& source_trace_entry,
                         rgw_zone_set *_zones_trace,
                         PerfCounters* counters,
                         const DoutPrefixProvider *dpp)
    : RGWAsyncRadosRequest(caller, cn), store(_store),
      source_zone(_source_zone),
      user_id(_user_id),
      src_bucket(_src_bucket),
      dest_placement_rule(_dest_placement_rule),
      dest_bucket_info(_dest_bucket_info),
      key(_key),
      dest_key(_dest_key),
      versioned_epoch(_versioned_epoch),
      copy_if_newer(_if_newer),
      filter(_filter),
      stat_follow_olh(_stat_follow_olh),
      source_trace_entry(source_trace_entry),
      counters(counters),
      dpp(dpp)
  {
    if (_zones_trace) {
      zones_trace = *_zones_trace;
    }
  }
};

class RGWFetchRemoteObjCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;
  rgw_zone_id source_zone;

  std::optional<rgw_user> user_id;

  rgw_bucket src_bucket;
  std::optional<rgw_placement_rule> dest_placement_rule;
  RGWBucketInfo dest_bucket_info;

  rgw_obj_key key;
  std::optional<rgw_obj_key> dest_key;
  std::optional<uint64_t> versioned_epoch;

  real_time src_mtime;

  bool copy_if_newer;

  std::shared_ptr<RGWFetchObjFilter> filter;

  RGWAsyncFetchRemoteObj *req;
  bool stat_follow_olh;
  const rgw_zone_set_entry& source_trace_entry;
  rgw_zone_set *zones_trace;
  PerfCounters* counters;
  const DoutPrefixProvider *dpp;

public:
  RGWFetchRemoteObjCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RadosStore* _store,
                      const rgw_zone_id& _source_zone,
                      std::optional<rgw_user> _user_id,
                      const rgw_bucket& _src_bucket,
		      std::optional<rgw_placement_rule> _dest_placement_rule,
                      const RGWBucketInfo& _dest_bucket_info,
                      const rgw_obj_key& _key,
                      const std::optional<rgw_obj_key>& _dest_key,
                      std::optional<uint64_t> _versioned_epoch,
                      bool _if_newer,
                      std::shared_ptr<RGWFetchObjFilter> _filter,
                      bool _stat_follow_olh,
                      const rgw_zone_set_entry& source_trace_entry,
                      rgw_zone_set *_zones_trace,
                      PerfCounters* counters,
                      const DoutPrefixProvider *dpp)
    : RGWSimpleCoroutine(_store->ctx()), cct(_store->ctx()),
      async_rados(_async_rados), store(_store),
      source_zone(_source_zone),
      user_id(_user_id),
      src_bucket(_src_bucket),
      dest_placement_rule(_dest_placement_rule),
      dest_bucket_info(_dest_bucket_info),
      key(_key),
      dest_key(_dest_key),
      versioned_epoch(_versioned_epoch),
      copy_if_newer(_if_newer),
      filter(_filter),
      req(NULL),
      stat_follow_olh(_stat_follow_olh),
      source_trace_entry(source_trace_entry),
      zones_trace(_zones_trace), counters(counters), dpp(dpp) {}


  ~RGWFetchRemoteObjCR() override {
    request_cleanup();
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncFetchRemoteObj(this, stack->create_completion_notifier(), store,
    source_zone, user_id, src_bucket, dest_placement_rule, dest_bucket_info,
                                     key, dest_key, versioned_epoch, copy_if_newer, filter,
                                     stat_follow_olh, source_trace_entry, zones_trace, counters, dpp);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    return req->get_ret_status();
  }
};

class RGWAsyncStatRemoteObj : public RGWAsyncRadosRequest {
  rgw::sal::RadosStore* store;
  rgw_zone_id source_zone;

  rgw_bucket src_bucket;
  rgw_obj_key key;

  ceph::real_time *pmtime;
  uint64_t *psize;
  std::string *petag;
  std::map<std::string, bufferlist> *pattrs;
  std::map<std::string, std::string> *pheaders;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncStatRemoteObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RadosStore* _store,
                         const rgw_zone_id& _source_zone,
                         rgw_bucket& _src_bucket,
                         const rgw_obj_key& _key,
                         ceph::real_time *_pmtime,
                         uint64_t *_psize,
                         std::string *_petag,
                         std::map<std::string, bufferlist> *_pattrs,
                         std::map<std::string, std::string> *_pheaders) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                                      source_zone(_source_zone),
                                                      src_bucket(_src_bucket),
                                                      key(_key),
                                                      pmtime(_pmtime),
                                                      psize(_psize),
                                                      petag(_petag),
                                                      pattrs(_pattrs),
                                                      pheaders(_pheaders) {}
};

class RGWStatRemoteObjCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;
  rgw_zone_id source_zone;

  rgw_bucket src_bucket;
  rgw_obj_key key;

  ceph::real_time *pmtime;
  uint64_t *psize;
  std::string *petag;
  std::map<std::string, bufferlist> *pattrs;
  std::map<std::string, std::string> *pheaders;

  RGWAsyncStatRemoteObj *req;

public:
  RGWStatRemoteObjCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RadosStore* _store,
                      const rgw_zone_id& _source_zone,
                      rgw_bucket& _src_bucket,
                      const rgw_obj_key& _key,
                      ceph::real_time *_pmtime,
                      uint64_t *_psize,
                      std::string *_petag,
                      std::map<std::string, bufferlist> *_pattrs,
                      std::map<std::string, std::string> *_pheaders) : RGWSimpleCoroutine(_store->ctx()), cct(_store->ctx()),
                                       async_rados(_async_rados), store(_store),
                                       source_zone(_source_zone),
                                       src_bucket(_src_bucket),
                                       key(_key),
                                       pmtime(_pmtime),
                                       psize(_psize),
                                       petag(_petag),
                                       pattrs(_pattrs),
                                       pheaders(_pheaders),
                                       req(NULL) {}


  ~RGWStatRemoteObjCR() override {
    request_cleanup();
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncStatRemoteObj(this, stack->create_completion_notifier(), store, source_zone,
                                    src_bucket, key, pmtime, psize, petag, pattrs, pheaders);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    return req->get_ret_status();
  }
};

class RGWAsyncRemoveObj : public RGWAsyncRadosRequest {
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* store;
  rgw_zone_id source_zone;

  std::unique_ptr<rgw::sal::Bucket> bucket;
  std::unique_ptr<rgw::sal::Object> obj;

  std::string owner;
  std::string owner_display_name;
  bool versioned;
  uint64_t versioned_epoch;
  std::string marker_version_id;

  bool del_if_older;
  ceph::real_time timestamp;
  rgw_zone_set zones_trace;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncRemoveObj(const DoutPrefixProvider *_dpp, RGWCoroutine *caller, RGWAioCompletionNotifier *cn, 
                         rgw::sal::RadosStore* _store,
                         const rgw_zone_id& _source_zone,
                         RGWBucketInfo& _bucket_info,
                         const rgw_obj_key& _key,
                         const std::string& _owner,
                         const std::string& _owner_display_name,
                         bool _versioned,
                         uint64_t _versioned_epoch,
                         bool _delete_marker,
                         bool _if_older,
                         real_time& _timestamp,
                         rgw_zone_set* _zones_trace) : RGWAsyncRadosRequest(caller, cn), dpp(_dpp), store(_store),
                                                      source_zone(_source_zone),
                                                      owner(_owner),
                                                      owner_display_name(_owner_display_name),
                                                      versioned(_versioned),
                                                      versioned_epoch(_versioned_epoch),
                                                      del_if_older(_if_older),
                                                      timestamp(_timestamp) {
    if (_delete_marker) {
      marker_version_id = _key.instance;
    }

    if (_zones_trace) {
      zones_trace = *_zones_trace;
    }
    bucket = store->get_bucket(_bucket_info);
    obj = bucket->get_object(_key);
  }
};

class RGWRemoveObjCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RadosStore* store;
  rgw_zone_id source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;
  bool versioned;
  uint64_t versioned_epoch;
  bool delete_marker;
  std::string owner;
  std::string owner_display_name;

  bool del_if_older;
  real_time timestamp;

  RGWAsyncRemoveObj *req;
  
  rgw_zone_set *zones_trace;

public:
  RGWRemoveObjCR(const DoutPrefixProvider *_dpp, RGWAsyncRadosProcessor *_async_rados, rgw::sal::RadosStore* _store,
                      const rgw_zone_id& _source_zone,
                      RGWBucketInfo& _bucket_info,
                      const rgw_obj_key& _key,
                      bool _versioned,
                      uint64_t _versioned_epoch,
                      std::string *_owner,
                      std::string *_owner_display_name,
                      bool _delete_marker,
                      real_time *_timestamp,
                      rgw_zone_set *_zones_trace) : RGWSimpleCoroutine(_store->ctx()), dpp(_dpp), cct(_store->ctx()),
                                       async_rados(_async_rados), store(_store),
                                       source_zone(_source_zone),
                                       bucket_info(_bucket_info),
                                       key(_key),
                                       versioned(_versioned),
                                       versioned_epoch(_versioned_epoch),
                                       delete_marker(_delete_marker), req(NULL), zones_trace(_zones_trace) {
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
  ~RGWRemoveObjCR() override {
    request_cleanup();
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncRemoveObj(dpp, this, stack->create_completion_notifier(), store, source_zone, bucket_info,
                                key, owner, owner_display_name, versioned, versioned_epoch,
                                delete_marker, del_if_older, timestamp, zones_trace);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    return req->get_ret_status();
  }
};

/// \brief Collect average latency
///
/// Used in data sync to back off on concurrency when latency of lock
/// operations rises.
///
/// \warning This class is not thread safe. We do not use a mutex
/// because all coroutines spawned by RGWDataSyncCR share a single thread.
class LatencyMonitor {
  ceph::timespan total;
  std::uint64_t count = 0;

public:

  LatencyMonitor() = default;
  void add_latency(ceph::timespan latency) {
    total += latency;
    ++count;
  }

  ceph::timespan avg_latency() {
    using namespace std::literals;
    return count == 0 ? 0s : total / count;
  }
};

class RGWContinuousLeaseCR : public RGWCoroutine {
  RGWAsyncRadosProcessor* async_rados;
  rgw::sal::RadosStore* store;

  const rgw_raw_obj obj;

  const std::string lock_name;
  const std::string cookie{RGWSimpleRadosLockCR::gen_random_cookie(cct)};

  int interval;
  bool going_down{false};
  bool locked{false};
  
  const ceph::timespan interval_tolerance;
  const ceph::timespan ts_interval;

  RGWCoroutine* caller;

  bool aborted{false};
  
  ceph::coarse_mono_time last_renew_try_time;
  ceph::coarse_mono_time current_time;

  LatencyMonitor* latency;

public:
  RGWContinuousLeaseCR(RGWAsyncRadosProcessor* async_rados,
                       rgw::sal::RadosStore* _store,
                       rgw_raw_obj obj, std::string lock_name,
                       int interval, RGWCoroutine* caller,
		       LatencyMonitor* const latency)
    : RGWCoroutine(_store->ctx()), async_rados(async_rados), store(_store),
      obj(std::move(obj)), lock_name(std::move(lock_name)),
      interval(interval), interval_tolerance(ceph::make_timespan(9*interval/10)),
      ts_interval(ceph::make_timespan(interval)), caller(caller), latency(latency)
  {}

  virtual ~RGWContinuousLeaseCR() override;

  int operate(const DoutPrefixProvider *dpp) override;

  bool is_locked() const {
    if (ceph::coarse_mono_clock::now() - last_renew_try_time > ts_interval) {
      return false;
    }
    return locked;
  }

  void set_locked(bool status) {
    locked = status;
  }

  void go_down() {
    going_down = true;
    wakeup();
  }

  void abort() {
    aborted = true;
  }
};

class RGWRadosTimelogAddCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* store;
  std::list<cls_log_entry> entries;

  std::string oid;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosTimelogAddCR(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* _store, const std::string& _oid,
		        const cls_log_entry& entry);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

class RGWRadosTimelogTrimCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* store;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
 protected:
  std::string oid;
  real_time start_time;
  real_time end_time;
  std::string from_marker;
  std::string to_marker;

 public:
  RGWRadosTimelogTrimCR(const DoutPrefixProvider *dpp, 
                        rgw::sal::RadosStore* store, const std::string& oid,
                        const real_time& start_time, const real_time& end_time,
                        const std::string& from_marker,
                        const std::string& to_marker);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

// wrapper to update last_trim_marker on success
class RGWSyncLogTrimCR : public RGWRadosTimelogTrimCR {
  CephContext *cct;
  std::string *last_trim_marker;
 public:
  static constexpr const char* max_marker = "99999999";

  RGWSyncLogTrimCR(const DoutPrefixProvider *dpp,
                   rgw::sal::RadosStore* store, const std::string& oid,
                   const std::string& to_marker, std::string *last_trim_marker);
  int request_complete() override;
};

class RGWAsyncStatObj : public RGWAsyncRadosRequest {
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* store;
  RGWBucketInfo bucket_info;
  rgw_obj obj;
  uint64_t *psize;
  real_time *pmtime;
  uint64_t *pepoch;
  RGWObjVersionTracker *objv_tracker;
protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncStatObj(const DoutPrefixProvider *dpp, RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RadosStore* store,
                  const RGWBucketInfo& _bucket_info, const rgw_obj& obj, uint64_t *psize = nullptr,
                  real_time *pmtime = nullptr, uint64_t *pepoch = nullptr,
                  RGWObjVersionTracker *objv_tracker = nullptr)
	  : RGWAsyncRadosRequest(caller, cn), dpp(dpp), store(store), obj(obj), psize(psize),
	  pmtime(pmtime), pepoch(pepoch), objv_tracker(objv_tracker) {}
};

class RGWStatObjCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* store;
  RGWAsyncRadosProcessor *async_rados;
  RGWBucketInfo bucket_info;
  rgw_obj obj;
  uint64_t *psize;
  real_time *pmtime;
  uint64_t *pepoch;
  RGWObjVersionTracker *objv_tracker;
  RGWAsyncStatObj *req = nullptr;
 public:
  RGWStatObjCR(const DoutPrefixProvider *dpp, RGWAsyncRadosProcessor *async_rados, rgw::sal::RadosStore* store,
	  const RGWBucketInfo& _bucket_info, const rgw_obj& obj, uint64_t *psize = nullptr,
	  real_time* pmtime = nullptr, uint64_t *pepoch = nullptr,
	  RGWObjVersionTracker *objv_tracker = nullptr);
  ~RGWStatObjCR() override {
    request_cleanup();
  }
  void request_cleanup() override;

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

/// coroutine wrapper for IoCtx::aio_notify()
class RGWRadosNotifyCR : public RGWSimpleCoroutine {
  rgw::sal::RadosStore* const store;
  const rgw_raw_obj obj;
  bufferlist request;
  const uint64_t timeout_ms;
  bufferlist *response;
  rgw_rados_ref ref;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosNotifyCR(rgw::sal::RadosStore* store, const rgw_raw_obj& obj,
                   bufferlist& request, uint64_t timeout_ms,
                   bufferlist *response);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

class RGWDataPostNotifyCR : public RGWCoroutine {
  RGWRados *store;
  RGWHTTPManager& http_manager;
  bc::flat_map<int, bc::flat_set<rgw_data_notify_entry> >& shards;
  const char *source_zone;
  RGWRESTConn *conn;

public:
  RGWDataPostNotifyCR(RGWRados *_store, RGWHTTPManager& _http_manager, bc::flat_map<int,
                    bc::flat_set<rgw_data_notify_entry> >& _shards, const char *_zone, RGWRESTConn *_conn)
                    : RGWCoroutine(_store->ctx()), store(_store), http_manager(_http_manager),
                      shards(_shards), source_zone(_zone), conn(_conn) {}

  int operate(const DoutPrefixProvider* dpp) override;
};

