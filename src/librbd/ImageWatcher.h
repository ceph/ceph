// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGE_WATCHER_H
#define CEPH_LIBRBD_IMAGE_WATCHER_H

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <boost/function.hpp>
#include "include/assert.h"

class entity_name_t;
class Finisher;
class SafeTimer;

namespace librbd {

  class AioCompletion;
  class ImageCtx;

  struct RemoteAsyncRequest {
    uint64_t gid;
    uint64_t handle;
    uint64_t request_id;

    RemoteAsyncRequest() : gid(), handle(), request_id() {}
    RemoteAsyncRequest(uint64_t gid_, uint64_t handle_, uint64_t request_id_)
      : gid(gid_), handle(handle_), request_id(request_id_) {}

    inline bool operator<(const RemoteAsyncRequest &rhs) const {
      if (gid != rhs.gid) {
	return gid < rhs.gid;
      } else if (handle != rhs.handle) {
	return handle < rhs.handle;
      } else {
	return request_id < rhs.request_id;
      }
    }
  };

  class ImageWatcher {
  public:

    ImageWatcher(ImageCtx& image_ctx);
    ~ImageWatcher();

    bool is_lock_supported() const;
    bool is_lock_owner() const;

    int register_watch();
    int unregister_watch();

    int try_lock();
    int request_lock(const boost::function<int(AioCompletion*)>& restart_op,
		     AioCompletion* c);
    int unlock();

    void assert_header_locked(librados::ObjectWriteOperation *op);

    int notify_flatten(uint64_t request_id, ProgressContext &prog_ctx);
    int notify_resize(uint64_t request_id, uint64_t size,
		      ProgressContext &prog_ctx);
    int notify_snap_create(const std::string &snap_name);

    static void notify_header_update(librados::IoCtx &io_ctx,
				     const std::string &oid);

  private:

    enum LockOwnerState {
      LOCK_OWNER_STATE_NOT_LOCKED,
      LOCK_OWNER_STATE_LOCKED,
      LOCK_OWNER_STATE_RELEASING
    };

    enum WatchState {
      WATCH_STATE_UNREGISTERED,
      WATCH_STATE_REGISTERED,
      WATCH_STATE_ERROR
    };

    typedef std::pair<Context *, ProgressContext *> AsyncRequest;
    typedef std::pair<boost::function<int(AioCompletion *)>,
		      AioCompletion *> AioRequest;

    struct WatchCtx : public librados::WatchCtx2 {
      ImageWatcher &image_watcher;

      WatchCtx(ImageWatcher &parent) : image_watcher(parent) {}

      virtual void handle_notify(uint64_t notify_id,
                                 uint64_t handle,
				 uint64_t notifier_id,
                                 bufferlist& bl);
      virtual void handle_error(uint64_t handle, int err);
    };

    class RemoteProgressContext : public ProgressContext {
    public:
      RemoteProgressContext(ImageWatcher &image_watcher,
			    const RemoteAsyncRequest &remote_async_request)
        : m_image_watcher(image_watcher),
          m_remote_async_request(remote_async_request)
      {
      }

      virtual int update_progress(uint64_t offset, uint64_t total) {
	m_image_watcher.schedule_async_progress(m_remote_async_request, offset,
						total);
        return 0;
      }

    private:
      ImageWatcher &m_image_watcher;
      RemoteAsyncRequest m_remote_async_request;
    };

    class RemoteContext : public Context {
    public:
      RemoteContext(ImageWatcher &image_watcher,
		    const RemoteAsyncRequest &remote_async_request,
		    RemoteProgressContext *prog_ctx)
        : m_image_watcher(image_watcher),
          m_remote_async_request(remote_async_request), m_prog_ctx(prog_ctx)
      {
      }

      ~RemoteContext() {
        delete m_prog_ctx;
      }

      virtual void finish(int r);

    private:
      ImageWatcher &m_image_watcher;
      RemoteAsyncRequest m_remote_async_request;
      RemoteProgressContext *m_prog_ctx;
    };

    ImageCtx &m_image_ctx;

    RWLock m_watch_lock;
    WatchCtx m_watch_ctx;
    uint64_t m_watch_handle;
    WatchState m_watch_state;

    LockOwnerState m_lock_owner_state;

    Finisher *m_finisher;

    Mutex m_timer_lock;
    SafeTimer *m_timer;

    RWLock m_async_request_lock;
    std::map<uint64_t, AsyncRequest> m_async_requests;
    std::set<RemoteAsyncRequest> m_async_pending;
    std::set<RemoteAsyncRequest> m_async_progress;

    Mutex m_aio_request_lock;
    std::vector<AioRequest> m_aio_requests;
    Context *m_retry_aio_context;

    std::string encode_lock_cookie() const;
    static bool decode_lock_cookie(const std::string &cookie, uint64_t *handle);

    int get_lock_owner_info(entity_name_t *locker, std::string *cookie,
			    std::string *address, uint64_t *handle);
    int lock();
    void release_lock();
    bool try_request_lock();
    void finalize_request_lock();
    void finalize_header_update();

    void schedule_retry_aio_requests(bool use_timer);
    void cancel_retry_aio_requests();
    void finalize_retry_aio_requests();
    void retry_aio_requests();

    void schedule_cancel_async_requests();
    void cancel_async_requests();

    void encode_async_request(uint64_t request_id, bufferlist &bl);
    static int decode_response_code(bufferlist &bl);

    void notify_released_lock();
    void notify_request_lock();
    int notify_lock_owner(bufferlist &bl, bufferlist &response);

    int notify_async_request(uint64_t async_request_id, bufferlist &in,
			     ProgressContext& prog_ctx);
    void notify_request_leadership();

    void schedule_async_progress(const RemoteAsyncRequest &remote_async_request,
				 uint64_t offset, uint64_t total);
    int notify_async_progress(const RemoteAsyncRequest &remote_async_request,
			      uint64_t offset, uint64_t total);
    void schedule_async_complete(const RemoteAsyncRequest &remote_async_request,
				 int r);
    int notify_async_complete(const RemoteAsyncRequest &remote_async_request,
			      int r);

    void handle_header_update();
    void handle_acquired_lock();
    void handle_released_lock();
    void handle_request_lock(bufferlist *out);

    void handle_async_progress(bufferlist::iterator iter);
    void handle_async_complete(bufferlist::iterator iter);
    void handle_flatten(bufferlist::iterator iter, bufferlist *out);
    void handle_resize(bufferlist::iterator iter, bufferlist *out);
    void handle_snap_create(bufferlist::iterator iter, bufferlist *out);
    void handle_unknown_op(bufferlist *out);
    void handle_notify(uint64_t notify_id, uint64_t handle, bufferlist &bl);
    void handle_error(uint64_t cookie, int err);
    void acknowledge_notify(uint64_t notify_id, uint64_t handle,
			    bufferlist &out);

    void schedule_reregister_watch();
    void reregister_watch();
  };

} // namespace librbd

#endif // CEPH_LIBRBD_IMAGE_WATCHER_H
