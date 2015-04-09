// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGE_WATCHER_H
#define CEPH_LIBRBD_IMAGE_WATCHER_H

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "librbd/WatchNotifyTypes.h"
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <boost/function.hpp>
#include "include/assert.h"

class entity_name_t;

namespace librbd {

  class AioCompletion;
  class ImageCtx;
  template <typename T> class TaskFinisher;

  class ImageWatcher {
  public:

    ImageWatcher(ImageCtx& image_ctx);
    ~ImageWatcher();

    bool is_lock_supported() const;
    bool is_lock_owner() const;

    int register_watch();
    int unregister_watch();

    int try_lock();
    void request_lock(const boost::function<void(AioCompletion*)>& restart_op,
		      AioCompletion* c);
    void prepare_unlock();
    void cancel_unlock();
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

    enum TaskCode {
      TASK_CODE_ACQUIRED_LOCK,
      TASK_CODE_REQUEST_LOCK,
      TASK_CODE_RELEASING_LOCK,
      TASK_CODE_RELEASED_LOCK,
      TASK_CODE_RETRY_AIO_REQUESTS,
      TASK_CODE_CANCEL_ASYNC_REQUESTS,
      TASK_CODE_REREGISTER_WATCH,
      TASK_CODE_ASYNC_REQUEST,
      TASK_CODE_ASYNC_PROGRESS
    };

    typedef std::pair<Context *, ProgressContext *> AsyncRequest;
    typedef std::pair<boost::function<void(AioCompletion *)>,
		      AioCompletion *> AioRequest;

    class Task {
    public:
      Task(TaskCode task_code) : m_task_code(task_code) {}
      Task(TaskCode task_code, const WatchNotify::AsyncRequestId &id)
        : m_task_code(task_code), m_async_request_id(id) {}

      inline bool operator<(const Task& rhs) const {
        if (m_task_code != rhs.m_task_code) {
          return m_task_code < rhs.m_task_code;
        } else if ((m_task_code == TASK_CODE_ASYNC_REQUEST ||
                    m_task_code == TASK_CODE_ASYNC_PROGRESS) &&
                   m_async_request_id != rhs.m_async_request_id) {
          return m_async_request_id < rhs.m_async_request_id;
        }
        return false;
      }
    private:
      TaskCode m_task_code;
      WatchNotify::AsyncRequestId m_async_request_id;
    };

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
			    const WatchNotify::AsyncRequestId &id)
        : m_image_watcher(image_watcher), m_async_request_id(id)
      {
      }

      virtual int update_progress(uint64_t offset, uint64_t total) {
	m_image_watcher.schedule_async_progress(m_async_request_id, offset,
						total);
        return 0;
      }

    private:
      ImageWatcher &m_image_watcher;
      WatchNotify::AsyncRequestId m_async_request_id;
    };

    class RemoteContext : public Context {
    public:
      RemoteContext(ImageWatcher &image_watcher,
		    const WatchNotify::AsyncRequestId &id,
		    RemoteProgressContext *prog_ctx)
        : m_image_watcher(image_watcher), m_async_request_id(id),
	  m_prog_ctx(prog_ctx)
      {
      }

      ~RemoteContext() {
        delete m_prog_ctx;
      }

      virtual void finish(int r);

    private:
      ImageWatcher &m_image_watcher;
      WatchNotify::AsyncRequestId m_async_request_id;
      RemoteProgressContext *m_prog_ctx;
    };

    struct HandlePayloadVisitor : public boost::static_visitor<void> {
      ImageWatcher *image_watcher;
      uint64_t notify_id;
      uint64_t handle;

      HandlePayloadVisitor(ImageWatcher *image_watcher_, uint64_t notify_id_,
			   uint64_t handle_)
	: image_watcher(image_watcher_), notify_id(notify_id_), handle(handle_)
      {
      }

      inline void operator()(const WatchNotify::HeaderUpdatePayload &payload) const {
	bufferlist out;
	image_watcher->handle_payload(payload, &out);
	image_watcher->acknowledge_notify(notify_id, handle, out);
      }

      template <typename Payload>
      inline void operator()(const Payload &payload) const {
	bufferlist out;
	image_watcher->handle_payload(payload, &out);
	image_watcher->acknowledge_notify(notify_id, handle, out);
      }
    };

    ImageCtx &m_image_ctx;

    RWLock m_watch_lock;
    WatchCtx m_watch_ctx;
    uint64_t m_watch_handle;
    WatchState m_watch_state;

    LockOwnerState m_lock_owner_state;

    TaskFinisher<Task> *m_task_finisher;

    RWLock m_async_request_lock;
    std::map<WatchNotify::AsyncRequestId, AsyncRequest> m_async_requests;
    std::set<WatchNotify::AsyncRequestId> m_async_pending;

    Mutex m_aio_request_lock;
    std::vector<AioRequest> m_aio_requests;

    Mutex m_owner_client_id_lock;
    WatchNotify::ClientId m_owner_client_id;

    std::string encode_lock_cookie() const;
    static bool decode_lock_cookie(const std::string &cookie, uint64_t *handle);

    int get_lock_owner_info(entity_name_t *locker, std::string *cookie,
			    std::string *address, uint64_t *handle);
    int lock();
    bool release_lock();
    bool try_request_lock();

    void schedule_retry_aio_requests(bool use_timer);
    void retry_aio_requests();

    void schedule_cancel_async_requests();
    void cancel_async_requests();

    WatchNotify::ClientId get_client_id();

    void notify_release_lock();
    void notify_released_lock();
    void notify_request_lock();
    int notify_lock_owner(bufferlist &bl);

    void schedule_async_request_timed_out(const WatchNotify::AsyncRequestId &id);
    void async_request_timed_out(const WatchNotify::AsyncRequestId &id);
    int notify_async_request(const WatchNotify::AsyncRequestId &id,
			     bufferlist &in, ProgressContext& prog_ctx);
    void notify_request_leadership();

    void schedule_async_progress(const WatchNotify::AsyncRequestId &id,
				 uint64_t offset, uint64_t total);
    int notify_async_progress(const WatchNotify::AsyncRequestId &id,
			      uint64_t offset, uint64_t total);
    void schedule_async_complete(const WatchNotify::AsyncRequestId &id,
				 int r);
    int notify_async_complete(const WatchNotify::AsyncRequestId &id,
			      int r);

    void handle_payload(const WatchNotify::HeaderUpdatePayload& payload,
		        bufferlist *out);
    void handle_payload(const WatchNotify::AcquiredLockPayload& payload,
		        bufferlist *out);
    void handle_payload(const WatchNotify::ReleasedLockPayload& payload,
		        bufferlist *out);
    void handle_payload(const WatchNotify::RequestLockPayload& payload,
		        bufferlist *out);
    void handle_payload(const WatchNotify::AsyncProgressPayload& payload,
		        bufferlist *out);
    void handle_payload(const WatchNotify::AsyncCompletePayload& payload,
		        bufferlist *out);
    void handle_payload(const WatchNotify::FlattenPayload& payload,
		        bufferlist *out);
    void handle_payload(const WatchNotify::ResizePayload& payload,
		        bufferlist *out);
    void handle_payload(const WatchNotify::SnapCreatePayload& payload,
		        bufferlist *out);
    void handle_payload(const WatchNotify::UnknownPayload& payload,
		        bufferlist *out);

    void handle_notify(uint64_t notify_id, uint64_t handle, bufferlist &bl);
    void handle_error(uint64_t cookie, int err);
    void acknowledge_notify(uint64_t notify_id, uint64_t handle,
			    bufferlist &out);

    void reregister_watch();
  };

} // namespace librbd

#endif // CEPH_LIBRBD_IMAGE_WATCHER_H
