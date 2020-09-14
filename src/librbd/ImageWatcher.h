// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_WATCHER_H
#define CEPH_LIBRBD_IMAGE_WATCHER_H

#include "cls/rbd/cls_rbd_types.h"
#include "common/AsyncOpTracker.h"
#include "common/ceph_mutex.h"
#include "include/Context.h"
#include "include/rbd/librbd.hpp"
#include "librbd/Watcher.h"
#include "librbd/WatchNotifyTypes.h"
#include <set>
#include <string>
#include <utility>

class entity_name_t;

namespace librbd {

class ImageCtx;
template <typename> class TaskFinisher;

template <typename ImageCtxT = ImageCtx>
class ImageWatcher : public Watcher {
public:
  ImageWatcher(ImageCtxT& image_ctx);
  ~ImageWatcher() override;

  void unregister_watch(Context *on_finish) override;
  void block_notifies(Context *on_finish) override;

  void notify_flatten(uint64_t request_id, ProgressContext &prog_ctx,
                      Context *on_finish);
  void notify_resize(uint64_t request_id, uint64_t size, bool allow_shrink,
                     ProgressContext &prog_ctx, Context *on_finish);
  void notify_snap_create(uint64_t request_id,
                          const cls::rbd::SnapshotNamespace &snap_namespace,
			  const std::string &snap_name,
                          uint64_t flags,
                          ProgressContext &prog_ctx,
			  Context *on_finish);
  void notify_snap_rename(const snapid_t &src_snap_id,
                          const std::string &dst_snap_name,
                          Context *on_finish);
  void notify_snap_remove(const cls::rbd::SnapshotNamespace &snap_namespace,
			  const std::string &snap_name,
			  Context *on_finish);
  void notify_snap_protect(const cls::rbd::SnapshotNamespace &snap_namespace,
			   const std::string &snap_name,
			   Context *on_finish);
  void notify_snap_unprotect(const cls::rbd::SnapshotNamespace &snap_namespace,
			     const std::string &snap_name,
			     Context *on_finish);
  void notify_rebuild_object_map(uint64_t request_id,
                                 ProgressContext &prog_ctx, Context *on_finish);
  void notify_rename(const std::string &image_name, Context *on_finish);

  void notify_update_features(uint64_t features, bool enabled,
                              Context *on_finish);

  void notify_migrate(uint64_t request_id, ProgressContext &prog_ctx,
                      Context *on_finish);

  void notify_sparsify(uint64_t request_id, size_t sparse_size,
                       ProgressContext &prog_ctx, Context *on_finish);

  void notify_acquired_lock();
  void notify_released_lock();
  void notify_request_lock();

  void notify_header_update(Context *on_finish);
  static void notify_header_update(librados::IoCtx &io_ctx,
                                   const std::string &oid);

  void notify_quiesce(uint64_t *request_id, ProgressContext &prog_ctx,
                      Context *on_finish);
  void notify_unquiesce(uint64_t request_id, Context *on_finish);

private:
  enum TaskCode {
    TASK_CODE_REQUEST_LOCK,
    TASK_CODE_CANCEL_ASYNC_REQUESTS,
    TASK_CODE_REREGISTER_WATCH,
    TASK_CODE_ASYNC_REQUEST,
    TASK_CODE_ASYNC_PROGRESS,
    TASK_CODE_QUIESCE,
  };

  typedef std::pair<Context *, ProgressContext *> AsyncRequest;

  class Task {
  public:
    Task(TaskCode task_code) : m_task_code(task_code) {}
    Task(TaskCode task_code, const watch_notify::AsyncRequestId &id)
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
    watch_notify::AsyncRequestId m_async_request_id;
  };

  class RemoteProgressContext : public ProgressContext {
  public:
    RemoteProgressContext(ImageWatcher &image_watcher,
                          const watch_notify::AsyncRequestId &id)
      : m_image_watcher(image_watcher), m_async_request_id(id)
    {
    }

    int update_progress(uint64_t offset, uint64_t total) override {
      m_image_watcher.schedule_async_progress(m_async_request_id, offset,
                                              total);
      return 0;
    }

  private:
    ImageWatcher &m_image_watcher;
    watch_notify::AsyncRequestId m_async_request_id;
  };

  class RemoteContext : public Context {
  public:
    RemoteContext(ImageWatcher &image_watcher,
      	          const watch_notify::AsyncRequestId &id,
      	          ProgressContext *prog_ctx)
      : m_image_watcher(image_watcher), m_async_request_id(id),
        m_prog_ctx(prog_ctx)
    {
    }

    ~RemoteContext() override {
      delete m_prog_ctx;
    }

    void finish(int r) override;

  private:
    ImageWatcher &m_image_watcher;
    watch_notify::AsyncRequestId m_async_request_id;
    ProgressContext *m_prog_ctx;
  };

  struct C_ProcessPayload;
  struct C_ResponseMessage : public Context {
    C_NotifyAck *notify_ack;

    C_ResponseMessage(C_NotifyAck *notify_ack) : notify_ack(notify_ack) {
    }
    void finish(int r) override;
  };

  ImageCtxT &m_image_ctx;

  TaskFinisher<Task> *m_task_finisher;

  ceph::shared_mutex m_async_request_lock;
  std::map<watch_notify::AsyncRequestId, AsyncRequest> m_async_requests;
  std::set<watch_notify::AsyncRequestId> m_async_pending;

  ceph::mutex m_owner_client_id_lock;
  watch_notify::ClientId m_owner_client_id;

  AsyncOpTracker m_async_op_tracker;

  void handle_register_watch(int r);

  void schedule_cancel_async_requests();
  void cancel_async_requests();

  void set_owner_client_id(const watch_notify::ClientId &client_id);
  watch_notify::ClientId get_client_id();

  void handle_request_lock(int r);
  void schedule_request_lock(bool use_timer, int timer_delay = -1);

  void notify_lock_owner(watch_notify::Payload *payload, Context *on_finish);

  Context *remove_async_request(const watch_notify::AsyncRequestId &id);
  void schedule_async_request_timed_out(const watch_notify::AsyncRequestId &id);
  void async_request_timed_out(const watch_notify::AsyncRequestId &id);
  void notify_async_request(const watch_notify::AsyncRequestId &id,
                            watch_notify::Payload *payload,
                            ProgressContext& prog_ctx,
                            Context *on_finish);

  void schedule_async_progress(const watch_notify::AsyncRequestId &id,
                               uint64_t offset, uint64_t total);
  int notify_async_progress(const watch_notify::AsyncRequestId &id,
                            uint64_t offset, uint64_t total);
  void schedule_async_complete(const watch_notify::AsyncRequestId &id, int r);
  void notify_async_complete(const watch_notify::AsyncRequestId &id, int r);
  void handle_async_complete(const watch_notify::AsyncRequestId &request, int r,
                             int ret_val);

  int prepare_async_request(const watch_notify::AsyncRequestId& id,
                            bool* new_request, Context** ctx,
                            ProgressContext** prog_ctx);

  Context *prepare_quiesce_request(const watch_notify::AsyncRequestId &request,
                                   C_NotifyAck *ack_ctx);
  Context *prepare_unquiesce_request(const watch_notify::AsyncRequestId &request);

  void notify_quiesce(const watch_notify::AsyncRequestId &async_request_id,
                      size_t attempts, ProgressContext &prog_ctx,
                      Context *on_finish);

  bool handle_payload(const watch_notify::HeaderUpdatePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::AcquiredLockPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::ReleasedLockPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::RequestLockPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::AsyncProgressPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::AsyncCompletePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::FlattenPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::ResizePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapCreatePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapRenamePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapRemovePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapProtectPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapUnprotectPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::RebuildObjectMapPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::RenamePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::UpdateFeaturesPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::MigratePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SparsifyPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::QuiescePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::UnquiescePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::UnknownPayload& payload,
                      C_NotifyAck *ctx);
  void process_payload(uint64_t notify_id, uint64_t handle,
                       watch_notify::Payload *payload);

  void handle_notify(uint64_t notify_id, uint64_t handle,
                     uint64_t notifier_id, bufferlist &bl) override;
  void handle_error(uint64_t cookie, int err) override;
  void handle_rewatch_complete(int r) override;

  void send_notify(watch_notify::Payload *payload, Context *ctx = nullptr);

};

} // namespace librbd

extern template class librbd::ImageWatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_WATCHER_H
