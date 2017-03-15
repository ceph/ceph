// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_INSTANCE_WATCHER_H
#define CEPH_RBD_MIRROR_INSTANCE_WATCHER_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/AsyncOpTracker.h"
#include "librbd/Watcher.h"
#include "librbd/managed_lock/Types.h"
#include "tools/rbd_mirror/instance_watcher/Types.h"

namespace librbd {

class ImageCtx;
template <typename> class ManagedLock;

}

namespace rbd {
namespace mirror {

template <typename> class InstanceReplayer;
template <typename> struct Threads;

template <typename ImageCtxT = librbd::ImageCtx>
class InstanceWatcher : protected librbd::Watcher {
public:
  static void get_instances(librados::IoCtx &io_ctx,
                            std::vector<std::string> *instance_ids,
                            Context *on_finish);
  static void remove_instance(librados::IoCtx &io_ctx,
                              ContextWQ *work_queue,
                              const std::string &instance_id,
                              Context *on_finish);

  static InstanceWatcher *create(
    librados::IoCtx &io_ctx, ContextWQ *work_queue,
    InstanceReplayer<ImageCtxT> *instance_replayer);
  void destroy() {
    delete this;
  }

  InstanceWatcher(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                  InstanceReplayer<ImageCtxT> *instance_replayer,
                  const std::string &instance_id);
  ~InstanceWatcher() override;

  inline std::string &get_instance_id() {
    return m_instance_id;
  }

  int init();
  void shut_down();

  void init(Context *on_finish);
  void shut_down(Context *on_finish);
  void remove(Context *on_finish);

  void notify_image_acquire(const std::string &instance_id,
                            const std::string &global_image_id,
                            const std::string &peer_mirror_uuid,
                            const std::string &peer_image_id,
                            Context *on_notify_ack);
  void notify_image_release(const std::string &instance_id,
                            const std::string &global_image_id,
                            const std::string &peer_mirror_uuid,
                            const std::string &peer_image_id,
			    bool schedule_delete, Context *on_notify_ack);

  void cancel_notify_requests(const std::string &instance_id);

private:
  /**
   * @verbatim
   *
   *       BREAK_INSTANCE_LOCK -------\
   *          ^                       |
   *          |               (error) |
   *       GET_INSTANCE_LOCKER  * * *>|
   *          ^ (remove)              |
   *          |                       |
   * <uninitialized> <----------------+---- WAIT_FOR_NOTIFY_OPS
   *    | (init)         ^            |        ^
   *    v        (error) *            |        |
   * REGISTER_INSTANCE * *     * * * *|* *> UNREGISTER_INSTANCE
   *    |                      *      |        ^
   *    v              (error) *      v        |
   * CREATE_INSTANCE_OBJECT  * *   * * * *> REMOVE_INSTANCE_OBJECT
   *    |                          *           ^
   *    v           (error)        *           |
   * REGISTER_WATCH  * * * * * * * *   * *> UNREGISTER_WATCH
   *    |                              *       ^
   *    v         (error)              *       |
   * ACQUIRE_LOCK  * * * * * * * * * * *    RELEASE_LOCK
   *    |                                      ^
   *    v       (shut_down)                    |
   * <watching> -------------------------------/
   *
   * @endverbatim
   */

  struct C_NotifyInstanceRequest;

  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    InstanceWatcher *instance_watcher;
    std::string instance_id;
    C_NotifyAck *on_notify_ack;

    HandlePayloadVisitor(InstanceWatcher *instance_watcher,
                         const std::string &instance_id,
                         C_NotifyAck *on_notify_ack)
      : instance_watcher(instance_watcher), instance_id(instance_id),
        on_notify_ack(on_notify_ack) {
    }

    template <typename Payload>
    inline void operator()(const Payload &payload) const {
      instance_watcher->handle_payload(instance_id, payload, on_notify_ack);
    }
  };

  struct Request {
    std::string instance_id;
    uint64_t request_id;
    C_NotifyAck *on_notify_ack = nullptr;

    Request(const std::string &instance_id, uint64_t request_id)
      : instance_id(instance_id), request_id(request_id) {
    }

    inline bool operator<(const Request &rhs) const {
      return instance_id < rhs.instance_id ||
        (instance_id == rhs.instance_id && request_id < rhs.request_id);
    }
  };

  Threads<ImageCtxT> *m_threads;
  InstanceReplayer<ImageCtxT> *m_instance_replayer;
  std::string m_instance_id;

  mutable Mutex m_lock;
  librbd::ManagedLock<ImageCtxT> *m_instance_lock;
  Context *m_on_finish = nullptr;
  int m_ret_val = 0;
  bool m_removing = false;
  librbd::managed_lock::Locker m_instance_locker;
  std::set<std::pair<std::string, C_NotifyInstanceRequest *>> m_notify_ops;
  AsyncOpTracker m_notify_op_tracker;
  uint64_t m_request_seq = 0;
  std::set<Request> m_requests;

  void register_instance();
  void handle_register_instance(int r);

  void create_instance_object();
  void handle_create_instance_object(int r);

  void register_watch();
  void handle_register_watch(int r);

  void acquire_lock();
  void handle_acquire_lock(int r);

  void release_lock();
  void handle_release_lock(int r);

  void unregister_watch();
  void handle_unregister_watch(int r);

  void remove_instance_object();
  void handle_remove_instance_object(int r);

  void unregister_instance();
  void handle_unregister_instance(int r);

  void wait_for_notify_ops();
  void handle_wait_for_notify_ops(int r);

  void get_instance_locker();
  void handle_get_instance_locker(int r);

  void break_instance_lock();
  void handle_break_instance_lock(int r);

  Context *prepare_request(const std::string &instance_id, uint64_t request_id,
                           C_NotifyAck *on_notify_ack);

  void handle_notify(uint64_t notify_id, uint64_t handle,
                     uint64_t notifier_id, bufferlist &bl) override;

  void handle_image_acquire(const std::string &global_image_id,
                            const std::string &peer_mirror_uuid,
                            const std::string &peer_image_id,
                            Context *on_finish);
  void handle_image_release(const std::string &global_image_id,
                            const std::string &peer_mirror_uuid,
                            const std::string &peer_image_id,
                            bool schedule_delete, Context *on_finish);

  void handle_payload(const std::string &instance_id,
                      const instance_watcher::ImageAcquirePayload &payload,
                      C_NotifyAck *on_notify_ack);
  void handle_payload(const std::string &instance_id,
                      const instance_watcher::ImageReleasePayload &payload,
                      C_NotifyAck *on_notify_ack);
  void handle_payload(const std::string &instance_id,
                      const instance_watcher::UnknownPayload &payload,
                      C_NotifyAck *on_notify_ack);
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_INSTANCE_WATCHER_H
