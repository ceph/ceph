// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_INSTANCE_WATCHER_H
#define CEPH_RBD_MIRROR_INSTANCE_WATCHER_H

#include <string>
#include <vector>
#include <boost/optional.hpp>

#include "librbd/Watcher.h"
#include "librbd/managed_lock/Types.h"

namespace librbd {
  class ImageCtx;
  template <typename> class ManagedLock;
}

namespace rbd {
namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class InstanceWatcher : protected librbd::Watcher {
public:
  static void get_instances(librados::IoCtx &io_ctx,
                            std::vector<std::string> *instance_ids,
                            Context *on_finish);
  static void remove_instance(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                              const std::string &instance_id,
                              Context *on_finish);

  static InstanceWatcher *create(
    librados::IoCtx &io_ctx, ContextWQ *work_queue,
    const boost::optional<std::string> &id = boost::none) {
    return new InstanceWatcher(io_ctx, work_queue, id);
  }
  void destroy() {
    delete this;
  }

  InstanceWatcher(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                  const boost::optional<std::string> &id = boost::none);
  ~InstanceWatcher() override;

  int init();
  void shut_down();

  void init(Context *on_finish);
  void shut_down(Context *on_finish);
  void remove(Context *on_finish);

protected:
  void handle_notify(uint64_t notify_id, uint64_t handle, uint64_t notifier_id,
                     bufferlist &bl) override;

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
   * <uninitialized> <----------------+--------\
   *    | (init)         ^            |        |
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

  bool m_owner;
  std::string m_instance_id;

  mutable Mutex m_lock;
  librbd::ManagedLock<ImageCtxT> *m_instance_lock;
  Context *m_on_finish = nullptr;
  int m_ret_val = 0;
  bool m_removing = false;
  librbd::managed_lock::Locker m_instance_locker;

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

  void get_instance_locker();
  void handle_get_instance_locker(int r);

  void break_instance_lock();
  void handle_break_instance_lock(int r);
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_INSTANCE_WATCHER_H
