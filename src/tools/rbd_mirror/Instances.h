// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_INSTANCES_H
#define CEPH_RBD_MIRROR_INSTANCES_H

#include <map>
#include <vector>

#include "include/buffer.h"
#include "common/AsyncOpTracker.h"
#include "common/Mutex.h"
#include "librbd/Watcher.h"

namespace librados { class IoCtx; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;

template <typename ImageCtxT = librbd::ImageCtx>
class Instances {
public:
  static Instances *create(Threads<ImageCtxT> *threads,
                           librados::IoCtx &ioctx) {
    return new Instances(threads, ioctx);
  }
  void destroy() {
    delete this;
  }

  Instances(Threads<ImageCtxT> *threads, librados::IoCtx &ioctx);
  virtual ~Instances();

  void init(Context *on_finish);
  void shut_down(Context *on_finish);

  void notify(const std::string &instance_id);
  void list(std::vector<std::string> *instance_ids);

private:
  /**
   * @verbatim
   *
   * <uninitialized> <---------------------\
   *    | (init)           ^               |
   *    v          (error) *               |
   * GET_INSTANCES * * * * *            WAIT_FOR_OPS
   *    |                                  ^
   *    v          (shut_down)             |
   * <initialized> ------------------------/
   *      .
   *      . (remove_instance)
   *      v
   *   REMOVE_INSTANCE
   *
   * @endverbatim
   */

  struct Instance {
    std::string id;
    Context *timer_task = nullptr;

    Instance(const std::string &instance_id) : id(instance_id) {
    }
  };

  struct C_Notify : Context {
    Instances *instances;
    std::string instance_id;

    C_Notify(Instances *instances, const std::string &instance_id)
      : instances(instances), instance_id(instance_id) {
      instances->m_async_op_tracker.start_op();
    }

    void finish(int r) override {
      instances->handle_notify(instance_id);
      instances->m_async_op_tracker.finish_op();
    }
  };

  Threads<ImageCtxT> *m_threads;
  librados::IoCtx &m_ioctx;
  CephContext *m_cct;

  Mutex m_lock;
  std::vector<std::string> m_instance_ids;
  std::map<std::string, Instance> m_instances;
  Context *m_on_finish = nullptr;
  AsyncOpTracker m_async_op_tracker;

  void handle_notify(const std::string &instance_id);

  void get_instances();
  void handle_get_instances(int r);

  void wait_for_ops();
  void handle_wait_for_ops(int r);

  void remove_instance(Instance &instance);
  void handle_remove_instance(int r);

  void cancel_remove_task(Instance &instance);
  void schedule_remove_task(Instance &instance);
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_INSTANCES_H
