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
  typedef std::vector<std::string> InstanceIds;

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

  void acked(const InstanceIds& instance_ids);

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

  enum InstanceState {
    INSTANCE_STATE_IDLE,
    INSTANCE_STATE_REMOVING
  };

  struct Instance {
    utime_t acked_time{};
    InstanceState state = INSTANCE_STATE_IDLE;
  };

  struct C_NotifyBase : public Context {
    Instances *instances;
    InstanceIds instance_ids;

    C_NotifyBase(Instances *instances, const InstanceIds& instance_ids)
      : instances(instances), instance_ids(instance_ids) {
      instances->m_async_op_tracker.start_op();
    }

    void finish(int r) override {
      execute();
      instances->m_async_op_tracker.finish_op();
    }

    virtual void execute() = 0;
  };

  struct C_HandleAcked : public C_NotifyBase {
    C_HandleAcked(Instances *instances, const InstanceIds& instance_ids)
      : C_NotifyBase(instances, instance_ids) {
    }

    void execute() override {
      this->instances->handle_acked(this->instance_ids);
    }
  };

  Threads<ImageCtxT> *m_threads;
  librados::IoCtx &m_ioctx;
  CephContext *m_cct;

  Mutex m_lock;
  InstanceIds m_instance_ids;
  std::map<std::string, Instance> m_instances;
  Context *m_on_finish = nullptr;
  AsyncOpTracker m_async_op_tracker;

  Context *m_timer_task = nullptr;

  void handle_acked(const InstanceIds& instance_ids);

  void get_instances();
  void handle_get_instances(int r);

  void wait_for_ops();
  void handle_wait_for_ops(int r);

  void remove_instances(const utime_t& time);
  void handle_remove_instances(int r, const InstanceIds& instance_ids);

  void cancel_remove_task();
  void schedule_remove_task(const utime_t& time);
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_INSTANCES_H
