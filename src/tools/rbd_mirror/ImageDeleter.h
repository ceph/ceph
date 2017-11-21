// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RBD_MIRROR_IMAGE_DELETER_H
#define CEPH_RBD_MIRROR_IMAGE_DELETER_H

#include "include/utime.h"
#include "common/AsyncOpTracker.h"
#include "common/Mutex.h"
#include "types.h"
#include "tools/rbd_mirror/image_deleter/Types.h"
#include <atomic>
#include <deque>
#include <iosfwd>
#include <memory>
#include <vector>

class AdminSocketHook;
class Context;
class ContextWQ;
class SafeTimer;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class ServiceDaemon;

/**
 * Manage deletion of non-primary images.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ImageDeleter {
public:
  ImageDeleter(ContextWQ *work_queue, SafeTimer *timer, Mutex *timer_lock,
               ServiceDaemon<librbd::ImageCtx>* service_daemon);
  ~ImageDeleter();
  ImageDeleter(const ImageDeleter&) = delete;
  ImageDeleter& operator=(const ImageDeleter&) = delete;

  void schedule_image_delete(IoCtxRef local_io_ctx,
                             const std::string& global_image_id,
                             bool ignore_orphaned,
                             Context *on_finish);
  void wait_for_scheduled_deletion(int64_t local_pool_id,
                                   const std::string &global_image_id,
                                   Context *ctx,
                                   bool notify_on_failed_retry=true);
  void cancel_waiter(int64_t local_pool_id,
                     const std::string &global_image_id);

  void print_status(Formatter *f, std::stringstream *ss);

  // for testing purposes
  std::vector<std::string> get_delete_queue_items();
  std::vector<std::pair<std::string, int> > get_failed_queue_items();

  inline void set_busy_timer_interval(double interval) {
    m_busy_interval = interval;
  }

private:

  struct DeleteInfo {
    int64_t local_pool_id;
    std::string global_image_id;
    IoCtxRef local_io_ctx;
    bool ignore_orphaned = false;
    Context *on_delete = nullptr;

    image_deleter::ErrorResult error_result = {};
    int error_code = 0;
    utime_t retry_time = {};
    int retries = 0;
    bool notify_on_failed_retry = true;

    DeleteInfo(int64_t local_pool_id, const std::string& global_image_id)
      : local_pool_id(local_pool_id), global_image_id(global_image_id) {
    }

    DeleteInfo(int64_t local_pool_id, const std::string& global_image_id,
               IoCtxRef local_io_ctx, bool ignore_orphaned,
               Context *on_delete)
      : local_pool_id(local_pool_id), global_image_id(global_image_id),
        local_io_ctx(local_io_ctx), ignore_orphaned(ignore_orphaned),
        on_delete(on_delete) {
    }

    inline bool operator==(const DeleteInfo& delete_info) const {
      return (local_pool_id == delete_info.local_pool_id &&
              global_image_id == delete_info.global_image_id);
    }

    friend std::ostream& operator<<(std::ostream& os, DeleteInfo& delete_info) {
      os << "[" << "local_pool_id=" << delete_info.local_pool_id << ", "
         << "global_image_id=" << delete_info.global_image_id << "]";
    return os;
    }

    void notify(int r);
    void print_status(Formatter *f, std::stringstream *ss,
                      bool print_failure_info=false);
  };
  typedef std::shared_ptr<DeleteInfo> DeleteInfoRef;
  typedef std::deque<DeleteInfoRef> DeleteQueue;

  ContextWQ *m_work_queue;
  SafeTimer *m_timer;
  Mutex *m_timer_lock;
  ServiceDaemon<librbd::ImageCtx>* m_service_daemon;

  std::atomic<unsigned> m_running { 1 };

  double m_busy_interval = 1;

  AsyncOpTracker m_async_op_tracker;

  Mutex m_lock;
  DeleteQueue m_delete_queue;
  DeleteQueue m_retry_delete_queue;
  DeleteQueue m_in_flight_delete_queue;

  AdminSocketHook *m_asok_hook;

  Context *m_timer_ctx = nullptr;

  bool process_image_delete();

  void complete_active_delete(DeleteInfoRef* delete_info, int r);
  void enqueue_failed_delete(DeleteInfoRef* delete_info, int error_code,
                             double retry_delay);

  DeleteInfoRef find_delete_info(int64_t local_pool_id,
                                 const std::string &global_image_id);

  void remove_images();
  void remove_image(DeleteInfoRef delete_info);
  void handle_remove_image(DeleteInfoRef delete_info, int r);

  void schedule_retry_timer();
  void cancel_retry_timer();
  void handle_retry_timer();

};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageDeleter<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_DELETER_H
