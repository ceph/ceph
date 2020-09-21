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
#include "common/ceph_mutex.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/image_deleter/Types.h"
#include <atomic>
#include <deque>
#include <iosfwd>
#include <map>
#include <memory>
#include <vector>

class AdminSocketHook;
class Context;
class SafeTimer;
namespace librbd {
struct ImageCtx;
namespace asio { struct ContextWQ; }
} // namespace librbd

namespace rbd {
namespace mirror {

template <typename> class ServiceDaemon;
template <typename> class Threads;
template <typename> class Throttler;

namespace image_deleter { template <typename> struct TrashWatcher; }

/**
 * Manage deletion of non-primary images.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ImageDeleter {
public:
  static ImageDeleter* create(
      librados::IoCtx& local_io_ctx, Threads<librbd::ImageCtx>* threads,
      Throttler<librbd::ImageCtx>* image_deletion_throttler,
      ServiceDaemon<librbd::ImageCtx>* service_daemon) {
    return new ImageDeleter(local_io_ctx, threads, image_deletion_throttler,
                            service_daemon);
  }

  ImageDeleter(librados::IoCtx& local_io_ctx,
               Threads<librbd::ImageCtx>* threads,
               Throttler<librbd::ImageCtx>* image_deletion_throttler,
               ServiceDaemon<librbd::ImageCtx>* service_daemon);

  ImageDeleter(const ImageDeleter&) = delete;
  ImageDeleter& operator=(const ImageDeleter&) = delete;

  static void trash_move(librados::IoCtx& local_io_ctx,
                         const std::string& global_image_id, bool resync,
                         librbd::asio::ContextWQ* work_queue,
                         Context* on_finish);

  void init(Context* on_finish);
  void shut_down(Context* on_finish);

  void print_status(Formatter *f);

  // for testing purposes
  void wait_for_deletion(const std::string &image_id,
                         bool scheduled_only, Context* on_finish);

  std::vector<std::string> get_delete_queue_items();
  std::vector<std::pair<std::string, int> > get_failed_queue_items();

  inline void set_busy_timer_interval(double interval) {
    m_busy_interval = interval;
  }

private:
  using clock_t = ceph::real_clock;
  struct TrashListener : public image_deleter::TrashListener {
    ImageDeleter *image_deleter;

    TrashListener(ImageDeleter *image_deleter) : image_deleter(image_deleter) {
    }

    void handle_trash_image(const std::string& image_id,
      const ceph::real_clock::time_point& deferment_end_time) override {
      image_deleter->handle_trash_image(image_id, deferment_end_time);
    }
  };

  struct DeleteInfo {
    std::string image_id;

    image_deleter::ErrorResult error_result = {};
    int error_code = 0;
    clock_t::time_point retry_time;
    int retries = 0;

    DeleteInfo(const std::string& image_id)
      : image_id(image_id) {
    }

    inline bool operator==(const DeleteInfo& delete_info) const {
      return (image_id == delete_info.image_id);
    }

    friend std::ostream& operator<<(std::ostream& os, DeleteInfo& delete_info) {
      os << "[image_id=" << delete_info.image_id << "]";
    return os;
    }

    void print_status(Formatter *f,
                      bool print_failure_info=false);
  };
  typedef std::shared_ptr<DeleteInfo> DeleteInfoRef;
  typedef std::deque<DeleteInfoRef> DeleteQueue;
  typedef std::map<std::string, Context*> OnDeleteContexts;

  librados::IoCtx& m_local_io_ctx;
  Threads<librbd::ImageCtx>* m_threads;
  Throttler<librbd::ImageCtx>* m_image_deletion_throttler;
  ServiceDaemon<librbd::ImageCtx>* m_service_daemon;

  image_deleter::TrashWatcher<ImageCtxT>* m_trash_watcher = nullptr;
  TrashListener m_trash_listener;

  std::atomic<unsigned> m_running { 1 };

  double m_busy_interval = 1;

  AsyncOpTracker m_async_op_tracker;

  ceph::mutex m_lock;
  DeleteQueue m_delete_queue;
  DeleteQueue m_retry_delete_queue;
  DeleteQueue m_in_flight_delete_queue;

  OnDeleteContexts m_on_delete_contexts;

  AdminSocketHook *m_asok_hook = nullptr;

  Context *m_timer_ctx = nullptr;

  bool process_image_delete();

  void complete_active_delete(DeleteInfoRef* delete_info, int r);
  void enqueue_failed_delete(DeleteInfoRef* delete_info, int error_code,
                             double retry_delay);

  DeleteInfoRef find_delete_info(const std::string &image_id);

  void remove_images();
  void remove_image(DeleteInfoRef delete_info);
  void handle_remove_image(DeleteInfoRef delete_info, int r);

  void schedule_retry_timer();
  void cancel_retry_timer();
  void handle_retry_timer();

  void handle_trash_image(const std::string& image_id,
                          const clock_t::time_point& deferment_end_time);

  void shut_down_trash_watcher(Context* on_finish);
  void wait_for_ops(Context* on_finish);
  void cancel_all_deletions(Context* on_finish);

  void notify_on_delete(const std::string& image_id, int r);

};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageDeleter<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_DELETER_H
