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

#ifndef CEPH_RBD_MIRROR_IMAGEDELETER_H
#define CEPH_RBD_MIRROR_IMAGEDELETER_H

#include <deque>
#include <vector>
#include "include/atomic.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "common/Timer.h"
#include "types.h"

class ContextWQ;

namespace rbd {
namespace mirror {

class ImageDeleterAdminSocketHook;

/**
 * Manage deletion of non-primary images.
 */
class ImageDeleter {
public:
  static const int EISPRM = 1000;

  ImageDeleter(ContextWQ *work_queue, SafeTimer *timer, Mutex *timer_lock);
  ~ImageDeleter();
  ImageDeleter(const ImageDeleter&) = delete;
  ImageDeleter& operator=(const ImageDeleter&) = delete;

  void schedule_image_delete(RadosRef local_rados,
                             int64_t local_pool_id,
                             const std::string& local_image_id,
                             const std::string& local_image_name,
                             const std::string& global_image_id);
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
  void set_failed_timer_interval(double interval);

private:

  class ImageDeleterThread : public Thread {
    ImageDeleter *m_image_deleter;
  public:
    ImageDeleterThread(ImageDeleter *image_deleter) :
      m_image_deleter(image_deleter) {}
    void *entry() {
      m_image_deleter->run();
      return 0;
    }
  };

  struct DeleteInfo {
    RadosRef local_rados;
    int64_t local_pool_id;
    std::string local_image_id;
    std::string local_image_name;
    std::string global_image_id;
    int error_code;
    int retries;
    bool notify_on_failed_retry;
    Context *on_delete;

    DeleteInfo(RadosRef local_rados, int64_t local_pool_id,
               const std::string& local_image_id,
               const std::string& local_image_name,
               const std::string& global_image_id) :
      local_rados(local_rados), local_pool_id(local_pool_id),
      local_image_id(local_image_id), local_image_name(local_image_name),
      global_image_id(global_image_id), error_code(0), retries(0),
      notify_on_failed_retry(true), on_delete(nullptr) {
    }

    bool match(int64_t local_pool_id, const std::string &global_image_id) {
      return (this->local_pool_id == local_pool_id &&
              this->global_image_id == global_image_id);
    }
    void notify(int r);
    void to_string(std::stringstream& ss);
    void print_status(Formatter *f, std::stringstream *ss,
                      bool print_failure_info=false);
  };

  atomic_t m_running;

  ContextWQ *m_work_queue;

  std::deque<std::unique_ptr<DeleteInfo> > m_delete_queue;
  Mutex m_delete_lock;
  Cond m_delete_queue_cond;

  unique_ptr<DeleteInfo> m_active_delete;

  ImageDeleterThread m_image_deleter_thread;

  std::deque<std::unique_ptr<DeleteInfo>> m_failed_queue;
  // TODO: make interval value configurable
  double m_failed_interval = 30;
  SafeTimer *m_failed_timer;
  Mutex *m_failed_timer_lock;

  ImageDeleterAdminSocketHook *m_asok_hook;

  void run();
  bool process_image_delete();
  int image_has_snapshots_and_children(librados::IoCtx *ioctx,
                                       std::string& image_id,
                                       bool *has_snapshots);

  void complete_active_delete(int r);
  void enqueue_failed_delete(int error_code);
  void retry_failed_deletions();

  unique_ptr<DeleteInfo> const*
  find_delete_info(int64_t local_pool_id, const std::string &global_image_id);

};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGEDELETER_H
