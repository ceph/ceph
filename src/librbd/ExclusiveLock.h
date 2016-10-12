// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "Lock.h"
#include <list>
#include <string>
#include <utility>

namespace librbd {

class Lock;
class ImageCtx;

template <typename ImageCtxT = ImageCtx>
class ExclusiveLock {
public:
  static const std::string WATCHER_LOCK_TAG;

  static ExclusiveLock *create(ImageCtxT &image_ctx) {
    return new ExclusiveLock<ImageCtxT>(image_ctx);
  }

  ExclusiveLock(ImageCtxT &image_ctx);
  ~ExclusiveLock();

  bool is_lock_owner() const;
  bool accept_requests(int *ret_val) const;

  void block_requests(int r);
  void unblock_requests();

  void init(uint64_t features, Context *on_init);
  void shut_down(Context *on_shutdown);

  void try_lock(Context *on_tried_lock);
  void request_lock(Context *on_locked);
  void release_lock(Context *on_released);

  void reacquire_lock(Context *on_reacquired = nullptr);

  void handle_peer_notification();

  void assert_header_locked(librados::ObjectWriteOperation *op);

  static bool decode_lock_cookie(const std::string &cookie, uint64_t *handle);

private:

  ImageCtxT &m_image_ctx;
  Lock *m_managed_lock;

  mutable Mutex m_lock;

  uint32_t m_request_blocked_count = 0;
  int m_request_blocked_ret_val = 0;

  void handle_acquire_lock(int r);

  void handle_release_lock(int r);
};

} // namespace librbd

extern template class librbd::ExclusiveLock<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_H
