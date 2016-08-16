// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_REACQUIRE_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_REACQUIRE_REQUEST_H

#include "include/int_types.h"
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace exclusive_lock {

template <typename ImageCtxT = ImageCtx>
class ReacquireRequest {
public:

  static ReacquireRequest *create(ImageCtxT &image_ctx,
                                  const std::string &old_cookie,
                                  const std::string &new_cookie,
                                  Context *on_finish) {
    return new ReacquireRequest(image_ctx, old_cookie, new_cookie, on_finish);
  }

  ReacquireRequest(ImageCtxT &image_ctx, const std::string &old_cookie,
                   const std::string &new_cookie, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * SET_COOKIE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  ImageCtxT &m_image_ctx;
  std::string m_old_cookie;
  std::string m_new_cookie;
  Context *m_on_finish;

  void set_cookie();
  void handle_set_cookie(int r);

};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::ReacquireRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_REACQUIRE_REQUEST_H
