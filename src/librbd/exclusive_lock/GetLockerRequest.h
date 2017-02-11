// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_GET_LOCKER_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_GET_LOCKER_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"

class Context;

namespace librbd {

struct ImageCtx;

namespace exclusive_lock {

struct Locker;

template <typename ImageCtxT = ImageCtx>
class GetLockerRequest {
public:
  static GetLockerRequest* create(ImageCtxT &image_ctx, Locker *locker,
                                  Context *on_finish) {
    return new GetLockerRequest(image_ctx, locker, on_finish);
  }

  void send();

private:
  ImageCtxT &m_image_ctx;
  Locker *m_locker;
  Context *m_on_finish;

  bufferlist m_out_bl;

  GetLockerRequest(ImageCtxT &image_ctx, Locker *locker, Context *on_finish)
    : m_image_ctx(image_ctx), m_locker(locker),  m_on_finish(on_finish) {
  }

  void send_get_lockers();
  void handle_get_lockers(int r);

  void finish(int r);

};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::GetLockerRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_GET_LOCKER_REQUEST_H
