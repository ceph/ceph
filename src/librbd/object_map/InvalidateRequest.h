// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_INVALIDATE_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_INVALIDATE_REQUEST_H

#include "include/int_types.h"
#include "librbd/AsyncRequest.h"

class Context;

namespace librbd {

class ImageCtx;

namespace object_map {

template <typename ImageCtxT = ImageCtx>
class InvalidateRequest : public AsyncRequest<ImageCtxT> {
public:
  static InvalidateRequest* create(ImageCtxT &image_ctx, uint64_t snap_id,
                                   bool force, Context *on_finish);

  InvalidateRequest(ImageCtxT &image_ctx, uint64_t snap_id, bool force,
                    Context *on_finish)
    : AsyncRequest<ImageCtxT>(image_ctx, on_finish),
      m_snap_id(snap_id), m_force(force) {
  }

  void send() override;

protected:
  bool should_complete(int r) override;

private:
  uint64_t m_snap_id;
  bool m_force;
};

} // namespace object_map
} // namespace librbd

extern template class librbd::object_map::InvalidateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OBJECT_MAP_INVALIDATE_REQUEST_H
