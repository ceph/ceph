// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OPERATION_OBJECT_MAP_ITERATE_H
#define CEPH_LIBRBD_OPERATION_OBJECT_MAP_ITERATE_H

#include <iostream>
#include <atomic>

#include "include/int_types.h"
#include "include/rbd/object_map_types.h"
#include "librbd/AsyncRequest.h"

namespace librbd {

class ImageCtx;
class ProgressContext;

namespace operation {

template <typename ImageCtxT = ImageCtx>
using ObjectIterateWork = bool(*)(ImageCtxT &image_ctx,
				  uint64_t object_no,
				  uint8_t current_state,
				  uint8_t new_state);

template <typename ImageCtxT = ImageCtx>
class ObjectMapIterateRequest : public AsyncRequest<ImageCtxT> {
public:
  ObjectMapIterateRequest(ImageCtxT &image_ctx, Context *on_finish,
			  ProgressContext &prog_ctx,
			  ObjectIterateWork<ImageCtxT> handle_mismatch)
    : AsyncRequest<ImageCtxT>(image_ctx, on_finish), m_image_ctx(image_ctx),
    m_prog_ctx(prog_ctx), m_handle_mismatch(handle_mismatch)
  {
  }

  void send() override;

protected:
  bool should_complete(int r) override;

private:
  enum State {
    STATE_VERIFY_OBJECTS,
    STATE_INVALIDATE_OBJECT_MAP
  };

  ImageCtxT &m_image_ctx;
  ProgressContext &m_prog_ctx;
  ObjectIterateWork<ImageCtxT> m_handle_mismatch;
  std::atomic_flag m_invalidate = ATOMIC_FLAG_INIT;
  State m_state = STATE_VERIFY_OBJECTS;

  void send_verify_objects();
  void send_invalidate_object_map();

  uint64_t get_image_size() const;
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::ObjectMapIterateRequest<librbd::ImageCtx>;

#endif
