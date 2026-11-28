// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_OPERATION_CHECK_OBJECT_MAP_REQUEST_H
#define CEPH_LIBRBD_OPERATION_CHECK_OBJECT_MAP_REQUEST_H

#include "include/int_types.h"
#include "librbd/AsyncRequest.h"

namespace librbd {

class ImageCtx;
class ProgressContext;
template <typename> class ObjectMap;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class CheckObjectMapRequest : public AsyncRequest<ImageCtxT> {
public:

  CheckObjectMapRequest(ImageCtxT &image_ctx, Context *on_finish,
                        ProgressContext &prog_ctx)
    : AsyncRequest<ImageCtxT>(image_ctx, on_finish), m_image_ctx(image_ctx),
      m_prog_ctx(prog_ctx)
  {
  }
  ~CheckObjectMapRequest() override;

  void send() override;

protected:
  bool should_complete(int r) override;

private:
  /**
   * Check object map goes through the following state machine to verify
   * per-object state:
   *
   * <start>
   *    |
   *    v
   * STATE_OPEN_OBJECT_MAP (skip if the object map is already loaded)
   *    |
   *    v
   * STATE_VERIFY_OBJECTS
   *    |
   *    v
   * <finish>
   *
   * The object map is not loaded into the image context for a snapshot that
   * still has a live parent overlap, so in that case a private instance is
   * opened for the duration of the operation.
   */
  enum State {
    STATE_OPEN_OBJECT_MAP,
    STATE_VERIFY_OBJECTS
  };

  ImageCtxT &m_image_ctx;
  ProgressContext &m_prog_ctx;
  ObjectMap<ImageCtxT> *m_object_map = nullptr;
  ObjectMap<ImageCtxT> *m_opened_object_map = nullptr;
  State m_state = STATE_VERIFY_OBJECTS;

  void send_open_object_map(uint64_t snap_id);
  void send_verify_objects();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::CheckObjectMapRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_CHECK_OBJECT_MAP_REQUEST_H
