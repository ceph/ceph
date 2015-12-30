// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OPERATION_REBUILD_OBJECT_MAP_REQUEST_H
#define CEPH_LIBRBD_OPERATION_REBUILD_OBJECT_MAP_REQUEST_H

#include "include/int_types.h"
#include "librbd/AsyncRequest.h"

namespace librbd {

class ImageCtx;
class ProgressContext;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class RebuildObjectMapRequest : public AsyncRequest<ImageCtxT> {
public:

  RebuildObjectMapRequest(ImageCtxT &image_ctx, Context *on_finish,
                          ProgressContext &prog_ctx)
    : AsyncRequest<ImageCtxT>(image_ctx, on_finish), m_image_ctx(image_ctx),
      m_prog_ctx(prog_ctx), m_attempted_trim(false)
  {
  }

  virtual void send();

protected:
  virtual bool should_complete(int r);

private:
  /**
   * Rebuild object map goes through the following state machine to
   * verify per-object state:
   *
   * <start>
   *  .   |               . . . . . . . . . .
   *  .   |               .                 .
   *  .   v               v                 .
   *  . STATE_RESIZE_OBJECT_MAP . . . > STATE_TRIM_IMAGE
   *  .          |
   *  .          v
   *  . . . > STATE_VERIFY_OBJECTS
   *             |
   *             v
   *          STATE_SAVE_OBJECT_MAP
   *             |
   *             v
   *          STATE_UPDATE_HEADER
   *
   * The _RESIZE_OBJECT_MAP state will be skipped if the object map
   * is appropriately sized for the image. The _TRIM_IMAGE state will
   * only be hit if the resize failed due to an in-use object.
   */
  enum State {
    STATE_RESIZE_OBJECT_MAP,
    STATE_TRIM_IMAGE,
    STATE_VERIFY_OBJECTS,
    STATE_SAVE_OBJECT_MAP,
    STATE_UPDATE_HEADER
  };

  ImageCtxT &m_image_ctx;
  ProgressContext &m_prog_ctx;
  State m_state;
  bool m_attempted_trim;

  void send_resize_object_map();
  void send_trim_image();
  void send_verify_objects();
  void send_save_object_map();
  void send_update_header();

  uint64_t get_image_size() const;

};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::RebuildObjectMapRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_REBUILD_OBJECT_MAP_REQUEST_H
