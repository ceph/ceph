// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OPERATION_FLATTEN_REQUEST_H
#define CEPH_LIBRBD_OPERATION_FLATTEN_REQUEST_H

#include "librbd/operation/Request.h"
#include "common/snap_types.h"

namespace librbd {

class ImageCtx;
class ProgressContext;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class FlattenRequest : public Request<ImageCtxT>
{
public:
  FlattenRequest(ImageCtxT &image_ctx, Context *on_finish,
                 uint64_t overlap_objects, const ::SnapContext &snapc,
                 ProgressContext &prog_ctx)
    : Request<ImageCtxT>(image_ctx, on_finish), m_overlap_objects(overlap_objects),
      m_snapc(snapc), m_prog_ctx(prog_ctx) {
  }

protected:
  void send_op() override;
  bool should_complete(int r) override;

  journal::Event create_event(uint64_t op_tid) const override {
    return journal::FlattenEvent(op_tid);
  }

private:
  /**
   * Flatten goes through the following state machine to copyup objects
   * from the parent image:
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * STATE_FLATTEN_OBJECTS ---> STATE_DETACH_CHILD  . . . . .
   *           .                         |                  .
   *           .                         |                  .
   *           .                         v                  .
   *           .               STATE_UPDATE_HEADER          .
   *           .                         |                  .
   *           .                         |                  .
   *           .                         \---> <finish> < . .
   *           .                                   ^
   *           .                                   .
   *           . . . . . . . . . . . . . . . . . . .
   *
   * @endverbatim
   *
   * The _DETACH_CHILD state will be skipped if the image has one or
   * more snapshots. The _UPDATE_HEADER state will be skipped if the
   * image was concurrently flattened by another client.
   */
  enum State {
    STATE_FLATTEN_OBJECTS,
    STATE_DETACH_CHILD,
    STATE_UPDATE_HEADER
  };

  uint64_t m_overlap_objects;
  ::SnapContext m_snapc;
  ProgressContext &m_prog_ctx;
  State m_state = STATE_FLATTEN_OBJECTS;

  bool send_detach_child();
  bool send_update_header();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::FlattenRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_FLATTEN_REQUEST_H
