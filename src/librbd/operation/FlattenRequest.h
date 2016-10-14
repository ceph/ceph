// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OPERATION_FLATTEN_REQUEST_H
#define CEPH_LIBRBD_OPERATION_FLATTEN_REQUEST_H

#include "librbd/operation/Request.h"
#include "librbd/parent_types.h"
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
		 uint64_t object_size, uint64_t overlap_objects,
		 const ::SnapContext &snapc, ProgressContext &prog_ctx)
    : Request<ImageCtxT>(image_ctx, on_finish), m_object_size(object_size),
      m_overlap_objects(overlap_objects), m_snapc(snapc), m_prog_ctx(prog_ctx),
      m_ignore_enoent(false)
  {
  }

protected:
  virtual void send_op();
  virtual bool should_complete(int r);

  virtual journal::Event create_event(uint64_t op_tid) const {
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
   * STATE_FLATTEN_OBJECTS ---> STATE_UPDATE_HEADER . . . . .
   *           .                         |                  .
   *           .                         |                  .
   *           .                         v                  .
   *           .               STATE_UPDATE_CHILDREN        .
   *           .                         |                  .
   *           .                         |                  .
   *           .                         \---> <finish> < . .
   *           .                                   ^
   *           .                                   .
   *           . . . . . . . . . . . . . . . . . . .
   *
   * @endverbatim
   *
   * The _UPDATE_CHILDREN state will be skipped if the image has one or
   * more snapshots. The _UPDATE_HEADER state will be skipped if the
   * image was concurrently flattened by another client.
   */
  enum State {
    STATE_FLATTEN_OBJECTS,
    STATE_UPDATE_HEADER,
    STATE_UPDATE_CHILDREN
  };

  uint64_t m_object_size;
  uint64_t m_overlap_objects;
  ::SnapContext m_snapc;
  ProgressContext &m_prog_ctx;
  State m_state;

  parent_spec m_parent_spec;
  bool m_ignore_enoent;

  bool send_update_header();
  bool send_update_children();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::FlattenRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_FLATTEN_REQUEST_H
