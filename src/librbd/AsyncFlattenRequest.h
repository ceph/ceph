// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_ASYNC_FLATTEN_REQUEST_H
#define CEPH_LIBRBD_ASYNC_FLATTEN_REQUEST_H

#include "librbd/AsyncRequest.h"
#include "librbd/parent_types.h"
#include "common/snap_types.h"

namespace librbd {

class ImageCtx;
class ProgressContext;

class AsyncFlattenRequest : public AsyncRequest
{
public:
  AsyncFlattenRequest(ImageCtx &image_ctx, Context *on_finish,
		      uint64_t object_size, uint64_t overlap_objects,
		      const ::SnapContext &snapc, ProgressContext &prog_ctx)
    : AsyncRequest(image_ctx, on_finish), m_object_size(object_size),
      m_overlap_objects(overlap_objects), m_snapc(snapc), m_prog_ctx(prog_ctx),
      m_ignore_enoent(false)
  {
  }

  virtual void send();

protected:
  virtual bool should_complete(int r);

private:
  /**
   * Flatten goes through the following state machine to copyup objects
   * from the parent image:
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

} // namespace librbd

#endif // CEPH_LIBRBD_ASYNC_FLATTEN_REQUEST_H
