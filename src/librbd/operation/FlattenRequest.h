// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OPERATION_FLATTEN_REQUEST_H
#define CEPH_LIBRBD_OPERATION_FLATTEN_REQUEST_H

#include "librbd/operation/Request.h"

namespace librbd {

class ImageCtx;
class ProgressContext;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class FlattenRequest : public Request<ImageCtxT>
{
public:
  FlattenRequest(ImageCtxT &image_ctx, Context *on_finish,
                 uint64_t start_object_no, uint64_t overlap_objects,
                 ProgressContext& prog_ctx)
      : Request<ImageCtxT>(image_ctx, on_finish),
        m_start_object_no(start_object_no),
        m_overlap_objects(overlap_objects),
        m_prog_ctx(prog_ctx) {}

protected:
  void send_op() override;
  bool should_complete(int r) override;

  journal::Event create_event(uint64_t op_tid) const override {
    return journal::FlattenEvent(op_tid);
  }

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * FLATTEN_OBJECTS
   *    |
   *    v
   * CRYPTO_FLATTEN
   *    |
   *    v
   * DETACH_CHILD
   *    |
   *    v
   * DETACH_PARENT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  uint64_t m_start_object_no;
  uint64_t m_overlap_objects;
  ProgressContext &m_prog_ctx;

  void flatten_objects();
  void handle_flatten_objects(int r);


  void crypto_flatten();
  void handle_crypto_flatten(int r);

  void detach_child();
  void handle_detach_child(int r);

  void detach_parent();
  void handle_detach_parent(int r);

};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::FlattenRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_FLATTEN_REQUEST_H
