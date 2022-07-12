// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OPERATION_SPARSIFY_REQUEST_H
#define CEPH_LIBRBD_OPERATION_SPARSIFY_REQUEST_H

#include "librbd/operation/Request.h"
#include "common/snap_types.h"

namespace librbd {

class ImageCtx;
class ProgressContext;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class SparsifyRequest : public Request<ImageCtxT>
{
public:
  SparsifyRequest(ImageCtxT &image_ctx, size_t sparse_size, Context *on_finish,
                  ProgressContext &prog_ctx)
    : Request<ImageCtxT>(image_ctx, on_finish), m_sparse_size(sparse_size),
      m_prog_ctx(prog_ctx) {
  }

protected:
  void send_op() override;
  bool should_complete(int r) override;
  bool can_affect_io() const override {
    return true;
  }
  journal::Event create_event(uint64_t op_tid) const override {
    ceph_abort();
    return journal::UnknownEvent();
  }

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * SPARSIFY OBJECTS
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  size_t m_sparse_size;
  ProgressContext &m_prog_ctx;

  void sparsify_objects();
  void handle_sparsify_objects(int r);
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::SparsifyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_SPARSIFY_REQUEST_H
