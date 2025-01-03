// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_IMAGE_DISPATCH_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_IMAGE_DISPATCH_H

#include "librbd/io/ImageDispatchInterface.h"
#include "include/int_types.h"
#include "include/buffer.h"
#include "common/ceph_mutex.h"
#include "common/zipkin_trace.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Types.h"
#include <atomic>
#include <list>
#include <unordered_set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace io {
struct AioCompletion;
}

namespace exclusive_lock {

template <typename ImageCtxT>
class ImageDispatch : public io::ImageDispatchInterface {
public:
  static ImageDispatch* create(ImageCtxT* image_ctx) {
    return new ImageDispatch(image_ctx);
  }
  void destroy() {
    delete this;
  }

  ImageDispatch(ImageCtxT* image_ctx);

  io::ImageDispatchLayer get_dispatch_layer() const override {
    return io::IMAGE_DISPATCH_LAYER_EXCLUSIVE_LOCK;
  }

  void set_require_lock(bool init_shutdown,
                        io::Direction direction, Context* on_finish);
  void unset_require_lock(io::Direction direction);

  void shut_down(Context* on_finish) override;

  bool read(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      io::ReadResult &&read_result, IOContext io_context, int op_flags,
      int read_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool write(
      io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool discard(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool write_same(
      io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool compare_and_write(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool flush(
      io::AioCompletion* aio_comp, io::FlushSource flush_source,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool list_snaps(
      io::AioCompletion* aio_comp, io::Extents&& image_extents,
      io::SnapIds&& snap_ids, int list_snaps_flags,
      io::SnapshotDelta* snapshot_delta, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool invalidate_cache(Context* on_finish) override {
    return false;
  }

private:
  typedef std::list<Context*> Contexts;
  typedef std::unordered_set<uint64_t> Tids;

  ImageCtxT* m_image_ctx;
  mutable ceph::shared_mutex m_lock;

  bool m_require_lock_on_read = false;
  bool m_require_lock_on_write = false;

  Contexts m_on_dispatches;

  bool set_require_lock(io::Direction direction, bool enabled);

  bool is_lock_required(bool read_op) const;

  bool needs_exclusive_lock(bool read_op, uint64_t tid,
                            io::DispatchResult* dispatch_result,
                            Context* on_dispatched);

  void handle_acquire_lock(int r);
};

} // namespace exclusiv_lock
} // namespace librbd

extern template class librbd::exclusive_lock::ImageDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_IMAGE_DISPATCH_H
