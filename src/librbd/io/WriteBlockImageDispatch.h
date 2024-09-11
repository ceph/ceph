// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_WRITE_BLOCK_IMAGE_DISPATCH_H
#define CEPH_LIBRBD_IO_WRITE_BLOCK_IMAGE_DISPATCH_H

#include "librbd/io/ImageDispatchInterface.h"
#include "include/int_types.h"
#include "include/buffer.h"
#include "common/ceph_mutex.h"
#include "common/zipkin_trace.h"
#include "common/Throttle.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Types.h"
#include <list>

struct Context;

namespace librbd {

struct ImageCtx;

namespace io {

struct AioCompletion;

template <typename ImageCtxT>
class WriteBlockImageDispatch : public ImageDispatchInterface {
public:
  WriteBlockImageDispatch(ImageCtxT* image_ctx);

  ImageDispatchLayer get_dispatch_layer() const override {
    return IMAGE_DISPATCH_LAYER_WRITE_BLOCK;
  }

  void shut_down(Context* on_finish) override;

  int block_writes();
  void block_writes(Context *on_blocked);
  void unblock_writes();

  inline bool writes_blocked() const {
    std::shared_lock locker{m_lock};
    return (m_write_blockers > 0);
  }

  void wait_on_writes_unblocked(Context *on_unblocked);

  bool read(
      AioCompletion* aio_comp, Extents &&image_extents,
      ReadResult &&read_result, IOContext io_context, int op_flags,
      int read_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }
  bool write(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool discard(
      AioCompletion* aio_comp, Extents &&image_extents,
      uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool write_same(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool compare_and_write(
      AioCompletion* aio_comp, Extents &&image_extents,
      bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool flush(
      AioCompletion* aio_comp, FlushSource flush_source,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool list_snaps(
      AioCompletion* aio_comp, Extents&& image_extents, SnapIds&& snap_ids,
      int list_snaps_flags, SnapshotDelta* snapshot_delta,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

private:
  struct C_BlockedWrites;

  typedef std::list<Context*> Contexts;

  ImageCtxT* m_image_ctx;

  mutable ceph::shared_mutex m_lock;
  Contexts m_on_dispatches;

  uint32_t m_write_blockers = 0;
  Contexts m_write_blocker_contexts;
  Contexts m_unblocked_write_waiter_contexts;
  uint64_t m_in_flight_writes = 0;

  void handle_finished(int r, uint64_t tid);

  bool process_io(uint64_t tid, DispatchResult* dispatch_result,
                  Context** on_finish, Context* on_dispatched);
  void flush_io(Context* on_finish);

  bool invalidate_cache(Context* on_finish) override {
    return false;
  }

  void handle_blocked_writes(int r);

};

} // namespace io
} // namespace librbd

extern template class librbd::io::WriteBlockImageDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_WRITE_BLOCK_IMAGE_DISPATCH_H
