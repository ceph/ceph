// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_DISPATCH_SPEC_H
#define CEPH_LIBRBD_IO_IMAGE_DISPATCH_SPEC_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/zipkin_trace.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/Types.h"
#include "librbd/io/ReadResult.h"
#include <boost/variant/variant.hpp>
#include <atomic>

namespace librbd {

class ImageCtx;

namespace io {

struct ImageDispatcherInterface;

class ImageDispatchSpec {
private:
  // helper to avoid extra heap allocation per object IO
  struct C_Dispatcher : public Context {
    ImageDispatchSpec* image_dispatch_spec;

    C_Dispatcher(ImageDispatchSpec* image_dispatch_spec)
      : image_dispatch_spec(image_dispatch_spec) {
    }

    void complete(int r) override;
    void finish(int r) override;
  };

public:
  struct Read {
    ReadResult read_result;
    int read_flags;

    Read(ReadResult &&read_result, int read_flags)
      : read_result(std::move(read_result)), read_flags(read_flags) {
    }
  };

  struct Discard {
    uint32_t discard_granularity_bytes;

    Discard(uint32_t discard_granularity_bytes)
      : discard_granularity_bytes(discard_granularity_bytes) {
    }
  };

  struct Write {
    bufferlist bl;

    Write(bufferlist&& bl) : bl(std::move(bl)) {
    }
  };

  struct WriteSame {
    bufferlist bl;

    WriteSame(bufferlist&& bl) : bl(std::move(bl)) {
    }
  };

  struct CompareAndWrite {
    bufferlist cmp_bl;
    bufferlist bl;
    uint64_t *mismatch_offset;

    CompareAndWrite(bufferlist&& cmp_bl, bufferlist&& bl,
                    uint64_t *mismatch_offset)
      : cmp_bl(std::move(cmp_bl)), bl(std::move(bl)),
        mismatch_offset(mismatch_offset) {
    }
  };

  struct Flush {
    FlushSource flush_source;

    Flush(FlushSource flush_source) : flush_source(flush_source) {
    }
  };

  struct ListSnaps {
    SnapIds snap_ids;
    int list_snaps_flags;
    SnapshotDelta* snapshot_delta;

    ListSnaps(SnapIds&& snap_ids, int list_snaps_flags,
              SnapshotDelta* snapshot_delta)
      : snap_ids(std::move(snap_ids)), list_snaps_flags(list_snaps_flags),
        snapshot_delta(snapshot_delta) {
    }
  };

  typedef boost::variant<Read,
                         Discard,
                         Write,
                         WriteSame,
                         CompareAndWrite,
                         Flush,
                         ListSnaps> Request;

  C_Dispatcher dispatcher_ctx;

  ImageDispatcherInterface* image_dispatcher;
  ImageDispatchLayer dispatch_layer;
  std::atomic<uint32_t> image_dispatch_flags = 0;
  DispatchResult dispatch_result = DISPATCH_RESULT_INVALID;

  AioCompletion* aio_comp;
  Extents image_extents;
  Request request;
  IOContext io_context;
  int op_flags;
  ZTracer::Trace parent_trace;
  uint64_t tid = 0;

  template <typename ImageCtxT = ImageCtx>
  static ImageDispatchSpec* create_read(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, Extents &&image_extents, ImageArea area,
      ReadResult &&read_result, IOContext io_context, int op_flags,
      int read_flags, const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx.io_image_dispatcher,
                                 image_dispatch_layer, aio_comp,
                                 std::move(image_extents), area,
                                 Read{std::move(read_result), read_flags},
                                 io_context, op_flags, parent_trace);
  }

  template <typename ImageCtxT = ImageCtx>
  static ImageDispatchSpec* create_discard(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, Extents &&image_extents, ImageArea area,
      uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx.io_image_dispatcher,
                                 image_dispatch_layer, aio_comp,
                                 std::move(image_extents), area,
                                 Discard{discard_granularity_bytes},
                                 {}, 0, parent_trace);
  }

  template <typename ImageCtxT = ImageCtx>
  static ImageDispatchSpec* create_write(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, Extents &&image_extents, ImageArea area,
      bufferlist &&bl, int op_flags, const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx.io_image_dispatcher,
                                 image_dispatch_layer, aio_comp,
                                 std::move(image_extents), area,
                                 Write{std::move(bl)},
                                 {}, op_flags, parent_trace);
  }

  template <typename ImageCtxT = ImageCtx>
  static ImageDispatchSpec* create_write_same(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, Extents &&image_extents, ImageArea area,
      bufferlist &&bl, int op_flags, const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx.io_image_dispatcher,
                                 image_dispatch_layer, aio_comp,
                                 std::move(image_extents), area,
                                 WriteSame{std::move(bl)},
                                 {}, op_flags, parent_trace);
  }

  template <typename ImageCtxT = ImageCtx>
  static ImageDispatchSpec* create_compare_and_write(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, Extents &&image_extents, ImageArea area,
      bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
      int op_flags, const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx.io_image_dispatcher,
                                 image_dispatch_layer, aio_comp,
                                 std::move(image_extents), area,
                                 CompareAndWrite{std::move(cmp_bl),
                                                 std::move(bl),
                                                 mismatch_offset},
                                 {}, op_flags, parent_trace);
  }

  template <typename ImageCtxT = ImageCtx>
  static ImageDispatchSpec* create_flush(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, FlushSource flush_source,
      const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx.io_image_dispatcher,
                                 image_dispatch_layer, aio_comp, {},
                                 ImageArea::DATA /* dummy for {} */,
                                 Flush{flush_source}, {}, 0, parent_trace);
  }

  template <typename ImageCtxT = ImageCtx>
  static ImageDispatchSpec* create_list_snaps(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, Extents &&image_extents, ImageArea area,
      SnapIds&& snap_ids, int list_snaps_flags, SnapshotDelta* snapshot_delta,
      const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx.io_image_dispatcher,
                                 image_dispatch_layer, aio_comp,
                                 std::move(image_extents), area,
                                 ListSnaps{std::move(snap_ids),
                                           list_snaps_flags, snapshot_delta},
                                 {}, 0, parent_trace);
  }

  ~ImageDispatchSpec() {
    aio_comp->put();
  }

  void send();
  void fail(int r);

private:
  struct SendVisitor;
  struct IsWriteOpVisitor;
  struct TokenRequestedVisitor;

  ImageDispatchSpec(ImageDispatcherInterface* image_dispatcher,
                    ImageDispatchLayer image_dispatch_layer,
                    AioCompletion* aio_comp, Extents&& image_extents,
                    ImageArea area, Request&& request, IOContext io_context,
                    int op_flags, const ZTracer::Trace& parent_trace)
    : dispatcher_ctx(this), image_dispatcher(image_dispatcher),
      dispatch_layer(image_dispatch_layer), aio_comp(aio_comp),
      image_extents(std::move(image_extents)), request(std::move(request)),
      io_context(io_context), op_flags(op_flags), parent_trace(parent_trace) {
    ceph_assert(aio_comp->image_dispatcher_ctx == nullptr);
    aio_comp->image_dispatcher_ctx = &dispatcher_ctx;
    aio_comp->get();

    switch (area) {
    case ImageArea::DATA:
      break;
    case ImageArea::CRYPTO_HEADER:
      image_dispatch_flags |= IMAGE_DISPATCH_FLAG_CRYPTO_HEADER;
      break;
    default:
      ceph_abort();
    }
  }
};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_IMAGE_DISPATCH_SPEC_H
