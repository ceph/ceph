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

template <typename ImageCtxT = ImageCtx>
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

    Read(ReadResult &&read_result) : read_result(std::move(read_result)) {
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

  typedef boost::variant<Read,
                         Discard,
                         Write,
                         WriteSame,
                         CompareAndWrite,
                         Flush> Request;

  C_Dispatcher dispatcher_ctx;

  ImageDispatcherInterface* image_dispatcher;
  ImageDispatchLayer dispatch_layer;
  std::atomic<uint32_t> image_dispatch_flags = 0;
  DispatchResult dispatch_result = DISPATCH_RESULT_INVALID;

  AioCompletion* aio_comp;
  Extents image_extents;
  Request request;
  int op_flags;
  ZTracer::Trace parent_trace;
  uint64_t tid;

  static ImageDispatchSpec* create_read(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, Extents &&image_extents,
      ReadResult &&read_result, int op_flags,
      const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx, image_dispatch_layer, aio_comp,
                                 std::move(image_extents),
                                 Read{std::move(read_result)},
                                 op_flags, parent_trace, 0);
  }

  static ImageDispatchSpec* create_discard(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, uint64_t off, uint64_t len,
      uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
      uint64_t tid) {
    return new ImageDispatchSpec(image_ctx, image_dispatch_layer, aio_comp,
                                 {{off, len}},
                                 Discard{discard_granularity_bytes},
                                 0, parent_trace, tid);
  }

  static ImageDispatchSpec* create_write(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, Extents &&image_extents,
      bufferlist &&bl, int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid) {
    return new ImageDispatchSpec(image_ctx, image_dispatch_layer, aio_comp,
                                 std::move(image_extents), Write{std::move(bl)},
                                 op_flags, parent_trace, tid);
  }

  static ImageDispatchSpec* create_write_same(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, uint64_t off, uint64_t len,
      bufferlist &&bl, int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid) {
    return new ImageDispatchSpec(image_ctx, image_dispatch_layer, aio_comp,
                                 {{off, len}}, WriteSame{std::move(bl)},
                                 op_flags, parent_trace, tid);
  }

  static ImageDispatchSpec* create_compare_and_write(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, Extents &&image_extents,
      bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
      int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid) {
    return new ImageDispatchSpec(image_ctx, image_dispatch_layer, aio_comp,
                                 std::move(image_extents),
                                 CompareAndWrite{std::move(cmp_bl),
                                                 std::move(bl),
                                                 mismatch_offset},
                                 op_flags, parent_trace, tid);
  }

  static ImageDispatchSpec* create_flush(
      ImageCtxT &image_ctx, ImageDispatchLayer image_dispatch_layer,
      AioCompletion *aio_comp, FlushSource flush_source,
      const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx, image_dispatch_layer, aio_comp, {},
                                 Flush{flush_source}, 0, parent_trace, 0);
  }

  ~ImageDispatchSpec() {
    aio_comp->put();
  }

  void send();
  void fail(int r);

  bool is_write_op() const;

  void start_op();

  const Extents& get_image_extents() const;

  AioCompletion* get_aio_completion() const {
    return aio_comp;
  }

  uint64_t get_tid();
  bool blocked = false;

private:
  struct SendVisitor;
  struct IsWriteOpVisitor;
  struct TokenRequestedVisitor;

  ImageDispatchSpec(ImageCtxT& image_ctx,
                    ImageDispatchLayer image_dispatch_layer,
                    AioCompletion* aio_comp, Extents&& image_extents,
                    Request&& request, int op_flags,
                    const ZTracer::Trace& parent_trace, uint64_t tid)
    : dispatcher_ctx(this), image_dispatcher(image_ctx.io_image_dispatcher),
      dispatch_layer(image_dispatch_layer), aio_comp(aio_comp),
      image_extents(std::move(image_extents)), request(std::move(request)),
      op_flags(op_flags), parent_trace(parent_trace), tid(tid) {
    aio_comp->image_dispatcher_ctx = &dispatcher_ctx;
    aio_comp->get();
  }

  void finish(int r);

  uint64_t extents_length();
};

} // namespace io
} // namespace librbd

extern template class librbd::io::ImageDispatchSpec<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_IMAGE_DISPATCH_SPEC_H
