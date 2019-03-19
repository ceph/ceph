// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_DISPATCH_SPEC_H
#define CEPH_LIBRBD_IO_IMAGE_DISPATCH_SPEC_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "common/zipkin_trace.h"
#include "librbd/io/Types.h"
#include "librbd/io/ReadResult.h"
#include <boost/variant/variant.hpp>

namespace librbd {

class ImageCtx;

namespace io {

class AioCompletion;

template <typename ImageCtxT = ImageCtx>
class ImageDispatchSpec {
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

  static ImageDispatchSpec* create_read_request(
      ImageCtxT &image_ctx, AioCompletion *aio_comp, Extents &&image_extents,
      ReadResult &&read_result, int op_flags,
      const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx, aio_comp,
                                 std::move(image_extents),
                                 Read{std::move(read_result)},
                                 op_flags, parent_trace);
  }

  static ImageDispatchSpec* create_discard_request(
      ImageCtxT &image_ctx, AioCompletion *aio_comp, uint64_t off, uint64_t len,
      uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx, aio_comp, {{off, len}},
                                 Discard{discard_granularity_bytes},
                                 0, parent_trace);
  }

  static ImageDispatchSpec* create_write_request(
      ImageCtxT &image_ctx, AioCompletion *aio_comp, Extents &&image_extents,
      bufferlist &&bl, int op_flags, const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx, aio_comp, std::move(image_extents),
                                 Write{std::move(bl)}, op_flags, parent_trace);
  }

  static ImageDispatchSpec* create_write_same_request(
      ImageCtxT &image_ctx, AioCompletion *aio_comp, uint64_t off, uint64_t len,
      bufferlist &&bl, int op_flags, const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx, aio_comp, {{off, len}},
                                 WriteSame{std::move(bl)}, op_flags,
                                 parent_trace);
  }

  static ImageDispatchSpec* create_compare_and_write_request(
      ImageCtxT &image_ctx, AioCompletion *aio_comp, Extents &&image_extents,
      bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
      int op_flags, const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx, aio_comp,
                                 std::move(image_extents),
                                 CompareAndWrite{std::move(cmp_bl),
                                                 std::move(bl),
                                                 mismatch_offset},
                                 op_flags, parent_trace);
  }

  static ImageDispatchSpec* create_flush_request(
      ImageCtxT &image_ctx, AioCompletion *aio_comp,
      FlushSource flush_source, const ZTracer::Trace &parent_trace) {
    return new ImageDispatchSpec(image_ctx, aio_comp, {}, Flush{flush_source},
                                 0, parent_trace);
  }

  void send();
  void fail(int r);

  bool is_write_op() const;

  void start_op();

  bool tokens_requested(uint64_t flag, uint64_t *tokens);

  bool was_throttled(uint64_t flag) {
    return m_throttled_flag & flag;
  }

  void set_throttled(uint64_t flag) {
    m_throttled_flag |= flag;
  }

  bool were_all_throttled() {
    return (m_throttled_flag & RBD_QOS_MASK) == RBD_QOS_MASK;
  }

private:
  typedef boost::variant<Read,
                         Discard,
                         Write,
                         WriteSame,
                         CompareAndWrite,
                         Flush> Request;

  struct SendVisitor;
  struct IsWriteOpVisitor;
  struct TokenRequestedVisitor;

  ImageDispatchSpec(ImageCtxT& image_ctx, AioCompletion* aio_comp,
                     Extents&& image_extents, Request&& request,
                     int op_flags, const ZTracer::Trace& parent_trace)
    : m_image_ctx(image_ctx), m_aio_comp(aio_comp),
      m_image_extents(std::move(image_extents)), m_request(std::move(request)),
      m_op_flags(op_flags), m_parent_trace(parent_trace) {
  }

  ImageCtxT& m_image_ctx;
  AioCompletion* m_aio_comp;
  Extents m_image_extents;
  Request m_request;
  int m_op_flags;
  ZTracer::Trace m_parent_trace;
  std::atomic<uint64_t> m_throttled_flag = 0;

  uint64_t extents_length();
};

} // namespace io
} // namespace librbd

extern template class librbd::io::ImageDispatchSpec<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_IMAGE_DISPATCH_SPEC_H
