// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_QOS_IMAGE_DISPATCH_H
#define CEPH_LIBRBD_IO_QOS_IMAGE_DISPATCH_H

#include "librbd/io/ImageDispatchInterface.h"
#include "include/int_types.h"
#include "include/buffer.h"
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
template <typename> class FlushTracker;

template <typename ImageCtxT>
class QosImageDispatch : public ImageDispatchInterface {
public:
  struct Tag {
    std::atomic<uint32_t>* image_dispatch_flags;
    Context* on_dispatched;

    Tag(std::atomic<uint32_t>* image_dispatch_flags, Context* on_dispatched)
      : image_dispatch_flags(image_dispatch_flags),
        on_dispatched(on_dispatched) {
    }
  };

  QosImageDispatch(ImageCtxT* image_ctx);
  ~QosImageDispatch() override;

  ImageDispatchLayer get_dispatch_layer() const override {
    return IMAGE_DISPATCH_LAYER_QOS;
  }

  void shut_down(Context* on_finish) override;

  void apply_qos_schedule_tick_min(uint64_t tick);
  void apply_qos_limit(uint64_t flag, uint64_t limit, uint64_t burst,
                       uint64_t burst_seconds);

  bool read(
      AioCompletion* aio_comp, Extents &&image_extents,
      ReadResult &&read_result, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;
  bool write(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;
  bool discard(
      AioCompletion* aio_comp, Extents &&image_extents,
      uint32_t discard_granularity_bytes,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;
  bool write_same(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;
  bool compare_and_write(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&cmp_bl,
      bufferlist &&bl, uint64_t *mismatch_offset, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;
  bool flush(
      AioCompletion* aio_comp, FlushSource flush_source,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;

  void handle_finished(int r, uint64_t tid) override;

private:
  ImageCtxT* m_image_ctx;

  std::list<std::pair<uint64_t, TokenBucketThrottle*> > m_throttles;
  uint64_t m_qos_enabled_flag = 0;

  FlushTracker<ImageCtxT>* m_flush_tracker;

  bool set_throttle_flag(std::atomic<uint32_t>* image_dispatch_flags,
                         uint32_t flag);
  bool needs_throttle(bool read_op, const Extents& image_extents, uint64_t tid,
                      std::atomic<uint32_t>* image_dispatch_flags,
                      DispatchResult* dispatch_result, Context* on_dispatched);
  void handle_throttle_ready(Tag&& tag, uint64_t flag);

};

} // namespace io
} // namespace librbd

extern template class librbd::io::QosImageDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_QOS_IMAGE_DISPATCH_H
