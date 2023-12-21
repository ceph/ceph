// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_DISPATCHER_H
#define CEPH_LIBRBD_IO_IMAGE_DISPATCHER_H

#include "include/int_types.h"
#include "common/ceph_mutex.h"
#include "librbd/io/Dispatcher.h"
#include "librbd/io/ImageDispatchInterface.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/io/Types.h"
#include <atomic>
#include <map>

struct Context;

namespace librbd {

struct ImageCtx;

namespace io {

template <typename> struct QosImageDispatch;
template <typename> struct WriteBlockImageDispatch;

template <typename ImageCtxT = ImageCtx>
class ImageDispatcher : public Dispatcher<ImageCtxT, ImageDispatcherInterface> {
public:
  ImageDispatcher(ImageCtxT* image_ctx);

  void invalidate_cache(Context* on_finish) override;

  void shut_down(Context* on_finish) override;

  void apply_qos_schedule_tick_min(uint64_t tick) override;
  void apply_qos_limit(uint64_t flag, uint64_t limit, uint64_t burst,
                       uint64_t burst_seconds) override;
  void apply_qos_exclude_ops(uint64_t exclude_ops) override;

  bool writes_blocked() const override;
  int block_writes() override;
  void block_writes(Context *on_blocked) override;

  void unblock_writes() override;
  void wait_on_writes_unblocked(Context *on_unblocked) override;

protected:
  bool send_dispatch(
    ImageDispatchInterface* image_dispatch,
    ImageDispatchSpec* image_dispatch_spec) override;

private:
  struct SendVisitor;
  struct PreprocessVisitor;

  using typename Dispatcher<ImageCtxT, ImageDispatcherInterface>::C_InvalidateCache;

  std::atomic<uint64_t> m_next_tid{0};

  QosImageDispatch<ImageCtxT>* m_qos_image_dispatch = nullptr;
  WriteBlockImageDispatch<ImageCtxT>* m_write_block_dispatch = nullptr;

  bool preprocess(ImageDispatchSpec* image_dispatch_spec);

};

} // namespace io
} // namespace librbd

extern template class librbd::io::ImageDispatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_IMAGE_DISPATCHER_H
