// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_PLUGIN_API_H
#define CEPH_LIBRBD_PLUGIN_API_H

#include "common/Timer.h"
#include "common/ceph_mutex.h"
#include "include/common_fwd.h"
#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "librbd/io/Types.h"
#include "librbd/io/ReadResult.h"

namespace ZTracer { struct Trace; }

namespace librbd {

namespace io {
class AioCompletion;
class C_AioRequest;
}

struct ImageCtx;

namespace plugin {

template <typename ImageCtxT>
struct Api {
  using Extents = librbd::io::Extents;

  Api() {}
  virtual ~Api() {}

  virtual void read_parent(
      ImageCtxT *image_ctx, uint64_t object_no, io::ReadExtents* extents,
      librados::snap_t snap_id, const ZTracer::Trace &trace,
      Context* on_finish);

  virtual void execute_image_metadata_set(
      ImageCtxT *image_ctx,
      const std::string &key,
      const std::string &value,
      Context *on_finish);

  virtual void execute_image_metadata_remove(
      ImageCtxT *image_ctx,
      const std::string &key,
      Context *on_finish);

  virtual void get_image_timer_instance(
      CephContext *cct, SafeTimer **timer,
      ceph::mutex **timer_lock);

  virtual bool test_image_features(
      ImageCtxT *image_ctx,
      uint64_t features);

  virtual void update_aio_comp(
      io::AioCompletion* aio_comp,
      uint32_t request_count,
      io::ReadResult& read_result,
      io::Extents &image_extents);

  virtual void update_aio_comp(
      io::AioCompletion* aio_comp,
      uint32_t request_count);

  virtual io::ReadResult::C_ImageReadRequest* create_image_read_request(
      io::AioCompletion* aio_comp, uint64_t buffer_offset,
      const Extents& image_extents);

  virtual io::C_AioRequest* create_aio_request(io::AioCompletion* aio_comp);

private:
  void start_in_flight_io(io::AioCompletion* aio_comp);
};

} // namespace plugin
} // namespace librbd

extern template class librbd::plugin::Api<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_PLUGIN_API_H
