// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Timer.h"
#include "librbd/plugin/Api.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/Utils.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"

namespace librbd {
namespace plugin {

template <typename I>
void Api<I>::read_parent(
    I *image_ctx, uint64_t object_no, io::ReadExtents* extents,
    librados::snap_t snap_id, const ZTracer::Trace &trace,
    Context* on_finish) {
  io::util::read_parent<I>(image_ctx, object_no, extents, snap_id, trace,
                           on_finish);
}

template <typename I>
void Api<I>::execute_image_metadata_set(
    I *image_ctx, const std::string &key,
    const std::string &value, Context *on_finish) {
  ImageCtx* ictx = util::get_image_ctx(image_ctx);
  ictx->operations->execute_metadata_set(key, value, on_finish);
}

template <typename I>
void Api<I>::execute_image_metadata_remove(
    I *image_ctx, const std::string &key, Context *on_finish) {
  ImageCtx* ictx = util::get_image_ctx(image_ctx);
  ictx->operations->execute_metadata_remove(key, on_finish);
}

template <typename I>
void Api<I>::get_image_timer_instance(
    CephContext *cct, SafeTimer **timer, ceph::mutex **timer_lock) {
  ImageCtx::get_timer_instance(cct, timer, timer_lock);
}

template <typename I>
bool Api<I>::test_image_features(I *image_ctx, uint64_t features) {
  return image_ctx->test_features(features);
}

template <typename I>
void Api<I>::update_aio_comp(io::AioCompletion* aio_comp,
                             uint32_t request_count,
                             io::ReadResult &read_result,
                             io::Extents &image_extents) {
  aio_comp->set_request_count(request_count);
  aio_comp->read_result = std::move(read_result);
  aio_comp->read_result.set_image_extents(image_extents);
  start_in_flight_io(aio_comp);
}

template <typename I>
void Api<I>::update_aio_comp(
    io::AioCompletion* aio_comp, uint32_t request_count) {
  aio_comp->set_request_count(request_count);
  start_in_flight_io(aio_comp);
}

template <typename I>
io::ReadResult::C_ImageReadRequest* Api<I>::create_image_read_request(
    io::AioCompletion* aio_comp, uint64_t buffer_offset,
    const Extents& image_extents) {
  return new io::ReadResult::C_ImageReadRequest(
    aio_comp, buffer_offset, image_extents);
}

template <typename I>
io::C_AioRequest* Api<I>::create_aio_request(io::AioCompletion* aio_comp) {
  io::C_AioRequest *req_comp = new io::C_AioRequest(aio_comp);
  return req_comp;
}

template <typename I>
void Api<I>::start_in_flight_io(io::AioCompletion* aio_comp) {
  if (!aio_comp->async_op.started()) {
    aio_comp->start_op();
  }
}

} // namespace plugin
} // namespace librbd

template class librbd::plugin::Api<librbd::ImageCtx>;
