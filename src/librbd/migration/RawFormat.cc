// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/RawFormat.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ReadResult.h"
#include "librbd/migration/FileStream.h"
#include "librbd/migration/SourceSpecBuilder.h"
#include "librbd/migration/StreamInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::RawFormat: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace migration {

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::RawFormat::OpenRequest " \
                           << this << " " << __func__ << ": "

template <typename I>
struct RawFormat<I>::OpenRequest {
  RawFormat* raw_format;
  Context* on_finish;

  uint64_t image_size = 0;

  OpenRequest(RawFormat* raw_format, Context* on_finish)
    : raw_format(raw_format), on_finish(on_finish) {
  }

  void send() {
    open_stream();
  }

  void open_stream() {
    auto cct = raw_format->m_image_ctx->cct;
    ldout(cct, 10) << dendl;

    auto ctx = util::create_context_callback<
      OpenRequest, &OpenRequest::handle_open_stream>(this);
    raw_format->m_stream->open(ctx);
  }

  void handle_open_stream(int r) {
    auto cct = raw_format->m_image_ctx->cct;
    ldout(cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to open stream: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    get_image_size();
  }

  void get_image_size() {
    auto cct = raw_format->m_image_ctx->cct;
    ldout(cct, 10) << dendl;

    auto ctx = util::create_context_callback<
      OpenRequest, &OpenRequest::handle_get_image_size>(this);
    raw_format->get_image_size(CEPH_NOSNAP, &image_size, ctx);
  }

  void handle_get_image_size(int r) {
    auto cct = raw_format->m_image_ctx->cct;
    ldout(cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to open stream: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    raw_format->m_image_ctx->image_lock.lock();
    raw_format->m_image_ctx->size = image_size;
    raw_format->m_image_ctx->image_lock.unlock();

    finish(0);
  }

  void finish(int r) {
    auto cct = raw_format->m_image_ctx->cct;
    ldout(cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      raw_format->m_image_ctx->state->close(new LambdaContext(
        [r, on_finish=on_finish](int _) { on_finish->complete(r); }));
    } else {
      on_finish->complete(0);
    }

    delete this;
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::RawFormat: " << this \
                           << " " << __func__ << ": "

template <typename I>
RawFormat<I>::RawFormat(
    I* image_ctx, const json_spirit::mObject& json_object,
    const SourceSpecBuilder<I>* source_spec_builder)
  : m_image_ctx(image_ctx), m_json_object(json_object),
    m_source_spec_builder(source_spec_builder) {
}

template <typename I>
void RawFormat<I>::open(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  int r = m_source_spec_builder->build_stream(m_json_object, &m_stream);
  if (r < 0) {
    lderr(cct) << "failed to build migration stream handler" << cpp_strerror(r)
               << dendl;
    m_image_ctx->state->close(
      new LambdaContext([r, on_finish](int _) { on_finish->complete(r); }));
    return;
  }

  auto req = new OpenRequest(this, on_finish);
  req->send();
}

template <typename I>
void RawFormat<I>::close(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  if (!m_stream) {
    on_finish->complete(0);
    return;
  }

  m_stream->close(on_finish);
}

template <typename I>
void RawFormat<I>::get_snapshots(SnapInfos* snap_infos, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  snap_infos->clear();
  on_finish->complete(0);
}

template <typename I>
void RawFormat<I>::get_image_size(uint64_t snap_id, uint64_t* size,
                                  Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  if (snap_id != CEPH_NOSNAP) {
    on_finish->complete(-EINVAL);
    return;
  }

  m_stream->get_size(size, on_finish);
}

template <typename I>
bool RawFormat<I>::read(
    io::AioCompletion* aio_comp, uint64_t snap_id, io::Extents&& image_extents,
    io::ReadResult&& read_result, int op_flags, int read_flags,
    const ZTracer::Trace &parent_trace) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  if (snap_id != CEPH_NOSNAP) {
    aio_comp->fail(-EINVAL);
    return true;
  }

  aio_comp->read_result = std::move(read_result);
  aio_comp->read_result.set_image_extents(image_extents);

  aio_comp->set_request_count(1);
  auto ctx = new io::ReadResult::C_ImageReadRequest(aio_comp,
                                                    image_extents);

  // raw directly maps the image-extent IO down to a byte IO extent
  m_stream->read(std::move(image_extents), &ctx->bl, ctx);
  return true;
}

template <typename I>
void RawFormat<I>::list_snaps(io::Extents&& image_extents,
                              io::SnapIds&& snap_ids, int list_snaps_flags,
                              io::SnapshotDelta* snapshot_delta,
                              const ZTracer::Trace &parent_trace,
                              Context* on_finish) {
  // raw does support snapshots so list the full IO extent as a delta
  auto& snapshot = (*snapshot_delta)[{CEPH_NOSNAP, CEPH_NOSNAP}];
  for (auto& image_extent : image_extents) {
    snapshot.insert(image_extent.first, image_extent.second,
                    {io::SPARSE_EXTENT_STATE_DATA, image_extent.second});
  }
  on_finish->complete(0);
}

} // namespace migration
} // namespace librbd

template class librbd::migration::RawFormat<librbd::ImageCtx>;
