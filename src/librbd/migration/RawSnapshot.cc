// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/RawSnapshot.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ReadResult.h"
#include "librbd/migration/SourceSpecBuilder.h"
#include "librbd/migration/StreamInterface.h"

namespace librbd {
namespace migration {

namespace {

const std::string NAME_KEY{"name"};

} // anonymous namespace

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::RawSnapshot::OpenRequest " \
                           << this << " " << __func__ << ": "

template <typename I>
struct RawSnapshot<I>::OpenRequest {
  RawSnapshot* raw_snapshot;
  Context* on_finish;

  OpenRequest(RawSnapshot* raw_snapshot, Context* on_finish)
    : raw_snapshot(raw_snapshot), on_finish(on_finish) {
  }

  void send() {
    open_stream();
  }

  void open_stream() {
    auto cct = raw_snapshot->m_image_ctx->cct;
    ldout(cct, 10) << dendl;

    auto ctx = util::create_context_callback<
      OpenRequest, &OpenRequest::handle_open_stream>(this);
    raw_snapshot->m_stream->open(ctx);
  }

  void handle_open_stream(int r) {
    auto cct = raw_snapshot->m_image_ctx->cct;
    ldout(cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to open stream: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    get_image_size();
  }

  void get_image_size() {
    auto cct = raw_snapshot->m_image_ctx->cct;
    ldout(cct, 10) << dendl;

    auto ctx = util::create_context_callback<
      OpenRequest, &OpenRequest::handle_get_image_size>(this);
    raw_snapshot->m_stream->get_size(&raw_snapshot->m_snap_info.size, ctx);
  }

  void handle_get_image_size(int r) {
    auto cct = raw_snapshot->m_image_ctx->cct;
    ldout(cct, 10) << "r=" << r << ", "
                   << "image_size=" << raw_snapshot->m_snap_info.size << dendl;

    if (r < 0) {
      lderr(cct) << "failed to open stream: " << cpp_strerror(r) << dendl;
      close_stream(r);
      return;
    }

    finish(0);
  }

  void close_stream(int r) {
    auto cct = raw_snapshot->m_image_ctx->cct;
    ldout(cct, 10) << dendl;

    auto ctx = new LambdaContext([this, r](int) {
      handle_close_stream(r);
    });
    raw_snapshot->m_stream->close(ctx);
  }

  void handle_close_stream(int r) {
    auto cct = raw_snapshot->m_image_ctx->cct;
    ldout(cct, 10) << "r=" << r << dendl;

    raw_snapshot->m_stream.reset();

    finish(r);
  }

  void finish(int r) {
    auto cct = raw_snapshot->m_image_ctx->cct;
    ldout(cct, 10) << "r=" << r << dendl;

    on_finish->complete(r);
    delete this;
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::RawSnapshot: " << this \
                           << " " << __func__ << ": "

template <typename I>
RawSnapshot<I>::RawSnapshot(I* image_ctx,
                            const json_spirit::mObject& json_object,
                            const SourceSpecBuilder<I>* source_spec_builder,
                            uint64_t index)
  : m_image_ctx(image_ctx), m_json_object(json_object),
    m_source_spec_builder(source_spec_builder), m_index(index),
    m_snap_info({}, {}, 0, {}, 0, 0, {}) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;
}

template <typename I>
void RawSnapshot<I>::open(SnapshotInterface* previous_snapshot,
                          Context* on_finish) {
  auto cct = m_image_ctx->cct;

  // special-case for treating the HEAD revision as a snapshot
  if (m_index != CEPH_NOSNAP) {
    auto& name_val = m_json_object[NAME_KEY];
    if (name_val.type() == json_spirit::str_type) {
      m_snap_info.name = name_val.get_str();
    } else if (name_val.type() == json_spirit::null_type) {
      uuid_d uuid_gen;
      uuid_gen.generate_random();

      m_snap_info.name = uuid_gen.to_string();
    } else {
      lderr(cct) << "invalid snapshot name" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
  }

  ldout(cct, 10) << "name=" << m_snap_info.name << dendl;

  int r = m_source_spec_builder->build_stream(m_image_ctx, m_json_object,
                                              &m_stream);
  if (r < 0) {
    lderr(cct) << "failed to build migration stream handler" << cpp_strerror(r)
               << dendl;
    on_finish->complete(r);
    return;
  }

  auto req = new OpenRequest(this, on_finish);
  req->send();
}

template <typename I>
void RawSnapshot<I>::close(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  if (!m_stream) {
    on_finish->complete(0);
    return;
  }

  m_stream->close(on_finish);
}

template <typename I>
void RawSnapshot<I>::read(io::AioCompletion* aio_comp,
                          io::Extents&& image_extents,
                          io::ReadResult&& read_result, int op_flags,
                          int read_flags,
                          const ZTracer::Trace &parent_trace) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  aio_comp->read_result = std::move(read_result);
  aio_comp->read_result.set_image_extents(image_extents);

  aio_comp->set_request_count(1);
  auto ctx = new io::ReadResult::C_ImageReadRequest(aio_comp,
                                                    0, image_extents);

  // raw directly maps the image-extent IO down to a byte IO extent
  m_stream->read(std::move(image_extents), &ctx->bl, ctx);
}

template <typename I>
void RawSnapshot<I>::list_snap(io::Extents&& image_extents,
                               int list_snaps_flags,
                               io::SparseExtents* sparse_extents,
                               const ZTracer::Trace &parent_trace,
                               Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  // raw does support sparse extents so list the full IO extent as a delta
  for (auto& [image_offset, image_length] : image_extents) {
    sparse_extents->insert(image_offset, image_length,
                           {io::SPARSE_EXTENT_STATE_DATA, image_length});
  }

  on_finish->complete(0);
}

} // namespace migration
} // namespace librbd

template class librbd::migration::RawSnapshot<librbd::ImageCtx>;
