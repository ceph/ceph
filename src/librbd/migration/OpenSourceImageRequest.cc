// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/OpenSourceImageRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/io/ImageDispatcher.h"
#include "librbd/migration/ImageDispatch.h"
#include "librbd/migration/NativeFormat.h"
#include "librbd/migration/SourceSpecBuilder.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::OpenSourceImageRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace migration {

template <typename I>
OpenSourceImageRequest<I>::OpenSourceImageRequest(
    I* dst_image_ctx, uint64_t src_snap_id,
    const MigrationInfo &migration_info, I** src_image_ctx, Context* on_finish)
  : m_dst_image_ctx(dst_image_ctx), m_src_snap_id(src_snap_id),
    m_migration_info(migration_info), m_src_image_ctx(src_image_ctx),
    m_on_finish(on_finish) {
  auto cct = m_dst_image_ctx->cct;
  ldout(cct, 10) << dendl;
}

template <typename I>
void OpenSourceImageRequest<I>::send() {
  open_source();
}

template <typename I>
void OpenSourceImageRequest<I>::open_source() {
  auto cct = m_dst_image_ctx->cct;
  ldout(cct, 10) << dendl;

  // note that all source image ctx properties are placeholders
  *m_src_image_ctx = I::create("", "", m_src_snap_id, m_dst_image_ctx->md_ctx,
                               true);
  (*m_src_image_ctx)->child = m_dst_image_ctx;

  auto source_spec = m_migration_info.source_spec;
  if (source_spec.empty()) {
    // implies legacy migration from RBD image in same cluster
    source_spec = NativeFormat<I>::build_source_spec(
      m_migration_info.pool_id, m_migration_info.pool_namespace,
      m_migration_info.image_name, m_migration_info.image_id);
  }

  SourceSpecBuilder<I> source_spec_builder{*m_src_image_ctx};
  json_spirit::mObject source_spec_object;
  int r = source_spec_builder.parse_source_spec(source_spec,
                                                &source_spec_object);
  if (r < 0) {
    lderr(cct) << "failed to parse migration source-spec:" << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  r = source_spec_builder.build_format(source_spec_object, &m_format);
  if (r < 0) {
    lderr(cct) << "failed to build migration format handler: "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  auto ctx = util::create_context_callback<
    OpenSourceImageRequest<I>,
    &OpenSourceImageRequest<I>::handle_open_source>(this);
  m_format->open(ctx);
}

template <typename I>
void OpenSourceImageRequest<I>::handle_open_source(int r) {
  auto cct = m_dst_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to open migration source: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  // intercept any IO requests to the source image
  auto io_image_dispatch = ImageDispatch<I>::create(
    *m_src_image_ctx, std::move(m_format));
  (*m_src_image_ctx)->io_image_dispatcher->register_dispatch(io_image_dispatch);

  finish(0);
}

template <typename I>
void OpenSourceImageRequest<I>::finish(int r) {
  auto cct = m_dst_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    delete *m_src_image_ctx;
    *m_src_image_ctx = nullptr;
  }
  m_on_finish->complete(r);
  delete this;
}

} // namespace migration
} // namespace librbd

template class librbd::migration::OpenSourceImageRequest<librbd::ImageCtx>;
