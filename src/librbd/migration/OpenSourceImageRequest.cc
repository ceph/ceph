// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/OpenSourceImageRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
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
    librados::IoCtx& io_ctx, I* dst_image_ctx, uint64_t src_snap_id,
    const MigrationInfo &migration_info, I** src_image_ctx, Context* on_finish)
  : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())), m_io_ctx(io_ctx),
    m_dst_image_ctx(dst_image_ctx), m_src_snap_id(src_snap_id),
    m_migration_info(migration_info), m_src_image_ctx(src_image_ctx),
    m_on_finish(on_finish) {
  ldout(m_cct, 10) << dendl;
}

template <typename I>
void OpenSourceImageRequest<I>::send() {
  open_source();
}

template <typename I>
void OpenSourceImageRequest<I>::open_source() {
  ldout(m_cct, 10) << dendl;

  bool import_only = true;
  auto source_spec = m_migration_info.source_spec;
  if (source_spec.empty()) {
    // implies legacy migration from RBD image in same cluster
    source_spec = NativeFormat<I>::build_source_spec(
      m_migration_info.pool_id, m_migration_info.pool_namespace,
      m_migration_info.image_name, m_migration_info.image_id);
    import_only = false;
  }

  ldout(m_cct, 15) << "source_spec=" << source_spec << ", "
                   << "source_snap_id=" << m_src_snap_id << ", "
                   << "import_only=" << import_only << dendl;

  SourceSpecBuilder<I> source_spec_builder{m_cct};
  json_spirit::mObject source_spec_object;
  int r = source_spec_builder.parse_source_spec(source_spec,
                                                &source_spec_object);
  if (r < 0) {
    lderr(m_cct) << "failed to parse migration source-spec:" << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  r = source_spec_builder.build_format(source_spec_object, import_only,
                                       &m_format);
  if (r < 0) {
    lderr(m_cct) << "failed to build migration format handler: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  auto ctx = util::create_context_callback<
    OpenSourceImageRequest<I>,
    &OpenSourceImageRequest<I>::handle_open_source>(this);
  m_format->open(m_io_ctx, m_dst_image_ctx, m_src_image_ctx, ctx);
}

template <typename I>
void OpenSourceImageRequest<I>::handle_open_source(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to open migration source: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  get_image_size();
}

template <typename I>
void OpenSourceImageRequest<I>::get_image_size() {
  ldout(m_cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    OpenSourceImageRequest<I>,
    &OpenSourceImageRequest<I>::handle_get_image_size>(this);
  m_format->get_image_size(CEPH_NOSNAP, &m_image_size, ctx);
}

template <typename I>
void OpenSourceImageRequest<I>::handle_get_image_size(int r) {
  ldout(m_cct, 10) << "r=" << r << ", "
                   << "image_size=" << m_image_size << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve image size: " << cpp_strerror(r)
                 << dendl;
    close_image(r);
    return;
  }

  auto src_image_ctx = *m_src_image_ctx;
  src_image_ctx->image_lock.lock();
  src_image_ctx->size = m_image_size;
  src_image_ctx->image_lock.unlock();

  get_snapshots();
}

template <typename I>
void OpenSourceImageRequest<I>::get_snapshots() {
  ldout(m_cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    OpenSourceImageRequest<I>,
    &OpenSourceImageRequest<I>::handle_get_snapshots>(this);
  m_format->get_snapshots(&m_snap_infos, ctx);
}

template <typename I>
void OpenSourceImageRequest<I>::handle_get_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve snapshots: " << cpp_strerror(r)
                 << dendl;
    close_image(r);
    return;
  }

  // copy snapshot metadata to image ctx
  auto src_image_ctx = *m_src_image_ctx;
  src_image_ctx->image_lock.lock();

  src_image_ctx->snaps.clear();
  src_image_ctx->snap_info.clear();
  src_image_ctx->snap_ids.clear();

  ::SnapContext snapc;
  for (auto it = m_snap_infos.rbegin(); it != m_snap_infos.rend(); ++it) {
    auto& [snap_id, snap_info] = *it;
    snapc.snaps.push_back(snap_id);

    ldout(m_cct, 10) << "adding snap: ns=" << snap_info.snap_namespace << ", "
                     << "name=" << snap_info.name << ", "
                     << "id=" << snap_id << dendl;
    src_image_ctx->add_snap(
      snap_info.snap_namespace, snap_info.name, snap_id,
      snap_info.size, snap_info.parent, snap_info.protection_status,
      snap_info.flags, snap_info.timestamp);
  }
  if (!snapc.snaps.empty()) {
    snapc.seq = snapc.snaps[0];
  }
  src_image_ctx->snapc = snapc;

  ldout(m_cct, 15) << "read snap id: " << m_src_snap_id << ", "
                   << "write snapc={"
                   << "seq=" << snapc.seq << ", "
                   << "snaps=" << snapc.snaps << "}" << dendl;

  // ensure data_ctx and data_io_context are pointing to correct snapshot
  if (m_src_snap_id != CEPH_NOSNAP) {
    int r = src_image_ctx->snap_set(m_src_snap_id);
    if (r < 0) {
      src_image_ctx->image_lock.unlock();

      lderr(m_cct) << "error setting source image snap id: "
                   << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
  }

  src_image_ctx->image_lock.unlock();

  finish(0);
}

template <typename I>
void OpenSourceImageRequest<I>::close_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  auto ctx = new LambdaContext([this, r](int) {
    finish(r);
  });
  (*m_src_image_ctx)->state->close(ctx);
}

template <typename I>
void OpenSourceImageRequest<I>::register_image_dispatch() {
  ldout(m_cct, 10) << dendl;

  // intercept any IO requests to the source image
  auto io_image_dispatch = ImageDispatch<I>::create(
    *m_src_image_ctx, std::move(m_format));
  (*m_src_image_ctx)->io_image_dispatcher->register_dispatch(io_image_dispatch);
}

template <typename I>
void OpenSourceImageRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    *m_src_image_ctx = nullptr;
  } else {
    register_image_dispatch();
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace migration
} // namespace librbd

template class librbd::migration::OpenSourceImageRequest<librbd::ImageCtx>;
