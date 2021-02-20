// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SyncPointCreateRequest.h"
#include "include/uuid.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/image_sync/Types.h"
#include "tools/rbd_mirror/image_sync/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_sync::SyncPointCreateRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_sync {

using librbd::util::create_context_callback;

template <typename I>
SyncPointCreateRequest<I>::SyncPointCreateRequest(
    I *remote_image_ctx,
    const std::string &local_mirror_uuid,
    SyncPointHandler* sync_point_handler,
    Context *on_finish)
  : m_remote_image_ctx(remote_image_ctx),
    m_local_mirror_uuid(local_mirror_uuid),
    m_sync_point_handler(sync_point_handler),
    m_on_finish(on_finish) {
  m_sync_points_copy = m_sync_point_handler->get_sync_points();
  ceph_assert(m_sync_points_copy.size() < 2);

  // initialize the updated client meta with the new sync point
  m_sync_points_copy.emplace_back();
  if (m_sync_points_copy.size() > 1) {
    m_sync_points_copy.back().from_snap_name =
      m_sync_points_copy.front().snap_name;
  }
}

template <typename I>
void SyncPointCreateRequest<I>::send() {
  send_update_sync_points();
}

template <typename I>
void SyncPointCreateRequest<I>::send_update_sync_points() {
  uuid_d uuid_gen;
  uuid_gen.generate_random();

  auto& sync_point = m_sync_points_copy.back();
  sync_point.snap_name = util::get_snapshot_name_prefix(m_local_mirror_uuid) +
                         uuid_gen.to_string();

  auto ctx = create_context_callback<
    SyncPointCreateRequest<I>,
    &SyncPointCreateRequest<I>::handle_update_sync_points>(this);
  m_sync_point_handler->update_sync_points(
    m_sync_point_handler->get_snap_seqs(), m_sync_points_copy, false, ctx);
}

template <typename I>
void SyncPointCreateRequest<I>::handle_update_sync_points(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update client data: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  send_refresh_image();
}

template <typename I>
void SyncPointCreateRequest<I>::send_refresh_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    SyncPointCreateRequest<I>, &SyncPointCreateRequest<I>::handle_refresh_image>(
      this);
  m_remote_image_ctx->state->refresh(ctx);
}

template <typename I>
void SyncPointCreateRequest<I>::handle_refresh_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": remote image refresh failed: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_create_snap();
}

template <typename I>
void SyncPointCreateRequest<I>::send_create_snap() {
  dout(20) << dendl;

  auto& sync_point = m_sync_points_copy.back();

  Context *ctx = create_context_callback<
    SyncPointCreateRequest<I>, &SyncPointCreateRequest<I>::handle_create_snap>(
      this);
  m_remote_image_ctx->operations->snap_create(
    cls::rbd::UserSnapshotNamespace(), sync_point.snap_name.c_str(),
    librbd::SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE, m_prog_ctx, ctx);
}

template <typename I>
void SyncPointCreateRequest<I>::handle_create_snap(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == -EEXIST) {
    send_update_sync_points();
    return;
  } else if (r < 0) {
    derr << ": failed to create snapshot: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_final_refresh_image();
}

template <typename I>
void SyncPointCreateRequest<I>::send_final_refresh_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    SyncPointCreateRequest<I>,
    &SyncPointCreateRequest<I>::handle_final_refresh_image>(this);
  m_remote_image_ctx->state->refresh(ctx);
}

template <typename I>
void SyncPointCreateRequest<I>::handle_final_refresh_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to refresh image for snapshot: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void SyncPointCreateRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_sync::SyncPointCreateRequest<librbd::ImageCtx>;
