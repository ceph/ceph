// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "RemoveImageSnapshotRequest.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include <shared_mutex>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::" \
  << "RemoveImageSnapshotRequest: " << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace group_replayer {

using librbd::util::create_context_callback;

template <typename I>
void RemoveImageSnapshotRequest<I>::send() {
  dout(10) << "image_id=" << m_image_id << ", snap_id=" << m_snap_id
           << ", group_snap_id=" << m_group_snap_id << dendl;
  int r = librbd::util::create_ioctx(m_group_io_ctx, "local image pool",
                                     m_image_pool_id, {}, &m_image_io_ctx);
  if (r < 0) {
    derr << "failed to open image pool: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_image_ctx = I::create("", m_image_id, nullptr, m_image_io_ctx, false);
  m_image_ctx->read_only_mask &= ~librbd::IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
  auto ctx = create_context_callback<RemoveImageSnapshotRequest<I>,
    &RemoveImageSnapshotRequest<I>::handle_open_image>(this);
  m_image_ctx->state->open(librbd::OPEN_FLAG_SKIP_OPEN_PARENT, ctx);
}

template <typename I>
void RemoveImageSnapshotRequest<I>::handle_open_image(int r) {
  dout(10) << "r=" << r << dendl;
  if (r < 0) {
    m_image_ctx = nullptr;
    finish(r == -ENOENT ? 0 : r);
    return;
  }

  int validate_r = 0;
  {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    auto snap_info = m_image_ctx->get_snap_info(m_snap_id);
    if (snap_info == nullptr) {
      validate_r = -ENOENT;
    } else {
      const auto* mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
        &snap_info->snap_namespace);
      const auto* group_ns =
        std::get_if<cls::rbd::ImageSnapshotNamespaceGroup>(
          &snap_info->snap_namespace);
      if ((mirror_ns == nullptr ||
            mirror_ns->group_snap_id != m_group_snap_id) &&
          (group_ns == nullptr ||
            group_ns->group_snapshot_id != m_group_snap_id)) {
        validate_r = -ESTALE;
      } else {
        m_snap_namespace = snap_info->snap_namespace;
        m_snap_name = snap_info->name;
      }
    }
  }

  if (validate_r != 0) {
    close_image(validate_r == -ENOENT ? 0 : validate_r);
    return;
  }

  auto ctx = new LambdaContext([this](int r) {
    close_image(r == -ENOENT ? 0 : r);
  });
  m_image_ctx->operations->snap_remove(m_snap_namespace, m_snap_name, ctx);
}

template <typename I>
void RemoveImageSnapshotRequest<I>::close_image(int r) {
  dout(10) << "r=" << r << dendl;
  m_ret_val = r;
  auto ctx = create_context_callback<RemoveImageSnapshotRequest<I>,
    &RemoveImageSnapshotRequest<I>::handle_close_image>(this);
  m_image_ctx->state->close(ctx);
}

template <typename I>
void RemoveImageSnapshotRequest<I>::handle_close_image(int r) {
  dout(10) << "r=" << r << dendl;
  m_image_ctx = nullptr;
  if (m_ret_val == 0 && r < 0) {
    m_ret_val = r;
  }
  finish(m_ret_val);
}

template <typename I>
void RemoveImageSnapshotRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;
  if (r < 0) {
    derr << "failed to remove snapshot " << m_snap_id << " for image "
         << m_image_id << ": " << cpp_strerror(r) << dendl;
  }
  m_on_finish->complete(r);
  delete this;
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::RemoveImageSnapshotRequest<
  librbd::ImageCtx>;
