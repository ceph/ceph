// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/ceph_assert.h"
#include "common/Cond.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Operations.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Types.h"
#include "librbd/Utils.h"
#include "librbd/api/Utils.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"

#include <algorithm>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::RemoveGroupSnapshotRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using util::create_rados_callback;
using util::create_context_callback;

template <typename I>
void RemoveGroupSnapshotRequest<I>::send() {
  ldout(m_cct, 10) << dendl;

  open_missing_image_contexts();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::open_missing_image_contexts() {
  std::set<cls::rbd::GroupImageSpec> missing_images;
  for (const auto& snap : m_group_snap->snaps) {
    if (snap.snap_id == CEPH_NOSNAP) {
      continue;
    }
    auto it = std::find_if(m_image_ctxs->begin(), m_image_ctxs->end(),
      [&snap](auto image_ctx) {
        return image_ctx != nullptr && image_ctx->id == snap.image_id &&
               image_ctx->md_ctx.get_id() == snap.pool;
      });
    if (it == m_image_ctxs->end()) {
      missing_images.emplace(snap.image_id, snap.pool);
    }
  }

  std::vector<std::pair<cls::rbd::GroupImageSpec, librados::IoCtx>> images;
  for (const auto& image_spec : missing_images) {
    librados::IoCtx image_io_ctx;
    int r = util::create_ioctx(m_group_io_ctx, "image", image_spec.pool_id,
                               {}, &image_io_ctx);
    if (r == -ENOENT) {
      ldout(m_cct, 10) << "image pool no longer exists: pool_id="
                       << image_spec.pool_id << dendl;
      continue;
    } else if (r < 0) {
      lderr(m_cct) << "failed to open image pool " << image_spec.pool_id
                   << ": " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
    images.emplace_back(image_spec, std::move(image_io_ctx));
  }

  auto ctx = create_context_callback<
    RemoveGroupSnapshotRequest<I>,
    &RemoveGroupSnapshotRequest<I>::handle_open_missing_image_contexts>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (auto& [image_spec, image_io_ctx] : images) {
    auto image_ctx = I::create("", image_spec.image_id, nullptr, image_io_ctx,
                               false);
    image_ctx->read_only_mask &= ~librbd::IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
    m_owned_image_ctxs.push_back(image_ctx);
    auto image_index = m_owned_image_ctxs.size() - 1;
    auto on_open = new LambdaContext(
      [this, image_index, image_spec,
       on_finish=gather_ctx->new_sub()](int r) {
        if (r < 0) {
          m_owned_image_ctxs[image_index] = nullptr;
          if (r == -ENOENT) {
            ldout(m_cct, 10) << "image no longer exists: image_id="
                             << image_spec.image_id << ", pool_id="
                             << image_spec.pool_id << dendl;
            r = 0;
          }
        }
        on_finish->complete(r);
      });
    image_ctx->state->open(librbd::OPEN_FLAG_SKIP_OPEN_PARENT, on_open);
  }
  gather_ctx->activate();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::handle_open_missing_image_contexts(int r) {
  if (r < 0) {
    lderr(m_cct) << "failed to open group snapshot images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_owned_image_contexts();
    return;
  }
  remove_group_image_snapshots();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::remove_group_image_snapshots() {
  ldout(m_cct, 10) << "group snap id: " << m_group_snap->id << dendl;

  auto ctx = create_context_callback<
      RemoveGroupSnapshotRequest<I>,
      &RemoveGroupSnapshotRequest<I>::handle_remove_group_image_snapshots>(this);

  C_Gather *gather_ctx = new C_Gather(m_cct, ctx);
  for (auto &snap : m_group_snap->snaps) {
    if (snap.snap_id == CEPH_NOSNAP) {
      continue;
    }

    auto it = std::find_if(m_image_ctxs->begin(), m_image_ctxs->end(),
      [&snap](auto image_ctx) {
        return image_ctx != nullptr && image_ctx->id == snap.image_id &&
               image_ctx->md_ctx.get_id() == snap.pool;
      });
    ImageCtx *ictx = nullptr;
    if (it != m_image_ctxs->end()) {
      ictx = *it;
    } else {
      auto owned_it = std::find_if(
        m_owned_image_ctxs.begin(), m_owned_image_ctxs.end(),
        [&snap](auto image_ctx) {
          return image_ctx != nullptr && image_ctx->id == snap.image_id &&
                 image_ctx->md_ctx.get_id() == snap.pool;
        });
      if (owned_it == m_owned_image_ctxs.end()) {
        // The image or its pool disappeared before it could be opened, so its
        // snapshot is already gone.
        continue;
      }
      ictx = *owned_it;
    }

    ldout(m_cct, 10) << "removing individual snapshot: "
                     << snap.snap_id << ", from image id:"
                     << snap.image_id << dendl;
    remove_image_snapshot(ictx, snap.snap_id, gather_ctx);
  }

  gather_ctx->activate();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::handle_remove_group_image_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove individual group image snapshots: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_owned_image_contexts();
    return;
  }

  remove_group_snapshot();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::remove_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::group_snap_remove(&op, m_group_snap->id);

  auto aio_comp = create_rados_callback<
    RemoveGroupSnapshotRequest<I>,
    &RemoveGroupSnapshotRequest<I>::handle_remove_group_snapshot>(this);
  int r = m_group_io_ctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                     aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::handle_remove_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove group snapshot metadata: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_owned_image_contexts();
    return;
  }

  close_owned_image_contexts();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::remove_image_snapshot(ImageCtx *image_ctx,
                                                      uint64_t snap_id,
                                                      C_Gather *gather_ctx) {
  ldout(m_cct, 10) << snap_id << dendl;

  int r = -ENOENT;
  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;

  image_ctx->image_lock.lock_shared();
  auto snap_it = image_ctx->snap_info.find(snap_id);
  if (snap_it != image_ctx->snap_info.end()) {
    r = 0;
    snap_namespace = snap_it->second.snap_namespace;
    snap_name = snap_it->second.name;
  }

  image_ctx->image_lock.unlock_shared();
  if (r == -ENOENT) {
    ldout(m_cct, 10) << "missing snapshot: snap_id=" << snap_id << dendl;
    return;
  }
  image_ctx->operations->snap_remove(snap_namespace, snap_name.c_str(),
                                     gather_ctx->new_sub());
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::close_owned_image_contexts() {
  auto ctx = create_context_callback<
    RemoveGroupSnapshotRequest<I>,
    &RemoveGroupSnapshotRequest<I>::handle_close_owned_image_contexts>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (auto image_ctx : m_owned_image_ctxs) {
    if (image_ctx != nullptr) {
      image_ctx->state->close(gather_ctx->new_sub());
    }
  }
  gather_ctx->activate();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::handle_close_owned_image_contexts(int r) {
  m_owned_image_ctxs.clear();
  if (m_ret_val == 0 && r < 0) {
    m_ret_val = r;
  }
  finish(m_ret_val);
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::RemoveGroupSnapshotRequest<librbd::ImageCtx>;
