// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SyncPointPruneRequest.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"

#include <set>
#include <shared_mutex> // for std::shared_lock

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_sync::SyncPointPruneRequest: " \
                           << this << " " << __func__
namespace rbd {
namespace mirror {
namespace image_sync {

using librbd::util::create_context_callback;

template <typename I>
SyncPointPruneRequest<I>::SyncPointPruneRequest(
    I *remote_image_ctx,
    bool sync_complete,
    SyncPointHandler* sync_point_handler,
    Context *on_finish)
  : m_remote_image_ctx(remote_image_ctx),
    m_sync_complete(sync_complete),
    m_sync_point_handler(sync_point_handler),
    m_on_finish(on_finish) {
  m_sync_points_copy = m_sync_point_handler->get_sync_points();
}

template <typename I>
void SyncPointPruneRequest<I>::send() {
  if (m_sync_points_copy.empty()) {
    send_remove_snap();
    return;
  }

  if (m_sync_complete) {
    // if sync is complete, we can remove the master sync point
    auto it = m_sync_points_copy.begin();
    auto& sync_point = *it;

    ++it;
    if (it == m_sync_points_copy.end() ||
        it->from_snap_name != sync_point.snap_name) {
      m_snap_names.push_back(sync_point.snap_name);
    }

    if (!sync_point.from_snap_name.empty()) {
      m_snap_names.push_back(sync_point.from_snap_name);
    }
  } else {
    // if we have more than one sync point or invalid sync points,
    // trim them off
    std::shared_lock image_locker{m_remote_image_ctx->image_lock};
    std::set<std::string> snap_names;
    for (auto it = m_sync_points_copy.rbegin();
         it != m_sync_points_copy.rend(); ++it) {
      auto& sync_point = *it;
      if (&sync_point == &m_sync_points_copy.front()) {
        if (m_remote_image_ctx->get_snap_id(
	      cls::rbd::UserSnapshotNamespace(), sync_point.snap_name) ==
              CEPH_NOSNAP) {
          derr << ": failed to locate sync point snapshot: "
               << sync_point.snap_name << dendl;
        } else if (!sync_point.from_snap_name.empty()) {
          derr << ": unexpected from_snap_name in primary sync point: "
               << sync_point.from_snap_name << dendl;
        } else {
          // first sync point is OK -- keep it
          break;
        }
        m_invalid_master_sync_point = true;
      }

      if (snap_names.count(sync_point.snap_name) == 0) {
        snap_names.insert(sync_point.snap_name);
        m_snap_names.push_back(sync_point.snap_name);
      }

      auto& front_sync_point = m_sync_points_copy.front();
      if (!sync_point.from_snap_name.empty() &&
          snap_names.count(sync_point.from_snap_name) == 0 &&
          sync_point.from_snap_name != front_sync_point.snap_name) {
        snap_names.insert(sync_point.from_snap_name);
        m_snap_names.push_back(sync_point.from_snap_name);
      }
    }
  }

  send_remove_snap();
}

template <typename I>
void SyncPointPruneRequest<I>::send_remove_snap() {
  if (m_snap_names.empty()) {
    send_refresh_image();
    return;
  }

  const std::string &snap_name = m_snap_names.front();

  dout(20) << ": snap_name=" << snap_name << dendl;

  Context *ctx = create_context_callback<
    SyncPointPruneRequest<I>, &SyncPointPruneRequest<I>::handle_remove_snap>(
      this);
  m_remote_image_ctx->operations->snap_remove(cls::rbd::UserSnapshotNamespace(),
					      snap_name.c_str(),
					      ctx);
}

template <typename I>
void SyncPointPruneRequest<I>::handle_remove_snap(int r) {
  dout(20) << ": r=" << r << dendl;

  ceph_assert(!m_snap_names.empty());
  std::string snap_name = m_snap_names.front();
  m_snap_names.pop_front();

  if (r == -ENOENT) {
    r = 0;
  }
  if (r < 0) {
    derr << ": failed to remove snapshot '" << snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_remove_snap();
}

template <typename I>
void SyncPointPruneRequest<I>::send_refresh_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    SyncPointPruneRequest<I>, &SyncPointPruneRequest<I>::handle_refresh_image>(
      this);
  m_remote_image_ctx->state->refresh(ctx);
}

template <typename I>
void SyncPointPruneRequest<I>::handle_refresh_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": remote image refresh failed: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_update_sync_points();
}

template <typename I>
void SyncPointPruneRequest<I>::send_update_sync_points() {
  dout(20) << dendl;

  if (m_sync_complete) {
    m_sync_points_copy.pop_front();
  } else {
    while (m_sync_points_copy.size() > 1) {
      m_sync_points_copy.pop_back();
    }
    if (m_invalid_master_sync_point) {
      // all subsequent sync points would have been pruned
      m_sync_points_copy.clear();
    }
  }

  auto ctx = create_context_callback<
    SyncPointPruneRequest<I>,
    &SyncPointPruneRequest<I>::handle_update_sync_points>(this);
  m_sync_point_handler->update_sync_points(
    m_sync_point_handler->get_snap_seqs(), m_sync_points_copy,
    m_sync_complete, ctx);
}

template <typename I>
void SyncPointPruneRequest<I>::handle_update_sync_points(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update client data: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void SyncPointPruneRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_sync::SyncPointPruneRequest<librbd::ImageCtx>;
