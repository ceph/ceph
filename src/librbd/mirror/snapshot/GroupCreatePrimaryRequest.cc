// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/GroupCreatePrimaryRequest.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "include/Context.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "librbd/api/Group.h"
#include "librbd/internal.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Operations.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/group/UnlinkPeerGroupRequest.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/mirror/snapshot/CreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "librbd/mirror/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::GroupCreatePrimaryRequest: " \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_rados_callback;


std::string prepare_primary_mirror_snap_name(CephContext *cct,
                                             const std::string &global_group_id,
                                             const std::string &snap_id) {
  ldout(cct, 10) << "global_group_id: " << global_group_id
                 << ", snap_id: " << snap_id << dendl;

  std::stringstream ind_snap_name_stream;
  ind_snap_name_stream << ".mirror.primary."
                       << global_group_id << "." << snap_id;
  return ind_snap_name_stream.str();
}


template <typename I>
struct C_ImageSnapshotCreate2 : public Context {
  I *ictx;
  uint64_t snap_create_flags;
  int64_t group_pool_id;
  std::string group_id;
  std::string group_snap_id;
  uint64_t *snap_id;
  Context *on_finish;

  cls::rbd::MirrorImage mirror_image;
  mirror::PromotionState promotion_state;
  std::string primary_mirror_uuid;

  C_ImageSnapshotCreate2(I *ictx, uint64_t snap_create_flags,
                         int64_t group_pool_id,
                         const std::string &group_id,
                         const std::string &group_snap_id,
                         uint64_t *snap_id,
                         Context *on_finish)
    : ictx(ictx), snap_create_flags(snap_create_flags),
      group_pool_id(group_pool_id), group_id(group_id),
      group_snap_id(group_snap_id), snap_id(snap_id),
      on_finish(on_finish) {
  }

  void finish(int r) override {
    if (r < 0 && r != -ENOENT) {
      on_finish->complete(r);
      return;
    }

    if (mirror_image.mode != cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT ||
        mirror_image.state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
      lderr(ictx->cct) << "snapshot based mirroring is not enabled" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }

    auto req = mirror::snapshot::CreatePrimaryRequest<I>::create(
      ictx, mirror_image.global_image_id, CEPH_NOSNAP, snap_create_flags, 0U,
      group_pool_id, group_id, group_snap_id, snap_id, on_finish);
    req->send();
  }
};


template <typename I>
void image_snapshot_create2(I *ictx, uint32_t flags,
                            const std::string &group_snap_id,
                            uint64_t *snap_id, Context *on_finish) {
  CephContext *cct = ictx->cct;
  ldout(cct, 10) << "ictx=" << ictx << dendl;

  uint64_t snap_create_flags = 0;
  int r = librbd::util::snap_create_flags_api_to_internal(cct, flags,
                                                          &snap_create_flags);
  if (r < 0) {
    on_finish->complete(r);
    return;
  }

  auto on_refresh = new LambdaContext(
    [ictx, snap_create_flags, group_snap_id, snap_id, on_finish](int r) {
      if (r < 0) {
        lderr(ictx->cct) << "refresh failed: " << cpp_strerror(r) << dendl;
        on_finish->complete(r);
        return;
      }

      auto ctx = new C_ImageSnapshotCreate2<I>(ictx, snap_create_flags,
                                               ictx->group_spec.pool_id,
                                               ictx->group_spec.group_id,
                                               group_snap_id, snap_id,
                                               on_finish);
      auto req = mirror::GetInfoRequest<I>::create(*ictx, &ctx->mirror_image,
                                                   &ctx->promotion_state,
                                                   &ctx->primary_mirror_uuid,
                                                   ctx);
      req->send();
    });

  if (ictx->state->is_refresh_required()) {
    ictx->state->refresh(on_refresh);
  } else {
    on_refresh->complete(0);
  }
}


template <typename I>
void GroupCreatePrimaryRequest<I>::send() {
 get_group_id();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::get_group_id() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::dir_get_id_start(&op, m_group_name);

  auto comp = create_rados_callback<
      GroupCreatePrimaryRequest<I>,
      &GroupCreatePrimaryRequest<I>::handle_get_group_id>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_get_group_id(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to get ID of group '" << m_group_name
                 << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  auto it = m_outbl.cbegin();
  r = cls_client::dir_get_id_finish(&it, &m_group_id);
  if (r < 0) {
    lderr(m_cct) << "failed to get ID of group '" << m_group_name
                 << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_mirror_group();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::get_mirror_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_group_get_start(&op, m_group_id);

  auto comp = create_rados_callback<
      GroupCreatePrimaryRequest<I>,
      &GroupCreatePrimaryRequest<I>::handle_get_mirror_group>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_get_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    ldout(m_cct, 10) << "mirroring for group '" << m_group_name
                     << "' disabled" << dendl;
    finish(-EINVAL);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirror group metadata for group '"
                 << m_group_name << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  auto it = m_outbl.cbegin();
  r = cls_client::mirror_group_get_finish(&it, &m_mirror_group);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirror group metadata for group '"
                 << m_group_name << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_mirror_group.mirror_image_mode !=
      cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
    auto mode = static_cast<rbd_mirror_image_mode_t>(
        m_mirror_group.mirror_image_mode);
    lderr(m_cct) << "cannot create snapshot, mirror mode is set to: "
                 << mode << dendl;
    finish(-EOPNOTSUPP);
    return;
  }

  get_last_mirror_snapshot_state();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::get_last_mirror_snapshot_state() {
  ldout(m_cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_get_last_mirror_snapshot_state>(
      this);

  auto req = group::ListSnapshotsRequest<I>::create(
    m_group_ioctx, m_group_id, true, true, &m_existing_group_snaps, ctx);

  req->send();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_get_last_mirror_snapshot_state(
    int r) {
  ldout(m_cct, 10) << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to list group snapshots of group '" << m_group_name
               << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  cls::rbd::MirrorSnapshotState state =
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY;
  for (auto it = m_existing_group_snaps.rbegin();
       it != m_existing_group_snaps.rend(); it++) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &it->snapshot_namespace);
    if (ns != nullptr) {
      // XXXMG: check primary_mirror_uuid matches?
      ldout(m_cct, 10) <<  "the state of existing group snap is: " << ns->state
                     << dendl;
      state = ns->state;
      break;
    }
  }

  if (state != cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
    lderr(m_cct) << "group " << m_group_name << " is not primary" << dendl;
    finish(-EINVAL);
    return;
  }

  generate_group_snap();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::generate_group_snap() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);
  m_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{};
  m_group_snap.name = prepare_primary_mirror_snap_name(
    m_cct, m_mirror_group.global_group_id, m_group_snap.id);

  prepare_group_images();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_prepare_group_images>(this);

  auto req = mirror::snapshot::GroupPrepareImagesRequest<I>::create(
    m_group_ioctx, m_group_id, cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY,
    m_flags, &m_group_snap, &m_image_ctxs, &m_quiesce_requests, ctx);

  req->send();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images '" << m_group_name
                 << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  create_image_snaps();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::create_image_snaps() {
  ldout(m_cct, 10) << "group name '" << m_group_name << "' group ID '"
                   << m_group_id
                   << "' group snap ID '" << m_group_snap.id << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_create_image_snaps>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  m_image_snap_ids.resize(m_image_ctxs.size(), CEPH_NOSNAP);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    image_snapshot_create2(m_image_ctxs[i], RBD_SNAP_CREATE_SKIP_QUIESCE,
                           m_group_snap.id, &m_image_snap_ids[i],
                           gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_create_image_snaps(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::string group_header_oid = librbd::util::group_header_name(m_group_id);

  if (r < 0) {
    lderr(m_cct) << "failed to create image snaps: "
                 << cpp_strerror(r) << dendl;

    if (m_ret_code == 0) {
      m_ret_code = r;
    }

    ldout(m_cct, 10) << "undoing group create snapshot: " << r << dendl;
    remove_incomplete_group_snap();
    return;
  } else {
    for (size_t i = 0; i < m_image_ctxs.size(); i++) {
      m_group_snap.snaps[i].snap_id = m_image_snap_ids[i];
    }

    m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;
    r = cls_client::group_snap_set(&m_group_ioctx, group_header_oid,
                                   m_group_snap);
    if (r < 0) {
      lderr(m_cct) << "failed to update group snapshot metadata: "
                 << cpp_strerror(r) << dendl;
      if (m_ret_code == 0) {
        m_ret_code = r;
      }

      ldout(m_cct, 10) << "undoing group create snapshot: " << r << dendl;
      remove_incomplete_group_snap();
      return;
    }

    *m_snap_id = m_group_snap.id;
  }

  if (!m_quiesce_requests.empty()) {
    notify_unquiesce();
    return;
  }

  if (m_ret_code == 0) {
    unlink_peer_group();
    return;
  }

  close_images();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::remove_incomplete_group_snap() {
  ldout(m_cct, 10) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_remove_incomplete_group_snap>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);


  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    if (m_group_snap.snaps[i].snap_id == CEPH_NOSNAP) {
      continue;
    }

    librbd::ImageCtx *ictx = m_image_ctxs[i];

    std::shared_lock image_locker{ictx->image_lock};
    auto info = ictx->get_snap_info(
      m_group_snap.snaps[i].snap_id);
    ceph_assert(info != nullptr);
    image_locker.unlock();

    ldout(m_cct, 10) << "removing individual snapshot: "
                     << info->name << dendl;

    ictx->operations->snap_remove(info->snap_namespace,
                                  info->name,
                                  gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_remove_incomplete_group_snap(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  // if previous attempts to remove this snapshot failed then the
  // image's snapshot may not exist
  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed cleaning up group member image snapshots: "
                 << cpp_strerror(r) << dendl;
  }

  if (r == 0) {
    r = cls_client::group_snap_remove(
      &m_group_ioctx,
      librbd::util::group_header_name(m_group_id),
      m_group_snap.id);

    if (r < 0) {
      lderr(m_cct) << "failed to remove group snapshot metadata: "
                   << cpp_strerror(r) << dendl;
    }
  }

  if (!m_quiesce_requests.empty()) {
    notify_unquiesce();
    return;
  }

  close_images();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::notify_unquiesce() {
  ldout(m_cct, 10) << dendl;

  ceph_assert(m_quiesce_requests.size() == m_image_ctxs.size());

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_notify_unquiesce>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  int image_count = m_image_ctxs.size();
  for (int i = 0; i < image_count; ++i) {
    auto ictx = m_image_ctxs[i];
    ictx->image_watcher->notify_unquiesce(m_quiesce_requests[i],
                                          gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_notify_unquiesce(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to notify the unquiesce requests: "
                 << cpp_strerror(r) << dendl;
  }

  if (m_ret_code == 0) {
    unlink_peer_group();
    return;
  }

  close_images();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::unlink_peer_group() {
  ldout(m_cct, 10) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_unlink_peer_group>(this);

  auto req = group::UnlinkPeerGroupRequest<I>::create(
    m_group_ioctx, m_group_id, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_unlink_peer_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unlink peer group: " << cpp_strerror(r)
                 << dendl;
  }

  close_images();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::close_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_close_images>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    ictx->state->close(gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to close images: " << cpp_strerror(r) << dendl;
  }

  m_image_ctxs.clear();
  finish(m_ret_code);
}

template <typename I>
void GroupCreatePrimaryRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::GroupCreatePrimaryRequest<librbd::ImageCtx>;
