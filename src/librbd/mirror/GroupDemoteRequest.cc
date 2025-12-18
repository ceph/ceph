// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupDemoteRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/mirror/GroupGetInfoRequest.h"
#include "librbd/mirror/snapshot/GroupUnlinkPeerRequest.h"
#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupDemoteRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
void GroupDemoteRequest<I>::send() {
  get_mirror_group_info();
}

template <typename I>
void GroupDemoteRequest<I>::get_mirror_group_info() {
  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_get_mirror_group_info>(this);
  // pass group_id to get ENOENT error returned back
  // when group is not mirror enabled
  auto request = mirror::GroupGetInfoRequest<I>::create(
    m_group_ioctx, "", m_group_id, &m_mirror_group, &m_promotion_state, ctx);
  request->send();
}

template <typename I>
void GroupDemoteRequest<I>::handle_get_mirror_group_info(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    lderr(m_cct) << "group is not enabled for mirroring: " << m_group_name
                 << dendl;
    finish(-EINVAL);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to get mirror group info: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (m_mirror_group.state != cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
    lderr(m_cct) << "mirror group " << m_group_name << " not in enabled state"
                 << dendl;
    finish(-EINVAL);
    return;
  } else if (m_promotion_state != mirror::PROMOTION_STATE_PRIMARY) {
    lderr(m_cct) << "group " << m_group_name << " is not primary" << dendl;
    finish(-EINVAL);
    return;
  }

  prepare_group_images();
}

template <typename I>
void GroupDemoteRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_prepare_group_images>(this);

  auto req = snapshot::GroupPrepareImagesRequest<I>::create(m_group_ioctx,
    m_group_id, m_image_ctxs, m_images, &m_mirror_images, &m_mirror_peer_uuids,
    snapshot::GroupPrepareImagesRequest<I>::OP_DEMOTE, false, ctx);
  req->send();
}

template <typename I>
void GroupDemoteRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  acquire_exclusive_locks();
}

template <typename I>
void GroupDemoteRequest<I>::acquire_exclusive_locks() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_acquire_exclusive_locks>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    std::unique_lock locker{m_image_ctxs[i]->owner_lock};
    if (m_image_ctxs[i]->exclusive_lock != nullptr) {
      ldout(m_cct, 10) << "acquiring exclusive lock for image_id="
                       << m_images[i].spec.image_id << dendl;
      m_image_ctxs[i]->exclusive_lock->block_requests(0);
      m_image_ctxs[i]->exclusive_lock->acquire_lock(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDemoteRequest<I>::handle_acquire_exclusive_locks(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to acquire exclusive locks: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    release_exclusive_locks();
    return;
  }

  // verify ownership for all images.
  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    std::unique_lock locker{m_image_ctxs[i]->owner_lock};
    if (m_image_ctxs[i]->exclusive_lock != nullptr &&
        !m_image_ctxs[i]->exclusive_lock->is_lock_owner()) {
      lderr(m_cct) << "lost ownership for image_id="
                   << m_images[i].spec.image_id << dendl;
      r = m_image_ctxs[i]->exclusive_lock->get_unlocked_op_error();
      m_ret_val = r;
      locker.unlock();
      release_exclusive_locks();
      return;
    }
  }

  create_primary_group_snapshot();
}

template <typename I>
void GroupDemoteRequest<I>::create_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);
  m_group_snap.name = ".mirror.primary." + m_mirror_group.global_group_id +
                      "." + m_group_snap.id;

  cls::rbd::MirrorSnapshotState state =
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED;
  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATING;

  librados::Rados rados(m_group_ioctx);
  int8_t require_osd_release;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve min OSD release: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    release_exclusive_locks();
    return;
  }

  auto complete =
    cls::rbd::get_mirror_group_snapshot_complete_initial(require_osd_release);
  m_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    state, m_mirror_peer_uuids, {}, {}, complete};

  m_group_snap.snaps.clear();
  for (auto image_ctx: m_image_ctxs) {
    m_group_snap.snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
                                    CEPH_NOSNAP);
  }

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_create_primary_group_snapshot>(this);
  r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupDemoteRequest<I>::handle_create_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    release_exclusive_locks();
    return;
  }

  enable_non_primary_features();
}

template <typename I>
void GroupDemoteRequest<I>::enable_non_primary_features() {
  if (m_images.empty()) {
    update_primary_group_snapshot();
    return;
  }
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_enable_non_primary_features>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  // ensure images are flagged with non-primary feature so that
  // standard RBD clients cannot write to it.
  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    I *image_ctx = m_image_ctxs[i];

    librados::ObjectWriteOperation op;
    cls_client::set_features(&op, RBD_FEATURE_NON_PRIMARY,
                             RBD_FEATURE_NON_PRIMARY);

    auto aio_comp = create_rados_callback(gather_ctx->new_sub());
    int r = image_ctx->md_ctx.aio_operate(image_ctx->header_oid, aio_comp, &op);
    ceph_assert(r == 0);
    aio_comp->release();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupDemoteRequest<I>::handle_enable_non_primary_features(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to enable non-primary features for images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    disable_non_primary_features();
    return;
  }

  create_images_primary_snapshots();
}

template <typename I>
void GroupDemoteRequest<I>::create_images_primary_snapshots() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_create_images_primary_snapshots>(this);

  m_snap_ids.resize(m_image_ctxs.size(), CEPH_NOSNAP);
  m_global_image_ids.clear();
  m_global_image_ids.reserve(m_image_ctxs.size());
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_global_image_ids.push_back(m_mirror_images[i].global_image_id);
  }

  auto req = snapshot::GroupImageCreatePrimaryRequest<I>::create(
    m_cct, m_image_ctxs, m_global_image_ids, SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE,
    (snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS |
     snapshot::CREATE_PRIMARY_FLAG_DEMOTED), m_group_snap.id,
    &m_snap_ids, false, ctx);
  req->send();
}

template <typename I>
void GroupDemoteRequest<I>::handle_create_images_primary_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_group_snap.snaps[i].snap_id = m_snap_ids[i];
  }

  if (r < 0) {
    lderr(m_cct) << "failed to create primary mirror image snapshots: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    disable_non_primary_features();
    return;
  }

  update_primary_group_snapshot();
}

template <typename I>
void GroupDemoteRequest<I>::update_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
  cls::rbd::set_mirror_group_snapshot_complete(m_group_snap);

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_update_primary_group_snapshot>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupDemoteRequest<I>::handle_update_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update primary group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    disable_non_primary_features();
    return;
  }
  // remove incomplete mirror group snapshots
  // and snapshots with no peer uuids.
  group_unlink_peer();
}

template <typename I>
void GroupDemoteRequest<I>::group_unlink_peer() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_group_unlink_peer>(this);

  auto req = mirror::snapshot::GroupUnlinkPeerRequest<I>::create(
    m_group_ioctx, m_group_id, &m_mirror_peer_uuids, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupDemoteRequest<I>::handle_group_unlink_peer(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unlink mirror group snapshot: " << cpp_strerror(r)
                 << dendl;
  }

  release_exclusive_locks();
}

template <typename I>
void GroupDemoteRequest<I>::release_exclusive_locks() {
  ldout(m_cct, 10) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_release_exclusive_locks>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    std::unique_lock locker{m_image_ctxs[i]->owner_lock};
    if (m_image_ctxs[i]->exclusive_lock != nullptr) {
      m_image_ctxs[i]->exclusive_lock->unblock_requests();
      m_image_ctxs[i]->exclusive_lock->release_lock(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDemoteRequest<I>::handle_release_exclusive_locks(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to release exclusive locks for images: "
                 << cpp_strerror(r) << dendl;
  }

  close_images();
}

template <typename I>
void GroupDemoteRequest<I>::close_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>, &GroupDemoteRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDemoteRequest<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to close images: " << cpp_strerror(r) << dendl;
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }

  finish(m_ret_val);
}

template <typename I>
void GroupDemoteRequest<I>::disable_non_primary_features() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_disable_non_primary_features>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    I *image_ctx = m_image_ctxs[i];

    librados::ObjectWriteOperation op;
    cls_client::set_features(&op, 0U, RBD_FEATURE_NON_PRIMARY);

    auto aio_comp = create_rados_callback(gather_ctx->new_sub());
    int r = image_ctx->md_ctx.aio_operate(image_ctx->header_oid, aio_comp, &op);
    ceph_assert(r == 0);
    aio_comp->release();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDemoteRequest<I>::handle_disable_non_primary_features(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable non-primary feature of images: "
                 << cpp_strerror(r) << dendl;
  }

  remove_primary_group_snapshot();
}

template <typename I>
void GroupDemoteRequest<I>::remove_primary_group_snapshot() {
  ldout(m_cct, 10) << "undoing group demote" << dendl;

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_remove_primary_group_snapshot>(this);

  auto req = snapshot::RemoveGroupSnapshotRequest<I>::create(m_group_ioctx,
     m_group_id, &m_group_snap, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupDemoteRequest<I>::handle_remove_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror group snapshot: " << cpp_strerror(r)
                 << dendl;
  }

  release_exclusive_locks();
}

template <typename I>
void GroupDemoteRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupDemoteRequest<librbd::ImageCtx>;
