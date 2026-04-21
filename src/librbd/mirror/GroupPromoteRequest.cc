// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupPromoteRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/Utils.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/mirror/GroupRemoveImageRequest.h"
#include "librbd/mirror/snapshot/CreateNonPrimaryRequest.h"
#include "librbd/mirror/snapshot/GroupUnlinkPeerRequest.h"
#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "librbd/mirror/DrainImageWatchersRequest.h"

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupPromoteRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
void GroupPromoteRequest<I>::send() {
  get_mirror_group();
}

template <typename I>
void GroupPromoteRequest<I>::get_mirror_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_group_get_start(&op, m_group_id);

  auto comp = create_rados_callback<
      GroupPromoteRequest<I>,
      &GroupPromoteRequest<I>::handle_get_mirror_group>(this);

  m_out_bl.clear();
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_get_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_group_get_finish(&iter, &m_mirror_group);
  }

  if (r == -ENOENT) {
    lderr(m_cct) << "group is not enabled for mirroring: " << m_group_name
                 << dendl;
    finish(-EINVAL);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirror group metadata: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_mirror_group.state != cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
    lderr(m_cct) << "mirror group not in enabled state" << dendl;
    finish(-EINVAL);
    return;
  }

  check_group_primary_state();
}

template <typename I>
void GroupPromoteRequest<I>::check_group_primary_state() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_check_group_primary_state>(this);

  auto req = librbd::group::ListSnapshotsRequest<>::create(
      m_group_ioctx, m_group_id, true, true, &m_snaps, ctx);
  req->send();
}

template <typename I>
void GroupPromoteRequest<I>::handle_check_group_primary_state(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to list group snapshots of group '" << m_group_name
                 << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  cls::rbd::GroupSnapshotState group_snap_state =
    cls::rbd::GROUP_SNAPSHOT_STATE_CREATING;
  cls::rbd::MirrorGroupSnapshotCompleteState sync =
    cls::rbd::MIRROR_GROUP_SNAPSHOT_INCOMPLETE;
  cls::rbd::MirrorSnapshotState state =
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED;

  for (auto it = m_snaps.rbegin(); it != m_snaps.rend(); it++) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &it->snapshot_namespace);
    if (ns == nullptr) {
      continue;
    }

    // XXXMG: check primary_mirror_uuid matches?
    group_snap_state = it->state;
    state = ns->state;
    sync = ns->complete;
    break;
  }

  if (state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
    lderr(m_cct) << "group " << m_group_name << " is already primary" << dendl;
    finish(-EINVAL);
    return;
  } else if (!m_force && (state == cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY
      || !is_mirror_group_snapshot_complete(group_snap_state, sync))) {
    lderr(m_cct) << "group " << m_group_name
                 << " is primary within a remote cluster or"
                 << " demotion is not propagated yet" << dendl;
    finish(-EBUSY);
    return;
  }

  remove_resync_key();
}

template <typename I>
void GroupPromoteRequest<I>::remove_resync_key() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::metadata_remove(&op, RBD_GROUP_RESYNC);

  auto aio_comp = create_rados_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_remove_resync_key>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_remove_resync_key(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed removing group resync key: " << RBD_GROUP_RESYNC
                 << " : " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  prepare_group_images();
}

template <typename I>
void GroupPromoteRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_prepare_group_images>(this);

  auto req = snapshot::GroupPrepareImagesRequest<I>::create(m_group_ioctx,
    m_group_id, m_image_ctxs, m_images, &m_mirror_images, &m_mirror_peer_uuids,
    "", snapshot::GroupPrepareImagesRequest<I>::OP_PROMOTE, m_force, ctx);
  req->send();
}

template <typename I>
void GroupPromoteRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  check_rollback_needed();
}

template <typename I>
void GroupPromoteRequest<I>::check_rollback_needed() {
  if (!m_force) {
    prepare_group_promotion();
    return;
  }
  ldout(m_cct, 10) << dendl;

  if (m_snaps.empty()) {
    lderr(m_cct) << "cannot rollback, no mirror group snapshot"
                 << " available on group: " << m_group_name << dendl;
    m_ret_val = -EINVAL;
    close_images();
    return;
  }
  ldout(m_cct, 10) << dendl;

  m_need_rollback = false;
  m_orphan_required = false;
  auto snap = m_snaps.rbegin();
  for (; snap != m_snaps.rend(); ++snap) {
    auto mirror_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &snap->snapshot_namespace);
    if (mirror_ns == nullptr || mirror_ns->is_orphan()) {
      continue;
    }

    if (!m_orphan_required) {
      m_orphan_required = !mirror_ns->is_demoted();
    }

    if (!is_mirror_group_snapshot_complete(snap->state, mirror_ns->complete)) {
      m_need_rollback = true;
      continue;
    }
    break;
  }

  if (snap == m_snaps.rend()) {
    lderr(m_cct) << "cannot rollback, no complete mirror group snapshot"
                 << " available on group: " << m_group_name << dendl;
    m_ret_val = -EINVAL;
    close_images();
    return;
  }
  m_rollback_group_snap = *snap;

  prepare_group_promotion();
}

template <typename I>
void GroupPromoteRequest<I>::prepare_group_promotion() {
  ldout(m_cct, 10) << dendl;
  ceph_assert(m_images.size() == m_image_ctxs.size());

  if (m_orphan_required || m_need_rollback) {
    create_group_orphan_snapshot();
    return;
  }

  bool can_skip_orphan = true;
  m_rollback_snap_ids.resize(m_images.size());
  for (size_t i = 0; i < m_images.size(); i++) {
    bool requires_orphan = false;
    if (!snapshot::util::can_create_primary_snapshot(m_image_ctxs[i], false, true,
                                                     &requires_orphan,
                                                     &m_rollback_snap_ids[i])) {
      lderr(m_cct) << "cannot promote image with image_id="
                   << m_images[i].spec.image_id << dendl;
      m_ret_val = -EINVAL;
      close_images();
      return;
    }
    can_skip_orphan = can_skip_orphan &&
      (m_rollback_snap_ids[i] == CEPH_NOSNAP && !requires_orphan);
  }

  if (can_skip_orphan) {
    create_primary_group_snapshot();
  } else {
    create_group_orphan_snapshot();
  }
}

template <typename I>
bool GroupPromoteRequest<I>::build_group_snapshot_metadata(
    bool is_primary, cls::rbd::GroupSnapshot* group_snap) {
  ldout(m_cct, 10) << dendl;

  group_snap->id = librbd::util::generate_image_id(m_group_ioctx);
  std::string prefix =
    (is_primary) ? ".mirror.primary." : ".mirror.non-primary.";

  group_snap->name = prefix + m_mirror_group.global_group_id +
                     "." + group_snap->id;

  group_snap->state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATING;

  librados::Rados rados(m_group_ioctx);
  int8_t require_osd_release;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve min OSD release: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    return false;
  }

  auto complete =
    cls::rbd::get_mirror_group_snapshot_complete_initial(require_osd_release);

  if (is_primary) {
    group_snap->snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
      cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, m_mirror_peer_uuids,
      {}, {}, complete};
  } else {
    group_snap->snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, {}, {}, complete};
  }

  group_snap->snaps.clear();
  for (auto image_ctx : m_image_ctxs) {
    group_snap->snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
                                   CEPH_NOSNAP);
  }

  return true;
}

template <typename I>
void GroupPromoteRequest<I>::create_group_orphan_snapshot() {
  ldout(m_cct, 10) <<  dendl;

  if (!build_group_snapshot_metadata(false, &m_orphan_group_snap)) {
    close_images();
    return;
  }

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_orphan_group_snap);

  auto aio_comp = create_rados_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_create_group_orphan_snapshot>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_create_group_orphan_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create group orphan snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }
  create_images_orphan_snapshots();
}

template <typename I>
void GroupPromoteRequest<I>::create_images_orphan_snapshots() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_create_images_orphan_snapshots>(this);

  m_orphan_snap_ids.resize(m_image_ctxs.size(), CEPH_NOSNAP);
  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto req = snapshot::CreateNonPrimaryRequest<I>::create(
      m_image_ctxs[i], false, "", "", CEPH_NOSNAP, {}, {},
      &m_orphan_snap_ids[i], gather_ctx->new_sub());
    req->send();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_create_images_orphan_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_orphan_group_snap.snaps[i].snap_id = m_orphan_snap_ids[i];
  }

  if (r < 0) {
    lderr(m_cct) << "failed to create orphan snapshots for images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  mark_group_orphan_snapshot_complete();
}

template <typename I>
void GroupPromoteRequest<I>::mark_group_orphan_snapshot_complete() {
  ldout(m_cct, 10) << dendl;

  m_orphan_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
  cls::rbd::set_mirror_group_snapshot_complete(m_orphan_group_snap);

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_orphan_group_snap);

  auto aio_comp = create_rados_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_mark_group_orphan_snapshot_complete>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_mark_group_orphan_snapshot_complete(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to mark group orphan snapshot complete: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  drain_image_watchers();
}

template <typename I>
void GroupPromoteRequest<I>::drain_image_watchers() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupPromoteRequest<I>,
        &GroupPromoteRequest<I>::handle_drain_image_watchers>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
     auto req = DrainImageWatchersRequest<I>::create(
      m_image_ctxs[i], gather_ctx->new_sub());
    req->send();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_drain_image_watchers(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    m_ret_val = r;
    lderr(m_cct) << "failed to unregister watchers for group images: "
                 << cpp_strerror(r) << dendl;
    close_images();
    return;
  }

  acquire_exclusive_locks();
}

template <typename I>
void GroupPromoteRequest<I>::acquire_exclusive_locks() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_acquire_exclusive_locks>(this);
  m_excl_locks_acquire = true;
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
void GroupPromoteRequest<I>::handle_acquire_exclusive_locks(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to acquire exclusive locks: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    release_exclusive_locks();
    return;
  }

  // post-gather verification phase
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

  // all required locks are owned
  if (!m_need_rollback) {
    ldout(m_cct, 10) << "no rollback required" << dendl;
    create_primary_group_snapshot();
    return;
  }

  // current membership
  std::vector<cls::rbd::GroupImageSpec> current_membership;
  for (const auto& image : m_images) {
    current_membership.push_back(image.spec);
  }

  // rollback membership
  auto* rollback_snap = &m_rollback_group_snap;
  std::vector<cls::rbd::GroupImageSpec> rollback_membership;
  for (auto& it : rollback_snap->snaps) {
    rollback_membership.emplace_back(it.image_id, it.pool);
  }

  if (rollback_membership == current_membership) {
    rollback();
    return;
  }

  ldout(m_cct, 10) << "rollback group snapshot membership with snap id: "
                   << rollback_snap->id
                   << ", does not match current group membership"
                   << dendl;

  std::set<std::pair<std::string, int64_t>> current_set;
  std::set<std::pair<std::string, int64_t>> rollback_set;
  for (auto& img : current_membership) {
    current_set.insert({img.image_id, img.pool_id});
  }
  for (auto& img : rollback_membership) {
    rollback_set.insert({img.image_id, img.pool_id});
  }

  m_to_add.clear();
  m_to_remove.clear();
  for (auto& img : rollback_membership) {
    if (!current_set.count({img.image_id, img.pool_id})) {
      m_to_add.push_back(img);
    }
  }
  for (auto& img : current_membership) {
    if (!rollback_set.count({img.image_id, img.pool_id})) {
      m_to_remove.push_back(img);
    }
  }

  ldout(m_cct, 10) << "fixing group membership, "
                   << "to_add=" << m_to_add.size()
                   << ", to_remove=" << m_to_remove.size() << dendl;
  if (!m_to_add.empty()) {
    lderr(m_cct) << "rollback requires adding images to group, but dynamic "
                 << "group removal is not supported today" << dendl;
    m_ret_val = -EINVAL;
    release_exclusive_locks();
    return;
  }
  if (m_to_remove.size() > 1) {
    lderr(m_cct) << "rollback requires more than one image to be removed from "
                 << "the group, this is not supported today" << dendl;
    m_ret_val = -EINVAL;
    release_exclusive_locks();
    return;
  }

  remove_images_from_group();
}

template <typename I>
void GroupPromoteRequest<I>::remove_images_from_group() {
  ldout(m_cct, 10) << dendl;

  auto gather = new C_Gather(m_cct,
    create_context_callback<GroupPromoteRequest<I>,
      &GroupPromoteRequest<I>::handle_remove_images_from_group>(this));

  for (auto& spec : m_to_remove) {
    librados::ObjectWriteOperation op;
    cls_client::group_image_remove(&op, {spec.image_id, spec.pool_id});

    Context* subctx = gather->new_sub();
    auto comp = create_rados_callback(subctx);

    int r = m_group_ioctx.aio_operate(util::group_header_name(m_group_id),
                                      comp, &op);
    ceph_assert(r == 0);

    comp->release();
  }

  gather->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_remove_images_from_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove images from group: "
                 << cpp_strerror(r) << dendl;

    m_ret_val = r;
    release_exclusive_locks();
    return;
  }

  // make a copy and erase removed images
  m_image_ctxs_old_membership = m_image_ctxs;
  for (auto& spec : m_to_remove) {
    auto it = std::find_if(
        m_image_ctxs.begin(), m_image_ctxs.end(),
        [&](I* ictx) {
          return ictx->id == spec.image_id &&
                 ictx->md_ctx.get_id() == spec.pool_id;
        });

    if (it != m_image_ctxs.end()) {
      m_image_ctxs.erase(it);
    }
  }

  // Remove from m_images
  for (auto& spec : m_to_remove) {
    auto it = std::remove_if(
        m_images.begin(), m_images.end(),
        [&](const cls::rbd::GroupImageStatus& image) {
          return image.spec.image_id == spec.image_id &&
                 image.spec.pool_id == spec.pool_id;
        });

    if (it != m_images.end()) {
      m_images.erase(it, m_images.end());
    }
  }

  ldout(m_cct, 10) << "updated membership: "
                   << "remaining images=" << m_images.size() << dendl;

  rollback();
}

template <typename I>
void GroupPromoteRequest<I>::rollback() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>, &GroupPromoteRequest<I>::handle_rollback>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    std::shared_lock owner_locker{m_image_ctxs[i]->owner_lock};
    std::shared_lock image_locker{m_image_ctxs[i]->image_lock};

    auto info = m_image_ctxs[i]->get_snap_info(
      m_rollback_group_snap.snaps[i].snap_id);
    if (info == nullptr) {
      lderr(m_cct) << "missing rollback snapshot for image "
                   << m_images[i].spec.image_id << dendl;
      m_ret_val = -ENOENT;
      release_exclusive_locks();
      return;
    }
    auto snap_namespace = info->snap_namespace;
    auto snap_name = info->name;

    image_locker.unlock();

    m_image_ctxs[i]->operations->execute_snap_rollback(snap_namespace,
      snap_name, m_progress_ctx, gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_rollback(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to rollback: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    release_exclusive_locks();
    return;
  }

  create_primary_group_snapshot();
}

template <typename I>
void GroupPromoteRequest<I>::create_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  if (!build_group_snapshot_metadata(true, &m_group_snap)) {
    release_exclusive_locks();
    return;
  }

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_create_primary_group_snapshot>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_create_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    release_exclusive_locks();
    return;
  }

  create_images_primary_snapshots();
}

template <typename I>
void GroupPromoteRequest<I>::create_images_primary_snapshots() {
  if (m_images.empty()) {
    update_primary_group_snapshot();
    return;
  }
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_create_images_primary_snapshots>(this);

  m_snap_ids.resize(m_image_ctxs.size(), CEPH_NOSNAP);
  m_global_image_ids.clear();
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_global_image_ids.push_back(m_mirror_images[i].global_image_id);
  }

  // FIXME: Check if snapshot::CREATE_PRIMARY_FLAG_FORCE is also
  // required for a regular (non-forced) group promote.
  auto req = snapshot::GroupImageCreatePrimaryRequest<I>::create(
    m_cct, m_image_ctxs, m_global_image_ids, SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE,
    (snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS |
     snapshot::CREATE_PRIMARY_FLAG_FORCE), m_group_snap.id,
    &m_snap_ids, !m_excl_locks_acquire, ctx);
  req->send();
}

template <typename I>
void GroupPromoteRequest<I>::handle_create_images_primary_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  // load the image snapshot(s) details in the group snapshot
  // irrespective of callback return value, so that the image
  // snapshot are pruned correctly either way.
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_group_snap.snaps[i].snap_id = m_snap_ids[i];
  }

  if (r < 0) {
    lderr(m_cct) << "failed to create primary mirror image snapshots: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    remove_primary_group_snapshot();
    return;
  }

  disable_non_primary_features();
}

template <typename I>
void GroupPromoteRequest<I>::disable_non_primary_features() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_disable_non_primary_features>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  // remove the non-primary feature flag so that the image can be
  // R/W by standard RBD clients
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
void GroupPromoteRequest<I>::handle_disable_non_primary_features(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable non-primary feature of images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    enable_non_primary_features();
    return;
  }

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    m_image_ctxs[i]->features &= ~RBD_FEATURE_NON_PRIMARY;
  }

  update_primary_group_snapshot();
}

template <typename I>
void GroupPromoteRequest<I>::update_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
  cls::rbd::set_mirror_group_snapshot_complete(m_group_snap);

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_update_primary_group_snapshot>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_update_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update primary group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    enable_non_primary_features();
    return;
  }

  group_unlink_peer();
}

template <typename I>
void GroupPromoteRequest<I>::group_unlink_peer() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_group_unlink_peer>(this);

  auto req = mirror::snapshot::GroupUnlinkPeerRequest<I>::create(
    m_group_ioctx, m_group_id, &m_mirror_peer_uuids, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupPromoteRequest<I>::handle_group_unlink_peer(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unlink mirror group snapshot: "
                 << cpp_strerror(r) << dendl;
    release_exclusive_locks();
    return;
  }

  disable_removed_images();
}

template <typename I>
void GroupPromoteRequest<I>::disable_removed_images() {
  if (m_to_remove.empty()) {
    release_exclusive_locks();
    return;
  }

  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_disable_removed_images>(this);

  auto gather = new C_Gather(m_cct, ctx);
  for (auto& spec : m_to_remove) {
    auto it = std::find_if(m_image_ctxs_old_membership.begin(),
      m_image_ctxs_old_membership.end(),
      [&](auto* ictx) {
        return spec.image_id == ictx->id &&
               spec.pool_id == ictx->md_ctx.get_id();
      });

    if (it == m_image_ctxs_old_membership.end()) {
      lderr(m_cct) << "missing ictx for image_id="
                   << spec.image_id << " pool_id="
                   << spec.pool_id << dendl;
      continue;
    }

    auto* ictx = *it;
    Context* subctx = gather->new_sub();
    auto req = mirror::GroupRemoveImageRequest<I>::create(
      ictx, m_group_id, m_group_ioctx, subctx);
    req->send();
  }

  gather->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_disable_removed_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable removed images: "
                 << cpp_strerror(r) << dendl;
    release_exclusive_locks();
    return;
  }

  // At this point, any non-primary and incomplete primary group snapshots
  // should already have been pruned. We can now safely remove the images
  // that are no longer members of the group.
  remove_non_member_images();
}

template <typename I>
void GroupPromoteRequest<I>::remove_non_member_images() {
  ldout(m_cct, 10) << dendl;

  Context *ctx = create_context_callback<GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_remove_non_member_images>(this);

  auto gather = new C_Gather(m_cct, ctx);

  // keep IoCtx objects alive until all RemoveRequests finish
  m_remove_ioctxs.clear();

  // Consider a scenario where a group snapshot is just created on the primary,
  // but the GroupReplayer on the secondary has not yet processed it. During
  // this window, an image may still appear in GROUP_IMAGE_LINK_STATE_INCOMPLETE
  // state. At this point the image is not fully added to the group yet.
  // If a force promote is triggered on the secondary in this state, there will
  // be no incomplete group snapshot available, so rollback will not be
  // triggered. However, the image in GROUP_IMAGE_LINK_STATE_INCOMPLETE may
  // still remain in the group and therefore needs to be cleaned up after
  // promotion.
  for (const auto& image : m_images) {
    if (image.state == cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE) {
      ldout(m_cct, 10) << "removing image with incomplete group link state: "
                       << image.spec.image_id << dendl;
      auto exists = std::any_of(m_to_remove.begin(), m_to_remove.end(),
        [&](const cls::rbd::GroupImageSpec& s) {
          return s.image_id == image.spec.image_id &&
          s.pool_id == image.spec.pool_id;
        });
      if (!exists) {
        m_to_remove.push_back(image.spec);
      }
    }
  }

  for (auto &spec : m_to_remove) {
    // allocate IoCtx in vector
    m_remove_ioctxs.emplace_back();
    librados::IoCtx &image_ioctx = m_remove_ioctxs.back();

    int r = util::create_ioctx(m_group_ioctx, "", spec.pool_id, {}, &image_ioctx);
    if (r < 0) {
      lderr(m_cct) << "failed to create ioctx: " << cpp_strerror(r) << dendl;
      continue;
    }

    if (spec.image_id.empty()) {
      continue;
    }

    std::string image_name;
    r = cls_client::dir_get_name(&image_ioctx, RBD_DIRECTORY,
                                 spec.image_id, &image_name);
    if (r < 0) {
      lderr(m_cct) << "failed to resolve image name for id "
                   << spec.image_id << ": " << cpp_strerror(r) << dendl;
      continue;
    }

    ldout(m_cct, 10) << "removing image " << image_name
                    << " (" << spec.image_id << ")" << dendl;

    Context *subctx = gather->new_sub();
    auto req = librbd::image::RemoveRequest<I>::create(image_ioctx, image_name,
      spec.image_id, false, false, m_progress_ctx, nullptr, subctx);

    req->send();
  }

  gather->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_remove_non_member_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed removing non-member images: "
                 << cpp_strerror(r) << dendl;
  }

  release_exclusive_locks();
}

template <typename I>
void GroupPromoteRequest<I>::release_exclusive_locks() {
  ldout(m_cct, 10) << dendl;
  if (!m_excl_locks_acquire) {
    close_images();
    return;
  }

  auto ctx = librbd::util::create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_release_exclusive_locks>(this);
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
void GroupPromoteRequest<I>::handle_release_exclusive_locks(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to release exclusive locks for images: "
                 << cpp_strerror(r) << dendl;
  }

  close_images();
}

template <typename I>
void GroupPromoteRequest<I>::close_images() {
  if (m_image_ctxs.empty()) {
    finish(m_ret_val);
    return;
  }
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (auto ictx: m_image_ctxs) {
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_close_images(int r) {
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
void GroupPromoteRequest<I>::enable_non_primary_features() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_enable_non_primary_features>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    I *image_ctx = m_image_ctxs[i];

    librados::ObjectWriteOperation op;
    cls_client::set_features(&op, RBD_FEATURE_NON_PRIMARY, RBD_FEATURE_NON_PRIMARY);

    auto aio_comp = create_rados_callback(gather_ctx->new_sub());
    int r = image_ctx->md_ctx.aio_operate(image_ctx->header_oid, aio_comp, &op);
    ceph_assert(r == 0);
    aio_comp->release();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_enable_non_primary_features(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to enable non-primary feature of images: "
                 << cpp_strerror(r) << dendl;
  }

  remove_primary_group_snapshot();
}

template <typename I>
void GroupPromoteRequest<I>::remove_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_remove_primary_group_snapshot>(this);

  auto req = snapshot::RemoveGroupSnapshotRequest<I>::create(m_group_ioctx,
     m_group_id, &m_group_snap, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupPromoteRequest<I>::handle_remove_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror group snapshot: "
                 << cpp_strerror(r) << dendl;
  }
  release_exclusive_locks();
}

template <typename I>
void GroupPromoteRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupPromoteRequest<librbd::ImageCtx>;
