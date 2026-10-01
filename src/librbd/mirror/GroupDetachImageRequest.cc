// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/mirror/GroupDetachImageRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/group/RemoveImageRequest.h"

#include "librbd/mirror/snapshot/CreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"

#include <shared_mutex>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix                                                    \
  *_dout << "librbd::mirror::GroupDetachImageRequest: " << this << " " \
         << __func__ << ": "

namespace librbd {
namespace mirror {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
GroupDetachImageRequest<I>::GroupDetachImageRequest(librados::IoCtx& io_ctx,
  const std::string& group_id, librados::IoCtx& image_ioctx,
  const std::string& image_id, uint64_t group_snap_create_flags,
  const cls::rbd::MirrorGroup& mirror_group, Context* on_finish) :
  m_group_ioctx(io_ctx),
  m_image_ioctx(image_ioctx),
  m_group_id(group_id),
  m_image_id(image_id),
  m_group_snap_create_flags(group_snap_create_flags),
  m_mirror_group(mirror_group),
  m_on_finish(on_finish),
  m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())) {}

template <typename I>
void GroupDetachImageRequest<I>::send() {
  ceph_assert(m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED);

  prepare_group_images();
}

template <typename I>
void GroupDetachImageRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_prepare_group_images>(this);

  auto req = snapshot::GroupPrepareImagesRequest<I>::create(m_group_ioctx,
    m_group_id, m_image_ctxs, m_images, &m_mirror_images, &m_mirror_peer_uuids,
    m_image_id, snapshot::GroupPrepareImagesRequest<I>::OP_REMOVE_IMAGE, false,
    ctx);

  req->send();
}

template <typename I>
void GroupDetachImageRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    if (m_image_ctxs[i] != nullptr && m_image_ctxs[i]->id == m_image_id) {
      m_detach_image_ctx = m_image_ctxs[i];
      m_detach_image_index = i;
      break;
    }
  }

  if (m_detach_image_ctx == nullptr) {
    lderr(m_cct) << "failed to find image being detached: " << m_image_id
                 << dendl;
    m_ret_val = -ENOENT;
    close_images();
    return;
  }

  validate_image();
}

template <typename I>
void GroupDetachImageRequest<I>::validate_image() {
  ldout(m_cct, 10) << dendl;

  ceph_assert(m_detach_image_index >= 0);
  auto& mirror_image = m_mirror_images[m_detach_image_index];
  m_original_mirror_image = mirror_image;

  if (mirror_image.type == cls::rbd::MIRROR_IMAGE_TYPE_STANDALONE) {
    ldout(m_cct, 10) << "resuming a partially completed image detach" << dendl;
    create_detached_image_snapshot();
    return;
  }

  if (mirror_image.type != cls::rbd::MIRROR_IMAGE_TYPE_GROUP) {
    lderr(m_cct) << "image is not group mirrored" << dendl;

    m_ret_val = -EINVAL;
    close_images();
    return;
  }

  if (mirror_image.mode != cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
    lderr(m_cct) << "unsupported mirror mode: " << mirror_image.mode << dendl;

    m_ret_val = -EINVAL;
    close_images();
    return;
  }

  detach_mirror_image();
}

template <typename I>
void GroupDetachImageRequest<I>::detach_mirror_image() {
  ldout(m_cct, 10) << dendl;

  auto& mirror_image = m_mirror_images[m_detach_image_index];

  mirror_image.type = cls::rbd::MIRROR_IMAGE_TYPE_STANDALONE;

  auto ctx = create_context_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_detach_mirror_image>(this);

  auto req = ImageStateUpdateRequest<I>::create(m_detach_image_ctx->md_ctx,
    m_detach_image_ctx->id, cls::rbd::MIRROR_IMAGE_STATE_ENABLED, mirror_image,
    ctx, true);

  req->send();
}

template <typename I>
void GroupDetachImageRequest<I>::handle_detach_mirror_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to convert group mirrored image to "
                    "standalone snapshot mirroring: "
                 << cpp_strerror(r) << dendl;

    m_ret_val = r;
    restore_mirror_image();
    return;
  }

  create_detached_image_snapshot();
}

template <typename I>
void GroupDetachImageRequest<I>::create_detached_image_snapshot() {
  ldout(m_cct, 10) << dendl;

  auto& mirror_image = m_mirror_images[m_detach_image_index];

  auto ctx = create_context_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_create_detached_image_snapshot>(this);

  auto req = snapshot::CreatePrimaryRequest<I>::create(m_detach_image_ctx,
    mirror_image.global_image_id,
    CEPH_NOSNAP, // clean_since_snap_id
    m_group_snap_create_flags, snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS,
    &m_detached_snap_id, ctx);

  req->send();
}

template <typename I>
void GroupDetachImageRequest<I>::handle_create_detached_image_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create standalone primary snapshot: "
                 << cpp_strerror(r) << dendl;

    m_ret_val = r;
    remove_detached_image_snapshot();
    return;
  }

  create_primary_group_snapshot();
}

template <typename I>
void GroupDetachImageRequest<I>::create_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  // generate_image_id is also used for group and group snapshot IDs
  m_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);

  m_group_snap.name = ".mirror.primary." + m_mirror_group.global_group_id +
                      "." + m_group_snap.id;

  ldout(m_cct, 10) << "creating group snapshot " << m_group_snap.name << dendl;

  librados::Rados rados(m_group_ioctx);

  int8_t require_osd_release;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve min OSD release: " << cpp_strerror(r)
                 << dendl;

    m_ret_val = r;
    remove_detached_image_snapshot();
    return;
  }

  auto complete =
    cls::rbd::get_mirror_group_snapshot_complete_initial(require_osd_release);

  m_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, m_mirror_peer_uuids, {}, {},
    complete};

  // Only include images that remain in the mirror group. The detached image
  // has already been converted to standalone snapshot mirroring.
  m_group_image_ctxs.clear();
  m_group_mirror_images.clear();
  m_group_snap.snaps.clear();

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    if (static_cast<int>(i) == m_detach_image_index) {
      continue;
    }

    auto image_ctx = m_image_ctxs[i];

    m_group_image_ctxs.push_back(image_ctx);
    m_group_mirror_images.push_back(m_mirror_images[i]);

    m_group_snap.snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
      CEPH_NOSNAP);
  }

  librados::ObjectWriteOperation op;

  // Create the group snapshot first. Add the image snapshot IDs after
  // GroupImageCreatePrimaryRequest completes.
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_create_primary_group_snapshot>(this);

  r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
    aio_comp, &op);

  ceph_assert(r == 0);

  aio_comp->release();
}

template <typename I>
void GroupDetachImageRequest<I>::handle_create_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create group snapshot: " << cpp_strerror(r)
                 << dendl;

    m_ret_val = r;
    remove_detached_image_snapshot();
    return;
  }

  // The group snapshot now has the final membership. Create primary image
  // snapshots for the images that remain in the group.
  create_primary_image_snapshots();
}

template <typename I>
void GroupDetachImageRequest<I>::create_primary_image_snapshots() {
  ldout(m_cct, 10) << dendl;

  // An empty group has no image snapshots to create.
  if (m_group_image_ctxs.empty()) {
    update_primary_group_snapshot();
    return;
  }

  ceph_assert(m_group_image_ctxs.size() == m_group_mirror_images.size());

  m_snap_ids.resize(m_group_image_ctxs.size(), CEPH_NOSNAP);

  std::vector<std::string> global_image_ids;
  global_image_ids.reserve(m_group_mirror_images.size());

  for (auto& mirror_image : m_group_mirror_images) {
    global_image_ids.push_back(mirror_image.global_image_id);
  }

  auto ctx = create_context_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_create_primary_image_snapshots>(this);

  auto req = snapshot::GroupImageCreatePrimaryRequest<I>::create(m_cct,
    m_group_image_ctxs, global_image_ids, m_group_snap_create_flags,
    snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS, m_group_snap.id,
    &m_snap_ids, true, ctx);

  req->send();
}

template <typename I>
void GroupDetachImageRequest<I>::handle_create_primary_image_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create primary image snapshots: "
                 << cpp_strerror(r) << dendl;

    m_ret_val = r;

    remove_primary_group_snapshot();
    return;
  }

  ceph_assert(m_group_snap.snaps.size() == m_snap_ids.size());

  for (size_t i = 0; i < m_group_snap.snaps.size(); ++i) {
    m_group_snap.snaps[i].snap_id = m_snap_ids[i];
  }

  update_primary_group_snapshot();
}

template <typename I>
void GroupDetachImageRequest<I>::update_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;

  cls::rbd::set_mirror_group_snapshot_complete(m_group_snap);

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_update_primary_group_snapshot>(this);

  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
    aio_comp, &op);

  ceph_assert(r == 0);

  aio_comp->release();
}

template <typename I>
void GroupDetachImageRequest<I>::handle_update_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update group snapshot: " << cpp_strerror(r)
                 << dendl;

    m_ret_val = r;

    remove_primary_group_snapshot();
    return;
  }

  remove_group_membership();
}

template <typename I>
void GroupDetachImageRequest<I>::remove_group_membership() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_remove_group_membership>(this);
  auto req = group::RemoveImageRequest<I>::create(m_group_ioctx, m_group_id,
    m_image_ioctx, m_image_id, ctx);
  req->send();
}

template <typename I>
void GroupDetachImageRequest<I>::handle_remove_group_membership(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove image from group: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    remove_primary_group_snapshot();
    return;
  }

  notify_mirroring_watcher();
}

template <typename I>
void GroupDetachImageRequest<I>::notify_mirroring_watcher() {
  ldout(m_cct, 10) << "m_image_ctxs.size()=" << m_image_ctxs.size() << dendl;

  ceph_assert(m_detach_image_index >= 0);

  auto& mirror_image = m_mirror_images[m_detach_image_index];

  auto ctx = create_context_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_notify_mirroring_watcher>(this);

  MirroringWatcher<I>::notify_group_membership_updated(m_group_ioctx,
    cls::rbd::MIRROR_IMAGE_STATE_ENABLED, m_image_id,
    mirror_image.global_image_id, m_group_id, m_mirror_group.global_group_id,
    m_image_ctxs.size() - 1, librbd::mirroring_watcher::GROUP_MEMBERSHIP_DETACH,
    ctx);
}

template <typename I>
void GroupDetachImageRequest<I>::handle_notify_mirroring_watcher(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to notify mirroring watcher: " << cpp_strerror(r)
                 << dendl;

    // The membership transition is durable. A watcher refresh will
    // rediscover it, so do not report a retryable operation failure.
  }

  close_images();
}

template <typename I>
void GroupDetachImageRequest<I>::remove_detached_image_snapshot() {
  ldout(m_cct, 10) << "snap_id=" << m_detached_snap_id << dendl;

  if (m_detached_snap_id == CEPH_NOSNAP) {
    restore_mirror_image();
    return;
  }

  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;
  {
    std::shared_lock image_locker{m_detach_image_ctx->image_lock};
    auto it = m_detach_image_ctx->snap_info.find(m_detached_snap_id);
    if (it == m_detach_image_ctx->snap_info.end()) {
      restore_mirror_image();
      return;
    }
    snap_namespace = it->second.snap_namespace;
    snap_name = it->second.name;
  }

  auto ctx = create_context_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_remove_detached_image_snapshot>(this);
  m_detach_image_ctx->operations->snap_remove(snap_namespace, snap_name, ctx);
}

template <typename I>
void GroupDetachImageRequest<I>::handle_remove_detached_image_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove detached image snapshot: "
                 << cpp_strerror(r) << dendl;
  }
  restore_mirror_image();
}

template <typename I>
void GroupDetachImageRequest<I>::restore_mirror_image() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_restore_mirror_image>(this);
  auto req = ImageStateUpdateRequest<I>::create(m_detach_image_ctx->md_ctx,
    m_detach_image_ctx->id, cls::rbd::MIRROR_IMAGE_STATE_ENABLED,
    m_original_mirror_image, ctx, true);
  req->send();
}

template <typename I>
void GroupDetachImageRequest<I>::handle_restore_mirror_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  if (r < 0) {
    lderr(m_cct) << "failed to restore group mirror image metadata: "
                 << cpp_strerror(r) << dendl;
  }
  close_images();
}

template <typename I>
void GroupDetachImageRequest<I>::remove_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_remove_primary_group_snapshot>(this);

  auto req = snapshot::RemoveGroupSnapshotRequest<I>::create(m_group_ioctx,
    m_group_id, &m_group_snap, &m_group_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupDetachImageRequest<I>::handle_remove_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove group snapshot: " << cpp_strerror(r)
                 << dendl;
  }

  remove_detached_image_snapshot();
}

template <typename I>
void GroupDetachImageRequest<I>::close_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupDetachImageRequest<I>,
    &GroupDetachImageRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto image_ctx : m_image_ctxs) {
    if (image_ctx != nullptr) {
      image_ctx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDetachImageRequest<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && m_ret_val == 0) {
    lderr(m_cct) << "failed to close images: " << cpp_strerror(r) << dendl;

    m_ret_val = r;
  }

  finish(m_ret_val);
}

template <typename I>
void GroupDetachImageRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);

  delete this;
}


} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupDetachImageRequest<librbd::ImageCtx>;
