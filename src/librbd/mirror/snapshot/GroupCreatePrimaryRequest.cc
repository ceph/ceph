// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/mirror/snapshot/GroupCreatePrimaryRequest.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "include/Context.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/mirror/snapshot/GroupUnlinkPeerRequest.h"
#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
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
using librbd::util::create_context_callback;

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
void GroupCreatePrimaryRequest<I>::send() {
  ldout(m_cct, 10) << dendl;

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

  for (auto it = m_existing_group_snaps.rbegin();
       it != m_existing_group_snaps.rend(); it++) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &it->snapshot_namespace);
    if (ns == nullptr) {
      continue;
    }

    ldout(m_cct, 10) << "found mirror group snapshot: state="
                     << ns->state << " complete=" << ns->complete
                     << dendl;

    // XXXMG: check primary_mirror_uuid matches?
    if (ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
      if (!is_mirror_group_snapshot_complete(it->state, ns->complete)) {
        // ignore incomplete primary snapshots
        continue;
      }
      prepare_group_images();
      return;
    } else if (ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED) {
      lderr(m_cct) << "group " << m_group_name
                   << " is primary-demoted" << dendl;
      finish(-EINVAL);
      return;
    } else {
      lderr(m_cct) << "group " << m_group_name << " is not primary" << dendl;
      finish(-EINVAL);
      return;
    }
  }

  lderr(m_cct) << "group " << m_group_name
               << " has no valid completed primary mirror snapshot" << dendl;
  finish(-EINVAL);
}

template<typename I>
void GroupCreatePrimaryRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_prepare_group_images>(this);

  auto req = snapshot::GroupPrepareImagesRequest<I>::create(m_group_ioctx,
    m_group_id, m_image_ctxs, m_images, &m_mirror_images, &m_mirror_peer_uuids,
    "", snapshot::GroupPrepareImagesRequest<I>::OP_CREATE_PRIMARY, false, ctx);
  req->send();
}

template<typename I>
void GroupCreatePrimaryRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images: " << cpp_strerror(r)
                 << dendl;
    m_ret_code = r;
    close_images();
    return;
  }

  generate_group_snap();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::generate_group_snap() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);
  m_group_snap.name = prepare_primary_mirror_snap_name(
    m_cct, m_mirror_group.global_group_id, m_group_snap.id);

  cls::rbd::MirrorSnapshotState state = cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY;

  librados::Rados rados(m_group_ioctx);
  int8_t require_osd_release;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve min OSD release: " << cpp_strerror(r)
                 << dendl;
    m_ret_code = r;
    close_images();
    return;
  }

  auto complete = cls::rbd::get_mirror_group_snapshot_complete_initial(require_osd_release);
  m_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    state, m_mirror_peer_uuids, {}, {}, complete};

  for (auto image_ctx: m_image_ctxs) {
    m_group_snap.snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
                                    CEPH_NOSNAP);
  }

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_generate_group_snap>(this);
  r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_generate_group_snap(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_code = r;
    close_images();
    return;
  }

  create_images_primary_snapshots();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::create_images_primary_snapshots() {
  if (m_images.empty()) {
    update_primary_group_snapshot();
    return;
  }
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_create_images_primary_snapshots>(this);

  ceph_assert(m_image_ctxs.size() == m_mirror_images.size());
  m_image_snap_ids.resize(m_image_ctxs.size(), CEPH_NOSNAP);
  m_global_image_ids.clear();
  m_global_image_ids.reserve(m_image_ctxs.size());
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_global_image_ids.push_back(m_mirror_images[i].global_image_id);
  }

  auto req = snapshot::GroupImageCreatePrimaryRequest<I>::create(
    m_cct, m_image_ctxs, m_global_image_ids, m_snap_create_flags,
    0U, m_group_snap.id, &m_image_snap_ids, true, ctx);
  req->send();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_create_images_primary_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_group_snap.snaps[i].snap_id = m_image_snap_ids[i];
  }

  if (r < 0) {
    lderr(m_cct) << "failed to create primary mirror image snapshots: "
                 << cpp_strerror(r) << dendl;
    m_ret_code = r;
    remove_primary_group_snapshot();
    return;
  }

  update_primary_group_snapshot();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::update_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
  cls::rbd::set_mirror_group_snapshot_complete(m_group_snap);

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_update_primary_group_snapshot>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_update_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update primary group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_code = r;
    remove_primary_group_snapshot();
    return;
  }

  *m_snap_id = m_group_snap.id;
  unlink_peer_group();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::unlink_peer_group() {
  ldout(m_cct, 10) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_unlink_peer_group>(this);

  auto req = GroupUnlinkPeerRequest<I>::create(
    m_group_ioctx, m_group_id, &m_mirror_peer_uuids, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_unlink_peer_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unlink group peers: " << cpp_strerror(r)
                 << dendl;
  }

  close_images();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::remove_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_remove_primary_group_snapshot>(this);

  auto req = snapshot::RemoveGroupSnapshotRequest<I>::create(m_group_ioctx,
     m_group_id, &m_group_snap, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_remove_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror group snapshot: "
                 << cpp_strerror(r) << dendl;
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
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
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
