// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/mirror/GroupAddImageRequest.h"
#include "common/Cond.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"
#include "librbd/mirror/ImageRemoveRequest.h"
#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"

#include <shared_mutex>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupAddImageRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
GroupAddImageRequest<I>::GroupAddImageRequest(librados::IoCtx &io_ctx,
                                     const std::string &group_id,
                                     const std::string &image_id,
                                     uint64_t group_snap_create_flags,
                                     cls::rbd::MirrorImageMode mode,
                                     const cls::rbd::MirrorGroup &mirror_group,
                                     Context *on_finish)
  : m_group_ioctx(io_ctx), m_group_id(group_id), m_image_id(image_id),
    m_group_snap_create_flags(group_snap_create_flags), m_mode(mode),
    m_mirror_group(mirror_group), m_on_finish(on_finish),
    m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())) {
}

template <typename I>
void GroupAddImageRequest<I>::send() {
  ceph_assert(m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED);

  prepare_group_images();
}

template <typename I>
void GroupAddImageRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  ceph_assert(m_image_ctxs.empty());

  auto ctx = create_context_callback<GroupAddImageRequest<I>,
    &GroupAddImageRequest<I>::handle_prepare_group_images>(this);

  auto req = snapshot::GroupPrepareImagesRequest<I>::create(
    m_group_ioctx, m_group_id, m_image_ctxs, m_images,
    &m_mirror_images, &m_mirror_peer_uuids, m_image_id,
    librbd::mirror::snapshot::GroupPrepareImagesRequest<I>::OP_ADD_IMAGE,
    false, ctx);

  req->send();
}

template <typename I>
void GroupAddImageRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  // get the newly added image context and index
  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    auto ictx = m_image_ctxs[i];
    if (ictx != nullptr && ictx->id == m_image_id) {
      m_add_image_ctx = ictx;
      m_add_image_index = i;
      break;
    }
  }
  ceph_assert(m_add_image_ctx != nullptr);

  validate_image();
}

template <typename I>
void GroupAddImageRequest<I>::validate_image() {
  ldout(m_cct, 10) << dendl;

  std::shared_lock image_locker{m_add_image_ctx->image_lock};

  // cloned images are not supported yet
  if (m_add_image_ctx->parent) {
    lderr(m_cct) << "cannot enable mirroring: cloned image not supported"
                 << dendl;
    m_ret_val = -EINVAL;
    close_images();
    return;
  }

  // FIXME: For now images must belong to the same pool as the group
  auto group_pool_id = m_group_ioctx.get_id();
  if (m_add_image_ctx->md_ctx.get_id() != group_pool_id) {
    lderr(m_cct) << "cannot enable mirroring: image in a different pool"
                 << dendl;
    m_ret_val = -EINVAL;
    close_images();
    return;
  }

  create_primary_group_snapshot();
}

template <typename I>
void GroupAddImageRequest<I>::create_primary_group_snapshot() {
  // generate_image_id is also used for group and group snapshot ids.
  m_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);
  m_group_snap.name = ".mirror.primary." + m_mirror_group.global_group_id
                      + "." + m_group_snap.id;

  ldout(m_cct, 10) << "creating group snapshot " << m_group_snap.name << dendl;

  librados::Rados rados(m_group_ioctx);
  int8_t require_osd_release;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve min OSD release: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  auto complete = cls::rbd::get_mirror_group_snapshot_complete_initial(
      require_osd_release);
  m_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, m_mirror_peer_uuids, {}, {},
    complete};

  for (auto image_ctx : m_image_ctxs) {
    m_group_snap.snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
                                    CEPH_NOSNAP);
  }

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupAddImageRequest<I>,
    &GroupAddImageRequest<I>::handle_create_primary_group_snapshot>(this);
  r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupAddImageRequest<I>::handle_create_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  create_primary_image_snapshots();
}

template <typename I>
void GroupAddImageRequest<I>::create_primary_image_snapshots() {
  ldout(m_cct, 10) << dendl;

  size_t num_images = m_image_ctxs.size();
  m_global_image_ids.resize(num_images);
  m_mirror_images.resize(num_images);
  m_snap_ids.resize(num_images);

  ceph_assert(m_image_ctxs.size() == m_global_image_ids.size());

  uuid_d uuid_gen;
  for (size_t i = 0; i < num_images; i++) {
    uuid_gen.generate_random();
    m_global_image_ids[i] = uuid_gen.to_string();
    m_mirror_images[i].global_image_id = m_global_image_ids[i];
  }

  auto ctx = librbd::util::create_context_callback<GroupAddImageRequest<I>,
    &GroupAddImageRequest<I>::handle_create_primary_image_snapshots>(this);

  auto req = snapshot::GroupImageCreatePrimaryRequest<I>::create(
    m_cct, m_image_ctxs, m_global_image_ids, m_group_snap_create_flags,
    snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS, m_group_snap.id,
    &m_snap_ids, true, ctx);

  req->send();
}

template <typename I>
void GroupAddImageRequest<I>::handle_create_primary_image_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

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

  update_primary_group_snapshot();
}

template <typename I>
void GroupAddImageRequest<I>::update_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
  cls::rbd::set_mirror_group_snapshot_complete(m_group_snap);

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<GroupAddImageRequest<I>,
    &GroupAddImageRequest<I>::handle_update_primary_group_snapshot>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupAddImageRequest<I>::handle_update_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    remove_primary_group_snapshot();
    return;
  }

  enable_mirror_image();
}

template <typename I>
void GroupAddImageRequest<I>::enable_mirror_image() {
  ldout(m_cct, 10) << dendl;

  ceph_assert(m_add_image_ctx != nullptr);

  cls::rbd::MirrorImage mirror_image;
  mirror_image.type = cls::rbd::MIRROR_IMAGE_TYPE_GROUP;
  mirror_image.mode = m_mode;
  mirror_image.global_image_id = m_global_image_ids[m_add_image_index];

  auto ctx = create_context_callback<GroupAddImageRequest<I>,
      &GroupAddImageRequest<I>::handle_enable_mirror_image>(this);

  auto req = ImageStateUpdateRequest<I>::create(m_add_image_ctx->md_ctx,
      m_add_image_ctx->id, cls::rbd::MIRROR_IMAGE_STATE_ENABLED,
      mirror_image, ctx);

  req->send();
}

template <typename I>
void GroupAddImageRequest<I>::handle_enable_mirror_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to enable mirror image: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    disable_mirror_image();
    return;
  }

  notify_mirroring_watcher();
}

template <typename I>
void GroupAddImageRequest<I>::notify_mirroring_watcher() {
  ldout(m_cct, 10) << dendl;

  ceph_assert(m_add_image_ctx != nullptr);

  auto ctx = util::create_context_callback<GroupAddImageRequest<I>,
      &GroupAddImageRequest<I>::handle_notify_mirroring_watcher>(this);

  MirroringWatcher<I>::notify_group_updated(m_group_ioctx,
      cls::rbd::MIRROR_GROUP_STATE_ENABLED, m_group_id,
      m_mirror_group.global_group_id, m_image_ctxs.size(), ctx);
}

template <typename I>
void GroupAddImageRequest<I>::handle_notify_mirroring_watcher(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to notify mirror group update for new image: "
                 << cpp_strerror(r) << dendl;
  }

  close_images();
}

template <typename I>
void GroupAddImageRequest<I>::close_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupAddImageRequest<I>,
      &GroupAddImageRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupAddImageRequest<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && m_ret_val == 0) {
    lderr(m_cct) << "failed to close images: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
  }

  finish(m_ret_val);
}


template <typename I>
void GroupAddImageRequest<I>::disable_mirror_image() {
  ldout(m_cct, 10) << dendl;

  ceph_assert(m_add_image_ctx != nullptr && m_add_image_index >= 0);

  cls::rbd::MirrorImage &mirror_image = m_mirror_images[m_add_image_index];
  if (mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_DISABLING ||
      mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_DISABLED) {
    remove_primary_group_snapshot();
    return;
  }

  auto ctx = create_context_callback<GroupAddImageRequest<I>,
      &GroupAddImageRequest<I>::handle_disable_mirror_image>(this);

  auto req = ImageStateUpdateRequest<I>::create(m_add_image_ctx->md_ctx,
      m_add_image_ctx->id, cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
      mirror_image, ctx);

  req->send();
}

template <typename I>
void GroupAddImageRequest<I>::handle_disable_mirror_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable mirror image: "
                 << cpp_strerror(r) << dendl;
    close_images();
    return;
  }

  remove_primary_group_snapshot();
}

template <typename I>
void GroupAddImageRequest<I>::remove_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupAddImageRequest<I>,
    &GroupAddImageRequest<I>::handle_remove_primary_group_snapshot>(this);

  auto req = snapshot::RemoveGroupSnapshotRequest<I>::create(m_group_ioctx,
     m_group_id, &m_group_snap, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupAddImageRequest<I>::handle_remove_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror group snapshot: "
                 << cpp_strerror(r) << dendl;
    close_images();
    return;
  }

  remove_mirror_image();
}

template <typename I>
void GroupAddImageRequest<I>::remove_mirror_image() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupAddImageRequest<I>,
    &GroupAddImageRequest<I>::handle_remove_mirror_image>(this);

  auto req = ImageRemoveRequest<I>::create(m_add_image_ctx->md_ctx,
      m_global_image_ids[m_add_image_index], m_add_image_ctx->id, ctx);

  req->send();
}

template <typename I>
void GroupAddImageRequest<I>::handle_remove_mirror_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror images: "
                 << cpp_strerror(r) << dendl;
  }

  close_images();
}

template <typename I>
void GroupAddImageRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupAddImageRequest<librbd::ImageCtx>;
