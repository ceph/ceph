// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/mirror/snapshot/GroupMirrorEnableUpdateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/ImageState.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"
#include "librbd/mirror/ImageRemoveRequest.h"
#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupMirrorEnableUpdateRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
GroupMirrorEnableUpdateRequest<I>* GroupMirrorEnableUpdateRequest<I>::create(
    librados::IoCtx& group_ioctx, const std::string& group_id,
    cls::rbd::MirrorGroup& mirror_group, std::vector<I*>& image_ctxs,
    std::vector<cls::rbd::GroupImageStatus>& images,
    const std::set<std::string>& mirror_peer_uuids,
    cls::rbd::MirrorImageMode mode, uint64_t group_snap_create_flags,
    const std::string& image_id, Type type,
    const std::unordered_map<std::string, std::string> &image_to_global_id,
    Context* on_finish) {

  return new GroupMirrorEnableUpdateRequest(group_ioctx, group_id,
    mirror_group, image_ctxs, images, mirror_peer_uuids, mode,
    group_snap_create_flags, image_id, type, image_to_global_id, on_finish);
}

template <typename I>
GroupMirrorEnableUpdateRequest<I>::GroupMirrorEnableUpdateRequest(
    librados::IoCtx& group_ioctx, const std::string& group_id,
    cls::rbd::MirrorGroup& mirror_group, std::vector<I*>& image_ctxs,
    std::vector<cls::rbd::GroupImageStatus>& images,
    const std::set<std::string>& mirror_peer_uuids,
    cls::rbd::MirrorImageMode mode, uint64_t group_snap_create_flags,
    const std::string& image_id, Type type,
    const std::unordered_map<std::string, std::string> &image_to_global_id,
    Context* on_finish)
  : m_group_ioctx(group_ioctx), m_group_id(group_id),
    m_mirror_group(mirror_group), m_image_ctxs(image_ctxs), m_images(images),
    m_mirror_peer_uuids(mirror_peer_uuids), m_mode(mode),
    m_group_snap_create_flags(group_snap_create_flags), m_image_id(image_id),
    m_type(type), m_image_to_global_id(image_to_global_id),
    m_on_finish(on_finish),
    m_cct(reinterpret_cast<CephContext*>(group_ioctx.cct())) {
  for (auto ictx : m_image_ctxs) {
    if (ictx != nullptr) {
      m_image_ctx_map.emplace(ictx->id, ictx);
    }
  }
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::send() {
  ldout(m_cct, 20) << "group_id=" << m_group_id
                   << " type=" << static_cast<int>(m_type)
                   << dendl;
  validate_images();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::validate_images() {
  ldout(m_cct, 10) << dendl;

  for (auto &image_ctx : m_image_ctxs) {
    if (m_type == Type::ADD_IMAGE && image_ctx->id != m_image_id) {
      continue;
    }
    std::shared_lock image_locker{image_ctx->image_lock};
    if (image_ctx->parent) {
      lderr(m_cct) << "cannot enable mirroring: cloned images are not "
                   << "supported" << dendl;
      m_ret_val = -EINVAL;

      close_images();
      return;
    }
  }

  // FIXME: Once the support for mirroring cloned images is added, need to
  // check that the parents are enabled for mirroring

  // FIXME: For now images must belong to the same pool as the group
  auto group_pool_id = m_group_ioctx.get_id();
  for (auto &image_ctx : m_image_ctxs) {
    if (m_type == Type::ADD_IMAGE && image_ctx->id != m_image_id) {
      continue;
    }
    std::shared_lock image_locker{image_ctx->image_lock};
    if (image_ctx->md_ctx.get_id() != group_pool_id) {
      lderr(m_cct) << "cannot enable mirroring: image in a different pool"
                   << dendl;
      m_ret_val = -EINVAL;

      close_images();
      return;
    }
  }

  if (m_type == Type::ADD_IMAGE) {
    create_primary_group_snapshot();
  } else {
    set_mirror_group_enabling();
  }
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::set_mirror_group_enabling() {
  ldout(m_cct, 10) << dendl;

  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_ENABLING;

  librados::ObjectWriteOperation op;
  cls_client::mirror_group_set(&op, m_group_id, m_mirror_group);
  auto aio_comp = create_rados_callback<
    GroupMirrorEnableUpdateRequest<I>,
    &GroupMirrorEnableUpdateRequest<I>::handle_set_mirror_group_enabling>(this);
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_set_mirror_group_enabling(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to set mirror group as enabling: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;

    close_images();
    return;
  }

  create_primary_group_snapshot();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::create_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.id = util::generate_image_id(m_group_ioctx);
  m_group_snap.name = ".mirror.primary." + m_mirror_group.global_group_id +
                      "." + m_group_snap.id;

  librados::Rados rados(m_group_ioctx);
  int8_t require_osd_release;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve min OSD release: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    disable_mirror_group();
    return;
  }

  auto complete = cls::rbd::get_mirror_group_snapshot_complete_initial(
          require_osd_release);
  m_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, m_mirror_peer_uuids,
      {}, {}, complete};

  for (auto ictx : m_image_ctxs) {
    m_group_snap.snaps.emplace_back(ictx->md_ctx.get_id(), ictx->id,
                                    CEPH_NOSNAP);
  }

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto comp = create_rados_callback<GroupMirrorEnableUpdateRequest<I>,
      &GroupMirrorEnableUpdateRequest<I>::handle_create_primary_group_snapshot>(
      this);

  r = m_group_ioctx.aio_operate(util::group_header_name(m_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_create_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_need_to_cleanup_group_snapshot = true;

  if (r < 0) {
    lderr(m_cct) << "failed to create primary group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    disable_mirror_group();
    return;
  }

  if (m_image_ctxs.empty()) {
    update_primary_group_snapshot();
  } else {
    create_primary_image_snapshots();
  }
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::create_primary_image_snapshots() {
  ldout(m_cct, 10) << dendl;

  size_t n = m_image_ctxs.size();
  m_global_image_ids.resize(n);
  m_snap_ids.resize(n, CEPH_NOSNAP);
  m_mirror_images.resize(n);

  for (size_t i = 0; i < n; i++) {
    auto ictx = m_image_ctxs[i];
    const auto& image_id = ictx->id;

    m_mirror_images[i].first = image_id;

    auto it = m_image_to_global_id.find(image_id);

    if (it != m_image_to_global_id.end()) {
      m_mirror_images[i].second.global_image_id = it->second;
      m_global_image_ids[i] = it->second;

      ldout(m_cct, 10)
        << "existing global image_id: "
        << m_global_image_ids[i] << dendl;
    } else {
      uuid_d uuid_gen;
      uuid_gen.generate_random();

      auto& mirror_image = m_mirror_images[i].second;
      mirror_image.type = cls::rbd::MIRROR_IMAGE_TYPE_GROUP;
      mirror_image.mode = m_mode;
      mirror_image.global_image_id = uuid_gen.to_string();

      m_global_image_ids[i] = mirror_image.global_image_id;

      ldout(m_cct, 10)
        << "new global image_id: "
        << m_global_image_ids[i] << dendl;
    }
  }

  auto ctx = create_context_callback<
    GroupMirrorEnableUpdateRequest<I>,
    &GroupMirrorEnableUpdateRequest<I>::handle_create_primary_image_snapshots>(
        this);

  auto req = snapshot::GroupImageCreatePrimaryRequest<I>::create(
      m_cct, m_image_ctxs, m_global_image_ids,
      m_group_snap_create_flags,
      snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS,
      m_group_snap.id, &m_snap_ids, true, ctx);

  req->send();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_create_primary_image_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create primary image snapshots: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;

    for (size_t i = 0; i < m_image_ctxs.size(); i++) {
      m_group_snap.snaps[i].snap_id = m_snap_ids[i];
    }

    disable_mirror_group();
    return;
  }

  update_primary_group_snapshot();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::update_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  for (size_t i = 0; i < m_snap_ids.size(); ++i) {
    m_group_snap.snaps[i].snap_id = m_snap_ids[i];
  }

  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
  cls::rbd::set_mirror_group_snapshot_complete(m_group_snap);

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto comp = create_rados_callback<GroupMirrorEnableUpdateRequest<I>,
      &GroupMirrorEnableUpdateRequest<I>::handle_update_primary_group_snapshot>(
      this);

  int r = m_group_ioctx.aio_operate(util::group_header_name(m_group_id), comp,
                                    &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_update_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update primary group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    disable_mirror_group();
    return;
  }

  if (m_image_ctxs.empty() && m_type != Type::ADD_IMAGE) {
    set_mirror_group_enabled();
  } else {
    set_mirror_images_enabled();
  }
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::set_mirror_images_enabled() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
      GroupMirrorEnableUpdateRequest<I>,
      &GroupMirrorEnableUpdateRequest<I>::handle_set_mirror_images_enabled>(
      this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto ictx = m_image_ctxs[i];

    if (m_type == Type::ADD_IMAGE && ictx->id != m_image_id) {
      continue;
    }

    auto req = ImageStateUpdateRequest<I>::create(
        ictx->md_ctx,
        ictx->id,
        cls::rbd::MIRROR_IMAGE_STATE_ENABLED,
        m_mirror_images[i].second,
        gather_ctx->new_sub());

    req->send();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_set_mirror_images_enabled(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to enable mirror images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    disable_mirror_group();
    return;
  }

  m_need_to_cleanup_mirror_images = true;

  if (m_type == Type::ADD_IMAGE) {
    notify_mirroring_watcher();
  } else {
    set_mirror_group_enabled();
  }
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::set_mirror_group_enabled() {
  ldout(m_cct, 10) << dendl;

  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_ENABLED;

  librados::ObjectWriteOperation op;
  cls_client::mirror_group_set(&op, m_group_id, m_mirror_group);
  auto aio_comp = create_rados_callback<
    GroupMirrorEnableUpdateRequest<I>,
    &GroupMirrorEnableUpdateRequest<I>::handle_set_mirror_group_enabled>(this);
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_set_mirror_group_enabled(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to set mirror group as enabled: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;

    disable_mirror_group();
    return;
  }

  notify_mirroring_watcher();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::notify_mirroring_watcher() {
  ldout(m_cct, 10) << m_image_ctxs.size() << dendl;

  auto ctx = util::create_context_callback<
    GroupMirrorEnableUpdateRequest<I>,
    &GroupMirrorEnableUpdateRequest<I>::handle_notify_mirroring_watcher>(this);

  MirroringWatcher<I>::notify_group_updated(
          m_group_ioctx, cls::rbd::MIRROR_GROUP_STATE_ENABLED, m_group_id,
          m_mirror_group.global_group_id, m_image_ctxs.size(), ctx);
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_notify_mirroring_watcher(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to notify mirror group update: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
  }

  close_images();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::disable_mirror_group() {
  ldout(m_cct, 10) << dendl;

  if (m_type == Type::ADD_IMAGE) {
    if (m_need_to_cleanup_group_snapshot) {
      remove_primary_group_snapshot();
    } else {
      disable_mirror_images();
    }
    return;
  }

  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLING;

  librados::ObjectWriteOperation op;
  cls_client::mirror_group_set(&op, m_group_id, m_mirror_group);

  auto aio_comp = create_rados_callback<
    GroupMirrorEnableUpdateRequest<I>,
    &GroupMirrorEnableUpdateRequest<I>::handle_disable_mirror_group>(this);

  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_disable_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable mirror group: " << cpp_strerror(r)
                 << dendl;
    close_images();
    return;
  }

  if (m_need_to_cleanup_mirror_images) {
    get_mirror_images_for_cleanup();
  } else if (m_need_to_cleanup_group_snapshot) {
    remove_primary_group_snapshot();
  } else {
    remove_mirror_group();
  }
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::get_mirror_images_for_cleanup() {
  ldout(m_cct, 10) << dendl;

  m_mirror_images.clear();
  m_mirror_images.resize(m_images.size());
  m_out_bls.resize(m_images.size());

  auto ctx = create_context_callback<
    GroupMirrorEnableUpdateRequest<I>,
    &GroupMirrorEnableUpdateRequest<I>::handle_get_mirror_images_for_cleanup>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_images.size(); i++) {
    librados::ObjectReadOperation op;
    cls_client::mirror_image_get_start(&op, m_images[i].spec.image_id);

    auto on_mirror_image_get = new LambdaContext(
      [this, i, new_sub_ctx=gather_ctx->new_sub()](int r) {
        if (r == 0) {
          auto iter = m_out_bls[i].cbegin();
          m_mirror_images[i].first = m_images[i].spec.image_id;

          r = cls_client::mirror_image_get_finish(
              &iter, &m_mirror_images[i].second);
        }

        if (r == -ENOENT) {
          r = 0;
          m_mirror_images[i].second.state =
             cls::rbd::MIRROR_IMAGE_STATE_DISABLED;
        } else if (r < 0) {
          lderr(m_cct) << "failed to get mirror image info for image_id="
                       << m_images[i].spec.image_id << dendl;
        }

        new_sub_ctx->complete(r);
      });

    auto comp = create_rados_callback(on_mirror_image_get);

    int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bls[i]);
    ceph_assert(r == 0);
    comp->release();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_get_mirror_images_for_cleanup(int r) {
  ldout(m_cct, 10) << "r=" << r <<  dendl;

  m_out_bls.clear();

  if (r < 0) {
    ldout(m_cct, 10) << "failed to get mirror image info for cleanup" << dendl;
    close_images();
    return;
  }

  disable_mirror_images();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::disable_mirror_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupMirrorEnableUpdateRequest<I>,
    &GroupMirrorEnableUpdateRequest<I>::handle_disable_mirror_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (const auto& image_status : m_images) {
    const auto& image_id = image_status.spec.image_id;

    auto it = m_image_ctx_map.find(image_id);
    if (it == m_image_ctx_map.end()) {
      continue;
    }

    auto ictx = it->second;

    if (m_type == Type::ADD_IMAGE &&
        ictx->id != m_image_id) {
      continue;
    }

    cls::rbd::MirrorImage mirror_image;

    auto gid_it = m_image_to_global_id.find(image_id);
    if (gid_it != m_image_to_global_id.end()) {
      mirror_image.global_image_id = gid_it->second;
    }

    auto req = ImageStateUpdateRequest<I>::create(
        ictx->md_ctx,
        ictx->id,
        cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
        mirror_image,
        gather_ctx->new_sub());

    req->send();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_disable_mirror_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable mirror images: " << cpp_strerror(r)
                 << dendl;
    close_images();
    return;
  }

  remove_primary_group_snapshot();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::remove_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupMirrorEnableUpdateRequest<I>,
    &GroupMirrorEnableUpdateRequest<I>::handle_remove_primary_group_snapshot>(this);

  auto req = snapshot::RemoveGroupSnapshotRequest<I>::create(m_group_ioctx,
     m_group_id, &m_group_snap, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_remove_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror group snapshot: " << cpp_strerror(r)
                 << dendl;
    close_images();
    return;
  }

  if (m_need_to_cleanup_mirror_images) {
    remove_mirror_images();
  } else {
    remove_mirror_group();
  }
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::remove_mirror_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupMirrorEnableUpdateRequest<I>,
    &GroupMirrorEnableUpdateRequest<I>::handle_remove_mirror_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (const auto& image_status : m_images) {
    const auto& image_id = image_status.spec.image_id;
    auto it = m_image_ctx_map.find(image_id);
    if (it == m_image_ctx_map.end()) {
      continue;
    }

    auto ictx = it->second;
    if (m_type == Type::ADD_IMAGE &&
        ictx->id != m_image_id) {
      continue;
    }

    auto gid_it = m_image_to_global_id.find(image_id);
    if (gid_it == m_image_to_global_id.end()) {
      continue;
    }

    auto req = ImageRemoveRequest<I>::create(
        ictx->md_ctx,
        gid_it->second,
        ictx->id,
        gather_ctx->new_sub());

    req->send();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_remove_mirror_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror images: " << cpp_strerror(r)
                 << dendl;
    close_images();
    return;
  }

  if (m_type == Type::ADD_IMAGE) {
    close_images();
  } else {
    remove_mirror_group();
  }
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::remove_mirror_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::mirror_group_remove(&op, m_group_id);

  auto comp = create_rados_callback<
    GroupMirrorEnableUpdateRequest<I>,
    &GroupMirrorEnableUpdateRequest<I>::handle_remove_mirror_group>(this);
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();

}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_remove_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror group: " << cpp_strerror(r)
                 << dendl;
  }

  close_images();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::close_images() {
  if (m_image_ctxs.empty()) {
    finish(m_ret_val);
    return;
  }
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupMirrorEnableUpdateRequest<I>, &GroupMirrorEnableUpdateRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupMirrorEnableUpdateRequest<I>::handle_close_images(int r) {
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
void GroupMirrorEnableUpdateRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  m_on_finish->complete(r);
  delete this;
}

template class GroupMirrorEnableUpdateRequest<librbd::ImageCtx>;

} // namespace snapshot
} // namespace mirror
} // namespace librbd
