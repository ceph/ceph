// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupDisableRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/api/Image.h"
#include "librbd/api/Mirror.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/mirror/snapshot/Utils.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/mirror/GroupGetInfoRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/mirror/ImageRemoveRequest.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"
#include "librbd/mirror/snapshot/PromoteRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"


#include <shared_mutex>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupDisableRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace {

const uint32_t MAX_RETURN = 1024;

} // anonymous namespace


using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
void GroupDisableRequest<I>::send() {
  get_mirror_group_info();
}

template <typename I>
void GroupDisableRequest<I>::get_mirror_group_info() {
  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_get_mirror_group_info>(this);

  auto request = mirror::GroupGetInfoRequest<I>::create(
    m_group_ioctx, "", m_group_id, &m_mirror_group, &m_promotion_state, ctx);
  request->send();
}

template <typename I>
void GroupDisableRequest<I>::handle_get_mirror_group_info(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  if (r == -ENOENT) {
    ldout(m_cct, 10) << "ignoring disable command: mirroring is not enabled for "
                     << "this group: " << m_group_name << dendl;
    finish(0);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to get mirror group info: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  bool is_primary = (m_promotion_state == mirror::PROMOTION_STATE_PRIMARY ||
                     m_promotion_state == mirror::PROMOTION_STATE_UNKNOWN);

  if (!is_primary && !m_force) {
    lderr(m_cct) << "mirrored group " << m_group_name
                 << " is not primary, add force option to disable mirroring"
                 << dendl;
    finish(-EINVAL);
    return;
  }

  prepare_group_images();
}

template <typename I>
void GroupDisableRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_prepare_group_images>(this);

  auto req = snapshot::GroupPrepareImagesRequest<I>::create(m_group_ioctx, m_group_id,
    m_image_ctxs, m_images, &m_mirror_images, &m_is_primary, &m_is_image_disabled,
    &m_mirror_peer_uuids, "disable", m_force, ctx);
  req->send();
}

template <typename I>
void GroupDisableRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_DISABLING) {
      bool all_images_disabled = true;
      for (size_t i = 0; i < m_image_ctxs.size(); i++) {
        all_images_disabled = all_images_disabled & m_is_image_disabled[i];
      }
      if (all_images_disabled) {
        // all images are already disabled
        // just need to perform operations on groups
        list_group_snapshots();
        return;
      }
    }
    lderr(m_cct) << "failed to prepare group images" << dendl;
    m_ret_val = r;
    restore_images_read_only_flag();
    return;
  }

  if (m_images.empty()) {
    set_mirror_group_disabling();
  } else {
    get_images_mirror_uuid();
  }
}

template <typename I>
void GroupDisableRequest<I>::get_images_mirror_uuid() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_get_images_mirror_uuid>(this);

  m_image_mirror_uuid.resize(m_image_ctxs.size());
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    api::Mirror<I>::uuid_get(m_image_ctxs[i]->md_ctx,
          &m_image_mirror_uuid[i], gather_ctx->new_sub());
  }
  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_get_images_mirror_uuid(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to get mirror_uuid" << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    restore_images_read_only_flag();
    return;
  }
  remove_snapshot_keys();
}

template <typename I>
void GroupDisableRequest<I>::remove_snapshot_keys() {
  ldout(m_cct, 10) << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    int r = m_image_ctxs[i]->operations->metadata_remove(
      mirror::snapshot::util::get_image_meta_key(m_image_mirror_uuid[i]));
    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "cannot remove snapshot image-meta key: " << cpp_strerror(r)
                   << dendl;
      m_ret_val = r;
      restore_images_read_only_flag();
      return;
    }
  }

  set_mirror_group_disabling();
}

template <typename I>
void GroupDisableRequest<I>::set_mirror_group_disabling() {
  ldout(m_cct, 10) << dendl;

  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLING;

  librados::ObjectWriteOperation op;
  cls_client::mirror_group_set(&op, m_group_id, m_mirror_group);
  auto aio_comp = create_rados_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_set_mirror_group_disabling>(this);
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupDisableRequest<I>::handle_set_mirror_group_disabling(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to set mirror group as disabling: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    restore_images_read_only_flag();
    return;
  }

  if (m_images.empty()) {
    list_group_snapshots();
  } else {
    set_mirror_images_disabling();
  }
}

template <typename I>
void GroupDisableRequest<I>::set_mirror_images_disabling() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_set_mirror_images_disabling>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto req = ImageStateUpdateRequest<I>::create(
      m_image_ctxs[i]->md_ctx, m_image_ctxs[i]->id,
      cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
      m_mirror_images[i], gather_ctx->new_sub());

    req->send();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_set_mirror_images_disabling(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable mirror images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    restore_images_state();
    return;
  }

  // promote non-primary images in case of force disable
  // to perform delete operations on snapshots and images.
  send_promote_images();
}

template <typename I>
void GroupDisableRequest<I>::send_promote_images() {
  ldout(m_cct, 10) << dendl;

  int non_primary = 0;
  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_send_promote_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    if (m_is_primary[i]) {
      continue;
    }
    non_primary++;
    auto req = mirror::snapshot::PromoteRequest<I>::create(
      m_image_ctxs[i], m_mirror_images[i].global_image_id, gather_ctx->new_sub());

    req->send();
  }

  if (non_primary == 0) {
    delete gather_ctx;
    remove_mirror_snapshots();
    return;
  }
  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_send_promote_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to promote images: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    restore_images_state();
    return;
  }

  send_refresh_images();
}

template <typename I>
void GroupDisableRequest<I>::send_refresh_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_send_refresh_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    if (m_image_ctxs[i]->state->is_refresh_required()) {
      m_image_ctxs[i]->state->refresh(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_send_refresh_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to refresh images: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    restore_images_state();
    return;
  }

  remove_mirror_snapshots();
}

template <typename I>
void GroupDisableRequest<I>::remove_mirror_snapshots() {
  ldout(m_cct, 10) << dendl;

  // remove snapshot-based mirroring snapshots
  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_remove_mirror_snapshots>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    {
      std::lock_guard locker{m_lock};
      std::shared_lock image_locker{m_image_ctxs[i]->image_lock};

      for (auto &it : m_image_ctxs[i]->snap_info) {
        auto &snap_info = it.second;
        auto type = cls::rbd::get_snap_namespace_type(
          snap_info.snap_namespace);
        if (type == cls::rbd::SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
          send_remove_snap(i, snap_info.snap_namespace, snap_info.name,
                           gather_ctx->new_sub());
        }
      }
    }
  }
  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_remove_mirror_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove mirroring snapshot for images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    restore_images_state();
    return;
  }

  send_remove_mirror_images();
}

template <typename I>
void GroupDisableRequest<I>::send_remove_snap(size_t i,
    const cls::rbd::SnapshotNamespace &snap_namespace,
    const std::string &snap_name, Context *on_finish) {
  ldout(m_cct, 10) << "snap_name=" << snap_name << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  auto ctx = new LambdaContext([this, i, snap_namespace, snap_name, on_finish](int r) {
      m_image_ctxs[i]->operations->snap_remove(snap_namespace,
                                               snap_name.c_str(),
                                               on_finish);
    });

  m_image_ctxs[i]->op_work_queue->queue(ctx, 0);
}

template <typename I>
void GroupDisableRequest<I>::send_remove_mirror_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_send_remove_mirror_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto req = ImageRemoveRequest<I>::create(
      m_image_ctxs[i]->md_ctx, m_mirror_images[i].global_image_id,
      m_image_ctxs[i]->id, gather_ctx->new_sub());
    req->send();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_send_remove_mirror_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    restore_images_state();
    return;
  }

  ldout(m_cct, 20) << "removed images state from rbd_mirroring object" << dendl;
  list_group_snapshots();
}

template <typename I>
void GroupDisableRequest<I>::list_group_snapshots() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>, &GroupDisableRequest<I>::handle_list_group_snapshots>(this);
  auto req = group::ListSnapshotsRequest<>::create(m_group_ioctx, m_group_id,
                                                   true, true, &m_snaps, ctx);
  req->send();
}

template <typename I>
void GroupDisableRequest<I>::handle_list_group_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to list snapshots for group: "
                 << m_group_name << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  remove_group_mirror_snaps();
}

template <typename I>
void GroupDisableRequest<I>::remove_group_mirror_snaps() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_remove_group_mirror_snaps>(this);

  std::string group_header_oid = librbd::util::group_header_name(m_group_id);
  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (auto &snap : m_snaps) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &snap.snapshot_namespace);
    if (ns == nullptr) {
      continue;
    }
    librados::ObjectWriteOperation op;
    cls_client::group_snap_remove(&op, snap.id);

    auto on_group_snap_remove = new LambdaContext(
      [this, new_sub_ctx=gather_ctx->new_sub()](int r) {
        if (r < 0) {
          lderr(m_cct) << "failed to remove group snapshot metadata: "
                       << cpp_strerror(r) << dendl;
        }
        new_sub_ctx->complete(r);
      });

    auto comp = create_rados_callback(on_group_snap_remove);
    int r = m_group_ioctx.aio_operate(group_header_oid, comp, &op);
    ceph_assert(r == 0);
    comp->release();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_remove_group_mirror_snaps(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove group mirror snapshots" << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  remove_mirror_group();
}

template <typename I>
void GroupDisableRequest<I>::remove_mirror_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_remove(&op, m_group_id);

  auto aio_comp = create_rados_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_remove_mirror_group>(this);

  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupDisableRequest<I>::handle_remove_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirroring group: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  notify_mirroring_watcher();
}

template <typename I>
void GroupDisableRequest<I>::notify_mirroring_watcher() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_notify_mirroring_watcher>(this);

  librbd::MirroringWatcher<I>::notify_group_updated(
    m_group_ioctx, cls::rbd::MIRROR_GROUP_STATE_DISABLED, m_group_id,
    m_mirror_group.global_group_id, m_images.size(), ctx);
}

template <typename I>
void GroupDisableRequest<I>::handle_notify_mirroring_watcher(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to notify mirroring group=" << m_group_name
                 << " updated: " << cpp_strerror(r) << dendl;
  }

  close_images();
}

template <typename I>
void GroupDisableRequest<I>::close_images() {
  if (m_image_ctxs.empty()) {
    finish(m_ret_val);
    return;
  }
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>, &GroupDisableRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (auto ictx: m_image_ctxs) {
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_close_images(int r) {
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
void GroupDisableRequest<I>::restore_images_state() {
  if (m_image_ctxs.empty()) {
    finish(m_ret_val);
    return;
  }
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_restore_images_state>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    librados::ObjectWriteOperation op;
    // attempt to restore the image state
    m_mirror_images[i].state = cls::rbd::MIRROR_IMAGE_STATE_ENABLED;
    cls_client::mirror_image_set(&op, m_image_ctxs[i]->id, m_mirror_images[i]);

    auto on_mirror_image_set = new LambdaContext(
      [this, i, new_sub_ctx=gather_ctx->new_sub()](int r) {
        if (r < 0) {
          lderr(m_image_ctxs[i]->cct) << "failed to re-enable image mirroring: "
                                      << cpp_strerror(r) << dendl;
        }
        new_sub_ctx->complete(r);
      });

    auto comp = create_rados_callback(on_mirror_image_set);
    int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op);
    ceph_assert(r == 0);
    comp->release();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_restore_images_state(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to restore images state" << dendl;
  }

  restore_images_read_only_flag();
}

template <typename I>
void GroupDisableRequest<I>::restore_images_read_only_flag() {
  if (m_image_ctxs.empty()) {
    finish(m_ret_val);
    return;
  }
  ldout(m_cct, 10) << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_image_ctxs[i]->image_lock.lock();
    m_image_ctxs[i]->read_only_mask |= IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
    m_image_ctxs[i]->image_lock.unlock();

    m_image_ctxs[i]->state->handle_update_notification();
  }

  close_images();
}

template <typename I>
void GroupDisableRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupDisableRequest<librbd::ImageCtx>;
