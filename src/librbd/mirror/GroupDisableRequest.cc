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
                     << "ths group: " << m_group_name << dendl;
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

  list_group_images();
}

template <typename I>
void GroupDisableRequest<I>::list_group_images() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_image_list_start(&op, m_start_after, MAX_RETURN);

  auto comp = create_rados_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_list_group_images>(this);

  m_out_bl.clear();
  int r = m_group_ioctx.aio_operate(
    librbd::util::group_header_name(m_group_id), comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupDisableRequest<I>::handle_list_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::group_image_list_finish(&iter, &images);
  }

  m_out_bl.clear();

  if (r < 0) {
    lderr(m_cct) << "error listing images in group: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  auto image_count = images.size();
  m_images.insert(m_images.end(), images.begin(), images.end());
  if (image_count == MAX_RETURN) {
    m_start_after = images.rbegin()->spec;
    list_group_images();
    return;
  }

  if (m_images.empty()) {
    set_mirror_group_disabling();
  } else {
    open_images();
  }
}

template <typename I>
void GroupDisableRequest<I>::open_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>, &GroupDisableRequest<I>::handle_open_images>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);
  int r = 0;
  std::vector<librados::IoCtx> ioctxs;

  for (const auto& image: m_images) {
    librados::IoCtx image_io_ctx;
    r = librbd::util::create_ioctx(m_group_ioctx, "image",
                                   image.spec.pool_id, {},
                                   &image_io_ctx);
    if (r < 0) {
      finish(r);
      return;
    }

    ioctxs.push_back(std::move(image_io_ctx));
  }

  for (size_t i = 0; i < m_images.size(); i++) {
    m_image_ctxs.push_back(
      new ImageCtx("", m_images[i].spec.image_id.c_str(), nullptr, ioctxs[i],
                   false));

    auto on_open = new LambdaContext(
      [this, i, new_sub_ctx=gather_ctx->new_sub()](int r) {
        // If asynchronous ImageState::open() fails, ImageState together with
        // ImageCtx is destroyed. Simply NULL-out the respective image_ctxs[i]
        // pointer to record that it's no longer valid.
        if (r < 0) {
          m_image_ctxs[i] = nullptr;
        }
        new_sub_ctx->complete(r);
      });

    // Open parent as well to check if the image is a clone
    m_image_ctxs[i]->state->open(0, on_open);
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_open_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to open group images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  m_is_primary.resize(m_images.size(), false);

  refresh_images();
}

template <typename I>
void GroupDisableRequest<I>::refresh_images() {
  ldout(m_cct, 10) << dendl;

  for (auto ictx : m_image_ctxs) {
    int r = ictx->state->refresh_if_required();
    if (r < 0) {
      CephContext *cct = ictx->cct;
      lderr(cct) << "refresh request failed: " << cpp_strerror(r) << dendl;
      m_ret_val = r;
      close_images();
      return;
    }
  }

  get_images_mirror_mode();
}

template <typename I>
void GroupDisableRequest<I>::get_images_mirror_mode() {
  ldout(m_cct, 10) << dendl;

  for (auto ictx : m_image_ctxs) {
    cls::rbd::MirrorMode mirror_mode;
    CephContext *cct = ictx->cct;
    int r = cls_client::mirror_mode_get(&ictx->md_ctx, &mirror_mode);
    if (r < 0) {
      lderr(cct) << "cannot disable mirroring: failed to retrieve pool "
        "mirroring mode: " << cpp_strerror(r) << dendl;
      m_ret_val = r;
      close_images();
      return;
    }

    if (mirror_mode != cls::rbd::MIRROR_MODE_IMAGE) {
      lderr(cct) << "cannot disable mirroring in the current pool mirroring "
        "mode" << dendl;
      m_ret_val = -EINVAL;
      close_images();
      return;
    }
  }

  get_images_mirror_info();
}

template <typename I>
void GroupDisableRequest<I>::get_images_mirror_info() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_get_images_mirror_info>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  m_mirror_images.resize(m_images.size());
  m_images_primary_mirror_uuids.resize(m_images.size());
  m_images_promotion_states.resize(m_images.size());

  for (size_t i = 0; i < m_images.size(); i++) {
    auto info_ctx = new LambdaContext(
      [this, i, new_sub_ctx=gather_ctx->new_sub()](int r) {
        CephContext *image_cct = m_image_ctxs[i]->cct;
        if (r < 0 && r != -ENOENT) {
          lderr(image_cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
                    << dendl;
          new_sub_ctx->complete(r);
          return;
        } else if (m_mirror_images[i].state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
          lderr(image_cct) << "mirroring is not currently enabled" << dendl;
          m_images_disabled = true;
          new_sub_ctx->complete(-EINVAL);
          return;
        }
        m_is_primary[i] = (m_images_promotion_states[i] == PROMOTION_STATE_PRIMARY ||
                           m_images_promotion_states[i] == PROMOTION_STATE_UNKNOWN);

        if (!m_is_primary[i] && !m_force) {
          lderr(image_cct) << "mirrored image is not primary, "
                           << "add force option to disable mirroring" << dendl;
          new_sub_ctx->complete(-EINVAL);
          return;
        }

        new_sub_ctx->complete(0);
      });
    auto req = GetInfoRequest<I>::create(*m_image_ctxs[i], &m_mirror_images[i],
                                         &m_images_promotion_states[i],
                                         &m_images_primary_mirror_uuids[i], info_ctx);
    req->send();

  }
  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_get_images_mirror_info(int r) {
  ldout(m_cct, 10) << "r=" << r <<  dendl;

  if (r < 0) {
    if (m_images_disabled && m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_DISABLING) {
      ldout(m_cct, 10) << "group images already disabled just need to disable group"
                       << dendl;
      m_images_disabled = false;
      list_group_snapshots();
      return;
    }

    lderr(m_cct) << "failed to get mirror images info: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  check_images_child_mirroring();
}

template <typename I>
void GroupDisableRequest<I>::check_images_child_mirroring() {
  ldout(m_cct, 10) <<  dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto ictx = m_image_ctxs[i];
    auto image_cct = ictx->cct;
    std::shared_lock image_locker{ictx->image_lock};
    for (const auto& info : ictx->snap_info) {
      cls::rbd::ParentImageSpec parent_spec{ictx->md_ctx.get_id(),
                                            ictx->md_ctx.get_namespace(),
                                            ictx->id, info.first};
      std::vector<librbd::linked_image_spec_t> child_images;
      int r = api::Image<I>::list_children(ictx, parent_spec, &child_images);
      if (r < 0) {
        lderr (image_cct) << "failed to list children of image" << dendl;
        m_ret_val = r;
        close_images();
        return;
      }

      if (child_images.empty()) {
        continue;
      }

      librados::IoCtx child_io_ctx;
      int64_t child_pool_id = -1;
      for (auto &child_image : child_images){
        std::string pool = child_image.pool_name;
        if (child_pool_id == -1 ||
            child_pool_id != child_image.pool_id ||
            child_io_ctx.get_namespace() != child_image.pool_namespace) {
          r = librbd::util::create_ioctx(ictx->md_ctx, "child image",
                                         child_image.pool_id,
                                         child_image.pool_namespace,
                                         &child_io_ctx);
          if (r < 0) {
            m_ret_val = r;
            close_images();
            return;
          }

          child_pool_id = child_image.pool_id;
        }

        cls::rbd::MirrorImage child_mirror_image_internal;
        r = cls_client::mirror_image_get(&child_io_ctx, child_image.image_id,
                                        &child_mirror_image_internal);
        if (r != -ENOENT) {
          lderr(image_cct) << "mirroring is enabled on one or more children "
                           << dendl;
          m_ret_val = -EBUSY;
          close_images();
          return;
        }
      }
    }
    image_locker.unlock();
  }

  update_images_read_only_mask();
}

template <typename I>
void GroupDisableRequest<I>::update_images_read_only_mask() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>,
    &GroupDisableRequest<I>::handle_update_images_read_only_mask>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  // don't let the non-primary feature bit prevent image updates
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_image_ctxs[i]->image_lock.lock();
    m_image_ctxs[i]->read_only_mask &= ~IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
    m_image_ctxs[i]->image_lock.unlock();
    m_image_ctxs[i]->state->refresh(gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_update_images_read_only_mask(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update images read only mask: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }
  remove_snapshot_keys();
}

template <typename I>
void GroupDisableRequest<I>::remove_snapshot_keys() {
  ldout(m_cct, 10) << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    std::string mirror_uuid;
    int r = api::Mirror<I>::uuid_get(m_image_ctxs[i]->md_ctx, &mirror_uuid);
    if(r < 0) {
      lderr(m_cct) << "failed to get mirror_uuid" << cpp_strerror(r)
                   << dendl;
      m_ret_val = r;
      close_images();
      return;
    }
    r = m_image_ctxs[i]->operations->metadata_remove(
      mirror::snapshot::util::get_image_meta_key(mirror_uuid));
    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "cannot remove snapshot image-meta key: " << cpp_strerror(r)
                 << dendl;
      m_ret_val = r;
      close_images();
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
    close_images();
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
    close_images();
    return;
  }

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
    lderr(m_cct) << "failed to promote image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
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
    lderr(m_cct) << "failed to refresh image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  remove_mirror_snapshots();
}

template <typename I>
void GroupDisableRequest<I>::remove_mirror_snapshots() {
  ldout(m_cct, 10) << dendl;

  // remove snapshot-based mirroring snapshots
  auto ctx = new LambdaContext([this](int r) {
    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "failed to remove mirroring snapshot for images: "
                   << cpp_strerror(r) << dendl;
      m_ret_val = r;
      close_images();
      return;
    }
    send_remove_mirror_images();
  });

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
          send_remove_snap(i, snap_info.snap_namespace, snap_info.name, gather_ctx->new_sub());
        }
      }
    }
  }
  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::send_remove_snap(size_t i,
    const cls::rbd::SnapshotNamespace &snap_namespace,
    const std::string &snap_name, Context *on_finish) {
  ldout(m_cct, 10) << ", snap_name=" << snap_name << dendl;

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
      m_image_ctxs[i]->md_ctx, m_mirror_images[i].global_image_id, m_image_ctxs[i]->id,
      gather_ctx->new_sub());
    req->send();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupDisableRequest<I>::handle_send_remove_mirror_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror image: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  ldout(m_cct, 20) << "removed images state from rbd_mirroring object" << dendl;
  close_images();
}

template <typename I>
void GroupDisableRequest<I>::close_images() {
  if (m_images.empty()) {
    finish(m_ret_val);
    return;
  }

  ldout(m_cct, 10) << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_image_ctxs[i]->image_lock.lock();
    m_image_ctxs[i]->read_only_mask |= IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
    m_image_ctxs[i]->image_lock.unlock();

    m_image_ctxs[i]->state->handle_update_notification();

    if (m_ret_val < 0) {
      // attempt to restore the image state
      m_mirror_images[i].state = cls::rbd::MIRROR_IMAGE_STATE_ENABLED;
      int r = cls_client::mirror_image_set(&m_image_ctxs[i]->md_ctx, m_image_ctxs[i]->id,
                                           m_mirror_images[i]);
      if (r < 0) {
        lderr(m_cct) << "failed to re-enable image mirroring: "
                     << cpp_strerror(r) << dendl;
      }
    }
  }

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

  if (m_ret_val < 0) {
    finish(m_ret_val);
    return;
  }

  list_group_snapshots();
}

template <typename I>
void GroupDisableRequest<I>::list_group_snapshots() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDisableRequest<I>, &GroupDisableRequest<I>::handle_list_group_snapshots>(this);
  auto req = group::ListSnapshotsRequest<>::create(m_group_ioctx, m_group_id,
                                                   true, true,
                                                   &m_snaps, ctx);
  req->send();
}

template <typename I>
void GroupDisableRequest<I>::handle_list_group_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to list snapshots for group: " << m_group_name << dendl;
    finish(r);
    return;
  }

  remove_group_mirror_snaps();
}

template <typename I>
void GroupDisableRequest<I>::remove_group_mirror_snaps() {
  ldout(m_cct, 10) << dendl;

  std::string group_header_oid = librbd::util::group_header_name(m_group_id);
  for (auto &snap : m_snaps) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &snap.snapshot_namespace);
    if (ns == nullptr) {
      continue;
    }
    int r = cls_client::group_snap_remove(&m_group_ioctx, group_header_oid, snap.id);
    if (r < 0) {
      lderr(m_cct) << "failed to remove group snapshot metadata: "
                   << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
  }

  mirror_group_remove();
}

template <typename I>
void GroupDisableRequest<I>::mirror_group_remove() {
  ldout(m_cct, 10) << dendl;

  int r = cls_client::mirror_group_remove(&m_group_ioctx, m_group_id);
  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove mirroring group metadata: "
                 << cpp_strerror(r) << dendl;
    finish(r);
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

  finish(r);
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
