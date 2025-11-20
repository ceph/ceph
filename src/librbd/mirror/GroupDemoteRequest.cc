// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupDemoteRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/Utils.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/mirror/GroupGetInfoRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/mirror/snapshot/GroupUnlinkPeerRequest.h"
#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"


#include <shared_mutex>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupDemoteRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace {

const uint32_t MAX_RETURN = 1024;

} // anonymous namespace


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

  auto request = mirror::GroupGetInfoRequest<I>::create(
    m_group_ioctx, "", m_group_id, &m_mirror_group, &m_promotion_state, ctx);
  request->send();
}

template <typename I>
void GroupDemoteRequest<I>::handle_get_mirror_group_info(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  if (r == -ENOENT) {
    ldout(m_cct, 10) << "group is not enabled for mirroring: " << m_group_name
                     << dendl;
    finish(-EINVAL);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to get mirror group info: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_promotion_state != mirror::PROMOTION_STATE_PRIMARY) {
    lderr(m_cct) << "group " << m_group_name << " is not primary" << dendl;
    finish(-EINVAL);
    return;
  }

  get_mirror_peer_list();
}


template <typename I>
void GroupDemoteRequest<I>::get_mirror_peer_list() {
  ldout(m_cct, 10) << dendl;

  m_default_ns_ioctx.dup(m_group_ioctx);
  m_default_ns_ioctx.set_namespace("");

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  auto comp = create_rados_callback<
      GroupDemoteRequest<I>,
      &GroupDemoteRequest<I>::handle_get_mirror_peer_list>(this);
  m_out_bl.clear();
  int r = m_default_ns_ioctx.aio_operate(RBD_MIRRORING, comp, &op,
                                         &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupDemoteRequest<I>::handle_get_mirror_peer_list(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = cls_client::mirror_peer_list_finish(&it, &peers);
  }

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirror peers: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  for (auto &peer : peers) {
    if (peer.mirror_peer_direction == cls::rbd::MIRROR_PEER_DIRECTION_RX) {
      continue;
    }
    m_mirror_peer_uuids.insert(peer.uuid);
  }

  if (m_mirror_peer_uuids.empty()) {
    lderr(m_cct) << "no mirror tx peers configured for the pool" << dendl;
    finish(-EINVAL);
    return;
  }

  list_group_images();
}

template <typename I>
void GroupDemoteRequest<I>::list_group_images() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_image_list_start(&op, m_start_after, MAX_RETURN);

  auto comp = create_rados_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_list_group_images>(this);

  m_out_bl.clear();
  int r = m_group_ioctx.aio_operate(
    librbd::util::group_header_name(m_group_id), comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupDemoteRequest<I>::handle_list_group_images(int r) {
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
    create_primary_group_snapshot();
  } else {
    open_images();
  }
}

template <typename I>
void GroupDemoteRequest<I>::open_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>, &GroupDemoteRequest<I>::handle_open_images>(this);
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
void GroupDemoteRequest<I>::handle_open_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to open group images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;

    close_images();
    return;
  }

  const size_t num_images = m_images.size();


  m_locks_acquired.resize(num_images, false);

  get_images_mirror_info();
}

template <typename I>
void GroupDemoteRequest<I>::get_images_mirror_info() {
  ldout(m_cct, 10) << dendl;
  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_get_images_mirror_info>(this);
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
          new_sub_ctx->complete(-EINVAL);
          return;
        } else if (m_images_promotion_states[i] != PROMOTION_STATE_PRIMARY) {
          lderr(image_cct) << "image is not primary" << dendl;
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
void GroupDemoteRequest<I>::handle_get_images_mirror_info(int r) {
  ldout(m_cct, 10) << "r=" << r <<  dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to get mirror images info: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  update_images_read_only_mask();
}

template <typename I>
void GroupDemoteRequest<I>::update_images_read_only_mask() {

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_update_images_read_only_mask>(this);
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
void GroupDemoteRequest<I>::handle_update_images_read_only_mask(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update images read only mask: "
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

  auto ctx = create_context_callback<GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_acquire_exclusive_locks>(this);
  m_excl_locks_acquired = true;
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    {
      std::unique_lock locker{m_image_ctxs[i]->owner_lock};
      if (m_image_ctxs[i]->exclusive_lock != nullptr &&
          !m_image_ctxs[i]->exclusive_lock->is_lock_owner()) {
        CephContext *cct = m_image_ctxs[i]->cct;
        ldout(cct, 15) << dendl;
        m_locks_acquired[i] = true;
        m_image_ctxs[i]->exclusive_lock->block_requests(0);
        m_image_ctxs[i]->exclusive_lock->acquire_lock(gather_ctx->new_sub());
      }
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupDemoteRequest<I>::handle_acquire_exclusive_locks(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to acquire exclusive lock: " << cpp_strerror(r)
               << dendl;
    m_ret_val = r;
    release_exclusive_locks();
    return;
  } else {
    for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
      std::unique_lock locker{m_image_ctxs[i]->owner_lock};
      if (m_image_ctxs[i]->exclusive_lock != nullptr &&
          !m_image_ctxs[i]->exclusive_lock->is_lock_owner()) {
        lderr(m_cct) << "failed to acquire exclusive lock" << dendl;
        r = m_image_ctxs[i]->exclusive_lock->get_unlocked_op_error();
        m_ret_val = r;
        locker.unlock();
        release_exclusive_locks();
        return;
      }
    }
  }

  create_primary_group_snapshot();
}


template <typename I>
void GroupDemoteRequest<I>::create_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);

  auto snap_name = ".mirror.primary." + m_mirror_group.global_group_id
                + "." + m_group_snap.id;
  m_group_snap.name = snap_name;

  cls::rbd::MirrorSnapshotState state = cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED;
  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATING;

  librados::Rados rados(m_group_ioctx);
  int8_t require_osd_release;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve min OSD release: " << cpp_strerror(r)
               << dendl;
    m_ret_val = r;
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
    remove_primary_group_snapshot();
    return;
  }
  
  if(m_images.empty()) {
    update_primary_group_snapshot();
  } else {
    enable_non_primary_features();
  }
}

template <typename I>
void GroupDemoteRequest<I>::enable_non_primary_features() {
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

    auto on_enable = new LambdaContext(
      [this, new_sub_ctx = gather_ctx->new_sub()](int r) {
        if (r < 0) {
          lderr(m_cct) << "failed to disable non-primary feature: "
                       << cpp_strerror(r) << dendl;
        }
        new_sub_ctx->complete(r);
      });

    auto aio_comp = create_rados_callback(on_enable);

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
    lderr(m_cct) << "failed to disable non-primary feature: "
               << cpp_strerror(r) << dendl;
    m_ret_val = r;
    remove_primary_group_snapshot();
    return;
  }

  create_images_primary_snapshots();
}

template <typename I>
void GroupDemoteRequest<I>::create_images_primary_snapshots() {
  if (m_images.empty()) {
    update_primary_group_snapshot();
    return;
  }

  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_create_images_primary_snapshots>(this);

  m_snap_ids.resize(m_image_ctxs.size(), CEPH_NOSNAP);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_global_image_ids.push_back(m_mirror_images[i].global_image_id);
  }

  auto req = snapshot::GroupImageCreatePrimaryRequest<I>::create(
    m_cct, m_image_ctxs, m_global_image_ids, SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE,
    (snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS |
      snapshot::CREATE_PRIMARY_FLAG_DEMOTED), m_group_snap.id, &m_snap_ids, !m_excl_locks_acquired, ctx);
  req->send();
}


template <typename I>
void GroupDemoteRequest<I>::handle_create_images_primary_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create primary mirror image snapshots: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;

    for (size_t i = 0; i < m_image_ctxs.size(); i++) {
      m_group_snap.snaps[i].snap_id = m_snap_ids[i];
    }
    remove_primary_group_snapshot();
    return;
  }

  update_primary_group_snapshot();
}

template <typename I>
void GroupDemoteRequest<I>::update_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_group_snap.snaps[i].snap_id = m_snap_ids[i];
  }
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
    remove_primary_group_snapshot();
    return;
  }

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
    m_ret_val = r;
    lderr(m_cct) << "failed to unlink mirror group snapshot: " << cpp_strerror(r)
                 << dendl;
    remove_primary_group_snapshot();
    return;
  }
  release_exclusive_locks();
}

template <typename I>
void GroupDemoteRequest<I>::release_exclusive_locks() {
  ldout(m_cct, 10) << dendl;
  if(!m_excl_locks_acquired) {
    close_images();
    return;
  }
  auto ctx = librbd::util::create_context_callback<
    GroupDemoteRequest<I>,
    &GroupDemoteRequest<I>::handle_release_exclusive_locks>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    if (m_locks_acquired[i]) {
      std::unique_lock locker{m_image_ctxs[i]->owner_lock};
      if (m_image_ctxs[i]->exclusive_lock != nullptr) {
        m_image_ctxs[i]->exclusive_lock->unblock_requests();
        m_image_ctxs[i]->exclusive_lock->release_lock(gather_ctx->new_sub());
      }
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
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }
  close_images();
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
void GroupDemoteRequest<I>::close_images() {
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
  }

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
void GroupDemoteRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupDemoteRequest<librbd::ImageCtx>;

