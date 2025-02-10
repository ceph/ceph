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
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Operations.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/mirror/snapshot/CreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/GroupUnlinkPeerRequest.h"
#include "librbd/mirror/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::GroupCreatePrimaryRequest: " \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

namespace {

const uint32_t MAX_RETURN = 1024;

} // anonymous namespace

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
struct C_ImageSnapshotCreate : public Context {
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

  C_ImageSnapshotCreate(I *ictx, uint64_t snap_create_flags,
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
      lderr(ictx->cct) << "snapshot based mirroring is not enabled for "
                       << ictx->id << dendl;
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
void image_snapshot_create(I *ictx, uint64_t snap_create_flags,
                            const std::string &group_snap_id,
                            uint64_t *snap_id, Context *on_finish) {
  CephContext *cct = ictx->cct;
  ldout(cct, 10) << "ictx=" << ictx << dendl;

  auto on_refresh = new LambdaContext(
    [ictx, snap_create_flags, group_snap_id, snap_id, on_finish](int r) {
      if (r < 0) {
        lderr(ictx->cct) << "refresh failed: " << cpp_strerror(r) << dendl;
        on_finish->complete(r);
        return;
      }
//TODO: validate the images earlier.
      auto ctx = new C_ImageSnapshotCreate<I>(ictx, snap_create_flags,
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
// TODO: Do away with this check as we will eventually use this
// class when promoting groups as well
  if (state != cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
    lderr(m_cct) << "group " << m_group_name << " is not primary" << dendl;
    finish(-EINVAL);
    return;
  }

  get_mirror_peer_list();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::get_mirror_peer_list() {
  ldout(m_cct, 10) << dendl;

  m_default_ns_ioctx.dup(m_group_ioctx);
  m_default_ns_ioctx.set_namespace("");

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  auto comp = create_rados_callback<
      GroupCreatePrimaryRequest<I>,
      &GroupCreatePrimaryRequest<I>::handle_get_mirror_peer_list>(this);

  m_outbl.clear();
  int r = m_default_ns_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_get_mirror_peer_list(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto it = m_outbl.cbegin();
    r = cls_client::mirror_peer_list_finish(&it, &peers);
  }

  if (r < 0) {
    lderr(m_cct) << "error listing mirror peers" << cpp_strerror(r) << dendl;
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
void GroupCreatePrimaryRequest<I>::list_group_images() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_image_list_start(&op, m_start_after, MAX_RETURN);

  auto comp = create_rados_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_list_group_images>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(
    librbd::util::group_header_name(m_group_id), comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_list_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_outbl.cbegin();
    r = cls_client::group_image_list_finish(&iter, &images);
  }

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

  open_group_images();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::open_group_images() {
  ldout(m_cct, 10) << dendl;

  if(m_images.empty()) {
    generate_group_snap();
    return;
  }

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_open_group_images>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  int r = 0;
  for (size_t i = 0; i < m_images.size(); i++) {
    auto &image = m_images[i];
    librbd::IoCtx image_io_ctx;
    r = librbd::util::create_ioctx(m_group_ioctx, "image",
                                   image.spec.pool_id, {},
                                   &image_io_ctx);
    if (r < 0) {
      m_ret_code = r;
      break;
    }

    librbd::ImageCtx* image_ctx = new ImageCtx("", image.spec.image_id.c_str(),
                                               nullptr, image_io_ctx, false);

    m_image_ctxs.push_back(image_ctx);
    image_ctx->state->open(0, gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_open_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && m_ret_code == 0) {
    m_ret_code = r;
  }

  if (m_ret_code < 0) {
    lderr(m_cct) << "failed to open group images: " << cpp_strerror(m_ret_code)
                 << dendl;
    close_images();
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

  // TODO: Fix this to handle primary demoted snaps
  cls::rbd::MirrorSnapshotState state = cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY;

  // Create incomplete group snap
  m_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    state, m_mirror_peer_uuids, {}, {}};

  for (auto image_ctx: m_image_ctxs) {
    m_group_snap.snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
                                    CEPH_NOSNAP);
  }

  set_snap_metadata();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::set_snap_metadata() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_set_snap_metadata>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                     aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_set_snap_metadata(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  if (r < 0) {
    lderr(m_cct) << "failed to set group snapshot metadata: " << cpp_strerror(r)
                 << dendl;
    m_ret_code = r;
    if (m_group_snap.state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
      remove_incomplete_group_snap();
    } else {
      close_images();
    }
    return;
  }

  if (m_group_snap.state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
      release_image_exclusive_locks();
  } else {
    notify_quiesce();
  }
}

template <typename I>
void GroupCreatePrimaryRequest<I>::notify_quiesce() {
  ldout(m_cct, 10) << dendl;

  if ((m_internal_flags & SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE) != 0) {
    acquire_image_exclusive_locks();
    return;
  }
  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_notify_quiesce>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  int image_count = m_image_ctxs.size();
  m_quiesce_requests.resize(image_count);

  for (int i = 0; i < image_count; ++i) {
    auto ictx = (m_image_ctxs)[i];
    ictx->image_watcher->notify_quiesce(&(m_quiesce_requests)[i], m_prog_ctx,
                                        gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_notify_quiesce(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 &&
      (m_internal_flags & SNAP_CREATE_FLAG_IGNORE_NOTIFY_QUIESCE_ERROR) == 0) {
    m_ret_code = r;
    notify_unquiesce();
    return;
  }

  acquire_image_exclusive_locks();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::acquire_image_exclusive_locks() {
  ldout(m_cct, 10) << dendl;

  m_release_locks = true;

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_acquire_image_exclusive_locks>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    std::shared_lock owner_lock{ictx->owner_lock};
    if (ictx->exclusive_lock != nullptr) {
      ictx->exclusive_lock->block_requests(-EBUSY);
      ictx->exclusive_lock->acquire_lock(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_acquire_image_exclusive_locks(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to acquire image exclusive locks: "
                 << cpp_strerror(r) << dendl;
    m_ret_code = r;
    remove_snap_metadata();
    return;
  }
  create_image_snaps();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::remove_snap_metadata() {
  ldout(m_cct, 10) << dendl;
  librados::ObjectWriteOperation op;
  cls_client::group_snap_remove(&op, m_group_snap.id);

  auto aio_comp = create_rados_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_remove_snap_metadata>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                     aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_remove_snap_metadata(int r) {
  ldout(m_cct, 10) << " r=" << r << dendl;
  if (r < 0) {
    // ignore error
    lderr(m_cct) << "failed to remove group snapshot metadata: "
                 << cpp_strerror(r) << dendl;
  }
  release_image_exclusive_locks();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::create_image_snaps() {
  ldout(m_cct, 10) << "group name: " << m_group_name
                   << ", group ID: " << m_group_id
                   << ", group snap ID: " << m_group_snap.id << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_create_image_snaps>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  m_image_snap_ids.resize(m_image_ctxs.size(), CEPH_NOSNAP);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    image_snapshot_create(m_image_ctxs[i], SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE,
			  m_group_snap.id,  &m_image_snap_ids[i],
                          gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_create_image_snaps(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::string group_header_oid = librbd::util::group_header_name(m_group_id);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_group_snap.snaps[i].snap_id = m_image_snap_ids[i];
  }

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
    m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;
    *m_snap_id = m_group_snap.id;

    set_snap_metadata();
  }
}

template <typename I>
void GroupCreatePrimaryRequest<I>::remove_incomplete_group_snap() {
  ldout(m_cct, 10) << dendl;


  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_remove_incomplete_group_snap>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);


  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    auto snap_id = m_group_snap.snaps[i].snap_id;
    if (snap_id == CEPH_NOSNAP) {
      continue;
    }

    librbd::ImageCtx *ictx = m_image_ctxs[i];

    std::shared_lock image_locker{ictx->image_lock};
    auto info = ictx->get_snap_info(snap_id);
    ceph_assert(info != nullptr);
    image_locker.unlock();

    ldout(m_cct, 10) << "removing individual snapshot: "
                     << snap_id << dendl;

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
    lderr(m_cct) << "failed to remove group member image snapshots: "
                 << cpp_strerror(r) << dendl;
  }

  if (r == 0) {
    remove_snap_metadata();
    return;
  }

  release_image_exclusive_locks();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::notify_unquiesce() {
  ldout(m_cct, 10) << dendl;

  if (m_quiesce_requests.empty()) {
    unlink_peer_group();
    return;
  }

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

  // Unlink snaps only if the current snap was created successfully
  if (m_ret_code != 0) {
    close_images();
    return;
  }

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
void GroupCreatePrimaryRequest<I>::release_image_exclusive_locks() {
  ldout(m_cct, 10) << dendl;

  if(!m_release_locks){
    notify_unquiesce();
    return;
  }
  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest<I>,
    &GroupCreatePrimaryRequest<I>::handle_release_image_exclusive_locks>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    std::shared_lock owner_lock{ictx->owner_lock};
    if (ictx->exclusive_lock != nullptr) {
      ictx->exclusive_lock->release_lock(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::handle_release_image_exclusive_locks(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to release exclusive locks for images: "
                 << cpp_strerror(r) << dendl;
  }
  notify_unquiesce();
}

template <typename I>
void GroupCreatePrimaryRequest<I>::close_images() {
  ldout(m_cct, 10) << dendl;

  if(m_image_ctxs.empty()) {
      finish(m_ret_code);
      return;
  }

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
