// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/GroupCreatePrimaryRequest2.h"
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
#include "librbd/mirror/GetMirrorImageRequest.h"
#include "librbd/mirror/snapshot/CreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/CreatePrimaryRequest2.h"
#include "librbd/mirror/snapshot/GroupUnlinkPeerRequest.h"
#include "librbd/mirror/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::GroupCreatePrimaryRequest2: " \
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
void GroupCreatePrimaryRequest2<I>::send() {
  ldout(m_cct, 10) << dendl;

  get_group_id();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::get_group_id() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::dir_get_id_start(&op, m_group_name);

  auto comp = create_rados_callback<
      GroupCreatePrimaryRequest2<I>,
      &GroupCreatePrimaryRequest2<I>::handle_get_group_id>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::handle_get_group_id(int r) {
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

  get_mirror_peer_list();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::get_mirror_peer_list() {
  ldout(m_cct, 10) << dendl;

  m_default_ns_ioctx.dup(m_group_ioctx);
  m_default_ns_ioctx.set_namespace("");

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  auto comp = create_rados_callback<
      GroupCreatePrimaryRequest2<I>,
      &GroupCreatePrimaryRequest2<I>::handle_get_mirror_peer_list>(this);

  m_outbl.clear();
  int r = m_default_ns_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::handle_get_mirror_peer_list(int r) {
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
void GroupCreatePrimaryRequest2<I>::list_group_images() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_image_list_start(&op, m_start_after, MAX_RETURN);

  auto comp = create_rados_callback<
    GroupCreatePrimaryRequest2<I>,
    &GroupCreatePrimaryRequest2<I>::handle_list_group_images>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(
    librbd::util::group_header_name(m_group_id), comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::handle_list_group_images(int r) {
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

  get_mirror_images();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::open_group_images() {
  ldout(m_cct, 10) << dendl;

  if(m_images.empty()) {
    generate_group_snap();
    return;
  }

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest2<I>,
    &GroupCreatePrimaryRequest2<I>::handle_open_group_images>(this);
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
void GroupCreatePrimaryRequest2<I>::handle_open_group_images(int r) {
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
void GroupCreatePrimaryRequest2<I>::generate_group_snap() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);
  m_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{};
  m_group_snap.name = prepare_primary_mirror_snap_name(
    m_cct, m_global_group_id, m_group_snap.id);

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
void GroupCreatePrimaryRequest2<I>::set_snap_metadata() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupCreatePrimaryRequest2<I>,
    &GroupCreatePrimaryRequest2<I>::handle_set_snap_metadata>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                     aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::handle_set_snap_metadata(int r) {
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
    unlink_peer_group(); 
  } else {
    create_image_snapshots();
  }
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::get_mirror_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest2<I>,
    &GroupCreatePrimaryRequest2<I>::handle_get_mirror_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  size_t i = 0;
  m_mirror_images.resize(m_images.size());
  for (i = 0; i < m_images.size(); i++) {
    auto image_spec = m_images[i].spec;
    auto req = GetMirrorImageRequest<I>::create(m_group_ioctx, image_spec.image_id,
                                                &m_mirror_images[i],
                                                gather_ctx->new_sub());
    req->send();
  }
  gather_ctx->activate();

}

template <typename I>
void GroupCreatePrimaryRequest2<I>::handle_get_mirror_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    ldout(m_cct, 10) << "failed to get mirror image info" << dendl;
    finish(r);
    return;
  }

  m_global_image_ids.resize(m_images.size());

  for (size_t i = 0; i < m_mirror_images.size(); i++) {
    if (m_mirror_images[i].state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED || 
        m_mirror_images[i].mode != cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
      lderr(m_cct) << "snapshot based mirroring is not enabled for: "
                   << m_images[i].spec.image_id << dendl;
      finish(-EINVAL);
      return;
    }
    m_global_image_ids[i] = m_mirror_images[i].global_image_id;
  }

  open_group_images();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::create_image_snapshots() {
  ldout(m_cct, 10) << dendl;

  if (m_images.size() == 0){
    m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;
    *m_snap_id = m_group_snap.id;

    set_snap_metadata();

    return;
  }
  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest2<I>,
    &GroupCreatePrimaryRequest2<I>::handle_create_image_snapshots>(this);

  m_clean_since_snap_ids.resize(m_images.size(), CEPH_NOSNAP);
  m_image_snap_ids.resize(m_image_ctxs.size(), CEPH_NOSNAP);

  auto req = CreatePrimaryRequest2<I>::create(m_image_ctxs,
                                       m_global_image_ids,
                                       m_clean_since_snap_ids,
                                       m_snap_create_flags,
                                       0U,
                                       m_group_snap.id,
                                       m_image_snap_ids,
                                       ctx);

  req->send();
}


template <typename I>
void GroupCreatePrimaryRequest2<I>::handle_create_image_snapshots(int r) {
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
void GroupCreatePrimaryRequest2<I>::remove_snap_metadata() {
  ldout(m_cct, 10) << dendl;
  librados::ObjectWriteOperation op;
  cls_client::group_snap_remove(&op, m_group_snap.id);

  auto aio_comp = create_rados_callback<
    GroupCreatePrimaryRequest2<I>,
    &GroupCreatePrimaryRequest2<I>::handle_remove_snap_metadata>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                     aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::handle_remove_snap_metadata(int r) {
  ldout(m_cct, 10) << " r=" << r << dendl;
  if (r < 0) {
    // ignore error
    lderr(m_cct) << "failed to remove group snapshot metadata: "
                 << cpp_strerror(r) << dendl;
  }
  close_images();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::remove_incomplete_group_snap() {
  ldout(m_cct, 10) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest2<I>,
    &GroupCreatePrimaryRequest2<I>::handle_remove_incomplete_group_snap>(this);
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
void GroupCreatePrimaryRequest2<I>::handle_remove_incomplete_group_snap(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  // if previous attempts to remove this snapshot failed then the
  // image's snapshot may not exist
  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove group member image snapshots: "
                 << cpp_strerror(r) << dendl;
    close_images();
    return;
  }

  if (r == 0) {
    remove_snap_metadata();
    return;
  }
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::unlink_peer_group() {
  ldout(m_cct, 10) << dendl;

  // Unlink snaps only if the current snap was created successfully
  if (m_ret_code != 0) {
    close_images();
    return;
  }

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest2<I>,
    &GroupCreatePrimaryRequest2<I>::handle_unlink_peer_group>(this);

  auto req = GroupUnlinkPeerRequest<I>::create(
    m_group_ioctx, m_group_id, &m_mirror_peer_uuids, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::handle_unlink_peer_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unlink group peers: " << cpp_strerror(r)
                 << dendl;
  }

  close_images();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::close_images() {
  ldout(m_cct, 10) << dendl;

  if(m_image_ctxs.empty()) {
      finish(m_ret_code);
      return;
  }

  auto ctx = librbd::util::create_context_callback<
    GroupCreatePrimaryRequest2<I>,
    &GroupCreatePrimaryRequest2<I>::handle_close_images>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    ictx->state->close(gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to close images: " << cpp_strerror(r) << dendl;
  }

  m_image_ctxs.clear();
  finish(m_ret_code);
}

template <typename I>
void GroupCreatePrimaryRequest2<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::GroupCreatePrimaryRequest2<librbd::ImageCtx>;
