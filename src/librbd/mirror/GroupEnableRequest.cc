// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupEnableRequest.h"
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

#include <shared_mutex>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupEnableRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace {

const uint32_t MAX_RETURN = 1024;

} // anonymous namespace


using util::create_context_callback;
using util::create_rados_callback;


template <typename I>
GroupEnableRequest<I>::GroupEnableRequest(librados::IoCtx &io_ctx,
                                          const std::string &group_id,
                                          uint64_t group_snap_create_flags,
                                          cls::rbd::MirrorImageMode mode,
                                          Context *on_finish)
  : m_group_ioctx(io_ctx), m_group_id(group_id),
    m_group_snap_create_flags(group_snap_create_flags), m_mode(mode),
    m_on_finish(on_finish), m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())) {
}

template <typename I>
void GroupEnableRequest<I>::send() {
  get_mirror_group();
}

template <typename I>
void GroupEnableRequest<I>::get_mirror_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_group_get_start(&op, m_group_id);

  using klass = GroupEnableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_mirror_group>(this);

  m_out_bls.resize(1);
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bls[0]);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_get_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    auto iter = m_out_bls[0].cbegin();
    r = cls_client::mirror_group_get_finish(&iter, &m_mirror_group);
  }

  m_out_bls[0].clear();

  if (r == 0) {
    if (m_mirror_group.mirror_image_mode != m_mode) {
      lderr(m_cct) << "invalid group mirroring mode" << dendl;
      r = -EINVAL;
    } else if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLING) {
      lderr(m_cct) << "mirroring on group is currently enabling"
                   << dendl;
      r = -EINVAL;
    } else if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
      ldout(m_cct, 10) << "mirroring on group is already enabled" << dendl;
    } else if (
        m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_DISABLING) {
      lderr(m_cct) << "mirroring on group is currently disabling" << dendl;
      r = -EINVAL;
    } else {
      lderr(m_cct) << "mirroring on group is in unexpected state: "
                   << m_mirror_group.state << dendl;
      r = -EINVAL;
    }
    finish(r);
    return;
  } else if (r < 0) {
    if (r == -EOPNOTSUPP) {
      lderr(m_cct) << "mirroring on group is not supported by OSD" << dendl;
      finish(r);
      return;
    } else if (r != -ENOENT) {
      lderr(m_cct) << "failed to retrieve mirror group metadata: "
                   << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
  }

  uuid_d uuid_gen;
  uuid_gen.generate_random();
  m_mirror_group.global_group_id = uuid_gen.to_string();
  m_mirror_group.mirror_image_mode = m_mode;

  get_mirror_peer_list();
}

template <typename I>
void GroupEnableRequest<I>::get_mirror_peer_list() {
  ldout(m_cct, 10) << dendl;

  m_default_ns_ioctx.dup(m_group_ioctx);
  m_default_ns_ioctx.set_namespace("");

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  auto comp = create_rados_callback<
      GroupEnableRequest<I>,
      &GroupEnableRequest<I>::handle_get_mirror_peer_list>(this);

  int r = m_default_ns_ioctx.aio_operate(RBD_MIRRORING, comp, &op,
                                         &m_out_bls[0]);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_get_mirror_peer_list(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto it = m_out_bls[0].cbegin();
    r = cls_client::mirror_peer_list_finish(&it, &peers);
  }

  m_out_bls[0].clear();

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
void GroupEnableRequest<I>::list_group_images() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_image_list_start(&op, m_start_after, MAX_RETURN);

  auto comp = create_rados_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_list_group_images>(this);

  int r = m_group_ioctx.aio_operate(
    librbd::util::group_header_name(m_group_id), comp, &op, &m_out_bls[0]);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_list_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_out_bls[0].cbegin();
    r = cls_client::group_image_list_finish(&iter, &images);
  }

  m_out_bls[0].clear();

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
    set_mirror_group_enabling();
  } else {
    check_mirror_images_disabled();
  }
}

template <typename I>
void GroupEnableRequest<I>::check_mirror_images_disabled() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_check_mirror_images_disabled>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  m_mirror_images.resize(m_images.size());
  m_out_bls.resize(m_images.size());
  for (size_t i = 0; i < m_images.size(); i++) {
    librados::ObjectReadOperation op;
    cls_client::mirror_image_get_start(&op, m_images[i].spec.image_id);

    auto on_mirror_image_get = new LambdaContext(
      [this, i, new_sub_ctx=gather_ctx->new_sub()](int r) {
        if (r == 0) {
          auto iter = m_out_bls[i].cbegin();
          r = cls_client::mirror_image_get_finish(&iter, &m_mirror_images[i]);
        }

        if (r == -ENOENT) {
          // image is disabled for mirroring as required
          r = 0;
        } else if (r == 0) {
          lderr(m_cct) << "image_id=" << m_images[i].spec.image_id
                       << " is not disabled for mirroring" << dendl;
          r = -EINVAL;
        } else {
          lderr(m_cct) << "failed to get mirror image info for image_id="
                       << m_images[i].spec.image_id << dendl;
        }

        new_sub_ctx->complete(r);
      });

    auto comp = create_rados_callback(on_mirror_image_get);

    int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op,
                                      &m_out_bls[i]);
    ceph_assert(r == 0);
    comp->release();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupEnableRequest<I>::handle_check_mirror_images_disabled(int r) {
  ldout(m_cct, 10) << "r=" << r <<  dendl;

  m_out_bls.clear();

  if (r < 0) {
    lderr(m_cct) << "images not disabled for mirroring: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  open_images();
}

template <typename I>
void GroupEnableRequest<I>::open_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>, &GroupEnableRequest<I>::handle_open_images>(this);
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
void GroupEnableRequest<I>::handle_open_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to open group images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;

    close_images();
    return;
  }

  validate_images();
}

template <typename I>
void GroupEnableRequest<I>::validate_images() {
  ldout(m_cct, 10) << dendl;

  for (auto &image_ctx : m_image_ctxs) {
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
    std::shared_lock image_locker{image_ctx->image_lock};
    if (image_ctx->md_ctx.get_id() != group_pool_id) {
      lderr(m_cct) << "cannot enable mirroring: image in a different pool"
                   << dendl;
      m_ret_val = -EINVAL;

      close_images();
      return;
    }
  }

  set_mirror_group_enabling();
}

template <typename I>
void GroupEnableRequest<I>::set_mirror_group_enabling() {
  ldout(m_cct, 10) << dendl;

  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_ENABLING;

  librados::ObjectWriteOperation op;
  cls_client::mirror_group_set(&op, m_group_id, m_mirror_group);
  auto aio_comp = create_rados_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_set_mirror_group_enabling>(this);
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_set_mirror_group_enabling(int r) {
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
void GroupEnableRequest<I>::create_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);

  auto snap_name = ".mirror.primary." + m_mirror_group.global_group_id
                + "." + m_group_snap.id;
  m_group_snap.name = snap_name;

  cls::rbd::MirrorSnapshotState state = cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY;

  // Create incomplete group snap
  m_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    state, m_mirror_peer_uuids, {}, {}};

  for (auto image_ctx: m_image_ctxs) {
    m_group_snap.snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
                                    CEPH_NOSNAP);
  }

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_create_primary_group_snapshot>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_create_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_need_to_cleanup_group_snapshot = true;

  if (r < 0) {
    lderr(m_cct) << "failed to create group snapshot: "
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
void GroupEnableRequest<I>::create_primary_image_snapshots() {
  ldout(m_cct, 10) << dendl;

  auto num_images = m_image_ctxs.size();
  m_global_image_ids.resize(num_images);

  uuid_d uuid_gen;
  for (size_t i = 0; i < num_images; i++) {
    uuid_gen.generate_random();
    m_global_image_ids[i] = uuid_gen.to_string();
    m_mirror_images[i].global_image_id = m_global_image_ids[i];
  }

  auto ctx = librbd::util::create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_create_primary_image_snapshots>(this);

  m_snap_ids.resize(num_images, CEPH_NOSNAP);

  // quiescing and requesting exclusive locks of images
  auto req = snapshot::GroupImageCreatePrimaryRequest<I>::create(
    m_cct, m_image_ctxs, m_global_image_ids, m_group_snap_create_flags,
    snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS, m_group_snap.id,
    &m_snap_ids, ctx);
  req->send();
}

template <typename I>
void GroupEnableRequest<I>::handle_create_primary_image_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create primary mirror image snapshots: "
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
void GroupEnableRequest<I>::update_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_group_snap.snaps[i].snap_id = m_snap_ids[i];
  }

  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;
  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_update_primary_group_snapshot>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_update_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;

    disable_mirror_group();
    return;
  }

  if (m_image_ctxs.empty()) {
    set_mirror_group_enabled();
  } else {
    set_mirror_images_enabled();
  }
}

template <typename I>
void GroupEnableRequest<I>::set_mirror_images_enabled() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_set_mirror_images_enabled>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto ictx = m_image_ctxs[i];

    m_mirror_images[i].type = cls::rbd::MIRROR_IMAGE_TYPE_GROUP;
    m_mirror_images[i].mode = m_mode;
    m_mirror_images[i].global_image_id = m_global_image_ids[i];

    auto req = ImageStateUpdateRequest<I>::create(
      ictx->md_ctx, ictx->id, cls::rbd::MIRROR_IMAGE_STATE_ENABLED,
      m_mirror_images[i], gather_ctx->new_sub());

    req->send();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupEnableRequest<I>::handle_set_mirror_images_enabled(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_need_to_cleanup_mirror_images = true;

  if (r < 0) {
    lderr(m_cct) << "failed to enabled mirror images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;

    disable_mirror_group();
    return;
  }

  set_mirror_group_enabled();
}

template <typename I>
void GroupEnableRequest<I>::set_mirror_group_enabled() {
  ldout(m_cct, 10) << dendl;

  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_ENABLED;

  librados::ObjectWriteOperation op;
  cls_client::mirror_group_set(&op, m_group_id, m_mirror_group);
  auto aio_comp = create_rados_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_set_mirror_group_enabled>(this);
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_set_mirror_group_enabled(int r) {
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
void GroupEnableRequest<I>::notify_mirroring_watcher() {
  ldout(m_cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_notify_mirroring_watcher>(this);

  MirroringWatcher<I>::notify_group_updated(
          m_group_ioctx, cls::rbd::MIRROR_GROUP_STATE_ENABLED, m_group_id,
          m_mirror_group.global_group_id, m_image_ctxs.size(), ctx);
}

template <typename I>
void GroupEnableRequest<I>::handle_notify_mirroring_watcher(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to notify mirror group update: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
  }

  close_images();
}

template <typename I>
void GroupEnableRequest<I>::close_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>, &GroupEnableRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupEnableRequest<I>::handle_close_images(int r) {
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
void GroupEnableRequest<I>::disable_mirror_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLING;

  cls_client::mirror_group_set(&op, m_group_id, m_mirror_group);
  auto aio_comp = create_rados_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_disable_mirror_group>(this);
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_disable_mirror_group(int r) {
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
void GroupEnableRequest<I>::get_mirror_images_for_cleanup() {
  ldout(m_cct, 10) << dendl;

  m_mirror_images.clear();
  m_mirror_images.resize(m_images.size());
  m_out_bls.resize(m_images.size());

  auto ctx = create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_get_mirror_images_for_cleanup>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_images.size(); i++) {
    librados::ObjectReadOperation op;
    cls_client::mirror_image_get_start(&op, m_images[i].spec.image_id);

    auto on_mirror_image_get = new LambdaContext(
      [this, i, new_sub_ctx=gather_ctx->new_sub()](int r) {
        if (r == 0) {
          auto iter = m_out_bls[i].cbegin();
          r = cls_client::mirror_image_get_finish(&iter, &m_mirror_images[i]);
        }

        if (r == -ENOENT) {
          r = 0;
          m_mirror_images[i].state = cls::rbd::MIRROR_IMAGE_STATE_DISABLED;
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
void GroupEnableRequest<I>::handle_get_mirror_images_for_cleanup(int r) {
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
void GroupEnableRequest<I>::disable_mirror_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_disable_mirror_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_images.size(); i++) {
    if (m_mirror_images[i].state != cls::rbd::MIRROR_IMAGE_STATE_DISABLED) {
      auto req = ImageStateUpdateRequest<I>::create(
        m_image_ctxs[i]->md_ctx, m_image_ctxs[i]->id,
        cls::rbd::MIRROR_IMAGE_STATE_DISABLING, m_mirror_images[i],
        gather_ctx->new_sub());
      req->send();
    }
  }
  gather_ctx->activate();
}

template <typename I>
void GroupEnableRequest<I>::handle_disable_mirror_images(int r) {
  ldout(m_cct, 10) << "r=" << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable mirror images: " << cpp_strerror(r)
                 << dendl;
    close_images();
    return;
  }

  remove_primary_group_snapshot();
}

template <typename I>
void GroupEnableRequest<I>::remove_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_remove_primary_group_snapshot>(this);

  auto req = snapshot::RemoveGroupSnapshotRequest<I>::create(m_group_ioctx,
     m_group_id, &m_group_snap, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupEnableRequest<I>::handle_remove_primary_group_snapshot(int r) {
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
void GroupEnableRequest<I>::remove_mirror_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_remove_mirror_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_images.size(); i++) {
    auto req = ImageRemoveRequest<I>::create(
      m_image_ctxs[i]->md_ctx, m_global_image_ids[i], m_image_ctxs[i]->id,
      gather_ctx->new_sub());
    req->send();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupEnableRequest<I>::handle_remove_mirror_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror images: " << cpp_strerror(r)
                 << dendl;
    close_images();
    return;
  }

  remove_mirror_group();
}

template <typename I>
void GroupEnableRequest<I>::remove_mirror_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::mirror_group_remove(&op, m_group_id);

  auto comp = create_rados_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_remove_mirror_group>(this);
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();

}

template <typename I>
void GroupEnableRequest<I>::handle_remove_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror group: " << cpp_strerror(r)
                 << dendl;
  }

  close_images();
}

template <typename I>
void GroupEnableRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupEnableRequest<librbd::ImageCtx>;
