// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/mirror/GroupEnableRequest.h"
#include "common/Cond.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"
#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "librbd/mirror/snapshot/GroupUnlinkPeerRequest.h"

#include <shared_mutex>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupEnableRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

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
      finish(-EINVAL);
      return;
    }

    if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLING) {
      ldout(m_cct, 10) << "resuming group enable" << dendl;
    } else {
      if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
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
    }
  } else if (r < 0) {
    if (r == -ENOENT) {
      m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLED;
    } else {
      if (r == -EOPNOTSUPP) {
        lderr(m_cct) << "mirroring on group is not supported by OSD" << dendl;
      } else {
        lderr(m_cct) << "failed to retrieve mirror group metadata: "
                     << cpp_strerror(r) << dendl;
      }
      finish(r);
      return;
    }
  }

  prepare_group_images();
}

template<typename I>
void GroupEnableRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_prepare_group_images>(this);

  auto req = snapshot::GroupPrepareImagesRequest<I>::create(m_group_ioctx,
    m_group_id, m_image_ctxs, m_images, nullptr, &m_mirror_peer_uuids, "", // no specific image
    snapshot::GroupPrepareImagesRequest<I>::OP_ENABLE, false, ctx);
  req->send();
}

template<typename I>
void GroupEnableRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images: "
                 << cpp_strerror(r) << dendl;
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
  if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLING) {
    check_primary_group_snap_complete();
    return;
  }

  ldout(m_cct, 10) << dendl;

  uuid_d uuid_gen;
  uuid_gen.generate_random();
  m_mirror_group.global_group_id = uuid_gen.to_string();
  m_mirror_group.mirror_image_mode = m_mode;
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

  check_primary_group_snap_complete();
}

template <typename I>
void GroupEnableRequest<I>::check_primary_group_snap_complete() {
  ldout(m_cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_check_primary_group_snap_complete>(
      this);

  auto req = group::ListSnapshotsRequest<I>::create(
    m_group_ioctx, m_group_id, true, true, &m_group_snaps, ctx);

  req->send();
}

template <typename I>
void GroupEnableRequest<I>::handle_check_primary_group_snap_complete(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to list group snapshots: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  // Inspect the latest mirror group snapshot. If it is primary and complete,
  // resume group enable from that snapshot. Otherwise, create a new primary
  // group snapshot.
  for (auto it = m_group_snaps.rbegin(); it != m_group_snaps.rend(); ++it) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &it->snapshot_namespace);
    if (ns == nullptr) {
      continue;
    }

    if (ns->state != cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
      lderr(m_cct) << "group snapshot '" << it->name
                   << "' namespace state is not primary (state="
                   << ns->state << ")" << dendl;
      m_ret_val = -EINVAL;
      close_images();
      return;
    }

    if (is_mirror_group_snapshot_complete(it->state, ns->complete)) {
      // Resume group enable using the inspected snapshot.
      m_group_snap = *it;
      // Fetch the global image IDs embedded in the member image snapshot
      // names. These IDs are required to enable mirroring for the images.
      fetch_global_image_ids();
      return;
    }

    // Latest mirror snapshot exists but is incomplete.
    break;
  }

  // No usable mirror group snapshot found. Create a new primary group snapshot.
  create_primary_group_snapshot();
}

/**
 * Extract global_image_id from a primary member image snapshot name.
 *
 * primary snapshot name format:
 *  .mirror.primary.<global_image_id>.<pool_id>_<group_id>_<group_snap_id>
 *
 */
std::string_view extract_global_image_id(std::string_view snap_name) {
  constexpr std::string_view prefix = ".mirror.primary.";

  if (!snap_name.starts_with(prefix)) {
    return {};
  }

  auto start = prefix.size();
  auto end = snap_name.find('.', start);
  if (end == std::string_view::npos) {
    return {};
  }

  return snap_name.substr(start, end - start);
}

template <typename I>
void GroupEnableRequest<I>::fetch_global_image_ids() {
  ldout(m_cct, 10) << "group snapshot '" << m_group_snap.name << "'" << dendl;

  auto num_images = m_image_ctxs.size();
  if (num_images != m_group_snap.snaps.size()) {
    lderr(m_cct) << "group snapshot '" << m_group_snap.name
                 << "' member snap count mismatch: snaps.size()="
                 << m_group_snap.snaps.size()
                 << ", expected=" << num_images << dendl;
    m_ret_val = -EINVAL;
    close_images();
    return;
  }

  // Extract each member image's global_image_id from its snapshot name.
  m_global_image_ids.resize(num_images);
  for (size_t i = 0; i < num_images; ++i) {
    auto ictx = m_image_ctxs[i];
    auto snap_spec = m_group_snap.snaps[i];

    // Validate that the image still contains the snapshot referenced by
    // the group snapshot membership.
    if (snap_spec.pool != ictx->md_ctx.get_id() ||
        snap_spec.image_id != ictx->id) {
      lderr(m_cct) << "couldn't locate image '" << snap_spec.image_id
                   << "' for group snapshot '" << m_group_snap.name
                   << "'" << dendl;
      m_ret_val = -EINVAL;
      close_images();
      return;
    }

    std::shared_lock image_locker{ictx->image_lock};
    auto snap_it = ictx->snap_info.find(snap_spec.snap_id);
    if (snap_it == ictx->snap_info.end()) {
      lderr(m_cct) << "image '" << ictx->id
                   << "' missing snap_id=" << snap_spec.snap_id
                   << " for group snapshot '" << m_group_snap.name
                   << "'" << dendl;
      m_ret_val = -EINVAL;
      close_images();
      return;
    }

    // Extract the global_image_id used for enabling a mirror image.
    auto global_image_id = std::string(
      extract_global_image_id(snap_it->second.name));
    uuid_d uuid;
    if (!uuid.parse(global_image_id.c_str())) {
      lderr(m_cct) << "image '" << ictx->id
                   << "' snapshot '" << snap_it->second.name
                   << "' does not contain a valid global_image_id" << dendl;
      m_ret_val = -EINVAL;
      close_images();
      return;
    }
    m_global_image_ids[i] = global_image_id;
    ldout(m_cct, 10) << "image '" << ictx->id
                     << "' global_image_id=" << m_global_image_ids[i]
                     << dendl;
  }

  set_mirror_images_enabled();
}

template <typename I>
void GroupEnableRequest<I>::create_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  // generate_image_id is also used for group and group snapshot ids.
  m_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);

  m_group_snap.name = ".mirror.primary." + m_mirror_group.global_group_id
                + "." + m_group_snap.id;
  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATING;

  ldout(m_cct, 10) << "creating group snapshot " << m_group_snap.name << dendl;

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
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, m_mirror_peer_uuids, {}, {},
    complete};

  for (auto image_ctx: m_image_ctxs) {
    m_group_snap.snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
                                    CEPH_NOSNAP);
  }

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_create_primary_group_snapshot>(this);
  r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_create_primary_group_snapshot(int r) {
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
void GroupEnableRequest<I>::create_primary_image_snapshots() {
  // GroupImageCreatePrimaryRequest asserts if there are no member images.
  // Skip this step and update the primary group snapshot directly.
  if (m_image_ctxs.empty()) {
    update_primary_group_snapshot();
    return;
  }

  ldout(m_cct, 10) << dendl;

  auto num_images = m_image_ctxs.size();
  m_global_image_ids.resize(num_images);

  uuid_d uuid_gen;
  for (size_t i = 0; i < num_images; i++) {
    uuid_gen.generate_random();
    m_global_image_ids[i] = uuid_gen.to_string();
  }

  auto ctx = librbd::util::create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_create_primary_image_snapshots>(this);

  m_snap_ids.resize(num_images, CEPH_NOSNAP);

  // quiescing and requesting exclusive locks of images
  auto req = snapshot::GroupImageCreatePrimaryRequest<I>::create(
    m_cct, m_image_ctxs, m_global_image_ids, m_group_snap_create_flags,
    snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS, m_group_snap.id,
    &m_snap_ids, true, ctx);
  req->send();
}

template <typename I>
void GroupEnableRequest<I>::handle_create_primary_image_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create primary mirror image snapshots: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;

    close_images();
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

  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
  cls::rbd::set_mirror_group_snapshot_complete(m_group_snap);

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

    close_images();
    return;
  }

  set_mirror_images_enabled();
}

template <typename I>
void GroupEnableRequest<I>::set_mirror_images_enabled() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_set_mirror_images_enabled>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  auto num_images = m_image_ctxs.size();
  m_mirror_images.resize(num_images);
  for (size_t i = 0; i < num_images; i++) {
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

  if (r < 0) {
    lderr(m_cct) << "failed to enabled mirror images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;

    close_images();
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

    close_images();
    return;
  }

  group_unlink_peer();
}

template <typename I>
void GroupEnableRequest<I>::group_unlink_peer() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_group_unlink_peer>(this);

  auto req = mirror::snapshot::GroupUnlinkPeerRequest<I>::create(
    m_group_ioctx, m_group_id, &m_mirror_peer_uuids, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupEnableRequest<I>::handle_group_unlink_peer(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unlink mirror group snapshot: "
                 << cpp_strerror(r) << dendl;
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
void GroupEnableRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupEnableRequest<librbd::ImageCtx>;
