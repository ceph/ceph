// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "RemoveLocalGroupRequest.h"
#include "include/rados/librados.hpp"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Types.h"
#include "librbd/Utils.h"
#include "librbd/group/AddImageRequest.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/group/RemoveImageRequest.h"
#include "librbd/mirror/GroupGetInfoRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
#include "tools/rbd_mirror/image_deleter/TrashMoveRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::" \
                           << "RemoveLocalGroupRequest: " << this << " " \
                           << __func__ << ": "
namespace {
  static const uint32_t MAX_RETURN = 1024;
} // anonymous namespace

namespace rbd {
namespace mirror {
namespace group_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void RemoveLocalGroupRequest<I>::send() {
  get_local_group_id();
}

template <typename I>
void RemoveLocalGroupRequest<I>::get_local_group_id() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_group_get_group_id_start(&op, m_global_group_id);

  m_out_bl.clear();
  auto comp = create_rados_callback<
      RemoveLocalGroupRequest<I>,
      &RemoveLocalGroupRequest<I>::handle_get_local_group_id>(this);

  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_get_local_group_id(int r) {
  dout(10) << "r=" << r << ", global_group_id: " << m_global_group_id << dendl;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_group_get_group_id_finish(
        &iter, &m_group_id);
  }

  if (r < 0) {
    if (r != -ENOENT) {
      derr << "error getting local group id: " << cpp_strerror(r) << dendl;
    }
    finish(r);
    return;
  }

  get_local_group_name();
}

template <typename I>
void RemoveLocalGroupRequest<I>::get_local_group_name() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_name_start(&op, m_group_id);

  m_out_bl.clear();
  auto comp = create_rados_callback<
      RemoveLocalGroupRequest<I>,
      &RemoveLocalGroupRequest<I>::handle_get_local_group_name>(this);

  int r = m_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op,
                                      &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_get_local_group_name(int r) {
  dout(10) << "r=" << r << dendl;

  m_group_name = "";
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::dir_get_name_finish(&iter, &m_group_name);
  }

  if (r == -ENOENT) {
    // proceed - we should have a mirror group record if we got this far
    dout(10) << "local group does not exist for id " << m_group_id
             << dendl;
  } else if (r < 0) {
    derr << "error getting local group name: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_mirror_group();
}

template <typename I>
void RemoveLocalGroupRequest<I>::get_mirror_group() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    RemoveLocalGroupRequest<I>,
    &RemoveLocalGroupRequest<I>::handle_get_mirror_group>(this);
  auto req = librbd::mirror::GroupGetInfoRequest<I>::create(
    m_io_ctx, "", m_group_id, &m_mirror_group, &m_promotion_state,
    ctx);
  req->send();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_get_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    if (r == -ENOENT) {
      dout(10) << "group " << m_global_group_id << " is not mirrored" << dendl;
    } else {
      derr << "error retrieving mirror info for group "
           << m_global_group_id << ": " << cpp_strerror(r) << dendl;
    }
    finish(r);
    return;
  }

  if (m_promotion_state == librbd::mirror::PROMOTION_STATE_PRIMARY) {
    dout(10) << "group " << m_global_group_id << " is local primary" << dendl;
    finish(-EPERM);
    return;
  } else if (m_promotion_state == librbd::mirror::PROMOTION_STATE_ORPHAN &&
             !m_resync) {
    dout(10) << "group " << m_global_group_id << " is orphaned" << dendl;
    finish(-EPERM);
    return;
  }

  if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
    m_notify_watcher = true;
  }

  disable_mirror_group();
}

template <typename I>
void RemoveLocalGroupRequest<I>::disable_mirror_group() {
  dout(10) << dendl;

  // need to send 'disabling' since the cls methods will fail if we aren't
  // in that state
  m_mirror_group.state= cls::rbd::MIRROR_GROUP_STATE_DISABLING;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_set(&op, m_group_id,
                                       m_mirror_group);

  auto aio_comp = create_rados_callback<
    RemoveLocalGroupRequest<I>,
    &RemoveLocalGroupRequest<I>::handle_disable_mirror_group>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_disable_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to disable mirror group " << m_global_group_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  list_group_snapshots();
}

template <typename I>
void RemoveLocalGroupRequest<I>::list_group_snapshots() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    RemoveLocalGroupRequest<I>,
    &RemoveLocalGroupRequest<I>::handle_list_group_snapshots>(this);
  auto req = librbd::group::ListSnapshotsRequest<I>::create(
    m_io_ctx, m_group_id, false, false, &m_group_snaps, ctx);
  req->send();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_list_group_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    list_group_images();
    return;
  } else if (r < 0) {
    derr << "failed to list local group snapshots: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  open_snapshot_images();
}

template <typename I>
void RemoveLocalGroupRequest<I>::open_snapshot_images() {
  dout(10) << dendl;

  std::set<cls::rbd::GroupImageSpec> image_specs;
  for (const auto& group_snap : m_group_snaps) {
    for (const auto& image_snap : group_snap.snaps) {
      if (image_snap.snap_id != CEPH_NOSNAP) {
        image_specs.emplace(image_snap.image_id, image_snap.pool);
      }
    }
  }

  std::vector<std::pair<cls::rbd::GroupImageSpec, librados::IoCtx>> images;
  for (const auto& image_spec : image_specs) {
    librados::IoCtx image_io_ctx;
    int r = librbd::util::create_ioctx(m_io_ctx, "image", image_spec.pool_id,
                                       {}, &image_io_ctx);
    if (r == -ENOENT) {
      dout(10) << "image pool no longer exists: pool_id="
               << image_spec.pool_id << dendl;
      continue;
    } else if (r < 0) {
      derr << "failed to open image pool " << image_spec.pool_id << ": "
           << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
    images.emplace_back(image_spec, std::move(image_io_ctx));
  }

  auto ctx = create_context_callback<
    RemoveLocalGroupRequest<I>,
    &RemoveLocalGroupRequest<I>::handle_open_snapshot_images>(this);
  auto cct = reinterpret_cast<CephContext*>(m_io_ctx.cct());
  auto gather_ctx = new C_Gather(cct, ctx);

  m_snapshot_image_ctxs.reserve(images.size());
  for (auto& [image_spec, image_io_ctx] : images) {
    auto image_ctx = I::create("", image_spec.image_id, nullptr, image_io_ctx,
                               false);
    image_ctx->read_only_mask &= ~librbd::IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
    m_snapshot_image_ctxs.push_back(image_ctx);
    auto image_index = m_snapshot_image_ctxs.size() - 1;

    auto on_open = new LambdaContext(
      [this, image_index, image_spec,
       on_finish=gather_ctx->new_sub()](int r) {
        if (r < 0) {
          m_snapshot_image_ctxs[image_index] = nullptr;
          if (r == -ENOENT) {
            // An earlier cleanup may have removed the image snapshots but
            // failed to remove the group snapshot metadata. After clearing
            // all remaining snapshots, the user can delete a former member
            // separately. Resync can still find its old ID in that metadata.
            dout(10) << "snapshot image no longer exists: image_id="
                     << image_spec.image_id << ", pool_id="
                     << image_spec.pool_id << dendl;
            r = 0;
          }
        }
        on_finish->complete(r);
      });
    image_ctx->state->open(librbd::OPEN_FLAG_SKIP_OPEN_PARENT, on_open);
  }

  gather_ctx->activate();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_open_snapshot_images(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to open group snapshot images: " << cpp_strerror(r)
         << dendl;
    m_ret_val = r;
    close_snapshot_images();
    return;
  }

  remove_group_snapshot();
}

template <typename I>
void RemoveLocalGroupRequest<I>::remove_group_snapshot() {
  if (m_group_snap_index == m_group_snaps.size()) {
    close_snapshot_images();
    return;
  }

  auto& group_snap = m_group_snaps[m_group_snap_index];
  dout(10) << "group_snap_id=" << group_snap.id << dendl;

  auto ctx = create_context_callback<
    RemoveLocalGroupRequest<I>,
    &RemoveLocalGroupRequest<I>::handle_remove_group_snapshot>(this);
  auto req = librbd::mirror::snapshot::RemoveGroupSnapshotRequest<I>::create(
    m_io_ctx, m_group_id, &group_snap, &m_snapshot_image_ctxs, ctx);
  req->send();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_remove_group_snapshot(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "failed to remove local group snapshot: " << cpp_strerror(r)
         << dendl;
    m_ret_val = r;
    close_snapshot_images();
    return;
  }

  ++m_group_snap_index;
  remove_group_snapshot();
}

template <typename I>
void RemoveLocalGroupRequest<I>::close_snapshot_images() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    RemoveLocalGroupRequest<I>,
    &RemoveLocalGroupRequest<I>::handle_close_snapshot_images>(this);
  auto cct = reinterpret_cast<CephContext*>(m_io_ctx.cct());
  auto gather_ctx = new C_Gather(cct, ctx);
  for (auto image_ctx : m_snapshot_image_ctxs) {
    if (image_ctx != nullptr) {
      image_ctx->state->close(gather_ctx->new_sub());
    }
  }
  gather_ctx->activate();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_close_snapshot_images(int r) {
  dout(10) << "r=" << r << dendl;

  m_snapshot_image_ctxs.clear();
  if (r < 0 && m_ret_val == 0) {
    derr << "failed to close group snapshot images: " << cpp_strerror(r)
         << dendl;
    m_ret_val = r;
  }

  if (m_ret_val < 0) {
    finish(m_ret_val);
    return;
  }

  list_group_images();
}

template <typename I>
void RemoveLocalGroupRequest<I>::list_group_images() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  cls::rbd::GroupImageSpec start_after;

  m_out_bl.clear();

  if (!m_images.empty()) {
    start_after = m_images.rbegin()->spec;
  }

  librbd::cls_client::group_image_list_start(&op, start_after, MAX_RETURN);
  auto comp = create_rados_callback<
      RemoveLocalGroupRequest<I>,
      &RemoveLocalGroupRequest<I>::handle_list_group_images>(this);
  int r = m_io_ctx.aio_operate(
      librbd::util::group_header_name(m_group_id), comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_list_group_images(int r) {
  dout(10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::group_image_list_finish(&iter, &images);
  }

  // -ENOENT == group creation failed in the middle
  if (r < 0 && r != -ENOENT) {
    dout(10) << "error listing local group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_images.insert(m_images.end(), images.begin(), images.end());

  if (images.size() == MAX_RETURN) {
    list_group_images();
    return;
  }
  m_num_images = m_images.size();
  dout(10) << "number of images: " << m_images.size() << dendl;
  get_mirror_images();
}

template <typename I>
void RemoveLocalGroupRequest<I>::get_mirror_images() {
  dout(10) << dendl;

  if (m_images.empty()) {
    remove_unmirrored_image_from_group();
    return;
  }

  auto &spec = m_images.front().spec;

  dout(10) << "pool_id: " << spec.pool_id
           << ", image_id " << spec.image_id << dendl;

  // FIXME: Currently images must be in the same pool as the group
  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_start(&op, spec.image_id);

  m_out_bl.clear();
  auto comp = create_rados_callback<
      RemoveLocalGroupRequest<I>,
      &RemoveLocalGroupRequest<I>::handle_get_mirror_images>(this);

  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_get_mirror_images(int r) {
  dout(10) << "r=" << r << dendl;

  auto &spec = m_images.front().spec;
  cls::rbd::MirrorImage mirror_image;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_image_get_finish(&iter, &mirror_image);
  }

  if (r == -ENOENT) {
    // ImageDeleter can remove mirror metadata and move the image to trash
    // before group teardown reaches this member. It still needs to be
    // detached from the group so its backlink no longer prevents deletion.
    m_unmirrored_images.emplace_back(spec.pool_id, spec.image_id);
  } else if (r < 0) {
    derr << "error getting local mirror image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else {
    m_trash_images.insert({mirror_image.global_image_id,
                           {spec.pool_id, spec.image_id}});
  }

  m_images.pop_front();
  get_mirror_images();
}

template <typename I>
void RemoveLocalGroupRequest<I>::remove_unmirrored_image_from_group() {
  if (m_unmirrored_images.empty()) {
    remove_image_from_group();
    return;
  }

  auto &[pool_id, image_id] = m_unmirrored_images.front();
  dout(10) << "image_id=" << image_id << " ,pool_id=" << pool_id
           << " ,group_id=" << m_group_id << dendl;

  auto ctx = create_context_callback<RemoveLocalGroupRequest,
    &RemoveLocalGroupRequest<I>::handle_remove_unmirrored_image_from_group>(
    this);

  auto req = librbd::group::RemoveImageRequest<I>::create(m_io_ctx, m_group_id,
    m_io_ctx, image_id, ctx);
  req->send();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_remove_unmirrored_image_from_group(
  int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    finish(r);
    return;
  }

  m_unmirrored_images.pop_front();
  remove_unmirrored_image_from_group();
}


template <typename I>
void RemoveLocalGroupRequest<I>::remove_image_from_group() {

  if (m_trash_images.empty()) {
    remove_local_group();
    return;
  }

  auto &[pool_id, image_id] = m_trash_images.begin()->second;
  dout(10) << "global_image_id=" << m_trash_images.begin()->first 
           << " ,image_id=" << image_id
           << " ,pool_id=" << pool_id
           << " ,group_id=" << m_group_id << dendl;

  auto ctx = create_context_callback<
    RemoveLocalGroupRequest,
    &RemoveLocalGroupRequest<I>::handle_remove_image_from_group>(this);

  auto req = librbd::group::RemoveImageRequest<I>::create(
      m_io_ctx, m_group_id, m_io_ctx, image_id, ctx);
  req->send();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_remove_image_from_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    finish(r);
    return;
  }

  move_image_to_trash();
}

template <typename I>
void RemoveLocalGroupRequest<I>::move_image_to_trash() {
  ceph_assert(!m_trash_images.empty());

  auto &global_image_id = m_trash_images.begin()->first;
  dout(10) << "global image id=" << global_image_id << dendl;

  auto ctx = create_context_callback<
    RemoveLocalGroupRequest,
    &RemoveLocalGroupRequest<I>::handle_move_image_to_trash>(this);

  auto req = image_deleter::TrashMoveRequest<I>::create(
      m_io_ctx, global_image_id, m_resync,
      m_work_queue, ctx);
  req->send();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_move_image_to_trash(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    // Put the image back so a later cleanup can find and retry it after an
    // rbd-mirror restart.
    m_ret_val = r;
    restore_image_to_group();
    return;
  }

  m_trash_images.erase(m_trash_images.begin());
  remove_image_from_group();
}

template <typename I>
void RemoveLocalGroupRequest<I>::restore_image_to_group() {
  ceph_assert(!m_trash_images.empty());
  ceph_assert(m_ret_val < 0);

  auto &[pool_id, image_id] = m_trash_images.begin()->second;
  dout(10) << "image_id=" << image_id << ", pool_id=" << pool_id
           << ", group_id=" << m_group_id << dendl;

  auto ctx = create_context_callback<
    RemoveLocalGroupRequest,
    &RemoveLocalGroupRequest<I>::handle_restore_image_to_group>(this);

  auto req = librbd::group::AddImageRequest<I>::create(
    m_io_ctx, m_group_id, m_io_ctx, image_id, ctx);
  req->send();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_restore_image_to_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -EEXIST && r != -ENOENT) {
    derr << "failed to restore image to group: " << cpp_strerror(r)
         << dendl;
  }

  finish(m_ret_val);
}

template <typename I>
void RemoveLocalGroupRequest<I>::remove_local_group() {
  dout(10) << "group_name=" << m_group_name
           << " ,group_id=" << m_group_id << dendl;

  librados::ObjectWriteOperation op;
  op.remove();
  auto comp = create_rados_callback<
      RemoveLocalGroupRequest<I>,
      &RemoveLocalGroupRequest<I>::handle_remove_local_group>(this);

  int r = m_io_ctx.aio_operate(
      librbd::util::group_header_name(m_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_remove_local_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "error removing local group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_local_group_id();
}

template <typename I>
void RemoveLocalGroupRequest<I>::remove_local_group_id() {
  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::group_dir_remove(&op, m_group_name, m_group_id);

  auto comp = create_rados_callback<
      RemoveLocalGroupRequest<I>,
      &RemoveLocalGroupRequest<I>::handle_remove_local_group_id>(this);

  int r = m_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_remove_local_group_id(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "error removing local group id: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_mirror_group();
}

template <typename I>
void RemoveLocalGroupRequest<I>::remove_mirror_group() {
  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_remove(&op, m_group_id);

  auto aio_comp = create_rados_callback<
    RemoveLocalGroupRequest<I>,
    &RemoveLocalGroupRequest<I>::handle_remove_mirror_group>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_remove_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to remove mirror group " << m_global_group_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

//FIXME: Should the mirroring watcher be notified?
  notify_mirroring_watcher();
}

template <typename I>
void RemoveLocalGroupRequest<I>::notify_mirroring_watcher() {
  if (!m_notify_watcher) {
    finish(0);
    return;
  }

  dout(10) << dendl;

  auto ctx = create_context_callback<
    RemoveLocalGroupRequest<I>,
    &RemoveLocalGroupRequest<I>::handle_notify_mirroring_watcher>(this);

  librbd::MirroringWatcher<I>::notify_group_updated(
    m_io_ctx, cls::rbd::MIRROR_GROUP_STATE_DISABLED, m_group_id,
    m_global_group_id, m_num_images, ctx);
}

template <typename I>
void RemoveLocalGroupRequest<I>::handle_notify_mirroring_watcher(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to notify mirror group update: " << cpp_strerror(r)
         << dendl;
  }

  finish(0);
}


template <typename I>
void RemoveLocalGroupRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::RemoveLocalGroupRequest<librbd::ImageCtx>;

