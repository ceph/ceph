// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "RemoveLocalGroupRequest.h"
#include "include/rados/librados.hpp"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"
#include "librbd/group/RemoveImageRequest.h"
#include "librbd/mirror/GroupGetInfoRequest.h"
#include "tools/rbd_mirror/group_replayer/GroupStateBuilder.h"
#include "tools/rbd_mirror/image_deleter/TrashMoveRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::" \
                           << "RemoveLocalGroupRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace group_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void RemoveLocalGroupRequest<I>::send() {
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

  if (r == -ENOENT) {
    dout(5) << "image " << m_global_group_id << " is not mirrored" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << "error retrieving mirror info for group "
         << m_global_group_id << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_promotion_state == librbd::mirror::PROMOTION_STATE_PRIMARY) {
    dout(10) << "group " << m_global_group_id << " is local primary" << dendl;
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
  m_mirror_group.global_group_id = m_global_group_id;
  m_mirror_group.mirror_image_mode = cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT;
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

  // Doing this so we have the pool id for when we support images in different
  // pools in the future.
  m_trash_images = m_state_builder->local_images;
  remove_image_from_group();
}

template <typename I>
void RemoveLocalGroupRequest<I>::remove_image_from_group() {

  if (m_trash_images.empty()) {
    remove_local_group();
    return;
  }

  auto &[pool_id, image_id] = m_trash_images.begin()->second;
  dout(10) << "image_id=" << image_id
           << " ,group_id=" << m_group_id << dendl;

/*
  int r = librbd::util::create_ioctx(m_local_io_ctx, "local image pool",
                                     pool_id, {}, &m_image_io_ctx);
  if (r < 0) {
    derr << "failed to open local image pool " << pool_id << ": "
         << cpp_strerror(r) << dendl;
    handle_remove_local_image_from_group(-ENOENT);
    return;
  }
*/

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
  }
  auto &global_image_id = m_trash_images.begin()->first;

  auto ctx = create_context_callback<
      RemoveLocalGroupRequest<I>,
      &RemoveLocalGroupRequest<I>::handle_move_images_to_trash>(this);

  auto req = image_deleter::TrashMoveRequest<I>::create(
      m_io_ctx, global_image_id, m_resync,
      m_work_queue, ctx);
  req->send();
}

/*
template <typename I>
void RemoveLocalGroupRequest<I>::move_images_to_trash() {
  dout(10) << dendl;

  auto &global_image_id = m_trash_images.begin()->first;

  auto ctx = create_context_callback<
    RemoveLocalGroupRequest,
    &RemoveLocalGroupRequest<I>::handle_move_images_to_trash>(this);

  auto req = image_deleter::TrashMoveRequest<I>::create(
      m_io_ctx, global_image_id, m_resync,
      m_work_queue, ctx);
  req->send();
}
*/
template <typename I>
void RemoveLocalGroupRequest<I>::handle_move_images_to_trash(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    finish(r);
  }

  m_trash_images.erase(m_trash_images.begin());
  remove_image_from_group();
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
  auto count = 0;
  if (!m_state_builder->local_images.empty()) {
      count = m_state_builder->local_images.size();
  }

  librbd::MirroringWatcher<I>::notify_group_updated(
    m_io_ctx, cls::rbd::MIRROR_GROUP_STATE_DISABLED, m_group_id,
    m_global_group_id, count, ctx);
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

