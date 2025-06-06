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
    remove_image_from_group();
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

  if (r < 0) {
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

