// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CreateLocalGroupRequest.h"
#include "include/rados/librados.hpp"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/group_replayer/GroupStateBuilder.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::" \
                           << "CreateLocalGroupRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace group_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void CreateLocalGroupRequest<I>::send() {
  dout(10) << "m_global_group_id=" << m_global_group_id << dendl;

  disable_mirror_group();
}

template <typename I>
void CreateLocalGroupRequest<I>::disable_mirror_group() {
  if (m_state_builder->local_group_id.empty()) {
    add_mirror_group();
    return;
  }

  dout(10) << dendl;

  // need to send 'disabling' since the cls methods will fail if we aren't
  // in that state
  m_mirror_group.global_group_id = m_global_group_id;
  m_mirror_group.mirror_image_mode = cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT;
  m_mirror_group.state= cls::rbd::MIRROR_GROUP_STATE_DISABLING;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_set(&op, m_state_builder->local_group_id,
                                       m_mirror_group);

  auto aio_comp = create_rados_callback<
    CreateLocalGroupRequest<I>,
    &CreateLocalGroupRequest<I>::handle_disable_mirror_group>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void CreateLocalGroupRequest<I>::handle_disable_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to disable mirror group " << m_global_group_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_mirror_group();
}

template <typename I>
void CreateLocalGroupRequest<I>::remove_mirror_group() {
  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_remove(&op, m_state_builder->local_group_id);

  auto aio_comp = create_rados_callback<
    CreateLocalGroupRequest<I>,
    &CreateLocalGroupRequest<I>::handle_remove_mirror_group>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void CreateLocalGroupRequest<I>::handle_remove_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to remove mirror group " << m_global_group_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_state_builder->local_group_id = "";
  add_mirror_group();
}

template <typename I>
void CreateLocalGroupRequest<I>::add_mirror_group() {
  ceph_assert(m_state_builder->local_group_id.empty());
  m_state_builder->local_group_id =
    librbd::util::generate_image_id<I>(m_local_io_ctx);

  dout(10) << "local_group_id=" << m_state_builder->local_group_id << dendl;

  // use 'creating' to track a partially constructed group. it will
  // be switched to 'enabled' once the group is fully created
  m_mirror_group.global_group_id = m_global_group_id;
  m_mirror_group.mirror_image_mode = cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT;
  m_mirror_group.state= cls::rbd::MIRROR_GROUP_STATE_CREATING;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_set(&op, m_state_builder->local_group_id,
                                       m_mirror_group);

  auto aio_comp = create_rados_callback<
    CreateLocalGroupRequest<I>,
    &CreateLocalGroupRequest<I>::handle_add_mirror_group>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void CreateLocalGroupRequest<I>::handle_add_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to register mirror group " << m_global_group_id << ": "
         << cpp_strerror(r) << dendl;
    this->finish(r);
    return;
  }

  create_local_group_id();
}

template <typename I>
void CreateLocalGroupRequest<I>::create_local_group_id() {
  dout(10) << "local_group_id=" << m_state_builder->local_group_id << dendl;

  std::string group_name = m_state_builder->group_name;
  std::string group_id = m_state_builder->local_group_id;

  librados::ObjectWriteOperation op;
  librbd::cls_client::group_dir_add(&op, group_name, group_id);

  auto comp = create_rados_callback<
      CreateLocalGroupRequest<I>,
      &CreateLocalGroupRequest<I>::handle_create_local_group_id>(this);

  int r = m_local_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void CreateLocalGroupRequest<I>::handle_create_local_group_id(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error creating local group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  create_local_group();
}

template <typename I>
void CreateLocalGroupRequest<I>::create_local_group() {
  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  op.create(true);
  auto comp = create_rados_callback<
      CreateLocalGroupRequest<I>,
      &CreateLocalGroupRequest<I>::handle_create_local_group>(this);

  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_state_builder->local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void CreateLocalGroupRequest<I>::handle_create_local_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to create local group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

//FIXME: Should this be set only after the first non-primary snap is created
//as is done in the ImageReplayer?
  enable_mirror_group();
}

template <typename I>
void CreateLocalGroupRequest<I>::enable_mirror_group() {
  dout(10) << dendl;

  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_ENABLED;
  m_mirror_group.global_group_id = m_global_group_id;
  m_mirror_group.mirror_image_mode = cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_set(&op, m_state_builder->local_group_id,
                                       m_mirror_group);

  auto aio_comp = create_rados_callback<
    CreateLocalGroupRequest<I>,
    &CreateLocalGroupRequest<I>::handle_enable_mirror_group>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void CreateLocalGroupRequest<I>::handle_enable_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;
  if (r < 0) {
    derr << "failed to enable mirror group " << m_global_group_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }
  notify_mirroring_watcher();
}

template <typename I>
void CreateLocalGroupRequest<I>::notify_mirroring_watcher() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    CreateLocalGroupRequest<I>,
    &CreateLocalGroupRequest<I>::handle_notify_mirroring_watcher>(this);

  librbd::MirroringWatcher<I>::notify_group_updated(
    m_local_io_ctx, m_mirror_group.state, m_state_builder->local_group_id,
    m_global_group_id, m_state_builder->remote_images.size(), ctx);
}

template <typename I>
void CreateLocalGroupRequest<I>::handle_notify_mirroring_watcher(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to notify mirror group update: " << cpp_strerror(r)
         << dendl;
  }

  finish(0);
}

template <typename I>
void CreateLocalGroupRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::CreateLocalGroupRequest<librbd::ImageCtx>;

