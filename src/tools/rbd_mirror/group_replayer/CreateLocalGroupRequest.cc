// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "CreateLocalGroupRequest.h"
#include "GroupStateBuilder.h"
#include "include/rados/librados.hpp"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"

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
  ceph_assert(m_state_builder->local_group_id.empty());
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
  dout(10) << "group_id=" << m_state_builder->local_group_id
           << ", group_name=" << m_state_builder->group_name << dendl;

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
      librbd::util::group_header_name(m_state_builder->local_group_id),
      comp, &op);
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

  // The group mirror state will be set to enabled once the first
  // non-primary mirror group snap is created.
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

