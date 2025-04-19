// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "GroupMirrorStateUpdateRequest.h"
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
                           << "GroupMirrorStateUpdateRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace group_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void GroupMirrorStateUpdateRequest<I>::send() {
  dout(10) << "m_local_group_id=" << m_local_group_id << dendl;
  get_mirror_group();
}

template <typename I>
void GroupMirrorStateUpdateRequest<I>::get_mirror_group() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_group_get_start(&op, m_local_group_id);

  auto aio_comp = create_rados_callback<
    GroupMirrorStateUpdateRequest<I>,
    &GroupMirrorStateUpdateRequest<I>::handle_get_mirror_group>(this);

  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupMirrorStateUpdateRequest<I>::handle_get_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_group_get_finish(&it, &m_mirror_group);
  }

  if (r == -ENOENT) {
    dout(20) << "mirroring is disabled" << dendl;
    finish(r);
    return;
  }

  if (r < 0) {
    derr << "failed to get mirror info of group '" << m_local_group_id << dendl;

    finish(r);
    return;
  }

  if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
    finish(0);
    return;
  }
  enable_mirror_group();
}

template <typename I>
void GroupMirrorStateUpdateRequest<I>::enable_mirror_group() {
  dout(10) << dendl;

  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_ENABLED;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_set(&op, m_local_group_id,
                                       m_mirror_group);

  auto aio_comp = create_rados_callback<
    GroupMirrorStateUpdateRequest<I>,
    &GroupMirrorStateUpdateRequest<I>::handle_enable_mirror_group>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupMirrorStateUpdateRequest<I>::handle_enable_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to mirror enable group " << m_local_group_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  notify_mirroring_watcher();
}

template <typename I>
void GroupMirrorStateUpdateRequest<I>::notify_mirroring_watcher() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    GroupMirrorStateUpdateRequest<I>,
    &GroupMirrorStateUpdateRequest<I>::handle_notify_mirroring_watcher>(this);

  librbd::MirroringWatcher<I>::notify_group_updated(
    m_local_io_ctx, m_mirror_group.state, m_local_group_id,
    m_mirror_group.global_group_id, m_num_images, ctx);
}

template <typename I>
void GroupMirrorStateUpdateRequest<I>::handle_notify_mirroring_watcher(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to notify mirror group update: " << cpp_strerror(r)
         << dendl;
  }

  finish(0);
}

template <typename I>
void GroupMirrorStateUpdateRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::GroupMirrorStateUpdateRequest<librbd::ImageCtx>;

