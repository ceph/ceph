// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/GetGroupInfoRequest.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::GetGroupInfoRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_rados_callback;

template <typename I>
void GetGroupInfoRequest<I>::send() {
  get_id();
}

template <typename I>
void GetGroupInfoRequest<I>::get_id() {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::dir_get_id_start(&op, m_group_name);

  auto comp = create_rados_callback<
      GetGroupInfoRequest<I>, &GetGroupInfoRequest<I>::handle_get_id>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GetGroupInfoRequest<I>::handle_get_id(int r) {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "failed to get ID of group '" << m_group_name
               << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  auto it = m_outbl.cbegin();
  r = cls_client::dir_get_id_finish(&it, &m_group_id);
  if (r < 0) {
    lderr(cct) << "failed to get ID of group '" << m_group_name
               << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_info();
}

template <typename I>
void GetGroupInfoRequest<I>::get_info() {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_group_get_start(&op, m_group_id);

  auto comp = create_rados_callback<
      GetGroupInfoRequest<I>, &GetGroupInfoRequest<I>::handle_get_info>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GetGroupInfoRequest<I>::handle_get_info(int r) {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "failed to get mirror info of group '" << m_group_name
               << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  auto it = m_outbl.cbegin();
  cls::rbd::MirrorGroup mirror_group;
  r = cls_client::mirror_group_get_finish(&it, &mirror_group);
  if (r < 0) {
    lderr(cct) << "failed to get mirror info of group '" << m_group_name
               << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_mirror_group_info->global_id = mirror_group.global_group_id;
  m_mirror_group_info->mirror_image_mode =
      static_cast<rbd_mirror_image_mode_t>(mirror_group.mirror_image_mode);
  m_mirror_group_info->state =
      static_cast<rbd_mirror_group_state_t>(mirror_group.state);

  finish(0);
}

template <typename I>
void GetGroupInfoRequest<I>::finish(int r) {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::GetGroupInfoRequest<librbd::ImageCtx>;
