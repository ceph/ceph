// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/mirror/GroupGetInfoRequest.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/group/ListSnapshotsRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupGetInfoRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_rados_callback;

template <typename I>
void GroupGetInfoRequest<I>::send() {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << dendl;

  if (m_group_name.empty() && m_group_id.empty()) {
    lderr(cct) << "both group name and group id cannot be empty" << dendl;
    finish(-EINVAL);
  }

  if (m_group_id.empty()) {
    get_id();
  } else {
    get_info();
  }
}

template <typename I>
void GroupGetInfoRequest<I>::get_id() {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::dir_get_id_start(&op, m_group_name);

  auto comp = create_rados_callback<
      GroupGetInfoRequest<I>, &GroupGetInfoRequest<I>::handle_get_id>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupGetInfoRequest<I>::handle_get_id(int r) {
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
void GroupGetInfoRequest<I>::get_info() {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_group_get_start(&op, m_group_id);

  auto comp = create_rados_callback<
      GroupGetInfoRequest<I>, &GroupGetInfoRequest<I>::handle_get_info>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupGetInfoRequest<I>::handle_get_info(int r) {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;
  
  m_mirror_group->state = cls::rbd::MIRROR_GROUP_STATE_DISABLED;
  *m_promotion_state = PROMOTION_STATE_UNKNOWN;

  if (r == 0) {
    auto it = m_outbl.cbegin();
    r = cls_client::mirror_group_get_finish(&it, m_mirror_group);
  }

  if (r == -ENOENT) {
    ldout(cct, 20) << "mirroring is disabled" << dendl;
    finish(r);
    return;
  }
  if (r < 0) {
    lderr(cct) << "failed to get mirror info of group '" << m_group_id
               << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_mirror_group->state == cls::rbd::MIRROR_GROUP_STATE_CREATING) {
    // No snapshots will have been created and it is likely that the
    // group has not been created either.
    finish(0);
    return;
  }

  get_last_mirror_snapshot_state();
}

template <typename I>
void GroupGetInfoRequest<I>::get_last_mirror_snapshot_state() {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    GroupGetInfoRequest<I>,
    &GroupGetInfoRequest<I>::handle_get_last_mirror_snapshot_state>(this);

  auto req = group::ListSnapshotsRequest<I>::create(
    m_group_ioctx, m_group_id, true, true, &m_group_snaps, ctx);

  req->send();
}

template <typename I>
void GroupGetInfoRequest<I>::handle_get_last_mirror_snapshot_state(int r) {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << dendl;

  // This could return -ENOENT if the group creation was interrupted
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to list group snapshots of group '" << m_group_id
               << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  for (auto it = m_group_snaps.rbegin(); it != m_group_snaps.rend(); it++) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &it->snapshot_namespace);
    if (ns != nullptr) {
      // XXXMG: check primary_mirror_uuid matches?
      switch (ns->state) {
      case cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY:
        *m_promotion_state = PROMOTION_STATE_PRIMARY;
        break;
      case cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY:
        *m_promotion_state = PROMOTION_STATE_NON_PRIMARY;
        break;
      case cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED:
      case cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED:
        *m_promotion_state = PROMOTION_STATE_ORPHAN;
        break;
      }
      break;
    }
  }

  finish(0);
}

template <typename I>
void GroupGetInfoRequest<I>::finish(int r) {
  auto cct = reinterpret_cast<CephContext *>(m_group_ioctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupGetInfoRequest<librbd::ImageCtx>;
