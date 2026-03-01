// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/mirror/GroupEnableRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "librbd/mirror/snapshot/GroupMirrorEnableUpdateRequest.h"

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
    } else if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
      ldout(m_cct, 10) << "mirroring on group already enabled" << dendl;
      finish(0);
      return;
    } else if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLING ||
               m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_DISABLING) {
      lderr(m_cct) << "mirroring on group in transitional state: "
                   << m_mirror_group.state << dendl;
      finish(-EINVAL);
      return;
    }
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

  // New group or fresh enable
  m_mirror_group = cls::rbd::MirrorGroup{};

  uuid_d uuid_gen;
  uuid_gen.generate_random();
  m_mirror_group.global_group_id = uuid_gen.to_string();
  m_mirror_group.mirror_image_mode = m_mode;
  m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLING;

  prepare_group_images();
}

template <typename I>
void GroupEnableRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_prepare_group_images>(this);

  m_prepare_req = snapshot::GroupPrepareImagesRequest<I>::create(m_group_ioctx,
    m_group_id, m_image_ctxs, m_images, nullptr, &m_mirror_peer_uuids, "", // no specific image
    snapshot::GroupPrepareImagesRequest<I>::OP_ENABLE, false, ctx);

  m_prepare_req->send();
}

template <typename I>
void GroupEnableRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  group_mirror_enable_update();
}

template <typename I>
void GroupEnableRequest<I>::group_mirror_enable_update() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_group_mirror_enable_update>(this);

  auto req = snapshot::GroupMirrorEnableUpdateRequest<I>::create(m_group_ioctx,
    m_group_id, m_mirror_group, m_image_ctxs, m_images, m_mirror_peer_uuids,
    m_mode, m_group_snap_create_flags, "",  // no image_id
    snapshot::GroupMirrorEnableUpdateRequest<I>::Type::ENABLE,
    m_prepare_req->get_image_to_global_id(), ctx);

  req->send();
}

template <typename I>
void GroupEnableRequest<I>::handle_group_mirror_enable_update(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_prepare_req = nullptr;
  finish(r);
}

template <typename I>
void GroupEnableRequest<I>::close_images() {
  if (m_image_ctxs.empty()) {
    finish(m_ret_val);
    return;
  }
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto* ictx : m_image_ctxs) {
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupEnableRequest<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && m_ret_val == 0) {
    lderr(m_cct) << "failed to close images: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
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
