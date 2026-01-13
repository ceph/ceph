// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupAddImageRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "librbd/mirror/snapshot/GroupMirrorEnableUpdateRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupAddImageRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using util::create_context_callback;

template <typename I>
GroupAddImageRequest<I>::GroupAddImageRequest(librados::IoCtx &io_ctx,
                                              const std::string &group_id,
                                              const std::string &image_id,
                                              uint64_t group_snap_create_flags,
                                              cls::rbd::MirrorImageMode mode,
                                              const cls::rbd::MirrorGroup &mirror_group,
                                              Context *on_finish)
  : m_group_ioctx(io_ctx), m_group_id(group_id), m_image_id(image_id),
    m_group_snap_create_flags(group_snap_create_flags), m_mode(mode),
    m_mirror_group(mirror_group), m_on_finish(on_finish),
    m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())) {
}

template <typename I>
void GroupAddImageRequest<I>::send() {
  ceph_assert(m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED);
  prepare_group_images();
}

template <typename I>
void GroupAddImageRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupAddImageRequest<I>,
      &GroupAddImageRequest<I>::handle_prepare_group_images>(this);

  m_prepare_req = snapshot::GroupPrepareImagesRequest<I>::create(
    m_group_ioctx, m_group_id, m_image_ctxs, m_images,
    &m_mirror_images, &m_mirror_peer_uuids, m_image_id,
    librbd::mirror::snapshot::GroupPrepareImagesRequest<I>::OP_ADD_IMAGE,
    false, ctx);

  m_prepare_req->send();
}

template <typename I>
void GroupAddImageRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  group_mirror_add_image_update();
}

template <typename I>
void GroupAddImageRequest<I>::group_mirror_add_image_update() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupAddImageRequest<I>,
        &GroupAddImageRequest<I>::handle_group_mirror_add_image_update>(this);

  auto req = snapshot::GroupMirrorEnableUpdateRequest<I>::create(
      m_group_ioctx, m_group_id, m_mirror_group, m_image_ctxs, m_images,
      m_mirror_peer_uuids, m_mode, m_group_snap_create_flags, m_image_id,
      snapshot::GroupMirrorEnableUpdateRequest<I>::Type::ADD_IMAGE,
      m_prepare_req->get_image_to_global_id(), ctx);

  req->send();
}

template <typename I>
void GroupAddImageRequest<I>::handle_group_mirror_add_image_update(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_prepare_req = nullptr;
  finish(r);
}

template <typename I>
void GroupAddImageRequest<I>::close_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupAddImageRequest<I>,
      &GroupAddImageRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto* ictx : m_image_ctxs) {
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupAddImageRequest<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && m_ret_val == 0) {
    lderr(m_cct) << "failed to close images: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
  }

  finish(m_ret_val);
}

template <typename I>
void GroupAddImageRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupAddImageRequest<librbd::ImageCtx>;
