// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_replayer/PrepareLocalImageRequest.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_client.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "tools/rbd_mirror/ImageDeleter.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"
#include "tools/rbd_mirror/image_replayer/snapshot/StateBuilder.h"
#include <type_traits>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::" \
                           << "PrepareLocalImageRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void PrepareLocalImageRequest<I>::send() {
  dout(10) << dendl;
  get_local_image_id();
}

template <typename I>
void PrepareLocalImageRequest<I>::get_local_image_id() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    PrepareLocalImageRequest<I>,
    &PrepareLocalImageRequest<I>::handle_get_local_image_id>(this);
  auto req = GetMirrorImageIdRequest<I>::create(m_io_ctx, m_global_image_id,
                                                &m_local_image_id, ctx);
  req->send();
}

template <typename I>
void PrepareLocalImageRequest<I>::handle_get_local_image_id(int r) {
  dout(10) << "r=" << r << ", "
           << "local_image_id=" << m_local_image_id << dendl;

  if (r < 0) {
    finish(r);
    return;
  }

  get_local_image_name();
}

template <typename I>
void PrepareLocalImageRequest<I>::get_local_image_name() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_name_start(&op, m_local_image_id);

  m_out_bl.clear();
  librados::AioCompletion *aio_comp = create_rados_callback<
    PrepareLocalImageRequest<I>,
    &PrepareLocalImageRequest<I>::handle_get_local_image_name>(this);
  int r = m_io_ctx.aio_operate(RBD_DIRECTORY, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void PrepareLocalImageRequest<I>::handle_get_local_image_name(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::dir_get_name_finish(&it, m_local_image_name);
  }

  if (r == -ENOENT) {
    // proceed we should have a mirror image record if we got this far
    dout(10) << "image does not exist for local image id " << m_local_image_id
             << dendl;
    *m_local_image_name = "";
  } else  if (r < 0) {
    derr << "failed to retrieve image name: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_mirror_info();
}

template <typename I>
void PrepareLocalImageRequest<I>::get_mirror_info() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    PrepareLocalImageRequest<I>,
    &PrepareLocalImageRequest<I>::handle_get_mirror_info>(this);
  auto req = librbd::mirror::GetInfoRequest<I>::create(
    m_io_ctx, m_work_queue, m_local_image_id, &m_mirror_image,
    &m_promotion_state, &m_primary_mirror_uuid, ctx);
  req->send();
}

template <typename I>
void PrepareLocalImageRequest<I>::handle_get_mirror_info(int r) {
  dout(10) << ": r=" << r << dendl;

  if (r < 0) {
    derr << "failed to retrieve local mirror image info: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  if (m_mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_CREATING) {
    dout(5) << "local image is still in creating state, issuing a removal"
            << dendl;
    move_to_trash();
    return;
  } else if (m_mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_DISABLING) {
    dout(5) << "local image mirroring is in disabling state" << dendl;
    finish(-ERESTART);
    return;
  }

  switch (m_mirror_image.mode) {
  case cls::rbd::MIRROR_IMAGE_MODE_JOURNAL:
    // journal-based local image exists
    {
      auto state_builder = journal::StateBuilder<I>::create(m_global_image_id);
      state_builder->local_primary_mirror_uuid = m_primary_mirror_uuid;
      *m_state_builder = state_builder;
    }
    break;
  case cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT:
    // snapshot-based local image exists
    *m_state_builder = snapshot::StateBuilder<I>::create(m_global_image_id);
    break;
  default:
    derr << "unsupported mirror image mode " << m_mirror_image.mode << " "
         << "for image " << m_global_image_id << dendl;
    finish(-EOPNOTSUPP);
    break;
  }

  dout(10) << "local_image_id=" << m_local_image_id << ", "
           << "local_promotion_state=" << m_promotion_state << ", "
           << "local_primary_mirror_uuid=" << m_primary_mirror_uuid << dendl;
  (*m_state_builder)->local_image_id = m_local_image_id;
  (*m_state_builder)->local_promotion_state = m_promotion_state;
  finish(0);
}

template <typename I>
void PrepareLocalImageRequest<I>::move_to_trash() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    PrepareLocalImageRequest<I>,
    &PrepareLocalImageRequest<I>::handle_move_to_trash>(this);
  ImageDeleter<I>::trash_move(m_io_ctx, m_global_image_id,
                              false, m_work_queue, ctx);
}

template <typename I>
void PrepareLocalImageRequest<I>::handle_move_to_trash(int r) {
  dout(10) << ": r=" << r << dendl;

  finish(-ENOENT);
}

template <typename I>
void PrepareLocalImageRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::PrepareLocalImageRequest<librbd::ImageCtx>;
