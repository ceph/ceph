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
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.h"
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
  dout(20) << dendl;
  get_local_image_id();
}

template <typename I>
void PrepareLocalImageRequest<I>::get_local_image_id() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    PrepareLocalImageRequest<I>,
    &PrepareLocalImageRequest<I>::handle_get_local_image_id>(this);
  auto req = GetMirrorImageIdRequest<I>::create(m_io_ctx, m_global_image_id,
                                                m_local_image_id, ctx);
  req->send();
}

template <typename I>
void PrepareLocalImageRequest<I>::handle_get_local_image_id(int r) {
  dout(20) << "r=" << r << ", "
           << "local_image_id=" << *m_local_image_id << dendl;

  if (r < 0) {
    finish(r);
    return;
  }

  get_mirror_state();
}

template <typename I>
void PrepareLocalImageRequest<I>::get_mirror_state() {
  dout(20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_start(&op, *m_local_image_id);

  m_out_bl.clear();
  librados::AioCompletion *aio_comp = create_rados_callback<
    PrepareLocalImageRequest<I>,
    &PrepareLocalImageRequest<I>::handle_get_mirror_state>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void PrepareLocalImageRequest<I>::handle_get_mirror_state(int r) {
  dout(20) << ": r=" << r << dendl;

  cls::rbd::MirrorImage mirror_image;
  if (r == 0) {
    bufferlist::iterator iter = m_out_bl.begin();
    r = librbd::cls_client::mirror_image_get_finish(&iter, &mirror_image);
  }

  if (r < 0) {
    derr << "failed to retrieve image mirror state: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  // TODO save current mirror state to determine if we should
  // delete a partially formed image
  // (e.g. MIRROR_IMAGE_STATE_CREATING/DELETING)

  get_tag_owner();
}

template <typename I>
void PrepareLocalImageRequest<I>::get_tag_owner() {
  // deduce the class type for the journal to support unit tests
  using Journal = typename std::decay<
    typename std::remove_pointer<decltype(std::declval<I>().journal)>
    ::type>::type;

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    PrepareLocalImageRequest<I>,
    &PrepareLocalImageRequest<I>::handle_get_tag_owner>(this);
  Journal::get_tag_owner(m_io_ctx, *m_local_image_id, m_tag_owner,
                         m_work_queue, ctx);
}

template <typename I>
void PrepareLocalImageRequest<I>::handle_get_tag_owner(int r) {
  dout(20) << "r=" << r << ", "
           << "tag_owner=" << *m_tag_owner << dendl;

  if (r < 0) {
    derr << "failed to retrieve journal tag owner: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void PrepareLocalImageRequest<I>::finish(int r) {
  dout(20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::PrepareLocalImageRequest<librbd::ImageCtx>;
