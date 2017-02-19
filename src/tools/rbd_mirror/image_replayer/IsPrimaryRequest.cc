// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "IsPrimaryRequest.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include <type_traits>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::IsPrimaryRequest: " \
                           << this << " " << __func__ << " "

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_ack_callback;

template <typename I>
IsPrimaryRequest<I>::IsPrimaryRequest(I *image_ctx, bool *primary,
                                      Context *on_finish)
  : m_image_ctx(image_ctx), m_primary(primary), m_on_finish(on_finish) {
}

template <typename I>
void IsPrimaryRequest<I>::send() {
  send_get_mirror_state();
}

template <typename I>
void IsPrimaryRequest<I>::send_get_mirror_state() {
  dout(20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_start(&op, m_image_ctx->id);

  librados::AioCompletion *aio_comp = create_rados_ack_callback<
    IsPrimaryRequest<I>, &IsPrimaryRequest<I>::handle_get_mirror_state>(this);
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op,
                                          &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void IsPrimaryRequest<I>::handle_get_mirror_state(int r) {
  dout(20) << ": r=" << r << dendl;

  cls::rbd::MirrorImage mirror_image;
  if (r == 0) {
    bufferlist::iterator iter = m_out_bl.begin();
    r = librbd::cls_client::mirror_image_get_finish(&iter, &mirror_image);
    if (r == 0) {
      if (mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
        send_is_tag_owner();
        return;
      } else if (mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_DISABLING) {
        dout(5) << ": image mirroring is being disabled" << dendl;
        *m_primary = false;
      } else {
        derr << ": image mirroring is disabled" << dendl;
        r = -EINVAL;
      }
    } else {
      derr << ": failed to decode image mirror state: " << cpp_strerror(r)
           << dendl;
    }
  } else {
    derr << ": failed to retrieve image mirror state: " << cpp_strerror(r)
         << dendl;
  }

  finish(r);
}

template <typename I>
void IsPrimaryRequest<I>::send_is_tag_owner() {
  // deduce the class type for the journal to support unit tests
  using Journal = typename std::decay<
    typename std::remove_pointer<decltype(std::declval<I>().journal)>
    ::type>::type;

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    IsPrimaryRequest<I>, &IsPrimaryRequest<I>::handle_is_tag_owner>(this);

  Journal::is_tag_owner(m_image_ctx, m_primary, ctx);
}

template <typename I>
void IsPrimaryRequest<I>::handle_is_tag_owner(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to query remote image tag owner: " << cpp_strerror(r)
         << dendl;
  }

  finish(r);
}

template <typename I>
void IsPrimaryRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::IsPrimaryRequest<librbd::ImageCtx>;
