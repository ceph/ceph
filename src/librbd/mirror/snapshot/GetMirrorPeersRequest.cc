// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/GetMirrorPeersRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/Types.h"
#include "librbd/mirror/snapshot/Utils.h"

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::GetMirrorPeersRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_rados_callback;

template <typename I>
void GetMirrorPeersRequest<I>::send() {
  m_cct = m_image_ctx->cct;
  m_default_ns_ctx.dup(m_image_ctx->md_ctx);
  m_default_ns_ctx.set_namespace("");
}


template <typename I>
void GetMirrorPeersRequest<I>::get_mirror_peers() {
  ldout(m_cct, 15) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  librados::AioCompletion *comp = create_rados_callback<
    GetMirrorPeersRequest<I>,
    &GetMirrorPeersRequest<I>::handle_get_mirror_peers>(this);
  m_out_bl.clear();
  int r = m_default_ns_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GetMirrorPeersRequest<I>::handle_get_mirror_peers(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_peer_list_finish(&iter, &peers);
  }

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirror peers: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  for (auto &peer : peers) {
    if (peer.mirror_peer_direction == cls::rbd::MIRROR_PEER_DIRECTION_RX) {
      continue;
    }
    m_mirror_peer_uuids.insert(peer.uuid);
  }

  finish(0);
}

template <typename I>
void GetMirrorPeersRequest<I>::finish(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::GetMirrorPeersRequest<librbd::ImageCtx>;
