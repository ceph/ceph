// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_client.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/journal/Types.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::" \
                           << "PrepareRemoteImageRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void PrepareRemoteImageRequest<I>::send() {
  get_remote_mirror_uuid();
}

template <typename I>
void PrepareRemoteImageRequest<I>::get_remote_mirror_uuid() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_uuid_get_start(&op);

  librados::AioCompletion *aio_comp = create_rados_callback<
    PrepareRemoteImageRequest<I>,
    &PrepareRemoteImageRequest<I>::handle_get_remote_mirror_uuid>(this);
  int r = m_remote_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void PrepareRemoteImageRequest<I>::handle_get_remote_mirror_uuid(int r) {
  if (r >= 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_uuid_get_finish(&it, m_remote_mirror_uuid);
    if (r >= 0 && m_remote_mirror_uuid->empty()) {
      r = -ENOENT;
    }
  }

  dout(10) << "r=" << r << dendl;
  if (r < 0) {
    if (r == -ENOENT) {
      dout(5) << "remote mirror uuid missing" << dendl;
    } else {
      derr << "failed to retrieve remote mirror uuid: " << cpp_strerror(r)
           << dendl;
    }
    finish(r);
    return;
  }

  get_remote_image_id();
}

template <typename I>
void PrepareRemoteImageRequest<I>::get_remote_image_id() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    PrepareRemoteImageRequest<I>,
    &PrepareRemoteImageRequest<I>::handle_get_remote_image_id>(this);
  auto req = GetMirrorImageIdRequest<I>::create(m_remote_io_ctx,
                                                m_global_image_id,
                                                m_remote_image_id, ctx);
  req->send();
}

template <typename I>
void PrepareRemoteImageRequest<I>::handle_get_remote_image_id(int r) {
  dout(10) << "r=" << r << ", "
           << "remote_image_id=" << *m_remote_image_id << dendl;

  if (r < 0) {
    finish(r);
    return;
  }

  get_client();
}

template <typename I>
void PrepareRemoteImageRequest<I>::get_client() {
  dout(10) << dendl;

  ceph_assert(*m_remote_journaler == nullptr);
  *m_remote_journaler = new Journaler(m_threads->work_queue, m_threads->timer,
                                      &m_threads->timer_lock, m_remote_io_ctx,
                                      *m_remote_image_id, m_local_mirror_uuid,
                                      m_journal_settings,
                                      m_cache_manager_handler);

  Context *ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
      PrepareRemoteImageRequest<I>,
      &PrepareRemoteImageRequest<I>::handle_get_client>(this));
  (*m_remote_journaler)->get_client(m_local_mirror_uuid, &m_client, ctx);
}

template <typename I>
void PrepareRemoteImageRequest<I>::handle_get_client(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << "client not registered" << dendl;
    register_client();
  } else if (r < 0) {
    derr << "failed to retrieve client: " << cpp_strerror(r) << dendl;
    finish(r);
  } else if (!util::decode_client_meta(m_client, m_client_meta)) {
    // require operator intervention since the data is corrupt
    finish(-EBADMSG);
  } else {
    // skip registration if it already exists
    *m_client_state = m_client.state;
    finish(0);
  }
}

template <typename I>
void PrepareRemoteImageRequest<I>::register_client() {
  dout(10) << dendl;

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;

  librbd::journal::ClientData client_data{mirror_peer_client_meta};
  bufferlist client_data_bl;
  encode(client_data, client_data_bl);

  Context *ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
      PrepareRemoteImageRequest<I>,
      &PrepareRemoteImageRequest<I>::handle_register_client>(this));
  (*m_remote_journaler)->register_client(client_data_bl, ctx);
}

template <typename I>
void PrepareRemoteImageRequest<I>::handle_register_client(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to register with remote journal: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  *m_client_state = cls::journal::CLIENT_STATE_CONNECTED;
  *m_client_meta = librbd::journal::MirrorPeerClientMeta(m_local_image_id);
  m_client_meta->state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;

  finish(0);
}

template <typename I>
void PrepareRemoteImageRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    delete *m_remote_journaler;
    *m_remote_journaler = nullptr;
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::PrepareRemoteImageRequest<librbd::ImageCtx>;
