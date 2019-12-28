// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CreateLocalImageRequest.h"
#include "include/rados/librados.hpp"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/ProgressContext.h"
#include "tools/rbd_mirror/image_replayer/CreateImageRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::journal::" \
                           << "CreateLocalImageRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

using librbd::util::create_context_callback;

template <typename I>
void CreateLocalImageRequest<I>::send() {
  *m_local_image_id = "";
  unregister_client();
}

template <typename I>
void CreateLocalImageRequest<I>::unregister_client() {
  dout(10) << dendl;
  update_progress("UNREGISTER_CLIENT");

  auto ctx = create_context_callback<
    CreateLocalImageRequest<I>,
    &CreateLocalImageRequest<I>::handle_unregister_client>(this);
  m_remote_journaler->unregister_client(ctx);
}

template <typename I>
void CreateLocalImageRequest<I>::handle_unregister_client(int r) {
  dout(10) << "r=" << r << dendl;
  if (r < 0 && r != -ENOENT) {
    derr << "failed to unregister with remote journal: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  *m_client_meta = librbd::journal::MirrorPeerClientMeta{""};
  register_client();
}

template <typename I>
void CreateLocalImageRequest<I>::register_client() {
  ceph_assert(m_local_image_id->empty());
  *m_local_image_id = librbd::util::generate_image_id<I>(m_local_io_ctx);
  dout(10) << "local_image_id=" << *m_local_image_id << dendl;
  update_progress("REGISTER_CLIENT");

  librbd::journal::MirrorPeerClientMeta client_meta{*m_local_image_id};
  client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;

  librbd::journal::ClientData client_data{client_meta};
  bufferlist client_data_bl;
  encode(client_data, client_data_bl);

  auto ctx = create_context_callback<
    CreateLocalImageRequest<I>,
    &CreateLocalImageRequest<I>::handle_register_client>(this);
  m_remote_journaler->register_client(client_data_bl, ctx);
}

template <typename I>
void CreateLocalImageRequest<I>::handle_register_client(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to register with remote journal: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  *m_client_meta = librbd::journal::MirrorPeerClientMeta{*m_local_image_id};
  m_client_meta->state = librbd::journal::MIRROR_PEER_STATE_SYNCING;

  create_local_image();
}

template <typename I>
void CreateLocalImageRequest<I>::create_local_image() {
  dout(10) << "local_image_id=" << *m_local_image_id << dendl;
  update_progress("CREATE_LOCAL_IMAGE");

  m_remote_image_ctx->image_lock.lock_shared();
  std::string image_name = m_remote_image_ctx->name;
  m_remote_image_ctx->image_lock.unlock_shared();

  auto ctx = create_context_callback<
    CreateLocalImageRequest<I>,
    &CreateLocalImageRequest<I>::handle_create_local_image>(this);
  auto request = CreateImageRequest<I>::create(
    m_threads, m_local_io_ctx, m_global_image_id, m_remote_mirror_uuid,
    image_name, *m_local_image_id, m_remote_image_ctx, ctx);
  request->send();
}
template <typename I>
void CreateLocalImageRequest<I>::handle_create_local_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -EBADF) {
    dout(5) << "image id " << *m_local_image_id << " already in-use" << dendl;
    *m_local_image_id = "";
    update_client_image();
    return;
  } else if (r < 0) {
    if (r == -ENOENT) {
      dout(10) << "parent image does not exist" << dendl;
    } else {
      derr << "failed to create local image: " << cpp_strerror(r) << dendl;
    }
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void CreateLocalImageRequest<I>::update_client_image() {
  ceph_assert(m_local_image_id->empty());
  *m_local_image_id = librbd::util::generate_image_id<I>(m_local_io_ctx);

  dout(10) << "local_image_id=" << *m_local_image_id << dendl;
  update_progress("UPDATE_CLIENT_IMAGE");

  librbd::journal::MirrorPeerClientMeta client_meta{*m_local_image_id};
  client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  encode(client_data, data_bl);

  auto ctx = create_context_callback<
    CreateLocalImageRequest<I>,
    &CreateLocalImageRequest<I>::handle_update_client_image>(this);
  m_remote_journaler->update_client(data_bl, ctx);
}

template <typename I>
void CreateLocalImageRequest<I>::handle_update_client_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to update client: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  *m_client_meta = librbd::journal::MirrorPeerClientMeta{*m_local_image_id};
  m_client_meta->state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  create_local_image();
}

template <typename I>
void CreateLocalImageRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

template <typename I>
void CreateLocalImageRequest<I>::update_progress(
    const std::string& description) {
  dout(15) << description << dendl;
  if (m_progress_ctx != nullptr) {
    m_progress_ctx->update_progress(description);
  }
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::journal::CreateLocalImageRequest<librbd::ImageCtx>;
