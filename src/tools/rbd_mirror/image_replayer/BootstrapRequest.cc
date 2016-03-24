// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BootstrapRequest.h"
#include "CloseImageRequest.h"
#include "OpenLocalImageRequest.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/journal/Types.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/ImageSync.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::BootstrapRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_ack_callback;

namespace {

template <typename I>
struct C_CreateImage : public Context {
  librados::IoCtx &local_io_ctx;
  std::string global_image_id;
  std::string local_image_name;
  I *remote_image_ctx;
  Context *on_finish;

  C_CreateImage(librados::IoCtx &local_io_ctx,
                const std::string &global_image_id,
                const std::string &local_image_name, I *remote_image_ctx,
                Context *on_finish)
    : local_io_ctx(local_io_ctx), global_image_id(global_image_id),
      local_image_name(local_image_name), remote_image_ctx(remote_image_ctx),
      on_finish(on_finish) {
  }

  virtual void finish(int r) override {
    assert(r == 0);

    // TODO: rbd-mirror should offer a feature mask capability
    RWLock::RLocker snap_locker(remote_image_ctx->snap_lock);
    int order = remote_image_ctx->order;

    CephContext *cct = reinterpret_cast<CephContext*>(local_io_ctx.cct());
    uint64_t journal_order = cct->_conf->rbd_journal_order;
    uint64_t journal_splay_width = cct->_conf->rbd_journal_splay_width;
    std::string journal_pool = cct->_conf->rbd_journal_pool;

    r = librbd::create_v2(local_io_ctx, local_image_name.c_str(),
                          reinterpret_cast<uint64_t>(this),
                          remote_image_ctx->size, order,
                          remote_image_ctx->features,
                          remote_image_ctx->stripe_unit,
                          remote_image_ctx->stripe_count,
                          journal_order, journal_splay_width, journal_pool,
                          global_image_id);
    on_finish->complete(r);
  }
};

} // anonymous namespace

template <typename I>
BootstrapRequest<I>::BootstrapRequest(librados::IoCtx &local_io_ctx,
                                      librados::IoCtx &remote_io_ctx,
                                      I **local_image_ctx,
                                      const std::string &local_image_name,
                                      const std::string &remote_image_id,
                                      const std::string &global_image_id,
                                      ContextWQ *work_queue, SafeTimer *timer,
                                      Mutex *timer_lock,
                                      const std::string &mirror_uuid,
                                      Journaler *journaler,
                                      MirrorPeerClientMeta *client_meta,
                                      Context *on_finish)
  : m_local_io_ctx(local_io_ctx), m_remote_io_ctx(remote_io_ctx),
    m_local_image_ctx(local_image_ctx), m_local_image_name(local_image_name),
    m_remote_image_id(remote_image_id), m_global_image_id(global_image_id),
    m_work_queue(work_queue), m_timer(timer), m_timer_lock(timer_lock),
    m_mirror_uuid(mirror_uuid), m_journaler(journaler),
    m_client_meta(client_meta), m_on_finish(on_finish) {
}

template <typename I>
BootstrapRequest<I>::~BootstrapRequest() {
  assert(m_remote_image_ctx == nullptr);
}

template <typename I>
void BootstrapRequest<I>::send() {
  get_local_image_id();
}

template <typename I>
void BootstrapRequest<I>::get_local_image_id() {
  dout(20) << dendl;

  // attempt to cross-reference a local image by the global image id
  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_image_id_start(&op, m_global_image_id);

  librados::AioCompletion *aio_comp = create_rados_ack_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_local_image_id>(
      this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_local_image_id(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == 0) {
    bufferlist::iterator iter = m_out_bl.begin();
    r = librbd::cls_client::mirror_image_get_image_id_finish(
      &iter, &m_local_image_id);
  }

  if (r == -ENOENT) {
    dout(10) << ": image not registered locally" << dendl;
  } else if (r < 0) {
    derr << ": failed to retreive local image id: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_client();
}

template <typename I>
void BootstrapRequest<I>::get_client() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_client>(
      this);
  m_journaler->get_client(m_mirror_uuid, &m_client, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_get_client(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << ": client not registered" << dendl;
  } else if (r < 0) {
    derr << ": failed to retreive client: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (decode_client_meta()) {
    // skip registration if it already exists
    open_remote_image();
    return;
  }

  register_client();
}

template <typename I>
void BootstrapRequest<I>::register_client() {
  dout(20) << dendl;

  // record an place-holder record
  librbd::journal::ClientData client_data{
    librbd::journal::MirrorPeerClientMeta{m_local_image_id}};
  bufferlist client_data_bl;
  ::encode(client_data, client_data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_register_client>(
      this);
  m_journaler->register_client(client_data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_register_client(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to register with remote journal: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  *m_client_meta = librbd::journal::MirrorPeerClientMeta(m_local_image_id);
  open_remote_image();
}

template <typename I>
void BootstrapRequest<I>::open_remote_image() {
  dout(20) << dendl;

  m_remote_image_ctx = I::create("", m_remote_image_id, nullptr,
                                 m_remote_io_ctx, false);
  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_remote_image>(
      this);
  m_remote_image_ctx->state->open(ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_open_remote_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to open remote image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  // default local image name to the remote image name if not provided
  if (m_local_image_name.empty()) {
    m_local_image_name = m_remote_image_ctx->name;
  }

  if (m_local_image_id.empty()) {
    create_local_image();
    return;
  }

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::open_local_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_local_image>(
      this);
  OpenLocalImageRequest<I> *request = OpenLocalImageRequest<I>::create(
    m_local_io_ctx, m_local_image_ctx,
    (!m_local_image_id.empty() ? std::string() : m_local_image_name),
    m_local_image_id, m_work_queue, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == -ENOENT) {
    assert(*m_local_image_ctx == nullptr);
    dout(10) << ": local image missing" << dendl;
    create_local_image();
    return;
  } else if (r == -EREMOTEIO) {
    assert(*m_local_image_ctx == nullptr);
    dout(10) << "local image is primary -- skipping image replay" << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  } else if (r < 0) {
    assert(*m_local_image_ctx == nullptr);
    derr << ": failed to open local image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  update_client();
}

template <typename I>
void BootstrapRequest<I>::remove_local_image() {
  dout(20) << dendl;

  // TODO
}

template <typename I>
void BootstrapRequest<I>::handle_remove_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  // TODO
}

template <typename I>
void BootstrapRequest<I>::create_local_image() {
  dout(20) << dendl;

  // TODO: librbd should provide an AIO image creation method -- this is
  //       blocking so we execute in our worker thread
  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_create_local_image>(
      this);
  m_work_queue->queue(new C_CreateImage<I>(m_local_io_ctx, m_global_image_id,
                                           m_local_image_name,
                                           m_remote_image_ctx, ctx), 0);
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to create local image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::update_client() {
  if (m_local_image_id == (*m_local_image_ctx)->id) {
    image_sync();
    return;
  }
  m_local_image_id = (*m_local_image_ctx)->id;

  dout(20) << dendl;

  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.image_id = m_local_image_id;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  ::encode(client_data, data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_update_client>(
      this);
  m_journaler->update_client(data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_update_client(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update client: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_local_image();
    return;
  }

  m_client_meta->image_id = m_local_image_id;
  image_sync();
}

template <typename I>
void BootstrapRequest<I>::image_sync() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_image_sync>(
      this);
  ImageSync<I> *request = ImageSync<I>::create(*m_local_image_ctx,
                                               m_remote_image_ctx, m_timer,
                                               m_timer_lock, m_mirror_uuid,
                                               m_journaler, m_client_meta, ctx);
  request->start();
}

template <typename I>
void BootstrapRequest<I>::handle_image_sync(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to sync remote image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_local_image();
    return;
  }

  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::close_local_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_close_local_image>(
      this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    m_local_image_ctx, m_work_queue, false, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_close_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": error encountered closing local image: " << cpp_strerror(r)
         << dendl;
  }

  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::close_remote_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_close_remote_image>(
      this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    &m_remote_image_ctx, m_work_queue, false, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_close_remote_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": error encountered closing remote image: " << cpp_strerror(r)
         << dendl;
  }

  finish(m_ret_val);
}

template <typename I>
void BootstrapRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

template <typename I>
bool BootstrapRequest<I>::decode_client_meta() {
  dout(20) << dendl;

  librbd::journal::ClientData client_data;
  bufferlist::iterator it = m_client.data.begin();
  try {
    ::decode(client_data, it);
  } catch (const buffer::error &err) {
    derr << ": failed to decode client meta data: " << err.what() << dendl;
    return true;
  }

  librbd::journal::MirrorPeerClientMeta *client_meta =
    boost::get<librbd::journal::MirrorPeerClientMeta>(&client_data.client_meta);
  if (client_meta == nullptr) {
    derr << ": unknown peer registration" << dendl;
    return true;
  } else if (!client_meta->image_id.empty()) {
    // have an image id -- use that to open the image
    m_local_image_id = client_meta->image_id;
  }

  *m_client_meta = *client_meta;

  dout(20) << ": client found: image_id=" << m_local_image_id << dendl;
  return true;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::BootstrapRequest<librbd::ImageCtx>;
