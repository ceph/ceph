// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BootstrapRequest.h"
#include "CloseImageRequest.h"
#include "OpenLocalImageRequest.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
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

namespace {

template <typename I>
struct C_CreateImage : public Context {
  librados::IoCtx &local_io_ctx;
  std::string local_image_name;
  I *remote_image_ctx;
  Context *on_finish;

  C_CreateImage(librados::IoCtx &local_io_ctx,
                const std::string &local_image_name, I *remote_image_ctx,
                Context *on_finish)
    : local_io_ctx(local_io_ctx), local_image_name(local_image_name),
      remote_image_ctx(remote_image_ctx), on_finish(on_finish) {
  }

  virtual void finish(int r) override {
    assert(r == 0);

    // TODO: rbd-mirror should offer a feature mask capability
    RWLock::RLocker snap_locker(remote_image_ctx->snap_lock);
    int order = remote_image_ctx->order;
    r = librbd::create(local_io_ctx, local_image_name.c_str(),
                       remote_image_ctx->size, false,
                       remote_image_ctx->features, &order,
                       remote_image_ctx->stripe_unit,
                       remote_image_ctx->stripe_count);
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
                                      ContextWQ *work_queue, SafeTimer *timer,
                                      Mutex *timer_lock,
                                      const std::string &mirror_uuid,
                                      Journaler *journaler,
                                      MirrorPeerClientMeta *client_meta,
                                      Context *on_finish)
  : m_local_io_ctx(local_io_ctx), m_remote_io_ctx(remote_io_ctx),
    m_local_image_ctx(local_image_ctx), m_local_image_name(local_image_name),
    m_remote_image_id(remote_image_id), m_work_queue(work_queue),
    m_timer(timer), m_timer_lock(timer_lock), m_mirror_uuid(mirror_uuid),
    m_journaler(journaler), m_client_meta(client_meta), m_on_finish(on_finish) {
}

template <typename I>
BootstrapRequest<I>::~BootstrapRequest() {
  assert(m_remote_image_ctx == nullptr);
}

template <typename I>
void BootstrapRequest<I>::send() {
  open_remote_image();
}

template <typename I>
void BootstrapRequest<I>::open_remote_image() {
  dout(20) << dendl;

  // TODO: need factory method to support mocking
  m_remote_image_ctx = new I("", m_remote_image_id, nullptr, m_remote_io_ctx,
                             false);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_remote_image>(
      this);
  m_remote_image_ctx->state->open(ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_open_remote_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << "failed to open remote image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  create_local_image();
}

template <typename I>
void BootstrapRequest<I>::create_local_image() {
  dout(20) << dendl;

  // TODO: local image might already exist (e.g. interrupted sync)
  //       need to determine what type of bootstrap we are performing

  // TODO: librbd should provide an AIO image creation method -- this is
  //       blocking so we execute in our worker thread
  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_create_local_image>(
      this);
  m_work_queue->queue(new C_CreateImage<I>(m_local_io_ctx, m_local_image_name,
                                           m_remote_image_ctx, ctx), 0);
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << "failed to create local image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_remote_image();
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
    m_local_io_ctx, m_local_image_ctx, m_local_image_name, "", m_work_queue,
    ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    assert(*m_local_image_ctx == nullptr);
    derr << "failed to open local image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  register_client();
}

template <typename I>
void BootstrapRequest<I>::register_client() {
  dout(20) << dendl;

  // TODO: if client fails to register newly created image to journal,
  //       need to ensure we can recover (i.e. see if image of the same
  //       name already exists)

  librbd::journal::MirrorPeerClientMeta client_meta(*m_client_meta);
  client_meta.image_id = (*m_local_image_ctx)->id;

  librbd::journal::ClientData client_data(client_meta);
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
    derr << "failed to register with remote journal: " << cpp_strerror(r)
         << dendl;
    close_local_image();
    return;
  }

  m_client_meta->image_id = (*m_local_image_ctx)->id;
  image_sync();
}

template <typename I>
void BootstrapRequest<I>::image_sync() {
  dout(20) << dendl;

  // TODO: need factory method to support mocking
  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_image_sync>(
      this);
  ImageSync<I> *request = new ImageSync<I>(*m_local_image_ctx,
                                           m_remote_image_ctx, m_timer,
                                           m_timer_lock, m_mirror_uuid,
                                           m_journaler, m_client_meta, ctx);
  request->start();
}

template <typename I>
void BootstrapRequest<I>::handle_image_sync(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << "failed to sync remote image: " << cpp_strerror(r) << dendl;
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
    m_local_image_ctx, m_work_queue, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_close_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << "error encountered closing local image: " << cpp_strerror(r)
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
    &m_remote_image_ctx, m_work_queue, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_close_remote_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << "error encountered closing remote image: " << cpp_strerror(r)
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

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::BootstrapRequest<librbd::ImageCtx>;
