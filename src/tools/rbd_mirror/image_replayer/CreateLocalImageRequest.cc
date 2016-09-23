// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Utils.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/dout.h"
#include "include/Context.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/image/RemoveRequest.h"
#include "CreateImageRequest.h"
#include "CreateLocalImageRequest.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::CreateLocalImageRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_ack_callback;

template<typename I>
CreateLocalImageRequest<I>::CreateLocalImageRequest(IoCtx &local_io_ctx,
  std::string *local_image_id, const std::string &local_image_name,
  const std::string &global_image_id, const std::string &remote_mirror_uuid,
  I *remote_image_ctx, ContextWQ *op_work_queue, Context *on_finish)
  : m_local_image_id(local_image_id), m_local_image_name(local_image_name),
    m_global_image_id(global_image_id), m_remote_mirror_uuid(remote_mirror_uuid),
    m_remote_image_ctx(remote_image_ctx), m_op_work_queue(op_work_queue),
    m_on_finish(on_finish) {
  m_local_io_ctx.dup(local_io_ctx);
  m_cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
}

template<typename I>
void CreateLocalImageRequest<I>::send() {
  dout(20) << dendl;

  assert(m_local_image_id);

  m_curr_image_id = *m_local_image_id;
  if (m_curr_image_id.empty()) {
    create_local_image_checkpoint_begin();
  } else {
    get_local_image_state();
  }
}

template<typename I>
void CreateLocalImageRequest<I>::get_local_image_state() {
  dout(20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_start(&op, m_curr_image_id);

  using klass = CreateLocalImageRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_get_local_image_state>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context* CreateLocalImageRequest<I>::handle_get_local_image_state(int *result) {
  dout(20) << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    derr << ": failed to retrieve local image state: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  if (*result < 0) {
    create_local_image_checkpoint_begin();
    return nullptr;
  }

  bufferlist::iterator iter = m_out_bl.begin();
  *result = librbd::cls_client::mirror_image_get_finish(&iter, &m_mirror_image);
  if (*result < 0) {
    derr << ": failed to retrieve local image state: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  // we should not have reached here if the state was anything else
  // other then the transient "CREATING" state.
  assert(m_mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_CREATING);

  // remove possibly half baked image and create a fresh local image
  remove_local_image_continue();
  return nullptr;
}

template<typename I>
void CreateLocalImageRequest<I>::remove_local_image_continue() {
  dout(20) << dendl;

  using klass = CreateLocalImageRequest<I>;
  Context *ctx = create_context_callback
    <klass, &klass::handle_remove_local_image_continue>(this);

  librbd::image::RemoveRequest<I> *req = librbd::image::RemoveRequest<I>::create(
    m_local_io_ctx, m_local_image_name, m_curr_image_id, true, m_no_op_prog_ctx,
    m_op_work_queue, ctx);
  req->send();
}

template<typename I>
Context* CreateLocalImageRequest<I>::handle_remove_local_image_continue(int *result) {
  dout(20) << ": r=" << *result << dendl;

  if (*result < 0) {
    derr << ": failed to cleanup interrupted image creation (previous run): "
         << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  create_local_image_checkpoint_begin();
  return nullptr;
}

template<typename I>
void CreateLocalImageRequest<I>::create_local_image_checkpoint_begin() {
  dout(20) << dendl;

  // set global image id as we can reach here from an -ENOENT
  m_mirror_image.global_image_id = m_global_image_id;
  m_mirror_image.state = cls::rbd::MIRROR_IMAGE_STATE_CREATING;

  m_curr_image_id = librbd::util::generate_image_id(m_local_io_ctx);

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_set(&op, m_curr_image_id, m_mirror_image);

  using klass = CreateLocalImageRequest<I>;
  librados::AioCompletion *rados_completion = create_rados_ack_callback
    <klass, &klass::handle_create_local_image_checkpoint_begin>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context* CreateLocalImageRequest<I>::handle_create_local_image_checkpoint_begin(int *result) {
  dout(20) << ": r=" << *result << dendl;

  if (*result < 0) {
    derr << ": checkpointing failed before local image creation: "
         << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  create_local_image();
  return nullptr;
}

template<typename I>
void CreateLocalImageRequest<I>::create_local_image() {
  dout(20) << dendl;

  using klass = CreateLocalImageRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_create_local_image>(this);

  CreateImageRequest<I> *req = CreateImageRequest<I>::create(
    m_local_io_ctx, m_op_work_queue, m_global_image_id, m_remote_mirror_uuid,
    m_curr_image_id, m_local_image_name, m_remote_image_ctx, ctx);
  req->send();
}

template<typename I>
Context* CreateLocalImageRequest<I>::handle_create_local_image(int *result) {
  dout(20) << ": r=" << *result << dendl;

  if (*result < 0) {
    m_ret_val = *result;
    derr << ": failed to create local image: " << cpp_strerror(*result) << dendl;
    create_local_image_checkpoint_remove();
    return nullptr;
  }

  create_local_image_checkpoint_end();
  return nullptr;
}

template<typename I>
void CreateLocalImageRequest<I>::create_local_image_checkpoint_remove() {
  dout(20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_remove(&op, m_curr_image_id);

  using klass = CreateLocalImageRequest<I>;
  librados::AioCompletion *rados_completion = create_rados_ack_callback
    <klass, &klass::handle_create_local_image_checkpoint_remove>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context* CreateLocalImageRequest<I>::handle_create_local_image_checkpoint_remove(int *result) {
  dout(20) << ": r=" << *result << dendl;

  if (*result < 0) {
    derr << ": failed to remove chekpoint post image create failure: "
         << cpp_strerror(*result) << dendl;
  }

  *result = m_ret_val;
  return m_on_finish;
}

template<typename I>
void CreateLocalImageRequest<I>::create_local_image_checkpoint_end() {
  dout(20) << dendl;

  m_mirror_image.state = cls::rbd::MIRROR_IMAGE_STATE_ENABLED;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_set(&op, m_curr_image_id, m_mirror_image);

  using klass = CreateLocalImageRequest<I>;
  librados::AioCompletion *rados_completion = create_rados_ack_callback
    <klass, &klass::handle_create_local_image_checkpoint_end>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context* CreateLocalImageRequest<I>::handle_create_local_image_checkpoint_end(int *result) {
  dout(20) << ": r=" << *result << dendl;

  if (*result < 0) {
    // It gets bad here -- the image was successfully created but
    // the checkpoint could not be marked. This tricks subsequent
    // bootstraps (for this image) into assuming that image creation
    // was interrupted in the last run. So, it's better to rollback
    // and remove the image.
    m_ret_val = *result;
    remove_local_image_done();
    return nullptr;
  }

  *m_local_image_id = m_curr_image_id;

  send_notify_watcher();
  return nullptr;
}

template<typename I>
void CreateLocalImageRequest<I>::send_notify_watcher() {
  dout(20) << dendl;

  using klass = CreateLocalImageRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_notify_watcher>(this);

  librbd::MirroringWatcher<>::notify_image_updated(
    m_local_io_ctx,cls::rbd::MIRROR_IMAGE_STATE_ENABLED, m_curr_image_id,
    m_global_image_id, ctx);
}

template<typename I>
Context* CreateLocalImageRequest<I>::handle_notify_watcher(int *result) {
  dout(20) << ": r=" << *result << dendl;

  if (*result < 0) {
    derr << ": failed to send update notification: " << cpp_strerror(*result) << dendl;
  }

  *result = 0;
  return m_on_finish;
}

template<typename I>
void CreateLocalImageRequest<I>::remove_local_image_done() {
  dout(20) << dendl;

  using klass = CreateLocalImageRequest<I>;
  Context *ctx = create_context_callback
    <klass, &klass::handle_remove_local_image_done>(this);

  librbd::image::RemoveRequest<I> *req = librbd::image::RemoveRequest<I>::create(
    m_local_io_ctx, m_local_image_name, m_curr_image_id, true, m_no_op_prog_ctx,
    m_op_work_queue, ctx);
  req->send();
}

template<typename I>
Context* CreateLocalImageRequest<I>::handle_remove_local_image_done(int *result) {
  dout(20) << ": r=" << *result << dendl;

  if (*result < 0) {
    derr << ": failed to remove image: " << cpp_strerror(*result) << dendl;
  }

  *result = m_ret_val;
  return m_on_finish;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::CreateLocalImageRequest<librbd::ImageCtx>;
