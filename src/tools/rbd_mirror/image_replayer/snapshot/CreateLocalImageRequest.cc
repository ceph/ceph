// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CreateLocalImageRequest.h"
#include "include/rados/librados.hpp"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/ProgressContext.h"
#include "tools/rbd_mirror/image_replayer/CreateImageRequest.h"
#include "tools/rbd_mirror/image_replayer/snapshot/StateBuilder.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::snapshot::" \
                           << "CreateLocalImageRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void CreateLocalImageRequest<I>::send() {
  disable_mirror_image();
}

template <typename I>
void CreateLocalImageRequest<I>::disable_mirror_image() {
  if (m_state_builder->local_image_id.empty()) {
    add_mirror_image();
    return;
  }

  dout(10) << dendl;
  update_progress("DISABLE_MIRROR_IMAGE");

  // need to send 'disabling' since the cls methods will fail if we aren't
  // in that state
  cls::rbd::MirrorImage mirror_image{
    cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, m_global_image_id,
    cls::rbd::MIRROR_IMAGE_STATE_DISABLING};
  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_set(&op, m_state_builder->local_image_id,
                                       mirror_image);

  auto aio_comp = create_rados_callback<
    CreateLocalImageRequest<I>,
    &CreateLocalImageRequest<I>::handle_disable_mirror_image>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void CreateLocalImageRequest<I>::handle_disable_mirror_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to disable mirror image " << m_global_image_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_mirror_image();
}

template <typename I>
void CreateLocalImageRequest<I>::remove_mirror_image() {
  dout(10) << dendl;
  update_progress("REMOVE_MIRROR_IMAGE");

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_remove(&op, m_state_builder->local_image_id);

  auto aio_comp = create_rados_callback<
    CreateLocalImageRequest<I>,
    &CreateLocalImageRequest<I>::handle_remove_mirror_image>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void CreateLocalImageRequest<I>::handle_remove_mirror_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to remove mirror image " << m_global_image_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_state_builder->local_image_id = "";
  add_mirror_image();
}

template <typename I>
void CreateLocalImageRequest<I>::add_mirror_image() {
  ceph_assert(m_state_builder->local_image_id.empty());
  m_state_builder->local_image_id =
    librbd::util::generate_image_id<I>(m_local_io_ctx);

  dout(10) << "local_image_id=" << m_state_builder->local_image_id << dendl;
  update_progress("ADD_MIRROR_IMAGE");

  // use 'creating' to track a partially constructed image. it will
  // be switched to 'enabled' once the image is fully created
  cls::rbd::MirrorImage mirror_image{
    cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, m_global_image_id,
    cls::rbd::MIRROR_IMAGE_STATE_CREATING};
  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_set(&op, m_state_builder->local_image_id,
                                       mirror_image);

  auto aio_comp = create_rados_callback<
    CreateLocalImageRequest<I>,
    &CreateLocalImageRequest<I>::handle_add_mirror_image>(this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void CreateLocalImageRequest<I>::handle_add_mirror_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to register mirror image " << m_global_image_id << ": "
         << cpp_strerror(r) << dendl;
    this->finish(r);
    return;
  }

  create_local_image();
}

template <typename I>
void CreateLocalImageRequest<I>::create_local_image() {
  dout(10) << "local_image_id=" << m_state_builder->local_image_id << dendl;
  update_progress("CREATE_LOCAL_IMAGE");

  m_remote_image_ctx->image_lock.lock_shared();
  std::string image_name = m_remote_image_ctx->name;
  m_remote_image_ctx->image_lock.unlock_shared();

  auto ctx = create_context_callback<
    CreateLocalImageRequest<I>,
    &CreateLocalImageRequest<I>::handle_create_local_image>(this);
  auto request = CreateImageRequest<I>::create(
    m_threads, m_local_io_ctx, m_global_image_id,
    m_state_builder->remote_mirror_uuid, image_name,
    m_state_builder->local_image_id, m_remote_image_ctx,
    m_pool_meta_cache, cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, ctx);
  request->send();
}
template <typename I>
void CreateLocalImageRequest<I>::handle_create_local_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -EBADF) {
    dout(5) << "image id " << m_state_builder->local_image_id << " "
            << "already in-use" << dendl;
    disable_mirror_image();
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
void CreateLocalImageRequest<I>::update_progress(
    const std::string& description) {
  dout(15) << description << dendl;
  if (m_progress_ctx != nullptr) {
    m_progress_ctx->update_progress(description);
  }
}

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::snapshot::CreateLocalImageRequest<librbd::ImageCtx>;
