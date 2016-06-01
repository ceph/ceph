// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CreateImageRequest.h"
#include "CloseImageRequest.h"
#include "OpenImageRequest.h"
#include "Utils.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::CreateImageRequest: " \
                           << this << " " << __func__

using librbd::util::create_context_callback;
using librbd::util::create_rados_ack_callback;

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename I>
CreateImageRequest<I>::CreateImageRequest(librados::IoCtx &local_io_ctx,
                                          ContextWQ *work_queue,
                                          const std::string &global_image_id,
                                          const std::string &remote_mirror_uuid,
                                          const std::string &local_image_name,
                                          I *remote_image_ctx,
                                          Context *on_finish)
  : m_local_io_ctx(local_io_ctx), m_work_queue(work_queue),
    m_global_image_id(global_image_id),
    m_remote_mirror_uuid(remote_mirror_uuid),
    m_local_image_name(local_image_name), m_remote_image_ctx(remote_image_ctx),
    m_on_finish(on_finish) {
}

template <typename I>
void CreateImageRequest<I>::send() {
  int r = validate_parent();
  if (r < 0) {
    error(r);
    return;
  }

  if (m_remote_parent_spec.pool_id == -1) {
    create_image();
  } else {
    get_parent_global_image_id();
  }
}

template <typename I>
void CreateImageRequest<I>::create_image() {
  dout(20) << dendl;

  // TODO: librbd should provide an AIO image creation method -- this is
  //       blocking so we execute in our worker thread
  Context *ctx = new FunctionContext([this](int r) {
      // TODO: rbd-mirror should offer a feature mask capability
      RWLock::RLocker snap_locker(m_remote_image_ctx->snap_lock);
      int order = m_remote_image_ctx->order;

      CephContext *cct = reinterpret_cast<CephContext*>(m_local_io_ctx.cct());
      uint64_t journal_order = cct->_conf->rbd_journal_order;
      uint64_t journal_splay_width = cct->_conf->rbd_journal_splay_width;
      std::string journal_pool = cct->_conf->rbd_journal_pool;

      // NOTE: bid is 64bit but overflow will result due to
      // RBD_MAX_BLOCK_NAME_SIZE being too small
      librados::Rados rados(m_local_io_ctx);
      uint64_t bid = rados.get_instance_id();

      r = utils::create_image(m_local_io_ctx, m_remote_image_ctx,
                              m_local_image_name.c_str(), bid,
                              m_remote_image_ctx->size, order,
                              m_remote_image_ctx->features,
                              m_remote_image_ctx->stripe_unit,
                              m_remote_image_ctx->stripe_count,
                              journal_order, journal_splay_width,
                              journal_pool, m_global_image_id,
                              m_remote_mirror_uuid);
      handle_create_image(r);
    });
  m_work_queue->queue(ctx, 0);
}

template <typename I>
void CreateImageRequest<I>::handle_create_image(int r) {
  dout(20) << ": r=" << r << dendl;
  if (r < 0) {
    derr << ": failed to create local image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void CreateImageRequest<I>::get_parent_global_image_id() {
  dout(20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_start(&op, m_remote_parent_spec.image_id);

  librados::AioCompletion *aio_comp = create_rados_ack_callback<
    CreateImageRequest<I>,
    &CreateImageRequest<I>::handle_get_parent_global_image_id>(this);
  m_out_bl.clear();
  int r = m_remote_parent_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op,
                                             &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void CreateImageRequest<I>::handle_get_parent_global_image_id(int r) {
  dout(20) << ": r=" << r << dendl;
  if (r == 0) {
    cls::rbd::MirrorImage mirror_image;
    bufferlist::iterator iter = m_out_bl.begin();
    r = librbd::cls_client::mirror_image_get_finish(&iter, &mirror_image);
    if (r == 0) {
      m_parent_global_image_id = mirror_image.global_image_id;
      dout(20) << ": parent_global_image_id=" << m_parent_global_image_id
               << dendl;
    }
  }

  if (r == -ENOENT) {
    dout(10) << ": parent image " << m_remote_parent_spec.image_id << " not mirrored"
             << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << ": failed to retrieve global image id for parent image "
         << m_remote_parent_spec.image_id << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_local_parent_image_id();
}

template <typename I>
void CreateImageRequest<I>::get_local_parent_image_id() {
  dout(20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_image_id_start(
    &op, m_parent_global_image_id);

  librados::AioCompletion *aio_comp = create_rados_ack_callback<
    CreateImageRequest<I>,
    &CreateImageRequest<I>::handle_get_local_parent_image_id>(this);
  m_out_bl.clear();
  int r = m_local_parent_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op,
                                            &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void CreateImageRequest<I>::handle_get_local_parent_image_id(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == 0) {
    bufferlist::iterator iter = m_out_bl.begin();
    r = librbd::cls_client::mirror_image_get_image_id_finish(
      &iter, &m_local_parent_spec.image_id);
  }

  if (r == -ENOENT) {
    dout(10) << ": parent image " << m_parent_global_image_id << " not "
             << "registered locally" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << ": failed to retrieve local image id for parent image "
         << m_parent_global_image_id << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  open_remote_parent_image();
}

template <typename I>
void CreateImageRequest<I>::open_remote_parent_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    CreateImageRequest<I>,
    &CreateImageRequest<I>::handle_open_remote_parent_image>(this);
  OpenImageRequest<I> *request = OpenImageRequest<I>::create(
    m_remote_parent_io_ctx, &m_remote_parent_image_ctx,
    m_remote_parent_spec.image_id, true, m_work_queue, ctx);
  request->send();
}

template <typename I>
void CreateImageRequest<I>::handle_open_remote_parent_image(int r) {
  dout(20) << ": r=" << r << dendl;
  if (r < 0) {
    derr << ": failed to open remote parent image " << m_parent_pool_name << "/"
         << m_remote_parent_spec.image_id << dendl;
    finish(r);
    return;
  }

  open_local_parent_image();
}

template <typename I>
void CreateImageRequest<I>::open_local_parent_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    CreateImageRequest<I>,
    &CreateImageRequest<I>::handle_open_local_parent_image>(this);
  OpenImageRequest<I> *request = OpenImageRequest<I>::create(
    m_local_parent_io_ctx, &m_local_parent_image_ctx, m_local_parent_spec.image_id,
    true, m_work_queue, ctx);
  request->send();
}

template <typename I>
void CreateImageRequest<I>::handle_open_local_parent_image(int r) {
  dout(20) << ": r=" << r << dendl;
  if (r < 0) {
    derr << ": failed to open local parent image " << m_parent_pool_name << "/"
         << m_local_parent_spec.image_id << dendl;
    m_ret_val = r;
    close_remote_parent_image();
    return;
  }

  set_local_parent_snap();
}

template <typename I>
void CreateImageRequest<I>::set_local_parent_snap() {
  dout(20) << dendl;

  {
    RWLock::RLocker remote_snap_locker(m_remote_parent_image_ctx->snap_lock);
    auto it = m_remote_parent_image_ctx->snap_info.find(
      m_remote_parent_spec.snap_id);
    if (it != m_remote_parent_image_ctx->snap_info.end()) {
      m_parent_snap_name = it->second.name;
    }
  }

  if (m_parent_snap_name.empty()) {
    m_ret_val = -ENOENT;
    close_local_parent_image();
    return;
  }
  dout(20) << ": parent_snap_name=" << m_parent_snap_name << dendl;

  Context *ctx = create_context_callback<
    CreateImageRequest<I>,
    &CreateImageRequest<I>::handle_set_local_parent_snap>(this);
  m_local_parent_image_ctx->state->snap_set(m_parent_snap_name, ctx);
}

template <typename I>
void CreateImageRequest<I>::handle_set_local_parent_snap(int r) {
  dout(20) << ": r=" << r << dendl;
  if (r < 0) {
    derr << ": failed to set parent snapshot " << m_parent_snap_name
         << ": " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_local_parent_image();
    return;
  }

  clone_image();
}

template <typename I>
void CreateImageRequest<I>::clone_image() {
  dout(20) << dendl;

  // TODO: librbd should provide an AIO image clone method -- this is
  //       blocking so we execute in our worker thread
  Context *ctx = new FunctionContext([this](int r) {
      RWLock::RLocker snap_locker(m_remote_image_ctx->snap_lock);

      librbd::ImageOptions opts;
      opts.set(RBD_IMAGE_OPTION_FEATURES, m_remote_image_ctx->features);
      opts.set(RBD_IMAGE_OPTION_ORDER, m_remote_image_ctx->order);
      opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, m_remote_image_ctx->stripe_unit);
      opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, m_remote_image_ctx->stripe_count);

      r = utils::clone_image(m_local_parent_image_ctx, m_local_io_ctx,
                             m_local_image_name.c_str(), opts,
                             m_global_image_id, m_remote_mirror_uuid);
      handle_clone_image(r);
    });
  m_work_queue->queue(ctx, 0);
}

template <typename I>
void CreateImageRequest<I>::handle_clone_image(int r) {
  dout(20) << ": r=" << r << dendl;
  if (r < 0) {
    derr << ": failed to clone image " << m_parent_pool_name << "/"
         << m_local_parent_image_ctx->name << " to "
         << m_local_image_name << dendl;
    m_ret_val = r;
  }

  close_local_parent_image();
}

template <typename I>
void CreateImageRequest<I>::close_local_parent_image() {
  dout(20) << dendl;
  Context *ctx = create_context_callback<
    CreateImageRequest<I>,
    &CreateImageRequest<I>::handle_close_local_parent_image>(this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    &m_local_parent_image_ctx, m_work_queue, false, ctx);
  request->send();
}

template <typename I>
void CreateImageRequest<I>::handle_close_local_parent_image(int r) {
  dout(20) << ": r=" << r << dendl;
  if (r < 0) {
    derr << ": error encountered closing local parent image: "
         << cpp_strerror(r) << dendl;
  }

  close_remote_parent_image();
}

template <typename I>
void CreateImageRequest<I>::close_remote_parent_image() {
  dout(20) << dendl;
  Context *ctx = create_context_callback<
    CreateImageRequest<I>,
    &CreateImageRequest<I>::handle_close_remote_parent_image>(this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    &m_remote_parent_image_ctx, m_work_queue, false, ctx);
  request->send();
}

template <typename I>
void CreateImageRequest<I>::handle_close_remote_parent_image(int r) {
  dout(20) << ": r=" << r << dendl;
  if (r < 0) {
    derr << ": error encountered closing remote parent image: "
         << cpp_strerror(r) << dendl;
  }

  finish(m_ret_val);
}

template <typename I>
void CreateImageRequest<I>::error(int r) {
  dout(20) << ": r=" << r << dendl;

  m_work_queue->queue(create_context_callback<
    CreateImageRequest<I>, &CreateImageRequest<I>::finish>(this), r);
}

template <typename I>
void CreateImageRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;
  m_on_finish->complete(r);
  delete this;
}

template <typename I>
int CreateImageRequest<I>::validate_parent() {
  RWLock::RLocker owner_locker(m_remote_image_ctx->owner_lock);
  RWLock::RLocker snap_locker(m_remote_image_ctx->snap_lock);

  m_remote_parent_spec = m_remote_image_ctx->parent_md.spec;

  // scan all remote snapshots for a linked parent
  for (auto &snap_info_pair : m_remote_image_ctx->snap_info) {
    auto &parent_spec = snap_info_pair.second.parent.spec;
    if (parent_spec.pool_id == -1) {
      continue;
    } else if (m_remote_parent_spec.pool_id == -1) {
      m_remote_parent_spec = parent_spec;
      continue;
    }

    if (m_remote_parent_spec != parent_spec) {
      derr << ": remote image parent spec mismatch" << dendl;
      return -EINVAL;
    }
  }

  if (m_remote_parent_spec.pool_id == -1) {
    return 0;
  }

  // map remote parent pool to local parent pool
  librados::Rados remote_rados(m_remote_image_ctx->md_ctx);
  int r = remote_rados.ioctx_create2(m_remote_parent_spec.pool_id,
                                     m_remote_parent_io_ctx);
  if (r < 0) {
    derr << ": failed to open remote parent pool " << m_remote_parent_spec.pool_id
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  m_parent_pool_name = m_remote_parent_io_ctx.get_pool_name();

  librados::Rados local_rados(m_local_io_ctx);
  r = local_rados.ioctx_create(m_parent_pool_name.c_str(),
                               m_local_parent_io_ctx);
  if (r < 0) {
    derr << ": failed to open local parent pool " << m_parent_pool_name << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::CreateImageRequest<librbd::ImageCtx>;
