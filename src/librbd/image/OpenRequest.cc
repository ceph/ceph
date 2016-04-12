// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/OpenRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/image/CloseRequest.h"
#include "librbd/image/RefreshRequest.h"
#include "librbd/image/SetSnapRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::OpenRequest: "

namespace librbd {
namespace image {

using util::create_context_callback;
using util::create_rados_ack_callback;

template <typename I>
OpenRequest<I>::OpenRequest(I *image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish), m_error_result(0) {
}

template <typename I>
void OpenRequest<I>::send() {
  send_v2_detect_header();
}

template <typename I>
void OpenRequest<I>::send_v1_detect_header() {
  librados::ObjectReadOperation op;
  op.stat(NULL, NULL, NULL);

  using klass = OpenRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_v1_detect_header>(this);
  m_out_bl.clear();
  m_image_ctx->md_ctx.aio_operate(util::old_header_name(m_image_ctx->name),
                                 comp, &op, &m_out_bl);
  comp->release();
}

template <typename I>
Context *OpenRequest<I>::handle_v1_detect_header(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    if (*result != -ENOENT) {
      lderr(cct) << "failed to stat image header: " << cpp_strerror(*result)
                 << dendl;
    }
    send_close_image(*result);
  } else {
    lderr(cct) << "RBD image format 1 is deprecated. "
               << "Please copy this image to image format 2." << dendl;

    m_image_ctx->old_format = true;
    m_image_ctx->header_oid = util::old_header_name(m_image_ctx->name);
    send_register_watch();
  }
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_v2_detect_header() {
  if (m_image_ctx->id.empty()) {
    CephContext *cct = m_image_ctx->cct;
    ldout(cct, 10) << this << " " << __func__ << dendl;

    librados::ObjectReadOperation op;
    op.stat(NULL, NULL, NULL);

    using klass = OpenRequest<I>;
    librados::AioCompletion *comp =
      create_rados_ack_callback<klass, &klass::handle_v2_detect_header>(this);
    m_out_bl.clear();
    m_image_ctx->md_ctx.aio_operate(util::id_obj_name(m_image_ctx->name),
                                   comp, &op, &m_out_bl);
    comp->release();
  } else {
    send_v2_get_name();
  }
}

template <typename I>
Context *OpenRequest<I>::handle_v2_detect_header(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result == -ENOENT) {
    send_v1_detect_header();
  } else if (*result < 0) {
    lderr(cct) << "failed to stat v2 image header: " << cpp_strerror(*result)
               << dendl;
    send_close_image(*result);
  } else {
    m_image_ctx->old_format = false;
    send_v2_get_id();
  }
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_v2_get_id() {
  if (m_image_ctx->id.empty()) {
    CephContext *cct = m_image_ctx->cct;
    ldout(cct, 10) << this << " " << __func__ << dendl;

    librados::ObjectReadOperation op;
    cls_client::get_id_start(&op);

    using klass = OpenRequest<I>;
    librados::AioCompletion *comp =
      create_rados_ack_callback<klass, &klass::handle_v2_get_id>(this);
    m_out_bl.clear();
    m_image_ctx->md_ctx.aio_operate(util::id_obj_name(m_image_ctx->name),
                                    comp, &op, &m_out_bl);
    comp->release();
  } else {
    send_v2_get_name();
  }
}

template <typename I>
Context *OpenRequest<I>::handle_v2_get_id(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::get_id_finish(&it, &m_image_ctx->id);
  }
  if (*result < 0) {
    lderr(cct) << "failed to retrieve image id: " << cpp_strerror(*result)
               << dendl;
    send_close_image(*result);
  } else {
    send_v2_get_immutable_metadata();
  }
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_v2_get_name() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::dir_get_name_start(&op, m_image_ctx->id);

  using klass = OpenRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_name>(this);
  m_out_bl.clear();
  m_image_ctx->md_ctx.aio_operate(RBD_DIRECTORY, comp, &op, &m_out_bl);
  comp->release();
}

template <typename I>
Context *OpenRequest<I>::handle_v2_get_name(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::dir_get_name_finish(&it, &m_image_ctx->name);
  }
  if (*result < 0) {
    lderr(cct) << "failed to retreive name: "
               << cpp_strerror(*result) << dendl;
    send_close_image(*result);
  } else {
    send_v2_get_immutable_metadata();
  }

  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_v2_get_immutable_metadata() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx->old_format = false;
  m_image_ctx->header_oid = util::header_name(m_image_ctx->id);

  librados::ObjectReadOperation op;
  cls_client::get_immutable_metadata_start(&op);

  using klass = OpenRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_immutable_metadata>(this);
  m_out_bl.clear();
  m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                  &m_out_bl);
  comp->release();
}

template <typename I>
Context *OpenRequest<I>::handle_v2_get_immutable_metadata(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::get_immutable_metadata_finish(
      &it, &m_image_ctx->object_prefix, &m_image_ctx->order);
  }
  if (*result < 0) {
    lderr(cct) << "failed to retreive immutable metadata: "
               << cpp_strerror(*result) << dendl;
    send_close_image(*result);
  } else {
    send_v2_get_stripe_unit_count();
  }

  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_v2_get_stripe_unit_count() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_stripe_unit_count_start(&op);

  using klass = OpenRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_stripe_unit_count>(this);
  m_out_bl.clear();
  m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                  &m_out_bl);
  comp->release();
}

template <typename I>
Context *OpenRequest<I>::handle_v2_get_stripe_unit_count(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::get_stripe_unit_count_finish(
      &it, &m_image_ctx->stripe_unit, &m_image_ctx->stripe_count);
  }

  if (*result == -ENOEXEC || *result == -EINVAL) {
    *result = 0;
  }

  if (*result < 0) {
    lderr(cct) << "failed to read striping metadata: " << cpp_strerror(*result)
               << dendl;
    send_close_image(*result);
  } else {
    send_register_watch();
  }
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_register_watch() {
  m_image_ctx->init();

  if (!m_image_ctx->read_only) {
    CephContext *cct = m_image_ctx->cct;
    ldout(cct, 10) << this << " " << __func__ << dendl;

    using klass = OpenRequest<I>;
    Context *ctx = create_context_callback<
      klass, &klass::handle_register_watch>(this);
    m_image_ctx->register_watch(ctx);
  } else {
    send_refresh();
  }
}

template <typename I>
Context *OpenRequest<I>::handle_register_watch(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to register watch: " << cpp_strerror(*result)
               << dendl;
    send_close_image(*result);
  } else {
    send_refresh();
  }
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_refresh() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = OpenRequest<I>;
  RefreshRequest<I> *ctx = RefreshRequest<I>::create(
    *m_image_ctx,
    create_context_callback<klass, &klass::handle_refresh>(this));
  ctx->send();
}

template <typename I>
Context *OpenRequest<I>::handle_refresh(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to refresh image: " << cpp_strerror(*result)
               << dendl;
    send_close_image(*result);
    return nullptr;
  } else {
    return send_set_snap(result);
  }
}

template <typename I>
Context *OpenRequest<I>::send_set_snap(int *result) {
  if (m_image_ctx->snap_name.empty()) {
    *result = 0;
    return m_on_finish;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = OpenRequest<I>;
  SetSnapRequest<I> *ctx = SetSnapRequest<I>::create(
    *m_image_ctx, m_image_ctx->snap_name,
    create_context_callback<klass, &klass::handle_set_snap>(this));
  ctx->send();
  return nullptr;
}

template <typename I>
Context *OpenRequest<I>::handle_set_snap(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to set image snapshot: " << cpp_strerror(*result)
               << dendl;
    send_close_image(*result);
    return nullptr;
  }

  return m_on_finish;
}

template <typename I>
void OpenRequest<I>::send_close_image(int error_result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_error_result = error_result;

  using klass = OpenRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_close_image>(
    this);
  CloseRequest<I> *req = CloseRequest<I>::create(m_image_ctx, ctx);
  req->send();
}

template <typename I>
Context *OpenRequest<I>::handle_close_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to close image: " << cpp_strerror(*result) << dendl;
  }
  if (m_error_result < 0) {
    *result = m_error_result;
  }
  return m_on_finish;
}

} // namespace image
} // namespace librbd

template class librbd::image::OpenRequest<librbd::ImageCtx>;
