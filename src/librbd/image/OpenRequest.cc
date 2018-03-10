// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/OpenRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/cache/ObjectCacherObjectDispatch.h"
#include "librbd/image/CloseRequest.h"
#include "librbd/image/RefreshRequest.h"
#include "librbd/image/SetSnapRequest.h"
#include <boost/algorithm/string/predicate.hpp>
#include "include/assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::OpenRequest: "

namespace librbd {
namespace image {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
OpenRequest<I>::OpenRequest(I *image_ctx, bool skip_open_parent,
                            Context *on_finish)
  : m_image_ctx(image_ctx), m_skip_open_parent_image(skip_open_parent),
    m_on_finish(on_finish), m_error_result(0) {
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
    create_rados_callback<klass, &klass::handle_v1_detect_header>(this);
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
    ldout(cct, 1) << "RBD image format 1 is deprecated. "
                  << "Please copy this image to image format 2." << dendl;

    m_image_ctx->old_format = true;
    m_image_ctx->header_oid = util::old_header_name(m_image_ctx->name);
    m_image_ctx->apply_metadata({}, true);

    send_refresh();
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
      create_rados_callback<klass, &klass::handle_v2_detect_header>(this);
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
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_id_start(&op);

  using klass = OpenRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_v2_get_id>(this);
  m_out_bl.clear();
  m_image_ctx->md_ctx.aio_operate(util::id_obj_name(m_image_ctx->name),
                                  comp, &op, &m_out_bl);
  comp->release();
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
    send_v2_get_initial_metadata();
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
  librados::AioCompletion *comp = create_rados_callback<
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
  if (*result < 0 && *result != -ENOENT) {
    lderr(cct) << "failed to retrieve name: "
               << cpp_strerror(*result) << dendl;
    send_close_image(*result);
  } else if (*result == -ENOENT) {
    // image does not exist in directory, look in the trash bin
    ldout(cct, 10) << "image id " << m_image_ctx->id << " does not exist in "
                   << "rbd directory, searching in rbd trash..." << dendl;
    send_v2_get_name_from_trash();
  } else {
    send_v2_get_initial_metadata();
  }
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_v2_get_name_from_trash() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::trash_get_start(&op, m_image_ctx->id);

  using klass = OpenRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v2_get_name_from_trash>(this);
  m_out_bl.clear();
  m_image_ctx->md_ctx.aio_operate(RBD_TRASH, comp, &op, &m_out_bl);
  comp->release();
}

template <typename I>
Context *OpenRequest<I>::handle_v2_get_name_from_trash(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  cls::rbd::TrashImageSpec trash_spec;
  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::trash_get_finish(&it, &trash_spec);
    m_image_ctx->name = trash_spec.name;
  }
  if (*result < 0) {
    if (*result == -EOPNOTSUPP) {
      *result = -ENOENT;
    }
    if (*result == -ENOENT) {
      ldout(cct, 5) << "failed to retrieve name for image id "
                    << m_image_ctx->id << dendl;
    } else {
      lderr(cct) << "failed to retrieve name from trash: "
                 << cpp_strerror(*result) << dendl;
    }
    send_close_image(*result);
  } else {
    send_v2_get_initial_metadata();
  }

  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_v2_get_initial_metadata() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx->old_format = false;
  m_image_ctx->header_oid = util::header_name(m_image_ctx->id);

  librados::ObjectReadOperation op;
  cls_client::get_initial_metadata_start(&op);

  using klass = OpenRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v2_get_initial_metadata>(this);
  m_out_bl.clear();
  m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                  &m_out_bl);
  comp->release();
}

template <typename I>
Context *OpenRequest<I>::handle_v2_get_initial_metadata(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::get_initial_metadata_finish(
      &it, &m_image_ctx->object_prefix, &m_image_ctx->order, &m_image_ctx->features);
  }
  if (*result < 0) {
    lderr(cct) << "failed to retrieve initial metadata: "
               << cpp_strerror(*result) << dendl;
    send_close_image(*result);
    return nullptr;
  }

  if (m_image_ctx->test_features(RBD_FEATURE_STRIPINGV2)) {
    send_v2_get_stripe_unit_count();
  } else {
    send_v2_get_create_timestamp();
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
  librados::AioCompletion *comp = create_rados_callback<
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
    return nullptr;
  }

  send_v2_get_create_timestamp();
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_v2_get_create_timestamp() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_create_timestamp_start(&op);

  using klass = OpenRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v2_get_create_timestamp>(this);
  m_out_bl.clear();
  m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                  &m_out_bl);
  comp->release();
}

template <typename I>
Context *OpenRequest<I>::handle_v2_get_create_timestamp(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::get_create_timestamp_finish(&it,
        &m_image_ctx->create_timestamp);
  }
  if (*result < 0 && *result != -EOPNOTSUPP) {
    lderr(cct) << "failed to retrieve create_timestamp: "
               << cpp_strerror(*result)
               << dendl;
    send_close_image(*result);
    return nullptr;
  }

  send_v2_get_data_pool();
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_v2_get_data_pool() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_data_pool_start(&op);

  using klass = OpenRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v2_get_data_pool>(this);
  m_out_bl.clear();
  m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                  &m_out_bl);
  comp->release();
}

template <typename I>
Context *OpenRequest<I>::handle_v2_get_data_pool(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  int64_t data_pool_id = -1;
  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::get_data_pool_finish(&it, &data_pool_id);
  } else if (*result == -EOPNOTSUPP) {
    *result = 0;
  }

  if (*result < 0) {
    lderr(cct) << "failed to read data pool: " << cpp_strerror(*result)
               << dendl;
    send_close_image(*result);
    return nullptr;
  }

  if (data_pool_id != -1) {
    librados::Rados rados(m_image_ctx->md_ctx);
    *result = rados.ioctx_create2(data_pool_id, m_image_ctx->data_ctx);
    if (*result < 0) {
      lderr(cct) << "failed to initialize data pool IO context: "
                 << cpp_strerror(*result) << dendl;
      send_close_image(*result);
      return nullptr;
    }
  }

  m_image_ctx->init_layout();
  send_refresh();
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_refresh() {
  m_image_ctx->init();

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = OpenRequest<I>;
  RefreshRequest<I> *req = RefreshRequest<I>::create(
    *m_image_ctx, false, m_skip_open_parent_image,
    create_context_callback<klass, &klass::handle_refresh>(this));
  req->send();
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
  }

  return send_init_cache(result);
}

template <typename I>
Context *OpenRequest<I>::send_init_cache(int *result) {
  // cache is disabled or parent image context
  if (!m_image_ctx->cache || m_image_ctx->child != nullptr) {
    return send_register_watch(result);
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  auto cache = cache::ObjectCacherObjectDispatch<I>::create(m_image_ctx);
  cache->init();

  // readahead requires the cache
  m_image_ctx->readahead.set_trigger_requests(
    m_image_ctx->readahead_trigger_requests);
  m_image_ctx->readahead.set_max_readahead_size(
    m_image_ctx->readahead_max_bytes);

  return send_register_watch(result);
}

template <typename I>
Context *OpenRequest<I>::send_register_watch(int *result) {
  if (m_image_ctx->read_only) {
    return send_set_snap(result);
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = OpenRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_register_watch>(this);
  m_image_ctx->register_watch(ctx);
  return nullptr;
}

template <typename I>
Context *OpenRequest<I>::handle_register_watch(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to register watch: " << cpp_strerror(*result)
               << dendl;
    send_close_image(*result);
    return nullptr;
  }

  return send_set_snap(result);
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
  SetSnapRequest<I> *req = SetSnapRequest<I>::create(
    *m_image_ctx, m_image_ctx->snap_namespace, m_image_ctx->snap_name,
    create_context_callback<klass, &klass::handle_set_snap>(this));
  req->send();
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
