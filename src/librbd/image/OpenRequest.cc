// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/OpenRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/PluginRegistry.h"
#include "librbd/Utils.h"
#include "librbd/cache/ObjectCacherObjectDispatch.h"
#include "librbd/cache/WriteAroundObjectDispatch.h"
#include "librbd/image/CloseRequest.h"
#include "librbd/image/RefreshRequest.h"
#include "librbd/image/SetSnapRequest.h"
#include "librbd/io/SimpleSchedulerObjectDispatch.h"
#include <boost/algorithm/string/predicate.hpp>
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::OpenRequest: "

namespace librbd {
namespace image {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
OpenRequest<I>::OpenRequest(I *image_ctx, uint64_t flags,
                            Context *on_finish)
  : m_image_ctx(image_ctx),
    m_skip_open_parent_image(flags & OPEN_FLAG_SKIP_OPEN_PARENT),
    m_on_finish(on_finish), m_error_result(0) {
  if ((flags & OPEN_FLAG_OLD_FORMAT) != 0) {
    m_image_ctx->old_format = true;
  }
  if ((flags & OPEN_FLAG_IGNORE_MIGRATING) != 0) {
    m_image_ctx->ignore_migrating = true;
  }
}

template <typename I>
void OpenRequest<I>::send() {
  if (m_image_ctx->old_format) {
    send_v1_detect_header();
  } else {
    send_v2_detect_header();
  }
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
    auto it = m_out_bl.cbegin();
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
    auto it = m_out_bl.cbegin();
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
    auto it = m_out_bl.cbegin();
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
  cls_client::get_size_start(&op, CEPH_NOSNAP);
  cls_client::get_object_prefix_start(&op);
  cls_client::get_features_start(&op, true);

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

  auto it = m_out_bl.cbegin();
  if (*result >= 0) {
    uint64_t size;
    *result = cls_client::get_size_finish(&it, &size, &m_image_ctx->order);
  }

  if (*result >= 0) {
    *result = cls_client::get_object_prefix_finish(&it,
                                                   &m_image_ctx->object_prefix);
  }

  if (*result >= 0) {
    uint64_t incompatible_features;
    *result = cls_client::get_features_finish(&it, &m_image_ctx->features,
                                              &incompatible_features);
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
    auto it = m_out_bl.cbegin();
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
    auto it = m_out_bl.cbegin();
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

  send_v2_get_access_modify_timestamp();
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_v2_get_access_modify_timestamp() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_access_timestamp_start(&op);
  cls_client::get_modify_timestamp_start(&op);
  //TODO: merge w/ create timestamp query after luminous EOLed

  using klass = OpenRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v2_get_access_modify_timestamp>(this);
  m_out_bl.clear();
  m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                  &m_out_bl);
  comp->release();
}

template <typename I>
Context *OpenRequest<I>::handle_v2_get_access_modify_timestamp(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    auto it = m_out_bl.cbegin();
    *result = cls_client::get_access_timestamp_finish(&it,
        &m_image_ctx->access_timestamp);
    if (*result == 0) 
      *result = cls_client::get_modify_timestamp_finish(&it,
        &m_image_ctx->modify_timestamp);
  }
  if (*result < 0 && *result != -EOPNOTSUPP) {
    lderr(cct) << "failed to retrieve access/modify_timestamp: "
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
    auto it = m_out_bl.cbegin();
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
    *result = util::create_ioctx(m_image_ctx->md_ctx, "data pool", data_pool_id,
                                 {}, &m_image_ctx->data_ctx);
    if (*result < 0) {
      if (*result != -ENOENT) {
        send_close_image(*result);
        return nullptr;
      }
      m_image_ctx->data_ctx.close();
    } else {
      m_image_ctx->data_ctx.set_namespace(m_image_ctx->md_ctx.get_namespace());
      m_image_ctx->rebuild_data_io_context();
    }
  } else {
    data_pool_id = m_image_ctx->md_ctx.get_id();
  }

  m_image_ctx->init_layout(data_pool_id);
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

  send_init_plugin_registry();
  return nullptr;
}

template <typename I>
void OpenRequest<I>::send_init_plugin_registry() {
  CephContext *cct = m_image_ctx->cct;

  auto plugins = m_image_ctx->config.template get_val<std::string>(
    "rbd_plugins");
  ldout(cct, 10) << __func__ << ": plugins=" << plugins << dendl;

  auto ctx = create_context_callback<
    OpenRequest<I>, &OpenRequest<I>::handle_init_plugin_registry>(this);
  m_image_ctx->plugin_registry->init(plugins, ctx);
}

template <typename I>
Context* OpenRequest<I>::handle_init_plugin_registry(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to initialize plugin registry: "
               << cpp_strerror(*result) << dendl;
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

  size_t max_dirty = m_image_ctx->config.template get_val<Option::size_t>(
    "rbd_cache_max_dirty");
  auto writethrough_until_flush = m_image_ctx->config.template get_val<bool>(
    "rbd_cache_writethrough_until_flush");
  auto cache_policy = m_image_ctx->config.template get_val<std::string>(
    "rbd_cache_policy");
  if (cache_policy == "writearound") {
    auto cache = cache::WriteAroundObjectDispatch<I>::create(
      m_image_ctx, max_dirty, writethrough_until_flush);
    cache->init();
  } else if (cache_policy == "writethrough" || cache_policy == "writeback") {
    if (cache_policy == "writethrough") {
      max_dirty = 0;
    }

    auto cache = cache::ObjectCacherObjectDispatch<I>::create(
      m_image_ctx, max_dirty, writethrough_until_flush);
    cache->init();

    // readahead requires the object cacher cache
    m_image_ctx->readahead.set_trigger_requests(
      m_image_ctx->config.template get_val<uint64_t>("rbd_readahead_trigger_requests"));
    m_image_ctx->readahead.set_max_readahead_size(
      m_image_ctx->config.template get_val<Option::size_t>("rbd_readahead_max_bytes"));
  }
  return send_register_watch(result);
}

template <typename I>
Context *OpenRequest<I>::send_register_watch(int *result) {
  if ((m_image_ctx->read_only_flags & IMAGE_READ_ONLY_FLAG_USER) != 0U) {
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

  if (*result == -EPERM) {
    ldout(cct, 5) << "user does not have write permission" << dendl;
    send_close_image(*result);
    return nullptr;
  } else if (*result < 0) {
    lderr(cct) << "failed to register watch: " << cpp_strerror(*result)
               << dendl;
    send_close_image(*result);
    return nullptr;
  }

  return send_set_snap(result);
}

template <typename I>
Context *OpenRequest<I>::send_set_snap(int *result) {
  if (m_image_ctx->snap_name.empty() &&
      m_image_ctx->open_snap_id == CEPH_NOSNAP) {
    *result = 0;
    return finalize(*result);
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  uint64_t snap_id = CEPH_NOSNAP;
  std::swap(m_image_ctx->open_snap_id, snap_id);
  if (snap_id == CEPH_NOSNAP) {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    snap_id = m_image_ctx->get_snap_id(m_image_ctx->snap_namespace,
                                       m_image_ctx->snap_name);
  }
  if (snap_id == CEPH_NOSNAP) {
    lderr(cct) << "failed to find snapshot " << m_image_ctx->snap_name << dendl;
    send_close_image(-ENOENT);
    return nullptr;
  }

  using klass = OpenRequest<I>;
  SetSnapRequest<I> *req = SetSnapRequest<I>::create(
    *m_image_ctx, snap_id,
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

  return finalize(*result);
}

template <typename I>
Context *OpenRequest<I>::finalize(int r) {
  if (r == 0) {
    auto io_scheduler_cfg =
      m_image_ctx->config.template get_val<std::string>("rbd_io_scheduler");

    if (io_scheduler_cfg == "simple" && !m_image_ctx->read_only) {
      auto io_scheduler =
        io::SimpleSchedulerObjectDispatch<I>::create(m_image_ctx);
      io_scheduler->init();
    }
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
