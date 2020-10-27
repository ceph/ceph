// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/NativeFormat.h"
#include "include/neorados/RADOS.hpp"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "json_spirit/json_spirit.h"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::NativeFormat: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace migration {

namespace {

const std::string TYPE_KEY{"type"};
const std::string POOL_ID_KEY{"pool_id"};
const std::string POOL_NAMESPACE_KEY{"pool_namespace"};
const std::string IMAGE_NAME_KEY{"image_name"};
const std::string IMAGE_ID_KEY{"image_id"};

} // anonymous namespace

template <typename I>
std::string NativeFormat<I>::build_source_spec(
    int64_t pool_id, const std::string& pool_namespace,
    const std::string& image_name, const std::string& image_id) {
  json_spirit::mObject source_spec;
  source_spec[TYPE_KEY] = "native";
  source_spec[POOL_ID_KEY] = pool_id;
  source_spec[POOL_NAMESPACE_KEY] = pool_namespace;
  source_spec[IMAGE_NAME_KEY] = image_name;
  if (!image_id.empty()) {
    source_spec[IMAGE_ID_KEY] = image_id;
  }
  return json_spirit::write(source_spec);
}

template <typename I>
NativeFormat<I>::NativeFormat(
    I* image_ctx, const json_spirit::mObject& json_object)
  : m_image_ctx(image_ctx), m_json_object(json_object) {
}

template <typename I>
void NativeFormat<I>::open(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto& pool_id_val = m_json_object[POOL_ID_KEY];
  if (pool_id_val.type() != json_spirit::int_type) {
    lderr(cct) << "missing or invalid pool id" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }
  m_pool_id = pool_id_val.get_int64();

  auto& pool_namespace_val = m_json_object[POOL_NAMESPACE_KEY];
  if (pool_namespace_val.type() != json_spirit::str_type) {
    lderr(cct) << "missing or invalid pool namespace" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }
  m_pool_namespace = pool_namespace_val.get_str();

  auto& image_name_val = m_json_object[IMAGE_NAME_KEY];
  if (image_name_val.type() != json_spirit::str_type) {
    lderr(cct) << "missing or invalid image name" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }
  m_image_name = image_name_val.get_str();

  auto& image_id_val = m_json_object[IMAGE_ID_KEY];
  if (image_id_val.type() == json_spirit::str_type) {
    m_image_id = image_id_val.get_str();
  } else if (image_id_val.type() != json_spirit::null_type) {
    lderr(cct) << "invalid image id" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  // TODO add support for external clusters
  librados::IoCtx io_ctx;
  int r = util::create_ioctx(m_image_ctx->md_ctx, "source image",
                             m_pool_id, m_pool_namespace, &io_ctx);
  if (r < 0) {
    on_finish->complete(r);
    return;
  }

  m_image_ctx->md_ctx.dup(io_ctx);
  m_image_ctx->data_ctx.dup(io_ctx);
  m_image_ctx->name = m_image_name;

  uint64_t flags = 0;
  if (m_image_id.empty()) {
    flags |= OPEN_FLAG_OLD_FORMAT;
  } else {
    m_image_ctx->id = m_image_id;
  }

  if (m_image_ctx->child != nullptr) {
    // set rados flags for reading the parent image
    if (m_image_ctx->child->config.template get_val<bool>("rbd_balance_parent_reads")) {
      m_image_ctx->set_read_flag(librados::OPERATION_BALANCE_READS);
    } else if (m_image_ctx->child->config.template get_val<bool>("rbd_localize_parent_reads")) {
      m_image_ctx->set_read_flag(librados::OPERATION_LOCALIZE_READS);
    }
  }

  // open the source RBD image
  auto ctx = util::create_async_context_callback(*m_image_ctx, on_finish);
  m_image_ctx->state->open(flags, ctx);
}

template <typename I>
void NativeFormat<I>::close(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  // the native librbd::image::CloseRequest handles all cleanup
  on_finish->complete(0);
}

template <typename I>
void NativeFormat<I>::get_snapshots(SnapInfos* snap_infos, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  m_image_ctx->image_lock.lock_shared();
  *snap_infos = m_image_ctx->snap_info;
  m_image_ctx->image_lock.unlock_shared();

  on_finish->complete(0);
}

template <typename I>
void NativeFormat<I>::get_image_size(uint64_t snap_id, uint64_t* size,
                                     Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  m_image_ctx->image_lock.lock_shared();
  *size = m_image_ctx->get_image_size(snap_id);
  m_image_ctx->image_lock.unlock_shared();


  on_finish->complete(0);
}

template <typename I>
void NativeFormat<I>::list_snaps(io::Extents&& image_extents,
                                 io::SnapIds&& snap_ids, int list_snaps_flags,
                                 io::SnapshotDelta* snapshot_delta,
                                 const ZTracer::Trace &parent_trace,
                                 Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  auto aio_comp = io::AioCompletion::create_and_start(
    on_finish, util::get_image_ctx(m_image_ctx), io::AIO_TYPE_GENERIC);
  auto req = io::ImageDispatchSpec::create_list_snaps(
    *m_image_ctx, io::IMAGE_DISPATCH_LAYER_MIGRATION, aio_comp,
    std::move(image_extents), std::move(snap_ids), list_snaps_flags,
    snapshot_delta, {});
  req->send();
}

} // namespace migration
} // namespace librbd

template class librbd::migration::NativeFormat<librbd::ImageCtx>;
