// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/NativeFormat.h"
#include "include/neorados/RADOS.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "json_spirit/json_spirit.h"
#include "boost/lexical_cast.hpp"
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
const std::string POOL_NAME_KEY{"pool_name"};
const std::string POOL_NAMESPACE_KEY{"pool_namespace"};
const std::string IMAGE_NAME_KEY{"image_name"};
const std::string IMAGE_ID_KEY{"image_id"};
const std::string SNAP_NAME_KEY{"snap_name"};
const std::string SNAP_ID_KEY{"snap_id"};

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
    const json_spirit::mObject& json_object, bool import_only)
  : m_json_object(json_object), m_import_only(import_only) {
}

template <typename I>
void NativeFormat<I>::open(librados::IoCtx& dst_io_ctx, I* dst_image_ctx,
                           I** src_image_ctx, Context* on_finish) {
  auto cct = reinterpret_cast<CephContext *>(dst_io_ctx.cct());
  ldout(cct, 10) << dendl;

  auto& pool_name_val = m_json_object[POOL_NAME_KEY];
  if (pool_name_val.type() == json_spirit::str_type) {
    librados::Rados rados(m_image_ctx->md_ctx);
    librados::IoCtx io_ctx;
    int r = rados.ioctx_create(pool_name_val.get_str().c_str(), io_ctx);
    if (r < 0) {
      lderr(cct) << "invalid pool name" << dendl;
      on_finish->complete(r);
      return;
    }

    m_pool_id = io_ctx.get_id();
  } else if (pool_name_val.type() != json_spirit::null_type) {
    lderr(cct) << "invalid pool name" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  auto& pool_id_val = m_json_object[POOL_ID_KEY];
  if (m_pool_id != -1 && pool_id_val.type() != json_spirit::null_type) {
    lderr(cct) << "cannot specify both pool name and pool id" << dendl;
    on_finish->complete(-EINVAL);
    return;
  } else if (pool_id_val.type() == json_spirit::int_type) {
    m_pool_id = pool_id_val.get_int64();
  } else if (pool_id_val.type() == json_spirit::str_type) {
    try {
      m_pool_id = boost::lexical_cast<int64_t>(pool_id_val.get_str());
    } catch (boost::bad_lexical_cast &) {
    }
  }

  if (m_pool_id == -1) {
    lderr(cct) << "missing or invalid pool id" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  auto& pool_namespace_val = m_json_object[POOL_NAMESPACE_KEY];
  if (pool_namespace_val.type() == json_spirit::str_type) {
    m_pool_namespace = pool_namespace_val.get_str();
  } else if (pool_namespace_val.type() != json_spirit::null_type) {
    lderr(cct) << "invalid pool namespace" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

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

  auto& snap_name_val = m_json_object[SNAP_NAME_KEY];
  if (snap_name_val.type() == json_spirit::str_type) {
    m_snap_name = snap_name_val.get_str();
  } else if (snap_name_val.type() != json_spirit::null_type) {
    lderr(cct) << "invalid snap name" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  auto& snap_id_val = m_json_object[SNAP_ID_KEY];
  if (!m_snap_name.empty() && snap_id_val.type() != json_spirit::null_type) {
    lderr(cct) << "cannot specify both snap name and snap id" << dendl;
    on_finish->complete(-EINVAL);
    return;
  } else if (snap_id_val.type() == json_spirit::str_type) {
    try {
      m_snap_id = boost::lexical_cast<uint64_t>(snap_id_val.get_str());
    } catch (boost::bad_lexical_cast &) {
    }
  } else if (snap_id_val.type() == json_spirit::int_type) {
    m_snap_id = snap_id_val.get_uint64();
  }

  if (snap_id_val.type() != json_spirit::null_type &&
      m_snap_id == CEPH_NOSNAP) {
    lderr(cct) << "invalid snap id" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  // snapshot is required for import to keep source read-only
  if (m_import_only && m_snap_name.empty() && m_snap_id == CEPH_NOSNAP) {
    lderr(cct) << "snapshot required for import" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  // TODO add support for external clusters
  librados::IoCtx src_io_ctx;
  int r = util::create_ioctx(dst_io_ctx, "source image",
                             m_pool_id, m_pool_namespace, &src_io_ctx);
  if (r < 0) {
    on_finish->complete(r);
    return;
  }

  *src_image_ctx = I::create(m_image_name, m_image_id, CEPH_NOSNAP, src_io_ctx,
                             true);
  m_image_ctx = *src_image_ctx;
  m_image_ctx->child = dst_image_ctx;

  uint64_t flags = 0;
  if (m_image_id.empty() && !m_import_only) {
    flags |= OPEN_FLAG_OLD_FORMAT;
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
  on_finish = new LambdaContext([this, on_finish](int r) {
    handle_open(r, on_finish); });
  m_image_ctx->state->open(flags, on_finish);
}

template <typename I>
void NativeFormat<I>::handle_open(int r, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  if (m_snap_id == CEPH_NOSNAP && m_snap_name.empty()) {
    on_finish->complete(0);
    return;
  }

  if (!m_snap_name.empty()) {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    m_snap_id = m_image_ctx->get_snap_id(cls::rbd::UserSnapshotNamespace{},
                                         m_snap_name);
  }

  if (m_snap_id == CEPH_NOSNAP) {
    lderr(cct) << "failed to locate snapshot " << m_snap_name << dendl;
    on_finish = new LambdaContext([on_finish](int) {
      on_finish->complete(-ENOENT); });
    m_image_ctx->state->close(on_finish);
    return;
  }

  on_finish = new LambdaContext([this, on_finish](int r) {
    handle_snap_set(r, on_finish); });
  m_image_ctx->state->snap_set(m_snap_id, on_finish);
}

template <typename I>
void NativeFormat<I>::handle_snap_set(int r, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to set snapshot " << m_snap_id << ": "
               << cpp_strerror(r) << dendl;
    on_finish = new LambdaContext([r, on_finish](int) {
      on_finish->complete(r); });
    m_image_ctx->state->close(on_finish);
    return;
  }

  on_finish->complete(0);
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
    std::move(image_extents), io::ImageArea::DATA, std::move(snap_ids),
    list_snaps_flags, snapshot_delta, {});
  req->send();
}

} // namespace migration
} // namespace librbd

template class librbd::migration::NativeFormat<librbd::ImageCtx>;
