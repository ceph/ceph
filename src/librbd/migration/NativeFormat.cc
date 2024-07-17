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
    I* image_ctx, const json_spirit::mObject& json_object, bool import_only)
  : m_image_ctx(image_ctx), m_json_object(json_object),
    m_import_only(import_only) {
}

template <typename I>
void NativeFormat<I>::open(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  int64_t pool_id = -1;
  std::string pool_namespace;
  std::string image_name;
  std::string image_id;

  if (auto it = m_json_object.find(POOL_NAME_KEY);
      it != m_json_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      librados::Rados rados(m_image_ctx->md_ctx);
      pool_id = rados.pool_lookup(it->second.get_str().c_str());
      if (pool_id < 0) {
        lderr(cct) << "failed to lookup pool" << dendl;
        on_finish->complete(static_cast<int>(pool_id));
        return;
      }
    } else {
      lderr(cct) << "invalid pool name" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
  }

  if (auto it = m_json_object.find(POOL_ID_KEY);
      it != m_json_object.end()) {
    if (pool_id != -1) {
      lderr(cct) << "cannot specify both pool name and pool id" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
    if (it->second.type() == json_spirit::int_type) {
      pool_id = it->second.get_int64();
    } else if (it->second.type() == json_spirit::str_type) {
      try {
        pool_id = boost::lexical_cast<int64_t>(it->second.get_str());
      } catch (boost::bad_lexical_cast&) {
      }
    }
    if (pool_id == -1) {
      lderr(cct) << "invalid pool id" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
  }

  if (pool_id == -1) {
    lderr(cct) << "missing pool name or pool id" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  if (auto it = m_json_object.find(POOL_NAMESPACE_KEY);
      it != m_json_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      pool_namespace = it->second.get_str();
    } else {
      lderr(cct) << "invalid pool namespace" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
  }

  if (auto it = m_json_object.find(IMAGE_NAME_KEY);
      it != m_json_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      image_name = it->second.get_str();
    } else {
      lderr(cct) << "invalid image name" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
  } else {
    lderr(cct) << "missing image name" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  if (auto it = m_json_object.find(IMAGE_ID_KEY);
      it != m_json_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      image_id = it->second.get_str();
    } else {
      lderr(cct) << "invalid image id" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
  }

  if (auto it = m_json_object.find(SNAP_NAME_KEY);
      it != m_json_object.end()) {
    if (it->second.type() == json_spirit::str_type) {
      m_snap_name = it->second.get_str();
    } else {
      lderr(cct) << "invalid snap name" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
  }

  if (auto it = m_json_object.find(SNAP_ID_KEY);
      it != m_json_object.end()) {
    if (!m_snap_name.empty()) {
      lderr(cct) << "cannot specify both snap name and snap id" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
    if (it->second.type() == json_spirit::int_type) {
      m_snap_id = it->second.get_uint64();
    } else if (it->second.type() == json_spirit::str_type) {
      try {
        m_snap_id = boost::lexical_cast<uint64_t>(it->second.get_str());
      } catch (boost::bad_lexical_cast&) {
      }
    }
    if (m_snap_id == CEPH_NOSNAP) {
      lderr(cct) << "invalid snap id" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
  }

  // snapshot is required for import to keep source read-only
  if (m_import_only && m_snap_name.empty() && m_snap_id == CEPH_NOSNAP) {
    lderr(cct) << "snapshot required for import" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  // TODO add support for external clusters
  librados::IoCtx io_ctx;
  int r = util::create_ioctx(m_image_ctx->md_ctx, "source image",
                             pool_id, pool_namespace, &io_ctx);
  if (r < 0) {
    on_finish->complete(r);
    return;
  }

  m_image_ctx->md_ctx.dup(io_ctx);
  m_image_ctx->data_ctx.dup(io_ctx);
  m_image_ctx->name = image_name;

  uint64_t flags = 0;
  if (image_id.empty() && !m_import_only) {
    flags |= OPEN_FLAG_OLD_FORMAT;
  } else {
    m_image_ctx->id = image_id;
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
