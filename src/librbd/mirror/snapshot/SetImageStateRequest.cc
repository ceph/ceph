// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/SetImageStateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/WriteImageStateRequest.h"

#include <boost/algorithm/string/predicate.hpp>

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror_snapshot::SetImageStateRequest: " \
                           << this << " " << __func__ << ": "

namespace {

const uint64_t MAX_METADATA_ITEMS = 128;

}

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void SetImageStateRequest<I>::send() {
  get_snap_limit();
}

template <typename I>
void SetImageStateRequest<I>::get_snap_limit() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  librados::ObjectReadOperation op;
  cls_client::snapshot_get_limit_start(&op);

  librados::AioCompletion *comp = create_rados_callback<
    SetImageStateRequest<I>,
    &SetImageStateRequest<I>::handle_get_snap_limit>(this);
  m_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                          &m_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void SetImageStateRequest<I>::handle_get_snap_limit(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r == 0) {
    auto it = m_bl.cbegin();
    r = cls_client::snapshot_get_limit_finish(&it, &m_image_state.snap_limit);
  }

  if (r < 0) {
    lderr(cct) << "failed to retrieve snapshot limit: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  get_metadata();
}

template <typename I>
void SetImageStateRequest<I>::get_metadata() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "start_key=" << m_last_metadata_key << dendl;

  librados::ObjectReadOperation op;
  cls_client::metadata_list_start(&op, m_last_metadata_key, MAX_METADATA_ITEMS);

  librados::AioCompletion *comp = create_rados_callback<
    SetImageStateRequest<I>,
    &SetImageStateRequest<I>::handle_get_metadata>(this);
  m_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                          &m_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void SetImageStateRequest<I>::handle_get_metadata(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  std::map<std::string, bufferlist> metadata;
  if (r == 0) {
    auto it = m_bl.cbegin();
    r = cls_client::metadata_list_finish(&it, &metadata);
  }

  if (r < 0) {
    lderr(cct) << "failed to retrieve metadata: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (!metadata.empty()) {
    m_image_state.metadata.insert(metadata.begin(), metadata.end());
    m_last_metadata_key = metadata.rbegin()->first;
    if (boost::starts_with(m_last_metadata_key,
                           ImageCtx::METADATA_CONF_PREFIX)) {
      get_metadata();
      return;
    }
  }

  {
    std::shared_lock image_locker{m_image_ctx->image_lock};

    m_image_state.name = m_image_ctx->name;
    m_image_state.features = m_image_ctx->features;

    for (auto &[snap_id, snap_info] : m_image_ctx->snap_info) {
      auto type = cls::rbd::get_snap_namespace_type(snap_info.snap_namespace);
      if (type == cls::rbd::SNAPSHOT_NAMESPACE_TYPE_MIRROR_PRIMARY ||
          type == cls::rbd::SNAPSHOT_NAMESPACE_TYPE_MIRROR_NON_PRIMARY) {
        continue;
      }
      m_image_state.snapshots[snap_id] = {snap_id, snap_info.snap_namespace,
                                          snap_info.name,
                                          snap_info.protection_status};
    }
  }

  write_image_state();
}

template <typename I>
void SetImageStateRequest<I>::write_image_state() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    SetImageStateRequest<I>,
    &SetImageStateRequest<I>::handle_write_image_state>(this);

  auto req = WriteImageStateRequest<I>::create(m_image_ctx, m_snap_id,
                                               m_image_state, ctx);
  req->send();
}

template <typename I>
void SetImageStateRequest<I>::handle_write_image_state(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to write image state: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void SetImageStateRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::SetImageStateRequest<librbd::ImageCtx>;
