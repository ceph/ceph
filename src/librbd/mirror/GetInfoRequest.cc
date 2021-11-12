// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GetInfoRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GetInfoRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
GetInfoRequest<I>::GetInfoRequest(librados::IoCtx& io_ctx,
                                  asio::ContextWQ *op_work_queue,
                                  const std::string &image_id,
                                  cls::rbd::MirrorImage *mirror_image,
                                  PromotionState *promotion_state,
                                  std::string* primary_mirror_uuid,
                                  Context *on_finish)
  : m_io_ctx(io_ctx), m_op_work_queue(op_work_queue), m_image_id(image_id),
    m_mirror_image(mirror_image), m_promotion_state(promotion_state),
    m_primary_mirror_uuid(primary_mirror_uuid), m_on_finish(on_finish),
    m_cct(reinterpret_cast<CephContext *>(io_ctx.cct())) {
}

template <typename I>
GetInfoRequest<I>::GetInfoRequest(I &image_ctx,
                                  cls::rbd::MirrorImage *mirror_image,
                                  PromotionState *promotion_state,
                                  std::string* primary_mirror_uuid,
                                  Context *on_finish)
  : m_image_ctx(&image_ctx), m_io_ctx(image_ctx.md_ctx),
    m_op_work_queue(image_ctx.op_work_queue), m_image_id(image_ctx.id),
    m_mirror_image(mirror_image), m_promotion_state(promotion_state),
    m_primary_mirror_uuid(primary_mirror_uuid), m_on_finish(on_finish),
    m_cct(image_ctx.cct) {
}

template <typename I>
void GetInfoRequest<I>::send() {
  get_mirror_image();
}

template <typename I>
void GetInfoRequest<I>::get_mirror_image() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_id);

  librados::AioCompletion *comp = create_rados_callback<
    GetInfoRequest<I>, &GetInfoRequest<I>::handle_get_mirror_image>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GetInfoRequest<I>::handle_get_mirror_image(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_mirror_image->state = cls::rbd::MIRROR_IMAGE_STATE_DISABLED;
  *m_promotion_state = PROMOTION_STATE_NON_PRIMARY;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_image_get_finish(&iter, m_mirror_image);
  }

  if (r == -ENOENT) {
    ldout(m_cct, 20) << "mirroring is disabled" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (m_mirror_image->mode == cls::rbd::MIRROR_IMAGE_MODE_JOURNAL) {
    get_journal_tag_owner();
  } else if (m_mirror_image->mode == cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
    get_snapcontext();
  } else {
    ldout(m_cct, 20) << "unknown mirror image mode: " << m_mirror_image->mode
                     << dendl;
    finish(-EOPNOTSUPP);
  }
}

template <typename I>
void GetInfoRequest<I>::get_journal_tag_owner() {
  ldout(m_cct, 20) << dendl;

  auto ctx = create_context_callback<
    GetInfoRequest<I>, &GetInfoRequest<I>::handle_get_journal_tag_owner>(this);
  Journal<I>::get_tag_owner(m_io_ctx, m_image_id, &m_mirror_uuid,
                            m_op_work_queue, ctx);
}

template <typename I>
void GetInfoRequest<I>::handle_get_journal_tag_owner(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to determine tag ownership: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  if (m_mirror_uuid == Journal<>::LOCAL_MIRROR_UUID) {
    *m_promotion_state = PROMOTION_STATE_PRIMARY;
    *m_primary_mirror_uuid = "";
  } else if (m_mirror_uuid == Journal<>::ORPHAN_MIRROR_UUID) {
    *m_promotion_state = PROMOTION_STATE_ORPHAN;
    *m_primary_mirror_uuid = "";
  } else {
    *m_primary_mirror_uuid = m_mirror_uuid;
  }

  finish(0);
}

template <typename I>
void GetInfoRequest<I>::get_snapcontext() {
  if (m_image_ctx != nullptr) {
    {
      std::shared_lock image_locker{m_image_ctx->image_lock};
      calc_promotion_state(m_image_ctx->snap_info);
    }
    finish(0);
    return;
  }

  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_snapcontext_start(&op);

  librados::AioCompletion *comp = create_rados_callback<
    GetInfoRequest<I>, &GetInfoRequest<I>::handle_get_snapcontext>(this);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(util::header_name(m_image_id), comp, &op,
                               &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GetInfoRequest<I>::handle_get_snapcontext(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r >= 0) {
    auto it = m_out_bl.cbegin();
    r = cls_client::get_snapcontext_finish(&it, &m_snapc);
  }

  if (r == -ENOENT &&
      m_mirror_image->state == cls::rbd::MIRROR_IMAGE_STATE_CREATING) {
    // image doesn't exist but we have a mirror image record for it
    ldout(m_cct, 10) << "image does not exist for mirror image id "
                     << m_image_id << dendl;
    *m_promotion_state = PROMOTION_STATE_UNKNOWN;
    *m_primary_mirror_uuid = "";
    finish(0);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to get snapcontext: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  get_snapshots();
}


template <typename I>
void GetInfoRequest<I>::get_snapshots() {
  ldout(m_cct, 20) << dendl;

  if (m_snapc.snaps.empty()) {
    handle_get_snapshots(0);
    return;
  }

  librados::ObjectReadOperation op;
  for (auto snap_id : m_snapc.snaps) {
    cls_client::snapshot_get_start(&op, snap_id);
  }

  librados::AioCompletion *comp = create_rados_callback<
    GetInfoRequest<I>, &GetInfoRequest<I>::handle_get_snapshots>(this);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(util::header_name(m_image_id), comp, &op,
                               &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GetInfoRequest<I>::handle_get_snapshots(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  std::map<librados::snap_t, SnapInfo> snap_info;

  auto it = m_out_bl.cbegin();
  for (auto snap_id : m_snapc.snaps) {
    cls::rbd::SnapshotInfo snap;
    if (r >= 0) {
      r = cls_client::snapshot_get_finish(&it, &snap);
    }
    snap_info.emplace(
      snap_id, SnapInfo(snap.name, snap.snapshot_namespace, 0, {}, 0, 0, {}));
  }

  if (r == -ENOENT) {
    // restart
    get_snapcontext();
    return;
  }

  if (r < 0) {
    lderr(m_cct) << "failed to get snapshots: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  calc_promotion_state(snap_info);
  finish(0);
}

template <typename I>
void GetInfoRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

template <typename I>
void GetInfoRequest<I>::calc_promotion_state(
    const std::map<librados::snap_t, SnapInfo> &snap_info) {
  *m_promotion_state = PROMOTION_STATE_UNKNOWN;
  *m_primary_mirror_uuid = "";

  for (auto it = snap_info.rbegin(); it != snap_info.rend(); it++) {
    auto mirror_ns = boost::get<cls::rbd::MirrorSnapshotNamespace>(
      &it->second.snap_namespace);

    if (mirror_ns != nullptr) {
      switch (mirror_ns->state) {
      case cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY:
        *m_promotion_state = PROMOTION_STATE_PRIMARY;
        break;
      case cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY:
        *m_promotion_state = PROMOTION_STATE_NON_PRIMARY;
        *m_primary_mirror_uuid = mirror_ns->primary_mirror_uuid;
        break;
      case cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED:
      case cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED:
        *m_promotion_state = PROMOTION_STATE_ORPHAN;
        break;
      }
      break;
    }
  }

  ldout(m_cct, 10) << "promotion_state=" << *m_promotion_state << ", "
                   << "primary_mirror_uuid=" << *m_primary_mirror_uuid << dendl;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GetInfoRequest<librbd::ImageCtx>;
