// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "DeepCopyRequest.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/deep_copy/ImageCopyRequest.h"
#include "librbd/deep_copy/MetadataCopyRequest.h"
#include "librbd/deep_copy/SnapshotCopyRequest.h"
#include "librbd/internal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::DeepCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {

using namespace librbd::deep_copy;

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;
using librbd::util::unique_lock_name;

template <typename I>
DeepCopyRequest<I>::DeepCopyRequest(I *src_image_ctx, I *dst_image_ctx,
                                    librados::snap_t src_snap_id_start,
                                    librados::snap_t src_snap_id_end,
                                    librados::snap_t dst_snap_id_start,
                                    bool flatten,
                                    const ObjectNumber &object_number,
                                    asio::ContextWQ *work_queue,
                                    SnapSeqs *snap_seqs,
                                    deep_copy::Handler *handler,
                                    Context *on_finish)
  : RefCountedObject(dst_image_ctx->cct), m_src_image_ctx(src_image_ctx),
    m_dst_image_ctx(dst_image_ctx), m_src_snap_id_start(src_snap_id_start),
    m_src_snap_id_end(src_snap_id_end), m_dst_snap_id_start(dst_snap_id_start),
    m_flatten(flatten), m_object_number(object_number),
    m_work_queue(work_queue), m_snap_seqs(snap_seqs), m_handler(handler),
    m_on_finish(on_finish), m_cct(dst_image_ctx->cct),
    m_lock(ceph::make_mutex(unique_lock_name("DeepCopyRequest::m_lock", this))) {
}

template <typename I>
DeepCopyRequest<I>::~DeepCopyRequest() {
  ceph_assert(m_snapshot_copy_request == nullptr);
  ceph_assert(m_image_copy_request == nullptr);
}

template <typename I>
void DeepCopyRequest<I>::send() {
  if (!m_src_image_ctx->data_ctx.is_valid()) {
    lderr(m_cct) << "missing data pool for source image" << dendl;
    finish(-ENODEV);
    return;
  }

  if (!m_dst_image_ctx->data_ctx.is_valid()) {
    lderr(m_cct) << "missing data pool for destination image" << dendl;
    finish(-ENODEV);
    return;
  }

  int r = validate_copy_points();
  if (r < 0) {
    finish(r);
    return;
  }

  send_copy_snapshots();
}

template <typename I>
void DeepCopyRequest<I>::cancel() {
  std::lock_guard locker{m_lock};

  ldout(m_cct, 20) << dendl;

  m_canceled = true;

  if (m_snapshot_copy_request != nullptr) {
    m_snapshot_copy_request->cancel();
  }

  if (m_image_copy_request != nullptr) {
    m_image_copy_request->cancel();
  }
}

template <typename I>
void DeepCopyRequest<I>::send_copy_snapshots() {
  m_lock.lock();
  if (m_canceled) {
    m_lock.unlock();
    finish(-ECANCELED);
    return;
  }

  ldout(m_cct, 20) << dendl;

  Context *ctx = create_context_callback<
    DeepCopyRequest<I>, &DeepCopyRequest<I>::handle_copy_snapshots>(this);
  m_snapshot_copy_request = SnapshotCopyRequest<I>::create(
    m_src_image_ctx, m_dst_image_ctx, m_src_snap_id_start, m_src_snap_id_end,
    m_dst_snap_id_start, m_flatten, m_work_queue, m_snap_seqs, ctx);
  m_snapshot_copy_request->get();
  m_lock.unlock();

  m_snapshot_copy_request->send();
}

template <typename I>
void DeepCopyRequest<I>::handle_copy_snapshots(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  {
    std::lock_guard locker{m_lock};
    m_snapshot_copy_request->put();
    m_snapshot_copy_request = nullptr;
    if (r == 0 && m_canceled) {
      r = -ECANCELED;
    }
  }

  if (r == -ECANCELED) {
    ldout(m_cct, 10) << "snapshot copy canceled" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to copy snapshot metadata: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  if (m_src_snap_id_end == CEPH_NOSNAP) {
    (*m_snap_seqs)[CEPH_NOSNAP] = CEPH_NOSNAP;
  }

  send_copy_image();
}

template <typename I>
void DeepCopyRequest<I>::send_copy_image() {
  m_lock.lock();
  if (m_canceled) {
    m_lock.unlock();
    finish(-ECANCELED);
    return;
  }

  ldout(m_cct, 20) << dendl;

  Context *ctx = create_context_callback<
    DeepCopyRequest<I>, &DeepCopyRequest<I>::handle_copy_image>(this);
  m_image_copy_request = ImageCopyRequest<I>::create(
    m_src_image_ctx, m_dst_image_ctx, m_src_snap_id_start, m_src_snap_id_end,
    m_dst_snap_id_start, m_flatten, m_object_number, *m_snap_seqs, m_handler,
    ctx);
  m_image_copy_request->get();
  m_lock.unlock();

  m_image_copy_request->send();
}

template <typename I>
void DeepCopyRequest<I>::handle_copy_image(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  {
    std::lock_guard locker{m_lock};
    m_image_copy_request->put();
    m_image_copy_request = nullptr;
    if (r == 0 && m_canceled) {
      r = -ECANCELED;
    }
  }

  if (r == -ECANCELED) {
    ldout(m_cct, 10) << "image copy canceled" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to copy image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_copy_object_map();
}

template <typename I>
void DeepCopyRequest<I>::send_copy_object_map() {
  m_dst_image_ctx->owner_lock.lock_shared();
  m_dst_image_ctx->image_lock.lock_shared();

  if (!m_dst_image_ctx->test_features(RBD_FEATURE_OBJECT_MAP,
                                      m_dst_image_ctx->image_lock)) {
    m_dst_image_ctx->image_lock.unlock_shared();
    m_dst_image_ctx->owner_lock.unlock_shared();
    send_copy_metadata();
    return;
  }
  if (m_src_snap_id_end == CEPH_NOSNAP) {
    m_dst_image_ctx->image_lock.unlock_shared();
    m_dst_image_ctx->owner_lock.unlock_shared();
    send_refresh_object_map();
    return;
  }

  ceph_assert(m_dst_image_ctx->object_map != nullptr);

  ldout(m_cct, 20) << dendl;

  Context *finish_op_ctx = nullptr;
  int r;
  if (m_dst_image_ctx->exclusive_lock != nullptr) {
    finish_op_ctx = m_dst_image_ctx->exclusive_lock->start_op(&r);
  }
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    m_dst_image_ctx->image_lock.unlock_shared();
    m_dst_image_ctx->owner_lock.unlock_shared();
    finish(r);
    return;
  }

  // rollback the object map (copy snapshot object map to HEAD)
  auto ctx = new LambdaContext([this, finish_op_ctx](int r) {
      handle_copy_object_map(r);
      finish_op_ctx->complete(0);
    });
  ceph_assert(m_snap_seqs->count(m_src_snap_id_end) > 0);
  librados::snap_t copy_snap_id = (*m_snap_seqs)[m_src_snap_id_end];
  m_dst_image_ctx->object_map->rollback(copy_snap_id, ctx);
  m_dst_image_ctx->image_lock.unlock_shared();
  m_dst_image_ctx->owner_lock.unlock_shared();
}

template <typename I>
void DeepCopyRequest<I>::handle_copy_object_map(int r) {
  ldout(m_cct, 20) << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to roll back object map: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  send_refresh_object_map();
}

template <typename I>
void DeepCopyRequest<I>::send_refresh_object_map() {
  int r;
  Context *finish_op_ctx = nullptr;
  {
    std::shared_lock owner_locker{m_dst_image_ctx->owner_lock};
    if (m_dst_image_ctx->exclusive_lock != nullptr) {
      finish_op_ctx = m_dst_image_ctx->exclusive_lock->start_op(&r);
    }
  }
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(r);
    return;
  }

  ldout(m_cct, 20) << dendl;

  auto ctx = new LambdaContext([this, finish_op_ctx](int r) {
      handle_refresh_object_map(r);
      finish_op_ctx->complete(0);
    });
  m_object_map = m_dst_image_ctx->create_object_map(CEPH_NOSNAP);
  m_object_map->open(ctx);
}

template <typename I>
void DeepCopyRequest<I>::handle_refresh_object_map(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to open object map: " << cpp_strerror(r)
                 << dendl;
    delete m_object_map;

    finish(r);
    return;
  }

  {
    std::unique_lock image_locker{m_dst_image_ctx->image_lock};
    std::swap(m_dst_image_ctx->object_map, m_object_map);
  }
  m_object_map->put();

  send_copy_metadata();
}

template <typename I>
void DeepCopyRequest<I>::send_copy_metadata() {
  ldout(m_cct, 20) << dendl;

  Context *ctx = create_context_callback<
    DeepCopyRequest<I>, &DeepCopyRequest<I>::handle_copy_metadata>(this);
  auto request = MetadataCopyRequest<I>::create(m_src_image_ctx,
                                                m_dst_image_ctx, ctx);
  request->send();
}

template <typename I>
void DeepCopyRequest<I>::handle_copy_metadata(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to copy metadata: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
int DeepCopyRequest<I>::validate_copy_points() {
  std::shared_lock image_locker{m_src_image_ctx->image_lock};

  if (m_src_snap_id_start != 0 &&
      m_src_image_ctx->snap_info.find(m_src_snap_id_start) ==
      m_src_image_ctx->snap_info.end()) {
    lderr(m_cct) << "invalid start snap_id " << m_src_snap_id_start << dendl;
    return -EINVAL;
  }

  if (m_src_snap_id_end != CEPH_NOSNAP &&
      m_src_image_ctx->snap_info.find(m_src_snap_id_end) ==
      m_src_image_ctx->snap_info.end()) {
    lderr(m_cct) << "invalid end snap_id " << m_src_snap_id_end << dendl;
    return -EINVAL;
  }

  return 0;
}

template <typename I>
void DeepCopyRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  put();
}

} // namespace librbd

template class librbd::DeepCopyRequest<librbd::ImageCtx>;
