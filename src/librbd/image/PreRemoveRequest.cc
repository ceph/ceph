// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/PreRemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/StandardPolicy.h"
#include "librbd/image/ListWatchersRequest.h"
#include "librbd/journal/DisabledPolicy.h"
#include "librbd/operation/SnapshotRemoveRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::PreRemoveRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace image {

namespace {

bool auto_delete_snapshot(const SnapInfo& snap_info) {
  auto snap_namespace_type = cls::rbd::get_snap_namespace_type(
    snap_info.snap_namespace);
  switch (snap_namespace_type) {
  case cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH:
    return true;
  default:
    return false;
  }
}

bool ignore_snapshot(const SnapInfo& snap_info) {
  auto snap_namespace_type = cls::rbd::get_snap_namespace_type(
    snap_info.snap_namespace);
  switch (snap_namespace_type) {
  case cls::rbd::SNAPSHOT_NAMESPACE_TYPE_MIRROR:
    return true;
  default:
    return false;
  }
}

} // anonymous namespace

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
void PreRemoveRequest<I>::send() {
  auto cct = m_image_ctx->cct;
  if (m_image_ctx->operations_disabled) {
    lderr(cct) << "image operations disabled due to unsupported op features"
               << dendl;
    finish(-EROFS);
    return;
  }

  acquire_exclusive_lock();
}

template <typename I>
void PreRemoveRequest<I>::acquire_exclusive_lock() {
  // lock for write for set_exclusive_lock_policy()
  std::unique_lock owner_locker{m_image_ctx->owner_lock};
  if (m_image_ctx->exclusive_lock == nullptr) {
    owner_locker.unlock();
    validate_image_removal();
    return;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  // refuse to release exclusive lock when (in the midst of) removing
  // the image
  m_image_ctx->set_exclusive_lock_policy(
    new exclusive_lock::StandardPolicy<I>(m_image_ctx));

  // do not attempt to open the journal when removing the image in case
  // it's corrupt
  if (m_image_ctx->test_features(RBD_FEATURE_JOURNALING)) {
    std::unique_lock image_locker{m_image_ctx->image_lock};
    m_image_ctx->set_journal_policy(new journal::DisabledPolicy());
  }

  m_exclusive_lock = m_image_ctx->exclusive_lock;

  auto ctx = create_context_callback<
    PreRemoveRequest<I>,
    &PreRemoveRequest<I>::handle_exclusive_lock>(this, m_exclusive_lock);
  m_exclusive_lock->acquire_lock(ctx);
}

template <typename I>
void PreRemoveRequest<I>::handle_exclusive_lock(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0 || !m_image_ctx->exclusive_lock->is_lock_owner()) {
    if (!m_force) {
      lderr(cct) << "cannot obtain exclusive lock - not removing" << dendl;
      finish(-EBUSY);
    } else {
      ldout(cct, 5) << "cannot obtain exclusive lock - "
                    << "proceeding due to force flag set" << dendl;
      shut_down_exclusive_lock();
    }
    return;
  }

  validate_image_removal();
}

template <typename I>
void PreRemoveRequest<I>::shut_down_exclusive_lock() {
  std::shared_lock owner_locker{m_image_ctx->owner_lock};
  if (m_image_ctx->exclusive_lock == nullptr) {
    owner_locker.unlock();
    validate_image_removal();
    return;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  auto ctx = create_context_callback<
    PreRemoveRequest<I>,
    &PreRemoveRequest<I>::handle_shut_down_exclusive_lock>(this);

  m_exclusive_lock = m_image_ctx->exclusive_lock;
  m_exclusive_lock->shut_down(ctx);
}

template <typename I>
void PreRemoveRequest<I>::handle_shut_down_exclusive_lock(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "r=" << r << dendl;

  m_exclusive_lock->put();
  m_exclusive_lock = nullptr;

  if (r < 0) {
    lderr(cct) << "error shutting down exclusive lock: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  ceph_assert(m_image_ctx->exclusive_lock == nullptr);
  validate_image_removal();
}

template <typename I>
void PreRemoveRequest<I>::validate_image_removal() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  if (!m_image_ctx->ignore_migrating &&
      m_image_ctx->test_features(RBD_FEATURE_MIGRATING)) {
    lderr(cct) << "image in migration state - not removing" << dendl;
    finish(-EBUSY);
    return;
  }

  check_image_snaps();
}

template <typename I>
void PreRemoveRequest<I>::check_image_snaps() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  m_image_ctx->image_lock.lock_shared();
  for (auto& snap_info : m_image_ctx->snap_info) {
    if (auto_delete_snapshot(snap_info.second)) {
      m_snap_infos.insert(snap_info);
    } else if (!ignore_snapshot(snap_info.second)) {
      m_image_ctx->image_lock.unlock_shared();

      ldout(cct, 5) << "image has snapshots - not removing" << dendl;
      finish(-ENOTEMPTY);
      return;
    }
  }
  m_image_ctx->image_lock.unlock_shared();

  list_image_watchers();
}

template <typename I>
void PreRemoveRequest<I>::list_image_watchers() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  int flags = LIST_WATCHERS_FILTER_OUT_MY_INSTANCE |
              LIST_WATCHERS_FILTER_OUT_MIRROR_INSTANCES;
  auto ctx = create_context_callback<
    PreRemoveRequest<I>,
    &PreRemoveRequest<I>::handle_list_image_watchers>(this);
  auto req = ListWatchersRequest<I>::create(*m_image_ctx, flags, &m_watchers,
                                            ctx);
  req->send();
}

template <typename I>
void PreRemoveRequest<I>::handle_list_image_watchers(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "error listing image watchers: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  check_image_watchers();
}

template <typename I>
void PreRemoveRequest<I>::check_image_watchers() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  if (!m_watchers.empty()) {
    lderr(cct) << "image has watchers - not removing" << dendl;
    finish(-EBUSY);
    return;
  }

  check_group();
}

template <typename I>
void PreRemoveRequest<I>::check_group() {
  if (m_image_ctx->old_format) {
    finish(0);
    return;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::image_group_get_start(&op);

  auto rados_completion = create_rados_callback<
    PreRemoveRequest<I>, &PreRemoveRequest<I>::handle_check_group>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid,
                                          rados_completion, &op, &m_out_bl);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
void PreRemoveRequest<I>::handle_check_group(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "r=" << r << dendl;

  cls::rbd::GroupSpec s;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::image_group_get_finish(&it, &s);
  }
  if (r < 0 && r != -EOPNOTSUPP) {
    lderr(cct) << "error fetching group for image: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (s.is_valid()) {
    lderr(cct) << "image is in a group - not removing" << dendl;
    finish(-EMLINK);
    return;
  }

  remove_snapshot();
}

template <typename I>
void PreRemoveRequest<I>::remove_snapshot() {
  if (m_snap_infos.empty()) {
    finish(0);
    return;
  }

  auto cct = m_image_ctx->cct;
  auto snap_id = m_snap_infos.begin()->first;
  auto& snap_info = m_snap_infos.begin()->second;
  ldout(cct, 20) << "snap_id=" << snap_id << ", "
                 << "snap_name=" << snap_info.name << dendl;

  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  auto ctx = create_context_callback<
    PreRemoveRequest<I>, &PreRemoveRequest<I>::handle_remove_snapshot>(this);
  auto req = librbd::operation::SnapshotRemoveRequest<I>::create(
    *m_image_ctx, snap_info.snap_namespace, snap_info.name,
    snap_id, ctx);
  req->send();

}

template <typename I>
void PreRemoveRequest<I>::handle_remove_snapshot(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r == -EBUSY) {
    ldout(cct, 5) << "skipping attached child" << dendl;
    if (m_ret_val == 0) {
      m_ret_val = -ECHILD;
    }
  } else if (r < 0 && r != -ENOENT) {
    auto snap_id = m_snap_infos.begin()->first;
    lderr(cct) << "failed to auto-prune snapshot " << snap_id << ": "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  ceph_assert(!m_snap_infos.empty());
  m_snap_infos.erase(m_snap_infos.begin());

  remove_snapshot();
}

template <typename I>
void PreRemoveRequest<I>::finish(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (m_ret_val == 0) {
    m_ret_val = r;
  }

  m_on_finish->complete(m_ret_val);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::PreRemoveRequest<librbd::ImageCtx>;
