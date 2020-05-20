// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_types.h"
#include "librbd/operation/SnapshotCreateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/mirror/snapshot/SetImageStateRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotCreateRequest: "

namespace librbd {
namespace operation {

using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
SnapshotCreateRequest<I>::SnapshotCreateRequest(I &image_ctx,
                                                Context *on_finish,
						const cls::rbd::SnapshotNamespace &snap_namespace,
                                                const std::string &snap_name,
                                                uint64_t journal_op_tid,
                                                uint64_t flags,
                                                ProgressContext &prog_ctx)
  : Request<I>(image_ctx, on_finish, journal_op_tid),
    m_snap_namespace(snap_namespace), m_snap_name(snap_name),
    m_skip_object_map(flags & SNAP_CREATE_FLAG_SKIP_OBJECT_MAP),
    m_prog_ctx(prog_ctx) {
}

template <typename I>
void SnapshotCreateRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if (!image_ctx.data_ctx.is_valid()) {
    lderr(cct) << "missing data pool" << dendl;
    this->async_complete(-ENODEV);
    return;
  }

  send_notify_quiesce();
}

template <typename I>
void SnapshotCreateRequest<I>::send_notify_quiesce() {
  I &image_ctx = this->m_image_ctx;

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_request_id = image_ctx.image_watcher->notify_quiesce(
    m_prog_ctx, create_async_context_callback(
      image_ctx, create_context_callback<SnapshotCreateRequest<I>,
      &SnapshotCreateRequest<I>::handle_notify_quiesce>(this)));
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_notify_quiesce(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to notify quiesce: " << cpp_strerror(*result)
               << dendl;
    return this->create_context_finisher(*result);
  }

  send_suspend_requests();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_suspend_requests() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  // TODO suspend (shrink) resize to ensure consistent RBD mirror
  send_suspend_aio();
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_suspend_requests(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  // TODO
  send_suspend_aio();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_suspend_aio() {
  I &image_ctx = this->m_image_ctx;
  std::shared_lock owner_locker{image_ctx.owner_lock};

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  image_ctx.io_image_dispatcher->block_writes(create_context_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_suspend_aio>(this));
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_suspend_aio(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*result) << dendl;
    save_result(result);
    send_notify_unquiesce();
    return nullptr;
  }

  send_append_op_event();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_append_op_event() {
  I &image_ctx = this->m_image_ctx;
  if (!this->template append_op_event<
        SnapshotCreateRequest<I>,
        &SnapshotCreateRequest<I>::handle_append_op_event>(this)) {
    send_allocate_snap_id();
    return;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_append_op_event(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to commit journal entry: " << cpp_strerror(*result)
               << dendl;
    save_result(result);
    send_notify_unquiesce();
    return nullptr;
  }

  send_allocate_snap_id();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_allocate_snap_id() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  librados::AioCompletion *rados_completion = create_rados_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_allocate_snap_id>(this);
  image_ctx.data_ctx.aio_selfmanaged_snap_create(&m_snap_id, rados_completion);
  rados_completion->release();
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_allocate_snap_id(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << ", "
                << "snap_id=" << m_snap_id << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to allocate snapshot id: " << cpp_strerror(*result)
               << dendl;
    save_result(result);
    send_notify_unquiesce();
    return nullptr;
  }

  send_create_snap();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_create_snap() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  std::shared_lock owner_locker{image_ctx.owner_lock};
  std::shared_lock image_locker{image_ctx.image_lock};

  // should have been canceled prior to releasing lock
  ceph_assert(image_ctx.exclusive_lock == nullptr ||
              image_ctx.exclusive_lock->is_lock_owner());

  // save current size / parent info for creating snapshot record in ImageCtx
  m_size = image_ctx.size;
  m_parent_info = image_ctx.parent_md;

  librados::ObjectWriteOperation op;
  if (image_ctx.old_format) {
    cls_client::old_snapshot_add(&op, m_snap_id, m_snap_name);
  } else {
    cls_client::snapshot_add(&op, m_snap_id, m_snap_name, m_snap_namespace);
  }

  librados::AioCompletion *rados_completion = create_rados_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_create_snap>(this);
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid,
                                       rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_create_snap(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == -ESTALE) {
    send_allocate_snap_id();
    return nullptr;
  } else if (*result < 0) {
    save_result(result);
    send_release_snap_id();
    return nullptr;
  }

  return send_create_object_map();
}

template <typename I>
Context *SnapshotCreateRequest<I>::send_create_object_map() {
  I &image_ctx = this->m_image_ctx;

  image_ctx.image_lock.lock_shared();
  if (image_ctx.object_map == nullptr || m_skip_object_map) {
    image_ctx.image_lock.unlock_shared();

    return send_create_image_state();
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  image_ctx.object_map->snapshot_add(
    m_snap_id, create_context_callback<
      SnapshotCreateRequest<I>,
      &SnapshotCreateRequest<I>::handle_create_object_map>(this));
  image_ctx.image_lock.unlock_shared();
  return nullptr;
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_create_object_map(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << this << " " << __func__ << ": failed to snapshot object map: "
               << cpp_strerror(*result) << dendl;

    save_result(result);
    update_snap_context();
    send_notify_unquiesce();
    return nullptr;
  }

  return send_create_image_state();
}

template <typename I>
Context *SnapshotCreateRequest<I>::send_create_image_state() {
  I &image_ctx = this->m_image_ctx;
  auto mirror_ns = boost::get<cls::rbd::MirrorSnapshotNamespace>(
    &m_snap_namespace);
  if (mirror_ns == nullptr || !mirror_ns->is_primary()) {
    update_snap_context();
    send_notify_unquiesce();
    return nullptr;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  auto req = mirror::snapshot::SetImageStateRequest<I>::create(
      &image_ctx, m_snap_id, create_context_callback<
      SnapshotCreateRequest<I>,
      &SnapshotCreateRequest<I>::handle_create_image_state>(this));
  req->send();
  return nullptr;
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_create_image_state(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  update_snap_context();
  if (*result < 0) {
    lderr(cct) << this << " " << __func__ << ": failed to snapshot object map: "
               << cpp_strerror(*result) << dendl;
    save_result(result);
  }

  send_notify_unquiesce();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_release_snap_id() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  ceph_assert(m_snap_id != CEPH_NOSNAP);

  librados::AioCompletion *rados_completion = create_rados_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_release_snap_id>(this);
  image_ctx.data_ctx.aio_selfmanaged_snap_remove(m_snap_id, rados_completion);
  rados_completion->release();
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_release_snap_id(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  send_notify_unquiesce();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_notify_unquiesce() {
  I &image_ctx = this->m_image_ctx;

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  image_ctx.io_image_dispatcher->unblock_writes();

  image_ctx.image_watcher->notify_unquiesce(
    m_request_id, create_context_callback<
      SnapshotCreateRequest<I>,
      &SnapshotCreateRequest<I>::handle_notify_unquiesce>(this));
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_notify_unquiesce(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to notify unquiesce: " << cpp_strerror(*result)
               << dendl;
    // ignore error
  }

  *result = m_ret_val;
  return this->create_context_finisher(m_ret_val);
}

template <typename I>
void SnapshotCreateRequest<I>::update_snap_context() {
  I &image_ctx = this->m_image_ctx;

  std::shared_lock owner_locker{image_ctx.owner_lock};
  std::unique_lock image_locker{image_ctx.image_lock};
  if (image_ctx.old_format) {
    return;
  }

  if (image_ctx.get_snap_info(m_snap_id) != NULL) {
    return;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  // should have been canceled prior to releasing lock
  ceph_assert(image_ctx.exclusive_lock == nullptr ||
              image_ctx.exclusive_lock->is_lock_owner());

  // immediately add a reference to the new snapshot
  utime_t snap_time = ceph_clock_now();
  image_ctx.add_snap(m_snap_namespace, m_snap_name, m_snap_id, m_size,
		     m_parent_info, RBD_PROTECTION_STATUS_UNPROTECTED,
		     0, snap_time);

  // immediately start using the new snap context if we
  // own the exclusive lock
  std::vector<snapid_t> snaps;
  snaps.push_back(m_snap_id);
  snaps.insert(snaps.end(), image_ctx.snapc.snaps.begin(),
               image_ctx.snapc.snaps.end());

  image_ctx.snapc.seq = m_snap_id;
  image_ctx.snapc.snaps.swap(snaps);
  image_ctx.data_ctx.selfmanaged_snap_set_write_ctx(
    image_ctx.snapc.seq, image_ctx.snaps);

  if (!image_ctx.migration_info.empty()) {
    auto it = image_ctx.migration_info.snap_map.find(CEPH_NOSNAP);
    ceph_assert(it != image_ctx.migration_info.snap_map.end());
    ceph_assert(!it->second.empty());
    if (it->second[0] == CEPH_NOSNAP) {
      ldout(cct, 5) << this << " " << __func__
                    << ": updating migration snap_map" << dendl;
      it->second[0] = m_snap_id;
    }
  }
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotCreateRequest<librbd::ImageCtx>;
