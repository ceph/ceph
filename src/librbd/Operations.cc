// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Operations.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/operation/FlattenRequest.h"
#include "librbd/operation/RebuildObjectMapRequest.h"
#include "librbd/operation/RenameRequest.h"
#include "librbd/operation/ResizeRequest.h"
#include "librbd/operation/SnapshotCreateRequest.h"
#include "librbd/operation/SnapshotProtectRequest.h"
#include "librbd/operation/SnapshotRemoveRequest.h"
#include "librbd/operation/SnapshotRenameRequest.h"
#include "librbd/operation/SnapshotRollbackRequest.h"
#include "librbd/operation/SnapshotUnprotectRequest.h"
#include <boost/bind.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Operations: "

namespace librbd {

template <typename I>
Operations<I>::Operations(I &image_ctx) : m_image_ctx(image_ctx) {
}

template <typename I>
int Operations<I>::flatten(ProgressContext &prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "flatten" << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  {
    RWLock::RLocker parent_locker(m_image_ctx.parent_lock);
    if (m_image_ctx.parent_md.spec.pool_id == -1) {
      lderr(cct) << "image has no parent" << dendl;
      return -EINVAL;
    }
  }

  uint64_t request_id = m_async_request_seq.inc();
  r = invoke_async_request("flatten", false,
                           boost::bind(&Operations<I>::flatten, this,
                                       boost::ref(prog_ctx), _1),
                           boost::bind(&ImageWatcher::notify_flatten,
                                       m_image_ctx.image_watcher, request_id,
                                       boost::ref(prog_ctx)));

  if (r < 0 && r != -EINVAL) {
    return r;
  }

  notify_change();
  ldout(cct, 20) << "flatten finished" << dendl;
  return 0;
}

template <typename I>
void Operations<I>::flatten(ProgressContext &prog_ctx, Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "flatten" << dendl;

  uint64_t object_size;
  uint64_t overlap_objects;
  ::SnapContext snapc;

  {
    uint64_t overlap;
    RWLock::RLocker l(m_image_ctx.snap_lock);
    RWLock::RLocker l2(m_image_ctx.parent_lock);

    if (m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }

    // can't flatten a non-clone
    if (m_image_ctx.parent_md.spec.pool_id == -1) {
      lderr(cct) << "image has no parent" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
    if (m_image_ctx.snap_id != CEPH_NOSNAP) {
      lderr(cct) << "snapshots cannot be flattened" << dendl;
      on_finish->complete(-EROFS);
      return;
    }

    snapc = m_image_ctx.snapc;
    assert(m_image_ctx.parent != NULL);
    int r = m_image_ctx.get_parent_overlap(CEPH_NOSNAP, &overlap);
    assert(r == 0);
    assert(overlap <= m_image_ctx.size);

    object_size = m_image_ctx.get_object_size();
    overlap_objects = Striper::get_num_objects(m_image_ctx.layout, overlap);
  }

  operation::FlattenRequest<I> *req = new operation::FlattenRequest<I>(
    m_image_ctx, on_finish, object_size, overlap_objects, snapc, prog_ctx);
  req->send();
}

template <typename I>
int Operations<I>::rebuild_object_map(ProgressContext &prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "rebuild_object_map" << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  uint64_t request_id = m_async_request_seq.inc();
  r = invoke_async_request("rebuild object map", true,
                           boost::bind(&Operations<I>::rebuild_object_map, this,
                                       boost::ref(prog_ctx), _1),
                           boost::bind(&ImageWatcher::notify_rebuild_object_map,
                                       m_image_ctx.image_watcher, request_id,
                                       boost::ref(prog_ctx)));

  ldout(cct, 10) << "rebuild object map finished" << dendl;
  if (r < 0) {
    return r;
  }

  notify_change();
  return 0;
}

template <typename I>
void Operations<I>::rebuild_object_map(ProgressContext &prog_ctx,
                                       Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  if (m_image_ctx.read_only) {
    on_finish->complete(-EROFS);
    return;
  }
  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
    on_finish->complete(-EINVAL);
    return;
  }

  operation::RebuildObjectMapRequest<I> *req =
    new operation::RebuildObjectMapRequest<I>(m_image_ctx, on_finish, prog_ctx);
  req->send();
}

template <typename I>
int Operations<I>::rename(const char *dstname) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": dest_name=" << dstname
                << dendl;

  int r = librbd::detect_format(m_image_ctx.md_ctx, dstname, NULL, NULL);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error checking for existing image called "
               << dstname << ":" << cpp_strerror(r) << dendl;
    return r;
  }
  if (r == 0) {
    lderr(cct) << "rbd image " << dstname << " already exists" << dendl;
    return -EEXIST;
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("rename", true,
                             boost::bind(&Operations<I>::rename, this,
                                         dstname, _1),
                             boost::bind(&ImageWatcher::notify_rename,
                                         m_image_ctx.image_watcher, dstname));
    if (r < 0 && r != -EEXIST) {
      return r;
    }
  } else {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    C_SaferCond cond_ctx;
    rename(dstname, &cond_ctx);

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }

  if (m_image_ctx.old_format) {
    notify_change();
  }
  return 0;
}

template <typename I>
void Operations<I>::rename(const char *dstname, Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    assert(m_image_ctx.exclusive_lock == nullptr ||
           m_image_ctx.exclusive_lock->is_lock_owner());
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": dest_name=" << dstname
                << dendl;

  operation::RenameRequest<I> *req =
    new operation::RenameRequest<I>(m_image_ctx, on_finish, dstname);
  req->send();
}

template <typename I>
int Operations<I>::resize(uint64_t size, ProgressContext& prog_ctx) {
  CephContext *cct = m_image_ctx.cct;

  m_image_ctx.snap_lock.get_read();
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "size=" << m_image_ctx.size << ", "
                << "new_size=" << size << dendl;
  m_image_ctx.snap_lock.put_read();

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  uint64_t request_id = m_async_request_seq.inc();
  r = invoke_async_request("resize", false,
                           boost::bind(&Operations<I>::resize, this,
                                       size, boost::ref(prog_ctx), _1, 0),
                           boost::bind(&ImageWatcher::notify_resize,
                                       m_image_ctx.image_watcher, request_id,
                                       size, boost::ref(prog_ctx)));

  m_image_ctx.perfcounter->inc(l_librbd_resize);
  notify_change();
  ldout(cct, 2) << "resize finished" << dendl;
  return r;
}

template <typename I>
void Operations<I>::resize(uint64_t size, ProgressContext &prog_ctx,
                           Context *on_finish, uint64_t journal_op_tid) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  m_image_ctx.snap_lock.get_read();
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "size=" << m_image_ctx.size << ", "
                << "new_size=" << size << dendl;
  m_image_ctx.snap_lock.put_read();

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  operation::ResizeRequest<I> *req = new operation::ResizeRequest<I>(
    m_image_ctx, on_finish, size, prog_ctx, journal_op_tid, false);
  req->send();
}

template <typename I>
int Operations<I>::snap_create(const char *snap_name) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0)
    return r;

  {
    RWLock::RLocker l(m_image_ctx.snap_lock);
    if (m_image_ctx.get_snap_id(snap_name) != CEPH_NOSNAP) {
      return -EEXIST;
    }
  }

  r = invoke_async_request("snap_create", true,
                           boost::bind(&Operations<I>::snap_create, this,
                                       snap_name, _1, 0),
                           boost::bind(&ImageWatcher::notify_snap_create,
                                       m_image_ctx.image_watcher, snap_name));
  if (r < 0 && r != -EEXIST) {
    return r;
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_create);
  notify_change();
  return 0;
}

template <typename I>
void Operations<I>::snap_create(const char *snap_name, Context *on_finish,
                                uint64_t journal_op_tid) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  operation::SnapshotCreateRequest<I> *req =
    new operation::SnapshotCreateRequest<I>(m_image_ctx, on_finish, snap_name,
                                            journal_op_tid);
  req->send();
}

template <typename I>
int Operations<I>::snap_rollback(const char *snap_name,
                                 ProgressContext& prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0)
    return r;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  {
    // need to drop snap_lock before invalidating cache
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (!m_image_ctx.snap_exists) {
      return -ENOENT;
    }

    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      return -EROFS;
    }

    uint64_t snap_id = m_image_ctx.get_snap_id(snap_name);
    if (snap_id == CEPH_NOSNAP) {
      lderr(cct) << "No such snapshot found." << dendl;
      return -ENOENT;
    }
  }

  r = prepare_image_update();
  if (r < 0) {
    return -EROFS;
  }
  if (m_image_ctx.exclusive_lock != nullptr &&
      !m_image_ctx.exclusive_lock->is_lock_owner()) {
    return -EROFS;
  }

  C_SaferCond cond_ctx;
  snap_rollback(snap_name, prog_ctx, &cond_ctx);
  r = cond_ctx.wait();
  if (r < 0) {
    return r;
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_rollback);
  notify_change();
  return r;
}

template <typename I>
void Operations<I>::snap_rollback(const char *snap_name,
                                  ProgressContext& prog_ctx,
                                  Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  uint64_t snap_id;
  uint64_t new_size;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    snap_id = m_image_ctx.get_snap_id(snap_name);
    if (snap_id == CEPH_NOSNAP) {
      lderr(cct) << "No such snapshot found." << dendl;
      on_finish->complete(-ENOENT);
      return;
    }

    new_size = m_image_ctx.get_image_size(snap_id);
  }

  // async mode used for journal replay
  operation::SnapshotRollbackRequest<I> *request =
    new operation::SnapshotRollbackRequest<I>(m_image_ctx, on_finish, snap_name,
                                              snap_id, new_size, prog_ctx);
  request->send();
}

template <typename I>
int Operations<I>::snap_remove(const char *snap_name) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only)
    return -EROFS;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0)
    return r;

  bool proxy_op = false;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.get_snap_id(snap_name) == CEPH_NOSNAP) {
      return -ENOENT;
    }
    proxy_op = ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0 ||
                (m_image_ctx.features & RBD_FEATURE_JOURNALING) != 0);
  }

  if (proxy_op) {
    r = invoke_async_request("snap_remove", true,
                             boost::bind(&Operations<I>::snap_remove, this,
                                         snap_name, _1),
                             boost::bind(&ImageWatcher::notify_snap_remove,
                                         m_image_ctx.image_watcher, snap_name));
    if (r < 0 && r != -ENOENT) {
      return r;
    }
  } else {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    C_SaferCond cond_ctx;
    snap_remove(snap_name, &cond_ctx);

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_remove);
  notify_change();
  return 0;
}

template <typename I>
void Operations<I>::snap_remove(const char *snap_name, Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  {
    if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
      assert(m_image_ctx.exclusive_lock == nullptr ||
             m_image_ctx.exclusive_lock->is_lock_owner());
    }
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  uint64_t snap_id;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    snap_id = m_image_ctx.get_snap_id(snap_name);
    if (snap_id == CEPH_NOSNAP) {
      lderr(m_image_ctx.cct) << "No such snapshot found." << dendl;
      on_finish->complete(-ENOENT);
      return;
    }

    bool is_protected;
    int r = m_image_ctx.is_snap_protected(snap_id, &is_protected);
    if (r < 0) {
      on_finish->complete(r);
      return;
    } else if (is_protected) {
      lderr(m_image_ctx.cct) << "snapshot is protected" << dendl;
      on_finish->complete(-EBUSY);
      return;
    }
  }

  operation::SnapshotRemoveRequest<I> *req =
    new operation::SnapshotRemoveRequest<I>(m_image_ctx, on_finish, snap_name,
                                            snap_id);
  req->send();
}

template <typename I>
int Operations<I>::snap_rename(const char *srcname, const char *dstname) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "snap_name=" << srcname << ", "
                << "new_snap_name=" << dstname << dendl;

  snapid_t snap_id;
  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0)
    return r;

  {
    RWLock::RLocker l(m_image_ctx.snap_lock);
    snap_id = m_image_ctx.get_snap_id(srcname);
    if (snap_id == CEPH_NOSNAP) {
      return -ENOENT;
    }
    if (m_image_ctx.get_snap_id(dstname) != CEPH_NOSNAP) {
      return -EEXIST;
    }
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("snap_rename", true,
                             boost::bind(&Operations<I>::snap_rename, this,
                                         snap_id, dstname, _1),
                             boost::bind(&ImageWatcher::notify_snap_rename,
                                         m_image_ctx.image_watcher, snap_id,
                                         dstname));
    if (r < 0 && r != -EEXIST) {
      return r;
    }
  } else {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    C_SaferCond cond_ctx;
    snap_rename(snap_id, dstname, &cond_ctx);

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_rename);
  notify_change();
  return 0;
}

template <typename I>
void Operations<I>::snap_rename(const uint64_t src_snap_id,
                                const char *dst_name, Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  if ((m_image_ctx.features & RBD_FEATURE_JOURNALING) != 0) {
    assert(m_image_ctx.exclusive_lock == nullptr ||
           m_image_ctx.exclusive_lock->is_lock_owner());
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "snap_id=" << src_snap_id << ", "
                << "new_snap_name=" << dst_name << dendl;

  operation::SnapshotRenameRequest<I> *req =
    new operation::SnapshotRenameRequest<I>(m_image_ctx, on_finish, src_snap_id,
                                            dst_name);
  req->send();
}

template <typename I>
int Operations<I>::snap_protect(const char *snap_name) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    bool is_protected;
    r = m_image_ctx.is_snap_protected(m_image_ctx.get_snap_id(snap_name),
                                      &is_protected);
    if (r < 0) {
      return r;
    }

    if (is_protected) {
      return -EBUSY;
    }
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("snap_protect", true,
                             boost::bind(&Operations<I>::snap_protect, this,
                                         snap_name, _1),
                             boost::bind(&ImageWatcher::notify_snap_protect,
                                         m_image_ctx.image_watcher, snap_name));
    if (r < 0 && r != -EBUSY) {
      return r;
    }
  } else {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    C_SaferCond cond_ctx;
    snap_protect(snap_name, &cond_ctx);

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }

  notify_change();
  return 0;
}

template <typename I>
void Operations<I>::snap_protect(const char *snap_name, Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    assert(m_image_ctx.exclusive_lock == nullptr ||
           m_image_ctx.exclusive_lock->is_lock_owner());
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  operation::SnapshotProtectRequest<I> *request =
    new operation::SnapshotProtectRequest<I>(m_image_ctx, on_finish, snap_name);
  request->send();
}

template <typename I>
int Operations<I>::snap_unprotect(const char *snap_name) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    bool is_unprotected;
    r = m_image_ctx.is_snap_unprotected(m_image_ctx.get_snap_id(snap_name),
                                  &is_unprotected);
    if (r < 0) {
      return r;
    }

    if (is_unprotected) {
      return -EINVAL;
    }
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("snap_unprotect", true,
                             boost::bind(&Operations<I>::snap_unprotect, this,
                                         snap_name, _1),
                             boost::bind(&ImageWatcher::notify_snap_unprotect,
                                         m_image_ctx.image_watcher, snap_name));
    if (r < 0 && r != -EINVAL) {
      return r;
    }
  } else {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    C_SaferCond cond_ctx;
    snap_unprotect(snap_name, &cond_ctx);

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }

  notify_change();
  return 0;
}

template <typename I>
void Operations<I>::snap_unprotect(const char *snap_name, Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    assert(m_image_ctx.exclusive_lock == nullptr ||
           m_image_ctx.exclusive_lock->is_lock_owner());
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  operation::SnapshotUnprotectRequest<I> *request =
    new operation::SnapshotUnprotectRequest<I>(m_image_ctx, on_finish,
                                               snap_name);
  request->send();
}

template <typename I>
int Operations<I>::prepare_image_update() {
  assert(m_image_ctx.owner_lock.is_locked() &&
         !m_image_ctx.owner_lock.is_wlocked());
  if (m_image_ctx.image_watcher == NULL) {
    return -EROFS;
  }

  // need to upgrade to a write lock
  int r = 0;
  bool trying_lock = false;
  C_SaferCond ctx;
  m_image_ctx.owner_lock.put_read();
  {
    RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
    if (m_image_ctx.exclusive_lock != nullptr &&
        !m_image_ctx.exclusive_lock->is_lock_owner()) {
      m_image_ctx.exclusive_lock->try_lock(&ctx);
      trying_lock = true;
    }
  }

  if (trying_lock) {
    r = ctx.wait();
  }
  m_image_ctx.owner_lock.get_read();

  return r;
}

template <typename I>
int Operations<I>::invoke_async_request(const std::string& request_type,
                                        bool permit_snapshot,
                                        const boost::function<void(Context*)>& local_request,
                                        const boost::function<int()>& remote_request) {
  CephContext *cct = m_image_ctx.cct;
  int r;
  do {
    C_SaferCond ctx;
    {
      RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
      {
        RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
        if (m_image_ctx.read_only ||
            (!permit_snapshot && m_image_ctx.snap_id != CEPH_NOSNAP)) {
          return -EROFS;
        }
      }

      while (m_image_ctx.exclusive_lock != nullptr) {
        r = prepare_image_update();
        if (r < 0) {
          return -EROFS;
        } else if (m_image_ctx.exclusive_lock->is_lock_owner()) {
          break;
        }

        r = remote_request();
        if (r != -ETIMEDOUT && r != -ERESTART) {
          return r;
        }
        ldout(cct, 5) << request_type << " timed out notifying lock owner"
                      << dendl;
      }

      local_request(&ctx);
    }

    r = ctx.wait();
    if (r == -ERESTART) {
      ldout(cct, 5) << request_type << " interrupted: restarting" << dendl;
    }
  } while (r == -ERESTART);
  return r;
}

template <typename I>
void Operations<I>::notify_change() {
  m_image_ctx.state->handle_update_notification();
  ImageWatcher::notify_header_update(m_image_ctx.md_ctx,
                                     m_image_ctx.header_oid);
}

} // namespace librbd

template class librbd::Operations<librbd::ImageCtx>;
