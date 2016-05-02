// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/RefreshRequest.h"
#include "include/stringify.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/image/RefreshParentRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::RefreshRequest: "

namespace librbd {
namespace image {

using util::create_rados_ack_callback;
using util::create_async_context_callback;
using util::create_context_callback;

template <typename I>
RefreshRequest<I>::RefreshRequest(I &image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx),
    m_on_finish(create_async_context_callback(m_image_ctx, on_finish)),
    m_error_result(0), m_flush_aio(false), m_exclusive_lock(nullptr),
    m_object_map(nullptr), m_journal(nullptr), m_refresh_parent(nullptr) {
}

template <typename I>
RefreshRequest<I>::~RefreshRequest() {
  // these require state machine to close
  assert(m_exclusive_lock == nullptr);
  assert(m_object_map == nullptr);
  assert(m_journal == nullptr);
  assert(m_refresh_parent == nullptr);
  assert(!m_blocked_writes);
}

template <typename I>
void RefreshRequest<I>::send() {
  if (m_image_ctx.old_format) {
    send_v1_read_header();
  } else {
    send_v2_get_mutable_metadata();
  }
}

template <typename I>
void RefreshRequest<I>::send_v1_read_header() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  op.read(0, 0, nullptr, nullptr);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v1_read_header>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_read_header(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": " << "r=" << *result << dendl;

  rbd_obj_header_ondisk v1_header;
  if (*result < 0) {
    return m_on_finish;
  } else if (m_out_bl.length() < sizeof(v1_header)) {
    lderr(cct) << "v1 header too small" << dendl;
    *result = -EIO;
    return m_on_finish;
  } else if (memcmp(RBD_HEADER_TEXT, m_out_bl.c_str(),
                    sizeof(RBD_HEADER_TEXT)) != 0) {
    lderr(cct) << "unrecognized v1 header" << dendl;
    *result = -ENXIO;
    return m_on_finish;
  }

  memcpy(&v1_header, m_out_bl.c_str(), sizeof(v1_header));
  m_order = v1_header.options.order;
  m_size = v1_header.image_size;
  m_object_prefix = v1_header.block_name;
  send_v1_get_snapshots();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v1_get_snapshots() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::old_snapshot_list_start(&op);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v1_get_snapshots>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_get_snapshots(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": " << "r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::old_snapshot_list_finish(
      &it, &m_snap_names, &m_snap_sizes, &m_snapc);
  }

  if (*result < 0) {
    lderr(cct) << "failed to retrieve v1 snapshots: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  if (!m_snapc.is_valid()) {
    lderr(cct) << "v1 image snap context is invalid" << dendl;
    *result = -EIO;
    return m_on_finish;
  }

  send_v1_get_locks();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v1_get_locks() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  rados::cls::lock::get_lock_info_start(&op, RBD_LOCK_NAME);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v1_get_locks>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_get_locks(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  // If EOPNOTSUPP, treat image as if there are no locks (we can't
  // query them).
  if (*result == -EOPNOTSUPP) {
    *result = 0;
  } else if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    ClsLockType lock_type;
    *result = rados::cls::lock::get_lock_info_finish(&it, &m_lockers,
                                                     &lock_type, &m_lock_tag);
    if (*result == 0) {
      m_exclusive_locked = (lock_type == LOCK_EXCLUSIVE);
    }
  }
  if (*result < 0) {
    lderr(cct) << "failed to retrieve locks: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v1_apply();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v1_apply() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // ensure we are not in a rados callback when applying updates
  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v1_apply>(this);
  m_image_ctx.op_work_queue->queue(ctx, 0);
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_apply(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  apply();
  return send_flush_aio();
}

template <typename I>
void RefreshRequest<I>::send_v2_get_mutable_metadata() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  uint64_t snap_id;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    snap_id = m_image_ctx.snap_id;
  }

  bool read_only = m_image_ctx.read_only || snap_id != CEPH_NOSNAP;
  librados::ObjectReadOperation op;
  cls_client::get_mutable_metadata_start(&op, read_only);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_mutable_metadata>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_mutable_metadata(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::get_mutable_metadata_finish(&it, &m_size, &m_features,
                                                      &m_incompatible_features,
                                                      &m_lockers,
                                                      &m_exclusive_locked,
                                                      &m_lock_tag, &m_snapc,
                                                      &m_parent_md);
  }
  if (*result < 0) {
    lderr(cct) << "failed to retrieve mutable metadata: "
               << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  uint64_t unsupported = m_incompatible_features & ~RBD_FEATURES_ALL;
  if (unsupported != 0ULL) {
    lderr(cct) << "Image uses unsupported features: " << unsupported << dendl;
    *result = -ENOSYS;
    return m_on_finish;
  }

  if (!m_snapc.is_valid()) {
    lderr(cct) << "image snap context is invalid!" << dendl;
    *result = -EIO;
    return m_on_finish;
  }

  send_v2_get_flags();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_flags() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_flags_start(&op, m_snapc.snaps);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_flags>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_flags(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    cls_client::get_flags_finish(&it, &m_flags, m_snapc.snaps, &m_snap_flags);
  }
  if (*result == -EOPNOTSUPP) {
    // Older OSD doesn't support RBD flags, need to assume the worst
    *result = 0;
    ldout(cct, 10) << "OSD does not support RBD flags, disabling object map "
                   << "optimizations" << dendl;
    m_flags = RBD_FLAG_OBJECT_MAP_INVALID;
    if ((m_features & RBD_FEATURE_FAST_DIFF) != 0) {
      m_flags |= RBD_FLAG_FAST_DIFF_INVALID;
    }

    std::vector<uint64_t> default_flags(m_snapc.snaps.size(), m_flags);
    m_snap_flags = std::move(default_flags);
  } else if (*result == -ENOENT) {
    ldout(cct, 10) << "out-of-sync snapshot state detected" << dendl;
    send_v2_get_mutable_metadata();
    return nullptr;
  } else if (*result < 0) {
    lderr(cct) << "failed to retrieve flags: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v2_get_snapshots();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_snapshots() {
  if (m_snapc.snaps.empty()) {
    m_snap_names.clear();
    m_snap_sizes.clear();
    m_snap_parents.clear();
    m_snap_protection.clear();
    send_v2_refresh_parent();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::snapshot_list_start(&op, m_snapc.snaps);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_snapshots>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_snapshots(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::snapshot_list_finish(&it, m_snapc.snaps,
                                               &m_snap_names, &m_snap_sizes,
                                               &m_snap_parents,
                                               &m_snap_protection);
  }
  if (*result == -ENOENT) {
    ldout(cct, 10) << "out-of-sync snapshot state detected" << dendl;
    send_v2_get_mutable_metadata();
    return nullptr;
  } else if (*result < 0) {
    lderr(cct) << "failed to retrieve snapshots: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v2_refresh_parent();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_refresh_parent() {
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    RWLock::RLocker parent_locker(m_image_ctx.parent_lock);

    parent_info parent_md;
    int r = get_parent_info(m_image_ctx.snap_id, &parent_md);
    if (r < 0 ||
        RefreshParentRequest<I>::is_refresh_required(m_image_ctx, parent_md)) {
      CephContext *cct = m_image_ctx.cct;
      ldout(cct, 10) << this << " " << __func__ << dendl;

      using klass = RefreshRequest<I>;
      Context *ctx = create_context_callback<
        klass, &klass::handle_v2_refresh_parent>(this);
      m_refresh_parent = RefreshParentRequest<I>::create(
        m_image_ctx, parent_md, ctx);
    }
  }

  if (m_refresh_parent != nullptr) {
    m_refresh_parent->send();
  } else {
    send_v2_init_exclusive_lock();
  }
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_refresh_parent(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to refresh parent image: " << cpp_strerror(*result)
               << dendl;
    save_result(result);
    send_v2_apply();
    return nullptr;
  }

  send_v2_init_exclusive_lock();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_init_exclusive_lock() {
  if ((m_features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0 ||
      m_image_ctx.read_only || !m_image_ctx.snap_name.empty() ||
      m_image_ctx.exclusive_lock != nullptr) {
    send_v2_open_object_map();
    return;
  }

  // implies exclusive lock dynamically enabled or image open in-progress
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // TODO need safe shut down
  m_exclusive_lock = m_image_ctx.create_exclusive_lock();

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_init_exclusive_lock>(this);

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  m_exclusive_lock->init(m_features, ctx);
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_init_exclusive_lock(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to initialize exclusive lock: "
               << cpp_strerror(*result) << dendl;
    save_result(result);
  }

  // object map and journal will be opened when exclusive lock is
  // acquired (if features are enabled)
  send_v2_apply();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_open_journal() {
  if ((m_features & RBD_FEATURE_JOURNALING) == 0 ||
      m_image_ctx.read_only ||
      !m_image_ctx.snap_name.empty() ||
      m_image_ctx.journal != nullptr ||
      m_image_ctx.exclusive_lock == nullptr ||
      !m_image_ctx.exclusive_lock->is_lock_owner()) {

    // journal dynamically enabled -- doesn't own exclusive lock
    if ((m_features & RBD_FEATURE_JOURNALING) != 0 &&
        m_image_ctx.exclusive_lock != nullptr &&
        m_image_ctx.journal == nullptr) {
      m_image_ctx.aio_work_queue->set_require_lock_on_read();
    }
    send_v2_block_writes();
    return;
  }

  // implies journal dynamically enabled since ExclusiveLock will init
  // the journal upon acquiring the lock
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_open_journal>(this);

  // TODO need safe close
  m_journal = m_image_ctx.create_journal();
  m_journal->open(ctx);
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_open_journal(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to initialize journal: " << cpp_strerror(*result)
               << dendl;
    save_result(result);
  }

  send_v2_block_writes();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_block_writes() {
  bool disabled_journaling = false;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    disabled_journaling = ((m_features & RBD_FEATURE_EXCLUSIVE_LOCK) != 0 &&
                           (m_features & RBD_FEATURE_JOURNALING) == 0 &&
                           m_image_ctx.journal != nullptr);
  }

  if (!disabled_journaling) {
    send_v2_apply();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // we need to block writes temporarily to avoid in-flight journal
  // writes
  m_blocked_writes = true;
  Context *ctx = create_context_callback<
    RefreshRequest<I>, &RefreshRequest<I>::handle_v2_block_writes>(this);

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  m_image_ctx.aio_work_queue->block_writes(ctx);
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_block_writes(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*result)
               << dendl;
    save_result(result);
  }
  send_v2_apply();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_open_object_map() {
  if ((m_features & RBD_FEATURE_OBJECT_MAP) == 0 ||
      m_image_ctx.object_map != nullptr ||
      (m_image_ctx.snap_name.empty() &&
       (m_image_ctx.read_only ||
        m_image_ctx.exclusive_lock == nullptr ||
        !m_image_ctx.exclusive_lock->is_lock_owner()))) {
    send_v2_open_journal();
    return;
  }

  // implies object map dynamically enabled or image open in-progress
  // since SetSnapRequest loads the object map for a snapshot and
  // ExclusiveLock loads the object map for HEAD
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  if (m_image_ctx.snap_name.empty()) {
    m_object_map = m_image_ctx.create_object_map(CEPH_NOSNAP);
  } else {
    for (size_t snap_idx = 0; snap_idx < m_snap_names.size(); ++snap_idx) {
      if (m_snap_names[snap_idx] == m_image_ctx.snap_name) {
        m_object_map = m_image_ctx.create_object_map(
          m_snapc.snaps[snap_idx].val);
        break;
      }
    }

    if (m_object_map == nullptr) {
      lderr(cct) << "failed to locate snapshot: " << m_image_ctx.snap_name
                 << dendl;
      send_v2_open_journal();
      return;
    }
  }

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_open_object_map>(this);
  m_object_map->open(ctx);
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_open_object_map(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  assert(*result == 0);
  send_v2_open_journal();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_apply() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // ensure we are not in a rados callback when applying updates
  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_apply>(this);
  m_image_ctx.op_work_queue->queue(ctx, 0);
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_apply(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  apply();
  return send_v2_finalize_refresh_parent();
}

template <typename I>
Context *RefreshRequest<I>::send_v2_finalize_refresh_parent() {
  if (m_refresh_parent == nullptr) {
    return send_v2_shut_down_exclusive_lock();
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_finalize_refresh_parent>(this);
  m_refresh_parent->finalize(ctx);
  return nullptr;
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_finalize_refresh_parent(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  assert(m_refresh_parent != nullptr);
  delete m_refresh_parent;
  m_refresh_parent = nullptr;

  return send_v2_shut_down_exclusive_lock();
}

template <typename I>
Context *RefreshRequest<I>::send_v2_shut_down_exclusive_lock() {
  if (m_exclusive_lock == nullptr) {
    return send_v2_close_journal();
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // exclusive lock feature was dynamically disabled. in-flight IO will be
  // flushed and in-flight requests will be canceled before releasing lock
  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_shut_down_exclusive_lock>(this);
  m_exclusive_lock->shut_down(ctx);
  return nullptr;
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_shut_down_exclusive_lock(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to shut down exclusive lock: "
               << cpp_strerror(*result) << dendl;
    save_result(result);
  }

  {
    RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
    assert(m_image_ctx.exclusive_lock == nullptr);
  }

  assert(m_exclusive_lock != nullptr);
  delete m_exclusive_lock;
  m_exclusive_lock = nullptr;

  return send_v2_close_journal();
}

template <typename I>
Context *RefreshRequest<I>::send_v2_close_journal() {
  if (m_journal == nullptr) {
    return send_v2_close_object_map();
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // journal feature was dynamically disabled
  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_close_journal>(this);
  m_journal->close(ctx);
  return nullptr;
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_close_journal(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    save_result(result);
    lderr(cct) << "failed to close journal: " << cpp_strerror(*result)
               << dendl;
  }

  assert(m_journal != nullptr);
  delete m_journal;
  m_journal = nullptr;

  assert(m_blocked_writes);
  m_blocked_writes = false;

  m_image_ctx.aio_work_queue->unblock_writes();
  return send_v2_close_object_map();
}

template <typename I>
Context *RefreshRequest<I>::send_v2_close_object_map() {
  if (m_object_map == nullptr) {
    return send_flush_aio();
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // object map was dynamically disabled
  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_close_object_map>(this);
  m_object_map->close(ctx);
  return nullptr;
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_close_object_map(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  assert(*result == 0);
  assert(m_object_map != nullptr);
  delete m_object_map;
  m_object_map = nullptr;

  return send_flush_aio();
}

template <typename I>
Context *RefreshRequest<I>::send_flush_aio() {
  if (m_flush_aio) {
    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 10) << this << " " << __func__ << dendl;

    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    using klass = RefreshRequest<I>;
    Context *ctx = create_context_callback<
      klass, &klass::handle_flush_aio>(this);
    m_image_ctx.flush(ctx);
    return nullptr;
  } else if (m_error_result < 0) {
    // propagate saved error back to caller
    Context *ctx = create_context_callback<
      RefreshRequest<I>, &RefreshRequest<I>::handle_error>(this);
    m_image_ctx.op_work_queue->queue(ctx, 0);
    return nullptr;
  }

  return m_on_finish;
}

template <typename I>
Context *RefreshRequest<I>::handle_flush_aio(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to flush pending AIO: " << cpp_strerror(*result)
               << dendl;
  }

  return handle_error(result);
}

template <typename I>
Context *RefreshRequest<I>::handle_error(int *result) {
  if (m_error_result < 0) {
    *result = m_error_result;

    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;
  }
  return m_on_finish;
}

template <typename I>
void RefreshRequest<I>::apply() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
  RWLock::WLocker md_locker(m_image_ctx.md_lock);

  {
    Mutex::Locker cache_locker(m_image_ctx.cache_lock);
    RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
    RWLock::WLocker parent_locker(m_image_ctx.parent_lock);

    m_image_ctx.size = m_size;
    m_image_ctx.lockers = m_lockers;
    m_image_ctx.lock_tag = m_lock_tag;
    m_image_ctx.exclusive_locked = m_exclusive_locked;

    if (m_image_ctx.old_format) {
      m_image_ctx.order = m_order;
      m_image_ctx.features = 0;
      m_image_ctx.flags = 0;
      m_image_ctx.object_prefix = std::move(m_object_prefix);
      m_image_ctx.init_layout();
    } else {
      m_image_ctx.features = m_features;
      m_image_ctx.flags = m_flags;
      m_image_ctx.parent_md = m_parent_md;
    }

    for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
      std::vector<librados::snap_t>::const_iterator it = std::find(
        m_image_ctx.snaps.begin(), m_image_ctx.snaps.end(),
        m_snapc.snaps[i].val);
      if (it == m_image_ctx.snaps.end()) {
        m_flush_aio = true;
        ldout(cct, 20) << "new snapshot id=" << m_snapc.snaps[i].val
                       << " name=" << m_snap_names[i]
                       << " size=" << m_snap_sizes[i]
                       << dendl;
      }
    }

    m_image_ctx.snaps.clear();
    m_image_ctx.snap_info.clear();
    m_image_ctx.snap_ids.clear();
    for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
      uint64_t flags = m_image_ctx.old_format ? 0 : m_snap_flags[i];
      uint8_t protection_status = m_image_ctx.old_format ?
        static_cast<uint8_t>(RBD_PROTECTION_STATUS_UNPROTECTED) :
        m_snap_protection[i];
      parent_info parent;
      if (!m_image_ctx.old_format) {
        parent = m_snap_parents[i];
      }

      m_image_ctx.add_snap(m_snap_names[i], m_snapc.snaps[i].val,
                           m_snap_sizes[i], parent, protection_status, flags);
    }
    m_image_ctx.snapc = m_snapc;

    if (m_image_ctx.snap_id != CEPH_NOSNAP &&
        m_image_ctx.get_snap_id(m_image_ctx.snap_name) != m_image_ctx.snap_id) {
      lderr(cct) << "tried to read from a snapshot that no longer exists: "
                 << m_image_ctx.snap_name << dendl;
      m_image_ctx.snap_exists = false;
    }

    if (m_refresh_parent != nullptr) {
      m_refresh_parent->apply();
    }
    m_image_ctx.data_ctx.selfmanaged_snap_set_write_ctx(m_image_ctx.snapc.seq,
                                                        m_image_ctx.snaps);

    // handle dynamically enabled / disabled features
    if (m_image_ctx.exclusive_lock != nullptr &&
        !m_image_ctx.test_features(RBD_FEATURE_EXCLUSIVE_LOCK,
                                   m_image_ctx.snap_lock)) {
      // disabling exclusive lock will automatically handle closing
      // object map and journaling
      assert(m_exclusive_lock == nullptr);
      m_exclusive_lock = m_image_ctx.exclusive_lock;
    } else {
      if (m_exclusive_lock != nullptr) {
        assert(m_image_ctx.exclusive_lock == nullptr);
        std::swap(m_exclusive_lock, m_image_ctx.exclusive_lock);
      }
      if (!m_image_ctx.test_features(RBD_FEATURE_JOURNALING,
                                     m_image_ctx.snap_lock)) {
        if (m_image_ctx.journal != nullptr) {
          m_image_ctx.aio_work_queue->clear_require_lock_on_read();
        }
        std::swap(m_journal, m_image_ctx.journal);
      } else if (m_journal != nullptr) {
        std::swap(m_journal, m_image_ctx.journal);
      }
      if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                     m_image_ctx.snap_lock) ||
          m_object_map != nullptr) {
        std::swap(m_object_map, m_image_ctx.object_map);
      }
    }
  }
}

template <typename I>
int RefreshRequest<I>::get_parent_info(uint64_t snap_id,
                                       parent_info *parent_md) {
  if (snap_id == CEPH_NOSNAP) {
    *parent_md = m_parent_md;
    return 0;
  } else {
    for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
      if (m_snapc.snaps[i].val == snap_id) {
        *parent_md = m_snap_parents[i];
        return 0;
      }
    }
  }
  return -ENOENT;
}

} // namespace image
} // namespace librbd

template class librbd::image::RefreshRequest<librbd::ImageCtx>;
