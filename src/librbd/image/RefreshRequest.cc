// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/ceph_assert.h"

#include "librbd/image/RefreshRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/deep_copy/Utils.h"
#include "librbd/image/GetMetadataRequest.h"
#include "librbd/image/RefreshParentRequest.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/journal/Policy.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::RefreshRequest: "

namespace librbd {
namespace image {

using util::create_rados_callback;
using util::create_async_context_callback;
using util::create_context_callback;

template <typename I>
RefreshRequest<I>::RefreshRequest(I &image_ctx, bool acquiring_lock,
                                  bool skip_open_parent, Context *on_finish)
  : m_image_ctx(image_ctx), m_acquiring_lock(acquiring_lock),
    m_skip_open_parent_image(skip_open_parent),
    m_on_finish(create_async_context_callback(m_image_ctx, on_finish)),
    m_error_result(0), m_flush_aio(false), m_exclusive_lock(nullptr),
    m_object_map(nullptr), m_journal(nullptr), m_refresh_parent(nullptr) {
  m_pool_metadata_io_ctx.dup(image_ctx.md_ctx);
  m_pool_metadata_io_ctx.set_namespace("");
}

template <typename I>
RefreshRequest<I>::~RefreshRequest() {
  // these require state machine to close
  ceph_assert(m_exclusive_lock == nullptr);
  ceph_assert(m_object_map == nullptr);
  ceph_assert(m_journal == nullptr);
  ceph_assert(m_refresh_parent == nullptr);
  ceph_assert(!m_blocked_writes);
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
void RefreshRequest<I>::send_get_migration_header() {
  if (m_image_ctx.ignore_migrating) {
    if (m_image_ctx.old_format) {
      send_v1_get_snapshots();
    } else {
      send_v2_get_metadata();
    }
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::migration_get_start(&op);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_migration_header>(this);
  m_out_bl.clear();
  m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                 &m_out_bl);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_get_migration_header(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    auto it = m_out_bl.cbegin();
    *result = cls_client::migration_get_finish(&it, &m_migration_spec);
  } else if (*result == -ENOENT) {
    ldout(cct, 5) << this << " " << __func__ << ": no migration header found"
                  << ", retrying" << dendl;
    send();
    return nullptr;
  }

  if (*result < 0) {
    lderr(cct) << "failed to retrieve migration header: "
               << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  switch(m_migration_spec.header_type) {
  case cls::rbd::MIGRATION_HEADER_TYPE_SRC:
    if (!m_read_only) {
      lderr(cct) << "image being migrated" << dendl;
      *result = -EROFS;
      return m_on_finish;
    }
    ldout(cct, 1) << this << " " << __func__ << ": migrating to: "
                  << m_migration_spec << dendl;
    break;
  case cls::rbd::MIGRATION_HEADER_TYPE_DST:
    ldout(cct, 1) << this << " " << __func__ << ": migrating from: "
                  << m_migration_spec << dendl;
    if (m_migration_spec.state != cls::rbd::MIGRATION_STATE_PREPARED &&
        m_migration_spec.state != cls::rbd::MIGRATION_STATE_EXECUTING &&
        m_migration_spec.state != cls::rbd::MIGRATION_STATE_EXECUTED) {
      ldout(cct, 5) << this << " " << __func__ << ": current migration state: "
                    << m_migration_spec.state << ", retrying" << dendl;
      send();
      return nullptr;
    }
    break;
  default:
    ldout(cct, 1) << this << " " << __func__ << ": migration type "
                  << m_migration_spec.header_type << dendl;
    *result = -EBADMSG;
    return m_on_finish;
  }

  if (m_image_ctx.old_format) {
    send_v1_get_snapshots();
  } else {
    send_v2_get_metadata();
  }
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v1_read_header() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  op.read(0, 0, nullptr, nullptr);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v1_read_header>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_read_header(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": " << "r=" << *result << dendl;

  rbd_obj_header_ondisk v1_header;
  bool migrating = false;
  if (*result < 0) {
    return m_on_finish;
  } else if (m_out_bl.length() < sizeof(v1_header)) {
    lderr(cct) << "v1 header too small" << dendl;
    *result = -EIO;
    return m_on_finish;
  } else if (memcmp(RBD_HEADER_TEXT, m_out_bl.c_str(),
                    sizeof(RBD_HEADER_TEXT)) != 0) {
    if (memcmp(RBD_MIGRATE_HEADER_TEXT, m_out_bl.c_str(),
               sizeof(RBD_MIGRATE_HEADER_TEXT)) == 0) {
      ldout(cct, 1) << this << " " << __func__ << ": migration v1 header detected"
                    << dendl;
      migrating = true;
    } else {
      lderr(cct) << "unrecognized v1 header" << dendl;
      *result = -ENXIO;
      return m_on_finish;
    }
  }

  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    m_read_only = m_image_ctx.read_only;
    m_read_only_flags = m_image_ctx.read_only_flags;
  }

  memcpy(&v1_header, m_out_bl.c_str(), sizeof(v1_header));
  m_order = v1_header.options.order;
  m_size = v1_header.image_size;
  m_object_prefix = v1_header.block_name;
  if (migrating) {
    send_get_migration_header();
  } else {
    send_v1_get_snapshots();
  }
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v1_get_snapshots() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::old_snapshot_list_start(&op);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v1_get_snapshots>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_get_snapshots(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": " << "r=" << *result << dendl;

  std::vector<std::string> snap_names;
  std::vector<uint64_t> snap_sizes;
  if (*result == 0) {
    auto it = m_out_bl.cbegin();
    *result = cls_client::old_snapshot_list_finish(&it, &snap_names,
                                                   &snap_sizes, &m_snapc);
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

  m_snap_infos.clear();
  for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
    m_snap_infos.push_back({m_snapc.snaps[i],
                            {cls::rbd::UserSnapshotNamespace{}},
                            snap_names[i], snap_sizes[i], {}, 0});
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
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v1_get_locks>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_get_locks(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  if (*result == 0) {
    auto it = m_out_bl.cbegin();
    ClsLockType lock_type;
    *result = rados::cls::lock::get_lock_info_finish(&it, &m_lockers,
                                                     &lock_type, &m_lock_tag);
    if (*result == 0) {
      m_exclusive_locked = (lock_type == ClsLockType::EXCLUSIVE);
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
    std::shared_lock image_locker{m_image_ctx.image_lock};
    snap_id = m_image_ctx.snap_id;
    m_read_only = m_image_ctx.read_only;
    m_read_only_flags = m_image_ctx.read_only_flags;
  }

  // mask out the non-primary read-only flag since its state can change
  bool read_only = (
    ((m_read_only_flags & ~IMAGE_READ_ONLY_FLAG_NON_PRIMARY) != 0) ||
    (snap_id != CEPH_NOSNAP));
  librados::ObjectReadOperation op;
  cls_client::get_size_start(&op, CEPH_NOSNAP);
  cls_client::get_features_start(&op, read_only);
  cls_client::get_flags_start(&op, CEPH_NOSNAP);
  cls_client::get_snapcontext_start(&op);
  rados::cls::lock::get_lock_info_start(&op, RBD_LOCK_NAME);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v2_get_mutable_metadata>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_mutable_metadata(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  auto it = m_out_bl.cbegin();
  if (*result >= 0) {
    uint8_t order;
    *result = cls_client::get_size_finish(&it, &m_size, &order);
  }

  if (*result >= 0) {
    *result = cls_client::get_features_finish(&it, &m_features,
                                              &m_incompatible_features);
  }

  if (*result >= 0) {
    *result = cls_client::get_flags_finish(&it, &m_flags);
  }

  if (*result >= 0) {
    *result = cls_client::get_snapcontext_finish(&it, &m_snapc);
  }

  if (*result >= 0) {
    ClsLockType lock_type = ClsLockType::NONE;
    *result = rados::cls::lock::get_lock_info_finish(&it, &m_lockers,
                                                     &lock_type, &m_lock_tag);
    if (*result == 0) {
      m_exclusive_locked = (lock_type == ClsLockType::EXCLUSIVE);
    }
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

  if (m_acquiring_lock && (m_features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
    ldout(cct, 5) << "ignoring dynamically disabled exclusive lock" << dendl;
    m_features |= RBD_FEATURE_EXCLUSIVE_LOCK;
    m_incomplete_update = true;
  }

  if (((m_incompatible_features & RBD_FEATURE_NON_PRIMARY) != 0U) &&
      ((m_read_only_flags & IMAGE_READ_ONLY_FLAG_NON_PRIMARY) == 0U) &&
      ((m_image_ctx.read_only_mask & IMAGE_READ_ONLY_FLAG_NON_PRIMARY) != 0U)) {
    // implies we opened a non-primary image in R/W mode
    ldout(cct, 5) << "adding non-primary read-only image flag" << dendl;
    m_read_only_flags |= IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
  } else if ((((m_incompatible_features & RBD_FEATURE_NON_PRIMARY) == 0U) ||
              ((m_image_ctx.read_only_mask &
                  IMAGE_READ_ONLY_FLAG_NON_PRIMARY) == 0U)) &&
             ((m_read_only_flags & IMAGE_READ_ONLY_FLAG_NON_PRIMARY) != 0U)) {
    ldout(cct, 5) << "removing non-primary read-only image flag" << dendl;
    m_read_only_flags &= ~IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
  }
  m_read_only = (m_read_only_flags != 0U);

  send_v2_get_parent();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_parent() {
  // NOTE: remove support when Mimic is EOLed
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": legacy=" << m_legacy_parent
                 << dendl;

  librados::ObjectReadOperation op;
  if (!m_legacy_parent) {
    cls_client::parent_get_start(&op);
    cls_client::parent_overlap_get_start(&op, CEPH_NOSNAP);
  } else {
    cls_client::get_parent_start(&op, CEPH_NOSNAP);
  }

  auto aio_comp = create_rados_callback<
    RefreshRequest<I>, &RefreshRequest<I>::handle_v2_get_parent>(this);
  m_out_bl.clear();
  m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, aio_comp, &op,
                                  &m_out_bl);
  aio_comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_parent(int *result) {
  // NOTE: remove support when Mimic is EOLed
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  auto it = m_out_bl.cbegin();
  if (!m_legacy_parent) {
    if (*result == 0) {
      *result = cls_client::parent_get_finish(&it, &m_parent_md.spec);
    }

    std::optional<uint64_t> parent_overlap;
    if (*result == 0) {
      *result = cls_client::parent_overlap_get_finish(&it, &parent_overlap);
    }

    if (*result == 0 && parent_overlap) {
      m_parent_md.overlap = *parent_overlap;
      m_head_parent_overlap = true;
    }
  } else if (*result == 0) {
    *result = cls_client::get_parent_finish(&it, &m_parent_md.spec,
                                            &m_parent_md.overlap);
    m_head_parent_overlap = true;
  }

  if (*result == -EOPNOTSUPP && !m_legacy_parent) {
    ldout(cct, 10) << "retrying using legacy parent method" << dendl;
    m_legacy_parent = true;
    send_v2_get_parent();
    return nullptr;
  } if (*result < 0) {
    lderr(cct) << "failed to retrieve parent: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  if ((m_features & RBD_FEATURE_MIGRATING) != 0) {
    ldout(cct, 1) << "migrating feature set" << dendl;
    send_get_migration_header();
    return nullptr;
  }

  send_v2_get_metadata();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_metadata() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  auto ctx = create_context_callback<
    RefreshRequest<I>, &RefreshRequest<I>::handle_v2_get_metadata>(this);
  auto req = GetMetadataRequest<I>::create(
    m_image_ctx.md_ctx, m_image_ctx.header_oid, true,
    ImageCtx::METADATA_CONF_PREFIX, ImageCtx::METADATA_CONF_PREFIX, 0U,
    &m_metadata, ctx);
  req->send();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_metadata(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to retrieve metadata: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v2_get_pool_metadata();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_pool_metadata() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  auto ctx = create_context_callback<
    RefreshRequest<I>, &RefreshRequest<I>::handle_v2_get_pool_metadata>(this);
  auto req = GetMetadataRequest<I>::create(
    m_pool_metadata_io_ctx, RBD_INFO, true, ImageCtx::METADATA_CONF_PREFIX,
    ImageCtx::METADATA_CONF_PREFIX, 0U, &m_metadata, ctx);
  req->send();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_pool_metadata(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to retrieve pool metadata: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  bool thread_safe = m_image_ctx.image_watcher->is_unregistered();
  m_image_ctx.apply_metadata(m_metadata, thread_safe);

  send_v2_get_op_features();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_op_features() {
  if ((m_features & RBD_FEATURE_OPERATIONS) == 0LL) {
    send_v2_get_group();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::op_features_get_start(&op);

  librados::AioCompletion *comp = create_rados_callback<
    RefreshRequest<I>, &RefreshRequest<I>::handle_v2_get_op_features>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_op_features(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  // -EOPNOTSUPP handler not required since feature bit implies OSD
  // supports the method
  if (*result == 0) {
    auto it = m_out_bl.cbegin();
    cls_client::op_features_get_finish(&it, &m_op_features);
  } else if (*result < 0) {
    lderr(cct) << "failed to retrieve op features: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v2_get_group();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_group() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::image_group_get_start(&op);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v2_get_group>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_group(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  if (*result == 0) {
    auto it = m_out_bl.cbegin();
    cls_client::image_group_get_finish(&it, &m_group_spec);
  }
  if (*result < 0 && *result != -EOPNOTSUPP) {
    lderr(cct) << "failed to retrieve group: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v2_get_snapshots();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_snapshots() {
  m_snap_infos.resize(m_snapc.snaps.size());
  m_snap_flags.resize(m_snapc.snaps.size());
  m_snap_parents.resize(m_snapc.snaps.size());
  m_snap_protection.resize(m_snapc.snaps.size());

  if (m_snapc.snaps.empty()) {
    send_v2_refresh_parent();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  for (auto snap_id : m_snapc.snaps) {
    if (m_legacy_snapshot != LEGACY_SNAPSHOT_DISABLED) {
      /// NOTE: remove after Luminous is retired
      cls_client::get_snapshot_name_start(&op, snap_id);
      cls_client::get_size_start(&op, snap_id);
      if (m_legacy_snapshot != LEGACY_SNAPSHOT_ENABLED_NO_TIMESTAMP) {
        cls_client::get_snapshot_timestamp_start(&op, snap_id);
      }
    } else {
      cls_client::snapshot_get_start(&op, snap_id);
    }

    if (m_legacy_parent) {
      cls_client::get_parent_start(&op, snap_id);
    } else {
      cls_client::parent_overlap_get_start(&op, snap_id);
    }

    cls_client::get_flags_start(&op, snap_id);
    cls_client::get_protection_status_start(&op, snap_id);
  }

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
    klass, &klass::handle_v2_get_snapshots>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_snapshots(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": " << "r=" << *result << dendl;

  auto it = m_out_bl.cbegin();
  for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
    if (m_legacy_snapshot != LEGACY_SNAPSHOT_DISABLED) {
      /// NOTE: remove after Luminous is retired
      std::string snap_name;
      if (*result >= 0) {
        *result = cls_client::get_snapshot_name_finish(&it, &snap_name);
      }

      uint64_t snap_size;
      if (*result >= 0) {
        uint8_t order;
        *result = cls_client::get_size_finish(&it, &snap_size, &order);
      }

      utime_t snap_timestamp;
      if (*result >= 0 &&
          m_legacy_snapshot != LEGACY_SNAPSHOT_ENABLED_NO_TIMESTAMP) {
        /// NOTE: remove after Jewel is retired
        *result = cls_client::get_snapshot_timestamp_finish(&it,
                                                            &snap_timestamp);
      }

      if (*result >= 0) {
        m_snap_infos[i] = {m_snapc.snaps[i],
                           {cls::rbd::UserSnapshotNamespace{}},
                           snap_name, snap_size, snap_timestamp, 0};
      }
    } else if (*result >= 0) {
      *result = cls_client::snapshot_get_finish(&it, &m_snap_infos[i]);
    }

    if (*result == 0) {
      if (m_legacy_parent) {
        *result = cls_client::get_parent_finish(&it, &m_snap_parents[i].spec,
                                                &m_snap_parents[i].overlap);
      } else {
        std::optional<uint64_t> parent_overlap;
        *result = cls_client::parent_overlap_get_finish(&it, &parent_overlap);
        if (*result == 0 && parent_overlap && m_parent_md.spec.pool_id > -1) {
          m_snap_parents[i].spec = m_parent_md.spec;
          m_snap_parents[i].overlap = *parent_overlap;
        }
      }
    }

    if (*result >= 0) {
      *result = cls_client::get_flags_finish(&it, &m_snap_flags[i]);
    }

    if (*result >= 0) {
      *result = cls_client::get_protection_status_finish(
        &it, &m_snap_protection[i]);
    }

    if (*result < 0) {
      break;
    }
  }

  if (*result == -ENOENT) {
    ldout(cct, 10) << "out-of-sync snapshot state detected" << dendl;
    send_v2_get_mutable_metadata();
    return nullptr;
  } else if (m_legacy_snapshot == LEGACY_SNAPSHOT_DISABLED &&
             *result == -EOPNOTSUPP) {
    ldout(cct, 10) << "retrying using legacy snapshot methods" << dendl;
    m_legacy_snapshot = LEGACY_SNAPSHOT_ENABLED;
    send_v2_get_snapshots();
    return nullptr;
  } else if (m_legacy_snapshot == LEGACY_SNAPSHOT_ENABLED &&
             *result == -EOPNOTSUPP) {
    ldout(cct, 10) << "retrying using legacy snapshot methods (jewel)" << dendl;
    m_legacy_snapshot = LEGACY_SNAPSHOT_ENABLED_NO_TIMESTAMP;
    send_v2_get_snapshots();
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
    std::shared_lock image_locker{m_image_ctx.image_lock};

    ParentImageInfo parent_md;
    MigrationInfo migration_info;
    int r = get_parent_info(m_image_ctx.snap_id, &parent_md, &migration_info);
    if (!m_skip_open_parent_image && (r < 0 ||
        RefreshParentRequest<I>::is_refresh_required(m_image_ctx, parent_md,
                                                     migration_info))) {
      CephContext *cct = m_image_ctx.cct;
      ldout(cct, 10) << this << " " << __func__ << dendl;

      using klass = RefreshRequest<I>;
      Context *ctx = create_context_callback<
        klass, &klass::handle_v2_refresh_parent>(this);
      m_refresh_parent = RefreshParentRequest<I>::create(
        m_image_ctx, parent_md, migration_info, ctx);
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
      m_read_only || !m_image_ctx.snap_name.empty() ||
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

  std::shared_lock owner_locker{m_image_ctx.owner_lock};
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
  bool journal_disabled = (
    (m_features & RBD_FEATURE_JOURNALING) == 0 ||
     m_read_only ||
     !m_image_ctx.snap_name.empty() ||
     m_image_ctx.journal != nullptr ||
     m_image_ctx.exclusive_lock == nullptr ||
     !m_image_ctx.exclusive_lock->is_lock_owner());
  bool journal_disabled_by_policy;
  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    journal_disabled_by_policy = (
      !journal_disabled &&
      m_image_ctx.get_journal_policy()->journal_disabled());
  }

  if (journal_disabled || journal_disabled_by_policy) {
    // journal dynamically enabled -- doesn't own exclusive lock
    if ((m_features & RBD_FEATURE_JOURNALING) != 0 &&
        !journal_disabled_by_policy &&
        m_image_ctx.exclusive_lock != nullptr &&
        m_image_ctx.journal == nullptr) {
      auto ctx = new LambdaContext([this](int) {
          send_v2_block_writes();
        });
      m_image_ctx.exclusive_lock->set_require_lock(
        librbd::io::DIRECTION_BOTH, ctx);
      return;
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
    std::shared_lock image_locker{m_image_ctx.image_lock};
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

  std::shared_lock owner_locker{m_image_ctx.owner_lock};
  m_image_ctx.io_image_dispatcher->block_writes(ctx);
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
       (m_read_only ||
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
    for (size_t snap_idx = 0; snap_idx < m_snap_infos.size(); ++snap_idx) {
      if (m_snap_infos[snap_idx].name == m_image_ctx.snap_name) {
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

  if (*result < 0) {
    lderr(cct) << "failed to open object map: " << cpp_strerror(*result)
               << dendl;
    m_object_map->put();
    m_object_map = nullptr;

    if (*result != -EFBIG) {
      save_result(result);
    }
  }

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

  ceph_assert(m_refresh_parent != nullptr);
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
    std::unique_lock owner_locker{m_image_ctx.owner_lock};
    ceph_assert(m_image_ctx.exclusive_lock == nullptr);
  }

  ceph_assert(m_exclusive_lock != nullptr);
  m_exclusive_lock->put();
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

  ceph_assert(m_journal != nullptr);
  m_journal->put();
  m_journal = nullptr;

  ceph_assert(m_blocked_writes);
  m_blocked_writes = false;

  m_image_ctx.io_image_dispatcher->unblock_writes();
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

  if (*result < 0) {
    lderr(cct) << "failed to close object map: " << cpp_strerror(*result)
               << dendl;
  }

  ceph_assert(m_object_map != nullptr);

  m_object_map->put();
  m_object_map = nullptr;

  return send_flush_aio();
}

template <typename I>
Context *RefreshRequest<I>::send_flush_aio() {
  if (m_incomplete_update && m_error_result == 0) {
    // if this was a partial refresh, notify ImageState
    m_error_result = -ERESTART;
  }

  if (m_flush_aio) {
    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 10) << this << " " << __func__ << dendl;

    std::shared_lock owner_locker{m_image_ctx.owner_lock};
    auto ctx = create_context_callback<
      RefreshRequest<I>, &RefreshRequest<I>::handle_flush_aio>(this);
    auto aio_comp = io::AioCompletion::create_and_start(
      ctx, util::get_image_ctx(&m_image_ctx), io::AIO_TYPE_FLUSH);
    auto req = io::ImageDispatchSpec<I>::create_flush(
      m_image_ctx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START, aio_comp,
      io::FLUSH_SOURCE_INTERNAL, {});
    req->send();
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

  std::scoped_lock locker{m_image_ctx.owner_lock, m_image_ctx.image_lock};

  m_image_ctx.read_only_flags = m_read_only_flags;
  m_image_ctx.read_only = m_read_only;
  m_image_ctx.size = m_size;
  m_image_ctx.lockers = m_lockers;
  m_image_ctx.lock_tag = m_lock_tag;
  m_image_ctx.exclusive_locked = m_exclusive_locked;

  std::map<uint64_t, uint64_t> migration_reverse_snap_seq;

  if (m_image_ctx.old_format) {
    m_image_ctx.order = m_order;
    m_image_ctx.features = 0;
    m_image_ctx.flags = 0;
    m_image_ctx.op_features = 0;
    m_image_ctx.operations_disabled = false;
    m_image_ctx.object_prefix = std::move(m_object_prefix);
    m_image_ctx.init_layout(m_image_ctx.md_ctx.get_id());
  } else {
    // HEAD revision doesn't have a defined overlap so it's only
    // applicable to snapshots
    if (!m_head_parent_overlap) {
      m_parent_md = {};
    }

    m_image_ctx.features = m_features;
    m_image_ctx.flags = m_flags;
    m_image_ctx.op_features = m_op_features;
    m_image_ctx.operations_disabled = (
      (m_op_features & ~RBD_OPERATION_FEATURES_ALL) != 0ULL);
    m_image_ctx.group_spec = m_group_spec;
    if (get_migration_info(&m_image_ctx.parent_md,
                           &m_image_ctx.migration_info)) {
      for (auto it : m_image_ctx.migration_info.snap_map) {
        migration_reverse_snap_seq[it.second.front()] = it.first;
      }
    } else {
      m_image_ctx.parent_md = m_parent_md;
      m_image_ctx.migration_info = {};
    }

    librados::Rados rados(m_image_ctx.md_ctx);
    int8_t require_osd_release;
    int r = rados.get_min_compatible_osd(&require_osd_release);
    if (r == 0 && require_osd_release >= CEPH_RELEASE_OCTOPUS) {
      m_image_ctx.enable_sparse_copyup = true;
    }
  }

  for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
    std::vector<librados::snap_t>::const_iterator it = std::find(
      m_image_ctx.snaps.begin(), m_image_ctx.snaps.end(),
      m_snapc.snaps[i].val);
    if (it == m_image_ctx.snaps.end()) {
      m_flush_aio = true;
      ldout(cct, 20) << "new snapshot id=" << m_snapc.snaps[i].val
                     << " name=" << m_snap_infos[i].name
                     << " size=" << m_snap_infos[i].image_size
                     << dendl;
    }
  }

  m_image_ctx.snaps.clear();
  m_image_ctx.snap_info.clear();
  m_image_ctx.snap_ids.clear();
  auto overlap = m_image_ctx.parent_md.overlap;
  for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
    uint64_t flags = m_image_ctx.old_format ? 0 : m_snap_flags[i];
    uint8_t protection_status = m_image_ctx.old_format ?
      static_cast<uint8_t>(RBD_PROTECTION_STATUS_UNPROTECTED) :
      m_snap_protection[i];
    ParentImageInfo parent;
    if (!m_image_ctx.old_format) {
      if (!m_image_ctx.migration_info.empty()) {
        parent = m_image_ctx.parent_md;
        auto it = migration_reverse_snap_seq.find(m_snapc.snaps[i].val);
        if (it != migration_reverse_snap_seq.end()) {
          parent.spec.snap_id = it->second;
          parent.overlap = m_snap_infos[i].image_size;
        } else {
          overlap = std::min(overlap, m_snap_infos[i].image_size);
          parent.overlap = overlap;
        }
      } else {
        parent = m_snap_parents[i];
      }
    }
    m_image_ctx.add_snap(m_snap_infos[i].snapshot_namespace,
                         m_snap_infos[i].name, m_snapc.snaps[i].val,
                         m_snap_infos[i].image_size, parent,
                         protection_status, flags,
                         m_snap_infos[i].timestamp);
  }
  m_image_ctx.parent_md.overlap = std::min(overlap, m_image_ctx.size);
  m_image_ctx.snapc = m_snapc;

  if (m_image_ctx.snap_id != CEPH_NOSNAP &&
      m_image_ctx.get_snap_id(m_image_ctx.snap_namespace,
                              m_image_ctx.snap_name) != m_image_ctx.snap_id) {
    lderr(cct) << "tried to read from a snapshot that no longer exists: "
               << m_image_ctx.snap_name << dendl;
    m_image_ctx.snap_exists = false;
  }

  if (m_refresh_parent != nullptr) {
    m_refresh_parent->apply();
  }
  if (m_image_ctx.data_ctx.is_valid()) {
    m_image_ctx.data_ctx.selfmanaged_snap_set_write_ctx(m_image_ctx.snapc.seq,
                                                        m_image_ctx.snaps);
    m_image_ctx.rebuild_data_io_context();
  }

  // handle dynamically enabled / disabled features
  if (m_image_ctx.exclusive_lock != nullptr &&
      !m_image_ctx.test_features(RBD_FEATURE_EXCLUSIVE_LOCK,
                                 m_image_ctx.image_lock)) {
    // disabling exclusive lock will automatically handle closing
    // object map and journaling
    ceph_assert(m_exclusive_lock == nullptr);
    m_exclusive_lock = m_image_ctx.exclusive_lock;
  } else {
    if (m_exclusive_lock != nullptr) {
      ceph_assert(m_image_ctx.exclusive_lock == nullptr);
      std::swap(m_exclusive_lock, m_image_ctx.exclusive_lock);
    }
    if (!m_image_ctx.test_features(RBD_FEATURE_JOURNALING,
                                   m_image_ctx.image_lock)) {
      if (!m_image_ctx.clone_copy_on_read && m_image_ctx.journal != nullptr) {
        m_image_ctx.exclusive_lock->unset_require_lock(io::DIRECTION_READ);
      }
      std::swap(m_journal, m_image_ctx.journal);
    } else if (m_journal != nullptr) {
      std::swap(m_journal, m_image_ctx.journal);
    }
    if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                   m_image_ctx.image_lock) ||
        m_object_map != nullptr) {
      std::swap(m_object_map, m_image_ctx.object_map);
    }
  }
}

template <typename I>
int RefreshRequest<I>::get_parent_info(uint64_t snap_id,
                                       ParentImageInfo *parent_md,
                                       MigrationInfo *migration_info) {
  if (get_migration_info(parent_md, migration_info)) {
    return 0;
  } else if (snap_id == CEPH_NOSNAP) {
    *parent_md = m_parent_md;
    *migration_info = {};
    return 0;
  } else {
    for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
      if (m_snapc.snaps[i].val == snap_id) {
        *parent_md = m_snap_parents[i];
        *migration_info = {};
        return 0;
      }
    }
  }
  return -ENOENT;
}

template <typename I>
bool RefreshRequest<I>::get_migration_info(ParentImageInfo *parent_md,
                                           MigrationInfo *migration_info) {
  if (m_migration_spec.header_type != cls::rbd::MIGRATION_HEADER_TYPE_DST ||
      (m_migration_spec.state != cls::rbd::MIGRATION_STATE_PREPARED &&
       m_migration_spec.state != cls::rbd::MIGRATION_STATE_EXECUTING)) {
    ceph_assert(m_migration_spec.header_type ==
                    cls::rbd::MIGRATION_HEADER_TYPE_SRC ||
                m_migration_spec.pool_id == -1 ||
                m_migration_spec.state == cls::rbd::MIGRATION_STATE_EXECUTED);

    return false;
  }

  parent_md->spec.pool_id = m_migration_spec.pool_id;
  parent_md->spec.pool_namespace = m_migration_spec.pool_namespace;
  parent_md->spec.image_id = m_migration_spec.image_id;
  parent_md->spec.snap_id = CEPH_NOSNAP;
  parent_md->overlap = std::min(m_size, m_migration_spec.overlap);

  auto snap_seqs = m_migration_spec.snap_seqs;
  // If new snapshots have been created on destination image after
  // migration stared, map the source CEPH_NOSNAP to the earliest of
  // these snapshots.
  snapid_t snap_id = snap_seqs.empty() ? 0 : snap_seqs.rbegin()->second;
  auto it = std::upper_bound(m_snapc.snaps.rbegin(), m_snapc.snaps.rend(),
                             snap_id);
  if (it != m_snapc.snaps.rend()) {
    snap_seqs[CEPH_NOSNAP] = *it;
  } else {
    snap_seqs[CEPH_NOSNAP] = CEPH_NOSNAP;
  }

  std::set<uint64_t> snap_ids;
  for (auto& it : snap_seqs) {
    snap_ids.insert(it.second);
  }
  uint64_t overlap = snap_ids.find(CEPH_NOSNAP) != snap_ids.end() ?
    parent_md->overlap : 0;
  for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
    if (snap_ids.find(m_snapc.snaps[i].val) != snap_ids.end()) {
      overlap = std::max(overlap, m_snap_infos[i].image_size);
    }
  }

  *migration_info = {m_migration_spec.pool_id, m_migration_spec.pool_namespace,
                     m_migration_spec.image_name, m_migration_spec.image_id, {},
                     overlap, m_migration_spec.flatten};

  deep_copy::util::compute_snap_map(m_image_ctx.cct, 0, CEPH_NOSNAP, {},
                                    snap_seqs, &migration_info->snap_map);
  return true;
}

} // namespace image
} // namespace librbd

template class librbd::image::RefreshRequest<librbd::ImageCtx>;
