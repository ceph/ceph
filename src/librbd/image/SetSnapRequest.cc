// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/SetSnapRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/image/RefreshParentRequest.h"
#include "librbd/io/ImageDispatcherInterface.h"

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::SetSnapRequest: "

namespace librbd {
namespace image {

using util::create_context_callback;

template <typename I>
SetSnapRequest<I>::SetSnapRequest(I &image_ctx, uint64_t snap_id,
                                  Context *on_finish)
  : m_image_ctx(image_ctx), m_snap_id(snap_id), m_on_finish(on_finish),
    m_exclusive_lock(nullptr), m_object_map(nullptr), m_refresh_parent(nullptr),
    m_writes_blocked(false) {
}

template <typename I>
SetSnapRequest<I>::~SetSnapRequest() {
  ceph_assert(!m_writes_blocked);
  delete m_refresh_parent;
  if (m_object_map) {
    m_object_map->put();
  }
  if (m_exclusive_lock) {
    m_exclusive_lock->put();
  }
}

template <typename I>
void SetSnapRequest<I>::send() {
  if (m_snap_id == CEPH_NOSNAP) {
    send_init_exclusive_lock();
  } else {
    send_block_writes();
  }
}

template <typename I>
void SetSnapRequest<I>::send_init_exclusive_lock() {
  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    if (m_image_ctx.exclusive_lock != nullptr) {
      ceph_assert(m_image_ctx.snap_id == CEPH_NOSNAP);
      send_complete();
      return;
    }
  }

  if (m_image_ctx.read_only ||
      !m_image_ctx.test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    int r = 0;
    if (send_refresh_parent(&r) != nullptr) {
      send_complete();
    }
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  m_exclusive_lock = ExclusiveLock<I>::create(m_image_ctx);

  using klass = SetSnapRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_init_exclusive_lock>(this);

  std::shared_lock owner_locker{m_image_ctx.owner_lock};
  m_exclusive_lock->init(m_image_ctx.features, ctx);
}

template <typename I>
Context *SetSnapRequest<I>::handle_init_exclusive_lock(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to initialize exclusive lock: "
               << cpp_strerror(*result) << dendl;
    finalize();
    return m_on_finish;
  }
  return send_refresh_parent(result);
}

template <typename I>
void SetSnapRequest<I>::send_block_writes() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  m_writes_blocked = true;

  using klass = SetSnapRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_block_writes>(this);

  std::shared_lock owner_locker{m_image_ctx.owner_lock};
  m_image_ctx.io_image_dispatcher->block_writes(ctx);
}

template <typename I>
Context *SetSnapRequest<I>::handle_block_writes(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*result)
               << dendl;
    finalize();
    return m_on_finish;
  }

  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    auto it = m_image_ctx.snap_info.find(m_snap_id);
    if (it == m_image_ctx.snap_info.end()) {
      ldout(cct, 5) << "failed to locate snapshot '" << m_snap_id << "'"
                    << dendl;

      *result = -ENOENT;
      finalize();
      return m_on_finish;
    }
  }

  return send_shut_down_exclusive_lock(result);
}

template <typename I>
Context *SetSnapRequest<I>::send_shut_down_exclusive_lock(int *result) {
  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    m_exclusive_lock = m_image_ctx.exclusive_lock;
  }

  if (m_exclusive_lock == nullptr) {
    return send_refresh_parent(result);
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = SetSnapRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_shut_down_exclusive_lock>(this);
  m_exclusive_lock->shut_down(ctx);
  return nullptr;
}

template <typename I>
Context *SetSnapRequest<I>::handle_shut_down_exclusive_lock(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to shut down exclusive lock: "
               << cpp_strerror(*result) << dendl;
    finalize();
    return m_on_finish;
  }

  return send_refresh_parent(result);
}

template <typename I>
Context *SetSnapRequest<I>::send_refresh_parent(int *result) {
  CephContext *cct = m_image_ctx.cct;

  ParentImageInfo parent_md;
  bool refresh_parent;
  {
    std::shared_lock image_locker{m_image_ctx.image_lock};

    const auto parent_info = m_image_ctx.get_parent_info(m_snap_id);
    if (parent_info == nullptr) {
      *result = -ENOENT;
      lderr(cct) << "failed to retrieve snapshot parent info" << dendl;
      finalize();
      return m_on_finish;
    }

    parent_md = *parent_info;
    refresh_parent = RefreshParentRequest<I>::is_refresh_required(
        m_image_ctx, parent_md, m_image_ctx.migration_info);
  }

  if (!refresh_parent) {
    if (m_snap_id == CEPH_NOSNAP) {
      // object map is loaded when exclusive lock is acquired
      *result = apply();
      finalize();
      return m_on_finish;
    } else {
      // load snapshot object map
      return send_open_object_map(result);
    }
  }

  ldout(cct, 10) << __func__ << dendl;

  using klass = SetSnapRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_refresh_parent>(this);
  m_refresh_parent = RefreshParentRequest<I>::create(m_image_ctx, parent_md,
                                                     m_image_ctx.migration_info,
                                                     ctx);
  m_refresh_parent->send();
  return nullptr;
}

template <typename I>
Context *SetSnapRequest<I>::handle_refresh_parent(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to refresh snapshot parent: " << cpp_strerror(*result)
               << dendl;
    finalize();
    return m_on_finish;
  }

  if (m_snap_id == CEPH_NOSNAP) {
    // object map is loaded when exclusive lock is acquired
    *result = apply();
    if (*result < 0) {
      finalize();
      return m_on_finish;
    }

    return send_finalize_refresh_parent(result);
  } else {
    // load snapshot object map
    return send_open_object_map(result);
  }
}

template <typename I>
Context *SetSnapRequest<I>::send_open_object_map(int *result) {
  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
    *result = apply();
    if (*result < 0) {
      finalize();
      return m_on_finish;
    }

    return send_finalize_refresh_parent(result);
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = SetSnapRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_open_object_map>(this);
  m_object_map = ObjectMap<I>::create(m_image_ctx, m_snap_id);
  m_object_map->open(ctx);
  return nullptr;
}

template <typename I>
Context *SetSnapRequest<I>::handle_open_object_map(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to open object map: " << cpp_strerror(*result)
               << dendl;
    m_object_map->put();
    m_object_map = nullptr;
  }

  *result = apply();
  if (*result < 0) {
    finalize();
    return m_on_finish;
  }

  return send_finalize_refresh_parent(result);
}

template <typename I>
Context *SetSnapRequest<I>::send_finalize_refresh_parent(int *result) {
  if (m_refresh_parent == nullptr) {
    finalize();
    return m_on_finish;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = SetSnapRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_finalize_refresh_parent>(this);
  m_refresh_parent->finalize(ctx);
  return nullptr;
}

template <typename I>
Context *SetSnapRequest<I>::handle_finalize_refresh_parent(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to close parent image: " << cpp_strerror(*result)
               << dendl;
  }
  finalize();
  return m_on_finish;
}

template <typename I>
int SetSnapRequest<I>::apply() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  std::scoped_lock locker{m_image_ctx.owner_lock, m_image_ctx.image_lock};
  if (m_snap_id != CEPH_NOSNAP) {
    ceph_assert(m_image_ctx.exclusive_lock == nullptr);
    int r = m_image_ctx.snap_set(m_snap_id);
    if (r < 0) {
      return r;
    }
  } else {
    std::swap(m_image_ctx.exclusive_lock, m_exclusive_lock);
    m_image_ctx.snap_unset();
  }

  if (m_refresh_parent != nullptr) {
    m_refresh_parent->apply();
  }

  std::swap(m_object_map, m_image_ctx.object_map);
  return 0;
}

template <typename I>
void SetSnapRequest<I>::finalize() {
  if (m_writes_blocked) {
    m_image_ctx.io_image_dispatcher->unblock_writes();
    m_writes_blocked = false;
  }
}

template <typename I>
void SetSnapRequest<I>::send_complete() {
  finalize();
  m_on_finish->complete(0);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::SetSnapRequest<librbd::ImageCtx>;
