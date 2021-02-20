// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_deleter/SnapshotPurgeRequest.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/journal/Policy.h"
#include "tools/rbd_mirror/image_deleter/Types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_deleter::SnapshotPurgeRequest: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_deleter {

using librbd::util::create_context_callback;

template <typename I>
void SnapshotPurgeRequest<I>::send() {
  open_image();
}

template <typename I>
void SnapshotPurgeRequest<I>::open_image() {
  dout(10) << dendl;
  m_image_ctx = I::create("", m_image_id, nullptr, m_io_ctx, false);

  // ensure non-primary images can be modified
  m_image_ctx->read_only_mask &= ~librbd::IMAGE_READ_ONLY_FLAG_NON_PRIMARY;

  {
    std::unique_lock image_locker{m_image_ctx->image_lock};
    m_image_ctx->set_journal_policy(new JournalPolicy());
  }

  Context *ctx = create_context_callback<
    SnapshotPurgeRequest<I>, &SnapshotPurgeRequest<I>::handle_open_image>(
      this);
  m_image_ctx->state->open(librbd::OPEN_FLAG_SKIP_OPEN_PARENT, ctx);
}

template <typename I>
void SnapshotPurgeRequest<I>::handle_open_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to open image '" << m_image_id << "': " << cpp_strerror(r)
         << dendl;
    m_image_ctx = nullptr;

    finish(r);
    return;
  }

  acquire_lock();
}

template <typename I>
void SnapshotPurgeRequest<I>::acquire_lock() {
  dout(10) << dendl;

  m_image_ctx->owner_lock.lock_shared();
  if (m_image_ctx->exclusive_lock == nullptr) {
    m_image_ctx->owner_lock.unlock_shared();

    start_snap_unprotect();
    return;
  }

  m_image_ctx->exclusive_lock->acquire_lock(create_context_callback<
    SnapshotPurgeRequest<I>, &SnapshotPurgeRequest<I>::handle_acquire_lock>(
      this));
  m_image_ctx->owner_lock.unlock_shared();
}

template <typename I>
void SnapshotPurgeRequest<I>::handle_acquire_lock(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to acquire exclusive lock: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_image();
    return;
  }

  start_snap_unprotect();
}

template <typename I>
void SnapshotPurgeRequest<I>::start_snap_unprotect() {
  dout(10) << dendl;

  {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    m_snaps = m_image_ctx->snaps;
  }
  snap_unprotect();
}

template <typename I>
void SnapshotPurgeRequest<I>::snap_unprotect() {
  if (m_snaps.empty()) {
    close_image();
    return;
  }

  librados::snap_t snap_id = m_snaps.back();
  m_image_ctx->image_lock.lock_shared();
  int r = m_image_ctx->get_snap_namespace(snap_id, &m_snap_namespace);
  if (r < 0) {
    m_image_ctx->image_lock.unlock_shared();

    derr << "failed to get snap namespace: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_image();
    return;
  }

  r = m_image_ctx->get_snap_name(snap_id, &m_snap_name);
  if (r < 0) {
    m_image_ctx->image_lock.unlock_shared();

    derr << "failed to get snap name: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_image();
    return;
  }

  bool is_protected;
  r = m_image_ctx->is_snap_protected(snap_id, &is_protected);
  if (r < 0) {
    m_image_ctx->image_lock.unlock_shared();

    derr << "failed to get snap protection status: " << cpp_strerror(r)
         << dendl;
    m_ret_val = r;
    close_image();
    return;
  }
  m_image_ctx->image_lock.unlock_shared();

  if (!is_protected) {
    snap_remove();
    return;
  }

  dout(10) << "snap_id=" << snap_id << ", "
           << "snap_namespace=" << m_snap_namespace << ", "
           << "snap_name=" << m_snap_name << dendl;

  auto finish_op_ctx = start_lock_op(&r);
  if (finish_op_ctx == nullptr) {
    derr << "lost exclusive lock" << dendl;
    m_ret_val = r;
    close_image();
    return;
  }

  auto ctx = new LambdaContext([this, finish_op_ctx](int r) {
      handle_snap_unprotect(r);
      finish_op_ctx->complete(0);
    });
  std::shared_lock owner_locker{m_image_ctx->owner_lock};
  m_image_ctx->operations->execute_snap_unprotect(
    m_snap_namespace, m_snap_name.c_str(), ctx);
}

template <typename I>
void SnapshotPurgeRequest<I>::handle_snap_unprotect(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -EBUSY) {
    dout(10) << "snapshot in-use" << dendl;
    m_ret_val = r;
    close_image();
    return;
  } else if (r < 0) {
    derr << "failed to unprotect snapshot: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_image();
    return;
  }

  {
    // avoid the need to refresh to delete the newly unprotected snapshot
    std::shared_lock image_locker{m_image_ctx->image_lock};
    librados::snap_t snap_id = m_snaps.back();
    auto snap_info_it = m_image_ctx->snap_info.find(snap_id);
    if (snap_info_it != m_image_ctx->snap_info.end()) {
      snap_info_it->second.protection_status =
        RBD_PROTECTION_STATUS_UNPROTECTED;
    }
  }

  snap_remove();
}

template <typename I>
void SnapshotPurgeRequest<I>::snap_remove() {
  librados::snap_t snap_id = m_snaps.back();
  dout(10) << "snap_id=" << snap_id << ", "
           << "snap_namespace=" << m_snap_namespace << ", "
           << "snap_name=" << m_snap_name << dendl;

  int r;
  auto finish_op_ctx = start_lock_op(&r);
  if (finish_op_ctx == nullptr) {
    derr << "lost exclusive lock" << dendl;
    m_ret_val = r;
    close_image();
    return;
  }

  auto ctx = new LambdaContext([this, finish_op_ctx](int r) {
      handle_snap_remove(r);
      finish_op_ctx->complete(0);
    });
  std::shared_lock owner_locker{m_image_ctx->owner_lock};
  m_image_ctx->operations->execute_snap_remove(
    m_snap_namespace, m_snap_name.c_str(), ctx);
}

template <typename I>
void SnapshotPurgeRequest<I>::handle_snap_remove(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -EBUSY) {
    dout(10) << "snapshot in-use" << dendl;
    m_ret_val = r;
    close_image();
    return;
  } else if (r < 0) {
    derr << "failed to remove snapshot: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_image();
    return;
  }

  m_snaps.pop_back();
  snap_unprotect();
}

template <typename I>
void SnapshotPurgeRequest<I>::close_image() {
  dout(10) << dendl;

  m_image_ctx->state->close(create_context_callback<
    SnapshotPurgeRequest<I>,
    &SnapshotPurgeRequest<I>::handle_close_image>(this));
}

template <typename I>
void SnapshotPurgeRequest<I>::handle_close_image(int r) {
  dout(10) << "r=" << r << dendl;

  m_image_ctx = nullptr;

  if (r < 0) {
    derr << "failed to close: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }
  finish(0);
}

template <typename I>
void SnapshotPurgeRequest<I>::finish(int r) {
  if (m_ret_val < 0) {
    r = m_ret_val;
  }

  m_on_finish->complete(r);
  delete this;
}

template <typename I>
Context *SnapshotPurgeRequest<I>::start_lock_op(int* r) {
  std::shared_lock owner_locker{m_image_ctx->owner_lock};
  if (m_image_ctx->exclusive_lock == nullptr) {
    return new LambdaContext([](int r) {});
  }
  return m_image_ctx->exclusive_lock->start_op(r);
}

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_deleter::SnapshotPurgeRequest<librbd::ImageCtx>;
