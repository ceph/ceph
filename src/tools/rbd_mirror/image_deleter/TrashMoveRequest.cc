// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_deleter/TrashMoveRequest.h"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/TrashWatcher.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/journal/ResetRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/trash/MoveRequest.h"
#include "tools/rbd_mirror/image_deleter/Types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_deleter::TrashMoveRequest: " \
                           << this << " " << __func__ << ": "
namespace rbd {
namespace mirror {
namespace image_deleter {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void TrashMoveRequest<I>::send() {
  get_mirror_image_id();
}

template <typename I>
void TrashMoveRequest<I>::get_mirror_image_id() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_image_id_start(&op, m_global_image_id);

  auto aio_comp = create_rados_callback<
    TrashMoveRequest<I>,
    &TrashMoveRequest<I>::handle_get_mirror_image_id>(this);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void TrashMoveRequest<I>::handle_get_mirror_image_id(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == 0) {
    auto bl_it = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_image_get_image_id_finish(&bl_it,
                                                             &m_image_id);
  }
  if (r == -ENOENT) {
    dout(10) << "image " << m_global_image_id << " is not mirrored" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << "error retrieving local id for image " << m_global_image_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_mirror_info();
}

template <typename I>
void TrashMoveRequest<I>::get_mirror_info() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    TrashMoveRequest<I>, &TrashMoveRequest<I>::handle_get_mirror_info>(this);
  auto req = librbd::mirror::GetInfoRequest<I>::create(
    m_io_ctx, m_op_work_queue, m_image_id, &m_mirror_image, &m_promotion_state,
    &m_primary_mirror_uuid, ctx);
  req->send();
}

template <typename I>
void TrashMoveRequest<I>::handle_get_mirror_info(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(5) << "image " << m_global_image_id << " is not mirrored" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << "error retrieving image primary info for image "
         << m_global_image_id << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_promotion_state == librbd::mirror::PROMOTION_STATE_PRIMARY) {
    dout(10) << "image " << m_global_image_id << " is local primary" << dendl;
    finish(-EPERM);
    return;
  } else if (m_promotion_state == librbd::mirror::PROMOTION_STATE_ORPHAN &&
             !m_resync) {
    dout(10) << "image " << m_global_image_id << " is orphaned" << dendl;
    finish(-EPERM);
    return;
  }

  disable_mirror_image();
}

template <typename I>
void TrashMoveRequest<I>::disable_mirror_image() {
  dout(10) << dendl;

  m_mirror_image.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_set(&op, m_image_id, m_mirror_image);

  auto aio_comp = create_rados_callback<
    TrashMoveRequest<I>,
    &TrashMoveRequest<I>::handle_disable_mirror_image>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void TrashMoveRequest<I>::handle_disable_mirror_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << "local image is not mirrored, aborting deletion." << dendl;
    finish(r);
    return;
  } else if (r == -EEXIST || r == -EINVAL) {
    derr << "cannot disable mirroring for image " << m_global_image_id
         << ": global_image_id has changed/reused: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << "cannot disable mirroring for image " << m_global_image_id
         << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  open_image();
}

template <typename I>
void TrashMoveRequest<I>::open_image() {
  dout(10) << dendl;

  m_image_ctx = I::create("", m_image_id, nullptr, m_io_ctx, false);

  // ensure non-primary images can be modified
  m_image_ctx->read_only_mask &= ~librbd::IMAGE_READ_ONLY_FLAG_NON_PRIMARY;

  {
    // don't attempt to open the journal
    std::unique_lock image_locker{m_image_ctx->image_lock};
    m_image_ctx->set_journal_policy(new JournalPolicy());
  }

  Context *ctx = create_context_callback<
    TrashMoveRequest<I>, &TrashMoveRequest<I>::handle_open_image>(this);
  m_image_ctx->state->open(librbd::OPEN_FLAG_SKIP_OPEN_PARENT, ctx);
}

template <typename I>
void TrashMoveRequest<I>::handle_open_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to open image: " << cpp_strerror(r) << dendl;
    m_image_ctx->destroy();
    m_image_ctx = nullptr;
    finish(r);
    return;
  }

  if (m_image_ctx->old_format) {
    derr << "cannot move v1 image to trash" << dendl;
    m_ret_val = -EINVAL;
    close_image();
    return;
  }

  reset_journal();
}

template <typename I>
void TrashMoveRequest<I>::reset_journal() {
  if (m_mirror_image.mode == cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
    // snapshot-based mirroring doesn't require journal feature
    acquire_lock();
    return;
  }

  dout(10) << dendl;

  // TODO use Journal thread pool for journal ops until converted to ASIO
  ContextWQ* context_wq;
  librbd::Journal<>::get_work_queue(
    reinterpret_cast<CephContext*>(m_io_ctx.cct()), &context_wq);

  // ensure that if the image is recovered any peers will split-brain
  auto ctx = create_context_callback<
    TrashMoveRequest<I>, &TrashMoveRequest<I>::handle_reset_journal>(this);
  auto req = librbd::journal::ResetRequest<I>::create(
    m_io_ctx, m_image_id, librbd::Journal<>::IMAGE_CLIENT_ID,
    librbd::Journal<>::LOCAL_MIRROR_UUID, context_wq, ctx);
  req->send();
}

template <typename I>
void TrashMoveRequest<I>::handle_reset_journal(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "failed to reset journal: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_image();
    return;
  }

  acquire_lock();
}

template <typename I>
void TrashMoveRequest<I>::acquire_lock() {
  m_image_ctx->owner_lock.lock_shared();
  if (m_image_ctx->exclusive_lock == nullptr) {
    m_image_ctx->owner_lock.unlock_shared();

    if (m_mirror_image.mode == cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
      // snapshot-based mirroring doesn't require exclusive-lock
      trash_move();
    } else {
      derr << "exclusive lock feature not enabled" << dendl;
      m_ret_val = -EINVAL;
      close_image();
    }
    return;
  }

  dout(10) << dendl;

  Context *ctx = create_context_callback<
    TrashMoveRequest<I>, &TrashMoveRequest<I>::handle_acquire_lock>(this);
  m_image_ctx->exclusive_lock->block_requests(0);
  m_image_ctx->exclusive_lock->acquire_lock(ctx);
  m_image_ctx->owner_lock.unlock_shared();
}

template <typename I>
void TrashMoveRequest<I>::handle_acquire_lock(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to acquire exclusive lock: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_image();
    return;
  }

  trash_move();
}

template <typename I>
void TrashMoveRequest<I>::trash_move() {
  dout(10) << dendl;

  utime_t delete_time{ceph_clock_now()};
  utime_t deferment_end_time{delete_time};
  deferment_end_time +=
    m_image_ctx->config.template get_val<uint64_t>("rbd_mirroring_delete_delay");

  m_trash_image_spec = {
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, m_image_ctx->name, delete_time,
    deferment_end_time};

  Context *ctx = create_context_callback<
    TrashMoveRequest<I>, &TrashMoveRequest<I>::handle_trash_move>(this);
  auto req = librbd::trash::MoveRequest<I>::create(
    m_io_ctx, m_image_id, m_trash_image_spec, ctx);
  req->send();
}

template <typename I>
void TrashMoveRequest<I>::handle_trash_move(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to move image to trash: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_image();
    return;
  }

  m_moved_to_trash = true;
  remove_mirror_image();
}

template <typename I>
void TrashMoveRequest<I>::remove_mirror_image() {
  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_remove(&op, m_image_id);

  auto aio_comp = create_rados_callback<
    TrashMoveRequest<I>,
    &TrashMoveRequest<I>::handle_remove_mirror_image>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void TrashMoveRequest<I>::handle_remove_mirror_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << "local image is not mirrored" << dendl;
  } else if (r < 0) {
    derr << "failed to remove mirror image state for " << m_global_image_id
         << ": " << cpp_strerror(r) << dendl;
    m_ret_val = r;
  }

  close_image();
}

template <typename I>
void TrashMoveRequest<I>::close_image() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    TrashMoveRequest<I>, &TrashMoveRequest<I>::handle_close_image>(this);
  m_image_ctx->state->close(ctx);
}

template <typename I>
void TrashMoveRequest<I>::handle_close_image(int r) {
  dout(10) << "r=" << r << dendl;

  m_image_ctx->destroy();
  m_image_ctx = nullptr;

  if (r < 0) {
    derr << "failed to close image: " << cpp_strerror(r) << dendl;
  }

  // don't send notification if we failed
  if (!m_moved_to_trash) {
    finish(0);
    return;
  }

  notify_trash_add();
}

template <typename I>
void TrashMoveRequest<I>::notify_trash_add() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    TrashMoveRequest<I>, &TrashMoveRequest<I>::handle_notify_trash_add>(this);
  librbd::TrashWatcher<I>::notify_image_added(m_io_ctx, m_image_id,
                                              m_trash_image_spec, ctx);
}

template <typename I>
void TrashMoveRequest<I>::handle_notify_trash_add(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to notify trash watchers: " << cpp_strerror(r) << dendl;
  }

  finish(0);
}

template <typename I>
void TrashMoveRequest<I>::finish(int r) {
  if (m_ret_val < 0) {
    r = m_ret_val;
  }

  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_deleter::TrashMoveRequest<librbd::ImageCtx>;

