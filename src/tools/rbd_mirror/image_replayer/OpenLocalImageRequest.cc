// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "CloseImageRequest.h"
#include "OpenLocalImageRequest.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/journal/Policy.h"
#include "librbd/mirror/GetInfoRequest.h"
#include <type_traits>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::OpenLocalImageRequest: " \
                           << this << " " << __func__ << " "

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;

namespace {

template <typename I>
struct MirrorExclusiveLockPolicy : public librbd::exclusive_lock::Policy {
  I *image_ctx;

  MirrorExclusiveLockPolicy(I *image_ctx) : image_ctx(image_ctx) {
  }

  bool may_auto_request_lock() override {
    return false;
  }

  int lock_requested(bool force) override {
    int r = -EROFS;
    {
      std::shared_lock owner_locker{image_ctx->owner_lock};
      std::shared_lock image_locker{image_ctx->image_lock};
      if (image_ctx->journal == nullptr || image_ctx->journal->is_tag_owner()) {
        r = 0;
      }
    }

    if (r == 0) {
      // if the local image journal has been closed or if it was (force)
      // promoted allow the lock to be released to another client
      image_ctx->exclusive_lock->release_lock(nullptr);
    }
    return r;
  }

  bool accept_blocked_request(
      librbd::exclusive_lock::OperationRequestType request_type) override {
    switch (request_type) {
    case librbd::exclusive_lock::OPERATION_REQUEST_TYPE_TRASH_SNAP_REMOVE:
    case librbd::exclusive_lock::OPERATION_REQUEST_TYPE_FORCE_PROMOTION:
      return true;
    default:
      return false;
    }
  }
};

struct MirrorJournalPolicy : public librbd::journal::Policy {
  librbd::asio::ContextWQ *work_queue;

  MirrorJournalPolicy(librbd::asio::ContextWQ *work_queue)
    : work_queue(work_queue) {
  }

  bool append_disabled() const override {
    // avoid recording any events to the local journal
    return true;
  }
  bool journal_disabled() const override {
    return false;
  }

  void allocate_tag_on_lock(Context *on_finish) override {
    // rbd-mirror will manually create tags by copying them from the peer
    work_queue->queue(on_finish, 0);
  }
};

} // anonymous namespace

template <typename I>
OpenLocalImageRequest<I>::OpenLocalImageRequest(
    librados::IoCtx &local_io_ctx,
    I **local_image_ctx,
    const std::string &local_image_id,
    librbd::asio::ContextWQ *work_queue,
    Context *on_finish)
  : m_local_io_ctx(local_io_ctx), m_local_image_ctx(local_image_ctx),
    m_local_image_id(local_image_id), m_work_queue(work_queue),
    m_on_finish(on_finish) {
}

template <typename I>
void OpenLocalImageRequest<I>::send() {
  send_open_image();
}

template <typename I>
void OpenLocalImageRequest<I>::send_open_image() {
  dout(20) << dendl;

  *m_local_image_ctx = I::create("", m_local_image_id, nullptr,
                                 m_local_io_ctx, false);

  // ensure non-primary images can be modified
  (*m_local_image_ctx)->read_only_mask =
    ~librbd::IMAGE_READ_ONLY_FLAG_NON_PRIMARY;

  {
    std::scoped_lock locker{(*m_local_image_ctx)->owner_lock,
			    (*m_local_image_ctx)->image_lock};
    (*m_local_image_ctx)->set_exclusive_lock_policy(
      new MirrorExclusiveLockPolicy<I>(*m_local_image_ctx));
    (*m_local_image_ctx)->set_journal_policy(
      new MirrorJournalPolicy(m_work_queue));
  }

  Context *ctx = create_context_callback<
    OpenLocalImageRequest<I>, &OpenLocalImageRequest<I>::handle_open_image>(
      this);
  (*m_local_image_ctx)->state->open(0, ctx);
}

template <typename I>
void OpenLocalImageRequest<I>::handle_open_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    if (r == -ENOENT) {
      dout(10) << ": local image does not exist" << dendl;
    } else {
      derr << ": failed to open image '" << m_local_image_id << "': "
           << cpp_strerror(r) << dendl;
    }
    *m_local_image_ctx = nullptr;
    finish(r);
    return;
  }

  send_get_mirror_info();
}

template <typename I>
void OpenLocalImageRequest<I>::send_get_mirror_info() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    OpenLocalImageRequest<I>,
    &OpenLocalImageRequest<I>::handle_get_mirror_info>(
      this);
  auto request = librbd::mirror::GetInfoRequest<I>::create(
    **m_local_image_ctx, &m_mirror_image, &m_promotion_state,
    &m_primary_mirror_uuid, ctx);
  request->send();
}

template <typename I>
void OpenLocalImageRequest<I>::handle_get_mirror_info(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == -ENOENT) {
    dout(5) << ": local image is not mirrored" << dendl;
    send_close_image(r);
    return;
  } else if (r < 0) {
    derr << ": error querying local image primary status: " << cpp_strerror(r)
         << dendl;
    send_close_image(r);
    return;
  }

  if (m_mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_DISABLING) {
    dout(5) << ": local image mirroring is being disabled" << dendl;
    send_close_image(-ENOENT);
    return;
  }

  // if the local image owns the tag -- don't steal the lock since
  // we aren't going to mirror peer data into this image anyway
  if (m_promotion_state == librbd::mirror::PROMOTION_STATE_PRIMARY) {
    dout(10) << ": local image is primary -- skipping image replay" << dendl;
    send_close_image(-EREMOTEIO);
    return;
  }

  send_lock_image();
}

template <typename I>
void OpenLocalImageRequest<I>::send_lock_image() {
  std::shared_lock owner_locker{(*m_local_image_ctx)->owner_lock};
  if ((*m_local_image_ctx)->exclusive_lock == nullptr) {
    owner_locker.unlock();
    if (m_mirror_image.mode == cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
      finish(0);
    } else {
      derr << ": image does not support exclusive lock" << dendl;
      send_close_image(-EINVAL);
    }
    return;
  }

  dout(20) << dendl;

  // disallow any proxied maintenance operations before grabbing lock
  (*m_local_image_ctx)->exclusive_lock->block_requests(-EROFS);

  Context *ctx = create_context_callback<
    OpenLocalImageRequest<I>, &OpenLocalImageRequest<I>::handle_lock_image>(
      this);

  (*m_local_image_ctx)->exclusive_lock->acquire_lock(ctx);
}

template <typename I>
void OpenLocalImageRequest<I>::handle_lock_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to lock image '" << m_local_image_id << "': "
       << cpp_strerror(r) << dendl;
    send_close_image(r);
    return;
  }

  {
    std::shared_lock owner_locker{(*m_local_image_ctx)->owner_lock};
    if ((*m_local_image_ctx)->exclusive_lock == nullptr ||
	!(*m_local_image_ctx)->exclusive_lock->is_lock_owner()) {
      derr << ": image is not locked" << dendl;
      send_close_image(-EBUSY);
      return;
    }
  }

  finish(0);
}

template <typename I>
void OpenLocalImageRequest<I>::send_close_image(int r) {
  dout(20) << dendl;

  if (m_ret_val == 0 && r < 0) {
    m_ret_val = r;
  }

  Context *ctx = create_context_callback<
    OpenLocalImageRequest<I>, &OpenLocalImageRequest<I>::handle_close_image>(
      this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    m_local_image_ctx, ctx);
  request->send();
}

template <typename I>
void OpenLocalImageRequest<I>::handle_close_image(int r) {
  dout(20) << dendl;

  ceph_assert(r == 0);
  finish(m_ret_val);
}

template <typename I>
void OpenLocalImageRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::OpenLocalImageRequest<librbd::ImageCtx>;
