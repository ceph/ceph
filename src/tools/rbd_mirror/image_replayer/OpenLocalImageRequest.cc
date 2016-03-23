// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "OpenLocalImageRequest.h"
#include "CloseImageRequest.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/journal/Policy.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::OpenLocalImageRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;

namespace {

struct MirrorExclusiveLockPolicy : public librbd::exclusive_lock::Policy {

  virtual void lock_requested(bool force) {
    // TODO: interlock is being requested (e.g. local promotion)
    // Wait for demote event from peer or abort replay on forced
    // promotion.
  }

};

struct MirrorJournalPolicy : public librbd::journal::Policy {
  ContextWQ *work_queue;

  MirrorJournalPolicy(ContextWQ *work_queue) : work_queue(work_queue) {
  }

  virtual void allocate_tag_on_lock(Context *on_finish) {
    // rbd-mirror will manually create tags by copying them from the peer
    work_queue->queue(on_finish, 0);
  }

  virtual void cancel_external_replay(Context *on_finish) {
    // TODO: journal is being closed due to a comms error.  This means
    // the journal is being closed and the exclusive lock is being released.
    // ImageReplayer needs to restart.
  }

};

} // anonymous namespace

template <typename I>
OpenLocalImageRequest<I>::OpenLocalImageRequest(librados::IoCtx &local_io_ctx,
                                                I **local_image_ctx,
                                                const std::string &local_image_name,
                                                const std::string &local_image_id,
                                                ContextWQ *work_queue,
                                                Context *on_finish)
  : m_local_io_ctx(local_io_ctx), m_local_image_ctx(local_image_ctx),
    m_local_image_name(local_image_name), m_local_image_id(local_image_id),
    m_work_queue(work_queue), m_on_finish(on_finish) {
}

template <typename I>
void OpenLocalImageRequest<I>::send() {
  send_open_image();
}

template <typename I>
void OpenLocalImageRequest<I>::send_open_image() {
  dout(20) << dendl;

  *m_local_image_ctx = new librbd::ImageCtx(m_local_image_name,
                                            m_local_image_id, nullptr,
                                            m_local_io_ctx, false);
  (*m_local_image_ctx)->set_exclusive_lock_policy(
    new MirrorExclusiveLockPolicy());
  (*m_local_image_ctx)->set_journal_policy(
    new MirrorJournalPolicy(m_work_queue));

  Context *ctx = create_context_callback<
    OpenLocalImageRequest<I>, &OpenLocalImageRequest<I>::handle_open_image>(
      this);
  (*m_local_image_ctx)->state->open(ctx);
}

template <typename I>
void OpenLocalImageRequest<I>::handle_open_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << "failed to open image '" << m_local_image_id << "': "
         << cpp_strerror(r) << dendl;
    send_close_image(r);
    return;
  } else if ((*m_local_image_ctx)->exclusive_lock == nullptr) {
    derr << "image does not support exclusive lock" << dendl;
    send_close_image(-EINVAL);
    return;
  }

  send_lock_image();
}

template <typename I>
void OpenLocalImageRequest<I>::send_lock_image() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    OpenLocalImageRequest<I>, &OpenLocalImageRequest<I>::handle_lock_image>(
      this);

  RWLock::RLocker owner_locker((*m_local_image_ctx)->owner_lock);
  (*m_local_image_ctx)->exclusive_lock->request_lock(ctx);
}

template <typename I>
void OpenLocalImageRequest<I>::handle_lock_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << "failed to lock image '" << m_local_image_id << "': "
       << cpp_strerror(r) << dendl;
    send_close_image(r);
    return;
  } else if ((*m_local_image_ctx)->exclusive_lock == nullptr ||
             !(*m_local_image_ctx)->exclusive_lock->is_lock_owner()) {
    derr << "image is not locked" << dendl;
    send_close_image(-EBUSY);
    return;
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
    m_local_image_ctx, m_work_queue, ctx);
  request->send();
}

template <typename I>
void OpenLocalImageRequest<I>::handle_close_image(int r) {
  dout(20) << dendl;

  assert(r == 0);
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
