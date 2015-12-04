// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/Request.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"

namespace librbd {
namespace operation {

template <typename I>
Request<I>::Request(I &image_ctx, Context *on_finish)
  : AsyncRequest<I>(image_ctx, on_finish), m_tid(0) {
}

template <typename I>
void Request<I>::send() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    if (image_ctx.journal != NULL &&
        !image_ctx.journal->is_journal_replaying()) {
      // journal might be replaying -- wait for it to complete
      if (!image_ctx.journal->is_journal_ready()) {
        image_ctx.journal->wait_for_journal_ready(
          new C_WaitForJournalReady(this));
        return;
      }

      journal::EventEntry event_entry(create_event());
      m_tid = image_ctx.journal->append_op_event(event_entry);
    }
  }

  send_op();
}

template <typename I>
void Request<I>::finish(int r) {
  AsyncRequest<I>::finish(r);

  I &image_ctx = this->m_image_ctx;
  RWLock::RLocker snap_locker(image_ctx.snap_lock);
  if (m_tid != 0 &&
      image_ctx.journal != NULL &&
      !image_ctx.journal->is_journal_replaying()) {
    // ops will be canceled / completed before closing journal
    assert(image_ctx.journal->is_journal_ready());

    image_ctx.journal->commit_op_event(m_tid, r);
  }
}

template <typename I>
void Request<I>::handle_journal_ready() {
  I &image_ctx = this->m_image_ctx;
  RWLock::RLocker owner_locker(image_ctx.owner_lock);
  send();
}

} // namespace operation
} // namespace librbd

template class librbd::operation::Request<librbd::ImageCtx>;
