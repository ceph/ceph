// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/Request.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"

namespace librbd {
namespace operation {

Request::Request(ImageCtx &image_ctx, Context *on_finish)
  : AsyncRequest(image_ctx, on_finish), m_tid(0) {
}

void Request::send() {
  assert(m_image_ctx.owner_lock.is_locked());

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.journal != NULL &&
        !m_image_ctx.journal->is_journal_replaying()) {
      // journal might be replaying -- wait for it to complete
      if (!m_image_ctx.journal->is_journal_ready()) {
        m_image_ctx.journal->wait_for_journal_ready(
          new C_WaitForJournalReady(this));
        return;
      }

      journal::EventEntry event_entry(create_event());
      m_tid = m_image_ctx.journal->append_op_event(event_entry);
    }
  }

  send_op();
}

void Request::finish(int r) {
  AsyncRequest::finish(r);

  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
  if (m_tid != 0 &&
      m_image_ctx.journal != NULL &&
      !m_image_ctx.journal->is_journal_replaying()) {
    // ops will be canceled / completed before closing journal
    assert(m_image_ctx.journal->is_journal_ready());

    m_image_ctx.journal->commit_op_event(m_tid, r);
  }
}

void Request::handle_journal_ready() {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  send();
}

} // namespace operation
} // namespace librbd
