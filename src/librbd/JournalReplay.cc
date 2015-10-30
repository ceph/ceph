// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/JournalReplay.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::JournalReplay: "

namespace librbd {

JournalReplay::JournalReplay(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), m_lock("JournalReplay::m_lock"), m_ret_val(0) {
}

JournalReplay::~JournalReplay() {
  assert(m_aio_completions.empty());
}

int JournalReplay::process(bufferlist::iterator it, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
  journal::EventEntry event_entry;
  try {
    ::decode(event_entry, it);
  } catch (const buffer::error &err) {
    lderr(cct) << "failed to decode event entry: " << err.what() << dendl;
    return -EINVAL;
  }

  boost::apply_visitor(EventVisitor(this, on_safe), event_entry.event);
  return 0;
}

int JournalReplay::flush() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Mutex::Locker locker(m_lock);
  while (!m_aio_completions.empty()) {
    m_cond.Wait(m_lock);
  }
  return m_ret_val;
}

void JournalReplay::handle_event(const journal::AioDiscardEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": AIO discard event" << dendl;

  AioCompletion *aio_comp = create_aio_completion(on_safe);
  AioImageRequest::aio_discard(&m_image_ctx, aio_comp, event.offset,
                               event.length);
}

void JournalReplay::handle_event(const journal::AioWriteEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": AIO write event" << dendl;

  bufferlist data = event.data;
  AioCompletion *aio_comp = create_aio_completion(on_safe);
  AioImageRequest::aio_write(&m_image_ctx, aio_comp, event.offset, event.length,
                             data.c_str(), 0);
}

void JournalReplay::handle_event(const journal::AioFlushEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": AIO flush event" << dendl;

  AioCompletion *aio_comp = create_aio_completion(on_safe);
  AioImageRequest::aio_flush(&m_image_ctx, aio_comp);
}

  void JournalReplay::handle_event(const journal::OpFinishEvent &event,
				   Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Op finish event" << dendl;
}

void JournalReplay::handle_event(const journal::SnapCreateEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap create event" << dendl;
}

void JournalReplay::handle_event(const journal::SnapRemoveEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap remove event" << dendl;
}

void JournalReplay::handle_event(const journal::SnapRenameEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap rename event" << dendl;
}

void JournalReplay::handle_event(const journal::SnapProtectEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap protect event" << dendl;
}

void JournalReplay::handle_event(const journal::SnapUnprotectEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap unprotect event"
                 << dendl;
}

void JournalReplay::handle_event(const journal::SnapRollbackEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Snap rollback start event"
                 << dendl;
}

void JournalReplay::handle_event(const journal::RenameEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Rename event" << dendl;
}

void JournalReplay::handle_event(const journal::ResizeEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Resize start event" << dendl;
}

void JournalReplay::handle_event(const journal::FlattenEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": Flatten start event" << dendl;
}

void JournalReplay::handle_event(const journal::UnknownEvent &event,
				 Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": unknown event" << dendl;
  on_safe->complete(0);
}

AioCompletion *JournalReplay::create_aio_completion(Context *on_safe) {
  Mutex::Locker locker(m_lock);
  AioCompletion *aio_comp = aio_create_completion_internal(
    this, &aio_completion_callback);
  m_aio_completions.insert(std::pair<AioCompletion*,Context*>(
			     aio_comp, on_safe));
  return aio_comp;
}

void JournalReplay::handle_aio_completion(AioCompletion *aio_comp) {
  Mutex::Locker locker(m_lock);

  AioCompletions::iterator it = m_aio_completions.find(aio_comp);
  assert(it != m_aio_completions.end());

  int r = aio_comp->get_return_value();

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": aio_comp=" << aio_comp << ", "
                 << "r=" << r << dendl;

  Context *on_safe = it->second;
  on_safe->complete(r);

  if (r < 0 && m_ret_val == 0) {
    m_ret_val = r;
  }

  m_aio_completions.erase(it);
  if (m_aio_completions.empty())
    m_cond.Signal();
}

void JournalReplay::aio_completion_callback(completion_t cb, void *arg) {
  JournalReplay *journal_replay = reinterpret_cast<JournalReplay *>(arg);
  AioCompletion *aio_comp = reinterpret_cast<AioCompletion *>(cb);

  journal_replay->handle_aio_completion(aio_comp);
  aio_comp->release();
}

} // namespace librbd
