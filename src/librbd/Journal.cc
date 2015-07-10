// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Journal.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/JournalTypes.h"
#include "journal/Journaler.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Journal: "

namespace librbd {

namespace {

const std::string CLIENT_DESCRIPTION = "master image";

} // anonymous namespace

Journal::Journal(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), m_journaler(NULL),
    m_lock("Journal::m_lock"), m_state(STATE_UNINITIALIZED),
    m_lock_listener(this), m_replay_handler(this), m_close_pending(false),
    m_next_tid(0), m_blocking_writes(false) {

  ldout(m_image_ctx.cct, 5) << this << ": ictx=" << &m_image_ctx << dendl;

  m_image_ctx.image_watcher->register_listener(&m_lock_listener);

  Mutex::Locker locker(m_lock);
  block_writes();
}

Journal::~Journal() {
  assert(m_journaler == NULL);

  m_image_ctx.image_watcher->unregister_listener(&m_lock_listener);

  Mutex::Locker locker(m_lock);
  unblock_writes();
}

bool Journal::is_journal_supported(ImageCtx &image_ctx) {
  assert(image_ctx.snap_lock.is_locked());
  return ((image_ctx.features & RBD_FEATURE_JOURNALING) &&
          !image_ctx.read_only && image_ctx.snap_id == CEPH_NOSNAP);
}

int Journal::create(librados::IoCtx &io_ctx, const std::string &image_id) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 5) << __func__ << ": image=" << image_id << dendl;

  ::journal::Journaler journaler(io_ctx, io_ctx, image_id, "");

  // TODO order / splay width via config / image metadata
  int r = journaler.create(24, 4);
  if (r < 0) {
    lderr(cct) << "failed to create journal: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = journaler.register_client(CLIENT_DESCRIPTION);
  if (r < 0) {
    lderr(cct) << "failed to register client: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int Journal::remove(librados::IoCtx &io_ctx, const std::string &image_id) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 5) << __func__ << ": image=" << image_id << dendl;

  return 0;
}

bool Journal::is_journal_ready() const {
  Mutex::Locker locker(m_lock);
  return (m_state == STATE_RECORDING);
}

void Journal::open() {
  Mutex::Locker locker(m_lock);
  if (m_journaler != NULL) {
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;
  create_journaler();
}

int Journal::close() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": state=" << m_state << dendl;

  Mutex::Locker locker(m_lock);
  if (m_state == STATE_UNINITIALIZED) {
    return 0;
  }

  int r;
  bool done = false;
  while (!done) {
    switch (m_state) {
    case STATE_UNINITIALIZED:
      done = true;
      break;
    case STATE_INITIALIZING:
    case STATE_REPLAYING:
      m_close_pending = true;
      wait_for_state_transition();
      break;
    case STATE_RECORDING:
      r = stop_recording();
      if (r < 0) {
        return r;
      }
      done = true;
      break;
    default:
      assert(false);
    }
  }

  destroy_journaler();
  return 0;
}

uint64_t Journal::append_event(AioCompletion *aio_comp,
                               const journal::EventEntry &event_entry,
                               const AioObjectRequests &requests,
                               bool flush_entry) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_state == STATE_RECORDING);

  bufferlist bl;
  ::encode(event_entry, bl);

  ::journal::Future future = m_journaler->append("", bl);
  uint64_t tid;
  {
    Mutex::Locker locker(m_lock);
    tid = m_next_tid++;
    m_events[tid] = Event(future, aio_comp, requests);
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": "
                 << "event=" << event_entry.get_event_type() << ", "
                 << "new_reqs=" << requests.size() << ", "
                 << "flush=" << flush_entry << ", tid=" << tid << dendl;

  Context *on_safe = new C_EventSafe(this, tid);
  if (flush_entry) {
    future.flush(on_safe);
  } else {
    future.wait(on_safe);
  }
  return tid;
}

void Journal::create_journaler() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  assert(m_lock.is_locked());
  assert(m_state == STATE_UNINITIALIZED);

  // TODO allow alternate pool for journal objects
  m_close_pending = false;
  m_journaler = new ::journal::Journaler(m_image_ctx.md_ctx, m_image_ctx.md_ctx,
                                         m_image_ctx.id, "");

  m_journaler->init(new C_InitJournal(this));
  transition_state(STATE_INITIALIZING);
}

void Journal::destroy_journaler() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  assert(m_lock.is_locked());

  m_close_pending = false;
  delete m_journaler;
  m_journaler = NULL;
  transition_state(STATE_UNINITIALIZED);
}

void Journal::handle_initialized(int r) {
  CephContext *cct = m_image_ctx.cct;
  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": r=" << r << dendl;
    Mutex::Locker locker(m_lock);

    // TODO: failed to open journal -- retry?
    destroy_journaler();
    create_journaler();
    return;
  }

  ldout(cct, 20) << this << " " << __func__ << dendl;
  Mutex::Locker locker(m_lock);
  if (m_close_pending) {
    destroy_journaler();
    return;
  }

  transition_state(STATE_REPLAYING);
  m_journaler->start_replay(&m_replay_handler);
}

void Journal::handle_replay_ready() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Mutex::Locker locker(m_lock);
  while (true) {
    if (m_close_pending) {
      m_journaler->stop_replay();
      destroy_journaler();
      return;
    }

    ::journal::Payload payload;
    if (!m_journaler->try_pop_front(&payload)) {
      return;
    }

    m_lock.Unlock();
    // TODO process the payload
    m_lock.Lock();
  }
}

void Journal::handle_replay_complete(int r) {
  CephContext *cct = m_image_ctx.cct;

  {
    Mutex::Locker locker(m_lock);
    if (r < 0) {
      lderr(cct) << this << " " << __func__ << ": r=" << r << dendl;

      // TODO: failed to replay journal -- retry?
      destroy_journaler();
      create_journaler();
      return;
    }

    ldout(cct, 20) << this << " " << __func__ << dendl;
    m_journaler->stop_replay();

    if (m_close_pending) {
      destroy_journaler();
      return;
    }

    m_journaler->start_append();
    transition_state(STATE_RECORDING);

    unblock_writes();
  }

  // kick peers to let them know they can re-request the lock now
  m_image_ctx.image_watcher->notify_lock_state();
}

void Journal::handle_event_safe(int r, uint64_t tid) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << ", "
                 << "tid=" << tid << dendl;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);

  AioCompletion *aio_comp;
  AioObjectRequests aio_object_requests;
  Contexts on_safe_contexts;
  {
    Mutex::Locker locker(m_lock);
    Events::iterator it = m_events.find(tid);
    assert(it != m_events.end());

    Event &event = it->second;
    aio_comp = event.aio_comp;
    aio_object_requests.swap(event.aio_object_requests);
    on_safe_contexts.swap(event.on_safe_contexts);
    m_events.erase(it);
  }

  ldout(cct, 20) << "completing tid=" << tid << dendl;

  assert(m_image_ctx.image_watcher->is_lock_owner());

  if (r < 0) {
    // don't send aio requests if the journal fails -- bubble error up
    aio_comp->fail(cct, r);
  } else {
    // send any waiting aio requests now that journal entry is safe
    for (AioObjectRequests::iterator it = aio_object_requests.begin();
         it != aio_object_requests.end(); ++it) {
      (*it)->send();
    }
  }

  // alert the cache about the journal event status
  for (Contexts::iterator it = on_safe_contexts.begin();
       it != on_safe_contexts.end(); ++it) {
    (*it)->complete(r);
  }
}

bool Journal::handle_requested_lock() {
  Mutex::Locker locker(m_lock);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": " << "state=" << m_state
                 << dendl;

  // prevent peers from taking our lock while we are replaying
  return (m_state != STATE_INITIALIZING && m_state != STATE_REPLAYING);
}

void Journal::handle_releasing_lock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Mutex::Locker locker(m_lock);
  if (m_state == STATE_INITIALIZING || m_state == STATE_REPLAYING) {
    // wait for replay to successfully interrupt
    m_close_pending = true;
    wait_for_state_transition();
  }

  if (m_state == STATE_UNINITIALIZED || m_state == STATE_RECORDING) {
    // prevent new write ops but allow pending ops to flush to the journal
    block_writes();
  }
}

void Journal::handle_lock_updated(bool lock_owner) {

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": "
                 << "owner=" << lock_owner << dendl;

  Mutex::Locker locker(m_lock);
  if (lock_owner && m_state == STATE_UNINITIALIZED) {
    create_journaler();
  } else if (!lock_owner && m_state != STATE_UNINITIALIZED) {
    assert(m_state == STATE_RECORDING);
    assert(m_events.empty());
    int r = stop_recording();
    if (r < 0) {
      // TODO handle failed journal writes
      assert(false);
    }
  }
}

int Journal::stop_recording() {
  C_SaferCond cond;

  m_journaler->stop_append(&cond);

  m_lock.Unlock();
  int r = cond.wait();
  m_lock.Lock();

  destroy_journaler();
  if (r < 0) {
    lderr(m_image_ctx.cct) << "failed to flush journal: " << cpp_strerror(r)
                           << dendl;
    return r;
  }
  return 0;
}

void Journal::block_writes() {
  assert(m_lock.is_locked());
  if (!m_blocking_writes) {
    m_blocking_writes = true;
    m_image_ctx.aio_work_queue->block_writes();
  }
}

void Journal::unblock_writes() {
  assert(m_lock.is_locked());
  if (m_blocking_writes) {
    m_blocking_writes = false;
    m_image_ctx.aio_work_queue->unblock_writes();
  }
}

void Journal::transition_state(State state) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": new state=" << state << dendl;
  assert(m_lock.is_locked());
  m_state = state;
  m_cond.Signal();
}

void Journal::wait_for_state_transition() {
  assert(m_lock.is_locked());
  State state = m_state;
  while (m_state == state) {
    m_cond.Wait(m_lock);
  }
}

} // namespace librbd
