// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Journal.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/JournalReplay.h"
#include "librbd/JournalTypes.h"
#include "journal/Journaler.h"
#include "journal/ReplayEntry.h"
#include "common/errno.h"
#include <boost/utility/enable_if.hpp>
#include <boost/type_traits/is_base_of.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Journal: "

namespace librbd {

namespace {

const std::string CLIENT_DESCRIPTION = "master image";

struct C_DestroyJournaler : public Context {
  ::journal::Journaler *journaler;

  C_DestroyJournaler(::journal::Journaler *_journaler) : journaler(_journaler) {
  }
  virtual void finish(int r) {
    delete journaler;
  }
};

struct SetOpRequestTid : public boost::static_visitor<void> {
  uint64_t tid;

  SetOpRequestTid(uint64_t _tid) : tid(_tid) {
  }

  template <typename Event>
  typename boost::enable_if<boost::is_base_of<journal::OpEventBase, Event>,
                            void>::type
  operator()(Event &event) const {
    event.tid = tid;
  }

  template <typename Event>
  typename boost::disable_if<boost::is_base_of<journal::OpEventBase, Event>,
                            void>::type
  operator()(Event &event) const {
    assert(false);
  }
};

struct C_ReplayCommitted : public Context {
  ::journal::Journaler *journaler;
  ::journal::ReplayEntry replay_entry;

  C_ReplayCommitted(::journal::Journaler *journaler,
		    ::journal::ReplayEntry &&replay_entry) :
    journaler(journaler), replay_entry(std::move(replay_entry)) {
  }
  virtual void finish(int r) {
    journaler->committed(replay_entry);
  }
};

} // anonymous namespace

Journal::Journal(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), m_journaler(NULL),
    m_lock("Journal::m_lock"), m_state(STATE_UNINITIALIZED),
    m_lock_listener(this), m_replay_handler(this), m_close_pending(false),
    m_event_lock("Journal::m_event_lock"), m_event_tid(0),
    m_blocking_writes(false), m_journal_replay(NULL) {

  ldout(m_image_ctx.cct, 5) << this << ": ictx=" << &m_image_ctx << dendl;

  m_image_ctx.image_watcher->register_listener(&m_lock_listener);

  Mutex::Locker locker(m_lock);
  block_writes();
}

Journal::~Journal() {
  m_image_ctx.op_work_queue->drain();
  assert(m_journaler == NULL);
  assert(m_journal_replay == NULL);
  assert(m_wait_for_state_contexts.empty());

  m_image_ctx.image_watcher->unregister_listener(&m_lock_listener);

  Mutex::Locker locker(m_lock);
  unblock_writes();
}

bool Journal::is_journal_supported(ImageCtx &image_ctx) {
  assert(image_ctx.snap_lock.is_locked());
  return ((image_ctx.features & RBD_FEATURE_JOURNALING) &&
          !image_ctx.read_only && image_ctx.snap_id == CEPH_NOSNAP);
}

int Journal::create(librados::IoCtx &io_ctx, const std::string &image_id,
		    uint8_t order, uint8_t splay_width,
		    const std::string &object_pool) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 5) << __func__ << ": image=" << image_id << dendl;

  int64_t pool_id = -1;
  if (!object_pool.empty()) {
    librados::Rados rados(io_ctx);
    IoCtx data_io_ctx;
    int r = rados.ioctx_create(object_pool.c_str(), data_io_ctx);
    if (r != 0) {
      lderr(cct) << "failed to create journal: "
		 << "error opening journal objects pool '" << object_pool
		 << "': " << cpp_strerror(r) << dendl;
      return r;
    }
    pool_id = data_io_ctx.get_id();
  }

  ::journal::Journaler journaler(io_ctx, image_id, "",
				 cct->_conf->rbd_journal_commit_age);

  int r = journaler.create(order, splay_width, pool_id);
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

  ::journal::Journaler journaler(io_ctx, image_id, "",
				 cct->_conf->rbd_journal_commit_age);

  bool journal_exists;
  int r = journaler.exists(&journal_exists);
  if (r < 0) {
    lderr(cct) << "failed to stat journal header: " << cpp_strerror(r) << dendl;
    return r;
  } else if (!journal_exists) {
    return 0;
  }

  C_SaferCond cond;
  journaler.init(&cond);

  r = cond.wait();
  if (r == -ENOENT) {
    return 0;
  } else if (r < 0) {
    lderr(cct) << "failed to initialize journal: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = journaler.remove(false);
  if (r < 0) {
    lderr(cct) << "failed to remove journal: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int Journal::reset(librados::IoCtx &io_ctx, const std::string &image_id) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 5) << __func__ << ": image=" << image_id << dendl;

  ::journal::Journaler journaler(io_ctx, image_id, "",
				 cct->_conf->rbd_journal_commit_age);

  C_SaferCond cond;
  journaler.init(&cond);

  int r = cond.wait();
  if (r == -ENOENT) {
    return 0;
  } else if (r < 0) {
    lderr(cct) << "failed to initialize journal: " << cpp_strerror(r) << dendl;
    return r;
  }

  uint8_t order, splay_width;
  int64_t pool_id;
  journaler.get_metadata(&order, &splay_width, &pool_id);

  r = journaler.remove(true);
  if (r < 0) {
    lderr(cct) << "failed to reset journal: " << cpp_strerror(r) << dendl;
    return r;
  }
  r = journaler.create(order, splay_width, pool_id);
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

bool Journal::is_journal_ready() const {
  Mutex::Locker locker(m_lock);
  return (m_state == STATE_RECORDING);
}

bool Journal::is_journal_replaying() const {
  Mutex::Locker locker(m_lock);
  return (m_state == STATE_REPLAYING);
}

void Journal::wait_for_journal_ready(Context *on_ready) {
  Mutex::Locker locker(m_lock);
  schedule_wait_for_ready(on_ready);
}

void Journal::wait_for_journal_ready() {
  Mutex::Locker locker(m_lock);
  while (m_state != STATE_RECORDING) {
    wait_for_state_transition();
  }
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
    case STATE_STOPPING_RECORDING:
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

uint64_t Journal::append_io_event(AioCompletion *aio_comp,
                                  const journal::EventEntry &event_entry,
                                  const AioObjectRequests &requests,
                                  uint64_t offset, size_t length,
                                  bool flush_entry) {
  assert(m_image_ctx.owner_lock.is_locked());

  bufferlist bl;
  ::encode(event_entry, bl);

  ::journal::Future future;
  uint64_t tid;
  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_RECORDING);

    future = m_journaler->append("", bl);

    Mutex::Locker event_locker(m_event_lock);
    tid = ++m_event_tid;
    assert(tid != 0);

    m_events[tid] = Event(future, aio_comp, requests, offset, length);
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": "
                 << "event=" << event_entry.get_event_type() << ", "
                 << "new_reqs=" << requests.size() << ", "
                 << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "flush=" << flush_entry << ", tid=" << tid << dendl;

  Context *on_safe = new C_EventSafe(this, tid);
  if (flush_entry) {
    future.flush(on_safe);
  } else {
    future.wait(on_safe);
  }
  return tid;
}

void Journal::commit_io_event(uint64_t tid, int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": tid=" << tid << ", "
                 "r=" << r << dendl;

  Mutex::Locker event_locker(m_event_lock);
  Events::iterator it = m_events.find(tid);
  if (it == m_events.end()) {
    return;
  }
  complete_event(it, r);
}

void Journal::commit_io_event_extent(uint64_t tid, uint64_t offset,
                                     uint64_t length, int r) {
  assert(length > 0);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": tid=" << tid << ", "
                 << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "r=" << r << dendl;

  Mutex::Locker event_locker(m_event_lock);
  Events::iterator it = m_events.find(tid);
  if (it == m_events.end()) {
    return;
  }

  Event &event = it->second;
  if (event.ret_val == 0 && r < 0) {
    event.ret_val = r;
  }

  ExtentInterval extent;
  extent.insert(offset, length);

  ExtentInterval intersect;
  intersect.intersection_of(extent, event.pending_extents);

  event.pending_extents.subtract(intersect);
  if (!event.pending_extents.empty()) {
    ldout(cct, 20) << "pending extents: " << event.pending_extents << dendl;
    return;
  }
  complete_event(it, event.ret_val);
}

uint64_t Journal::append_op_event(journal::EventEntry &event_entry) {
  assert(m_image_ctx.owner_lock.is_locked());

  uint64_t tid;
  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_RECORDING);

    Mutex::Locker event_locker(m_event_lock);
    tid = ++m_event_tid;
    assert(tid != 0);

    // inject the generated tid into the provided event entry
    boost::apply_visitor(SetOpRequestTid(tid), event_entry.event);

    bufferlist bl;
    ::encode(event_entry, bl);
    m_journaler->committed(m_journaler->append("", bl));
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "event=" << event_entry.get_event_type() << ", "
                 << "tid=" << tid << dendl;
  return tid;
}

void Journal::commit_op_event(uint64_t tid, int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": tid=" << tid << dendl;

  journal::EventEntry event_entry((journal::OpFinishEvent(tid, r)));

  bufferlist bl;
  ::encode(event_entry, bl);

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_RECORDING);

    m_journaler->committed(m_journaler->append("", bl));
  }
}

void Journal::flush_event(uint64_t tid, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": tid=" << tid << ", "
                 << "on_safe=" << on_safe << dendl;

  ::journal::Future future;
  {
    Mutex::Locker event_locker(m_event_lock);
    future = wait_event(m_lock, tid, on_safe);
  }

  if (future.is_valid()) {
    future.flush(NULL);
  }
}

void Journal::wait_event(uint64_t tid, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": tid=" << tid << ", "
                 << "on_safe=" << on_safe << dendl;

  Mutex::Locker event_locker(m_event_lock);
  wait_event(m_lock, tid, on_safe);
}

::journal::Future Journal::wait_event(Mutex &lock, uint64_t tid,
                                      Context *on_safe) {
  assert(m_event_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;

  Events::iterator it = m_events.find(tid);
  if (it == m_events.end() || it->second.safe) {
    // journal entry already safe
    ldout(cct, 20) << "journal entry already safe" << dendl;
    m_image_ctx.op_work_queue->queue(on_safe, 0);
    return ::journal::Future();
  }

  Event &event = it->second;
  event.on_safe_contexts.push_back(on_safe);
  return event.future;
}

void Journal::create_journaler() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  assert(m_lock.is_locked());
  assert(m_state == STATE_UNINITIALIZED);
  assert(m_journaler == NULL);

  m_close_pending = false;
  m_journaler = new ::journal::Journaler(m_image_ctx.md_ctx, m_image_ctx.id, "",
                                         m_image_ctx.journal_commit_age);
  m_journaler->init(new C_InitJournal(this));
  transition_state(STATE_INITIALIZING);
}

void Journal::destroy_journaler() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  assert(m_lock.is_locked());

  delete m_journal_replay;
  m_journal_replay = NULL;

  m_close_pending = false;
  m_image_ctx.op_work_queue->queue(new C_DestroyJournaler(m_journaler), 0);
  m_journaler = NULL;

  transition_state(STATE_UNINITIALIZED);
}

void Journal::complete_event(Events::iterator it, int r) {
  assert(m_event_lock.is_locked());
  assert(m_state == STATE_RECORDING);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": tid=" << it->first << " "
                 << "r=" << r << dendl;

  m_journaler->committed(it->second.future);
  if (it->second.safe) {
    m_events.erase(it);
  }
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

  ldout(cct, 20) << __func__ << ": Journaler" << *m_journaler << dendl;

  m_journal_replay = new JournalReplay(m_image_ctx);

  transition_state(STATE_REPLAYING);
  m_journaler->start_replay(&m_replay_handler);
}

void Journal::handle_replay_ready() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Mutex::Locker locker(m_lock);
  if (m_state != STATE_REPLAYING) {
    return;
  }

  while (true) {
    if (m_close_pending) {
      m_journaler->stop_replay();
      destroy_journaler();
      return;
    }

    ::journal::ReplayEntry replay_entry;
    if (!m_journaler->try_pop_front(&replay_entry)) {
      return;
    }

    m_lock.Unlock();
    bufferlist data = replay_entry.get_data();
    bufferlist::iterator it = data.begin();
    int r = m_journal_replay->process(it, new C_ReplayCommitted(m_journaler,
								std::move(replay_entry)));
    m_lock.Lock();

    if (r < 0) {
      // TODO
    }
  }
}

void Journal::handle_replay_complete(int r) {
  CephContext *cct = m_image_ctx.cct;

  {
    Mutex::Locker locker(m_lock);
    if (m_state != STATE_REPLAYING) {
      return;
    }

    if (r == 0) {
      r = m_journal_replay->flush();
    }
    delete m_journal_replay;
    m_journal_replay = NULL;

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

    m_journaler->start_append(m_image_ctx.journal_object_flush_interval,
			      m_image_ctx.journal_object_flush_bytes,
			      m_image_ctx.journal_object_flush_age);
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

  // TODO: ensure this callback never sees a failure
  AioCompletion *aio_comp;
  AioObjectRequests aio_object_requests;
  Contexts on_safe_contexts;
  {
    Mutex::Locker event_locker(m_event_lock);
    Events::iterator it = m_events.find(tid);
    assert(it != m_events.end());

    Event &event = it->second;
    aio_comp = event.aio_comp;
    aio_object_requests.swap(event.aio_object_requests);
    on_safe_contexts.swap(event.on_safe_contexts);

    if (event.pending_extents.empty()) {
      m_events.erase(it);
    } else {
      event.safe = true;
    }
  }

  ldout(cct, 20) << "completing tid=" << tid << dendl;

  if (r < 0) {
    // don't send aio requests if the journal fails -- bubble error up
    aio_comp->fail(cct, r);
  } else {
    // send any waiting aio requests now that journal entry is safe
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    assert(m_image_ctx.image_watcher->is_lock_owner());

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

  // prevent peers from taking our lock while we are replaying since that
  // will stale forward progress
  return (m_state != STATE_INITIALIZING && m_state != STATE_REPLAYING);
}

void Journal::handle_lock_updated(ImageWatcher::LockUpdateState state) {

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": "
                 << "state=" << state << dendl;

  Mutex::Locker locker(m_lock);
  if (state == ImageWatcher::LOCK_UPDATE_STATE_LOCKED &&
      m_state == STATE_UNINITIALIZED) {
    create_journaler();
  } else if (state == ImageWatcher::LOCK_UPDATE_STATE_RELEASING) {
    if (m_state == STATE_INITIALIZING || m_state == STATE_REPLAYING) {
      // wait for replay to successfully interrupt
      m_close_pending = true;
      wait_for_state_transition();
    }

    if (m_state == STATE_UNINITIALIZED || m_state == STATE_RECORDING) {
      // prevent new write ops but allow pending ops to flush to the journal
      block_writes();
    }
    if (m_state == STATE_RECORDING) {
      flush_journal();
    }
  } else if ((state == ImageWatcher::LOCK_UPDATE_STATE_NOT_SUPPORTED ||
              state == ImageWatcher::LOCK_UPDATE_STATE_UNLOCKED) &&
             m_state != STATE_UNINITIALIZED &&
             m_state != STATE_STOPPING_RECORDING) {
    assert(m_state == STATE_RECORDING);
    {
      Mutex::Locker event_locker(m_event_lock);
      assert(m_events.empty());
    }

    int r = stop_recording();
    if (r < 0) {
      // TODO handle failed journal writes
      assert(false);
    }
  }
}

int Journal::stop_recording() {
  assert(m_lock.is_locked());
  assert(m_journaler != NULL);

  transition_state(STATE_STOPPING_RECORDING);

  C_SaferCond cond;
  m_lock.Unlock();
  m_journaler->stop_append(&cond);
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

void Journal::flush_journal() {
  assert(m_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  m_lock.Unlock();
  C_SaferCond cond_ctx;
  m_journaler->flush(&cond_ctx);
  cond_ctx.wait();
  m_lock.Lock();
}

void Journal::transition_state(State state) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": new state=" << state << dendl;
  assert(m_lock.is_locked());
  m_state = state;
  m_cond.Signal();

  Contexts wait_for_state_contexts;
  wait_for_state_contexts.swap(m_wait_for_state_contexts);
  for (Contexts::iterator it = wait_for_state_contexts.begin();
       it != wait_for_state_contexts.end(); ++it) {
    (*it)->complete(0);
  }
}

void Journal::wait_for_state_transition() {
  assert(m_lock.is_locked());
  State state = m_state;
  while (m_state == state) {
    m_cond.Wait(m_lock);
  }
}

void Journal::schedule_wait_for_ready(Context *on_ready) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << __func__ << ": on_ready=" << on_ready << dendl;

  assert(m_lock.is_locked());
  m_wait_for_state_contexts.push_back(new C_WaitForReady(this, on_ready));
}

void Journal::handle_wait_for_ready(Context *on_ready) {
  assert(m_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": on_ready=" << on_ready << ", "
                 << "state=" << m_state << dendl;

  if (m_state == STATE_RECORDING) {
    m_image_ctx.op_work_queue->queue(on_ready, 0);
  } else {
    schedule_wait_for_ready(on_ready);
  }
}

std::ostream &operator<<(std::ostream &os, const Journal::State &state) {
  switch (state) {
  case Journal::STATE_UNINITIALIZED:
    os << "Uninitialized";
    break;
  case Journal::STATE_INITIALIZING:
    os << "Initializing";
    break;
  case Journal::STATE_REPLAYING:
    os << "Replaying";
    break;
  case Journal::STATE_RECORDING:
    os << "Recording";
    break;
  case Journal::STATE_STOPPING_RECORDING:
    os << "StoppingRecording";
    break;
  default:
    os << "Unknown (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

} // namespace librbd
