// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalMetadata.h"
#include "journal/Utils.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "cls/journal/cls_journal_client.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalMetadata: "

namespace journal {

using namespace cls::journal;

JournalMetadata::JournalMetadata(librados::IoCtx &ioctx,
                                 const std::string &oid,
                                 const std::string &client_id,
                                 double commit_interval)
    : m_cct(NULL), m_oid(oid), m_client_id(client_id),
      m_commit_interval(commit_interval), m_order(0), m_splay_width(0),
      m_initialized(false), m_timer(NULL),
      m_timer_lock("JournalMetadata::m_timer_lock"),
      m_lock("JournalMetadata::m_lock"), m_watch_ctx(this), m_watch_handle(0),
      m_update_notifications(0), m_commit_position_pending(false),
      m_commit_position_ctx(NULL) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
}

JournalMetadata::~JournalMetadata() {
  if (m_timer != NULL) {
    Mutex::Locker locker(m_timer_lock);
    m_timer->shutdown();
    delete m_timer;
    m_timer = NULL;
  }

  m_ioctx.unwatch2(m_watch_handle);
  librados::Rados rados(m_ioctx);
  rados.watch_flush();
}

int JournalMetadata::init() {
  assert(!m_initialized);
  m_initialized = true;

  int r = client::get_immutable_metadata(m_ioctx, m_oid, &m_order,
                                         &m_splay_width);
  if (r < 0) {
    lderr(m_cct) << __func__ << ": failed to retrieve journal metadata: "
                 << cpp_strerror(r) << dendl;
    return r;
  }

  m_timer = new SafeTimer(m_cct, m_timer_lock, false);
  m_timer->init();

  r = m_ioctx.watch2(m_oid, &m_watch_handle, &m_watch_ctx);
  if (r < 0) {
    lderr(m_cct) << __func__ << ": failed to watch journal"
                 << cpp_strerror(r) << dendl;
    return r;
  }

  C_SaferCond cond;
  refresh(&cond);
  r = cond.wait();
  if (r < 0) {
    return r;
  }
  return 0;
}

int JournalMetadata::register_client(const std::string &description) {
  assert(!m_client_id.empty());

  ldout(m_cct, 10) << __func__ << ": " << m_client_id << dendl;
  int r = client::client_register(m_ioctx, m_oid, m_client_id, description);
  if (r < 0) {
    lderr(m_cct) << "failed to register journal client '" << m_client_id
                 << "': " << cpp_strerror(r) << dendl;
    return r;
  }

  notify_update();
  return 0;
}

int JournalMetadata::unregister_client() {
  assert(!m_client_id.empty());

  ldout(m_cct, 10) << __func__ << ": " << m_client_id << dendl;
  int r = client::client_unregister(m_ioctx, m_oid, m_client_id);
  if (r < 0) {
    lderr(m_cct) << "failed to unregister journal client '" << m_client_id
                 << "': " << cpp_strerror(r) << dendl;
    return r;
  }

  notify_update();
  return 0;
}

void JournalMetadata::add_listener(Listener *listener) {
  Mutex::Locker locker(m_lock);
  while (m_update_notifications > 0) {
    m_update_cond.Wait(m_lock);
  }
  m_listeners.push_back(listener);
}

void JournalMetadata::remove_listener(Listener *listener) {
  Mutex::Locker locker(m_lock);
  while (m_update_notifications > 0) {
    m_update_cond.Wait(m_lock);
  }
  m_listeners.remove(listener);
}

void JournalMetadata::set_minimum_set(uint64_t object_set) {
  Mutex::Locker locker(m_lock);

  ldout(m_cct, 20) << __func__ << ": current=" << m_minimum_set
                   << ", new=" << object_set << dendl;
  if (m_minimum_set >= object_set) {
    return;
  }

  librados::ObjectWriteOperation op;
  client::set_minimum_set(&op, object_set);

  C_NotifyUpdate *ctx = new C_NotifyUpdate(this);
  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, comp, &op);
  assert(r == 0);
  comp->release();

  m_minimum_set = object_set;
}

void JournalMetadata::set_active_set(uint64_t object_set) {
  Mutex::Locker locker(m_lock);

  ldout(m_cct, 20) << __func__ << ": current=" << m_active_set
                   << ", new=" << object_set << dendl;
  if (m_active_set >= object_set) {
    return;
  }

  librados::ObjectWriteOperation op;
  client::set_active_set(&op, object_set);

  C_NotifyUpdate *ctx = new C_NotifyUpdate(this);
  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, comp, &op);
  assert(r == 0);
  comp->release();

  m_active_set = object_set;
}

void JournalMetadata::set_commit_position(
    const ObjectSetPosition &commit_position, Context *on_safe) {
  assert(on_safe != NULL);

  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << __func__ << ": current=" << m_client.commit_position
                   << ", new=" << commit_position << dendl;
  if (commit_position <= m_client.commit_position ||
      commit_position <= m_commit_position) {
    on_safe->complete(-ESTALE);
    return;
  }

  if (m_commit_position_ctx != NULL) {
    m_commit_position_ctx->complete(-ESTALE);
  }

  m_client.commit_position = commit_position;
  m_commit_position = commit_position;
  m_commit_position_ctx = on_safe;
  schedule_commit_task();
}

void JournalMetadata::reserve_tid(const std::string &tag, uint64_t tid) {
  Mutex::Locker locker(m_lock);
  uint64_t &allocated_tid = m_allocated_tids[tag];
  if (allocated_tid <= tid) {
    allocated_tid = tid + 1;
  }
}

bool JournalMetadata::get_last_allocated_tid(const std::string &tag,
                                             uint64_t *tid) const {
  Mutex::Locker locker(m_lock);

  AllocatedTids::const_iterator it = m_allocated_tids.find(tag);
  if (it == m_allocated_tids.end()) {
    return false;
  }

  assert(it->second > 0);
  *tid = it->second - 1;
  return true;
}

void JournalMetadata::refresh(Context *on_complete) {
  ldout(m_cct, 10) << "refreshing journal metadata" << dendl;
  C_Refresh *refresh = new C_Refresh(this, on_complete);
  client::get_mutable_metadata(m_ioctx, m_oid, &refresh->minimum_set,
                               &refresh->active_set,
                               &refresh->registered_clients, refresh);
}

void JournalMetadata::handle_refresh_complete(C_Refresh *refresh, int r) {
  ldout(m_cct, 10) << "refreshed journal metadata: r=" << r << dendl;
  if (r == 0) {
    Mutex::Locker locker(m_lock);

    Client client(m_client_id, "");
    RegisteredClients::iterator it = refresh->registered_clients.find(client);
    if (it != refresh->registered_clients.end()) {
      m_minimum_set = refresh->minimum_set;
      m_active_set = refresh->active_set;
      m_registered_clients = refresh->registered_clients;
      m_client = *it;

      ++m_update_notifications;
      m_lock.Unlock();
      for (Listeners::iterator it = m_listeners.begin();
           it != m_listeners.end(); ++it) {
        (*it)->handle_update(this);
      }
      m_lock.Lock();
      if (--m_update_notifications == 0) {
        m_update_cond.Signal();
      }
    } else {
      lderr(m_cct) << "failed to locate client: " << m_client_id << dendl;
      r = -ENOENT;
    }
  }

  if (refresh->on_finish != NULL) {
    refresh->on_finish->complete(r);
  }
}

void JournalMetadata::schedule_commit_task() {
  assert(m_lock.is_locked());

  Mutex::Locker timer_locker(m_timer_lock);
  if (!m_commit_position_pending) {
    m_commit_position_pending = true;
    m_timer->add_event_after(m_commit_interval, new C_CommitPositionTask(this));
  }
}

void JournalMetadata::handle_commit_position_task() {
  Mutex::Locker locker(m_lock);

  librados::ObjectWriteOperation op;
  client::client_commit(&op, m_client_id, m_commit_position);

  C_NotifyUpdate *ctx = new C_NotifyUpdate(this, m_commit_position_ctx);
  m_commit_position_ctx = NULL;

  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

void JournalMetadata::schedule_watch_reset() {
  Mutex::Locker locker(m_timer_lock);
  m_timer->add_event_after(0.1, new C_WatchReset(this));
}

void JournalMetadata::handle_watch_reset() {
  int r = m_ioctx.watch2(m_oid, &m_watch_handle, &m_watch_ctx);
  if (r < 0) {
    lderr(m_cct) << __func__ << ": failed to watch journal"
                 << cpp_strerror(r) << dendl;
    schedule_watch_reset();
  } else {
    ldout(m_cct, 10) << __func__ << ": reset journal watch" << dendl;
    refresh(NULL);
  }
}

void JournalMetadata::handle_watch_notify(uint64_t notify_id, uint64_t cookie) {
  ldout(m_cct, 10) << "journal header updated" << dendl;

  bufferlist bl;
  m_ioctx.notify_ack(m_oid, notify_id, cookie, bl);

  refresh(NULL);
}

void JournalMetadata::handle_watch_error(int err) {
  lderr(m_cct) << "journal watch error: " << cpp_strerror(err) << dendl;
  schedule_watch_reset();
}

void JournalMetadata::notify_update() {
  ldout(m_cct, 10) << "notifying journal header update" << dendl;

  bufferlist bl;
  m_ioctx.notify2(m_oid, bl, 5000, NULL);
}

void JournalMetadata::async_notify_update() {
  ldout(m_cct, 10) << "async notifying journal header update" << dendl;

  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(NULL, NULL, NULL);

  bufferlist bl;
  int r = m_ioctx.aio_notify(m_oid, comp, bl, 5000, NULL);
  assert(r == 0);

  comp->release();
}

} // namespace journal
