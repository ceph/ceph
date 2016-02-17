// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalMetadata.h"
#include "journal/Utils.h"
#include "common/errno.h"
#include "common/Finisher.h"
#include "common/Timer.h"
#include "cls/journal/cls_journal_client.h"
#include <functional>
#include <set>

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalMetadata: "

namespace journal {

using namespace cls::journal;

namespace {

// does not compare object number
inline bool entry_positions_less_equal(const ObjectSetPosition &lhs,
                                       const ObjectSetPosition &rhs) {
  if (lhs.entry_positions == rhs.entry_positions) {
    return true;
  }

  if (lhs.entry_positions.size() < rhs.entry_positions.size()) {
    return true;
  } else if (lhs.entry_positions.size() > rhs.entry_positions.size()) {
    return false;
  }

  std::map<uint64_t, uint64_t> rhs_tids;
  for (EntryPositions::const_iterator it = rhs.entry_positions.begin();
       it != rhs.entry_positions.end(); ++it) {
    rhs_tids[it->tag_tid] = it->entry_tid;
  }

  for (EntryPositions::const_iterator it = lhs.entry_positions.begin();
       it != lhs.entry_positions.end(); ++it) {
    const EntryPosition &entry_position = *it;
    if (entry_position.entry_tid < rhs_tids[entry_position.tag_tid]) {
      return true;
    }
  }
  return false;
}

struct C_AllocateTag : public Context {
  CephContext *cct;
  librados::IoCtx &ioctx;
  const std::string &oid;
  AsyncOpTracker &async_op_tracker;
  uint64_t tag_class;
  Tag *tag;
  Context *on_finish;

  bufferlist out_bl;

  C_AllocateTag(CephContext *cct, librados::IoCtx &ioctx,
                const std::string &oid, AsyncOpTracker &async_op_tracker,
                uint64_t tag_class, const bufferlist &data, Tag *tag,
                Context *on_finish)
    : cct(cct), ioctx(ioctx), oid(oid), async_op_tracker(async_op_tracker),
      tag_class(tag_class), tag(tag), on_finish(on_finish) {
    async_op_tracker.start_op();
    tag->data = data;
  }
  virtual ~C_AllocateTag() {
    async_op_tracker.finish_op();
  }

  void send() {
    send_get_next_tag_tid();
  }

  void send_get_next_tag_tid() {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << dendl;

    librados::ObjectReadOperation op;
    client::get_next_tag_tid_start(&op);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_AllocateTag, &C_AllocateTag::handle_get_next_tag_tid>);

    out_bl.clear();
    int r = ioctx.aio_operate(oid, comp, &op, &out_bl);
    assert(r == 0);
    comp->release();
  }

  void handle_get_next_tag_tid(int r) {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << ": r=" << r << dendl;

    if (r == 0) {
      bufferlist::iterator iter = out_bl.begin();
      r = client::get_next_tag_tid_finish(&iter, &tag->tid);
    }
    if (r < 0) {
      complete(r);
      return;
    }
    send_tag_create();
  }

  void send_tag_create() {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << dendl;

    librados::ObjectWriteOperation op;
    client::tag_create(&op, tag->tid, tag_class, tag->data);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_AllocateTag, &C_AllocateTag::handle_tag_create>);

    int r = ioctx.aio_operate(oid, comp, &op);
    assert(r == 0);
    comp->release();
  }

  void handle_tag_create(int r) {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << ": r=" << r << dendl;

    if (r == -ESTALE) {
      send_get_next_tag_tid();
      return;
    } else if (r < 0) {
      complete(r);
      return;
    }

    send_get_tag();
  }

  void send_get_tag() {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << dendl;

    librados::ObjectReadOperation op;
    client::get_tag_start(&op, tag->tid);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_AllocateTag, &C_AllocateTag::handle_get_tag>);

    out_bl.clear();
    int r = ioctx.aio_operate(oid, comp, &op, &out_bl);
    assert(r == 0);
    comp->release();
  }

  void handle_get_tag(int r) {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << ": r=" << r << dendl;

    if (r == 0) {
      bufferlist::iterator iter = out_bl.begin();

      cls::journal::Tag journal_tag;
      r = client::get_tag_finish(&iter, &journal_tag);
      if (r == 0) {
        *tag = journal_tag;
      }
    }
    complete(r);
  }

  virtual void finish(int r) override {
    on_finish->complete(r);
  }
};

struct C_GetTags : public Context {
  CephContext *cct;
  librados::IoCtx &ioctx;
  const std::string &oid;
  const std::string &client_id;
  AsyncOpTracker &async_op_tracker;
  boost::optional<uint64_t> tag_class;
  JournalMetadata::Tags *tags;
  Context *on_finish;

  const uint64_t MAX_RETURN = 64;
  uint64_t start_after_tag_tid = 0;
  bufferlist out_bl;

  C_GetTags(CephContext *cct, librados::IoCtx &ioctx, const std::string &oid,
            const std::string &client_id, AsyncOpTracker &async_op_tracker,
            const boost::optional<uint64_t> &tag_class,
            JournalMetadata::Tags *tags, Context *on_finish)
    : cct(cct), ioctx(ioctx), oid(oid), client_id(client_id),
      async_op_tracker(async_op_tracker), tag_class(tag_class), tags(tags),
      on_finish(on_finish) {
    async_op_tracker.start_op();
  }
  virtual ~C_GetTags() {
    async_op_tracker.finish_op();
  }

  void send() {
    send_tag_list();
  }

  void send_tag_list() {
    librados::ObjectReadOperation op;
    client::tag_list_start(&op, start_after_tag_tid, MAX_RETURN, client_id,
                           tag_class);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_GetTags, &C_GetTags::handle_tag_list>);

    out_bl.clear();
    int r = ioctx.aio_operate(oid, comp, &op, &out_bl);
    assert(r == 0);
    comp->release();
  }

  void handle_tag_list(int r) {
    if (r == 0) {
      std::set<cls::journal::Tag> journal_tags;
      bufferlist::iterator iter = out_bl.begin();
      r = client::tag_list_finish(&iter, &journal_tags);
      if (r == 0) {
        for (auto &journal_tag : journal_tags) {
          tags->push_back(journal_tag);
          start_after_tag_tid = journal_tag.tid;
        }

        if (journal_tags.size() == MAX_RETURN) {
          send_tag_list();
          return;
        }
      }
    }
    complete(r);
  }

  virtual void finish(int r) override {
    on_finish->complete(r);
  }
};

} // anonymous namespace

JournalMetadata::JournalMetadata(librados::IoCtx &ioctx,
                                 const std::string &oid,
                                 const std::string &client_id,
                                 double commit_interval)
    : RefCountedObject(NULL, 0), m_cct(NULL), m_oid(oid),
      m_client_id(client_id), m_commit_interval(commit_interval), m_order(0),
      m_splay_width(0), m_pool_id(-1), m_initialized(false), m_finisher(NULL),
      m_timer(NULL), m_timer_lock("JournalMetadata::m_timer_lock"),
      m_lock("JournalMetadata::m_lock"), m_commit_tid(0), m_watch_ctx(this),
      m_watch_handle(0), m_minimum_set(0), m_active_set(0),
      m_update_notifications(0), m_commit_position_ctx(NULL),
      m_commit_position_task_ctx(NULL) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
}

JournalMetadata::~JournalMetadata() {
  if (m_initialized) {
    shutdown();
  }
}

void JournalMetadata::init(Context *on_init) {
  assert(!m_initialized);
  m_initialized = true;

  m_finisher = new Finisher(m_cct);
  m_finisher->start();

  m_timer = new SafeTimer(m_cct, m_timer_lock, true);
  m_timer->init();

  int r = m_ioctx.watch2(m_oid, &m_watch_handle, &m_watch_ctx);
  if (r < 0) {
    lderr(m_cct) << __func__ << ": failed to watch journal"
                 << cpp_strerror(r) << dendl;
    on_init->complete(r);
    return;
  }

  C_ImmutableMetadata *ctx = new C_ImmutableMetadata(this, on_init);
  client::get_immutable_metadata(m_ioctx, m_oid, &m_order, &m_splay_width,
                                 &m_pool_id, ctx);
}

void JournalMetadata::shutdown() {

  ldout(m_cct, 20) << __func__ << dendl;

  assert(m_initialized);
  {
    Mutex::Locker locker(m_lock);
    m_initialized = false;

    if (m_watch_handle != 0) {
      m_ioctx.unwatch2(m_watch_handle);
      m_watch_handle = 0;
    }
  }

  flush_commit_position();

  if (m_timer != NULL) {
    Mutex::Locker locker(m_timer_lock);
    m_timer->shutdown();
    delete m_timer;
    m_timer = NULL;
  }

  if (m_finisher != NULL) {
    m_finisher->stop();
    delete m_finisher;
    m_finisher = NULL;
  }

  librados::Rados rados(m_ioctx);
  rados.watch_flush();

  m_async_op_tracker.wait_for_ops();
  m_ioctx.aio_flush();
}

int JournalMetadata::register_client(const bufferlist &data) {
  ldout(m_cct, 10) << __func__ << ": " << m_client_id << dendl;
  int r = client::client_register(m_ioctx, m_oid, m_client_id, data);
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

void JournalMetadata::allocate_tag(uint64_t tag_class, const bufferlist &data,
                                   Tag *tag, Context *on_finish) {
  C_AllocateTag *ctx = new C_AllocateTag(m_cct, m_ioctx, m_oid,
                                         m_async_op_tracker, tag_class,
                                         data, tag, on_finish);
  ctx->send();
}

void JournalMetadata::get_tags(const boost::optional<uint64_t> &tag_class,
                               Tags *tags, Context *on_finish) {
  C_GetTags *ctx = new C_GetTags(m_cct, m_ioctx, m_oid, m_client_id,
                                 m_async_op_tracker, tag_class,
                                 tags, on_finish);
  ctx->send();
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

void JournalMetadata::flush_commit_position() {

  ldout(m_cct, 20) << __func__ << dendl;

  {
    Mutex::Locker timer_locker(m_timer_lock);
    Mutex::Locker locker(m_lock);
    if (m_commit_position_task_ctx == NULL) {
      return;
    }

    m_timer->cancel_event(m_commit_position_task_ctx);
    m_commit_position_task_ctx = NULL;
  }
  handle_commit_position_task();
}

void JournalMetadata::set_commit_position(
    const ObjectSetPosition &commit_position, Context *on_safe) {
  assert(on_safe != NULL);

  Context *stale_ctx = nullptr;
  {
    Mutex::Locker timer_locker(m_timer_lock);
    Mutex::Locker locker(m_lock);
    ldout(m_cct, 20) << __func__ << ": current=" << m_client.commit_position
                     << ", new=" << commit_position << dendl;
    if (entry_positions_less_equal(commit_position, m_client.commit_position) ||
        entry_positions_less_equal(commit_position, m_commit_position)) {
      stale_ctx = on_safe;
    } else {
      stale_ctx = m_commit_position_ctx;

      m_client.commit_position = commit_position;
      m_commit_position = commit_position;
      m_commit_position_ctx = on_safe;
      schedule_commit_task();
    }
  }

  if (stale_ctx != nullptr) {
    stale_ctx->complete(-ESTALE);
  }
}

void JournalMetadata::reserve_entry_tid(uint64_t tag_tid, uint64_t entry_tid) {
  Mutex::Locker locker(m_lock);
  uint64_t &allocated_entry_tid = m_allocated_entry_tids[tag_tid];
  if (allocated_entry_tid <= entry_tid) {
    allocated_entry_tid = entry_tid + 1;
  }
}

bool JournalMetadata::get_last_allocated_entry_tid(uint64_t tag_tid,
                                                   uint64_t *entry_tid) const {
  Mutex::Locker locker(m_lock);

  AllocatedEntryTids::const_iterator it = m_allocated_entry_tids.find(tag_tid);
  if (it == m_allocated_entry_tids.end()) {
    return false;
  }

  assert(it->second > 0);
  *entry_tid = it->second - 1;
  return true;
}

void JournalMetadata::handle_immutable_metadata(int r, Context *on_init) {
  if (r < 0) {
    lderr(m_cct) << "failed to initialize immutable metadata: "
                 << cpp_strerror(r) << dendl;
    on_init->complete(r);
    return;
  }

  ldout(m_cct, 10) << "initialized immutable metadata" << dendl;
  refresh(on_init);
}

void JournalMetadata::refresh(Context *on_complete) {
  ldout(m_cct, 10) << "refreshing mutable metadata" << dendl;
  C_Refresh *refresh = new C_Refresh(this, on_complete);
  client::get_mutable_metadata(m_ioctx, m_oid, &refresh->minimum_set,
                               &refresh->active_set,
                               &refresh->registered_clients, refresh);
}

void JournalMetadata::handle_refresh_complete(C_Refresh *refresh, int r) {
  ldout(m_cct, 10) << "refreshed mutable metadata: r=" << r << dendl;
  if (r == 0) {
    Mutex::Locker locker(m_lock);

    Client client(m_client_id, bufferlist());
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

  ldout(m_cct, 20) << __func__ << dendl;

  assert(m_timer_lock.is_locked());
  assert(m_lock.is_locked());

  if (m_commit_position_task_ctx == NULL) {
    m_commit_position_task_ctx = new C_CommitPositionTask(this);
    m_timer->add_event_after(m_commit_interval, m_commit_position_task_ctx);
  }
}

void JournalMetadata::handle_commit_position_task() {

  ldout(m_cct, 20) << __func__ << dendl;

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

  m_commit_position_task_ctx = NULL;
}

void JournalMetadata::schedule_watch_reset() {
  assert(m_timer_lock.is_locked());
  m_timer->add_event_after(0.1, new C_WatchReset(this));
}

void JournalMetadata::handle_watch_reset() {
  assert(m_timer_lock.is_locked());
  if (!m_initialized) {
    return;
  }

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
  Mutex::Locker timer_locker(m_timer_lock);
  Mutex::Locker locker(m_lock);

  // release old watch on error
  if (m_watch_handle != 0) {
    m_ioctx.unwatch2(m_watch_handle);
    m_watch_handle = 0;
  }

  if (m_initialized && err != -ENOENT) {
    schedule_watch_reset();
  }
}

uint64_t JournalMetadata::allocate_commit_tid(uint64_t object_num,
                                              uint64_t tag_tid,
                                              uint64_t entry_tid) {
  Mutex::Locker locker(m_lock);
  uint64_t commit_tid = ++m_commit_tid;
  m_pending_commit_tids[commit_tid] = CommitEntry(object_num, tag_tid,
                                                  entry_tid);

  ldout(m_cct, 20) << "allocated commit tid: commit_tid=" << commit_tid << " ["
                   << "object_num=" << object_num << ", "
                   << "tag_tid=" << tag_tid << ", entry_tid=" << entry_tid << "]"
                   << dendl;
  return commit_tid;
}

bool JournalMetadata::committed(uint64_t commit_tid,
                                ObjectSetPosition *object_set_position) {
  ldout(m_cct, 20) << "committed tid=" << commit_tid << dendl;

  Mutex::Locker locker(m_lock);
  {
    CommitTids::iterator it = m_pending_commit_tids.find(commit_tid);
    assert(it != m_pending_commit_tids.end());

    CommitEntry &commit_entry = it->second;
    commit_entry.committed = true;
  }

  if (!m_commit_position.entry_positions.empty()) {
    *object_set_position = m_commit_position;
  } else {
    *object_set_position = m_client.commit_position;
  }

  bool update_commit_position = false;
  while (!m_pending_commit_tids.empty()) {
    CommitTids::iterator it = m_pending_commit_tids.begin();
    CommitEntry &commit_entry = it->second;
    if (!commit_entry.committed) {
      break;
    }

    object_set_position->object_number = commit_entry.object_num;
    if (!object_set_position->entry_positions.empty() &&
        object_set_position->entry_positions.front().tag_tid ==
          commit_entry.tag_tid) {
      object_set_position->entry_positions.front() = EntryPosition(
        commit_entry.tag_tid, commit_entry.entry_tid);
    } else {
      object_set_position->entry_positions.push_front(EntryPosition(
        commit_entry.tag_tid, commit_entry.entry_tid));
    }
    m_pending_commit_tids.erase(it);
    update_commit_position = true;
  }

  if (update_commit_position) {
    // prune the position to have unique tags in commit-order
    std::set<uint64_t> in_use_tag_tids;
    EntryPositions::iterator it = object_set_position->entry_positions.begin();
    while (it != object_set_position->entry_positions.end()) {
      if (!in_use_tag_tids.insert(it->tag_tid).second) {
        it = object_set_position->entry_positions.erase(it);
      } else {
        ++it;
      }
    }

    ldout(m_cct, 20) << "updated object set position: " << *object_set_position
                     << dendl;
  }
  return update_commit_position;
}

void JournalMetadata::notify_update() {
  ldout(m_cct, 10) << "notifying journal header update" << dendl;

  bufferlist bl;
  m_ioctx.notify2(m_oid, bl, 5000, NULL);
}

void JournalMetadata::async_notify_update() {
  ldout(m_cct, 10) << "async notifying journal header update" << dendl;

  C_AioNotify *ctx = new C_AioNotify(this);
  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);

  bufferlist bl;
  int r = m_ioctx.aio_notify(m_oid, comp, bl, 5000, NULL);
  assert(r == 0);

  comp->release();
}

void JournalMetadata::handle_notified(int r) {
  ldout(m_cct, 10) << "notified journal header update: r=" << r << dendl;
}

std::ostream &operator<<(std::ostream &os,
			 const JournalMetadata::RegisteredClients &clients) {
  os << "[";
  for (JournalMetadata::RegisteredClients::const_iterator c = clients.begin();
       c != clients.end(); ++c) {
    os << (c == clients.begin() ? "" : ", " ) << *c;
  }
  os << "]";
  return os;
}

std::ostream &operator<<(std::ostream &os,
			 const JournalMetadata &jm) {
  Mutex::Locker locker(jm.m_lock);
  os << "[oid=" << jm.m_oid << ", "
     << "initialized=" << jm.m_initialized << ", "
     << "order=" << (int)jm.m_order << ", "
     << "splay_width=" << (int)jm.m_splay_width << ", "
     << "pool_id=" << jm.m_pool_id << ", "
     << "minimum_set=" << jm.m_minimum_set << ", "
     << "active_set=" << jm.m_active_set << ", "
     << "client_id=" << jm.m_client_id << ", "
     << "commit_tid=" << jm.m_commit_tid << ", "
     << "commit_interval=" << jm.m_commit_interval << ", "
     << "commit_position=" << jm.m_commit_position << ", "
     << "registered_clients=" << jm.m_registered_clients << "]";
  return os;
}

} // namespace journal
