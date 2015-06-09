// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/ObjectPlayer.h"
#include "journal/Utils.h"
#include "common/Timer.h"
#include <limits>

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "ObjectPlayer: "

namespace journal {

namespace {

void rados_ctx_callback(rados_completion_t c, void *arg) {
  Context *ctx = reinterpret_cast<Context *>(arg);
  ctx->complete(rados_aio_get_return_value(c));
}

} // anonymous namespace

ObjectPlayer::ObjectPlayer(librados::IoCtx &ioctx,
                           const std::string &object_oid_prefix,
                           uint64_t object_num, SafeTimer &timer,
                           Mutex &timer_lock, uint8_t order)
  : RefCountedObject(NULL, 0), m_object_num(object_num),
    m_oid(utils::get_object_name(object_oid_prefix, m_object_num)),
    m_cct(NULL), m_timer(timer), m_timer_lock(timer_lock), m_order(order),
    m_watch_interval(0), m_watch_task(NULL), m_watch_fetch(this),
    m_lock(utils::unique_lock_name("ObjectPlayer::m_lock", this)),
    m_fetch_in_progress(false), m_read_off(0), m_watch_ctx(NULL),
    m_watch_ctx_in_progress(false) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
}

ObjectPlayer::~ObjectPlayer() {
  {
    Mutex::Locker locker(m_lock);
    assert(m_watch_ctx == NULL);
  }
}

void ObjectPlayer::fetch(Context *on_finish) {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << dendl;

  Mutex::Locker locker(m_lock);
  m_fetch_in_progress = true;

  C_Fetch *context = new C_Fetch(this, on_finish);
  librados::ObjectReadOperation op;
  op.read(m_read_off, 2 << m_order, &context->read_bl, NULL);

  librados::AioCompletion *rados_completion =
    librados::Rados::aio_create_completion(context, rados_ctx_callback, NULL);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op, 0, NULL);
  assert(r == 0);
  rados_completion->release();
}

void ObjectPlayer::watch(Context *on_fetch, double interval) {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " watch" << dendl;
  {
    Mutex::Locker locker(m_lock);
    assert(m_watch_ctx == NULL);
    m_watch_ctx = on_fetch;
  }
  {
    Mutex::Locker locker(m_timer_lock);
    m_watch_interval = interval;
  }
  schedule_watch();
}

void ObjectPlayer::unwatch() {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " unwatch" << dendl;
  {
    Mutex::Locker locker(m_lock);
    while (m_watch_ctx_in_progress) {
      m_watch_ctx_cond.Wait(m_lock);
    }
    delete m_watch_ctx;
    m_watch_ctx = NULL;
  }
  cancel_watch();
}

void ObjectPlayer::front(Entry *entry) const {
  Mutex::Locker locker(m_lock);
  assert(!m_entries.empty());
  *entry = m_entries.front();
}

void ObjectPlayer::pop_front() {
  Mutex::Locker locker(m_lock);
  assert(!m_entries.empty());
  m_entries.pop_front();
}

int ObjectPlayer::handle_fetch_complete(int r, const bufferlist &bl) {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << ", r=" << r << ", len="
                   << bl.length() << dendl;

  m_fetch_in_progress = false;
  if (r < 0) {
    return r;
  }
  if (bl.length() == 0) {
    return -ENOENT;
  }

  Mutex::Locker locker(m_lock);
  m_read_bl.append(bl);

  bool invalid = false;
  uint32_t invalid_start_off = 0;

  bufferlist::iterator iter(&m_read_bl, m_read_off);
  while (!iter.end()) {
    uint32_t bytes_needed;
    if (!Entry::is_readable(iter, &bytes_needed)) {
      if (bytes_needed != 0) {
        invalid_start_off = iter.get_off();
        invalid = true;
        lderr(m_cct) << ": partial record at offset " << iter.get_off()
                     << dendl;
        break;
      }

      if (!invalid) {
        invalid_start_off = iter.get_off();
        invalid = true;
        lderr(m_cct) << ": detected corrupt journal entry at offset "
                     << invalid_start_off << dendl;
      }
      ++iter;
      continue;
    }

    if (invalid) {
      uint32_t invalid_end_off = iter.get_off();
      lderr(m_cct) << ": corruption range [" << invalid_start_off
                   << ", " << invalid_end_off << ")" << dendl;
      m_invalid_ranges.insert(invalid_start_off, invalid_end_off);
      invalid = false;
    }

    Entry entry;
    ::decode(entry, iter);
    ldout(m_cct, 20) << ": " << entry << " decoded" << dendl;

    EntryKey entry_key(std::make_pair(entry.get_tag(), entry.get_tid()));
    if (m_entry_keys.find(entry_key) == m_entry_keys.end()) {
      m_entry_keys[entry_key] = m_entries.insert(m_entries.end(), entry);
    } else {
      ldout(m_cct, 10) << ": " << entry << " is duplicate, replacing" << dendl;
      *m_entry_keys[entry_key] = entry;
    }
  }

  m_read_off = m_read_bl.length();
  if (invalid) {
    uint32_t invalid_end_off = m_read_bl.length();
    lderr(m_cct) << ": corruption range [" << invalid_start_off
                 << ", " << invalid_end_off << ")" << dendl;
    m_invalid_ranges.insert(invalid_start_off, invalid_end_off);
  }

  if (!m_invalid_ranges.empty()) {
    r = -EINVAL;
  }
  return r;
}

void ObjectPlayer::schedule_watch() {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " scheduling watch" << dendl;
  Mutex::Locker locker(m_timer_lock);
  assert(m_watch_task == NULL);
  m_watch_task = new C_WatchTask(this);
  m_timer.add_event_after(m_watch_interval, m_watch_task);
}

void ObjectPlayer::cancel_watch() {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " cancelling watch" << dendl;
  Mutex::Locker locker(m_timer_lock);
  if (m_watch_task != NULL) {
    m_timer.cancel_event(m_watch_task);
    m_watch_task = NULL;
  }
}

void ObjectPlayer::handle_watch_task() {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << " polling" << dendl;
  {
    Mutex::Locker locker(m_timer_lock);
    m_watch_task = NULL;
  }
  fetch(&m_watch_fetch);
}

void ObjectPlayer::handle_watch_fetched(int r) {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << " poll complete, r=" << r
                   << dendl;
  if (r == -ENOENT) {
    schedule_watch();
    return;
  }

  Context *on_finish;
  {
    Mutex::Locker locker(m_lock);
    m_watch_ctx_in_progress = true;
    on_finish = m_watch_ctx;
    m_watch_ctx = NULL;
  }

  if (on_finish != NULL) {
    on_finish->complete(r);
  }

  {
    Mutex::Locker locker(m_lock);
    m_watch_ctx_in_progress = false;
    m_watch_ctx_cond.Signal();
  }
}

void ObjectPlayer::C_Fetch::finish(int r) {
  r = object_player->handle_fetch_complete(r, read_bl);
  on_finish->complete(r);
  object_player->put();
}

void ObjectPlayer::C_WatchTask::finish(int r) {
  object_player->handle_watch_task();
}

void ObjectPlayer::C_WatchFetch::finish(int r) {
  object_player->handle_watch_fetched(r);
}

} // namespace journal
