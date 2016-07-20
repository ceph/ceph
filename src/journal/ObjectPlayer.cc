// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/ObjectPlayer.h"
#include "journal/Utils.h"
#include "common/Timer.h"
#include <limits>

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "ObjectPlayer: " << this << " "

namespace journal {

ObjectPlayer::ObjectPlayer(librados::IoCtx &ioctx,
                           const std::string &object_oid_prefix,
                           uint64_t object_num, SafeTimer &timer,
                           Mutex &timer_lock, uint8_t order,
                           uint64_t max_fetch_bytes)
  : RefCountedObject(NULL, 0), m_object_num(object_num),
    m_oid(utils::get_object_name(object_oid_prefix, m_object_num)),
    m_cct(NULL), m_timer(timer), m_timer_lock(timer_lock), m_order(order),
    m_max_fetch_bytes(max_fetch_bytes > 0 ? max_fetch_bytes : 2 << order),
    m_watch_interval(0), m_watch_task(NULL),
    m_lock(utils::unique_lock_name("ObjectPlayer::m_lock", this)),
    m_fetch_in_progress(false) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
}

ObjectPlayer::~ObjectPlayer() {
  {
    Mutex::Locker timer_locker(m_timer_lock);
    Mutex::Locker locker(m_lock);
    assert(!m_fetch_in_progress);
    assert(m_watch_ctx == nullptr);
  }
}

void ObjectPlayer::fetch(Context *on_finish) {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << dendl;

  Mutex::Locker locker(m_lock);
  assert(!m_fetch_in_progress);
  m_fetch_in_progress = true;

  C_Fetch *context = new C_Fetch(this, on_finish);
  librados::ObjectReadOperation op;
  op.read(m_read_off, m_max_fetch_bytes, &context->read_bl, NULL);
  op.set_op_flags2(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  librados::AioCompletion *rados_completion =
    librados::Rados::aio_create_completion(context, utils::rados_ctx_callback,
                                           NULL);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op, 0, NULL);
  assert(r == 0);
  rados_completion->release();
}

void ObjectPlayer::watch(Context *on_fetch, double interval) {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " watch" << dendl;

  Mutex::Locker timer_locker(m_timer_lock);
  m_watch_interval = interval;

  assert(m_watch_ctx == nullptr);
  m_watch_ctx = on_fetch;

  schedule_watch();
}

void ObjectPlayer::unwatch() {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " unwatch" << dendl;
  Context *watch_ctx = nullptr;
  {
    Mutex::Locker timer_locker(m_timer_lock);
    assert(!m_unwatched);
    m_unwatched = true;

    if (!cancel_watch()) {
      return;
    }

    std::swap(watch_ctx, m_watch_ctx);
  }

  if (watch_ctx != nullptr) {
    watch_ctx->complete(-ECANCELED);
  }
}

void ObjectPlayer::front(Entry *entry) const {
  Mutex::Locker locker(m_lock);
  assert(!m_entries.empty());
  *entry = m_entries.front();
}

void ObjectPlayer::pop_front() {
  Mutex::Locker locker(m_lock);
  assert(!m_entries.empty());

  auto &entry = m_entries.front();
  m_entry_keys.erase({entry.get_tag_tid(), entry.get_entry_tid()});
  m_entries.pop_front();
}

int ObjectPlayer::handle_fetch_complete(int r, const bufferlist &bl,
                                        bool *refetch) {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << ", r=" << r << ", len="
                   << bl.length() << dendl;

  *refetch = false;
  if (r == -ENOENT) {
    return 0;
  } else if (r < 0) {
    return r;
  } else if (bl.length() == 0) {
    return 0;
  }

  Mutex::Locker locker(m_lock);
  assert(m_fetch_in_progress);
  m_read_off += bl.length();
  m_read_bl.append(bl);
  m_refetch_state = REFETCH_STATE_REQUIRED;

  bool full_fetch = (m_max_fetch_bytes == 2U << m_order);
  bool partial_entry = false;
  bool invalid = false;
  uint32_t invalid_start_off = 0;

  clear_invalid_range(m_read_bl_off, m_read_bl.length());
  bufferlist::iterator iter(&m_read_bl, 0);
  while (!iter.end()) {
    uint32_t bytes_needed;
    uint32_t bl_off = iter.get_off();
    if (!Entry::is_readable(iter, &bytes_needed)) {
      if (bytes_needed != 0) {
        invalid_start_off = m_read_bl_off + bl_off;
        invalid = true;
        partial_entry = true;
        if (full_fetch) {
          lderr(m_cct) << ": partial record at offset " << invalid_start_off
                       << dendl;
        } else {
          ldout(m_cct, 20) << ": partial record detected, will re-fetch"
                           << dendl;
        }
        break;
      }

      if (!invalid) {
        invalid_start_off = m_read_bl_off + bl_off;
        invalid = true;
        lderr(m_cct) << ": detected corrupt journal entry at offset "
                     << invalid_start_off << dendl;
      }
      ++iter;
      continue;
    }

    Entry entry;
    ::decode(entry, iter);
    ldout(m_cct, 20) << ": " << entry << " decoded" << dendl;

    uint32_t entry_len = iter.get_off() - bl_off;
    if (invalid) {
      // new corrupt region detected
      uint32_t invalid_end_off = m_read_bl_off + bl_off;
      lderr(m_cct) << ": corruption range [" << invalid_start_off
                   << ", " << invalid_end_off << ")" << dendl;
      m_invalid_ranges.insert(invalid_start_off,
                              invalid_end_off - invalid_start_off);
      invalid = false;
    }

    EntryKey entry_key(std::make_pair(entry.get_tag_tid(),
                                      entry.get_entry_tid()));
    if (m_entry_keys.find(entry_key) == m_entry_keys.end()) {
      m_entry_keys[entry_key] = m_entries.insert(m_entries.end(), entry);
    } else {
      ldout(m_cct, 10) << ": " << entry << " is duplicate, replacing" << dendl;
      *m_entry_keys[entry_key] = entry;
    }

    // prune decoded / corrupted journal entries from front of bl
    bufferlist sub_bl;
    sub_bl.substr_of(m_read_bl, iter.get_off(),
                     m_read_bl.length() - iter.get_off());
    sub_bl.swap(m_read_bl);
    iter = bufferlist::iterator(&m_read_bl, 0);

    // advance the decoded entry offset
    m_read_bl_off += entry_len;
  }

  if (invalid) {
    uint32_t invalid_end_off = m_read_bl_off + m_read_bl.length();
    if (!partial_entry) {
      lderr(m_cct) << ": corruption range [" << invalid_start_off
                   << ", " << invalid_end_off << ")" << dendl;
    }
    m_invalid_ranges.insert(invalid_start_off,
                            invalid_end_off - invalid_start_off);
  }

  if (!m_invalid_ranges.empty() && !partial_entry) {
    return -EBADMSG;
  } else if (partial_entry && (full_fetch || m_entries.empty())) {
    *refetch = true;
    return -EAGAIN;
  }

  return 0;
}

void ObjectPlayer::clear_invalid_range(uint32_t off, uint32_t len) {
  // possibly remove previously partial record region
  InvalidRanges decode_range;
  decode_range.insert(off, len);
  InvalidRanges intersect_range;
  intersect_range.intersection_of(m_invalid_ranges, decode_range);
  if (!intersect_range.empty()) {
    ldout(m_cct, 20) << ": clearing invalid range: " << intersect_range
                     << dendl;
    m_invalid_ranges.subtract(intersect_range);
  }
}

void ObjectPlayer::schedule_watch() {
  assert(m_timer_lock.is_locked());
  if (m_watch_ctx == NULL) {
    return;
  }

  ldout(m_cct, 20) << __func__ << ": " << m_oid << " scheduling watch" << dendl;
  assert(m_watch_task == NULL);
  m_watch_task = new C_WatchTask(this);
  m_timer.add_event_after(m_watch_interval, m_watch_task);
}

bool ObjectPlayer::cancel_watch() {
  assert(m_timer_lock.is_locked());
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " cancelling watch" << dendl;
  if (m_watch_task != nullptr) {
    bool canceled = m_timer.cancel_event(m_watch_task);
    assert(canceled);

    m_watch_task = nullptr;
    return true;
  }
  return false;
}

void ObjectPlayer::handle_watch_task() {
  assert(m_timer_lock.is_locked());

  ldout(m_cct, 10) << __func__ << ": " << m_oid << " polling" << dendl;
  assert(m_watch_ctx != nullptr);
  assert(m_watch_task != nullptr);

  m_watch_task = nullptr;
  fetch(new C_WatchFetch(this));
}

void ObjectPlayer::handle_watch_fetched(int r) {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << " poll complete, r=" << r
                   << dendl;

  Context *watch_ctx = nullptr;
  {
    Mutex::Locker timer_locker(m_timer_lock);
    std::swap(watch_ctx, m_watch_ctx);

    if (m_unwatched) {
      m_unwatched = false;
      r = -ECANCELED;
    }
  }

  if (watch_ctx != nullptr) {
    watch_ctx->complete(r);
  }
}

void ObjectPlayer::C_Fetch::finish(int r) {
  bool refetch = false;
  r = object_player->handle_fetch_complete(r, read_bl, &refetch);

  {
    Mutex::Locker locker(object_player->m_lock);
    object_player->m_fetch_in_progress = false;
  }

  if (refetch) {
    object_player->fetch(on_finish);
    return;
  }

  object_player.reset();
  on_finish->complete(r);
}

void ObjectPlayer::C_WatchTask::finish(int r) {
  object_player->handle_watch_task();
}

void ObjectPlayer::C_WatchFetch::finish(int r) {
  object_player->handle_watch_fetched(r);
}

} // namespace journal
