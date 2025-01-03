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

namespace {

bool advance_to_last_pad_byte(uint32_t off, bufferlist::const_iterator *iter,
                              uint32_t *pad_len, bool *partial_entry) {
  const uint32_t MAX_PAD = 8;
  auto pad_bytes = MAX_PAD - off % MAX_PAD;
  auto next = *iter;

  ceph_assert(!next.end());
  if (*next != '\0') {
    return false;
  }

  for (auto i = pad_bytes - 1; i > 0; i--) {
    if ((++next).end()) {
      *partial_entry = true;
      return false;
    }
    if (*next != '\0') {
      return false;
    }
  }

  *iter = next;
  *pad_len += pad_bytes;
  return true;
}

} // anonymous namespace

ObjectPlayer::ObjectPlayer(librados::IoCtx &ioctx,
                           const std::string& object_oid_prefix,
                           uint64_t object_num, SafeTimer &timer,
                           ceph::mutex &timer_lock, uint8_t order,
                           uint64_t max_fetch_bytes)
  : m_object_num(object_num),
    m_oid(utils::get_object_name(object_oid_prefix, m_object_num)),
    m_timer(timer), m_timer_lock(timer_lock), m_order(order),
    m_max_fetch_bytes(max_fetch_bytes > 0 ? max_fetch_bytes : 2 << order),
    m_lock(ceph::make_mutex(utils::unique_lock_name("ObjectPlayer::m_lock", this)))
{
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
}

ObjectPlayer::~ObjectPlayer() {
  {
    std::lock_guard timer_locker{m_timer_lock};
    std::lock_guard locker{m_lock};
    ceph_assert(!m_fetch_in_progress);
    ceph_assert(m_watch_ctx == nullptr);
  }
}

void ObjectPlayer::fetch(Context *on_finish) {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << dendl;

  std::lock_guard locker{m_lock};
  ceph_assert(!m_fetch_in_progress);
  m_fetch_in_progress = true;

  C_Fetch *context = new C_Fetch(this, on_finish);
  librados::ObjectReadOperation op;
  op.read(m_read_off, m_max_fetch_bytes, &context->read_bl, NULL);
  op.set_op_flags2(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  auto rados_completion =
    librados::Rados::aio_create_completion(context, utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op, 0, NULL);
  ceph_assert(r == 0);
  rados_completion->release();
}

void ObjectPlayer::watch(Context *on_fetch, double interval) {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " watch" << dendl;

  std::lock_guard timer_locker{m_timer_lock};
  m_watch_interval = interval;

  ceph_assert(m_watch_ctx == nullptr);
  m_watch_ctx = on_fetch;

  schedule_watch();
}

void ObjectPlayer::unwatch() {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " unwatch" << dendl;
  Context *watch_ctx = nullptr;
  {
    std::lock_guard timer_locker{m_timer_lock};
    ceph_assert(!m_unwatched);
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
  std::lock_guard locker{m_lock};
  ceph_assert(!m_entries.empty());
  *entry = m_entries.front();
}

void ObjectPlayer::pop_front() {
  std::lock_guard locker{m_lock};
  ceph_assert(!m_entries.empty());

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

  std::lock_guard locker{m_lock};
  ceph_assert(m_fetch_in_progress);
  m_read_off += bl.length();
  m_read_bl.append(bl);
  m_refetch_state = REFETCH_STATE_REQUIRED;

  bool full_fetch = (m_max_fetch_bytes == 2U << m_order);
  bool partial_entry = false;
  bool invalid = false;
  uint32_t invalid_start_off = 0;

  clear_invalid_range(m_read_bl_off, m_read_bl.length());
  bufferlist::const_iterator iter{&m_read_bl, 0};
  uint32_t pad_len = 0;
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

      if (!advance_to_last_pad_byte(m_read_bl_off + iter.get_off(), &iter,
                                    &pad_len, &partial_entry)) {
        invalid_start_off = m_read_bl_off + bl_off;
        invalid = true;
        if (partial_entry) {
          if (full_fetch) {
            lderr(m_cct) << ": partial pad at offset " << invalid_start_off
                         << dendl;
          } else {
            ldout(m_cct, 20) << ": partial pad detected, will re-fetch"
                             << dendl;
          }
        } else {
          lderr(m_cct) << ": detected corrupt journal entry at offset "
                       << invalid_start_off << dendl;
        }
        break;
      }
      ++iter;
      continue;
    }

    Entry entry;
    decode(entry, iter);
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

      m_read_bl_off = invalid_end_off;
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
    m_read_bl_off += entry_len + pad_len;
    pad_len = 0;
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
  ceph_assert(ceph_mutex_is_locked(m_timer_lock));
  if (m_watch_ctx == NULL) {
    return;
  }

  ldout(m_cct, 20) << __func__ << ": " << m_oid << " scheduling watch" << dendl;
  ceph_assert(m_watch_task == nullptr);
  m_watch_task = m_timer.add_event_after(
    m_watch_interval,
    new LambdaContext([this](int) {
	handle_watch_task();
      }));
}

bool ObjectPlayer::cancel_watch() {
  ceph_assert(ceph_mutex_is_locked(m_timer_lock));
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " cancelling watch" << dendl;
  if (m_watch_task != nullptr) {
    bool canceled = m_timer.cancel_event(m_watch_task);
    ceph_assert(canceled);

    m_watch_task = nullptr;
    return true;
  }
  return false;
}

void ObjectPlayer::handle_watch_task() {
  ceph_assert(ceph_mutex_is_locked(m_timer_lock));

  ldout(m_cct, 10) << __func__ << ": " << m_oid << " polling" << dendl;
  ceph_assert(m_watch_ctx != nullptr);
  ceph_assert(m_watch_task != nullptr);

  m_watch_task = nullptr;
  fetch(new C_WatchFetch(this));
}

void ObjectPlayer::handle_watch_fetched(int r) {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << " poll complete, r=" << r
                   << dendl;

  Context *watch_ctx = nullptr;
  {
    std::lock_guard timer_locker{m_timer_lock};
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
    std::lock_guard locker{object_player->m_lock};
    object_player->m_fetch_in_progress = false;
  }

  if (refetch) {
    object_player->fetch(on_finish);
    return;
  }

  object_player.reset();
  on_finish->complete(r);
}

void ObjectPlayer::C_WatchFetch::finish(int r) {
  object_player->handle_watch_fetched(r);
}

} // namespace journal
