// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalPlayer.h"
#include "common/Finisher.h"
#include "journal/Entry.h"
#include "journal/ReplayHandler.h"
#include "journal/Utils.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalPlayer: "

namespace journal {

namespace {

struct C_HandleComplete : public Context {
  ReplayHandler *replay_handler;

  C_HandleComplete(ReplayHandler *_replay_handler)
    : replay_handler(_replay_handler) {
    replay_handler->get();
  }
  virtual ~C_HandleComplete() {
    replay_handler->put();
  }
  virtual void finish(int r) {
    replay_handler->handle_complete(r);
  }
};

struct C_HandleEntriesAvailable : public Context {
  ReplayHandler *replay_handler;

  C_HandleEntriesAvailable(ReplayHandler *_replay_handler)
      : replay_handler(_replay_handler) {
    replay_handler->get();
  }
  virtual ~C_HandleEntriesAvailable() {
    replay_handler->put();
  }
  virtual void finish(int r) {
    replay_handler->handle_entries_available();
  }
};

} // anonymous namespace

JournalPlayer::JournalPlayer(librados::IoCtx &ioctx,
                             const std::string &object_oid_prefix,
                             const JournalMetadataPtr& journal_metadata,
                             ReplayHandler *replay_handler)
  : m_cct(NULL), m_object_oid_prefix(object_oid_prefix),
    m_journal_metadata(journal_metadata), m_replay_handler(replay_handler),
    m_lock("JournalPlayer::m_lock"), m_state(STATE_INIT), m_splay_offset(0),
    m_watch_enabled(false), m_watch_scheduled(false), m_watch_interval(0),
    m_commit_object(0) {
  m_replay_handler->get();
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());

  ObjectSetPosition commit_position;
  m_journal_metadata->get_commit_position(&commit_position);
  if (!commit_position.entry_positions.empty()) {
    uint8_t splay_width = m_journal_metadata->get_splay_width();
    m_splay_offset = commit_position.object_number % splay_width;
    m_commit_object = commit_position.object_number;
    m_commit_tag = commit_position.entry_positions.front().tag;

    for (EntryPositions::const_iterator it =
           commit_position.entry_positions.begin();
         it != commit_position.entry_positions.end(); ++it) {
      const EntryPosition &entry_position = *it;
      m_commit_tids[entry_position.tag] = entry_position.tid;
    }
  }
}

JournalPlayer::~JournalPlayer() {
  m_async_op_tracker.wait_for_ops();
  m_replay_handler->put();
}

void JournalPlayer::prefetch() {
  Mutex::Locker locker(m_lock);
  assert(m_state == STATE_INIT);
  m_state = STATE_PREFETCH;

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  for (uint8_t splay_index = 0; splay_index < splay_width; ++splay_index) {
    m_prefetch_splay_offsets.insert(splay_index);
  }

  uint64_t object_set = m_commit_object / splay_width;
  uint64_t active_set = m_journal_metadata->get_active_set();

  uint32_t object_count = splay_width *
                          std::min<uint64_t>(2, active_set - object_set + 1);
  ldout(m_cct, 10) << __func__ << ": prefetching " << object_count << " "
                   << "objects" << dendl;

  // prefetch starting from the last known commit set
  uint64_t start_object = object_set * splay_width;
  for (uint64_t object_number = start_object;
       object_number < start_object + object_count; ++object_number) {
    fetch(object_number);
  }
}

void JournalPlayer::prefetch_and_watch(double interval) {
  {
    Mutex::Locker locker(m_lock);
    m_watch_enabled = true;
    m_watch_interval = interval;
  }
  prefetch();
}

void JournalPlayer::unwatch() {
  Mutex::Locker locker(m_lock);
  m_watch_enabled = false;
  if (m_watch_scheduled) {
    ObjectPlayerPtr object_player = get_object_player();
    assert(object_player);

    object_player->unwatch();
    m_watch_scheduled = false;
  }
}

bool JournalPlayer::try_pop_front(Entry *entry, uint64_t *commit_tid) {
  ldout(m_cct, 20) << __func__ << dendl;
  Mutex::Locker locker(m_lock);
  if (m_state != STATE_PLAYBACK) {
    return false;
  }

  ObjectPlayerPtr object_player = get_object_player();
  assert(object_player);

  if (object_player->empty()) {
    if (m_watch_enabled && !m_watch_scheduled) {
      object_player->watch(
        new C_Watch(this, object_player->get_object_number()),
        m_watch_interval);
      m_watch_scheduled = true;
    } else if (!m_watch_enabled && !object_player->is_fetch_in_progress()) {
      ldout(m_cct, 10) << __func__ << ": replay complete" << dendl;
      m_journal_metadata->get_finisher().queue(new C_HandleComplete(
        m_replay_handler), 0);
    }
    return false;
  }

  object_player->front(entry);
  object_player->pop_front();

  uint64_t last_tid;
  if (m_journal_metadata->get_last_allocated_tid(entry->get_tag(), &last_tid) &&
      entry->get_tid() != last_tid + 1) {
    lderr(m_cct) << "missing prior journal entry: " << *entry << dendl;

    m_state = STATE_ERROR;
    m_journal_metadata->get_finisher().queue(new C_HandleComplete(
      m_replay_handler), -EINVAL);
    return false;
  }

  // skip to next splay offset if we cannot apply the next entry in-sequence
  if (!object_player->empty()) {
    Entry peek_entry;
    object_player->front(&peek_entry);
    if (peek_entry.get_tag() == entry->get_tag() ||
        (m_journal_metadata->get_last_allocated_tid(peek_entry.get_tag(),
                                                    &last_tid) &&
         last_tid + 1 != peek_entry.get_tid())) {
      advance_splay_object();
    }
  } else {
    advance_splay_object();
    remove_empty_object_player(object_player);
  }

  m_journal_metadata->reserve_tid(entry->get_tag(), entry->get_tid());
  *commit_tid = m_journal_metadata->allocate_commit_tid(
    object_player->get_object_number(), entry->get_tag(), entry->get_tid());
  return true;
}

void JournalPlayer::process_state(uint64_t object_number, int r) {
  ldout(m_cct, 10) << __func__ << ": object_num=" << object_number << ", "
                   << "r=" << r << dendl;
  if (r >= 0) {
    Mutex::Locker locker(m_lock);
    switch (m_state) {
    case STATE_PREFETCH:
      ldout(m_cct, 10) << "PREFETCH" << dendl;
      r = process_prefetch(object_number);
      break;
    case STATE_PLAYBACK:
      ldout(m_cct, 10) << "PLAYBACK" << dendl;
      r = process_playback(object_number);
      break;
    case STATE_ERROR:
      ldout(m_cct, 10) << "ERROR" << dendl;
      break;
    default:
      lderr(m_cct) << "UNEXPECTED STATE (" << m_state << ")" << dendl;
      assert(false);
      break;
    }
  }

  if (r < 0) {
    {
      Mutex::Locker locker(m_lock);
      m_state = STATE_ERROR;
    }
    m_replay_handler->handle_complete(r);
  }
}

int JournalPlayer::process_prefetch(uint64_t object_number) {
  ldout(m_cct, 10) << __func__ << ": object_num=" << object_number << dendl;
  assert(m_lock.is_locked());

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;

  PrefetchSplayOffsets::iterator it = m_prefetch_splay_offsets.find(
    splay_offset);
  if (it == m_prefetch_splay_offsets.end()) {
    return 0;
  }

  bool prefetch_complete = false;
  assert(m_object_players.count(splay_offset) == 1);
  ObjectPlayers &object_players = m_object_players[splay_offset];

  // prefetch in-order since a newer splay object could prefetch first
  while (!object_players.begin()->second->is_fetch_in_progress()) {
    ObjectPlayerPtr object_player = object_players.begin()->second;

    // skip past known committed records
    if (!m_commit_tids.empty() && !object_player->empty()) {
      ldout(m_cct, 15) << "seeking known commit position in "
                       << object_player->get_oid() << dendl;
      Entry entry;
      while (!m_commit_tids.empty() && !object_player->empty()) {
        object_player->front(&entry);
        if (entry.get_tid() > m_commit_tids[entry.get_tag()]) {
          ldout(m_cct, 10) << "located next uncommitted entry: " << entry
                           << dendl;
          break;
        }

        ldout(m_cct, 20) << "skipping committed entry: " << entry << dendl;
        m_journal_metadata->reserve_tid(entry.get_tag(), entry.get_tid());
        object_player->pop_front();
      }

      // if this object contains the commit position, our read should start with
      // the next consistent journal entry in the sequence
      if (!m_commit_tids.empty() &&
          object_player->get_object_number() == m_commit_object) {
        if (object_player->empty()) {
          advance_splay_object();
        } else {
          Entry entry;
          object_player->front(&entry);
          if (entry.get_tag() == m_commit_tag) {
            advance_splay_object();
          }
        }
      }
    }

    // if the object is empty, pre-fetch the next splay object
    if (!remove_empty_object_player(object_player)) {
      prefetch_complete = true;
      break;
    }
  }

  if (!prefetch_complete) {
    return 0;
  }

  m_prefetch_splay_offsets.erase(it);
  if (!m_prefetch_splay_offsets.empty()) {
    return 0;
  }

  m_state = STATE_PLAYBACK;
  ObjectPlayerPtr object_player = get_object_player();
  if (!object_player->empty()) {
    ldout(m_cct, 10) << __func__ << ": entries available" << dendl;
    m_journal_metadata->get_finisher().queue(new C_HandleEntriesAvailable(
      m_replay_handler), 0);
  } else if (m_watch_enabled) {
    object_player->watch(
      new C_Watch(this, object_player->get_object_number()),
      m_watch_interval);
    m_watch_scheduled = true;
  } else {
    ldout(m_cct, 10) << __func__ << ": no uncommitted entries available"
                     << dendl;
    m_journal_metadata->get_finisher().queue(new C_HandleComplete(
      m_replay_handler), 0);
  }
  return 0;
}

int JournalPlayer::process_playback(uint64_t object_number) {
  ldout(m_cct, 10) << __func__ << ": object_num=" << object_number << dendl;
  assert(m_lock.is_locked());

  m_watch_scheduled = false;

  ObjectPlayerPtr object_player = get_object_player();
  if (object_player->get_object_number() == object_number) {
    uint8_t splay_width = m_journal_metadata->get_splay_width();
    uint64_t active_set = m_journal_metadata->get_active_set();
    uint64_t object_set = object_player->get_object_number() / splay_width;
    if (!object_player->empty()) {
      ldout(m_cct, 10) << __func__ << ": entries available" << dendl;
      m_journal_metadata->get_finisher().queue(new C_HandleEntriesAvailable(
        m_replay_handler), 0);
    } else if (object_set == active_set) {
      ldout(m_cct, 10) << __func__ << ": replay complete" << dendl;
      m_journal_metadata->get_finisher().queue(new C_HandleComplete(
        m_replay_handler), 0);
    }
  }
  return 0;
}

const JournalPlayer::ObjectPlayers &JournalPlayer::get_object_players() const {
  assert(m_lock.is_locked());

  SplayedObjectPlayers::const_iterator it = m_object_players.find(
    m_splay_offset);
  assert(it != m_object_players.end());

  return it->second;
}

ObjectPlayerPtr JournalPlayer::get_object_player() const {
  assert(m_lock.is_locked());

  const ObjectPlayers &object_players = get_object_players();
  return object_players.begin()->second;
}

ObjectPlayerPtr JournalPlayer::get_next_set_object_player() const {
  assert(m_lock.is_locked());

  const ObjectPlayers &object_players = get_object_players();
  return object_players.rbegin()->second;
}

void JournalPlayer::advance_splay_object() {
  assert(m_lock.is_locked());
  ++m_splay_offset;
  m_splay_offset %= m_journal_metadata->get_splay_width();
  ldout(m_cct, 20) << __func__ << ": new offset "
                   << static_cast<uint32_t>(m_splay_offset) << dendl;
}

bool JournalPlayer::remove_empty_object_player(const ObjectPlayerPtr &player) {
  assert(m_lock.is_locked());

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint64_t object_set = player->get_object_number() / splay_width;
  uint64_t active_set = m_journal_metadata->get_active_set();
  if (!player->empty() || object_set == active_set) {
    return false;
  }

  ldout(m_cct, 15) << player->get_oid() << " empty" << dendl;
  ObjectPlayers &object_players = m_object_players[
    player->get_object_number() % splay_width];
  assert(!object_players.empty());

  uint64_t next_object_num = object_players.rbegin()->first + splay_width;
  uint64_t next_object_set = next_object_num / splay_width;
  if (next_object_set <= active_set) {
    fetch(next_object_num);
  }
  object_players.erase(player->get_object_number());
  return true;
}

void JournalPlayer::fetch(uint64_t object_num) {
  assert(m_lock.is_locked());

  std::string oid = utils::get_object_name(m_object_oid_prefix, object_num);

  ldout(m_cct, 10) << __func__ << ": " << oid << dendl;
  C_Fetch *fetch_ctx = new C_Fetch(this, object_num);
  ObjectPlayerPtr object_player(new ObjectPlayer(
    m_ioctx, m_object_oid_prefix, object_num, m_journal_metadata->get_timer(),
    m_journal_metadata->get_timer_lock(), m_journal_metadata->get_order()));

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  m_object_players[object_num % splay_width][object_num] = object_player;
  object_player->fetch(fetch_ctx);
}

void JournalPlayer::handle_fetched(uint64_t object_num, int r) {
  ldout(m_cct, 10) << __func__ << ": "
                   << utils::get_object_name(m_object_oid_prefix, object_num)
                   << ": r=" << r << dendl;
  if (r == -ENOENT) {
    r = 0;
  }
  if (r == 0) {
    Mutex::Locker locker(m_lock);
    uint8_t splay_width = m_journal_metadata->get_splay_width();
    uint8_t splay_offset = object_num % splay_width;
    assert(m_object_players.count(splay_offset) == 1);
    ObjectPlayers &object_players = m_object_players[splay_offset];

    assert(object_players.count(object_num) == 1);
    ObjectPlayerPtr object_player = object_players[object_num];
    remove_empty_object_player(object_player);
  }

  process_state(object_num, r);
}

void JournalPlayer::handle_watch(uint64_t object_num, int r) {
  ldout(m_cct, 10) << __func__ << ": "
                   << utils::get_object_name(m_object_oid_prefix, object_num)
                   << ": r=" << r << dendl;
  process_state(object_num, r);
}

} // namespace journal
