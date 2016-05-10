// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalPlayer.h"
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

  explicit C_HandleComplete(ReplayHandler *_replay_handler)
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

  explicit C_HandleEntriesAvailable(ReplayHandler *_replay_handler)
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

  if (!commit_position.object_positions.empty()) {
    ldout(m_cct, 5) << "commit position: " << commit_position << dendl;

    // start replay after the last committed entry's object
    uint8_t splay_width = m_journal_metadata->get_splay_width();
    auto &active_position = commit_position.object_positions.front();
    m_active_tag_tid = active_position.tag_tid;
    m_commit_object = active_position.object_number;
    m_splay_offset = m_commit_object % splay_width;
    for (auto &position : commit_position.object_positions) {
      uint8_t splay_offset = position.object_number % splay_width;
      m_commit_positions[splay_offset] = position;
    }
  }
}

JournalPlayer::~JournalPlayer() {
  m_async_op_tracker.wait_for_ops();
  {
    Mutex::Locker locker(m_lock);
    assert(m_fetch_object_numbers.empty());
  }
  m_replay_handler->put();
}

void JournalPlayer::prefetch() {
  Mutex::Locker locker(m_lock);
  assert(m_state == STATE_INIT);
  m_state = STATE_PREFETCH;

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  for (uint8_t splay_offset = 0; splay_offset < splay_width; ++splay_offset) {
    m_prefetch_splay_offsets.insert(splay_offset);
  }

  // compute active object for each splay offset (might be before
  // active set)
  std::map<uint8_t, uint64_t> splay_offset_to_objects;
  for (auto &position : m_commit_positions) {
    assert(splay_offset_to_objects.count(position.first) == 0);
    splay_offset_to_objects[position.first] = position.second.object_number;
  }

  // prefetch the active object for each splay offset (and the following object)
  uint64_t active_set = m_journal_metadata->get_active_set();
  uint64_t max_object_number = (splay_width * (active_set + 1)) - 1;
  std::set<uint64_t> prefetch_object_numbers;
  for (uint8_t splay_offset = 0; splay_offset < splay_width; ++splay_offset) {
    uint64_t object_number = splay_offset;
    if (splay_offset_to_objects.count(splay_offset) != 0) {
      object_number = splay_offset_to_objects[splay_offset];
    }

    prefetch_object_numbers.insert(object_number);
    if (object_number + splay_width <= max_object_number) {
      prefetch_object_numbers.insert(object_number + splay_width);
    }
  }

  ldout(m_cct, 10) << __func__ << ": prefetching "
                   << prefetch_object_numbers.size() << " " << "objects"
                   << dendl;
  for (auto object_number : prefetch_object_numbers) {
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
  ldout(m_cct, 20) << __func__ << dendl;
  Mutex::Locker locker(m_lock);
  m_watch_enabled = false;
  if (m_watch_scheduled) {
    for (auto &players : m_object_players) {
      players.second.begin()->second->unwatch();
    }
    m_watch_scheduled = false;
  }
}

bool JournalPlayer::try_pop_front(Entry *entry, uint64_t *commit_tid) {
  ldout(m_cct, 20) << __func__ << dendl;
  Mutex::Locker locker(m_lock);

  if (m_state != STATE_PLAYBACK) {
    m_handler_notified = false;
    return false;
  }

  if (!is_object_set_ready()) {
    m_handler_notified = false;
    return false;
  }

  if (!verify_playback_ready()) {
    if (!m_watch_enabled) {
      notify_complete(0);
    } else if (!m_watch_scheduled) {
      m_handler_notified = false;
      schedule_watch();
    }
    return false;
  }

  ObjectPlayerPtr object_player = get_object_player();
  assert(object_player && !object_player->empty());

  object_player->front(entry);
  object_player->pop_front();

  uint64_t last_entry_tid;
  if (m_active_tag_tid && *m_active_tag_tid != entry->get_tag_tid()) {
    lderr(m_cct) << "unexpected tag in journal entry: " << *entry << dendl;

    m_state = STATE_ERROR;
    notify_complete(-ENOMSG);
    return false;
  } else if (m_journal_metadata->get_last_allocated_entry_tid(
               entry->get_tag_tid(), &last_entry_tid) &&
             entry->get_entry_tid() != last_entry_tid + 1) {
    lderr(m_cct) << "missing prior journal entry: " << *entry << dendl;

    m_state = STATE_ERROR;
    notify_complete(-ENOMSG);
    return false;
  }

  m_active_tag_tid = entry->get_tag_tid();
  advance_splay_object();
  remove_empty_object_player(object_player);

  m_journal_metadata->reserve_entry_tid(entry->get_tag_tid(),
                                        entry->get_entry_tid());
  *commit_tid = m_journal_metadata->allocate_commit_tid(
    object_player->get_object_number(), entry->get_tag_tid(),
    entry->get_entry_tid());
  return true;
}

void JournalPlayer::process_state(uint64_t object_number, int r) {
  ldout(m_cct, 10) << __func__ << ": object_num=" << object_number << ", "
                   << "r=" << r << dendl;

  assert(m_lock.is_locked());
  if (r >= 0) {
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
    m_state = STATE_ERROR;
    notify_complete(r);
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
  while (m_fetch_object_numbers.count(
           object_players.begin()->second->get_object_number()) == 0) {
    ObjectPlayerPtr object_player = object_players.begin()->second;
    uint64_t player_object_number = object_player->get_object_number();

    // skip past known committed records
    if (m_commit_positions.count(splay_offset) != 0 &&
        !object_player->empty()) {
      ObjectPosition &position = m_commit_positions[splay_offset];

      ldout(m_cct, 15) << "seeking known commit position " << position << " in "
                       << object_player->get_oid() << dendl;

      bool found_commit = false;
      Entry entry;
      while (!object_player->empty()) {
        object_player->front(&entry);

        if (entry.get_tag_tid() == position.tag_tid &&
            entry.get_entry_tid() == position.entry_tid) {
          found_commit = true;
        } else if (found_commit) {
          ldout(m_cct, 10) << "located next uncommitted entry: " << entry
                           << dendl;
          break;
        }

        ldout(m_cct, 20) << "skipping committed entry: " << entry << dendl;
        m_journal_metadata->reserve_entry_tid(entry.get_tag_tid(),
                                              entry.get_entry_tid());
        object_player->pop_front();
      }

      // if this object contains the commit position, our read should start with
      // the next consistent journal entry in the sequence
      if (player_object_number == m_commit_object) {
        if (object_player->empty()) {
          advance_splay_object();
        } else {
          Entry entry;
          object_player->front(&entry);
          if (entry.get_tag_tid() == position.tag_tid) {
            advance_splay_object();
          }
        }
      }

      // do not search for commit position for this object
      // if we've already seen it
      if (found_commit) {
        m_commit_positions.erase(splay_offset);
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
  if (!is_object_set_ready()) {
    ldout(m_cct, 10) << __func__ << ": waiting for full object set" << dendl;
  } else if (verify_playback_ready()) {
    notify_entries_available();
  } else if (m_watch_enabled) {
    schedule_watch();
  } else {
    ldout(m_cct, 10) << __func__ << ": no uncommitted entries available"
                     << dendl;
    notify_complete(0);
  }
  return 0;
}

int JournalPlayer::process_playback(uint64_t object_number) {
  ldout(m_cct, 10) << __func__ << ": object_num=" << object_number << dendl;
  assert(m_lock.is_locked());

  if (!is_object_set_ready()) {
    return 0;
  }

  ObjectPlayerPtr object_player = get_object_player();
  if (verify_playback_ready()) {
    notify_entries_available();
  } else if (m_watch_enabled) {
    schedule_watch();
  } else {
    uint8_t splay_width = m_journal_metadata->get_splay_width();
    uint64_t active_set = m_journal_metadata->get_active_set();
    uint64_t object_set = object_player->get_object_number() / splay_width;
    if (object_set == active_set) {
      notify_complete(0);
    }
  }
  return 0;
}

bool JournalPlayer::is_object_set_ready() const {
  assert(m_lock.is_locked());
  if (m_watch_scheduled || !m_fetch_object_numbers.empty()) {
    return false;
  }
  return true;
}

bool JournalPlayer::verify_playback_ready() {
  assert(m_lock.is_locked());
  assert(is_object_set_ready());

  ObjectPlayerPtr object_player = get_object_player();
  assert(object_player);

  // Verify is the active object player has another entry available
  // in the sequence
  Entry entry;
  bool entry_available = false;
  if (!object_player->empty()) {
    entry_available = true;
    object_player->front(&entry);
    if (!m_active_tag_tid || entry.get_tag_tid() == *m_active_tag_tid) {
      return true;
    }
  }

  // if we just advanced to this object, make sure we have the latest
  // set of data before advancing to a new tag
  if (m_watch_enabled && m_watch_required) {
    m_watch_required = false;
    return false;
  }

  // NOTE: replay currently does not check tag class to playback multiple tags
  // from different classes (issue #14909).  When a new tag is discovered, it
  // is assumed that the previous tag was closed at the last replayable entry.
  object_player = m_object_players.begin()->second.begin()->second;
  if (!object_player->empty() && m_active_tag_tid) {
    object_player->front(&entry);
    if (entry.get_tag_tid() > *m_active_tag_tid &&
        entry.get_entry_tid() == 0) {
      uint8_t splay_width = m_journal_metadata->get_splay_width();
      m_active_tag_tid = entry.get_tag_tid();
      m_splay_offset = object_player->get_object_number() % splay_width;

      ldout(m_cct, 20) << __func__ << ": new tag " << entry.get_tag_tid() << " "
                       << "detected, adjusting offset to "
                       << static_cast<uint32_t>(m_splay_offset) << dendl;
      return true;
    }
  }

  // if any entry is available, we can test if the sequence is corrupt
  return entry_available;
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
  m_watch_required = true;
  ldout(m_cct, 20) << __func__ << ": new offset "
                   << static_cast<uint32_t>(m_splay_offset) << dendl;
}

bool JournalPlayer::remove_empty_object_player(const ObjectPlayerPtr &player) {
  assert(m_lock.is_locked());
  assert(!m_watch_scheduled);

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

  assert(m_fetch_object_numbers.count(object_num) == 0);
  m_fetch_object_numbers.insert(object_num);

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

  Mutex::Locker locker(m_lock);
  assert(m_fetch_object_numbers.count(object_num) == 1);
  m_fetch_object_numbers.erase(object_num);

  if (r == -ENOENT) {
    r = 0;
  }
  if (r == 0) {
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

void JournalPlayer::schedule_watch() {
  ldout(m_cct, 10) << __func__ << dendl;
  assert(m_lock.is_locked());
  if (m_watch_scheduled) {
    return;
  }

  // poll first splay offset and active splay offset since
  // new records should only appear in those two objects
  C_Watch *ctx = new C_Watch(this);

  ObjectPlayerPtr object_player = get_object_player();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  if (object_player->get_object_number() % splay_width != 0) {
    ++ctx->pending_fetches;

    ObjectPlayerPtr first_object_player =
      m_object_players.begin()->second.begin()->second;
    first_object_player->watch(ctx, m_watch_interval);
  }

  object_player->watch(ctx, m_watch_interval);
  m_watch_scheduled = true;
}

void JournalPlayer::handle_watch(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;
  if (r == -ECANCELED) {
    // unwatch of object player(s)
    return;
  }

  Mutex::Locker locker(m_lock);
  m_watch_scheduled = false;

  std::set<uint64_t> object_numbers;
  for (auto &players : m_object_players) {
    object_numbers.insert(
      players.second.begin()->second->get_object_number());
  }

  for (auto object_num : object_numbers) {
    process_state(object_num, r);
  }
}

void JournalPlayer::notify_entries_available() {
  assert(m_lock.is_locked());
  if (m_handler_notified) {
    return;
  }
  m_handler_notified = true;

  ldout(m_cct, 10) << __func__ << ": entries available" << dendl;
  m_journal_metadata->queue(new C_HandleEntriesAvailable(
    m_replay_handler), 0);
}

void JournalPlayer::notify_complete(int r) {
  assert(m_lock.is_locked());
  m_handler_notified = true;

  ldout(m_cct, 10) << __func__ << ": replay complete: r=" << r << dendl;
  m_journal_metadata->queue(new C_HandleComplete(
    m_replay_handler), r);
}

} // namespace journal
