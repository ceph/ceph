// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalPlayer.h"
#include "journal/Entry.h"
#include "journal/ReplayHandler.h"
#include "journal/Utils.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalPlayer: " << this << " "

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
    m_watch_enabled(false), m_watch_scheduled(false), m_watch_interval(0) {
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
    m_commit_position_valid = true;
    m_commit_position = active_position;
    m_splay_offset = active_position.object_number % splay_width;
    for (auto &position : commit_position.object_positions) {
      uint8_t splay_offset = position.object_number % splay_width;
      m_commit_positions[splay_offset] = position;
    }
  }
}

JournalPlayer::~JournalPlayer() {
  assert(m_async_op_tracker.empty());
  {
    Mutex::Locker locker(m_lock);
    assert(m_shut_down);
    assert(m_fetch_object_numbers.empty());
    assert(!m_watch_scheduled);
  }
  m_replay_handler->put();
}

void JournalPlayer::prefetch() {
  Mutex::Locker locker(m_lock);
  assert(m_state == STATE_INIT);
  m_state = STATE_PREFETCH;

  m_active_set = m_journal_metadata->get_active_set();
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

  // prefetch the active object for each splay offset
  std::set<uint64_t> prefetch_object_numbers;
  for (uint8_t splay_offset = 0; splay_offset < splay_width; ++splay_offset) {
    uint64_t object_number = splay_offset;
    if (splay_offset_to_objects.count(splay_offset) != 0) {
      object_number = splay_offset_to_objects[splay_offset];
    }

    prefetch_object_numbers.insert(object_number);
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
    m_watch_step = WATCH_STEP_FETCH_CURRENT;
  }
  prefetch();
}

void JournalPlayer::shut_down(Context *on_finish) {
  ldout(m_cct, 20) << __func__ << dendl;
  Mutex::Locker locker(m_lock);

  assert(!m_shut_down);
  m_shut_down = true;
  m_watch_enabled = false;

  on_finish = utils::create_async_context_callback(
      m_journal_metadata, on_finish);

  if (m_watch_scheduled) {
    ObjectPlayerPtr object_player = get_object_player();
    switch (m_watch_step) {
    case WATCH_STEP_FETCH_FIRST:
      object_player = m_object_players.begin()->second;
      // fallthrough
    case WATCH_STEP_FETCH_CURRENT:
      object_player->unwatch();
      break;
    case WATCH_STEP_ASSERT_ACTIVE:
      break;
    }
  }

  m_async_op_tracker.wait_for_ops(on_finish);
}

bool JournalPlayer::try_pop_front(Entry *entry, uint64_t *commit_tid) {
  ldout(m_cct, 20) << __func__ << dendl;
  Mutex::Locker locker(m_lock);

  if (m_state != STATE_PLAYBACK) {
    m_handler_notified = false;
    return false;
  }

  if (!verify_playback_ready()) {
    if (!is_object_set_ready()) {
      m_handler_notified = false;
    } else {
      refetch(true);
    }
    return false;
  }

  ObjectPlayerPtr object_player = get_object_player();
  assert(object_player && !object_player->empty());

  object_player->front(entry);
  object_player->pop_front();

  uint64_t last_entry_tid;
  if (m_journal_metadata->get_last_allocated_entry_tid(
        entry->get_tag_tid(), &last_entry_tid) &&
      entry->get_entry_tid() != last_entry_tid + 1) {
    lderr(m_cct) << "missing prior journal entry: " << *entry << dendl;

    m_state = STATE_ERROR;
    notify_complete(-ENOMSG);
    return false;
  }

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
  ObjectPlayerPtr object_player = m_object_players[splay_offset];

  // prefetch in-order since a newer splay object could prefetch first
  if (m_fetch_object_numbers.count(object_player->get_object_number()) == 0) {
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

      // do not search for commit position for this object
      // if we've already seen it
      if (found_commit) {
        m_commit_positions.erase(splay_offset);
      }
    }

    // if the object is empty, pre-fetch the next splay object
    if (object_player->empty() && object_player->refetch_required()) {
      ldout(m_cct, 10) << "refetching potentially partially decoded object"
                       << dendl;
      object_player->set_refetch_state(ObjectPlayer::REFETCH_STATE_NONE);
      fetch(object_player);
    } else if (!remove_empty_object_player(object_player)) {
      ldout(m_cct, 10) << "prefetch of object complete" << dendl;
      prefetch_complete = true;
    }
  }

  if (!prefetch_complete) {
    return 0;
  }

  m_prefetch_splay_offsets.erase(it);
  if (!m_prefetch_splay_offsets.empty()) {
    return 0;
  }

  ldout(m_cct, 10) << "switching to playback mode" << dendl;
  m_state = STATE_PLAYBACK;

  // if we have a valid commit position, our read should start with
  // the next consistent journal entry in the sequence
  if (m_commit_position_valid) {
    splay_offset = m_commit_position.object_number % splay_width;
    object_player = m_object_players[splay_offset];

    if (object_player->empty()) {
      if (!object_player->refetch_required()) {
        advance_splay_object();
      }
    } else {
      Entry entry;
      object_player->front(&entry);
      if (entry.get_tag_tid() == m_commit_position.tag_tid) {
        advance_splay_object();
      }
    }
  }

  if (verify_playback_ready()) {
    notify_entries_available();
  } else if (is_object_set_ready()) {
    refetch(false);
  }
  return 0;
}

int JournalPlayer::process_playback(uint64_t object_number) {
  ldout(m_cct, 10) << __func__ << ": object_num=" << object_number << dendl;
  assert(m_lock.is_locked());

  if (verify_playback_ready()) {
    notify_entries_available();
  } else if (is_object_set_ready()) {
    refetch(false);
  }
  return 0;
}

bool JournalPlayer::is_object_set_ready() const {
  assert(m_lock.is_locked());
  if (m_watch_scheduled || !m_fetch_object_numbers.empty()) {
    ldout(m_cct, 20) << __func__ << ": waiting for in-flight fetch" << dendl;
    return false;
  }

  return true;
}

bool JournalPlayer::verify_playback_ready() {
  assert(m_lock.is_locked());

  while (true) {
    if (!is_object_set_ready()) {
      ldout(m_cct, 10) << __func__ << ": waiting for full object set" << dendl;
      return false;
    }

    ObjectPlayerPtr object_player = get_object_player();
    assert(object_player);
    uint64_t object_num = object_player->get_object_number();

    // Verify is the active object player has another entry available
    // in the sequence
    // NOTE: replay currently does not check tag class to playback multiple tags
    // from different classes (issue #14909).  When a new tag is discovered, it
    // is assumed that the previous tag was closed at the last replayable entry.
    Entry entry;
    if (!object_player->empty()) {
      m_watch_prune_active_tag = false;
      object_player->front(&entry);

      if (!m_active_tag_tid) {
        ldout(m_cct, 10) << __func__ << ": "
                         << "object_num=" << object_num << ", "
                         << "initial tag=" << entry.get_tag_tid()
                         << dendl;
        m_active_tag_tid = entry.get_tag_tid();
        return true;
      } else if (entry.get_tag_tid() < *m_active_tag_tid ||
                 (m_prune_tag_tid && entry.get_tag_tid() <= *m_prune_tag_tid)) {
        // entry occurred before the current active tag
        ldout(m_cct, 10) << __func__ << ": detected stale entry: "
                         << "object_num=" << object_num << ", "
                         << "entry=" << entry << dendl;
        prune_tag(entry.get_tag_tid());
        continue;
      } else if (entry.get_tag_tid() > *m_active_tag_tid) {
        // new tag at current playback position -- implies that previous
        // tag ended abruptly without flushing out all records
        // search for the start record for the next tag
        ldout(m_cct, 10) << __func__ << ": new tag detected: "
                         << "object_num=" << object_num << ", "
                         << "active_tag=" << *m_active_tag_tid << ", "
                         << "new_tag=" << entry.get_tag_tid() << dendl;
        if (entry.get_entry_tid() == 0) {
          // first entry in new tag -- can promote to active
          prune_active_tag(entry.get_tag_tid());
          return true;
        } else {
          // prune current active and wait for initial entry for new tag
          prune_active_tag(boost::none);
          continue;
        }
      } else {
        ldout(m_cct, 20) << __func__ << ": "
                         << "object_num=" << object_num << ", "
                         << "entry: " << entry << dendl;
        assert(entry.get_tag_tid() == *m_active_tag_tid);
        return true;
      }
    } else {
      if (!m_active_tag_tid) {
        // waiting for our first entry
        ldout(m_cct, 10) << __func__ << ": waiting for first entry: "
                         << "object_num=" << object_num << dendl;
        return false;
      } else if (m_prune_tag_tid && *m_prune_tag_tid == *m_active_tag_tid) {
        ldout(m_cct, 10) << __func__ << ": no more entries" << dendl;
        return false;
      } else if (m_watch_enabled && m_watch_prune_active_tag) {
        // detected current tag is now longer active and we have re-read the
        // current object but it's still empty, so this tag is done
        ldout(m_cct, 10) << __func__ << ": assuming no more in-sequence entries: "
                         << "object_num=" << object_num << ", "
                         << "active_tag " << *m_active_tag_tid << dendl;
        prune_active_tag(boost::none);
        continue;
      } else if (object_player->refetch_required()) {
        // if the active object requires a refetch, don't proceed looking for a
        // new tag before this process completes
        ldout(m_cct, 10) << __func__ << ": refetch required: "
                         << "object_num=" << object_num << dendl;
        return false;
      } else if (!m_watch_enabled) {
        // current playback position is empty so this tag is done
        ldout(m_cct, 10) << __func__ << ": no more in-sequence entries: "
                         << "object_num=" << object_num << ", "
                         << "active_tag=" << *m_active_tag_tid << dendl;
        prune_active_tag(boost::none);
        continue;
      } else if (!m_watch_scheduled) {
        // no more entries and we don't have an active watch in-progress
        ldout(m_cct, 10) << __func__ << ": no more entries -- watch required"
                         << dendl;
        return false;
      }
    }
  }
  return false;
}

void JournalPlayer::prune_tag(uint64_t tag_tid) {
  assert(m_lock.is_locked());
  ldout(m_cct, 10) << __func__ << ": pruning remaining entries for tag "
                   << tag_tid << dendl;

  // prune records that are at or below the largest prune tag tid
  if (!m_prune_tag_tid || *m_prune_tag_tid < tag_tid) {
    m_prune_tag_tid = tag_tid;
  }

  bool pruned = false;
  for (auto &player_pair : m_object_players) {
    ObjectPlayerPtr object_player(player_pair.second);
    ldout(m_cct, 15) << __func__ << ": checking " << object_player->get_oid()
                     << dendl;
    while (!object_player->empty()) {
      Entry entry;
      object_player->front(&entry);
      if (entry.get_tag_tid() == tag_tid) {
        ldout(m_cct, 20) << __func__ << ": pruned " << entry << dendl;
        object_player->pop_front();
        pruned = true;
      } else {
        break;
      }
    }
  }

  // avoid watch delay when pruning stale tags from journal objects
  if (pruned) {
    ldout(m_cct, 15) << __func__ << ": reseting refetch state to immediate"
                     << dendl;
    for (auto &player_pair : m_object_players) {
      ObjectPlayerPtr object_player(player_pair.second);
      object_player->set_refetch_state(ObjectPlayer::REFETCH_STATE_IMMEDIATE);
    }
  }

  // trim empty player to prefetch the next available object
  for (auto &player_pair : m_object_players) {
    remove_empty_object_player(player_pair.second);
  }
}

void JournalPlayer::prune_active_tag(const boost::optional<uint64_t>& tag_tid) {
  assert(m_lock.is_locked());
  assert(m_active_tag_tid);

  uint64_t active_tag_tid = *m_active_tag_tid;
  if (tag_tid) {
    m_active_tag_tid = tag_tid;
  }
  m_splay_offset = 0;
  m_watch_step = WATCH_STEP_FETCH_CURRENT;

  prune_tag(active_tag_tid);
}

ObjectPlayerPtr JournalPlayer::get_object_player() const {
  assert(m_lock.is_locked());

  SplayedObjectPlayers::const_iterator it = m_object_players.find(
    m_splay_offset);
  assert(it != m_object_players.end());
  return it->second;
}

ObjectPlayerPtr JournalPlayer::get_object_player(uint64_t object_number) const {
  assert(m_lock.is_locked());

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;
  auto splay_it = m_object_players.find(splay_offset);
  assert(splay_it != m_object_players.end());

  ObjectPlayerPtr object_player = splay_it->second;
  assert(object_player->get_object_number() == object_number);
  return object_player;
}

void JournalPlayer::advance_splay_object() {
  assert(m_lock.is_locked());
  ++m_splay_offset;
  m_splay_offset %= m_journal_metadata->get_splay_width();
  m_watch_step = WATCH_STEP_FETCH_CURRENT;
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
  } else if (player->refetch_required()) {
    ldout(m_cct, 20) << __func__ << ": " << player->get_oid() << " requires "
                     << "a refetch" << dendl;
    return false;
  } else if (m_active_set != active_set) {
    ldout(m_cct, 20) << __func__ << ": new active set detected, all players "
                     << "require refetch" << dendl;
    m_active_set = active_set;
    for (auto &pair : m_object_players) {
      pair.second->set_refetch_state(ObjectPlayer::REFETCH_STATE_IMMEDIATE);
    }
    return false;
  }

  ldout(m_cct, 15) << __func__ << ": " << player->get_oid() << " empty"
                   << dendl;

  m_watch_prune_active_tag = false;
  m_watch_step = WATCH_STEP_FETCH_CURRENT;

  uint64_t next_object_num = player->get_object_number() + splay_width;
  fetch(next_object_num);
  return true;
}

void JournalPlayer::fetch(uint64_t object_num) {
  assert(m_lock.is_locked());

  ObjectPlayerPtr object_player(new ObjectPlayer(
    m_ioctx, m_object_oid_prefix, object_num, m_journal_metadata->get_timer(),
    m_journal_metadata->get_timer_lock(), m_journal_metadata->get_order(),
    m_journal_metadata->get_settings().max_fetch_bytes));

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  m_object_players[object_num % splay_width] = object_player;
  fetch(object_player);
}

void JournalPlayer::fetch(const ObjectPlayerPtr &object_player) {
  assert(m_lock.is_locked());

  uint64_t object_num = object_player->get_object_number();
  std::string oid = utils::get_object_name(m_object_oid_prefix, object_num);
  assert(m_fetch_object_numbers.count(object_num) == 0);
  m_fetch_object_numbers.insert(object_num);

  ldout(m_cct, 10) << __func__ << ": " << oid << dendl;
  C_Fetch *fetch_ctx = new C_Fetch(this, object_num);

  object_player->fetch(fetch_ctx);
}

void JournalPlayer::handle_fetched(uint64_t object_num, int r) {
  ldout(m_cct, 10) << __func__ << ": "
                   << utils::get_object_name(m_object_oid_prefix, object_num)
                   << ": r=" << r << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_fetch_object_numbers.count(object_num) == 1);
  m_fetch_object_numbers.erase(object_num);

  if (m_shut_down) {
    return;
  }

  if (r == 0) {
    ObjectPlayerPtr object_player = get_object_player(object_num);
    remove_empty_object_player(object_player);
  }
  process_state(object_num, r);
}

void JournalPlayer::refetch(bool immediate) {
  ldout(m_cct, 10) << __func__ << dendl;
  assert(m_lock.is_locked());
  m_handler_notified = false;

  // if watching the object, handle the periodic re-fetch
  if (m_watch_enabled) {
    schedule_watch(immediate);
    return;
  }

  ObjectPlayerPtr object_player = get_object_player();
  if (object_player->refetch_required()) {
    object_player->set_refetch_state(ObjectPlayer::REFETCH_STATE_NONE);
    fetch(object_player);
    return;
  }

  notify_complete(0);
}

void JournalPlayer::schedule_watch(bool immediate) {
  ldout(m_cct, 10) << __func__ << dendl;
  assert(m_lock.is_locked());
  if (m_watch_scheduled) {
    return;
  }

  m_watch_scheduled = true;

  if (m_watch_step == WATCH_STEP_ASSERT_ACTIVE) {
    // detect if a new tag has been created in case we are blocked
    // by an incomplete tag sequence
    ldout(m_cct, 20) << __func__ << ": asserting active tag="
                     << *m_active_tag_tid << dendl;

    m_async_op_tracker.start_op();
    FunctionContext *ctx = new FunctionContext([this](int r) {
        handle_watch_assert_active(r);
      });
    m_journal_metadata->assert_active_tag(*m_active_tag_tid, ctx);
    return;
  }

  ObjectPlayerPtr object_player;
  double watch_interval = m_watch_interval;

  switch (m_watch_step) {
  case WATCH_STEP_FETCH_CURRENT:
    {
      object_player = get_object_player();

      uint8_t splay_width = m_journal_metadata->get_splay_width();
      uint64_t active_set = m_journal_metadata->get_active_set();
      uint64_t object_set = object_player->get_object_number() / splay_width;
      if (immediate ||
          (object_player->get_refetch_state() ==
             ObjectPlayer::REFETCH_STATE_IMMEDIATE) ||
          (object_set < active_set && object_player->refetch_required())) {
        ldout(m_cct, 20) << __func__ << ": immediately refetching "
                         << object_player->get_oid()
                         << dendl;
        object_player->set_refetch_state(ObjectPlayer::REFETCH_STATE_NONE);
        watch_interval = 0;
      }
    }
    break;
  case WATCH_STEP_FETCH_FIRST:
    object_player = m_object_players.begin()->second;
    watch_interval = 0;
    break;
  default:
    assert(false);
  }

  ldout(m_cct, 20) << __func__ << ": scheduling watch on "
                   << object_player->get_oid() << dendl;
  Context *ctx = utils::create_async_context_callback(
    m_journal_metadata, new C_Watch(this, object_player->get_object_number()));
  object_player->watch(ctx, watch_interval);
}

void JournalPlayer::handle_watch(uint64_t object_num, int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;
  Mutex::Locker locker(m_lock);
  assert(m_watch_scheduled);
  m_watch_scheduled = false;

  if (m_shut_down || r == -ECANCELED) {
    // unwatch of object player(s)
    return;
  }

  ObjectPlayerPtr object_player = get_object_player(object_num);
  if (r == 0 && object_player->empty()) {
    // possibly need to prune this empty object player if we've
    // already fetched it after the active set was advanced with no
    // new records
    remove_empty_object_player(object_player);
  }

  // determine what object to query on next watch schedule tick
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  if (m_watch_step == WATCH_STEP_FETCH_CURRENT &&
      object_player->get_object_number() % splay_width != 0) {
    m_watch_step = WATCH_STEP_FETCH_FIRST;
  } else if (m_active_tag_tid) {
    m_watch_step = WATCH_STEP_ASSERT_ACTIVE;
  } else {
    m_watch_step = WATCH_STEP_FETCH_CURRENT;
  }

  process_state(object_num, r);
}

void JournalPlayer::handle_watch_assert_active(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_watch_scheduled);
  m_watch_scheduled = false;

  if (r == -ESTALE) {
    // newer tag exists -- since we are at this step in the watch sequence,
    // we know we can prune the active tag if watch fails again
    ldout(m_cct, 10) << __func__ << ": tag " << *m_active_tag_tid << " "
                     << "no longer active" << dendl;
    m_watch_prune_active_tag = true;
  }

  m_watch_step = WATCH_STEP_FETCH_CURRENT;
  if (!m_shut_down && m_watch_enabled) {
    schedule_watch(false);
  }
  m_async_op_tracker.finish_op();
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
