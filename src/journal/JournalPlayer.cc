// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalPlayer.h"
#include "common/Finisher.h"
#include "journal/ReplayHandler.h"
#include "journal/Utils.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalPlayer: "

namespace journal {

namespace {

struct C_HandleComplete: public Context {
  ReplayHandler *replay_handler;

  C_HandleComplete(ReplayHandler *_replay_handler)
    : replay_handler(_replay_handler) {
  }

  virtual void finish(int r) {
    replay_handler->handle_complete(r);
  }
};

} // anonymous namespace

JournalPlayer::JournalPlayer(librados::IoCtx &ioctx,
                             const std::string &object_oid_prefix,
                             const JournalMetadataPtr& journal_metadata,
                             ReplayHandler *replay_handler)
  : m_cct(NULL), m_object_oid_prefix(object_oid_prefix),
    m_journal_metadata(journal_metadata), m_replay_handler(replay_handler),
    m_process_state(this), m_lock("JournalPlayer::m_lock"), m_state(STATE_INIT),
    m_splay_offset(0), m_watch_enabled(false), m_watch_scheduled(false),
    m_watch_interval(0), m_commit_object(0) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());

  ObjectSetPosition commit_position;
  m_journal_metadata->get_commit_position(&commit_position);
  if (!commit_position.entry_positions.empty()) {
    uint8_t splay_width = m_journal_metadata->get_splay_width();
    m_splay_offset = commit_position.object_number % splay_width;
    m_commit_object = commit_position.object_number;
    m_commit_tag = commit_position.entry_positions.front().tag;

    for (size_t i=0; i<commit_position.entry_positions.size(); ++i) {
      const EntryPosition &entry_position = commit_position.entry_positions[i];
      m_commit_tids[entry_position.tag] = entry_position.tid;
    }
  }
}

void JournalPlayer::prefetch() {
  m_lock.Lock();
  assert(m_state == STATE_INIT);
  m_state = STATE_PREFETCH;

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  ldout(m_cct, 10) << __func__ << ": prefetching " << (2 * splay_width) << " "
                   << "objects" << dendl;

  // prefetch starting from the last known commit set
  C_PrefetchBatch *ctx = new C_PrefetchBatch(this);
  uint64_t start_object = (m_commit_object / splay_width) * splay_width;
  for (uint64_t object_number = start_object;
       object_number < start_object + (2 * splay_width); ++object_number) {
    ctx->add_fetch();
    fetch(object_number, ctx);
  }
  m_lock.Unlock();

  ctx->complete(0);
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
  if (m_watch_scheduled) {
    ObjectPlayerPtr object_player = get_object_player();
    assert(object_player);

    object_player->unwatch();
    m_watch_scheduled = false;
  }
}

bool JournalPlayer::try_pop_front(Entry *entry,
                                  ObjectSetPosition *object_set_position) {
  Mutex::Locker locker(m_lock);
  if (m_state != STATE_PLAYBACK) {
    return false;
  }

  ObjectPlayerPtr object_player = get_object_player();
  assert(object_player);

  if (object_player->empty()) {
    if (m_watch_enabled && !m_watch_scheduled) {
      object_player->watch(&m_process_state, m_watch_interval);
      m_watch_scheduled = true;
    } else if (!m_watch_enabled && !object_player->is_fetch_in_progress()) {
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

    ObjectPlayerPtr next_set_object_player = get_next_set_object_player();
    if (!next_set_object_player->empty()) {
      remove_object_player(object_player, &m_process_state);
    }
  }

  // TODO populate the object_set_position w/ current object number and
  //      unique entry tag mappings
  m_journal_metadata->reserve_tid(entry->get_tag(), entry->get_tid());
  return true;
}

void JournalPlayer::process_state(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;
  if (r == 0) {
    Mutex::Locker locker(m_lock);
    switch (m_state) {
    case STATE_PREFETCH:
      ldout(m_cct, 10) << "PREFETCH" << dendl;
      r = process_prefetch();
      break;
    case STATE_PLAYBACK:
      ldout(m_cct, 10) << "PLAYBACK" << dendl;
      r = process_playback();
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

int JournalPlayer::process_prefetch() {
  ldout(m_cct, 10) << __func__ << dendl;
  assert(m_lock.is_locked());

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  for (uint8_t splay_offset = 0; splay_offset < splay_width; ++splay_offset) {
    assert(m_object_players.count(splay_offset) == 1);

    ObjectPlayers &object_players = m_object_players[splay_offset];
    assert(object_players.size() == 2);

    ObjectPlayerPtr object_player = object_players.begin()->second;
    assert(!object_player->is_fetch_in_progress());

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

    ObjectPlayerPtr next_set_object_player = get_next_set_object_player();
    if (object_player->empty() && !next_set_object_player->empty()) {
      ldout(m_cct, 15) << object_player->get_oid() << " empty" << dendl;
      remove_object_player(object_player, &m_process_state);
    }
  }

  m_state = STATE_PLAYBACK;
  ObjectPlayerPtr object_player = get_object_player();
  if (!object_player->empty()) {
    ldout(m_cct, 10) << __func__ << ": entries available" << dendl;
    m_lock.Unlock();
    m_replay_handler->handle_entries_available();
    m_lock.Lock();
  } else if (m_watch_enabled) {
    object_player->watch(&m_process_state, m_watch_interval);
    m_watch_scheduled = true;
  } else {
    ldout(m_cct, 10) << __func__ << ": no uncommitted entries available"
                     << dendl;
    m_journal_metadata->get_finisher().queue(new C_HandleComplete(
      m_replay_handler), 0);
  }
  return 0;
}

int JournalPlayer::process_playback() {
  ldout(m_cct, 10) << __func__ << dendl;
  assert(m_lock.is_locked());

  m_watch_scheduled = false;

  ObjectPlayerPtr object_player = get_object_player();
  if (!object_player->empty()) {
    ldout(m_cct, 10) << __func__ << ": entries available" << dendl;
    m_lock.Unlock();
    m_replay_handler->handle_entries_available();
    m_lock.Lock();
  }
  return 0;
}

const JournalPlayer::ObjectPlayers &JournalPlayer::get_object_players() const {
  assert(m_lock.is_locked());

  assert(m_object_players.count(m_splay_offset) == 1);
  SplayedObjectPlayers::const_iterator it = m_object_players.find(
    m_splay_offset);
  assert(it != m_object_players.end());

  const ObjectPlayers &object_players = it->second;
  assert(object_players.size() == 2);
  return object_players;
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

void JournalPlayer::remove_object_player(const ObjectPlayerPtr &object_player,
                                         Context *on_fetch) {
  assert(m_lock.is_locked());

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  ObjectPlayers &object_players = m_object_players[
    object_player->get_object_number() % splay_width];
  assert(!object_players.empty());
  assert(object_players.begin()->second == object_player);
  object_players.erase(object_players.begin());

  fetch(object_player->get_object_number() + (2 * splay_width), on_fetch);
}

void JournalPlayer::fetch(uint64_t object_num, Context *ctx) {
  assert(m_lock.is_locked());

  std::string oid = utils::get_object_name(m_object_oid_prefix, object_num);

  ldout(m_cct, 10) << __func__ << ": " << oid << dendl;
  C_Fetch *fetch_ctx = new C_Fetch(this, object_num, ctx);
  ObjectPlayerPtr object_player(new ObjectPlayer(
    m_ioctx, m_object_oid_prefix, object_num, m_journal_metadata->get_timer(),
    m_journal_metadata->get_timer_lock(), m_journal_metadata->get_order()));

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  m_object_players[object_num % splay_width][object_num] = object_player;
  object_player->fetch(fetch_ctx);
}

int JournalPlayer::handle_fetched(int r, uint64_t object_num) {
  std::string oid = utils::get_object_name(m_object_oid_prefix, object_num);

  ldout(m_cct, 10) << __func__ << ": fetched "
                   << utils::get_object_name(m_object_oid_prefix, object_num)
                   << ": r=" << r << dendl;
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return 0;
}

JournalPlayer::C_PrefetchBatch::C_PrefetchBatch(JournalPlayer *p)
  : player(p), lock("JournalPlayer::C_PrefetchBatch::lock"), refs(1),
    return_value(0) {
  player->get();
}

void JournalPlayer::C_PrefetchBatch::add_fetch() {
  Mutex::Locker locker(lock);
  ++refs;
}

void JournalPlayer::C_PrefetchBatch::complete(int r) {
  {
    Mutex::Locker locker(lock);
    if (r < 0 && return_value == 0) {
      return_value = r;
    }
    --refs;
  }

  if (refs == 0) {
    player->process_state(return_value);
    player->put();
    delete this;
  }
}

} // namespace journal
