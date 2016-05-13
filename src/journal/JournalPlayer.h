// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNAL_PLAYER_H
#define CEPH_JOURNAL_JOURNAL_PLAYER_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "journal/AsyncOpTracker.h"
#include "journal/JournalMetadata.h"
#include "journal/ObjectPlayer.h"
#include "cls/journal/cls_journal_types.h"
#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <map>

class SafeTimer;

namespace journal {

class Entry;
class ReplayHandler;

class JournalPlayer {
public:
  typedef cls::journal::ObjectPosition ObjectPosition;
  typedef cls::journal::ObjectPositions ObjectPositions;
  typedef cls::journal::ObjectSetPosition ObjectSetPosition;

  JournalPlayer(librados::IoCtx &ioctx, const std::string &object_oid_prefix,
                const JournalMetadataPtr& journal_metadata,
                ReplayHandler *replay_handler);
  ~JournalPlayer();

  void prefetch();
  void prefetch_and_watch(double interval);
  void unwatch();

  bool try_pop_front(Entry *entry, uint64_t *commit_tid);

private:
  typedef std::set<uint8_t> PrefetchSplayOffsets;
  typedef std::map<uint64_t, ObjectPlayerPtr> ObjectPlayers;
  typedef std::map<uint8_t, ObjectPlayers> SplayedObjectPlayers;
  typedef std::map<uint8_t, ObjectPosition> SplayedObjectPositions;
  typedef std::set<uint64_t> ObjectNumbers;

  enum State {
    STATE_INIT,
    STATE_PREFETCH,
    STATE_PLAYBACK,
    STATE_ERROR
  };

  struct C_Fetch : public Context {
    JournalPlayer *player;
    uint64_t object_num;
    C_Fetch(JournalPlayer *p, uint64_t o) : player(p), object_num(o) {
      player->m_async_op_tracker.start_op();
    }
    virtual ~C_Fetch() {
      player->m_async_op_tracker.finish_op();
    }
    virtual void finish(int r) {
      player->handle_fetched(object_num, r);
    }
  };

  struct C_Watch : public Context {
    JournalPlayer *player;
    Mutex lock;
    uint8_t pending_fetches = 1;
    int ret_val = 0;

    C_Watch(JournalPlayer *player)
      : player(player), lock("JournalPlayer::C_Watch::lock") {
    }

    virtual void complete(int r) override {
      lock.Lock();
      if (ret_val == 0 && r < 0) {
        ret_val = r;
      }

      assert(pending_fetches > 0);
      if (--pending_fetches == 0) {
        lock.Unlock();
        Context::complete(ret_val);
      } else {
        lock.Unlock();
      }
    }

    virtual void finish(int r) override {
      player->handle_watch(r);
    }
  };

  librados::IoCtx m_ioctx;
  CephContext *m_cct;
  std::string m_object_oid_prefix;
  JournalMetadataPtr m_journal_metadata;

  ReplayHandler *m_replay_handler;

  AsyncOpTracker m_async_op_tracker;

  mutable Mutex m_lock;
  State m_state;
  uint8_t m_splay_offset;

  bool m_watch_enabled;
  bool m_watch_scheduled;
  double m_watch_interval;

  bool m_handler_notified = false;

  ObjectNumbers m_fetch_object_numbers;

  PrefetchSplayOffsets m_prefetch_splay_offsets;
  SplayedObjectPlayers m_object_players;
  uint64_t m_commit_object;
  SplayedObjectPositions m_commit_positions;
  boost::optional<uint64_t> m_active_tag_tid = boost::none;

  void advance_splay_object();

  bool is_object_set_ready() const;
  bool verify_playback_ready();
  const ObjectPlayers &get_object_players() const;
  ObjectPlayerPtr get_object_player() const;
  ObjectPlayerPtr get_next_set_object_player() const;
  bool remove_empty_object_player(const ObjectPlayerPtr &object_player);

  void process_state(uint64_t object_number, int r);
  int process_prefetch(uint64_t object_number);
  int process_playback(uint64_t object_number);

  void fetch(uint64_t object_num);
  void handle_fetched(uint64_t object_num, int r);

  void schedule_watch();
  void handle_watch(int r);

  void notify_entries_available();
  void notify_complete(int r);
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_PLAYER_H
