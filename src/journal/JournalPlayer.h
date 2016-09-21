// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNAL_PLAYER_H
#define CEPH_JOURNAL_JOURNAL_PLAYER_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/AsyncOpTracker.h"
#include "common/Mutex.h"
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
  void shut_down(Context *on_finish);

  bool try_pop_front(Entry *entry, uint64_t *commit_tid);

private:
  typedef std::set<uint8_t> PrefetchSplayOffsets;
  typedef std::map<uint8_t, ObjectPlayerPtr> SplayedObjectPlayers;
  typedef std::map<uint8_t, ObjectPosition> SplayedObjectPositions;
  typedef std::set<uint64_t> ObjectNumbers;

  enum State {
    STATE_INIT,
    STATE_PREFETCH,
    STATE_PLAYBACK,
    STATE_ERROR
  };

  enum WatchStep {
    WATCH_STEP_FETCH_CURRENT,
    WATCH_STEP_FETCH_FIRST,
    WATCH_STEP_ASSERT_ACTIVE
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
    uint64_t object_num;
    C_Watch(JournalPlayer *player, uint64_t object_num)
      : player(player), object_num(object_num) {
      player->m_async_op_tracker.start_op();
    }
    virtual ~C_Watch() {
      player->m_async_op_tracker.finish_op();
    }

    virtual void finish(int r) override {
      player->handle_watch(object_num, r);
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
  WatchStep m_watch_step = WATCH_STEP_FETCH_CURRENT;
  bool m_watch_prune_active_tag = false;

  bool m_shut_down = false;
  bool m_handler_notified = false;

  ObjectNumbers m_fetch_object_numbers;

  PrefetchSplayOffsets m_prefetch_splay_offsets;
  SplayedObjectPlayers m_object_players;

  bool m_commit_position_valid = false;
  ObjectPosition m_commit_position;
  SplayedObjectPositions m_commit_positions;
  uint64_t m_active_set;

  boost::optional<uint64_t> m_active_tag_tid = boost::none;
  boost::optional<uint64_t> m_prune_tag_tid = boost::none;

  void advance_splay_object();

  bool is_object_set_ready() const;
  bool verify_playback_ready();
  void prune_tag(uint64_t tag_tid);
  void prune_active_tag(const boost::optional<uint64_t>& tag_tid);

  ObjectPlayerPtr get_object_player() const;
  ObjectPlayerPtr get_object_player(uint64_t object_number) const;
  bool remove_empty_object_player(const ObjectPlayerPtr &object_player);

  void process_state(uint64_t object_number, int r);
  int process_prefetch(uint64_t object_number);
  int process_playback(uint64_t object_number);

  void fetch(uint64_t object_num);
  void fetch(const ObjectPlayerPtr &object_player);
  void handle_fetched(uint64_t object_num, int r);
  void refetch(bool immediate);

  void schedule_watch(bool immediate);
  void handle_watch(uint64_t object_num, int r);
  void handle_watch_assert_active(int r);

  void notify_entries_available();
  void notify_complete(int r);
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_PLAYER_H
