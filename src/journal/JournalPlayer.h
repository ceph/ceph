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
#include <map>

class SafeTimer;

namespace journal {

class Entry;
class ReplayHandler;

class JournalPlayer {
public:
  typedef cls::journal::EntryPosition EntryPosition;
  typedef cls::journal::EntryPositions EntryPositions;
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
  typedef std::map<std::string, uint64_t> AllocatedTids;
  typedef std::map<uint64_t, ObjectPlayerPtr> ObjectPlayers;
  typedef std::map<uint8_t, ObjectPlayers> SplayedObjectPlayers;

  enum State {
    STATE_INIT,
    STATE_PREFETCH,
    STATE_PLAYBACK,
    STATE_ERROR
  };

  struct C_Watch : public Context {
    JournalPlayer *player;
    uint64_t object_num;

    C_Watch(JournalPlayer *p, uint64_t o) : player(p), object_num(o) {
    }
    virtual void finish(int r) {
      player->handle_watch(object_num, r);
    }
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

  PrefetchSplayOffsets m_prefetch_splay_offsets;
  SplayedObjectPlayers m_object_players;
  uint64_t m_commit_object;
  std::string m_commit_tag;
  AllocatedTids m_commit_tids;

  void advance_splay_object();

  const ObjectPlayers &get_object_players() const;
  ObjectPlayerPtr get_object_player() const;
  ObjectPlayerPtr get_next_set_object_player() const;
  bool remove_empty_object_player(const ObjectPlayerPtr &object_player);

  void process_state(uint64_t object_number, int r);
  int process_prefetch(uint64_t object_number);
  int process_playback(uint64_t object_number);

  void fetch(uint64_t object_num);
  void handle_fetched(uint64_t object_num, int r);
  void handle_watch(uint64_t object_num, int r);
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_PLAYER_H
