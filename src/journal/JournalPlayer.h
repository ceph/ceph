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

  bool try_pop_front(Entry *entry, ObjectSetPosition *object_set_position);

private:
  typedef std::map<std::string, uint64_t> AllocatedTids;
  typedef std::map<uint64_t, ObjectPlayerPtr> ObjectPlayers;
  typedef std::map<uint8_t, ObjectPlayers> SplayedObjectPlayers;

  enum State {
    STATE_INIT,
    STATE_PREFETCH,
    STATE_PLAYBACK,
    STATE_ERROR
  };

  struct C_ProcessState : public Context {
    JournalPlayer *player;
    C_ProcessState(JournalPlayer *p) : player(p) {}
    virtual void complete(int r) {
      player->process_state(r);
    }
    virtual void finish(int r) {}
  };

  struct C_PrefetchBatch : public Context {
    JournalPlayer *player;
    Mutex lock;
    uint32_t refs;
    int return_value;

    C_PrefetchBatch(JournalPlayer *p);
    virtual ~C_PrefetchBatch() {
      player->m_async_op_tracker.finish_op();
    }
    void add_fetch();
    virtual void complete(int r);
    virtual void finish(int r) {}
  };

  struct C_Fetch : public Context {
    JournalPlayer *player;
    uint64_t object_num;
    Context *on_fetch;
    C_Fetch(JournalPlayer *p, uint64_t o, Context *c)
      : player(p), object_num(o), on_fetch(c) {
      player->m_async_op_tracker.start_op();
    }
    virtual ~C_Fetch() {
      player->m_async_op_tracker.finish_op();
    }
    virtual void finish(int r) {
      r = player->handle_fetched(r, object_num);
      on_fetch->complete(r);
    }
  };

  librados::IoCtx m_ioctx;
  CephContext *m_cct;
  std::string m_object_oid_prefix;
  JournalMetadataPtr m_journal_metadata;

  ReplayHandler *m_replay_handler;

  C_ProcessState m_process_state;

  AsyncOpTracker m_async_op_tracker;

  mutable Mutex m_lock;
  State m_state;
  uint8_t m_splay_offset;

  bool m_watch_enabled;
  bool m_watch_scheduled;
  double m_watch_interval;

  SplayedObjectPlayers m_object_players;
  uint64_t m_commit_object;
  std::string m_commit_tag;
  AllocatedTids m_commit_tids;

  void advance_splay_object();

  const ObjectPlayers &get_object_players() const;
  ObjectPlayerPtr get_object_player() const;
  ObjectPlayerPtr get_next_set_object_player() const;
  void remove_object_player(const ObjectPlayerPtr &object_player,
                            Context *on_fetch);

  void process_state(int r);
  int process_prefetch();
  int process_playback();

  void fetch(uint64_t object_num, Context *ctx);
  int handle_fetched(int r, uint64_t object_num);
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_PLAYER_H
