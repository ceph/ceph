// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_OBJECT_PLAYER_H
#define CEPH_JOURNAL_OBJECT_PLAYER_H

#include "include/Context.h"
#include "include/interval_set.h"
#include "include/rados/librados.hpp"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/RefCountedObj.h"
#include "journal/Entry.h"
#include <list>
#include <string>
#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <boost/unordered_map.hpp>
#include "include/assert.h"

class SafeTimer;

namespace journal {

class ObjectPlayer;
typedef boost::intrusive_ptr<ObjectPlayer> ObjectPlayerPtr;

class ObjectPlayer : public RefCountedObject {
public:
  typedef std::list<Entry> Entries;
  typedef interval_set<uint64_t> InvalidRanges;

  enum RefetchState {
    REFETCH_STATE_NONE,
    REFETCH_STATE_REQUIRED,
    REFETCH_STATE_IMMEDIATE
  };

  ObjectPlayer(librados::IoCtx &ioctx, const std::string &object_oid_prefix,
               uint64_t object_num, SafeTimer &timer, Mutex &timer_lock,
               uint8_t order, uint64_t max_fetch_bytes);
  ~ObjectPlayer();

  inline const std::string &get_oid() const {
    return m_oid;
  }
  inline uint64_t get_object_number() const {
    return m_object_num;
  }

  void fetch(Context *on_finish);
  void watch(Context *on_fetch, double interval);
  void unwatch();

  void front(Entry *entry) const;
  void pop_front();
  inline bool empty() const {
    Mutex::Locker locker(m_lock);
    return m_entries.empty();
  }

  inline void get_entries(Entries *entries) {
    Mutex::Locker locker(m_lock);
    *entries = m_entries;
  }
  inline void get_invalid_ranges(InvalidRanges *invalid_ranges) {
    Mutex::Locker locker(m_lock);
    *invalid_ranges = m_invalid_ranges;
  }

  inline bool refetch_required() const {
    return (get_refetch_state() != REFETCH_STATE_NONE);
  }
  inline RefetchState get_refetch_state() const {
    return m_refetch_state;
  }
  inline void set_refetch_state(RefetchState refetch_state) {
    m_refetch_state = refetch_state;
  }

private:
  typedef std::pair<uint64_t, uint64_t> EntryKey;
  typedef boost::unordered_map<EntryKey, Entries::iterator> EntryKeys;

  struct C_Fetch : public Context {
    ObjectPlayerPtr object_player;
    Context *on_finish;
    bufferlist read_bl;
    C_Fetch(ObjectPlayer *o, Context *ctx) : object_player(o), on_finish(ctx) {
    }
    virtual void finish(int r);
  };
  struct C_WatchTask : public Context {
    ObjectPlayerPtr object_player;
    C_WatchTask(ObjectPlayer *o) : object_player(o) {
    }
    virtual void finish(int r);
  };
  struct C_WatchFetch : public Context {
    ObjectPlayerPtr object_player;
    C_WatchFetch(ObjectPlayer *o) : object_player(o) {
    }
    virtual void finish(int r);
  };

  librados::IoCtx m_ioctx;
  uint64_t m_object_num;
  std::string m_oid;
  CephContext *m_cct;

  SafeTimer &m_timer;
  Mutex &m_timer_lock;

  uint8_t m_order;
  uint64_t m_max_fetch_bytes;

  double m_watch_interval;
  Context *m_watch_task;

  mutable Mutex m_lock;
  bool m_fetch_in_progress;
  bufferlist m_read_bl;
  uint32_t m_read_off = 0;
  uint32_t m_read_bl_off = 0;

  Entries m_entries;
  EntryKeys m_entry_keys;
  InvalidRanges m_invalid_ranges;

  Context *m_watch_ctx = nullptr;

  bool m_unwatched = false;
  RefetchState m_refetch_state = REFETCH_STATE_IMMEDIATE;

  int handle_fetch_complete(int r, const bufferlist &bl, bool *refetch);

  void clear_invalid_range(uint32_t off, uint32_t len);

  void schedule_watch();
  bool cancel_watch();
  void handle_watch_task();
  void handle_watch_fetched(int r);
};

} // namespace journal

#endif // CEPH_JOURNAL_OBJECT_PLAYER_H
