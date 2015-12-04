// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_H
#define CEPH_LIBRBD_JOURNAL_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/interval_set.h"
#include "include/unordered_map.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "journal/Future.h"
#include "journal/ReplayHandler.h"
#include "librbd/ImageWatcher.h"
#include <algorithm>
#include <list>
#include <string>

class Context;
namespace journal {
class Journaler;
}

namespace librbd {

class AioCompletion;
class AioObjectRequest;
class ImageCtx;
class JournalReplay;

namespace journal {
class EventEntry;
}

class Journal {
public:
  typedef std::list<AioObjectRequest *> AioObjectRequests;

  Journal(ImageCtx &image_ctx);
  ~Journal();

  static bool is_journal_supported(ImageCtx &image_ctx);
  static int create(librados::IoCtx &io_ctx, const std::string &image_id,
		    uint8_t order, uint8_t splay_width,
		    const std::string &object_pool);
  static int remove(librados::IoCtx &io_ctx, const std::string &image_id);
  static int reset(librados::IoCtx &io_ctx, const std::string &image_id);

  bool is_journal_ready() const;
  bool is_journal_replaying() const;

  void wait_for_journal_ready(Context *on_ready);
  void wait_for_journal_ready();

  void open();
  int close();

  uint64_t append_io_event(AioCompletion *aio_comp,
                           const journal::EventEntry &event_entry,
                           const AioObjectRequests &requests,
                           uint64_t offset, size_t length,
                           bool flush_entry);
  void commit_io_event(uint64_t tid, int r);
  void commit_io_event_extent(uint64_t tid, uint64_t offset, uint64_t length,
                              int r);

  uint64_t append_op_event(journal::EventEntry &event_entry);
  void commit_op_event(uint64_t tid, int r);

  void flush_event(uint64_t tid, Context *on_safe);
  void wait_event(uint64_t tid, Context *on_safe);

private:
  typedef std::list<Context *> Contexts;
  typedef interval_set<uint64_t> ExtentInterval;

  enum State {
    STATE_UNINITIALIZED,
    STATE_INITIALIZING,
    STATE_REPLAYING,
    STATE_RECORDING,
    STATE_STOPPING_RECORDING
  };

  struct Event {
    ::journal::Future future;
    AioCompletion *aio_comp;
    AioObjectRequests aio_object_requests;
    Contexts on_safe_contexts;
    ExtentInterval pending_extents;
    bool safe;
    int ret_val;

    Event() : aio_comp(NULL) {
    }
    Event(const ::journal::Future &_future, AioCompletion *_aio_comp,
          const AioObjectRequests &_requests, uint64_t offset, size_t length)
      : future(_future), aio_comp(_aio_comp), aio_object_requests(_requests),
        safe(false), ret_val(0) {
      if (length > 0) {
        pending_extents.insert(offset, length);
      }
    }
  };
  typedef ceph::unordered_map<uint64_t, Event> Events;

  struct LockListener : public ImageWatcher::Listener {
    Journal *journal;
    LockListener(Journal *_journal) : journal(_journal) {
    }

    virtual bool handle_requested_lock() {
      return journal->handle_requested_lock();
    }
    virtual void handle_lock_updated(ImageWatcher::LockUpdateState state) {
      journal->handle_lock_updated(state);
    }
  };

  struct C_InitJournal : public Context {
    Journal *journal;

    C_InitJournal(Journal *_journal) : journal(_journal) {
    }

    virtual void finish(int r) {
      journal->handle_initialized(r);
    }
  };

  struct C_EventSafe : public Context {
    Journal *journal;
    uint64_t tid;

    C_EventSafe(Journal *_journal, uint64_t _tid)
      : journal(_journal), tid(_tid) {
    }

    virtual void finish(int r) {
      journal->handle_event_safe(r, tid);
    }
  };

  struct C_WaitForReady : public Context {
    Journal *journal;
    Context *on_ready;

    C_WaitForReady(Journal *_journal, Context *_on_ready)
      : journal(_journal), on_ready(_on_ready) {
    }

    virtual void finish(int r) {
      journal->handle_wait_for_ready(on_ready);
    }
  };

  struct ReplayHandler : public ::journal::ReplayHandler {
    Journal *journal;
    ReplayHandler(Journal *_journal) : journal(_journal) {
    }

    virtual void get() {
      // TODO
    }
    virtual void put() {
      // TODO
    }

    virtual void handle_entries_available() {
      journal->handle_replay_ready();
    }
    virtual void handle_complete(int r) {
      journal->handle_replay_complete(r);
    }
  };

  ImageCtx &m_image_ctx;

  ::journal::Journaler *m_journaler;
  mutable Mutex m_lock;
  Cond m_cond;
  State m_state;

  Contexts m_wait_for_state_contexts;
  LockListener m_lock_listener;

  ReplayHandler m_replay_handler;
  bool m_close_pending;

  Mutex m_event_lock;
  uint64_t m_event_tid;
  Events m_events;

  bool m_blocking_writes;

  JournalReplay *m_journal_replay;

  ::journal::Future wait_event(Mutex &lock, uint64_t tid, Context *on_safe);

  void create_journaler();
  void destroy_journaler();

  void complete_event(Events::iterator it, int r);

  void handle_initialized(int r);

  void handle_replay_ready();
  void handle_replay_complete(int r);

  void handle_event_safe(int r, uint64_t tid);

  bool handle_requested_lock();
  void handle_lock_updated(ImageWatcher::LockUpdateState state);

  int stop_recording();

  void block_writes();
  void unblock_writes();

  void flush_journal();
  void transition_state(State state);
  void wait_for_state_transition();
  void schedule_wait_for_ready(Context *on_ready);
  void handle_wait_for_ready(Context *on_ready);

  friend std::ostream &operator<<(std::ostream &os, const State &state);
};

} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_H
