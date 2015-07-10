// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_H
#define CEPH_LIBRBD_JOURNAL_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/unordered_map.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "journal/Future.h"
#include "journal/ReplayHandler.h"
#include "librbd/ImageWatcher.h"
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
namespace journal {
class EventEntry;
}

class Journal {
public:
  typedef std::list<AioObjectRequest *> AioObjectRequests;

  Journal(ImageCtx &image_ctx);
  ~Journal();

  static bool is_journal_supported(ImageCtx &image_ctx);
  static int create(librados::IoCtx &io_ctx, const std::string &image_id);
  static int remove(librados::IoCtx &io_ctx, const std::string &image_id);

  bool is_journal_ready() const;

  void open();
  int close();

  uint64_t append_event(AioCompletion *aio_comp,
                        const journal::EventEntry &event_entry,
                        const AioObjectRequests &requests,
                        bool flush_entry);

private:
  typedef std::list<Context *> Contexts;

  enum State {
    STATE_UNINITIALIZED,
    STATE_INITIALIZING,
    STATE_REPLAYING,
    STATE_RECORDING,
  };

  struct Event {
    ::journal::Future future;
    AioCompletion *aio_comp;
    AioObjectRequests aio_object_requests;
    Contexts on_safe_contexts;

    Event() : aio_comp(NULL) {
    }
    Event(const ::journal::Future &_future, AioCompletion *_aio_comp,
          const AioObjectRequests &_requests)
      : future(_future), aio_comp(_aio_comp), aio_object_requests(_requests) {
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
    virtual void handle_releasing_lock() {
      journal->handle_releasing_lock();
    }
    virtual void handle_lock_updated(bool lock_supported, bool lock_owner) {
      journal->handle_lock_updated(lock_owner);
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

  struct ReplayHandler : public ::journal::ReplayHandler {
    Journal *journal;
    ReplayHandler(Journal *_journal) : journal(_journal) {
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

  LockListener m_lock_listener;

  ReplayHandler m_replay_handler;
  bool m_close_pending;

  uint64_t m_next_tid;
  Events m_events;

  bool m_blocking_writes;

  void create_journaler();
  void destroy_journaler();

  void handle_initialized(int r);

  void handle_replay_ready();
  void handle_replay_complete(int r);

  void handle_event_safe(int r, uint64_t tid);

  bool handle_requested_lock();
  void handle_releasing_lock();
  void handle_lock_updated(bool lock_owner);

  int stop_recording();

  void block_writes();
  void unblock_writes();

  void transition_state(State state);
  void wait_for_state_transition();
};

} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_H
