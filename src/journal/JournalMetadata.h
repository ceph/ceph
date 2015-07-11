// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNAL_METADATA_H
#define CEPH_JOURNAL_JOURNAL_METADATA_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/RefCountedObj.h"
#include "cls/journal/cls_journal_types.h"
#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <list>
#include <map>
#include <string>
#include "include/assert.h"

class SafeTimer;

namespace journal {

class JournalMetadata;
typedef boost::intrusive_ptr<JournalMetadata> JournalMetadataPtr;

class JournalMetadata : public RefCountedObject, boost::noncopyable {
public:
  typedef cls::journal::EntryPosition EntryPosition;
  typedef cls::journal::EntryPositions EntryPositions;
  typedef cls::journal::ObjectSetPosition ObjectSetPosition;
  typedef cls::journal::Client Client;

  typedef std::set<Client> RegisteredClients;

  struct Listener {
    virtual ~Listener() {};
    virtual void handle_update(JournalMetadata *) = 0;
  };

  JournalMetadata(librados::IoCtx &ioctx, const std::string &oid,
                  const std::string &client_id, double commit_interval);
  ~JournalMetadata();

  void init(Context *on_init);

  void add_listener(Listener *listener);
  void remove_listener(Listener *listener);

  int register_client(const std::string &description);
  int unregister_client();

  inline const std::string &get_client_id() const {
    return m_client_id;
  }
  inline uint8_t get_order() const {
    return m_order;
  }
  inline uint8_t get_splay_width() const {
    return m_splay_width;
  }

  inline SafeTimer &get_timer() {
    return *m_timer;
  }
  inline Mutex &get_timer_lock() {
    return m_timer_lock;
  }

  void set_minimum_set(uint64_t object_set);
  inline uint64_t get_minimum_set() const {
    Mutex::Locker locker(m_lock);
    return m_minimum_set;
  }

  void set_active_set(uint64_t object_set);
  inline uint64_t get_active_set() const {
    Mutex::Locker locker(m_lock);
    return m_active_set;
  }

  void set_commit_position(const ObjectSetPosition &commit_position,
                           Context *on_safe);
  void get_commit_position(ObjectSetPosition *commit_position) const {
    Mutex::Locker locker(m_lock);
    *commit_position = m_client.commit_position;
  }

  void get_registered_clients(RegisteredClients *registered_clients) {
    Mutex::Locker locker(m_lock);
    *registered_clients = m_registered_clients;
  }

  inline uint64_t allocate_tid(const std::string &tag) {
    Mutex::Locker locker(m_lock);
    return m_allocated_tids[tag]++;
  }
  void reserve_tid(const std::string &tag, uint64_t tid);
  bool get_last_allocated_tid(const std::string &tag, uint64_t *tid) const;

  void notify_update();
  void async_notify_update();

private:
  typedef std::map<std::string, uint64_t> AllocatedTids;
  typedef std::list<Listener*> Listeners;

  struct C_WatchCtx : public librados::WatchCtx2 {
    JournalMetadata *journal_metadata;

    C_WatchCtx(JournalMetadata *_journal_metadata)
      : journal_metadata(_journal_metadata) {}

    virtual void handle_notify(uint64_t notify_id, uint64_t cookie,
                               uint64_t notifier_id, bufferlist& bl) {
      journal_metadata->handle_watch_notify(notify_id, cookie);
    }
    virtual void handle_error(uint64_t cookie, int err) {
      journal_metadata->handle_watch_error(err);
    }
  };

  struct C_WatchReset : public Context {
    JournalMetadataPtr journal_metadata;

    C_WatchReset(JournalMetadata *_journal_metadata)
      : journal_metadata(_journal_metadata) {}

    virtual void finish(int r) {
      journal_metadata->handle_watch_reset();
    }
  };

  struct C_CommitPositionTask : public Context {
    JournalMetadataPtr journal_metadata;

    C_CommitPositionTask(JournalMetadata *_journal_metadata)
      : journal_metadata(_journal_metadata) {}

    virtual void finish(int r) {
      journal_metadata->handle_commit_position_task();
    };
  };

  struct C_NotifyUpdate : public Context {
    JournalMetadataPtr journal_metadata;
    Context *on_safe;

    C_NotifyUpdate(JournalMetadata *_journal_metadata, Context *_on_safe = NULL)
      : journal_metadata(_journal_metadata), on_safe(_on_safe) {}

    virtual void finish(int r) {
      if (r == 0) {
        journal_metadata->async_notify_update();
      }
      if (on_safe != NULL) {
        on_safe->complete(r);
      }
    }
  };

  struct C_ImmutableMetadata : public Context {
    JournalMetadataPtr journal_metadata;
    Context *on_finish;

    C_ImmutableMetadata(JournalMetadata *_journal_metadata, Context *_on_finish)
      : journal_metadata(_journal_metadata), on_finish(_on_finish) {
    }

    virtual void finish(int r) {
      journal_metadata->handle_immutable_metadata(r, on_finish);
    }

  };
  struct C_Refresh : public Context {
    JournalMetadataPtr journal_metadata;
    uint64_t minimum_set;
    uint64_t active_set;
    RegisteredClients registered_clients;
    Context *on_finish;

    C_Refresh(JournalMetadata *_journal_metadata, Context *_on_finish)
      : journal_metadata(_journal_metadata), minimum_set(0), active_set(0),
        on_finish(_on_finish) {}

    virtual void finish(int r) {
      journal_metadata->handle_refresh_complete(this, r);
    }
  };

  librados::IoCtx m_ioctx;
  CephContext *m_cct;
  std::string m_oid;
  std::string m_client_id;
  double m_commit_interval;

  uint8_t m_order;
  uint8_t m_splay_width;
  bool m_initialized;

  SafeTimer *m_timer;
  Mutex m_timer_lock;

  mutable Mutex m_lock;

  Listeners m_listeners;

  C_WatchCtx m_watch_ctx;
  uint64_t m_watch_handle;

  uint64_t m_minimum_set;
  uint64_t m_active_set;
  RegisteredClients m_registered_clients;
  Client m_client;

  AllocatedTids m_allocated_tids;

  size_t m_update_notifications;
  Cond m_update_cond;

  bool m_commit_position_pending;
  ObjectSetPosition m_commit_position;
  Context *m_commit_position_ctx;

  void handle_immutable_metadata(int r, Context *on_init);

  void refresh(Context *on_finish);
  void handle_refresh_complete(C_Refresh *refresh, int r);

  void schedule_commit_task();
  void handle_commit_position_task();

  void schedule_watch_reset();
  void handle_watch_reset();
  void handle_watch_notify(uint64_t notify_id, uint64_t cookie);
  void handle_watch_error(int err);
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_METADATA_H
