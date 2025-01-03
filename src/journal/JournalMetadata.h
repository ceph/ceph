// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNAL_METADATA_H
#define CEPH_JOURNAL_JOURNAL_METADATA_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/AsyncOpTracker.h"
#include "common/Cond.h"
#include "common/Timer.h"
#include "common/ceph_mutex.h"
#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "cls/journal/cls_journal_types.h"
#include "journal/JournalMetadataListener.h"
#include "journal/Settings.h"
#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <functional>
#include <list>
#include <map>
#include <string>
#include "include/ceph_assert.h"

namespace journal {

class JournalMetadata : public RefCountedObject, boost::noncopyable {
public:
  typedef std::function<Context*()> CreateContext;
  typedef cls::journal::ObjectPosition ObjectPosition;
  typedef cls::journal::ObjectPositions ObjectPositions;
  typedef cls::journal::ObjectSetPosition ObjectSetPosition;
  typedef cls::journal::Client Client;
  typedef cls::journal::Tag Tag;

  typedef std::set<Client> RegisteredClients;
  typedef std::list<Tag> Tags;

  void init(Context *on_init);
  void shut_down(Context *on_finish);

  bool is_initialized() const { return m_initialized; }

  void get_immutable_metadata(uint8_t *order, uint8_t *splay_width,
			      int64_t *pool_id, Context *on_finish);

  void get_mutable_metadata(uint64_t *minimum_set, uint64_t *active_set,
			    RegisteredClients *clients, Context *on_finish);

  void add_listener(JournalMetadataListener *listener);
  void remove_listener(JournalMetadataListener *listener);

  void register_client(const bufferlist &data, Context *on_finish);
  void update_client(const bufferlist &data, Context *on_finish);
  void unregister_client(Context *on_finish);
  void get_client(const std::string &client_id, cls::journal::Client *client,
                  Context *on_finish);

  void allocate_tag(uint64_t tag_class, const bufferlist &data,
                    Tag *tag, Context *on_finish);
  void get_tag(uint64_t tag_tid, Tag *tag, Context *on_finish);
  void get_tags(uint64_t start_after_tag_tid,
                const boost::optional<uint64_t> &tag_class, Tags *tags,
                Context *on_finish);

  inline const Settings &get_settings() const {
    return m_settings;
  }
  inline const std::string &get_client_id() const {
    return m_client_id;
  }
  inline uint8_t get_order() const {
    return m_order;
  }
  inline uint64_t get_object_size() const {
    return 1 << m_order;
  }
  inline uint8_t get_splay_width() const {
    return m_splay_width;
  }
  inline int64_t get_pool_id() const {
    return m_pool_id;
  }

  inline void queue(Context *on_finish, int r) {
    m_work_queue->queue(on_finish, r);
  }

  inline ContextWQ *get_work_queue() {
    return m_work_queue;
  }

  inline SafeTimer &get_timer() {
    return *m_timer;
  }
  inline ceph::mutex &get_timer_lock() {
    return *m_timer_lock;
  }

  void set_minimum_set(uint64_t object_set);
  inline uint64_t get_minimum_set() const {
    std::lock_guard locker{m_lock};
    return m_minimum_set;
  }

  int set_active_set(uint64_t object_set);
  void set_active_set(uint64_t object_set, Context *on_finish);
  inline uint64_t get_active_set() const {
    std::lock_guard locker{m_lock};
    return m_active_set;
  }

  void assert_active_tag(uint64_t tag_tid, Context *on_finish);

  void flush_commit_position();
  void flush_commit_position(Context *on_safe);
  void get_commit_position(ObjectSetPosition *commit_position) const {
    std::lock_guard locker{m_lock};
    *commit_position = m_client.commit_position;
  }

  void get_registered_clients(RegisteredClients *registered_clients) {
    std::lock_guard locker{m_lock};
    *registered_clients = m_registered_clients;
  }

  inline uint64_t allocate_entry_tid(uint64_t tag_tid) {
    std::lock_guard locker{m_lock};
    return m_allocated_entry_tids[tag_tid]++;
  }
  void reserve_entry_tid(uint64_t tag_tid, uint64_t entry_tid);
  bool get_last_allocated_entry_tid(uint64_t tag_tid, uint64_t *entry_tid) const;

  uint64_t allocate_commit_tid(uint64_t object_num, uint64_t tag_tid,
                               uint64_t entry_tid);
  void overflow_commit_tid(uint64_t commit_tid, uint64_t object_num);
  void get_commit_entry(uint64_t commit_tid, uint64_t *object_num,
                        uint64_t *tag_tid, uint64_t *entry_tid);
  void committed(uint64_t commit_tid, const CreateContext &create_context);

  void notify_update();
  void async_notify_update(Context *on_safe);

  void wait_for_ops();

private:
  FRIEND_MAKE_REF(JournalMetadata);
  JournalMetadata(ContextWQ *work_queue, SafeTimer *timer, ceph::mutex *timer_lock,
                  librados::IoCtx &ioctx, const std::string &oid,
                  const std::string &client_id, const Settings &settings);
  ~JournalMetadata() override;

  typedef std::map<uint64_t, uint64_t> AllocatedEntryTids;
  typedef std::list<JournalMetadataListener*> Listeners;
  typedef std::list<Context*> Contexts;

  struct CommitEntry {
    uint64_t object_num;
    uint64_t tag_tid;
    uint64_t entry_tid;
    bool committed;

    CommitEntry() : object_num(0), tag_tid(0), entry_tid(0), committed(false) {
    }
    CommitEntry(uint64_t _object_num, uint64_t _tag_tid, uint64_t _entry_tid)
      : object_num(_object_num), tag_tid(_tag_tid), entry_tid(_entry_tid),
        committed(false) {
    }
  };
  typedef std::map<uint64_t, CommitEntry> CommitTids;

  struct C_WatchCtx : public librados::WatchCtx2 {
    JournalMetadata *journal_metadata;

    C_WatchCtx(JournalMetadata *_journal_metadata)
      : journal_metadata(_journal_metadata) {}

    void handle_notify(uint64_t notify_id, uint64_t cookie,
                               uint64_t notifier_id, bufferlist& bl) override {
      journal_metadata->handle_watch_notify(notify_id, cookie);
    }
    void handle_error(uint64_t cookie, int err) override {
      journal_metadata->handle_watch_error(err);
    }
  };

  struct C_WatchReset : public Context {
    JournalMetadata *journal_metadata;

    C_WatchReset(JournalMetadata *_journal_metadata)
      : journal_metadata(_journal_metadata) {
      journal_metadata->m_async_op_tracker.start_op();
    }
    ~C_WatchReset() override {
      journal_metadata->m_async_op_tracker.finish_op();
    }
    void finish(int r) override {
      journal_metadata->handle_watch_reset();
    }
  };

  struct C_CommitPositionTask : public Context {
    JournalMetadata *journal_metadata;

    C_CommitPositionTask(JournalMetadata *_journal_metadata)
      : journal_metadata(_journal_metadata) {
      journal_metadata->m_async_op_tracker.start_op();
    }
    ~C_CommitPositionTask() override {
      journal_metadata->m_async_op_tracker.finish_op();
    }
    void finish(int r) override {
      std::lock_guard locker{journal_metadata->m_lock};
      journal_metadata->handle_commit_position_task();
    };
  };

  struct C_AioNotify : public Context {
    JournalMetadata* journal_metadata;
    Context *on_safe;

    C_AioNotify(JournalMetadata *_journal_metadata, Context *_on_safe)
      : journal_metadata(_journal_metadata), on_safe(_on_safe) {
      journal_metadata->m_async_op_tracker.start_op();
    }
    ~C_AioNotify() override {
      journal_metadata->m_async_op_tracker.finish_op();
    }
    void finish(int r) override {
      journal_metadata->handle_notified(r);
      if (on_safe != nullptr) {
        on_safe->complete(0);
      }
    }
  };

  struct C_NotifyUpdate : public Context {
    JournalMetadata* journal_metadata;
    Context *on_safe;

    C_NotifyUpdate(JournalMetadata *_journal_metadata, Context *_on_safe = NULL)
      : journal_metadata(_journal_metadata), on_safe(_on_safe) {
      journal_metadata->m_async_op_tracker.start_op();
    }
    ~C_NotifyUpdate() override {
      journal_metadata->m_async_op_tracker.finish_op();
    }
    void finish(int r) override {
      if (r == 0) {
        journal_metadata->async_notify_update(on_safe);
        return;
      }
      if (on_safe != NULL) {
        on_safe->complete(r);
      }
    }
  };

  struct C_ImmutableMetadata : public Context {
    JournalMetadata* journal_metadata;
    Context *on_finish;

    C_ImmutableMetadata(JournalMetadata *_journal_metadata, Context *_on_finish)
      : journal_metadata(_journal_metadata), on_finish(_on_finish) {
      std::lock_guard locker{journal_metadata->m_lock};
      journal_metadata->m_async_op_tracker.start_op();
    }
    ~C_ImmutableMetadata() override {
      journal_metadata->m_async_op_tracker.finish_op();
    }
    void finish(int r) override {
      journal_metadata->handle_immutable_metadata(r, on_finish);
    }
  };

  struct C_Refresh : public Context {
    JournalMetadata* journal_metadata;
    uint64_t minimum_set;
    uint64_t active_set;
    RegisteredClients registered_clients;

    C_Refresh(JournalMetadata *_journal_metadata)
      : journal_metadata(_journal_metadata), minimum_set(0), active_set(0) {
      std::lock_guard locker{journal_metadata->m_lock};
      journal_metadata->m_async_op_tracker.start_op();
    }
    ~C_Refresh() override {
      journal_metadata->m_async_op_tracker.finish_op();
    }
    void finish(int r) override {
      journal_metadata->handle_refresh_complete(this, r);
    }
  };

  librados::IoCtx m_ioctx;
  CephContext *m_cct = nullptr;
  std::string m_oid;
  std::string m_client_id;
  Settings m_settings;

  uint8_t m_order = 0;
  uint8_t m_splay_width = 0;
  int64_t m_pool_id = -1;
  bool m_initialized = false;

  ContextWQ *m_work_queue;
  SafeTimer *m_timer;
  ceph::mutex *m_timer_lock;

  mutable ceph::mutex m_lock = ceph::make_mutex("JournalMetadata::m_lock");

  uint64_t m_commit_tid = 0;
  CommitTids m_pending_commit_tids;

  Listeners m_listeners;

  C_WatchCtx m_watch_ctx;
  uint64_t m_watch_handle = 0;

  uint64_t m_minimum_set = 0;
  uint64_t m_active_set = 0;
  RegisteredClients m_registered_clients;
  Client m_client;

  AllocatedEntryTids m_allocated_entry_tids;

  size_t m_update_notifications = 0;
  ceph::condition_variable m_update_cond;

  size_t m_ignore_watch_notifies = 0;
  size_t m_refreshes_in_progress = 0;
  Contexts m_refresh_ctxs;

  uint64_t m_commit_position_tid = 0;
  ObjectSetPosition m_commit_position;
  Context *m_commit_position_ctx = nullptr;
  Context *m_commit_position_task_ctx = nullptr;

  size_t m_flush_commits_in_progress = 0;
  Contexts m_flush_commit_position_ctxs;

  AsyncOpTracker m_async_op_tracker;

  void handle_immutable_metadata(int r, Context *on_init);

  void refresh(Context *on_finish);
  void handle_refresh_complete(C_Refresh *refresh, int r);

  void cancel_commit_task();
  void schedule_commit_task();
  void handle_commit_position_task();

  void schedule_watch_reset();
  void handle_watch_reset();
  void handle_watch_notify(uint64_t notify_id, uint64_t cookie);
  void handle_watch_error(int err);
  void handle_notified(int r);

  void schedule_laggy_clients_disconnect(Context *on_finish);

  friend std::ostream &operator<<(std::ostream &os,
				  const JournalMetadata &journal_metadata);
};

std::ostream &operator<<(std::ostream &os,
			 const JournalMetadata::RegisteredClients &clients);

std::ostream &operator<<(std::ostream &os,
			 const JournalMetadata &journal_metadata);

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_METADATA_H
