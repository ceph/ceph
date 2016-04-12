// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNALER_H
#define CEPH_JOURNAL_JOURNALER_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "journal/Future.h"
#include "cls/journal/cls_journal_types.h"
#include <list>
#include <map>
#include <string>
#include "include/assert.h"

class ContextWQ;
class SafeTimer;
class ThreadPool;

namespace journal {

class JournalMetadata;
class JournalPlayer;
class JournalRecorder;
class JournalTrimmer;
class ReplayEntry;
class ReplayHandler;

class Journaler {
public:
  struct Threads {
    Threads(CephContext *cct);
    ~Threads();

    ThreadPool *thread_pool = nullptr;
    ContextWQ *work_queue = nullptr;

    SafeTimer *timer = nullptr;
    Mutex timer_lock;
  };

  typedef cls::journal::Tag Tag;
  typedef std::list<cls::journal::Tag> Tags;
  typedef std::set<cls::journal::Client> RegisteredClients;

  static std::string header_oid(const std::string &journal_id);
  static std::string object_oid_prefix(int pool_id,
				       const std::string &journal_id);

  Journaler(librados::IoCtx &header_ioctx, const std::string &journal_id,
	    const std::string &client_id, double commit_interval);
  Journaler(ContextWQ *work_queue, SafeTimer *timer, Mutex *timer_lock,
            librados::IoCtx &header_ioctx, const std::string &journal_id,
	    const std::string &client_id, double commit_interval);
  ~Journaler();

  int exists(bool *header_exists) const;
  int create(uint8_t order, uint8_t splay_width, int64_t pool_id);
  int remove(bool force);

  void init(Context *on_init);
  void shut_down();

  bool is_initialized() const;

  void get_immutable_metadata(uint8_t *order, uint8_t *splay_width,
			      int64_t *pool_id, Context *on_finish);
  void get_mutable_metadata(uint64_t *minimum_set, uint64_t *active_set,
			    RegisteredClients *clients, Context *on_finish);

  int register_client(const bufferlist &data);
  void register_client(const bufferlist &data, Context *on_finish);

  int unregister_client();
  void unregister_client(Context *on_finish);

  void update_client(const bufferlist &data, Context *on_finish);
  void get_client(const std::string &client_id, cls::journal::Client *client,
                  Context *on_finish);
  int get_cached_client(const std::string &client_id,
                        cls::journal::Client *client);

  void flush_commit_position(Context *on_safe);

  void allocate_tag(const bufferlist &data, cls::journal::Tag *tag,
                    Context *on_finish);
  void allocate_tag(uint64_t tag_class, const bufferlist &data,
                    cls::journal::Tag *tag, Context *on_finish);
  void get_tag(uint64_t tag_tid, Tag *tag, Context *on_finish);
  void get_tags(uint64_t tag_class, Tags *tags, Context *on_finish);

  void start_replay(ReplayHandler *replay_handler);
  void start_live_replay(ReplayHandler *replay_handler, double interval);
  bool try_pop_front(ReplayEntry *replay_entry, uint64_t *tag_tid = nullptr);
  void stop_replay();

  void start_append(int flush_interval, uint64_t flush_bytes, double flush_age);
  Future append(uint64_t tag_tid, const bufferlist &bl);
  void flush_append(Context *on_safe);
  void stop_append(Context *on_safe);

  void committed(const ReplayEntry &replay_entry);
  void committed(const Future &future);

  void get_metadata(uint8_t *order, uint8_t *splay_width, int64_t *pool_id);

private:
  struct C_InitJournaler : public Context {
    Journaler *journaler;
    Context *on_safe;
    C_InitJournaler(Journaler *_journaler, Context *_on_safe)
      : journaler(_journaler), on_safe(_on_safe) {
    }
    virtual void finish(int r) {
      if (r == 0) {
	r = journaler->init_complete();
      }
      on_safe->complete(r);
    }
  };

  Threads *m_threads = nullptr;

  mutable librados::IoCtx m_header_ioctx;
  librados::IoCtx m_data_ioctx;
  CephContext *m_cct;
  std::string m_client_id;

  std::string m_header_oid;
  std::string m_object_oid_prefix;

  JournalMetadata *m_metadata = nullptr;
  JournalPlayer *m_player = nullptr;
  JournalRecorder *m_recorder = nullptr;
  JournalTrimmer *m_trimmer = nullptr;

  void set_up(ContextWQ *work_queue, SafeTimer *timer, Mutex *timer_lock,
              librados::IoCtx &header_ioctx, const std::string &journal_id,
              double commit_interval);

  int init_complete();
  void create_player(ReplayHandler *replay_handler);

  friend std::ostream &operator<<(std::ostream &os,
				  const Journaler &journaler);
};

std::ostream &operator<<(std::ostream &os,
			 const Journaler &journaler);

} // namespace journal

#endif // CEPH_JOURNAL_JOURNALER_H
