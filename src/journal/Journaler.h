// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNALER_H
#define CEPH_JOURNAL_JOURNALER_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "journal/Future.h"
#include "journal/JournalMetadataListener.h"
#include "cls/journal/cls_journal_types.h"
#include "common/Timer.h"
#include <list>
#include <map>
#include <string>
#include "include/ceph_assert.h"

class ContextWQ;
class ThreadPool;

namespace journal {

struct CacheManagerHandler;

class JournalTrimmer;
class ReplayEntry;
class ReplayHandler;
class Settings;

class Journaler {
public:
  struct Threads {
    Threads(CephContext *cct);
    ~Threads();

    ThreadPool *thread_pool = nullptr;
    ContextWQ *work_queue = nullptr;

    SafeTimer *timer;
    ceph::mutex timer_lock = ceph::make_mutex("Journaler::timer_lock");
  };

  typedef cls::journal::Tag Tag;
  typedef std::list<cls::journal::Tag> Tags;
  typedef std::set<cls::journal::Client> RegisteredClients;

  static std::string header_oid(const std::string &journal_id);
  static std::string object_oid_prefix(int pool_id,
				       const std::string &journal_id);

  Journaler(librados::IoCtx &header_ioctx, const std::string &journal_id,
	    const std::string &client_id, const Settings &settings,
            CacheManagerHandler *cache_manager_handler);
  Journaler(ContextWQ *work_queue, SafeTimer *timer, ceph::mutex *timer_lock,
            librados::IoCtx &header_ioctx, const std::string &journal_id,
	    const std::string &client_id, const Settings &settings,
            CacheManagerHandler *cache_manager_handler);
  ~Journaler();

  void exists(Context *on_finish) const;
  void create(uint8_t order, uint8_t splay_width, int64_t pool_id, Context *ctx);
  void remove(bool force, Context *on_finish);

  void init(Context *on_init);
  void shut_down();
  void shut_down(Context *on_finish);

  bool is_initialized() const;

  void get_immutable_metadata(uint8_t *order, uint8_t *splay_width,
			      int64_t *pool_id, Context *on_finish);
  void get_mutable_metadata(uint64_t *minimum_set, uint64_t *active_set,
			    RegisteredClients *clients, Context *on_finish);

  void add_listener(JournalMetadataListener *listener);
  void remove_listener(JournalMetadataListener *listener);

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
  void get_tags(uint64_t start_after_tag_tid, uint64_t tag_class, Tags *tags,
                Context *on_finish);

  void start_replay(ReplayHandler* replay_handler);
  void start_live_replay(ReplayHandler* replay_handler, double interval);
  bool try_pop_front(ReplayEntry *replay_entry, uint64_t *tag_tid = nullptr);
  void stop_replay();
  void stop_replay(Context *on_finish);

  uint64_t get_max_append_size() const;
  void start_append(uint64_t max_in_flight_appends);
  void set_append_batch_options(int flush_interval, uint64_t flush_bytes,
                                double flush_age);
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
    void finish(int r) override {
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
  CacheManagerHandler *m_cache_manager_handler;

  std::string m_header_oid;
  std::string m_object_oid_prefix;

  bool m_initialized = false;
  ceph::ref_t<class JournalMetadata> m_metadata;
  std::unique_ptr<class JournalPlayer> m_player;
  std::unique_ptr<class JournalRecorder> m_recorder;
  JournalTrimmer *m_trimmer = nullptr;

  void set_up(ContextWQ *work_queue, SafeTimer *timer, ceph::mutex *timer_lock,
              librados::IoCtx &header_ioctx, const std::string &journal_id,
              const Settings &settings);

  int init_complete();
  void create_player(ReplayHandler* replay_handler);

  friend std::ostream &operator<<(std::ostream &os,
				  const Journaler &journaler);
};

std::ostream &operator<<(std::ostream &os,
			 const Journaler &journaler);

} // namespace journal

#endif // CEPH_JOURNAL_JOURNALER_H
