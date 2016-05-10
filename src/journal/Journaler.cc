// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/Journaler.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "journal/Entry.h"
#include "journal/FutureImpl.h"
#include "journal/JournalMetadata.h"
#include "journal/JournalPlayer.h"
#include "journal/JournalRecorder.h"
#include "journal/JournalTrimmer.h"
#include "journal/ReplayEntry.h"
#include "journal/ReplayHandler.h"
#include "cls/journal/cls_journal_client.h"
#include "cls/journal/cls_journal_types.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "Journaler: "

namespace journal {

namespace {

static const std::string JOURNAL_HEADER_PREFIX = "journal.";
static const std::string JOURNAL_OBJECT_PREFIX = "journal_data.";

struct C_DeleteRecorder : public Context {
  JournalRecorder *recorder;
  Context *on_safe;
  C_DeleteRecorder(JournalRecorder *_recorder, Context *_on_safe)
    : recorder(_recorder), on_safe(_on_safe) {
  }
  virtual void finish(int r) {
    delete recorder;
    on_safe->complete(r);
  }
};

} // anonymous namespace

using namespace cls::journal;

std::string Journaler::header_oid(const std::string &journal_id) {
  return JOURNAL_HEADER_PREFIX + journal_id;
}

std::string Journaler::object_oid_prefix(int pool_id,
					 const std::string &journal_id) {
  return JOURNAL_OBJECT_PREFIX + stringify(pool_id) + "." + journal_id + ".";
}

Journaler::Threads::Threads(CephContext *cct)
    : timer_lock("Journaler::timer_lock") {
  thread_pool = new ThreadPool(cct, "Journaler::thread_pool", "tp_journal", 1);
  thread_pool->start();

  work_queue = new ContextWQ("Journaler::work_queue", 60, thread_pool);

  timer = new SafeTimer(cct, timer_lock, true);
  timer->init();
}

Journaler::Threads::~Threads() {
  {
    Mutex::Locker timer_locker(timer_lock);
    timer->shutdown();
  }
  delete timer;

  work_queue->drain();
  delete work_queue;

  thread_pool->stop();
  delete thread_pool;
}

Journaler::Journaler(librados::IoCtx &header_ioctx,
                     const std::string &journal_id,
                     const std::string &client_id, double commit_interval)
    : m_threads(new Threads(reinterpret_cast<CephContext*>(header_ioctx.cct()))),
      m_client_id(client_id) {
  set_up(m_threads->work_queue, m_threads->timer, &m_threads->timer_lock,
         header_ioctx, journal_id, commit_interval);
}

Journaler::Journaler(ContextWQ *work_queue, SafeTimer *timer,
                     Mutex *timer_lock, librados::IoCtx &header_ioctx,
		     const std::string &journal_id,
		     const std::string &client_id, double commit_interval)
    : m_client_id(client_id) {
  set_up(work_queue, timer, timer_lock, header_ioctx, journal_id,
         commit_interval);
}

void Journaler::set_up(ContextWQ *work_queue, SafeTimer *timer,
                       Mutex *timer_lock, librados::IoCtx &header_ioctx,
                       const std::string &journal_id, double commit_interval) {
  m_header_ioctx.dup(header_ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_header_ioctx.cct());

  m_header_oid = header_oid(journal_id);
  m_object_oid_prefix = object_oid_prefix(m_header_ioctx.get_id(), journal_id);

  m_metadata = new JournalMetadata(work_queue, timer, timer_lock,
                                   m_header_ioctx, m_header_oid, m_client_id,
                                   commit_interval);
  m_metadata->get();
}

Journaler::~Journaler() {
  if (m_metadata != nullptr) {
    m_metadata->put();
    m_metadata = nullptr;
  }
  delete m_trimmer;
  assert(m_player == nullptr);
  assert(m_recorder == nullptr);

  delete m_threads;
}

int Journaler::exists(bool *header_exists) const {
  int r = m_header_ioctx.stat(m_header_oid, NULL, NULL);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  *header_exists = (r == 0);
  return 0;
}

void Journaler::init(Context *on_init) {
  m_metadata->init(new C_InitJournaler(this, on_init));
}

int Journaler::init_complete() {
  int64_t pool_id = m_metadata->get_pool_id();

  if (pool_id < 0 || pool_id == m_header_ioctx.get_id()) {
    ldout(m_cct, 20) << "using image pool for journal data" << dendl;
    m_data_ioctx.dup(m_header_ioctx);
  } else {
    ldout(m_cct, 20) << "using pool id=" << pool_id << " for journal data"
		     << dendl;
    librados::Rados rados(m_header_ioctx);
    int r = rados.ioctx_create2(pool_id, m_data_ioctx);
    if (r < 0) {
      if (r == -ENOENT) {
	ldout(m_cct, 1) << "pool id=" << pool_id << " no longer exists"
			<< dendl;
      }
      return r;
    }
  }
  m_trimmer = new JournalTrimmer(m_data_ioctx, m_object_oid_prefix,
                                 m_metadata);
  return 0;
}

void Journaler::shut_down() {
  m_metadata->shut_down();
}

bool Journaler::is_initialized() const {
  return m_metadata->is_initialized();
}

void Journaler::get_immutable_metadata(uint8_t *order, uint8_t *splay_width,
				       int64_t *pool_id, Context *on_finish) {
  m_metadata->get_immutable_metadata(order, splay_width, pool_id, on_finish);
}

void Journaler::get_mutable_metadata(uint64_t *minimum_set,
				     uint64_t *active_set,
				     RegisteredClients *clients,
				     Context *on_finish) {
  m_metadata->get_mutable_metadata(minimum_set, active_set, clients, on_finish);
}

int Journaler::create(uint8_t order, uint8_t splay_width, int64_t pool_id) {
  if (order > 64 || order < 12) {
    lderr(m_cct) << "order must be in the range [12, 64]" << dendl;
    return -EDOM;
  }
  if (splay_width == 0) {
    return -EINVAL;
  }

  ldout(m_cct, 5) << "creating new journal: " << m_header_oid << dendl;
  int r = client::create(m_header_ioctx, m_header_oid, order, splay_width,
			 pool_id);
  if (r < 0) {
    lderr(m_cct) << "failed to create journal: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int Journaler::remove(bool force) {
  m_metadata->shut_down();

  ldout(m_cct, 5) << "removing journal: " << m_header_oid << dendl;
  int r = m_trimmer->remove_objects(force);
  if (r < 0) {
    lderr(m_cct) << "failed to remove journal objects: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  r = m_header_ioctx.remove(m_header_oid);
  if (r < 0) {
    lderr(m_cct) << "failed to remove journal header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }
  return 0;
}

void Journaler::flush_commit_position(Context *on_safe) {
  m_metadata->flush_commit_position(on_safe);
}

int Journaler::register_client(const bufferlist &data) {
  C_SaferCond cond;
  register_client(data, &cond);
  return cond.wait();
}

int Journaler::unregister_client() {
  C_SaferCond cond;
  unregister_client(&cond);
  return cond.wait();
}

void Journaler::register_client(const bufferlist &data, Context *on_finish) {
  return m_metadata->register_client(data, on_finish);
}

void Journaler::update_client(const bufferlist &data, Context *on_finish) {
  return m_metadata->update_client(data, on_finish);
}

void Journaler::unregister_client(Context *on_finish) {
  return m_metadata->unregister_client(on_finish);
}

void Journaler::get_client(const std::string &client_id,
                           cls::journal::Client *client,
                           Context *on_finish) {
  m_metadata->get_client(client_id, client, on_finish);
}

int Journaler::get_cached_client(const std::string &client_id,
                                 cls::journal::Client *client) {
  RegisteredClients clients;
  m_metadata->get_registered_clients(&clients);

  auto it = clients.find({client_id, {}});
  if (it == clients.end()) {
    return -ENOENT;
  }

  *client = *it;
  return 0;
}

void Journaler::allocate_tag(const bufferlist &data, cls::journal::Tag *tag,
                             Context *on_finish) {
  m_metadata->allocate_tag(cls::journal::Tag::TAG_CLASS_NEW, data, tag,
                           on_finish);
}

void Journaler::allocate_tag(uint64_t tag_class, const bufferlist &data,
                             cls::journal::Tag *tag, Context *on_finish) {
  m_metadata->allocate_tag(tag_class, data, tag, on_finish);
}

void Journaler::get_tag(uint64_t tag_tid, Tag *tag, Context *on_finish) {
  m_metadata->get_tag(tag_tid, tag, on_finish);
}

void Journaler::get_tags(uint64_t tag_class, Tags *tags, Context *on_finish) {
  m_metadata->get_tags(tag_class, tags, on_finish);
}

void Journaler::start_replay(ReplayHandler *replay_handler) {
  create_player(replay_handler);
  m_player->prefetch();
}

void Journaler::start_live_replay(ReplayHandler *replay_handler,
                                  double interval) {
  create_player(replay_handler);
  m_player->prefetch_and_watch(interval);
}

bool Journaler::try_pop_front(ReplayEntry *replay_entry,
			      uint64_t *tag_tid) {
  assert(m_player != NULL);

  Entry entry;
  uint64_t commit_tid;
  if (!m_player->try_pop_front(&entry, &commit_tid)) {
    return false;
  }

  *replay_entry = ReplayEntry(entry.get_data(), commit_tid);
  if (tag_tid != nullptr) {
    *tag_tid = entry.get_tag_tid();
  }
  return true;
}

void Journaler::stop_replay() {
  assert(m_player != NULL);
  m_player->unwatch();
  delete m_player;
  m_player = NULL;
}

void Journaler::committed(const ReplayEntry &replay_entry) {
  m_trimmer->committed(replay_entry.get_commit_tid());
}

void Journaler::committed(const Future &future) {
  FutureImplPtr future_impl = future.get_future_impl();
  m_trimmer->committed(future_impl->get_commit_tid());
}

void Journaler::start_append(int flush_interval, uint64_t flush_bytes,
			     double flush_age) {
  assert(m_recorder == NULL);

  // TODO verify active object set >= current replay object set

  m_recorder = new JournalRecorder(m_data_ioctx, m_object_oid_prefix,
				   m_metadata, flush_interval, flush_bytes,
				   flush_age);
}

void Journaler::stop_append(Context *on_safe) {
  assert(m_recorder != NULL);

  flush_append(new C_DeleteRecorder(m_recorder, on_safe));
  m_recorder = NULL;
}

uint64_t Journaler::get_max_append_size() const {
  return m_metadata->get_object_size() - Entry::get_fixed_size();
}

Future Journaler::append(uint64_t tag_tid, const bufferlist &payload_bl) {
  return m_recorder->append(tag_tid, payload_bl);
}

void Journaler::flush_append(Context *on_safe) {
  m_recorder->flush(on_safe);
}

void Journaler::create_player(ReplayHandler *replay_handler) {
  assert(m_player == NULL);
  m_player = new JournalPlayer(m_data_ioctx, m_object_oid_prefix, m_metadata,
                               replay_handler);
}

void Journaler::get_metadata(uint8_t *order, uint8_t *splay_width,
			     int64_t *pool_id) {
  assert(m_metadata != NULL);

  *order = m_metadata->get_order();
  *splay_width = m_metadata->get_splay_width();
  *pool_id = m_metadata->get_pool_id();
}

std::ostream &operator<<(std::ostream &os,
			 const Journaler &journaler) {
  os << "[metadata=";
  if (journaler.m_metadata != NULL) {
    os << *journaler.m_metadata;
  } else {
    os << "NULL";
  }
  os << "]";
  return os;
}

} // namespace journal
