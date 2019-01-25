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
#include "Utils.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "Journaler: " << this << " "

namespace journal {

namespace {

static const std::string JOURNAL_HEADER_PREFIX = "journal.";
static const std::string JOURNAL_OBJECT_PREFIX = "journal_data.";

} // anonymous namespace

using namespace cls::journal;
using utils::rados_ctx_callback;

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
  timer = nullptr;

  work_queue->drain();
  delete work_queue;
  work_queue = nullptr;

  thread_pool->stop();
  delete thread_pool;
  thread_pool = nullptr;
}

Journaler::Journaler(librados::IoCtx &header_ioctx,
                     const std::string &journal_id,
                     const std::string &client_id, const Settings &settings)
    : m_threads(new Threads(reinterpret_cast<CephContext*>(header_ioctx.cct()))),
      m_client_id(client_id) {
  set_up(m_threads->work_queue, m_threads->timer, &m_threads->timer_lock,
         header_ioctx, journal_id, settings);
}

Journaler::Journaler(ContextWQ *work_queue, SafeTimer *timer,
                     Mutex *timer_lock, librados::IoCtx &header_ioctx,
		     const std::string &journal_id,
		     const std::string &client_id, const Settings &settings)
    : m_client_id(client_id) {
  set_up(work_queue, timer, timer_lock, header_ioctx, journal_id,
         settings);
}

void Journaler::set_up(ContextWQ *work_queue, SafeTimer *timer,
                       Mutex *timer_lock, librados::IoCtx &header_ioctx,
                       const std::string &journal_id,
                       const Settings &settings) {
  m_header_ioctx.dup(header_ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_header_ioctx.cct());

  m_header_oid = header_oid(journal_id);
  m_object_oid_prefix = object_oid_prefix(m_header_ioctx.get_id(), journal_id);

  m_metadata = new JournalMetadata(work_queue, timer, timer_lock,
                                   m_header_ioctx, m_header_oid, m_client_id,
                                   settings);
  m_metadata->get();
}

Journaler::~Journaler() {
  if (m_metadata != nullptr) {
    ceph_assert(!m_metadata->is_initialized());
    if (!m_initialized) {
      // never initialized -- ensure any in-flight ops are complete
      // since we wouldn't expect shut_down to be invoked
      m_metadata->wait_for_ops();
    }
    m_metadata->put();
    m_metadata = nullptr;
  }
  ceph_assert(m_trimmer == nullptr);
  ceph_assert(m_player == nullptr);
  ceph_assert(m_recorder == nullptr);

  delete m_threads;
  m_threads = nullptr;
}

void Journaler::exists(Context *on_finish) const {
  librados::ObjectReadOperation op;
  op.stat(nullptr, nullptr, nullptr);

  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(on_finish, nullptr, rados_ctx_callback);
  int r = m_header_ioctx.aio_operate(m_header_oid, comp, &op, nullptr);
  ceph_assert(r == 0);
  comp->release();
}

void Journaler::init(Context *on_init) {
  m_initialized = true;
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
  C_SaferCond ctx;
  shut_down(&ctx);
  ctx.wait();
}

void Journaler::shut_down(Context *on_finish) {
  ceph_assert(m_player == nullptr);
  ceph_assert(m_recorder == nullptr);

  JournalMetadata *metadata = nullptr;
  ceph_assert(m_metadata != nullptr);
  std::swap(metadata, m_metadata);
  ceph_assert(metadata != nullptr);

  on_finish = new FunctionContext([metadata, on_finish](int r) {
      metadata->put();
      on_finish->complete(0);
    });

  JournalTrimmer *trimmer = nullptr;
  std::swap(trimmer, m_trimmer);
  if (!trimmer) {
    metadata->shut_down(on_finish);
    return;
  }

  on_finish = new FunctionContext([trimmer, metadata, on_finish](int r) {
      delete trimmer;
      metadata->shut_down(on_finish);
    });
  trimmer->shut_down(on_finish);
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

void Journaler::create(uint8_t order, uint8_t splay_width,
                      int64_t pool_id, Context *on_finish) {
  if (order > 26 || order < 12) {
    lderr(m_cct) << "order must be in the range [12, 26]" << dendl;
    on_finish->complete(-EDOM);
    return;
  }
  if (splay_width == 0) {
    on_finish->complete(-EINVAL);
    return;
  }

  ldout(m_cct, 5) << "creating new journal: " << m_header_oid << dendl;

  librados::ObjectWriteOperation op;
  client::create(&op, order, splay_width, pool_id);

  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(on_finish, nullptr, rados_ctx_callback);
  int r = m_header_ioctx.aio_operate(m_header_oid, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

void Journaler::remove(bool force, Context *on_finish) {
  // chain journal removal (reverse order)
  on_finish = new FunctionContext([this, on_finish](int r) {
      librados::AioCompletion *comp = librados::Rados::aio_create_completion(
        on_finish, nullptr, utils::rados_ctx_callback);
      r = m_header_ioctx.aio_remove(m_header_oid, comp);
      ceph_assert(r == 0);
      comp->release();
    });

  on_finish = new FunctionContext([this, force, on_finish](int r) {
      m_trimmer->remove_objects(force, on_finish);
    });

  m_metadata->shut_down(on_finish);
}

void Journaler::flush_commit_position(Context *on_safe) {
  m_metadata->flush_commit_position(on_safe);
}

void Journaler::add_listener(JournalMetadataListener *listener) {
  m_metadata->add_listener(listener);
}

void Journaler::remove_listener(JournalMetadataListener *listener) {
  m_metadata->remove_listener(listener);
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
  m_metadata->get_tags(0, tag_class, tags, on_finish);
}

void Journaler::get_tags(uint64_t start_after_tag_tid, uint64_t tag_class,
                         Tags *tags, Context *on_finish) {
  m_metadata->get_tags(start_after_tag_tid, tag_class, tags, on_finish);
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
  ceph_assert(m_player != nullptr);

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
  C_SaferCond ctx;
  stop_replay(&ctx);
  ctx.wait();
}

void Journaler::stop_replay(Context *on_finish) {
  JournalPlayer *player = nullptr;
  ceph_assert(m_player != nullptr);
  std::swap(player, m_player);
  ceph_assert(player != nullptr);

  on_finish = new FunctionContext([player, on_finish](int r) {
      delete player;
      on_finish->complete(r);
    });
  player->shut_down(on_finish);
}

void Journaler::committed(const ReplayEntry &replay_entry) {
  m_trimmer->committed(replay_entry.get_commit_tid());
}

void Journaler::committed(const Future &future) {
  FutureImplPtr future_impl = future.get_future_impl();
  m_trimmer->committed(future_impl->get_commit_tid());
}

void Journaler::start_append(int flush_interval, uint64_t flush_bytes,
			     double flush_age, uint64_t max_in_flight_appends) {
  ceph_assert(m_recorder == nullptr);

  // TODO verify active object set >= current replay object set

  m_recorder = new JournalRecorder(m_data_ioctx, m_object_oid_prefix,
				   m_metadata, flush_interval, flush_bytes,
				   flush_age, max_in_flight_appends);
}

void Journaler::stop_append(Context *on_safe) {
  JournalRecorder *recorder = nullptr;
  ceph_assert(m_recorder != nullptr);
  std::swap(recorder, m_recorder);
  ceph_assert(recorder != nullptr);

  on_safe = new FunctionContext([recorder, on_safe](int r) {
      delete recorder;
      on_safe->complete(r);
    });
  recorder->flush(on_safe);
}

uint64_t Journaler::get_max_append_size() const {
  uint64_t max_payload_size = m_metadata->get_object_size() -
                              Entry::get_fixed_size();
  if (m_metadata->get_settings().max_payload_bytes > 0) {
    max_payload_size = std::min(max_payload_size,
				m_metadata->get_settings().max_payload_bytes);
  }
  return max_payload_size;
}

Future Journaler::append(uint64_t tag_tid, const bufferlist &payload_bl) {
  return m_recorder->append(tag_tid, payload_bl);
}

void Journaler::flush_append(Context *on_safe) {
  m_recorder->flush(on_safe);
}

void Journaler::create_player(ReplayHandler *replay_handler) {
  ceph_assert(m_player == nullptr);
  m_player = new JournalPlayer(m_data_ioctx, m_object_oid_prefix, m_metadata,
                               replay_handler);
}

void Journaler::get_metadata(uint8_t *order, uint8_t *splay_width,
			     int64_t *pool_id) {
  ceph_assert(m_metadata != nullptr);

  *order = m_metadata->get_order();
  *splay_width = m_metadata->get_splay_width();
  *pool_id = m_metadata->get_pool_id();
}

std::ostream &operator<<(std::ostream &os,
			 const Journaler &journaler) {
  os << "[metadata=";
  if (journaler.m_metadata) {
    os << *journaler.m_metadata;
  } else {
    os << "NULL";
  }
  os << "]";
  return os;
}

} // namespace journal
