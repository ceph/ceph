// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalRecorder.h"
#include "common/errno.h"
#include "journal/Entry.h"
#include "journal/Utils.h"

#include <atomic>

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalRecorder: " << this << " " << __func__ \
                           << ": "

using std::shared_ptr;

namespace journal {

namespace {

struct C_Flush : public Context {
  ceph::ref_t<JournalMetadata> journal_metadata;
  Context *on_finish;
  std::atomic<int64_t> pending_flushes{0};
  int ret_val = 0;

  C_Flush(ceph::ref_t<JournalMetadata> _journal_metadata, Context *_on_finish,
          size_t _pending_flushes)
    : journal_metadata(std::move(_journal_metadata)),
      on_finish(_on_finish),
      pending_flushes(_pending_flushes) {
  }

  void complete(int r) override {
    if (r < 0 && ret_val == 0) {
      ret_val = r;
    }
    if (--pending_flushes == 0) {
      // ensure all prior callback have been flushed as well
      journal_metadata->queue(on_finish, ret_val);
      delete this;
    }
  }
  void finish(int r) override {
  }
};

} // anonymous namespace

JournalRecorder::JournalRecorder(librados::IoCtx &ioctx,
                                 std::string_view object_oid_prefix,
                                 ceph::ref_t<JournalMetadata> journal_metadata,
                                 uint64_t max_in_flight_appends)
  : m_object_oid_prefix(object_oid_prefix),
    m_journal_metadata(std::move(journal_metadata)),
    m_max_in_flight_appends(max_in_flight_appends),
    m_listener(this),
    m_object_handler(this),
    m_current_set(m_journal_metadata->get_active_set()),
    m_object_locks{ceph::make_lock_container<ceph::mutex>(
      m_journal_metadata->get_splay_width(), [](const size_t splay_offset) {
      return ceph::make_mutex("ObjectRecorder::m_lock::" +
			      std::to_string(splay_offset));
    })}
{
  std::lock_guard locker{m_lock};
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  for (uint8_t splay_offset = 0; splay_offset < splay_width; ++splay_offset) {
    uint64_t object_number = splay_offset + (m_current_set * splay_width);
    std::lock_guard locker{m_object_locks[splay_offset]};
    m_object_ptrs[splay_offset] = create_object_recorder(
      object_number, &m_object_locks[splay_offset]);
  }

  m_journal_metadata->add_listener(&m_listener);
}

JournalRecorder::~JournalRecorder() {
  m_journal_metadata->remove_listener(&m_listener);

  std::lock_guard locker{m_lock};
  ceph_assert(m_in_flight_advance_sets == 0);
  ceph_assert(m_in_flight_object_closes == 0);
}

void JournalRecorder::shut_down(Context *on_safe) {
  on_safe = new LambdaContext(
    [this, on_safe](int r) {
      Context *ctx = nullptr;
      {
	std::lock_guard locker{m_lock};
        if (m_in_flight_advance_sets != 0) {
          ceph_assert(m_on_object_set_advanced == nullptr);
          m_on_object_set_advanced = new LambdaContext(
            [on_safe, r](int) {
              on_safe->complete(r);
            });
        } else {
          ctx = on_safe;
        }
      }
      if (ctx != nullptr) {
        ctx->complete(r);
      }
    });
  flush(on_safe);
}

void JournalRecorder::set_append_batch_options(int flush_interval,
                                               uint64_t flush_bytes,
                                               double flush_age) {
  ldout(m_cct, 5) << "flush_interval=" << flush_interval << ", "
                  << "flush_bytes=" << flush_bytes << ", "
                  << "flush_age=" << flush_age << dendl;

  std::lock_guard locker{m_lock};
  m_flush_interval = flush_interval;
  m_flush_bytes = flush_bytes;
  m_flush_age = flush_age;

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  for (uint8_t splay_offset = 0; splay_offset < splay_width; ++splay_offset) {
    std::lock_guard object_locker{m_object_locks[splay_offset]};
    auto object_recorder = get_object(splay_offset);
    object_recorder->set_append_batch_options(flush_interval, flush_bytes,
                                              flush_age);
  }
}

Future JournalRecorder::append(uint64_t tag_tid,
                               const bufferlist &payload_bl) {
  ldout(m_cct, 20) << "tag_tid=" << tag_tid << dendl;

  m_lock.lock();

  uint64_t entry_tid = m_journal_metadata->allocate_entry_tid(tag_tid);
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = entry_tid % splay_width;

  auto object_ptr = get_object(splay_offset);
  uint64_t commit_tid = m_journal_metadata->allocate_commit_tid(
    object_ptr->get_object_number(), tag_tid, entry_tid);
  auto future = ceph::make_ref<FutureImpl>(tag_tid, entry_tid, commit_tid);
  future->init(m_prev_future);
  m_prev_future = future;

  m_object_locks[splay_offset].lock();
  m_lock.unlock();

  bufferlist entry_bl;
  encode(Entry(future->get_tag_tid(), future->get_entry_tid(), payload_bl),
	 entry_bl);
  ceph_assert(entry_bl.length() <= m_journal_metadata->get_object_size());

  bool object_full = object_ptr->append({{future, entry_bl}});
  m_object_locks[splay_offset].unlock();

  if (object_full) {
    ldout(m_cct, 10) << "object " << object_ptr->get_oid() << " now full"
                     << dendl;
    std::lock_guard l{m_lock};
    close_and_advance_object_set(object_ptr->get_object_number() / splay_width);
  }
  return Future(future);
}

void JournalRecorder::flush(Context *on_safe) {
  ldout(m_cct, 20) << dendl;

  C_Flush *ctx;
  {
    std::lock_guard locker{m_lock};

    ctx = new C_Flush(m_journal_metadata, on_safe, m_object_ptrs.size() + 1);
    for (const auto& p : m_object_ptrs) {
      p.second->flush(ctx);
    }

  }

  // avoid holding the lock in case there is nothing to flush
  ctx->complete(0);
}

ceph::ref_t<ObjectRecorder> JournalRecorder::get_object(uint8_t splay_offset) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  const auto& object_recorder = m_object_ptrs.at(splay_offset);
  ceph_assert(object_recorder);
  return object_recorder;
}

void JournalRecorder::close_and_advance_object_set(uint64_t object_set) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  // entry overflow from open object
  if (m_current_set != object_set) {
    ldout(m_cct, 20) << "close already in-progress" << dendl;
    return;
  }

  // we shouldn't overflow upon append if already closed and we
  // shouldn't receive an overflowed callback if already closed
  ceph_assert(m_in_flight_advance_sets == 0);
  ceph_assert(m_in_flight_object_closes == 0);

  uint64_t active_set = m_journal_metadata->get_active_set();
  ceph_assert(m_current_set == active_set);
  ++m_current_set;
  ++m_in_flight_advance_sets;

  ldout(m_cct, 10) << "closing active object set " << object_set << dendl;
  if (close_object_set(m_current_set)) {
    advance_object_set();
  }
}

void JournalRecorder::advance_object_set() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  ceph_assert(m_in_flight_object_closes == 0);
  ldout(m_cct, 10) << "advance to object set " << m_current_set << dendl;
  m_journal_metadata->set_active_set(m_current_set, new C_AdvanceObjectSet(
    this));
}

void JournalRecorder::handle_advance_object_set(int r) {
  Context *on_object_set_advanced = nullptr;
  {
    std::lock_guard locker{m_lock};
    ldout(m_cct, 20) << __func__ << ": r=" << r << dendl;

    ceph_assert(m_in_flight_advance_sets > 0);
    --m_in_flight_advance_sets;

    if (r < 0 && r != -ESTALE) {
      lderr(m_cct) << "failed to advance object set: " << cpp_strerror(r)
                   << dendl;
    }

    if (m_in_flight_advance_sets == 0 && m_in_flight_object_closes == 0) {
      open_object_set();
      std::swap(on_object_set_advanced, m_on_object_set_advanced);
    }
  }
  if (on_object_set_advanced != nullptr) {
    on_object_set_advanced->complete(0);
  }
}

void JournalRecorder::open_object_set() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  ldout(m_cct, 10) << "opening object set " << m_current_set << dendl;

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  bool overflowed = false;

  auto lockers{lock_object_recorders()};
  for (const auto& p : m_object_ptrs) {
    const auto& object_recorder = p.second;
    uint64_t object_number = object_recorder->get_object_number();
    if (object_number / splay_width != m_current_set) {
      ceph_assert(object_recorder->is_closed());

      // ready to close object and open object in active set
      if (create_next_object_recorder(object_recorder)) {
        overflowed = true;
      }
    }
  }
  lockers.clear();

  if (overflowed) {
    ldout(m_cct, 10) << "object set " << m_current_set << " now full" << dendl;
    ldout(m_cct, 10) << "" << dendl;
    close_and_advance_object_set(m_current_set);
  }
}

bool JournalRecorder::close_object_set(uint64_t active_set) {
  ldout(m_cct, 10) << "active_set=" << active_set << dendl;
  ceph_assert(ceph_mutex_is_locked(m_lock));

  // object recorders will invoke overflow handler as they complete
  // closing the object to ensure correct order of future appends
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  auto lockers{lock_object_recorders()};
  for (const auto& p : m_object_ptrs) {
    const auto& object_recorder = p.second;
    if (object_recorder->get_object_number() / splay_width != active_set) {
      ldout(m_cct, 10) << "closing object " << object_recorder->get_oid()
                       << dendl;
      // flush out all queued appends and hold future appends
      if (!object_recorder->close()) {
        ++m_in_flight_object_closes;
        ldout(m_cct, 10) << "object " << object_recorder->get_oid() << " "
                         << "close in-progress" << dendl;
      } else {
        ldout(m_cct, 10) << "object " << object_recorder->get_oid() << " closed"
                         << dendl;
      }
    }
  }
  return (m_in_flight_object_closes == 0);
}

ceph::ref_t<ObjectRecorder> JournalRecorder::create_object_recorder(
    uint64_t object_number, ceph::mutex* lock) {
  ldout(m_cct, 10) << "object_number=" << object_number << dendl;
  auto object_recorder = ceph::make_ref<ObjectRecorder>(
    m_ioctx, utils::get_object_name(m_object_oid_prefix, object_number),
    object_number, lock, m_journal_metadata->get_work_queue(),
    &m_object_handler, m_journal_metadata->get_order(),
    m_max_in_flight_appends);
  object_recorder->set_append_batch_options(m_flush_interval, m_flush_bytes,
                                            m_flush_age);
  return object_recorder;
}

bool JournalRecorder::create_next_object_recorder(
    ceph::ref_t<ObjectRecorder> object_recorder) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  uint64_t object_number = object_recorder->get_object_number();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;
  ldout(m_cct, 10) << "object_number=" << object_number << dendl;

  ceph_assert(ceph_mutex_is_locked(m_object_locks[splay_offset]));

  auto new_object_recorder = create_object_recorder(
     (m_current_set * splay_width) + splay_offset, &m_object_locks[splay_offset]);

  ldout(m_cct, 10) << "old oid=" << object_recorder->get_oid() << ", "
                   << "new oid=" << new_object_recorder->get_oid() << dendl;
  AppendBuffers append_buffers;
  object_recorder->claim_append_buffers(&append_buffers);

  // update the commit record to point to the correct object number
  for (auto &append_buffer : append_buffers) {
    m_journal_metadata->overflow_commit_tid(
      append_buffer.first->get_commit_tid(),
      new_object_recorder->get_object_number());
  }

  bool object_full = new_object_recorder->append(std::move(append_buffers));
  if (object_full) {
    ldout(m_cct, 10) << "object " << new_object_recorder->get_oid() << " "
                     << "now full" << dendl;
  }

  m_object_ptrs[splay_offset] = std::move(new_object_recorder);
  return object_full;
}

void JournalRecorder::handle_update() {
  std::lock_guard locker{m_lock};

  uint64_t active_set = m_journal_metadata->get_active_set();
  if (m_current_set < active_set) {
    // peer journal client advanced the active set
    ldout(m_cct, 10) << "current_set=" << m_current_set << ", "
                     << "active_set=" << active_set << dendl;

    uint64_t current_set = m_current_set;
    m_current_set = active_set;
    if (m_in_flight_advance_sets == 0 && m_in_flight_object_closes == 0) {
      ldout(m_cct, 10) << "closing current object set " << current_set << dendl;
      if (close_object_set(active_set)) {
        open_object_set();
      }
    }
  }
}

void JournalRecorder::handle_closed(ObjectRecorder *object_recorder) {
  ldout(m_cct, 10) << object_recorder->get_oid() << dendl;

  std::lock_guard locker{m_lock};

  uint64_t object_number = object_recorder->get_object_number();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;
  auto& active_object_recorder = m_object_ptrs.at(splay_offset);
  ceph_assert(active_object_recorder->get_object_number() == object_number);

  ceph_assert(m_in_flight_object_closes > 0);
  --m_in_flight_object_closes;

  // object closed after advance active set committed
  ldout(m_cct, 10) << "object " << active_object_recorder->get_oid()
                   << " closed" << dendl;
  if (m_in_flight_object_closes == 0) {
    if (m_in_flight_advance_sets == 0) {
      // peer forced closing of object set
      open_object_set();
    } else {
      // local overflow advanced object set
      advance_object_set();
    }
  }
}

void JournalRecorder::handle_overflow(ObjectRecorder *object_recorder) {
  ldout(m_cct, 10) << object_recorder->get_oid() << dendl;

  std::lock_guard locker{m_lock};

  uint64_t object_number = object_recorder->get_object_number();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;
  auto& active_object_recorder = m_object_ptrs.at(splay_offset);
  ceph_assert(active_object_recorder->get_object_number() == object_number);

  ldout(m_cct, 10) << "object " << active_object_recorder->get_oid()
                   << " overflowed" << dendl;
  close_and_advance_object_set(object_number / splay_width);
}

JournalRecorder::Lockers JournalRecorder::lock_object_recorders() {
  Lockers lockers;
  lockers.reserve(m_object_ptrs.size());
  for (auto& lock : m_object_locks) {
    lockers.emplace_back(lock);
  }
  return lockers;
}

} // namespace journal
