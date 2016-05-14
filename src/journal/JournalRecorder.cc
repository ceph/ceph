// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalRecorder.h"
#include "common/errno.h"
#include "journal/Entry.h"
#include "journal/Utils.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalRecorder: "

namespace journal {

namespace {

struct C_Flush : public Context {
  JournalMetadataPtr journal_metadata;
  Context *on_finish;
  atomic_t pending_flushes;
  int ret_val;

  C_Flush(JournalMetadataPtr _journal_metadata, Context *_on_finish,
          size_t _pending_flushes)
    : journal_metadata(_journal_metadata), on_finish(_on_finish),
      pending_flushes(_pending_flushes), ret_val(0) {
  }

  virtual void complete(int r) {
    if (r < 0 && ret_val == 0) {
      ret_val = r;
    }
    if (pending_flushes.dec() == 0) {
      // ensure all prior callback have been flushed as well
      journal_metadata->queue(on_finish, ret_val);
      delete this;
    }
  }
  virtual void finish(int r) {
  }
};

} // anonymous namespace

JournalRecorder::JournalRecorder(librados::IoCtx &ioctx,
                                 const std::string &object_oid_prefix,
                                 const JournalMetadataPtr& journal_metadata,
                                 uint32_t flush_interval, uint64_t flush_bytes,
                                 double flush_age)
  : m_cct(NULL), m_object_oid_prefix(object_oid_prefix),
    m_journal_metadata(journal_metadata), m_flush_interval(flush_interval),
    m_flush_bytes(flush_bytes), m_flush_age(flush_age), m_listener(this),
    m_object_handler(this), m_lock("JournalerRecorder::m_lock"),
    m_current_set(m_journal_metadata->get_active_set()) {

  Mutex::Locker locker(m_lock);
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  for (uint8_t splay_offset = 0; splay_offset < splay_width; ++splay_offset) {
    uint64_t object_number = splay_offset + (m_current_set * splay_width);
    m_object_ptrs[splay_offset] = create_object_recorder(object_number);
  }

  m_journal_metadata->add_listener(&m_listener);
}

JournalRecorder::~JournalRecorder() {
  m_journal_metadata->remove_listener(&m_listener);

  Mutex::Locker locker(m_lock);
  assert(m_in_flight_advance_sets == 0);
  assert(m_in_flight_object_closes == 0);
}

Future JournalRecorder::append(uint64_t tag_tid,
                               const bufferlist &payload_bl) {

  Mutex::Locker locker(m_lock);

  uint64_t entry_tid = m_journal_metadata->allocate_entry_tid(tag_tid);
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = entry_tid % splay_width;

  ObjectRecorderPtr object_ptr = get_object(splay_offset);
  uint64_t commit_tid = m_journal_metadata->allocate_commit_tid(
    object_ptr->get_object_number(), tag_tid, entry_tid);
  FutureImplPtr future(new FutureImpl(tag_tid, entry_tid, commit_tid));
  future->init(m_prev_future);
  m_prev_future = future;

  bufferlist entry_bl;
  ::encode(Entry(future->get_tag_tid(), future->get_entry_tid(), payload_bl),
           entry_bl);
  assert(entry_bl.length() <= m_journal_metadata->get_object_size());

  AppendBuffers append_buffers;
  append_buffers.push_back(std::make_pair(future, entry_bl));
  bool object_full = object_ptr->append(append_buffers);

  if (object_full) {
    ldout(m_cct, 10) << "object " << object_ptr->get_oid() << " now full"
                     << dendl;
    close_and_advance_object_set(object_ptr->get_object_number() / splay_width);
  }
  return Future(future);
}

void JournalRecorder::flush(Context *on_safe) {
  C_Flush *ctx;
  {
    Mutex::Locker locker(m_lock);

    ctx = new C_Flush(m_journal_metadata, on_safe, m_object_ptrs.size() + 1);
    for (ObjectRecorderPtrs::iterator it = m_object_ptrs.begin();
         it != m_object_ptrs.end(); ++it) {
      it->second->flush(ctx);
    }
  }

  // avoid holding the lock in case there is nothing to flush
  ctx->complete(0);
}

ObjectRecorderPtr JournalRecorder::get_object(uint8_t splay_offset) {
  assert(m_lock.is_locked());

  ObjectRecorderPtr object_recoder = m_object_ptrs[splay_offset];
  assert(object_recoder != NULL);
  return object_recoder;
}

void JournalRecorder::close_and_advance_object_set(uint64_t object_set) {
  assert(m_lock.is_locked());

  // entry overflow from open object
  if (m_current_set != object_set) {
    ldout(m_cct, 20) << __func__ << ": close already in-progress" << dendl;
    return;
  }

  // we shouldn't overflow upon append if already closed and we
  // shouldn't receive an overflowed callback if already closed
  assert(m_in_flight_advance_sets == 0);
  assert(m_in_flight_object_closes == 0);

  uint64_t active_set = m_journal_metadata->get_active_set();
  assert(m_current_set == active_set);
  ++m_current_set;
  ++m_in_flight_advance_sets;

  ldout(m_cct, 20) << __func__ << ": closing active object set "
                   << object_set << dendl;
  if (close_object_set(m_current_set)) {
    advance_object_set();
  }
}

void JournalRecorder::advance_object_set() {
  assert(m_lock.is_locked());

  assert(m_in_flight_object_closes == 0);
  ldout(m_cct, 20) << __func__ << ": advance to object set " << m_current_set
                   << dendl;
  m_journal_metadata->set_active_set(m_current_set, new C_AdvanceObjectSet(
    this));
}

void JournalRecorder::handle_advance_object_set(int r) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << __func__ << ": r=" << r << dendl;

  assert(m_in_flight_advance_sets > 0);
  --m_in_flight_advance_sets;

  if (r < 0 && r != -ESTALE) {
    lderr(m_cct) << __func__ << ": failed to advance object set: "
                 << cpp_strerror(r) << dendl;
  }

  if (m_in_flight_advance_sets == 0 && m_in_flight_object_closes == 0) {
    open_object_set();
  }
}

void JournalRecorder::open_object_set() {
  assert(m_lock.is_locked());

  ldout(m_cct, 10) << __func__ << ": opening object set " << m_current_set
                   << dendl;

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  for (ObjectRecorderPtrs::iterator it = m_object_ptrs.begin();
       it != m_object_ptrs.end(); ++it) {
    ObjectRecorderPtr object_recorder = it->second;
    if (object_recorder->get_object_number() / splay_width != m_current_set) {
      assert(object_recorder->is_closed());

      // ready to close object and open object in active set
      create_next_object_recorder(object_recorder);
    }
  }
}

bool JournalRecorder::close_object_set(uint64_t active_set) {
  assert(m_lock.is_locked());

  // object recorders will invoke overflow handler as they complete
  // closing the object to ensure correct order of future appends
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  for (ObjectRecorderPtrs::iterator it = m_object_ptrs.begin();
       it != m_object_ptrs.end(); ++it) {
    ObjectRecorderPtr object_recorder = it->second;
    if (object_recorder->get_object_number() / splay_width != active_set) {
      ldout(m_cct, 10) << __func__ << ": closing object "
                       << object_recorder->get_oid() << dendl;
      // flush out all queued appends and hold future appends
      if (!object_recorder->close()) {
        ++m_in_flight_object_closes;
      } else {
        ldout(m_cct, 20) << __func__ << ": object "
                         << object_recorder->get_oid() << " closed" << dendl;
      }
    }
  }
  return (m_in_flight_object_closes == 0);
}

ObjectRecorderPtr JournalRecorder::create_object_recorder(
    uint64_t object_number) {
  ObjectRecorderPtr object_recorder(new ObjectRecorder(
    m_ioctx, utils::get_object_name(m_object_oid_prefix, object_number),
    object_number, m_journal_metadata->get_timer(),
    m_journal_metadata->get_timer_lock(), &m_object_handler,
    m_journal_metadata->get_order(), m_flush_interval, m_flush_bytes,
    m_flush_age));
  return object_recorder;
}

void JournalRecorder::create_next_object_recorder(
    ObjectRecorderPtr object_recorder) {
  assert(m_lock.is_locked());

  uint64_t object_number = object_recorder->get_object_number();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;

  ObjectRecorderPtr new_object_recorder = create_object_recorder(
     (m_current_set * splay_width) + splay_offset);

  ldout(m_cct, 10) << __func__ << ": "
                   << "old oid=" << object_recorder->get_oid() << ", "
                   << "new oid=" << new_object_recorder->get_oid() << dendl;
  AppendBuffers append_buffers;
  object_recorder->claim_append_buffers(&append_buffers);
  new_object_recorder->append(append_buffers);

  m_object_ptrs[splay_offset] = new_object_recorder;
}

void JournalRecorder::handle_update() {
  Mutex::Locker locker(m_lock);

  uint64_t active_set = m_journal_metadata->get_active_set();
  if (m_current_set < active_set) {
    // peer journal client advanced the active set
    ldout(m_cct, 20) << __func__ << ": "
                     << "current_set=" << m_current_set << ", "
                     << "active_set=" << active_set << dendl;

    uint64_t current_set = m_current_set;
    m_current_set = active_set;
    if (m_in_flight_advance_sets == 0 && m_in_flight_object_closes == 0) {
      ldout(m_cct, 20) << __func__ << ": closing current object set "
                       << current_set << dendl;
      if (close_object_set(active_set)) {
        open_object_set();
      }
    }
  }
}

void JournalRecorder::handle_closed(ObjectRecorder *object_recorder) {
  ldout(m_cct, 10) << __func__ << ": " << object_recorder->get_oid() << dendl;

  Mutex::Locker locker(m_lock);

  uint64_t object_number = object_recorder->get_object_number();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;
  ObjectRecorderPtr active_object_recorder = m_object_ptrs[splay_offset];
  assert(active_object_recorder->get_object_number() == object_number);

  assert(m_in_flight_object_closes > 0);
  --m_in_flight_object_closes;

  // object closed after advance active set committed
  ldout(m_cct, 20) << __func__ << ": object "
                   << active_object_recorder->get_oid() << " closed" << dendl;
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
  ldout(m_cct, 10) << __func__ << ": " << object_recorder->get_oid() << dendl;

  Mutex::Locker locker(m_lock);

  uint64_t object_number = object_recorder->get_object_number();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;
  ObjectRecorderPtr active_object_recorder = m_object_ptrs[splay_offset];
  assert(active_object_recorder->get_object_number() == object_number);

  ldout(m_cct, 20) << __func__ << ": object "
                   << active_object_recorder->get_oid() << " overflowed"
                   << dendl;
  close_and_advance_object_set(object_number / splay_width);
}

} // namespace journal
