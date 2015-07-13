// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalRecorder.h"
#include "journal/Entry.h"
#include "journal/Utils.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalRecorder: "

namespace journal {

JournalRecorder::JournalRecorder(librados::IoCtx &ioctx,
                                 const std::string &object_oid_prefix,
                                 const JournalMetadataPtr& journal_metadata,
                                 uint32_t flush_interval, uint64_t flush_bytes,
                                 double flush_age)
  : m_cct(NULL), m_object_oid_prefix(object_oid_prefix),
    m_journal_metadata(journal_metadata), m_flush_interval(flush_interval),
    m_flush_bytes(flush_bytes), m_flush_age(flush_age), m_listener(this),
    m_overflow_handler(this), m_lock("JournalerRecorder::m_lock"),
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
}

Future JournalRecorder::append(const std::string &tag,
                               const bufferlist &payload_bl) {
  Mutex::Locker locker(m_lock);

  uint64_t tid = m_journal_metadata->allocate_tid(tag);
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = tid % splay_width;

  ObjectRecorderPtr object_ptr = get_object(splay_offset);
  FutureImplPtr future(new FutureImpl(tag, tid));
  future->init(m_prev_future);
  m_prev_future = future;

  bufferlist entry_bl;
  ::encode(Entry(future->get_tag(), future->get_tid(), payload_bl), entry_bl);

  AppendBuffers append_buffers;
  append_buffers.push_back(std::make_pair(future, entry_bl));
  bool object_full = object_ptr->append(append_buffers);

  // TODO populate the object_set_position

  if (object_full) {
    ldout(m_cct, 10) << "object " << object_ptr->get_oid() << " now full"
                     << dendl;
    close_object_set(object_ptr->get_object_number() / splay_width);
  }

  return Future(future);
}

void JournalRecorder::flush(Context *on_safe) {
  Mutex::Locker locker(m_lock);

  C_Flush *ctx = new C_Flush(on_safe, m_object_ptrs.size());
  for (ObjectRecorderPtrs::iterator it = m_object_ptrs.begin();
       it != m_object_ptrs.end(); ++it) {
    it->second->flush(ctx);
  }
}

ObjectRecorderPtr JournalRecorder::get_object(uint8_t splay_offset) {
  assert(m_lock.is_locked());

  ObjectRecorderPtr object_recoder = m_object_ptrs[splay_offset];
  assert(object_recoder != NULL);
  return object_recoder;
}

void JournalRecorder::close_object_set(uint64_t object_set) {
  assert(m_lock.is_locked());

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  if (object_set != m_current_set) {
    return;
  }

  uint64_t active_set = m_journal_metadata->get_active_set();
  if (active_set < m_current_set + 1) {
    m_journal_metadata->set_active_set(m_current_set + 1);
  }
  m_current_set = m_journal_metadata->get_active_set();

  ldout(m_cct, 10) << __func__ << ": advancing to object set "
                   << m_current_set << dendl;

  // object recorders will invoke overflow handler as they complete
  // closing the object to ensure correct order of future appends
  for (ObjectRecorderPtrs::iterator it = m_object_ptrs.begin();
       it != m_object_ptrs.end(); ++it) {
    ObjectRecorderPtr object_recorder = it->second;
    if (object_recorder != NULL &&
        object_recorder->get_object_number() / splay_width == m_current_set) {
      if (object_recorder->close_object()) {
        // no in-flight ops, immediately create new recorder
        create_next_object_recorder(object_recorder);
      }
    }
  }
}

ObjectRecorderPtr JournalRecorder::create_object_recorder(
    uint64_t object_number) {
  ObjectRecorderPtr object_recorder(new ObjectRecorder(
    m_ioctx, utils::get_object_name(m_object_oid_prefix, object_number),
    object_number, m_journal_metadata->get_timer(),
    m_journal_metadata->get_timer_lock(), &m_overflow_handler,
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

  AppendBuffers append_buffers;
  object_recorder->claim_append_buffers(&append_buffers);
  new_object_recorder->append(append_buffers);

  m_object_ptrs[splay_offset] = new_object_recorder;
}

void JournalRecorder::handle_update() {
  Mutex::Locker locker(m_lock);

  uint64_t active_set = m_journal_metadata->get_active_set();
  if (active_set > m_current_set) {
    close_object_set(m_current_set);
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

  close_object_set(object_number / splay_width);
  create_next_object_recorder(active_object_recorder);
}

} // namespace journal
