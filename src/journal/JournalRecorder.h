// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNAL_RECORDER_H
#define CEPH_JOURNAL_JOURNAL_RECORDER_H

#include "include/int_types.h"
#include "include/atomic.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "journal/Future.h"
#include "journal/FutureImpl.h"
#include "journal/JournalMetadata.h"
#include "journal/ObjectRecorder.h"
#include <map>
#include <string>

class SafeTimer;

namespace journal {

class JournalRecorder {
public:
  JournalRecorder(librados::IoCtx &ioctx, const std::string &object_oid_prefix,
                  const JournalMetadataPtr &journal_metadata,
                  uint32_t flush_interval, uint64_t flush_bytes,
                  double flush_age);
  ~JournalRecorder();

  Future append(uint64_t tag_tid, const bufferlist &bl);
  void flush(Context *on_safe);

  ObjectRecorderPtr get_object(uint8_t splay_offset);

private:
  typedef std::map<uint8_t, ObjectRecorderPtr> ObjectRecorderPtrs;

  struct Listener : public JournalMetadataListener {
    JournalRecorder *journal_recorder;

    Listener(JournalRecorder *_journal_recorder)
      : journal_recorder(_journal_recorder) {}

    virtual void handle_update(JournalMetadata *) {
      journal_recorder->handle_update();
    }
  };

  struct ObjectHandler : public ObjectRecorder::Handler {
    JournalRecorder *journal_recorder;

    ObjectHandler(JournalRecorder *_journal_recorder)
      : journal_recorder(_journal_recorder) {
    }

    virtual void closed(ObjectRecorder *object_recorder) {
      journal_recorder->handle_closed(object_recorder);
    }
    virtual void overflow(ObjectRecorder *object_recorder) {
      journal_recorder->handle_overflow(object_recorder);
    }
  };

  struct C_AdvanceObjectSet : public Context {
    JournalRecorder *journal_recorder;

    C_AdvanceObjectSet(JournalRecorder *_journal_recorder)
      : journal_recorder(_journal_recorder) {
    }
    virtual void finish(int r) {
      journal_recorder->handle_advance_object_set(r);
    }
  };

  librados::IoCtx m_ioctx;
  CephContext *m_cct;
  std::string m_object_oid_prefix;

  JournalMetadataPtr m_journal_metadata;

  uint32_t m_flush_interval;
  uint64_t m_flush_bytes;
  double m_flush_age;

  Listener m_listener;
  ObjectHandler m_object_handler;

  Mutex m_lock;

  uint32_t m_in_flight_advance_sets = 0;
  uint32_t m_in_flight_object_closes = 0;
  uint64_t m_current_set;
  ObjectRecorderPtrs m_object_ptrs;

  FutureImplPtr m_prev_future;

  void open_object_set();
  bool close_object_set(uint64_t active_set);

  void advance_object_set();
  void handle_advance_object_set(int r);

  void close_and_advance_object_set(uint64_t object_set);

  ObjectRecorderPtr create_object_recorder(uint64_t object_number);
  void create_next_object_recorder(ObjectRecorderPtr object_recorder);

  void handle_update();

  void handle_closed(ObjectRecorder *object_recorder);
  void handle_overflow(ObjectRecorder *object_recorder);
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_RECORDER_H
