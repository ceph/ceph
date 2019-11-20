// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNAL_RECORDER_H
#define CEPH_JOURNAL_JOURNAL_RECORDER_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "common/containers.h"
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
  JournalRecorder(librados::IoCtx &ioctx, std::string_view object_oid_prefix,
                  ceph::ref_t<JournalMetadata> journal_metadata,
                  uint64_t max_in_flight_appends);
  ~JournalRecorder();

  void shut_down(Context *on_safe);

  void set_append_batch_options(int flush_interval, uint64_t flush_bytes,
                                double flush_age);

  Future append(uint64_t tag_tid, const bufferlist &bl);
  void flush(Context *on_safe);

  ceph::ref_t<ObjectRecorder> get_object(uint8_t splay_offset);

private:
  typedef std::map<uint8_t, ceph::ref_t<ObjectRecorder>> ObjectRecorderPtrs;
  typedef std::vector<std::unique_lock<ceph::mutex>> Lockers;

  struct Listener : public JournalMetadataListener {
    JournalRecorder *journal_recorder;

    Listener(JournalRecorder *_journal_recorder)
      : journal_recorder(_journal_recorder) {}

    void handle_update(JournalMetadata *) override {
      journal_recorder->handle_update();
    }
  };

  struct ObjectHandler : public ObjectRecorder::Handler {
    JournalRecorder *journal_recorder;

    ObjectHandler(JournalRecorder *_journal_recorder)
      : journal_recorder(_journal_recorder) {
    }

    void closed(ObjectRecorder *object_recorder) override {
      journal_recorder->handle_closed(object_recorder);
    }
    void overflow(ObjectRecorder *object_recorder) override {
      journal_recorder->handle_overflow(object_recorder);
    }
  };

  struct C_AdvanceObjectSet : public Context {
    JournalRecorder *journal_recorder;

    C_AdvanceObjectSet(JournalRecorder *_journal_recorder)
      : journal_recorder(_journal_recorder) {
    }
    void finish(int r) override {
      journal_recorder->handle_advance_object_set(r);
    }
  };

  librados::IoCtx m_ioctx;
  CephContext *m_cct = nullptr;
  std::string m_object_oid_prefix;

  ceph::ref_t<JournalMetadata> m_journal_metadata;

  uint32_t m_flush_interval = 0;
  uint64_t m_flush_bytes = 0;
  double m_flush_age = 0;
  uint64_t m_max_in_flight_appends;

  Listener m_listener;
  ObjectHandler m_object_handler;

  ceph::mutex m_lock = ceph::make_mutex("JournalerRecorder::m_lock");

  uint32_t m_in_flight_advance_sets = 0;
  uint32_t m_in_flight_object_closes = 0;
  uint64_t m_current_set;
  ObjectRecorderPtrs m_object_ptrs;
  ceph::containers::tiny_vector<ceph::mutex> m_object_locks;

  ceph::ref_t<FutureImpl> m_prev_future;

  Context *m_on_object_set_advanced = nullptr;

  void open_object_set();
  bool close_object_set(uint64_t active_set);

  void advance_object_set();
  void handle_advance_object_set(int r);

  void close_and_advance_object_set(uint64_t object_set);

  ceph::ref_t<ObjectRecorder> create_object_recorder(uint64_t object_number,
                                           ceph::mutex* lock);
  bool create_next_object_recorder(ceph::ref_t<ObjectRecorder> object_recorder);

  void handle_update();

  void handle_closed(ObjectRecorder *object_recorder);
  void handle_overflow(ObjectRecorder *object_recorder);

  Lockers lock_object_recorders();
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_RECORDER_H
