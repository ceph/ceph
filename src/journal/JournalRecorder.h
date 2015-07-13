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

  Future append(const std::string &tag, const bufferlist &bl);
  void flush(Context *on_safe);

  ObjectRecorderPtr get_object(uint8_t splay_offset);

private:
  typedef std::map<uint8_t, ObjectRecorderPtr> ObjectRecorderPtrs;

  struct Listener : public JournalMetadata::Listener {
    JournalRecorder *journal_recorder;

    Listener(JournalRecorder *_journal_recorder)
      : journal_recorder(_journal_recorder) {}

    virtual void handle_update(JournalMetadata *) {
      journal_recorder->handle_update();
    }
  };

  struct OverflowHandler : public ObjectRecorder::OverflowHandler {
    JournalRecorder *journal_recorder;

    OverflowHandler(JournalRecorder *_journal_recorder)
      : journal_recorder(_journal_recorder) {}

    virtual void overflow(ObjectRecorder *object_recorder) {
      journal_recorder->handle_overflow(object_recorder);
    }
  };

  struct C_Flush : public Context {
    Context *on_finish;
    atomic_t pending_flushes;
    int ret_val;

    C_Flush(Context *_on_finish, size_t _pending_flushes)
      : on_finish(_on_finish), pending_flushes(_pending_flushes), ret_val(0) {
    }

    virtual void complete(int r) {
      if (r < 0 && ret_val == 0) {
        ret_val = r;
      }
      if (pending_flushes.dec() == 0) {
        on_finish->complete(ret_val);
        delete this;
      }
    }
    virtual void finish(int r) {
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
  OverflowHandler m_overflow_handler;

  Mutex m_lock;

  uint64_t m_current_set;
  ObjectRecorderPtrs m_object_ptrs;

  FutureImplPtr m_prev_future;

  void close_object_set(uint64_t object_set);
  ObjectRecorderPtr create_object_recorder(uint64_t object_number);
  void create_next_object_recorder(ObjectRecorderPtr object_recorder);

  void handle_update();
  void handle_overflow(ObjectRecorder *object_recorder);
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_RECORDER_H
