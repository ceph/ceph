// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNAL_TRIMMER_H
#define CEPH_JOURNAL_JOURNAL_TRIMMER_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "common/Mutex.h"
#include "journal/AsyncOpTracker.h"
#include "journal/JournalMetadata.h"
#include "cls/journal/cls_journal_types.h"

namespace journal {

class JournalTrimmer {
public:
  typedef cls::journal::ObjectSetPosition ObjectSetPosition;

  JournalTrimmer(librados::IoCtx &ioctx, const std::string &object_oid_prefix,
                 const JournalMetadataPtr &journal_metadata);
  ~JournalTrimmer();

  int remove_objects(bool force);
  void committed(uint64_t commit_tid);

private:
  struct C_CommitPositionSafe : public Context {
    JournalTrimmer *journal_trimmer;
    ObjectSetPosition object_set_position;

    C_CommitPositionSafe(JournalTrimmer *_journal_trimmer,
                         const ObjectSetPosition &_object_set_position)
      : journal_trimmer(_journal_trimmer),
        object_set_position(_object_set_position) {}

    virtual void finish(int r) {
      journal_trimmer->handle_commit_position_safe(r, object_set_position);
      journal_trimmer->m_async_op_tracker.finish_op();
    }
  };
  struct C_RemoveSet : public Context {
    JournalTrimmer *journal_trimmer;
    uint64_t object_set;
    Mutex lock;
    uint32_t refs;
    int return_value;

    C_RemoveSet(JournalTrimmer *_journal_trimmer, uint64_t _object_set,
                uint8_t _splay_width);
    virtual void complete(int r);
    virtual void finish(int r) {
      journal_trimmer->handle_set_removed(r, object_set);
      journal_trimmer->m_async_op_tracker.finish_op();
    }
  };

  librados::IoCtx m_ioctx;
  CephContext *m_cct;
  std::string m_object_oid_prefix;

  JournalMetadataPtr m_journal_metadata;

  AsyncOpTracker m_async_op_tracker;

  Mutex m_lock;

  bool m_remove_set_pending;
  uint64_t m_remove_set;
  Context *m_remove_set_ctx;

  void trim_objects(uint64_t minimum_set);
  void remove_set(uint64_t object_set);

  void handle_commit_position_safe(int r, const ObjectSetPosition &position);

  void handle_set_removed(int r, uint64_t object_set);
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_TRIMMER_H
