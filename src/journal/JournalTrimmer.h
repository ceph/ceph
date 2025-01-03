// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_JOURNAL_TRIMMER_H
#define CEPH_JOURNAL_JOURNAL_TRIMMER_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "common/AsyncOpTracker.h"
#include "journal/JournalMetadata.h"
#include "cls/journal/cls_journal_types.h"
#include <functional>

struct Context;

namespace journal {

class JournalTrimmer {
public:
  typedef cls::journal::ObjectSetPosition ObjectSetPosition;

  JournalTrimmer(librados::IoCtx &ioctx, const std::string &object_oid_prefix,
                 const ceph::ref_t<JournalMetadata> &journal_metadata);
  ~JournalTrimmer();

  void shut_down(Context *on_finish);

  void remove_objects(bool force, Context *on_finish);
  void committed(uint64_t commit_tid);

private:
  typedef std::function<Context*()> CreateContext;

  struct MetadataListener : public JournalMetadataListener {
    JournalTrimmer *journal_trimmer;

    MetadataListener(JournalTrimmer *journal_trimmer)
      : journal_trimmer(journal_trimmer) {
    }
    void handle_update(JournalMetadata *) override {
      journal_trimmer->handle_metadata_updated();
    }
  };

  struct C_CommitPositionSafe : public Context {
    JournalTrimmer *journal_trimmer;

    C_CommitPositionSafe(JournalTrimmer *_journal_trimmer)
      : journal_trimmer(_journal_trimmer) {
      journal_trimmer->m_async_op_tracker.start_op();
    }
    ~C_CommitPositionSafe() override {
      journal_trimmer->m_async_op_tracker.finish_op();
    }

    void finish(int r) override {
    }
  };

  struct C_RemoveSet;

  librados::IoCtx m_ioctx;
  CephContext *m_cct;
  std::string m_object_oid_prefix;

  ceph::ref_t<JournalMetadata> m_journal_metadata;
  MetadataListener m_metadata_listener;

  AsyncOpTracker m_async_op_tracker;

  ceph::mutex m_lock = ceph::make_mutex("JournalTrimmer::m_lock");

  bool m_remove_set_pending;
  uint64_t m_remove_set;
  Context *m_remove_set_ctx;

  bool m_shutdown = false;

  CreateContext m_create_commit_position_safe_context = [this]() {
      return new C_CommitPositionSafe(this);
    };

  void trim_objects(uint64_t minimum_set);
  void remove_set(uint64_t object_set);

  void handle_metadata_updated();
  void handle_set_removed(int r, uint64_t object_set);
};

} // namespace journal

#endif // CEPH_JOURNAL_JOURNAL_TRIMMER_H
