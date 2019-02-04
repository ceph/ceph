// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_JOURNAL_H
#define CEPH_TEST_LIBRBD_MOCK_JOURNAL_H

#include "gmock/gmock.h"
#include "include/rados/librados_fwd.hpp"
#include "librbd/Journal.h"
#include "librbd/journal/Types.h"
#include <list>

struct Context;
struct ContextWQ;

namespace librbd {

struct ImageCtx;

struct MockJournal {
  static MockJournal *s_instance;
  static MockJournal *get_instance() {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  template <typename ImageCtxT>
  static int is_tag_owner(ImageCtxT *image_ctx, bool *is_tag_owner) {
    return get_instance()->is_tag_owner(is_tag_owner);
  }

  static void get_tag_owner(librados::IoCtx &,
                            const std::string &global_image_id,
                            std::string *tag_owner, ContextWQ *work_queue,
                            Context *on_finish) {
    get_instance()->get_tag_owner(global_image_id, tag_owner,
                                  work_queue, on_finish);
  }

  MockJournal() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(is_journal_ready, bool());
  MOCK_CONST_METHOD0(is_journal_replaying, bool());
  MOCK_CONST_METHOD0(is_journal_appending, bool());

  MOCK_METHOD1(wait_for_journal_ready, void(Context *));

  MOCK_METHOD4(get_tag_owner, void(const std::string &,
                                   std::string *, ContextWQ *,
                                   Context *));

  MOCK_CONST_METHOD0(is_tag_owner, bool());
  MOCK_CONST_METHOD1(is_tag_owner, int(bool *));
  MOCK_METHOD3(allocate_tag, void(const std::string &mirror_uuid,
                                  const journal::TagPredecessor &predecessor,
                                  Context *on_finish));

  MOCK_METHOD1(open, void(Context *));
  MOCK_METHOD1(close, void(Context *));

  MOCK_CONST_METHOD0(get_tag_tid, uint64_t());
  MOCK_CONST_METHOD0(get_tag_data, journal::TagData());

  MOCK_METHOD0(allocate_op_tid, uint64_t());

  MOCK_METHOD3(append_op_event_mock, void(uint64_t, const journal::EventEntry&,
                                          Context *));
  void append_op_event(uint64_t op_tid, journal::EventEntry &&event_entry,
                       Context *on_safe) {
    // googlemock doesn't support move semantics
    append_op_event_mock(op_tid, event_entry, on_safe);
  }

  MOCK_METHOD2(flush_event, void(uint64_t, Context *));
  MOCK_METHOD2(wait_event, void(uint64_t, Context *));

  MOCK_METHOD3(commit_op_event, void(uint64_t, int, Context *));
  MOCK_METHOD2(replay_op_ready, void(uint64_t, Context *));

  MOCK_METHOD1(add_listener, void(journal::Listener *));
  MOCK_METHOD1(remove_listener, void(journal::Listener *));

  MOCK_METHOD1(is_resync_requested, int(bool *));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_JOURNAL_H
