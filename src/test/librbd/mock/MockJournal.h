// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_JOURNAL_H
#define CEPH_TEST_LIBRBD_MOCK_JOURNAL_H

#include "gmock/gmock.h"
#include "librbd/Journal.h"
#include "librbd/journal/Types.h"
#include <list>

namespace librbd {

struct AioObjectRequestHandle;
struct ImageCtx;

struct MockJournal {
  typedef std::list<AioObjectRequestHandle *> AioObjectRequests;

  static MockJournal *s_instance;
  static MockJournal *get_instance() {
    assert(s_instance != nullptr);
    return s_instance;
  }

  template <typename ImageCtxT>
  static int is_tag_owner(ImageCtxT *image_ctx, bool *is_tag_owner) {
    return get_instance()->is_tag_owner(is_tag_owner);
  }

  MockJournal() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(is_journal_ready, bool());
  MOCK_CONST_METHOD0(is_journal_replaying, bool());
  MOCK_CONST_METHOD0(is_journal_appending, bool());

  MOCK_METHOD1(wait_for_journal_ready, void(Context *));

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

  MOCK_METHOD5(append_write_event, uint64_t(uint64_t, size_t,
                                            const bufferlist &,
                                            const AioObjectRequests &, bool));
  MOCK_METHOD5(append_io_event_mock, uint64_t(const journal::EventEntry&,
                                              const AioObjectRequests &,
                                              uint64_t, size_t, bool));
  uint64_t append_io_event(journal::EventEntry &&event_entry,
                           const AioObjectRequests &requests,
                           uint64_t offset, size_t length,
                           bool flush_entry) {
    // googlemock doesn't support move semantics
    return append_io_event_mock(event_entry, requests, offset, length,
                                flush_entry);
  }

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

  MOCK_METHOD2(add_listener, void(journal::ListenerType,
                                  journal::JournalListenerPtr));
  MOCK_METHOD2(remove_listener, void(journal::ListenerType,
                                     journal::JournalListenerPtr));

  MOCK_METHOD1(check_resync_requested, int(bool *));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_JOURNAL_H
