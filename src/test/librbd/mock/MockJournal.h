// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_JOURNAL_H
#define CEPH_TEST_LIBRBD_MOCK_JOURNAL_H

#include "gmock/gmock.h"
#include "librbd/Journal.h"
#include "librbd/journal/Types.h"

namespace librbd {

struct MockJournal {
  MOCK_CONST_METHOD0(is_journal_ready, bool());
  MOCK_CONST_METHOD0(is_journal_replaying, bool());

  MOCK_METHOD1(wait_for_journal_ready, void(Context *));

  MOCK_CONST_METHOD0(is_tag_owner, bool());
  MOCK_METHOD2(allocate_tag, void(const std::string &, Context *));

  MOCK_METHOD1(open, void(Context *));
  MOCK_METHOD1(close, void(Context *));

  MOCK_METHOD0(allocate_op_tid, uint64_t());

  MOCK_METHOD3(append_op_event_mock, void(uint64_t, const journal::EventEntry&,
                                          Context *));
  void append_op_event(uint64_t op_tid, journal::EventEntry &&event_entry,
                       Context *on_safe) {
    // googlemock doesn't support move semantics
    append_op_event_mock(op_tid, event_entry, on_safe);
  }

  MOCK_METHOD2(commit_op_event, void(uint64_t, int));
  MOCK_METHOD2(replay_op_ready, void(uint64_t, Context *));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_JOURNAL_H
