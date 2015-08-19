// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_JOURNAL_H
#define CEPH_TEST_LIBRBD_MOCK_JOURNAL_H

#include "gmock/gmock.h"
#include "librbd/Journal.h"

namespace librbd {

struct MockJournal {
  MOCK_CONST_METHOD0(is_journal_ready, bool());
  MOCK_CONST_METHOD0(is_journal_replaying, bool());

  MOCK_METHOD1(wait_for_journal_ready, void(Context *));

  MOCK_METHOD1(append_op_event, uint64_t(journal::EventEntry&));
  MOCK_METHOD2(commit_op_event, void(uint64_t, int));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_JOURNAL_H
