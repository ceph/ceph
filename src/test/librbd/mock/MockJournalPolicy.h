// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_JOURNAL_POLICY_H
#define CEPH_TEST_LIBRBD_MOCK_JOURNAL_POLICY_H

#include "librbd/journal/Policy.h"
#include "gmock/gmock.h"

namespace librbd {

struct MockJournalPolicy : public journal::Policy {

  MOCK_METHOD1(allocate_tag_on_lock, void(Context*));
  MOCK_METHOD1(cancel_external_replay, void(Context*));

};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_JOURNAL_POLICY_H
