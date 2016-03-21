// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef TEST_RBD_MIRROR_MOCK_JOURNALER_H
#define TEST_RBD_MIRROR_MOCK_JOURNALER_H

#include <gmock/gmock.h>
#include "librbd/Journal.h"
#include "librbd/journal/TypeTraits.h"

namespace journal {

struct MockJournaler {
  static MockJournaler *s_instance;
  static MockJournaler &get_instance() {
    assert(s_instance != nullptr);
    return *s_instance;
  }

  MockJournaler() {
    s_instance = this;
  }

  MOCK_METHOD2(update_client, void(const bufferlist&, Context *on_safe));
};

} // namespace journal

namespace librbd {

struct MockImageCtx;

namespace journal {

template <>
struct TypeTraits<MockImageCtx> {
  typedef ::journal::MockJournaler Journaler;
};

} // namespace journal
} // namespace librbd

#endif // TEST_RBD_MIRROR_MOCK_JOURNALER_H
