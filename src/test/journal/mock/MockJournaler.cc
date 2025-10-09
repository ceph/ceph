// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "MockJournaler.h"

namespace journal {

MockFuture *MockFuture::s_instance = nullptr;
MockReplayEntry *MockReplayEntry::s_instance = nullptr;
MockJournaler *MockJournaler::s_instance = nullptr;

std::ostream &operator<<(std::ostream &os, const MockJournalerProxy &) {
  return os;
}

} // namespace journal
