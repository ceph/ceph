// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MockJournaler.h"

namespace journal {

MockReplayEntry *MockReplayEntry::s_instance = nullptr;
MockJournaler *MockJournaler::s_instance = nullptr;

std::ostream &operator<<(std::ostream &os, const MockJournalerProxy &) {
  return os;
}

} // namespace journal
