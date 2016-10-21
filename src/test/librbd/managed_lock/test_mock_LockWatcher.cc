// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/managed_lock/test_mock_LockWatcher.h"

namespace librbd {
namespace managed_lock {

MockLockWatcher *MockLockWatcher::s_instance = nullptr;
const std::string MockLockWatcher::WATCHER_LOCK_TAG("internal");

bool MockLockWatcher::decode_lock_cookie(const std::string &tag, uint64_t *handle) {
  std::string prefix;
  std::istringstream ss(tag);
  if (!(ss >> prefix >> *handle) || prefix != "auto") {
    return false;
  }
  return true;
}

} // namespace managed_lock
} // namespace librbd
