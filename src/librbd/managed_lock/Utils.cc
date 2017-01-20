// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/assert.h"
#include "librbd/managed_lock/Utils.h"
#include <sstream>

namespace librbd {
namespace managed_lock {
namespace util {

namespace {

const std::string WATCHER_LOCK_COOKIE_PREFIX = "auto";
const std::string WATCHER_LOCK_TAG("internal");

} // anonymous namespace

const std::string &get_watcher_lock_tag() {
  return WATCHER_LOCK_TAG;
}

bool decode_lock_cookie(const std::string &tag, uint64_t *handle) {
  std::string prefix;
  std::istringstream ss(tag);
  if (!(ss >> prefix >> *handle) || prefix != WATCHER_LOCK_COOKIE_PREFIX) {
    return false;
  }
  return true;
}

std::string encode_lock_cookie(uint64_t watch_handle) {
  assert(watch_handle != 0);
  std::ostringstream ss;
  ss << WATCHER_LOCK_COOKIE_PREFIX << " " << watch_handle;
  return ss.str();
}

} // namespace util
} // namespace managed_lock
} // namespace librbd


