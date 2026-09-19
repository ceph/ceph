// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "Types.h"

#include <iostream>

namespace cephfs {
namespace mirror {

const std::string &priority_mode_name(PriorityMode mode) {
  switch (mode) {
  case PriorityMode::PER_THREAD:
    return PRIORITY_MODE_PER_THREAD;
  case PriorityMode::THREAD_SHARED:
  default:
    return PRIORITY_MODE_THREAD_SHARED;
  }
}

bool priority_mode_from_name(std::string_view name, PriorityMode *mode) {
  if (name == PRIORITY_MODE_THREAD_SHARED) {
    *mode = PriorityMode::THREAD_SHARED;
    return true;
  }
  if (name == PRIORITY_MODE_PER_THREAD) {
    *mode = PriorityMode::PER_THREAD;
    return true;
  }
  return false;
}

std::ostream& operator<<(std::ostream& out, PriorityMode mode) {
  out << priority_mode_name(mode);
  return out;
}

std::ostream& operator<<(std::ostream& out, const Filesystem &filesystem) {
  out << "{fscid=" << filesystem.fscid << ", fs_name=" << filesystem.fs_name << "}";
  return out;
}

std::ostream& operator<<(std::ostream& out, const FilesystemSpec &spec) {
  out << "{filesystem=" << spec.filesystem << ", pool_id=" << spec.pool_id << "}";
  return out;
}

} // namespace mirror
} // namespace cephfs

