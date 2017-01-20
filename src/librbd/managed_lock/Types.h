// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_TYPES_H
#define CEPH_LIBRBD_MANAGED_LOCK_TYPES_H

#include "msg/msg_types.h"
#include <string>

namespace librbd {
namespace managed_lock {

struct Locker {
  entity_name_t entity;
  std::string cookie;
  std::string address;
  uint64_t handle;
};

enum Mode {
  EXCLUSIVE,
  SHARED
};


} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_MANAGED_LOCK_TYPES_H
