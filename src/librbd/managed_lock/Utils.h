// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_UTILS_H
#define CEPH_LIBRBD_MANAGED_LOCK_UTILS_H

#include "include/int_types.h"
#include <string>

namespace librbd {
namespace managed_lock {
namespace util {

const std::string &get_watcher_lock_tag();

bool decode_lock_cookie(const std::string &tag, uint64_t *handle);
std::string encode_lock_cookie(uint64_t watch_handle);

} // namespace util
} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_MANAGED_LOCK_UTILS_H
