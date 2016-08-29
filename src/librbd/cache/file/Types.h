// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_TYPES
#define CEPH_LIBRBD_CACHE_FILE_TYPES

#include "include/buffer_fwd.h"
#include "include/int_types.h"

namespace librbd {
namespace cache {
namespace file {

/**
 * Persistent on-disk cache structures
 */

namespace stupid_policy {

struct Entry {
  bool dirty;
  bool allocated;
  uint64_t block;
};

} // namespace stupid_policy

} // namespace file
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_FILE_TYPES
