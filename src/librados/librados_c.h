// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRADOS_C_H
#define LIBRADOS_C_H

#include "include/types.h"
#include "include/rados/librados.h"

namespace __librados_base {

struct rados_pool_stat_t {
  uint64_t num_bytes;
  uint64_t num_kb;
  uint64_t num_objects;
  uint64_t num_object_clones;
  uint64_t num_object_copies;
  uint64_t num_objects_missing_on_primary;
  uint64_t num_objects_unfound;
  uint64_t num_objects_degraded;
  uint64_t num_rd;
  uint64_t num_rd_kb;
  uint64_t num_wr;
  uint64_t num_wr_kb;
};

} // namespace __librados_base

#endif // LIBRADOS_C_H
