// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_UTILS_H
#define CEPH_LIBRBD_DEEP_COPY_UTILS_H

#include "include/rados/librados.hpp"
#include "librbd/Types.h"
#include "librbd/deep_copy/Types.h"

#include <boost/optional.hpp>

namespace librbd {
namespace deep_copy {
namespace util {

void compute_snap_map(librados::snap_t snap_id_start,
                      librados::snap_t snap_id_end,
                      const SnapSeqs &snap_seqs,
                      SnapMap *snap_map);

} // namespace util
} // namespace deep_copy
} // namespace librbd

#endif // CEPH_LIBRBD_DEEP_COPY_UTILS_H
