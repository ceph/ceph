// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_UTILS_H
#define CEPH_LIBRBD_MIGRATION_UTILS_H

#include "include/common_fwd.h"
#include "librbd/io/Types.h"
#include "librbd/migration/Types.h"
#include <optional>
#include <string>

namespace librbd {
namespace migration {
namespace util {

int parse_url(CephContext* cct, const std::string& url, UrlSpec* url_spec);

void zero_shrunk_snapshot(CephContext* cct, const io::Extents& image_extents,
                          uint64_t snap_id, uint64_t new_size,
                          std::optional<uint64_t> *previous_size,
                          io::SparseExtents* sparse_extents);
void merge_snapshot_delta(const io::SnapIds& snap_ids,
                          io::SnapshotDelta* snapshot_delta);

} // namespace util
} // namespace migration
} // namespace librbd

#endif // CEPH_LIBRBD_MIGRATION_UTILS_H
