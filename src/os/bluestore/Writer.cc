// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "Writer.h"
#include "include/intarith.h"
#include "os/bluestore/bluestore_types.h"


/// Empties range [offset~length] of object o that is in collection c.
/// Collects unused elements:
/// released - sequence of allocation units that are no longer used
/// pruned_blobs - set of blobs that are no longer used
/// shared_changed - set of shared blobs that are modified,
///                  including the case of shared blob being empty
/// statfs_delta - delta of stats
BlueStore::extent_map_t::iterator BlueStore::_punch_hole_2(
  Collection* c,
  OnodeRef& o,
  uint32_t offset,
  uint32_t length,
  PExtentVector& released,
  std::vector<BlobRef>& pruned_blobs,       //completely emptied out blobs
  std::set<SharedBlobRef>& shared_changed,  //shared blobs that have changed
  volatile_statfs& statfs_delta)
{
  ExtentMap& emap = o->extent_map;
  uint32_t end = offset + length;
  auto p = emap.maybe_split_at(offset);
  while (p != emap.extent_map.end() && p->logical_offset < end) {
    // here split tail extent, if needed
    if (end < p->logical_end()) {
      p = emap.split_at(p, end);
      --p;
    }
    // here always whole lextent to drop
    auto& bblob = p->blob->dirty_blob();
    uint32_t released_size = 0;
    if (!bblob.is_shared()) {
      released_size =
        p->blob->put_ref_accumulate(c, p->blob_offset, p->length, &released);
    } else {
      // make sure shared blob is loaded
      c->load_shared_blob(p->blob->get_shared_blob());
      // more complicated shared blob release
      PExtentVector local_released;  //no longer used by local blob
      PExtentVector shared_released; //no longer used by shared blob too
      p->blob->put_ref_accumulate(c, p->blob_offset, p->length, &local_released);
      // filter local release disk regions
      // through SharedBlob's multi-ref ref_map disk regions
      bool unshare = false; //is there a chance that shared blob can be unshared?
      // TODO - make put_ref return released_size directly
      for (auto de: local_released) {
        p->blob->get_shared_blob()->put_ref(de.offset, de.length, &shared_released, &unshare);
      }
      for (auto& de : shared_released) {
        released_size += de.length;
      }
      released.insert(released.end(), shared_released.begin(), shared_released.end());
      shared_changed.insert(p->blob->get_shared_blob());
    }
    statfs_delta.allocated() -= released_size;
    statfs_delta.stored() -= p->length;
    if (bblob.is_compressed()) {
      statfs_delta.compressed_allocated() -= released_size;
      statfs_delta.compressed_original() -= p->length;
      if (!bblob.has_disk()) {
        statfs_delta.compressed() -= bblob.get_compressed_payload_length();
      }
    }
    if (!bblob.has_disk()) {
      pruned_blobs.push_back(p->blob);
      if (p->blob->is_spanning()) {
        emap.spanning_blob_map.erase(p->blob->id);
        p->blob->id = -1;
      }
    }
    Extent* e = &(*p);
    p = emap.extent_map.erase(p);
    delete e;
  }
  return p;
}


/// Signals that a range [offset~length] is no longer used.
/// Collects allocation units that became unused into *released_disk.
/// Returns:
///   disk space size to release
uint32_t BlueStore::Blob::put_ref_accumulate(
  Collection *coll,
  uint32_t offset,
  uint32_t length,
  PExtentVector *released_disk)
{
  ceph_assert(length > 0);
  uint32_t res = 0;
  auto [in_blob_offset, in_blob_length] = used_in_blob.put_simple(offset, length);
  if (in_blob_length != 0) {
    bluestore_blob_t& b = dirty_blob();
    res = b.release_extents(in_blob_offset, in_blob_length, released_disk);
    return res;
  }
  return res;
}

inline void BlueStore::Blob::add_tail(
  uint32_t new_blob_size,
  uint32_t min_release_size)
{
  ceph_assert(p2phase(new_blob_size, min_release_size) == 0);
  dirty_blob().add_tail(new_blob_size);
  used_in_blob.add_tail(new_blob_size, min_release_size);
}

inline void bluestore_blob_use_tracker_t::init_and_ref(
  uint32_t full_length,
  uint32_t tracked_chunk)
{
  ceph_assert(p2phase(full_length, tracked_chunk) == 0);
  uint32_t _num_au = full_length / tracked_chunk;
  au_size = tracked_chunk;
  if ( _num_au > 1) {
    allocate(_num_au);
    for (uint32_t i = 0; i < num_au; i++) {
      bytes_per_au[i] = tracked_chunk;
    }
  } else {
    total_bytes = full_length;
  }
}

inline void bluestore_blob_t::allocated_full(
  uint32_t length,
  PExtentVector&& allocs)
{
  ceph_assert(extents.size() == 0);
  extents.swap(allocs);
  logical_length = length;
}

