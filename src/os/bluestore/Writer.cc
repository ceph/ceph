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
#include "common/debug.h"
#include "include/intarith.h"
#include "os/bluestore/bluestore_types.h"

std::ostream& operator<<(std::ostream& out, const BlueStore::Writer::blob_data_printer& printer)
{
  out << std::hex;
  uint32_t lof = printer.base_position;
  for (const auto& q: printer.blobs) {
    out << " " << lof << "~" << q.real_length;
    if (q.is_compressed()) {
      out << "(" << q.compressed_length << ")";
    }
    lof += q.real_length;
  }
  out << std::dec;
  return out;
}

/// Empties range [offset~length] of object o that is in collection c.
/// Collects unused elements:
/// released - sequence of allocation units that are no longer used
/// pruned_blobs - set of blobs that are no longer used
/// shared_changed - set of shared blobs that are modified,
///                  including the case of shared blob being empty
/// statfs_delta - delta of stats
/// returns: iterator to ExtentMap following last element removed
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
      for (const auto& de: local_released) {
        p->blob->get_shared_blob()->put_ref(de.offset, de.length, &shared_released, &unshare);
      }
      for (const auto& de : shared_released) {
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

// split data
inline bufferlist split_left(bufferlist& data, uint32_t split_pos)
{
  bufferlist left;
  left.substr_of(data, 0, split_pos);
  data.splice(0, split_pos);
  return left;
}
inline bufferlist split_right(bufferlist& data, uint32_t split_pos)
{
  bufferlist right;
  data.splice(split_pos, data.length() - split_pos, &right);
  return right;
}

// should _maybe_expand_blob go to Blob ?
inline void BlueStore::Writer::_maybe_expand_blob(
  Blob* blob,
  uint32_t new_blob_size)
{
  ceph_assert(blob->get_blob().get_logical_length() > 0);
  if (blob->get_blob().get_logical_length() < new_blob_size) {
    uint32_t min_release_size = blob->get_blob_use_tracker().au_size;
    blob->add_tail(new_blob_size, min_release_size);
  }
}

#define dout_context bstore->cct
#define dout_subsys ceph_subsys_bluestore

//general levels:
// 10 init, fundamental state changes (not present here)
// 15 key functions, important params
// 20 most functions, most params
// 25 all functions, key variables
// 30 prints passing data (not used here)
// modifiers of extent, blob, onode printout:
// +0 nick + sdisk + suse
// +1 nick + sdisk + suse + sbuf
// +2 nick + sdisk + suse + sbuf + schk + attrs
// +3 ptr + disk + use + buf
// +4 ptr + disk + use + chk + buf + attrs
using exmp_it = BlueStore::extent_map_t::iterator;

uint16_t BlueStore::Writer::debug_level_to_pp_mode(CephContext* cct) {
  static constexpr uint16_t modes[5] = {
    P::NICK + P::SDISK + P::SUSE,
    P::NICK + P::SDISK + P::SUSE + P::SBUF,
    P::NICK + P::SDISK + P::SUSE + P::SBUF + P::SCHK + P::ATTRS,
    P::PTR + P::DISK + P::USE + P::BUF,
    P::PTR + P::DISK + P::USE + P::BUF + P::CHK + P::ATTRS
  };
  int level = cct->_conf->subsys.get_gather_level(dout_subsys);
  if (level >= 30) return modes[4];
  if (level <= 15) return modes[0];
  return modes[level % 5];
}


inline BlueStore::extent_map_t::iterator BlueStore::Writer::_find_mutable_blob_left(
  BlueStore::extent_map_t::iterator it,
  uint32_t search_begin, // only interested in blobs that are
  uint32_t search_end,   // within range [begin - end)
  uint32_t mapmust_begin,// for 'unused' case: the area
  uint32_t mapmust_end)  // [begin - end) must be mapped
{
  extent_map_t& map = onode->extent_map.extent_map;
  if (it == map.begin()) {
    return map.end();
  }
  do {
    --it;
    if (it->logical_offset < search_begin) break;
    if (search_begin > it->blob_start()) continue;
    if (it->blob_end() > search_end) continue;
    if (it->blob_start() > mapmust_begin) continue;
    auto bblob = it->blob->get_blob();
    if (!bblob.is_mutable()) continue;
    if (bblob.has_csum()) {
      uint32_t mask = (mapmust_begin - it->blob_start()) |
        (mapmust_end - it->blob_start());
      if (p2phase(mask, bblob.get_csum_chunk_size()) != 0) continue;
    }
    if (bblob.has_unused()) {
      // very difficult to expand blob with unused (our unused logic is ekhm)
      if (it->blob_end() <= mapmust_end) continue;
    }
    return it;
  } while (it != map.begin());
  return map.end();
}

inline BlueStore::extent_map_t::iterator BlueStore::Writer::_find_mutable_blob_right(
  BlueStore::extent_map_t::iterator it,
  uint32_t search_begin,  // only interested in blobs that are
  uint32_t search_end,    // within range [begin - end)
  uint32_t mapmust_begin, // for 'unused' case: the area
  uint32_t mapmust_end)   // [begin - end) must be mapped
{
  extent_map_t& map = onode->extent_map.extent_map;
  for (;it != map.end();++it) {
    if (it->logical_offset >= search_end) break;
    if (search_begin > it->blob_start()) continue;
    if (it->blob_end() > search_end) continue;
    if (it->blob_start() > mapmust_begin) continue;
    auto bblob = it->blob->get_blob();
    if (!bblob.is_mutable()) continue;
    if (bblob.has_csum()) {
      uint32_t mask = (mapmust_begin - it->blob_start()) |
        (mapmust_end - it->blob_start());
      if (p2phase(mask, bblob.get_csum_chunk_size()) != 0) continue;
    }
    if (bblob.has_unused()) {
      // very difficult to expand blob with unused (our unused logic is ekhm)
      if (it->blob_end() <= mapmust_end) continue;
    }
    return it;
    break;
  };
  return map.end();
}

void BlueStore::Writer::_get_disk_space(
  uint32_t length,
  PExtentVector& dst)
{
  while (length > 0) {
    ceph_assert(disk_allocs.it->length > 0);
    uint32_t s = std::min(length, disk_allocs.it->length - disk_allocs.pos);
    length -= s;
    dst.emplace_back(disk_allocs.it->offset + disk_allocs.pos, s);
    disk_allocs.pos += s;
    if (disk_allocs.it->length == disk_allocs.pos) {
      ++disk_allocs.it;
      disk_allocs.pos = 0;
    }
  }
}

inline void BlueStore::Writer::_crop_allocs_to_io(
  PExtentVector& disk_extents,
  uint32_t crop_front,
  uint32_t crop_back)
{
  if (crop_front > 0) {
    ceph_assert(disk_extents.front().length > crop_front);
    disk_extents.front().offset += crop_front;
    disk_extents.front().length -= crop_front;
  }
  if (crop_back > 0) {
    ceph_assert(disk_extents.back().length > crop_back);
    disk_extents.back().length -= crop_back;
  }
}

/*
1. _blob_put_data (tool)
  Modifies existing blob to contain specific data, does not care
  for allocations. Does not check anything.

2. _blob_put_data_subau
  Modifies existing blob on range that is allocated, but 'unused'.
  Data is block aligned. No ref++;

3. _blob_put_data_allocate
  Modifies existing blob on unallocated range, puts allocations.
  Data is au aligned. No ref++;

4. _blob_put_data_combined
  No reason to combine 2 + 3.

5. _blob_create_with_data
  Create new blob with wctx specs.
  Gives blob allocation units. Puts data to blob. Sets unused.
  No ref++.

6. _blob_create_full
  Create new blob with wctx specs.
  Gives blob allocation units. Puts data to blob. No unused.
  Full ref++ done.
*/

inline void BlueStore::Writer::_blob_put_data(
  Blob* blob,
  uint32_t in_blob_offset,
  bufferlist disk_data)
{
  auto& bblob = blob->dirty_blob();
  uint32_t in_blob_end = in_blob_offset + disk_data.length();
  // update csum, used_in_blob and unused
  if (bblob.has_csum()) {
    // calc_csum has fallback for csum == NONE, but is not inlined
    bblob.calc_csum(in_blob_offset, disk_data);
  }
  bblob.mark_used(in_blob_offset, in_blob_end - in_blob_offset);
  // do not update ref, we do not know how much of the data is actually used
}

/// Modifies blob to accomodate new data.
/// For partial AU overwrites only.
/// Requirements:
/// - target range is block aligned
/// - has unused
/// - target range is 'unused'
/// By extension:
/// - csum & tracker are large enough
/// No ref++.
/// Similar to _blob_put_data_allocate, but does not put new allocations
inline void BlueStore::Writer::_blob_put_data_subau(
  Blob* blob,
  uint32_t in_blob_offset,
  bufferlist disk_data)
{
  auto& bblob = blob->dirty_blob();
  uint32_t in_blob_end = in_blob_offset + disk_data.length();
  ceph_assert(bblob.is_mutable());
  //TODO WHY? - ceph_assert(bblob.has_unused());
  //TODO WHY? - ceph_assert(bblob.is_unused(in_blob_offset, in_blob_end - in_blob_offset));
  uint32_t chunk_size = bblob.get_chunk_size(bstore->block_size);
  ceph_assert(p2phase(in_blob_offset, chunk_size) == 0);
  ceph_assert(p2phase(in_blob_end, chunk_size) == 0);
  ceph_assert(bblob.get_logical_length() >= in_blob_end);
  _blob_put_data(blob, in_blob_offset, disk_data);
}


/// Modifies blob to accomodate new data.
/// For AU aligned operations only.
/// Requirements:
/// - blob is mutable
/// - target range is AU aligned
/// - csum and tracker are large enough
/// Calculates csum, clears unused.
/// Moves disk space from disk_allocs to blob.
/// No ref++.
inline void BlueStore::Writer::_blob_put_data_allocate(
  Blob* blob,
  uint32_t in_blob_offset,
  bufferlist disk_data)
{
  dout(25) << __func__ << "@" << std::hex << in_blob_offset
    << "~" << disk_data.length() << std::dec << " -> " << blob->print(pp_mode) << dendl;
  auto& bblob = blob->dirty_blob();
  uint32_t in_blob_end = in_blob_offset + disk_data.length();
  ceph_assert(bblob.is_mutable());
  ceph_assert(p2phase(in_blob_offset, (uint32_t)bstore->min_alloc_size) == 0);
  ceph_assert(p2phase(in_blob_end, (uint32_t)bstore->min_alloc_size) == 0);
  ceph_assert(bblob.get_logical_length() >= in_blob_end);
  _blob_put_data(blob, in_blob_offset, disk_data);
  PExtentVector blob_allocs;
  _get_disk_space(in_blob_end - in_blob_offset, blob_allocs);
  bblob.allocated(in_blob_offset, in_blob_end - in_blob_offset, blob_allocs);
  _schedule_io(blob_allocs, disk_data);

  dout(25) << __func__ << " 0x" << std::hex << disk_data.length()
    << "@" << in_blob_offset << std::dec << " -> "
    << blob->print(pp_mode) << " no ref yet" << dendl;
}

/// Modifies blob to accomodate new data.
/// Only operates on new AUs. Takes those AUs from 'disk_allocs'.
/// Requirements:
/// - blob is mutable
/// - target range is csum and tracker aligned
/// - csum and tracker are large enough
/// No AU alignment requirement.
/// Calculates csum, clears unused.
/// No ref++.
/// Very similiar to _blob_put_data_allocate, but also allows for partial AU writes.
/// to newly allocated AUs
inline void BlueStore::Writer::_blob_put_data_subau_allocate(
  Blob* blob,
  uint32_t in_blob_offset,
  bufferlist disk_data)
{
  dout(25) << __func__ << "@" << std::hex << in_blob_offset
    << "~" << disk_data.length() << std::dec << " -> " << blob->print(pp_mode) << dendl;
  auto& bblob = blob->dirty_blob();
  uint32_t au_size = bstore->min_alloc_size;
  uint32_t in_blob_end = in_blob_offset + disk_data.length();
  uint32_t chunk_size = bblob.get_chunk_size(bstore->block_size);
  ceph_assert(bblob.is_mutable());
  ceph_assert(p2phase(in_blob_offset, chunk_size) == 0);
  ceph_assert(p2phase(in_blob_end, chunk_size) == 0);
  ceph_assert(bblob.get_logical_length() >= in_blob_end);
  uint32_t in_blob_alloc_offset = p2align(in_blob_offset, au_size);
  uint32_t in_blob_alloc_end = p2roundup(in_blob_end, au_size);
  _blob_put_data(blob, in_blob_offset, disk_data);
  PExtentVector blob_allocs;
  _get_disk_space(in_blob_alloc_end - in_blob_alloc_offset, blob_allocs);
  bblob.allocated(in_blob_alloc_offset, in_blob_alloc_end - in_blob_alloc_offset, blob_allocs);
  PExtentVector& disk_extents = blob_allocs;
  _crop_allocs_to_io(disk_extents, in_blob_offset - in_blob_alloc_offset,
    in_blob_alloc_end - in_blob_offset - disk_data.length());
  _schedule_io(disk_extents, disk_data);
  dout(25) << __func__ << " 0x" << std::hex << disk_data.length()
    << "@" << in_blob_offset << std::dec << " -> "
    << blob->print(pp_mode) << " no ref yet" << dendl;
}


/// Create new blob with wctx specs.
/// Allowed for block and AU alignments.
/// Requirements:
/// - target range is block aligned
/// Calculates csum, sets unused.
/// Moves disk space from disk_allocs to blob.
/// No ref++.
BlueStore::BlobRef BlueStore::Writer::_blob_create_with_data(
  uint32_t in_blob_offset,
  bufferlist& disk_data)
{
  uint32_t block_size = bstore->block_size;
  uint32_t min_alloc_size = bstore->min_alloc_size;
  ceph_assert(p2phase(in_blob_offset, block_size) == 0);
  ceph_assert(p2phase(disk_data.length(), block_size) == 0);
  BlobRef blob = onode->c->new_blob();
  bluestore_blob_t &bblob = blob->dirty_blob();
  uint32_t data_length = disk_data.length();
  uint32_t alloc_offset = p2align(in_blob_offset, min_alloc_size);
  uint32_t blob_length = p2roundup(in_blob_offset + data_length, min_alloc_size);
  uint32_t tracked_unit = min_alloc_size;
  uint32_t csum_length_mask = in_blob_offset | data_length; //to find 2^n common denominator
  uint32_t csum_order = // conv 8 -> 32 so "<<" does not overflow
    std::min<uint32_t>(wctx->csum_order, std::countr_zero(csum_length_mask));
  if (wctx->csum_type != Checksummer::CSUM_NONE) {
    bblob.init_csum(wctx->csum_type, csum_order, blob_length);
    bblob.calc_csum(in_blob_offset, disk_data);
    tracked_unit = std::max(1u << csum_order, min_alloc_size);
  }
  blob->dirty_blob_use_tracker().init(blob_length, tracked_unit);
  PExtentVector blob_allocs;
  _get_disk_space(blob_length - alloc_offset, blob_allocs);
  bblob.allocated(alloc_offset, blob_length - alloc_offset, blob_allocs);
  //^sets also logical_length = blob_length
  dout(25) << __func__ << " @0x" << std::hex << in_blob_offset
    << "~" << disk_data.length()
    << " alloc_offset=" << alloc_offset
    << " -> " << blob->print(pp_mode) << dendl;
  PExtentVector& disk_extents = blob_allocs;
  _crop_allocs_to_io(disk_extents, in_blob_offset - alloc_offset,
    blob_length - in_blob_offset - disk_data.length());
  _schedule_io(disk_extents, disk_data);
  return blob;
}

/// Create new blob with wctx specs, fill with data.
/// Requirements:
/// - data is AU aligned
/// Calculates csum, sets unused.
/// Moves disk space from disk_allocs to blob.
/// Full ref done.
BlueStore::BlobRef BlueStore::Writer::_blob_create_full(
  bufferlist& disk_data)
{
  uint32_t min_alloc_size = bstore->min_alloc_size;
  uint32_t blob_length = disk_data.length();
  ceph_assert(p2phase<uint32_t>(blob_length, bstore->min_alloc_size) == 0);
  BlobRef blob = onode->c->new_blob();

  //uint32_t in_blob_end = disk_data.length();
  bluestore_blob_t &bblob = blob->dirty_blob();
  uint32_t tracked_unit = min_alloc_size;
  uint32_t csum_order = // conv 8 -> 32 so "<<" does not overflow
    std::min<uint32_t>(wctx->csum_order, std::countr_zero(blob_length));
  if (wctx->csum_type != Checksummer::CSUM_NONE) {
    bblob.init_csum(wctx->csum_type, csum_order, blob_length);
    bblob.calc_csum(0, disk_data);
    tracked_unit = std::max(1u << csum_order, min_alloc_size);
  }
  //std::cout << "blob_length=" << blob_length << std::endl;
  blob->dirty_blob_use_tracker().init_and_ref(blob_length, tracked_unit);
  PExtentVector blob_allocs;
  _get_disk_space(blob_length, blob_allocs);
  _schedule_io(blob_allocs, disk_data); //have to do before move()
  bblob.allocated_full(blob_length, std::move(blob_allocs));
  bblob.mark_used(0, blob_length); //todo - optimize; this obviously clears it
  return blob;
}

/**
 * Note from developer
 * This module tries to keep naming convention:
 * 1) Data location in object is named "position/location/begin", not "offset".
 * 2) Data location within blob is named "offset".
 * 3) Disk location is named "position/location", not "offset".
 */

/*
  note for myself
  I decided to not mix sub-au writes and normal writes.
  When there is sub-au write to blob there are 2 cases:
  a) entire write region is "unused"
     In this case we do speed up direct write
  b) some part is "used"
     1) read block-wise and do deferred
     2) read au-wise and have choice deferred / direct
  end note for myself

  Let's treat case of 'unused' as special.
  If such thing happens, move execution of it outside
  optimization logic.
  So, before going to main processing we do 'unused'.
  Then we crop the data and continue with rest.
  It is only first and last blob that can be unused.

  The real use case of unused is when AU is 64k, block is 4k.
  There is a difference in expected size of deferred on appends:
  without its avg ~32k, with it only ~2k.
  The unused feature would be useful if ZERO_OP could reset used->unused,
  but this one is not easy.
  This is why we do not bother with considering unused
  in non-head / non-tail blobs.
  Which change of default AU 64k->4k, its importance dwindles.

  note for myself
  The presence of blobs with unused makes impact on alignment restrictions?
  It seems reasonable that expand-read should be to block size.
  Even if we allocate larger AU, there is no need to write to empty.
  Overwrite must be deferred or to unused.
  Can I just make a determination that unused is an excuse not to do deferred?
  Or is writing to unused just a signal that reallocation is not an option?
  Clearly if something is unused, then it does exist.
  So write-selection function could make a determination what to do.
  But having limitations complicates optimization alg
  If I sacrifice optimization of defered, will I be done?

*/

/**
 * Transfer to disk modulated by unused() bits
 *
 * Blob can have unused() bits; it encodes which disk blocks are allocated,
 * but have never been used. Those bits determine if we can do direct or
 * deferred write is required.
 * Function has \ref Writer::test_write_divertor bypass for testing purposes.
 *
 * disk_position - Location must be disk block aligned.
 * data          - Data to write.
 * mask          - Set of unused() bits, starting from bit 0.
 * chunk_size    - Size covered by one "mask" bit.
 */
inline void BlueStore::Writer::_schedule_io_masked(
  uint64_t disk_position,
  bufferlist data,
  bluestore_blob_t::unused_t mask,
  uint32_t chunk_size)
{
  if (test_write_divertor == nullptr) {
    int32_t data_left = data.length();
    while (data_left > 0) {
      bool chunk_is_unused = (mask & 1) != 0;
      bufferlist ddata;
      data.splice(0, chunk_size, &ddata);
      if (chunk_is_unused) {
        bstore->bdev->aio_write(disk_position, ddata, &txc->ioc, false);
        bstore->logger->inc(l_bluestore_write_small_unused);
      } else {
        bluestore_deferred_op_t *op = bstore->_get_deferred_op(txc, ddata.length());
        op->op = bluestore_deferred_op_t::OP_WRITE;
        op->extents.emplace_back(bluestore_pextent_t(disk_position, chunk_size));
        op->data = ddata;
        bstore->logger->inc(l_bluestore_issued_deferred_writes);
        bstore->logger->inc(l_bluestore_issued_deferred_write_bytes, ddata.length());
      }
      disk_position += chunk_size;
      data_left -= chunk_size;
      mask >>= 1;
    }
    ceph_assert(data_left == 0);
  } else {
    int32_t data_left = data.length();
    while (data_left > 0) {
      bool chunk_is_unused = (mask & 1) != 0;
      bufferlist ddata;
      data.splice(0, chunk_size, &ddata);
      test_write_divertor->write(disk_position, ddata, !chunk_is_unused);
      disk_position += chunk_size;
      data_left -= chunk_size;
      mask >>= 1;
    }
    ceph_assert(data_left == 0);
  }
}

/**
 * Transfer to disk
 *
 * Initiates transfer of data to disk.
 * Depends on \ref Writer::do_deferred to select direct or deferred action.
 * If \ref Writer::test_write_divertor bypass is set it overrides default path.
 *
 * disk_extents   - Target disk blocks
 * data           - Data.
 */
inline void BlueStore::Writer::_schedule_io(
  const PExtentVector& disk_extents,
  bufferlist data)
{
  if (test_write_divertor == nullptr) {
    if (do_deferred) {
      bluestore_deferred_op_t *op = bstore->_get_deferred_op(txc, data.length());
      op->op = bluestore_deferred_op_t::OP_WRITE;
      op->extents = disk_extents;
      op->data = data;
      bstore->logger->inc(l_bluestore_issued_deferred_writes);
      bstore->logger->inc(l_bluestore_issued_deferred_write_bytes, data.length());
    } else {
      for (const auto& loc : disk_extents) {
        bufferlist data_chunk;
        data.splice(0, loc.length, &data_chunk);
        bstore->bdev->aio_write(loc.offset, data_chunk, &txc->ioc, false);
      }
      ceph_assert(data.length() == 0);
    }
  } else {
    for (const auto& loc: disk_extents) {
      bufferlist data_chunk;
      data.splice(0, loc.length, &data_chunk);
      test_write_divertor->write(loc.offset, data_chunk, do_deferred);
    }
    ceph_assert(data.length() == 0);
  }
}

/**
 * Read part of own data
 *
 * Rados protocol allows for byte aligned writes. Disk blocks are larger and
 * we need to read data that is around to form whole block.
 *
 * If \ref Writer::test_read_divertor is set it overrides default.
 */
inline bufferlist BlueStore::Writer::_read_self(
  uint32_t position,
  uint32_t length)
{
  if (test_read_divertor == nullptr) {
    bufferlist result;
    int r;
    r = bstore->_do_read(onode->c, onode, position, length, result);
    ceph_assert(r >= 0 && r <= (int)length);
    size_t zlen = length - r;
    if (zlen) {
      result.append_zero(zlen);
      bstore->logger->inc(l_bluestore_write_pad_bytes, zlen);
    }
    bstore->logger->inc(l_bluestore_write_small_pre_read);
    return result;
  } else {
    return test_read_divertor->read(position, length);
  }
}

// used to put data to blobs that does not require allocation
// crops data from bufferlist,
// returns disk pos and length and mask
// or updates wctx does deferred/direct
void BlueStore::Writer::_try_reuse_allocated_l(
  exmp_it after_punch_it,   // hint, we could have found it ourselves
  uint32_t& logical_offset, // will fix value if something consumed
  uint32_t ref_end_offset,  // limit to ref, if data was padded
  blob_data_t& bd)            // modified when consumed
{
  uint32_t search_stop = p2align(logical_offset, (uint32_t)wctx->target_blob_size);
  uint32_t au_size = bstore->min_alloc_size;
  uint32_t block_size = bstore->block_size;
  ceph_assert(!bd.is_compressed());
  ceph_assert(p2phase<uint32_t>(logical_offset, au_size) != 0);
  BlueStore::ExtentMap& emap = onode->extent_map;
  auto it = after_punch_it;
  while (it != emap.extent_map.begin()) {
    --it;
    // first of all, check it we can even use the blob here
    if (it->blob_end() < search_stop) break;
    if (it->blob_end() <= logical_offset) continue; // need at least something
    Blob* b = it->blob.get();
    dout(25) << __func__ << " trying " << b->print(pp_mode) << dendl;
    bluestore_blob_t bb = b->dirty_blob();
    if (!bb.is_mutable()) continue;
    // all offsets must be aligned to blob chunk_size,
    // which is larger of csum and device block granularity
    bufferlist& data = bd.disk_data;
    uint32_t chunk_size = it->blob->get_blob().get_chunk_size(block_size);
    if (p2phase(logical_offset, chunk_size) != 0) continue;
    // this blob can handle required granularity
    // the blob might, or might not be allocated where we need it
    // note we operate on 1 AU max
    uint32_t blob_offset = it->blob_start();
    uint32_t want_subau_begin = logical_offset; //it is chunk_size aligned
    uint32_t want_subau_end = p2roundup(logical_offset, au_size);
    if (logical_offset + data.length() < want_subau_end) {
      // we do not have enough data to cut at AU, try chunk
      want_subau_end = logical_offset + data.length();
      if (p2phase(want_subau_end, chunk_size) !=0) continue;
    }
    if (want_subau_begin < it->blob_start()) continue;
    if (want_subau_begin >= it->blob_end()) continue;
    uint32_t in_blob_offset = want_subau_begin - blob_offset;
    uint64_t subau_disk_offset = bb.get_allocation_at(in_blob_offset);
    if (subau_disk_offset == bluestore_blob_t::NO_ALLOCATION) continue;
    dout(25) << __func__ << " 0x" << std::hex << want_subau_begin << "-"
      << want_subau_end << std::dec << " -> " << b->print(pp_mode) << dendl;
    uint32_t data_size = want_subau_end - want_subau_begin;
    bufferlist data_at_left = split_left(data, data_size);
    bd.real_length -= data_size;
    uint32_t mask = bb.get_unused_mask(in_blob_offset, data_size, chunk_size);
    _blob_put_data_subau(b, in_blob_offset, data_at_left);
    // transfer do disk
    _schedule_io_masked(subau_disk_offset, data_at_left, mask, chunk_size);

    uint32_t ref_end = std::min(ref_end_offset, want_subau_end);
    //fixme/improve - need something without stupid extras - that is without coll
    b->get_ref(onode->c, in_blob_offset, ref_end - want_subau_begin);
    Extent *le = new Extent(
      want_subau_begin, in_blob_offset, ref_end - want_subau_begin, it->blob);
    dout(20) << __func__ << " new extent " << le->print(pp_mode) << dendl;
    emap.extent_map.insert(*le);

    logical_offset += data_size;
    break;
  }
}

// used to put data to blobs that does not require allocation
// crops data from bufferlist,
// returns disk pos and length and mask
// or updates wctx does deferred/direct
// AU | AU                    | AU
//    |bl|bl|bl|bl|bl|bl|bl|bl|
//    |csum |csum |csum |csum |
// datadatadatadatada           case A - input rejected
//       tadatadat              case B - input rejected
void BlueStore::Writer::_try_reuse_allocated_r(
  exmp_it after_punch_it,   // hint, we could have found it ourselves
  uint32_t& end_offset,     // will fix value if something consumed
  uint32_t ref_end_offset,  // limit to ref, if data was padded
  blob_data_t& bd)            // modified when consumed
{
  // this function should be called only when its applicable
  // that is, data is not compressed and is not AU aligned
  uint32_t au_size = bstore->min_alloc_size;
  uint32_t block_size = bstore->block_size;
  uint32_t blob_size = wctx->target_blob_size;
  uint32_t search_end = p2roundup(end_offset, blob_size);
  ceph_assert(!bd.is_compressed());
  ceph_assert(p2phase<uint32_t>(end_offset, au_size) != 0);
  BlueStore::ExtentMap& emap = onode->extent_map;
  for (auto& it = after_punch_it; it != emap.extent_map.end(); ++it) {
    // first of all, check it we can even use the blob here
    if (it->logical_offset >= search_end) break;
    Blob* b = it->blob.get();
    dout(25) << __func__ << " trying " << b->print(pp_mode) << dendl;
    bluestore_blob_t bb = b->dirty_blob();
    if (!bb.is_mutable()) continue;

    // all offsets must be aligned to blob chunk_size,
    // which is larger of csum and device block granularity
    bufferlist& data = bd.disk_data;
    uint32_t chunk_size = it->blob->get_blob().get_chunk_size(block_size);
    if (p2phase(end_offset, chunk_size) != 0) continue; //case A
    uint32_t blob_offset = it->blob_start();
    uint32_t want_subau_begin = p2align(end_offset, au_size); //we operate on 1 AU max
    uint32_t want_subau_end = end_offset; //it is chunk_size aligned
    if (data.length() < end_offset - want_subau_begin) {
      // we do not have enough data to cut at AU, fallback to chunk
      want_subau_begin = end_offset - data.length();
      if (p2phase(want_subau_begin, chunk_size) != 0) continue; //case B
    }
    if (want_subau_begin < it->blob_start()) continue;
    if (want_subau_begin >= it->blob_end()) continue;
    uint32_t in_blob_offset = want_subau_begin - blob_offset;
    uint64_t subau_disk_offset = bb.get_allocation_at(in_blob_offset);
    if (subau_disk_offset == bluestore_blob_t::NO_ALLOCATION) continue;
    dout(25) << __func__ << " 0x" << std::hex << want_subau_begin << "-"
      << want_subau_end << std::dec << " -> " << b->print(pp_mode) << dendl;
    uint32_t data_size = want_subau_end - want_subau_begin;
    bufferlist data_at_right = split_right(data, data.length() - data_size);
    bd.real_length -= data_size;
    uint32_t mask = bb.get_unused_mask(in_blob_offset, data_size, chunk_size);
    _blob_put_data_subau(b, in_blob_offset, data_at_right);
    //transfer to disk
    _schedule_io_masked(subau_disk_offset, data_at_right, mask, chunk_size);

    uint32_t ref_end = std::min(ref_end_offset, want_subau_end);
    //fixme/improve - need something without stupid extras - that is without coll
    b->get_ref(onode->c, in_blob_offset, ref_end - want_subau_begin);
    Extent *le = new Extent(
      want_subau_begin, in_blob_offset, ref_end - want_subau_begin, it->blob);
    dout(20) << __func__ << " new extent " << le->print(pp_mode) << dendl;
    emap.extent_map.insert(*le);

    end_offset -= data_size;
    break;
  }
}

/**
 * Export some data to neighboring blobs.
 *
 * Sometimes punch_hole_2 will clear only part of AU.
 * Example: AU = 64K, DiskBlock = 4K, CSUM = 16K.
 * Punch_hole_2 will always align to max(DiskBlock, CSUM) and get rid of whole AUs,
 * but the boundary ones might need to leave some data intact, leaving some
 * space unused. This function tries to use that space.
 *
 * If possible function cuts portions of data from first and last
 * element of blob_data_t sequence. Params logical_offset, end_offset and
 * ref_end_offset are updated to reflect data truncation.
 * Only uncompressed input data is eligiable for being moved to other blobs.
 *
 * logical_offset - In-object offset of first byte in bd.
 * end_offset     - Offset of last byte in bd.
 * ref_end_offset - Last byte that should be part of object; ref_end_offset <= end_offset.
 * bd             - Continous sequence of data blocks to be put to object.
 * after_punch_it - Hint from punch_hole_2.
 *                  Blobs to modify will be either left of it (for left search),
 *                  or right of it (for right side search).
 */
void BlueStore::Writer::_try_put_data_on_allocated(
  uint32_t& logical_offset,
  uint32_t& end_offset,
  uint32_t& ref_end_offset,
  blob_vec& bd,
  exmp_it after_punch_it)
{
  const char* func_name = __func__;
  auto print = [&](const char* caption) {
    dout(25) << func_name << caption << std::hex << logical_offset << ".."
      << end_offset << " ref_end=" << ref_end_offset << " bd=";
    uint32_t lof = logical_offset;
    for (const auto& q: bd) {
      *_dout << " " << lof << "~" << q.disk_data.length();
      lof += q.disk_data.length();
    }
    *_dout << std::dec << dendl;
  };
  print(" IN ");
  ceph_assert(bstore->min_alloc_size != bstore->block_size);
  ceph_assert(bd.size() >= 1);
  if (!bd[0].is_compressed() &&
    p2phase<uint32_t>(logical_offset, bstore->min_alloc_size) != 0) {
    // check if we have already allocated space to fill
    _try_reuse_allocated_l(after_punch_it, logical_offset, ref_end_offset, bd[0]);
  }
  if (bd[0].real_length == 0) {
    bd.erase(bd.begin());
  }
  if (logical_offset == end_offset) {
    // it is possible that we already consumed all
    goto out;
  }
  print(" MID ");
  {
    ceph_assert(bd.size() >= 1);
    auto &bd_back = bd.back();
    if (!bd_back.is_compressed() &&
      p2phase<uint32_t>(end_offset, bstore->min_alloc_size) != 0) {
      // check if we have some allocated space to fill
      _try_reuse_allocated_r(after_punch_it, end_offset, ref_end_offset, bd_back);
    }
    if (bd_back.real_length == 0) {
      bd.erase(bd.end() - 1);
    }
  }
  out:
  print(" OUT ");
}

/**
 * Puts data to onode by creating new blobs/extents.
 *
 * Does not check if data can be merged into other blobs are done.
 * Requires that the target region is already emptied (\ref punch_hole_2).
 *
 * Input data is a continous sequence of blob_data_t segments
 * that starts at logical_offset.
 * This is the final step in processing write op.
 *
 * logical_offset - Offset of first blob_data_t element.
 * ref_end_offset - Actual data end, it might be earlier then last blob_data_t.
 *                  It happens because we pad data to disk block alignment,
 *                  while we preserve logical range of put data.
 * bd_it..bd_end  - Sequence of blob_data_t to put.
 */
void BlueStore::Writer::_do_put_new_blobs(
  uint32_t logical_offset,
  uint32_t ref_end_offset,
  blob_vec::iterator& bd_it,
  blob_vec::iterator bd_end)
{
  extent_map_t& emap = onode->extent_map.extent_map;
  uint32_t blob_size = wctx->target_blob_size;
  while (bd_it != bd_end) {
    Extent* le;
    if (!bd_it->is_compressed()) {
      // only 1st blob to write can have blob_location != logical_offset
      uint32_t blob_location = p2align(logical_offset, blob_size);
      BlobRef new_blob;
      uint32_t in_blob_offset = logical_offset - blob_location;
      uint32_t ref_end = std::min(ref_end_offset, logical_offset + bd_it->disk_data.length());
      if (blob_location == logical_offset &&
          bd_it->disk_data.length() >= blob_size &&
          ref_end_offset - blob_location >= blob_size) {
        new_blob = _blob_create_full(bd_it->disk_data);
        // all already ref'ed
      } else {
        new_blob = _blob_create_with_data(in_blob_offset, bd_it->disk_data);
        new_blob->get_ref(onode->c, in_blob_offset, ref_end - blob_location - in_blob_offset);
      }
      le = new Extent(
        logical_offset, in_blob_offset, ref_end - logical_offset, new_blob);
      dout(20) << __func__ << " new extent+blob " << le->print(pp_mode) << dendl;
      emap.insert(*le);
      logical_offset = ref_end;
    } else {
      // compressed
      ceph_assert(false);
    }
    bstore->logger->inc(l_bluestore_write_big);
    bstore->logger->inc(l_bluestore_write_big_bytes, le->length);
    ++bd_it;
  }
}

void BlueStore::Writer::_do_put_blobs(
  uint32_t logical_offset,
  uint32_t data_end_offset,
  uint32_t ref_end_offset,
  blob_vec& bd,
  exmp_it after_punch_it)
{
  Collection* coll = onode->c;
  extent_map_t& emap = onode->extent_map.extent_map;
  uint32_t au_size = bstore->min_alloc_size;
  uint32_t blob_size = wctx->target_blob_size;
  auto bd_it = bd.begin();
  exmp_it to_it;
  uint32_t left_bound = p2align(logical_offset, blob_size);
  uint32_t right_bound = p2roundup(logical_offset, blob_size);
  // Try to put first data pack to already existing blob
  if (!bd_it->is_compressed()) {
    // it is thinkable to put the data to some blob
    exmp_it left_b = _find_mutable_blob_left(
      after_punch_it, left_bound, right_bound,
      logical_offset, logical_offset + bd_it->disk_data.length());
    if (left_b != emap.end()) {
      uint32_t in_blob_offset = logical_offset - left_b->blob_start();
      uint32_t in_blob_end = in_blob_offset + bd_it->disk_data.length();
      uint32_t data_end_offset = logical_offset + bd_it->disk_data.length();
      _maybe_expand_blob(left_b->blob.get(), p2roundup(in_blob_end, au_size));
      _blob_put_data_subau_allocate(
        left_b->blob.get(), in_blob_offset, bd_it->disk_data);
      uint32_t ref_end = std::min(ref_end_offset, data_end_offset);
      //fixme/improve - need something without stupid extras - that is without coll
      left_b->blob->get_ref(coll, in_blob_offset, ref_end - logical_offset);
      Extent *le = new Extent(
        logical_offset, in_blob_offset, ref_end - logical_offset, left_b->blob);
      dout(20) << __func__ << " new extent " << le->print(pp_mode) << dendl;
      emap.insert(*le);
      logical_offset = ref_end;
      ++bd_it;
      bstore->logger->inc(l_bluestore_write_small);
      bstore->logger->inc(l_bluestore_write_small_bytes, le->length);
    } else {
      // it is still possible to use first bd and put it into
      // blob after punch_hole
      // can blob before punch_hole be different then blob after punch_hole ?
    }
  }
  if (bd_it != bd.end()) {
    // still something to process
    auto back_it = bd.end() - 1;
    if (!back_it->is_compressed()) {
      // it is thinkable to put the data to some after
      uint32_t left_bound = p2align(data_end_offset, blob_size);
      uint32_t right_bound = p2roundup(data_end_offset, blob_size);
      exmp_it right_b = _find_mutable_blob_right(
          after_punch_it, left_bound, right_bound,
          data_end_offset - back_it->disk_data.length(), data_end_offset);
      if (right_b != emap.end()) {
        // Before putting last blob, put all previous;
        // it is nicer to have AUs in order.
        if (bd_it != back_it) {
          // Last blob will be merged, we put blobs without the last.
          _do_put_new_blobs(logical_offset, ref_end_offset, bd_it, back_it);
        }
        uint32_t data_begin_offset = data_end_offset - back_it->disk_data.length();
        uint32_t in_blob_offset = data_begin_offset - right_b->blob_start();
        _maybe_expand_blob(right_b->blob.get(), in_blob_offset + bd_it->disk_data.length());
        _blob_put_data_subau_allocate(
          right_b->blob.get(), in_blob_offset, back_it->disk_data);
        uint32_t ref_end = std::min(ref_end_offset, data_begin_offset + back_it->disk_data.length());
        //fixme - need something without stupid extras
        right_b->blob->get_ref(coll, in_blob_offset, ref_end - data_begin_offset);
        Extent *le = new Extent(
          data_begin_offset, in_blob_offset, ref_end - data_begin_offset, right_b->blob);
        dout(20) << __func__ << " new extent " << le->print(pp_mode) << dendl;
        emap.insert(*le);
        bd.erase(back_it); //TODO - or other way of limiting end
        bstore->logger->inc(l_bluestore_write_small);
        bstore->logger->inc(l_bluestore_write_small_bytes, le->length);
      }
    }
  }

  // that's it about blob reuse, now is the time to full blobs
  if (bd_it != bd.end()) {
    _do_put_new_blobs(logical_offset, ref_end_offset, bd_it, bd.end());
  }
}

/**
 * The idea is to give us a chance to reuse blob.
 * To do so, we must have enough to for block/csum/au.
 * The decision is to either read or to pad with zeros.
 * We return pair:
 * first: true = pad with 0s, false = read the region
 * second: new logical offset for data
 * NOTE: Unlike _write_expand_r expanded punch_hole region
 *       is always equal to ref'ed region.
 * NOTE2: This function can be called without split_at(logical_offset)
 * NOTE3: If logical_offset is AU aligned, some blobs have larger csum.
 *        We ignore them, in result not even wanting to expand.
 */
std::pair<bool, uint32_t> BlueStore::Writer::_write_expand_l(
  uint32_t logical_offset)
{
  uint32_t block_size = bstore->block_size;
  uint32_t off_stop = p2align<uint32_t>(logical_offset, bstore->min_alloc_size);
  // no need to go earlier then one AU
  ceph_assert(off_stop != logical_offset); // to prevent superfluous invocation
  uint32_t min_off = p2align(logical_offset, block_size);
  uint32_t new_data_off = min_off;
  bool     new_data_pad = true; // unless otherwise stated, we pad
  exmp_it it = onode->extent_map.seek_lextent(logical_offset);
  // it can be extent in which we are interested in
  if (it == onode->extent_map.extent_map.end() ||
    it->logical_offset >= logical_offset) {
    if (it == onode->extent_map.extent_map.begin()) {
      goto done;
    }
    --it; //step back to the first extent to consider
  }
  do {
    if (it->logical_end() < off_stop) {
      // Nothing before this point will be interesting.
      // Not found blob to adapt to.
      break;
    }
    if (!it->blob->get_blob().is_mutable()) {
      new_data_pad = false; // we have to read data here
      if (it == onode->extent_map.extent_map.begin()) break;
      --it;
      continue;
    }
    // we take first blob that we can
    uint32_t can_off = p2align<uint32_t>(logical_offset, it->blob->get_blob().get_chunk_size(block_size));
    // ^smallest stop point that blob can accomodate
    off_stop = can_off;
    new_data_off = can_off;
    // the blob is mapped, so it has space for at least up to begin of AU@logical_offset
    if (it->logical_offset < logical_offset && logical_offset < it->logical_end()) {
      // ^ this only works for the first extent we check
      new_data_pad = false;
    } else {
      if (it->logical_end() <= can_off) {
        // we have a fortunate area in blob that was mapped but not used
        // the new_data_pad here depends on whether we have visited immutable blobs
      } else {
        // interested in using this blob, but there is data, must read
        new_data_pad = false;
        //^ read means we must expand punch_hole / ref, but not outside object size
      }
    }
  } while ((it != onode->extent_map.extent_map.begin()) && (--it, true));
  done:
  dout(25) << __func__ << std::hex << " logical_offset=0x" << logical_offset
    << " -> 0x" << new_data_off << (new_data_pad ? " pad" : " read") << dendl;
  return std::make_pair(new_data_pad, new_data_off);
}

/**
 * The idea is to give us a chance to reuse blob.
 * To do so, we must have enough to for block/csum/au.
 * The decision is to either read or to pad with zeros.
 * We return pair:
 * first: true = pad with 0s, false = read the region
 * second: new end offset for data
 * NOTE: When we pad with 0s, we do not expand ref range.
 *       When we read, we expand ref range.
 *       Ref range cannot to outside object size.
 * NOTE2: This function can be called without split_at(end_offset)
 * NOTE3: If logical_offset is AU aligned, some blobs have larger csum.
 *       We ignore them, in result not even wanting to expand.
 */
std::pair<bool, uint32_t> BlueStore::Writer::_write_expand_r(
  uint32_t end_offset)
{
  uint32_t block_size = bstore->block_size;
  uint32_t end_stop = p2roundup<uint32_t>(end_offset, bstore->min_alloc_size);
  // no need to go further then one AU, since new blob it if happens can allocate one AU
  ceph_assert(end_stop != end_offset); // to prevent superfluous invocation
  uint32_t min_end = p2roundup(end_offset, block_size);
  uint32_t new_data_end = min_end;
  bool     new_data_pad = true; // unless otherwise stated, we pad
  exmp_it it = onode->extent_map.seek_lextent(end_offset);
  for (; it != onode->extent_map.extent_map.end(); ++it) {
    if (it->logical_offset >= end_stop) {
      // nothing beyond this point is interesting
      // no blob should have an free AU outside its logical mapping
      // This is failure in reuse search.
      break;
    }
    if (!it->blob->get_blob().is_mutable()) {
      new_data_pad = false; //must read...
      continue;
    }
    // if at end_offset is something then this blob certainly qualifies
    // we take first blob that we can
    uint32_t can_end = p2roundup<uint32_t>(end_offset, it->blob->get_blob().get_chunk_size(block_size));
    // ^smallest stop point that blob can accomodate
    end_stop = can_end;
    new_data_end = can_end;
    // the blob is mapped, so it has space for at least up to end of AU@end_offset
    if (it->logical_offset <= end_offset && end_offset < it->logical_end()) {
      // ^ this only works for the first extent we check
      new_data_pad = false;
      //^ read means we must expand punch_hole / ref, but not outside object size
    } else {
      if (can_end <= it->logical_offset) {
        // we have a fortunate area in blob that was mapped but not used
        // the new_data_pad here depends on whether we have visited immutable blobs
      } else {
        // interested in using this blob, but there is data, must read
        new_data_pad = false;
        //^ read means we must expand punch_hole / ref, but not outside object size
      }
    }
  }
  dout(25) << __func__ << std::hex << " end_offset=0x" << end_offset
    << " -> 0x" << new_data_end << (new_data_pad ? " pad" : " read") << dendl;
  return std::make_pair(new_data_pad, new_data_end);
}



// This function is a centralized place to make a decision on
// whether to use deferred or direct writes.
// The assumption behind it is that having parts of write executed as
// deferred and other parts as direct is suboptimal in any case.
void BlueStore::Writer::_defer_or_allocate(uint32_t need_size)
{
  // make a deferred decision
  uint32_t released_size = 0;
  for (const auto& r : released) {
    released_size += r.length;
  }
  uint32_t au_size = bstore->min_alloc_size;
  do_deferred = need_size <= released_size && released_size < bstore->prefer_deferred_size;
  dout(15) << __func__ << " released=0x" << std::hex << released_size
    << " need=0x" << need_size << std::dec
    << (do_deferred ? " deferred" : " direct") << dendl;

  if (do_deferred) {
    disk_allocs.it = released.begin();
    statfs_delta.allocated() += need_size;
    disk_allocs.pos = 0;
  } else {
    int64_t new_alloc_size = bstore->alloc->allocate(need_size, au_size, 0, 0, &allocated);
    ceph_assert(need_size == new_alloc_size);
    statfs_delta.allocated() += new_alloc_size;
    disk_allocs.it = allocated.begin();
    disk_allocs.pos = 0;
  }
}

// data (input) is split into chunks bd (output)
// data is emptied as a result
void BlueStore::Writer::_split_data(
  uint32_t location,
  bufferlist& data,
  blob_vec& bd)
{
  ceph_assert(bd.empty());
  bd.reserve(data.length() / wctx->target_blob_size + 2);
  auto lof = location;
  uint32_t end_offset = location + data.length();
  while (lof < end_offset) {
    uint32_t p = p2remain<uint32_t>(lof, wctx->target_blob_size);
    if (p > end_offset - lof) p = end_offset - lof;
    bufferlist tmp;
    data.splice(0, p, &tmp);
    bd.emplace_back(blob_data_t{p, 0, tmp, tmp});
    lof += p;
  }
}

void BlueStore::Writer::_align_to_disk_block(
  uint32_t& location,
  uint32_t& data_end,
  blob_vec& blobs)
{
  ceph_assert(!blobs.empty());
  uint32_t au_size = bstore->min_alloc_size;
  bool left_do_pad;
  bool right_do_pad;
  uint32_t left_location;
  uint32_t right_location;
  if (p2phase(location, au_size) != 0) {
    blob_data_t& first_blob = blobs.front();
    if (!first_blob.is_compressed()) {
      // try to make at least disk block aligned
      std::tie(left_do_pad, left_location) = _write_expand_l(location);
      if (left_location < location) {
        bufferlist tmp;
        if (left_do_pad) {
          tmp.append_zero(location - left_location);
          bstore->logger->inc(l_bluestore_write_pad_bytes, location - left_location);
        } else {
          tmp = _read_self(left_location, location - left_location);
        }
        tmp.claim_append(first_blob.disk_data);
        first_blob.disk_data.swap(tmp);
        first_blob.real_length += location - left_location;
        location = left_location;
      }
    }
  }
  if (p2phase(data_end, au_size) != 0) {
    blob_data_t& last_blob = blobs.back();
    if (!last_blob.is_compressed()) {
    // try to make at least disk block aligned
      std::tie(right_do_pad, right_location) = _write_expand_r(data_end);
      if (data_end < right_location) {
        // TODO - when we right-expand because of some blob csum restriction, it is possible
        // we will be left-blob-csum-unaligned. It is wasted space.
        // Think if we want to fix it.
        if (right_do_pad) {
          last_blob.disk_data.append_zero(right_location - data_end);
          bstore->logger->inc(l_bluestore_write_pad_bytes, right_location - data_end);
        } else {
          bufferlist tmp;
          tmp = _read_self(data_end, right_location - data_end);
          last_blob.disk_data.append(tmp);
        }
        last_blob.real_length += right_location - data_end;
      }
      data_end = right_location;
    }
  }
}

// Writes uncompressed data.
void BlueStore::Writer::do_write(
  uint32_t location,
  bufferlist& data)
{
  do_deferred = false;
  disk_allocs.it = allocated.end();
  disk_allocs.pos = 0;
  dout(20) << __func__ << " 0x" << std::hex << location << "~" << data.length() << dendl;
  dout(25) << "on: " << onode->print(pp_mode) << dendl;
  blob_vec bd;
  uint32_t ref_end = location + data.length();
  uint32_t data_end = location + data.length();
  _split_data(location, data, bd);
  _align_to_disk_block(location, data_end, bd);
  if (ref_end < onode->onode.size) {
    ref_end = std::min<uint32_t>(data_end, onode->onode.size);
  }
  dout(20) << "blobs to put:" << blob_data_printer(bd, location) << dendl;
  statfs_delta.stored() += ref_end - location;
  exmp_it after_punch_it =
    bstore->_punch_hole_2(onode->c, onode, location, data_end - location,
    released, pruned_blobs, txc->shared_blobs, statfs_delta);
  dout(25) << "after punch_hole_2: " << std::endl << onode->print(pp_mode) << dendl;

  // todo: if we align to disk block before splitting, we could do it in one go
  uint32_t pos = location;
  for (auto& b : bd) {
    bstore->_buffer_cache_write(this->txc, onode, pos, b.disk_data,
      wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);
    pos += b.disk_data.length();
  }
  ceph_assert(pos == data_end);

  uint32_t au_size = bstore->min_alloc_size;
  if (au_size != bstore->block_size) {
    _try_put_data_on_allocated(location, data_end, ref_end, bd, after_punch_it);
  }
  if (location != data_end) {
    // make a deferred decision
    uint32_t need_size = 0;
    uint32_t location_tmp = location;
    for (auto& i : bd) {
      uint32_t location_end = location_tmp + i.real_length;
      need_size += p2roundup(location_end, au_size) - p2align(location_tmp, au_size);
      location_tmp = location_end;
    }

    _defer_or_allocate(need_size);
    _do_put_blobs(location, data_end, ref_end, bd, after_punch_it);
  } else {
    // Unlikely, but we just put everything.
    ceph_assert(bd.size() == 0);
  }
  if (onode->onode.size < ref_end)
    onode->onode.size = ref_end;
  _collect_released_allocated();
  // update statfs
  txc->statfs_delta += statfs_delta;
  onode->extent_map.compress_extent_map(location, data_end - location);
  onode->extent_map.dirty_range(location, data_end-location);
  onode->extent_map.maybe_reshard(location, data_end);
  dout(25) << "result: " << std::endl << onode->print(pp_mode) << dendl;
}

/**
 * Move allocated and released regions to txc.
 * NOTE: Consider in future to directly use variables in txc.
 */
void BlueStore::Writer::_collect_released_allocated()
{
  if (!do_deferred) {
    // When we do direct all released is really released.
    for (const auto& e : released) {
      txc->released.insert(e.offset, e.length);
    }
    // We do not accept allocating more than really using later.
    ceph_assert(disk_allocs.it == allocated.end());
  } else {
    // When when we do deferred it is possible to not use all.
    // Release the unused rest.
    uint32_t pos = disk_allocs.pos;
    while (disk_allocs.it != released.end()) {
      auto& e = *disk_allocs.it;
      dout(15) << "Deferred, some left unused location=0x"
        << std::hex << e.offset + pos << "~" << e.length - pos << std::dec << dendl;
      txc->released.insert(e.offset + pos, e.length - pos);
      pos = 0;
      ++disk_allocs.it;
    }
  }
  for (auto e : allocated) {
    txc->allocated.insert(e.offset, e.length);
  }
  released.clear();
  allocated.clear();
}

/**
 * Debug function that extracts data from BufferSpace buffers.
 * Typically it is useless - it is not guaranteed that buffers will not be evicted.
 */
void BlueStore::Writer::debug_iterate_buffers(
  std::function<void(uint32_t offset, const bufferlist& data)> data_callback)
{
  for (const auto& b : onode->bc.buffer_map) {
    data_callback(b.offset, b.data);
  }
}
