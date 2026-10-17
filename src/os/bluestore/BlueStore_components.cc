// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/dout.h"
#include "common/debug.h"

#include "BlueStore.h"
#include "BlueStore_components.h"
#include "BlueStore_inlines.h"
#include "os/kv.h"
#include "common/pretty_binary.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore

using std::min;
using std::numeric_limits;
using std::less;
using std::list;
using std::map;
using std::max;
using std::ostream;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;

// Garbage Collector
#undef dout_prefix
#define dout_prefix *_dout << "bluestore.GarbageCollector "
#undef dout_context
#define dout_context cct


void bluestore::GarbageCollector::process_protrusive_extents(
  const bluestore::ExtentMap& extent_map,
  uint64_t start_offset,
  uint64_t end_offset,
  uint64_t start_touch_offset,
  uint64_t end_touch_offset,
  uint64_t min_alloc_size)
{
  ceph_assert(start_offset <= start_touch_offset && end_offset >= end_touch_offset);

  uint64_t lookup_start_offset = p2align(start_offset, min_alloc_size);
  uint64_t lookup_end_offset = round_up_to(end_offset, min_alloc_size);

  dout(30) << __func__ << " (hex): [" << std::hex
    << lookup_start_offset << ", " << lookup_end_offset
    << ")" << std::dec << dendl;

  for (auto it = extent_map.seek_lextent(lookup_start_offset);
    it != extent_map.extent_map.end() &&
    it->logical_offset < lookup_end_offset;
    ++it) {
    uint64_t alloc_unit_start = it->logical_offset / min_alloc_size;
    uint64_t alloc_unit_end = (it->logical_end() - 1) / min_alloc_size;

    dout(30) << __func__ << " " << *it
      << "alloc_units: " << alloc_unit_start << ".." << alloc_unit_end
      << dendl;

    Blob* b = it->blob.get();

    if (it->logical_offset >= start_touch_offset &&
      it->logical_end() <= end_touch_offset) {
      // Process extents within the range affected by
      // the current write request.
      // Need to take into account if existing extents
      // can be merged with them (uncompressed case)
      if (!b->get_blob().is_compressed()) {
	if (blob_info_counted && used_alloc_unit == alloc_unit_start) {
	  --blob_info_counted->expected_allocations; // don't need to allocate
	  // new AU for compressed
	  // data since another
	  // collocated uncompressed
	  // blob already exists
	  dout(30) << __func__ << " --expected:"
	    << alloc_unit_start << dendl;
	}
	used_alloc_unit = alloc_unit_end;
	blob_info_counted = nullptr;
      }
    } else if (b->get_blob().is_compressed()) {

      // additionally we take compressed blobs that were not impacted
      // by the write into account too
      BlobInfo& bi =
	affected_blobs.emplace(
	  b, BlobInfo(b->get_referenced_bytes())).first->second;

      int adjust =
	(used_alloc_unit && used_alloc_unit == alloc_unit_start) ? 0 : 1;
      bi.expected_allocations += alloc_unit_end - alloc_unit_start + adjust;
      dout(30) << __func__ << " expected_allocations="
	<< bi.expected_allocations << " end_au:"
	<< alloc_unit_end << dendl;

      blob_info_counted = &bi;
      used_alloc_unit = alloc_unit_end;

      ceph_assert(it->length <= bi.referenced_bytes);
      bi.referenced_bytes -= it->length;
      dout(30) << __func__ << " affected_blob:" << *b
	<< " unref 0x" << std::hex << it->length
	<< " referenced = 0x" << bi.referenced_bytes
	<< std::dec << dendl;
      // NOTE: we can't move specific blob to resulting GC list here
      // when reference counter == 0 since subsequent extents might
      // decrement its expected_allocation.
      // Hence need to enumerate all the extents first.
      if (!bi.collect_candidate) {
	bi.first_lextent = it;
	bi.collect_candidate = true;
      }
      bi.last_lextent = it;
    } else {
      if (blob_info_counted && used_alloc_unit == alloc_unit_start) {
	// don't need to allocate new AU for compressed data since another
	// collocated uncompressed blob already exists
	--blob_info_counted->expected_allocations;
	dout(30) << __func__ << " --expected_allocations:"
	  << alloc_unit_start << dendl;
      }
      used_alloc_unit = alloc_unit_end;
      blob_info_counted = nullptr;
    }
  }

  for (auto b_it = affected_blobs.begin();
    b_it != affected_blobs.end();
    ++b_it) {
    Blob* b = b_it->first;
    BlobInfo& bi = b_it->second;
    if (bi.referenced_bytes == 0) {
      uint64_t len_on_disk = b_it->first->get_blob().get_ondisk_capacity();
      int64_t blob_expected_for_release =
	round_up_to(len_on_disk, min_alloc_size) / min_alloc_size;

      dout(30) << __func__ << " " << *(b_it->first)
	<< " expected4release=" << blob_expected_for_release
	<< " expected_allocations=" << bi.expected_allocations
	<< dendl;
      int64_t benefit = blob_expected_for_release - bi.expected_allocations;
      if (benefit >= g_conf()->bluestore_gc_enable_blob_threshold) {
	if (bi.collect_candidate) {
	  auto it = bi.first_lextent;
	  bool bExit = false;
	  do {
	    if (it->blob.get() == b) {
	      extents_to_collect.insert(it->logical_offset, it->length);
	    }
	    bExit = it == bi.last_lextent;
	    ++it;
	  } while (!bExit);
	}
	expected_for_release += blob_expected_for_release;
	expected_allocations += bi.expected_allocations;
      }
    }
  }
}

int64_t bluestore::GarbageCollector::estimate(
  uint64_t start_offset,
  uint64_t length,
  const bluestore::ExtentMap& extent_map,
  const bluestore::OldExtentMap& old_extents,
  uint64_t min_alloc_size)
{

  affected_blobs.clear();
  extents_to_collect.clear();
  used_alloc_unit = boost::optional<uint64_t >();
  blob_info_counted = nullptr;

  uint64_t gc_start_offset = start_offset;
  uint64_t gc_end_offset = start_offset + length;

  uint64_t end_offset = start_offset + length;

  for (auto it = old_extents.begin(); it != old_extents.end(); ++it) {
    Blob* b = it->e.blob.get();
    if (b->get_blob().is_compressed()) {

      // update gc_start_offset/gc_end_offset if needed
      gc_start_offset = min(gc_start_offset, (uint64_t)it->e.blob_start());
      gc_end_offset = std::max(gc_end_offset, (uint64_t)it->e.blob_end());

      auto o = it->e.logical_offset;
      auto l = it->e.length;

      uint64_t ref_bytes = b->get_referenced_bytes();
      // micro optimization to bypass blobs that have no more references
      if (ref_bytes != 0) {
	dout(30) << __func__ << " affected_blob:" << *b
	  << " unref 0x" << std::hex << o << "~" << l
	  << std::dec << dendl;
	affected_blobs.emplace(b, BlobInfo(ref_bytes));
      }
    }
  }
  dout(30) << __func__ << " gc range(hex): [" << std::hex
    << gc_start_offset << ", " << gc_end_offset
    << ")" << std::dec << dendl;

  // enumerate preceeding extents to check if they reference affected blobs
  if (gc_start_offset < start_offset || gc_end_offset > end_offset) {
    process_protrusive_extents(extent_map,
      gc_start_offset,
      gc_end_offset,
      start_offset,
      end_offset,
      min_alloc_size);
  }
  return expected_for_release - expected_allocations;
}

// TransContext
bool bluestore::TransContext::add_writing(Onode* o, uint32_t off, uint32_t len)
{
  std::lock_guard l(writings_lock);

  // Need to indicate non-initial observers that we're done.
  if (were_writings && writings.empty()) {
    return false;
  }
  writings.emplace_back(o, off, len);
  were_writings = true;
  return true;
}

void bluestore::TransContext::finish_writing()
{
  write_list_t finished;
  {
    std::lock_guard l(writings_lock);
    finished.swap(writings);
  }
  for (auto& e : finished) {
    e.onode->finish_write(this, e.offset, e.length);
  }
}

// WriteContext

bluestore::WriteContext::WriteContext() {
  old_extents = std::make_unique<OldExtentMap>();
}
/// Checks for writes to the same pextent within a blob
bool bluestore::WriteContext::has_conflict(
  BlobRef b,
  uint64_t loffs,
  uint64_t loffs_end,
  uint64_t min_alloc_size)
{
  ceph_assert((loffs % min_alloc_size) == 0);
  ceph_assert((loffs_end % min_alloc_size) == 0);
  for (auto w : writes) {
    if (b == w.b) {
      auto loffs2 = p2align(w.logical_offset, min_alloc_size);
      auto loffs2_end = p2roundup(w.logical_offset + w.length0, min_alloc_size);
      if ((loffs <= loffs2 && loffs_end > loffs2) ||
	(loffs >= loffs2 && loffs < loffs2_end)) {
	return true;
      }
    }
  }
  return false;
}

// DeferredBatch
#undef dout_prefix
#define dout_prefix *_dout << "bluestore.DeferredBatch(" << this << ") "
#undef dout_context
#define dout_context cct

void bluestore::DeferredBatch::prepare_write(
  CephContext* cct,
  uint64_t seq, uint64_t offset, uint64_t length,
  bufferlist::const_iterator& blp)
{
  _discard(cct, offset, length);
  auto i = iomap.insert(std::make_pair(offset, deferred_io()));
  ceph_assert(i.second);  // this should be a new insertion
  i.first->second.seq = seq;
  blp.copy(length, i.first->second.bl);
  i.first->second.bl.reassign_to_mempool(
    mempool::mempool_bluestore_writing_deferred);
  dout(20) << __func__ << " seq " << seq
    << " 0x" << std::hex << offset << "~" << length
    << " crc " << i.first->second.bl.crc32c(-1)
    << std::dec << dendl;
#ifdef DEBUG_DEFERRED
  seq_bytes[seq] += length;
  _audit(cct);
#endif
}

void bluestore::DeferredBatch::_discard(
  CephContext* cct, uint64_t offset, uint64_t length)
{
  generic_dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
    << std::dec << dendl;
  [[maybe_unused]] uint64_t delta;
  auto p = iomap.lower_bound(offset);
  if (p != iomap.begin()) {
    --p;
    auto end = p->first + p->second.bl.length();
    if (end > offset) {
      bufferlist head;
      head.substr_of(p->second.bl, 0, offset - p->first);
      dout(20) << __func__ << "  keep head " << p->second.seq
	<< " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	<< " -> 0x" << head.length() << std::dec << dendl;
      if (end > offset + length) {
	bufferlist tail;
	tail.substr_of(p->second.bl, offset + length - p->first,
	  end - (offset + length));
	dout(20) << __func__ << "  keep tail " << p->second.seq
	  << " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	  << " -> 0x" << tail.length() << std::dec << dendl;
	auto& n = iomap[offset + length];
	n.bl.swap(tail);
	n.seq = p->second.seq;
	delta = length;
      }
      else {
	delta = end - offset;
      }
#if defined(DEBUG_DEFERRED)
      auto i = seq_bytes.find(p->second.seq);
      ceph_assert(i != seq_bytes.end());
      i->second -= delta;
      ceph_assert(i->second >= 0);
#endif
      p->second.bl.swap(head);
    }
    ++p;
  }
  while (p != iomap.end()) {
    if (p->first >= offset + length) {
      break;
    }
    auto end = p->first + p->second.bl.length();
    if (end > offset + length) {
      unsigned drop_front = offset + length - p->first;
      unsigned keep_tail = end - (offset + length);
      dout(20) << __func__ << "  truncate front " << p->second.seq
	<< " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	<< " drop_front 0x" << drop_front << " keep_tail 0x" << keep_tail
	<< " to 0x" << (offset + length) << "~" << keep_tail
	<< std::dec << dendl;
      auto& s = iomap[offset + length];
      s.seq = p->second.seq;
      s.bl.substr_of(p->second.bl, drop_front, keep_tail);
      delta = drop_front;
    }
    else {
      dout(20) << __func__ << "  drop " << p->second.seq
	<< " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	<< std::dec << dendl;
      delta = p->second.bl.length();
    }
#if defined(DEBUG_DEFERRED)
    auto i = seq_bytes.find(p->second.seq);
    ceph_assert(i != seq_bytes.end());
    i->second -= delta;
    ceph_assert(i->second >= 0);
#endif
    p = iomap.erase(p);
  }
}

#if defined(DEBUG_DEFERRED)
void bluestore::DeferredBatch::_audit(CephContext* cct)
{
  map<uint64_t, int> sb;
  for (auto p : seq_bytes) {
    sb[p.first] = 0;  // make sure we have the same set of keys
  }
  uint64_t pos = 0;
  for (auto& p : iomap) {
    ceph_assert(p.first >= pos);
    sb[p.second.seq] += p.second.bl.length();
    pos = p.first + p.second.bl.length();
  }
  ceph_assert(sb == seq_bytes);
}
#endif

//BigDeferredWriteContext
bool bluestore::BigDeferredWriteContext::can_defer(
  bluestore::extent_map_t::iterator ep,
  uint64_t prefer_deferred_size,
  uint64_t block_size,
  uint64_t offset,
  uint64_t l)
{
  bool res = false;
  auto& blob = ep->blob->get_blob();
  if (offset >= ep->blob_start() &&
    blob.is_mutable()) {
    off = offset;
    b_off = offset - ep->blob_start();
    uint64_t chunk_size = blob.get_chunk_size(block_size);
    uint64_t ondisk = blob.get_ondisk_capacity();
    used = std::min(l, ondisk - b_off);

    // will read some data to fill out the chunk?
    head_read = p2phase<uint64_t>(b_off, chunk_size);
    tail_read = p2nphase<uint64_t>(b_off + used, chunk_size);
    b_off -= head_read;

    ceph_assert(b_off % chunk_size == 0);
    ceph_assert(blob_aligned_len() % chunk_size == 0);

    res = blob_aligned_len() < prefer_deferred_size &&
      blob_aligned_len() <= ondisk &&
      blob.is_allocated(b_off, blob_aligned_len());
    if (res) {
      blob_ref = ep->blob;
      blob_start = ep->blob_start();
    }
  }
  return res;
}
bool bluestore::BigDeferredWriteContext::apply_defer()
{
  int r = blob_ref->get_blob().map(
    b_off, blob_aligned_len(),
    [&](const bluestore_pextent_t& pext,
      uint64_t offset,
      uint64_t length) {
	// apply deferred if overwrite breaks blob continuity only.
	// if it totally overlaps some pextent - fallback to regular write
	if (pext.offset < offset ||
	  pext.end() > offset + length) {
	  res_extents.emplace_back(bluestore_pextent_t(offset, length));
	  return 0;
	}
	return -1;
    });
  return r >= 0;
}
