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
#include "BlueStore_objects.h"
#include "os/bluestore/bluestore_types.h"
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

using bid_t = decltype(bluestore::Blob::id);

// Blob

void bluestore::Blob::set_shared_blob(BlueStore::SharedBlobRef sb) {
  ceph_assert((bool)sb);
  ceph_assert(!shared_blob);
  ceph_assert(sb->collection = collection);
  shared_blob = sb;
  ceph_assert(get_cache());
}

bool bluestore::Blob::is_shared_loaded() const {
  return shared_blob && shared_blob->is_loaded();
}

BlueStore::BufferCacheShard* bluestore::Blob::get_cache() {
  return collection ? collection->cache : nullptr;
}

uint64_t bluestore::Blob::get_sbid() const {
  return shared_blob ? shared_blob->get_sbid() : 0;
}

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.blob(" << this << ") "
#undef dout_context
#define dout_context collection->store->cct

bluestore::Blob::~Blob()
{
 again:
  auto coll_cache = get_cache();
  if (coll_cache) {
    std::lock_guard l(coll_cache->lock);
    if (coll_cache != get_cache()) {
      goto again;
    }
    coll_cache->rm_blob();
  }
}

void bluestore::Blob::dump(Formatter* f) const
{
  if (is_spanning()) {
    f->dump_unsigned("spanning_id ", id);
  }
  blob.dump(f);
  if (shared_blob) {
    f->dump_object("shared", *shared_blob);
  }
}

namespace bluestore {
  ostream& operator<<(ostream& out, const bluestore::Blob& b)
  {
    out << "Blob(" << &b;
    if (b.is_spanning()) {
      out << " spanning " << b.id;
    }
    out << " " << b.get_blob() << " " << b.get_blob_use_tracker();
    if (b.shared_blob) {
      out << " " << *b.shared_blob;
    } else {
      out << " (shared_blob=NULL)";
    }
    out << ")";
    return out;
  }
}

void bluestore::Blob::get_ref(
  BlueStore::Collection *coll,
  uint32_t offset,
  uint32_t length)
{
  // Caller has to initialize Blob's logical length prior to increment 
  // references.  Otherwise one is neither unable to determine required
  // amount of counters in case of per-au tracking nor obtain min_release_size
  // for single counter mode.
  ceph_assert_decode(get_blob().get_logical_length() != 0);
  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
           << std::dec << " " << *this << dendl;

  if (used_in_blob.is_empty()) {
    uint32_t min_release_size =
      get_blob().get_release_size(coll->store->get_min_alloc_size());
    uint64_t l = get_blob().get_logical_length();
    dout(20) << __func__ << " init 0x" << std::hex << l << ", "
             << min_release_size << std::dec << dendl;
    used_in_blob.init(l, min_release_size);
  }
  used_in_blob.get(
    offset,
    length);
}

bool bluestore::Blob::put_ref(
  BlueStore::Collection *coll,
  uint32_t offset,
  uint32_t length,
  PExtentVector *r)
{
  PExtentVector logical;

  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
           << std::dec << " " << *this << dendl;
  
  bool empty = used_in_blob.put(
    offset,
    length,
    &logical);
  r->clear();
  // nothing to release
  if (!empty && logical.empty()) {
    return false;
  }

  bluestore_blob_t& b = dirty_blob();
  return b.release_extents(empty, logical, r);
}

bool bluestore::Blob::can_reuse_blob(uint32_t min_alloc_size,
                		     uint32_t target_blob_size,
		                     uint32_t b_offset,
		                     uint32_t *length0) {
  ceph_assert(min_alloc_size);
  ceph_assert(target_blob_size);
  if (!get_blob().is_mutable()) {
    return false;
  }

  uint32_t length = *length0;
  uint32_t end = b_offset + length;

  // Currently for the sake of simplicity we omit blob reuse if data is
  // unaligned with csum chunk. Later we can perform padding if needed.
  if (get_blob().has_csum() &&
     ((b_offset % get_blob().get_csum_chunk_size()) != 0 ||
      (end % get_blob().get_csum_chunk_size()) != 0)) {
    return false;
  }

  auto blen = get_blob().get_logical_length();
  uint32_t new_blen = blen;

  // make sure target_blob_size isn't less than current blob len
  target_blob_size = std::max(blen, target_blob_size);

  if (b_offset >= blen) {
    // new data totally stands out of the existing blob
    new_blen = end;
  } else {
    // new data overlaps with the existing blob
    new_blen = std::max(blen, end);

    uint32_t overlap = 0;
    if (new_blen > blen) {
      overlap = blen - b_offset;
    } else {
      overlap = length;
    }

    if (!get_blob().is_unallocated(b_offset, overlap)) {
      // abort if any piece of the overlap has already been allocated
      return false;
    }
  }

  if (new_blen > blen) {
    int64_t overflow = int64_t(new_blen) - target_blob_size;
    // Unable to decrease the provided length to fit into max_blob_size
    if (overflow >= length) {
      return false;
    }

    // FIXME: in some cases we could reduce unused resolution
    if (get_blob().has_unused()) {
      return false;
    }

    if (overflow > 0) {
      new_blen -= overflow;
      length -= overflow;
      *length0 = length;
    }

    if (new_blen > blen) {
      ceph_assert(dirty_blob().is_mutable());
      dirty_blob().add_tail(new_blen);
      used_in_blob.add_tail(new_blen,
                            get_blob().get_release_size(min_alloc_size));
    }
  }
  return true;
}

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.blob(" << this << ") "
#undef dout_context
#define dout_context cct

void bluestore::Blob::dup(const Blob& from, bool copy_used_in_blob)
{
  set_shared_blob(from.shared_blob);
  blob.dup(from.blob);
  if (copy_used_in_blob) {
    used_in_blob = from.used_in_blob;
  } else {
    ceph_assert(from.blob.is_compressed());
    ceph_assert(from.used_in_blob.num_au <= 1);
    used_in_blob.init(from.used_in_blob.au_size, from.used_in_blob.au_size);
  }
  for (auto p : blob.get_extents()) {
    if (p.is_valid()) {
      get_dirty_shared_blob()->get_ref(p.offset, p.length);
    }
  }
}

// copies part of a Blob
// it is used to create a consistent blob out of parts of other blobs
void bluestore::Blob::copy_from(
  CephContext* cct, const Blob& from, uint32_t min_release_size, uint32_t start, uint32_t len)
{
  dout(20) << __func__ << " to=" << *this << " from=" << from
	   << " [" << std::hex << start << "~" << len
	   << "] min_release=" << min_release_size << std::dec << dendl;

  auto& bto = blob;
  auto& bfrom = from.blob;
  ceph_assert(!bfrom.is_compressed()); // not suitable for compressed (immutable) blobs
  ceph_assert(!bfrom.has_unused());
  // below to asserts are not required to make function work
  // they check if it is run in desired context
  ceph_assert(bfrom.is_shared());
  ceph_assert(shared_blob);
  ceph_assert(shared_blob == from.shared_blob);

  // split len to pre_len, main_len, post_len
  uint32_t start_aligned = p2align(start, min_release_size);
  uint32_t start_roundup = p2roundup(start, min_release_size);
  uint32_t end_aligned = p2align(start + len, min_release_size);
  uint32_t end_roundup = p2roundup(start + len, min_release_size);
  dout(25) << __func__ << " extent split:"
	   << std::hex << start_aligned << "~" << start_roundup << "~"
	   << end_aligned << "~" << end_roundup << std::dec << dendl;

  if (bto.get_logical_length() == 0) {
    // this is initialization
    bto.adjust_to(from.blob, end_roundup);
    ceph_assert(min_release_size == from.used_in_blob.au_size);
    used_in_blob.init(end_roundup, min_release_size);
  } else if (bto.get_logical_length() < end_roundup) {
    ceph_assert(!bto.is_compressed());
    bto.add_tail(end_roundup);
    used_in_blob.add_tail(end_roundup, used_in_blob.au_size);
  }

  if (end_aligned >= start_roundup) {
    copy_extents(cct, from, start_aligned,
		 start_roundup - start_aligned,/*pre_len*/
		 end_aligned - start_roundup,/*main_len*/
		 end_roundup - end_aligned/*post_len*/);
  } else {
    // it is uncommon case that <start, start + len) in single allocation unit
    copy_extents(cct, from, start_aligned,
		 start_roundup - start_aligned,/*pre_len*/
		 0 /*main_len*/, 0/*post_len*/);
  }
  // copy relevant csum items
  if (bto.has_csum()) {
    size_t csd_value_size = bto.get_csum_value_size();
    size_t csd_item_start = p2align(start, uint32_t(1 << bto.csum_chunk_order)) >> bto.csum_chunk_order;
    size_t csd_item_end = p2roundup(start + len, uint32_t(1 << bto.csum_chunk_order)) >> bto.csum_chunk_order;
    ceph_assert(bto.  csum_data.length() >= csd_item_end * csd_value_size);
    ceph_assert(bfrom.csum_data.length() >= csd_item_end * csd_value_size);
    memcpy(bto.  csum_data.c_str() + csd_item_start * csd_value_size,
	   bfrom.csum_data.c_str() + csd_item_start * csd_value_size,
	   (csd_item_end - csd_item_start) * csd_value_size);
  }
  used_in_blob.get(start, len);
  dout(20) << __func__ << " result=" << *this << dendl;
}

void bluestore::Blob::copy_extents(
  CephContext* cct, const Blob& from, uint32_t start,
  uint32_t pre_len, uint32_t main_len, uint32_t post_len)
{
  // There are 2 valid states:
  // 1) `to` is not defined on [pos~len] range
  //    (need to copy this region - return true)
  // 2) `from` and `to` are exact on [pos~len] range
  //    (no need to copy region - return false)
  // Otherwise just assert.
  auto check_sane_need_copy = [&](
    const PExtentVector& from,
    const PExtentVector& to,
    uint32_t pos, uint32_t len) -> bool
  {
    uint32_t pto = pos;
    auto ito = to.begin();
    while (ito != to.end() && pto >= ito->length) {
      pto -= ito->length;
      ++ito;
    }
    if (ito == to.end()) return true; // case 1 - obviously empty
    if (!ito->is_valid()) {
      // now sanity check that all the rest is invalid too
      pto += len;
      while (ito != to.end() && pto >= ito->length) {
        ceph_assert(!ito->is_valid());
        pto -= ito->length;
        ++ito;
      }
      return true;
    }
    uint32_t pfrom = pos;
    auto ifrom = from.begin();
    while (ifrom != from.end() && pfrom >= ifrom->length) {
      pfrom -= ifrom->length;
      ++ifrom;
    }
    ceph_assert(ifrom != from.end());
    ceph_assert(ifrom->is_valid());
    // here we require from and to be the same
    while (len > 0) {
      ceph_assert(ifrom->offset + pfrom == ito->offset + pto);
      uint32_t jump = std::min(len, ifrom->length - pfrom);
      jump = std::min(jump, ito->length - pto);
      pfrom += jump;
      if (pfrom == ifrom->length) {
        pfrom = 0;
        ++ifrom;
      }
      pto += jump;
      if (pto == ito->length) {
        pto = 0;
        ++ito;
      }
      len -= jump;
    }
    return false;
  };
  const PExtentVector& exfrom = from.blob.get_extents();
  PExtentVector& exto = blob.dirty_extents();
  dout(20) << __func__ << " 0x" << std::hex << start << " "
	   << pre_len << "/" << main_len << "/" << post_len << std::dec << dendl;

  // the extents that cover same area must be the same
  if (pre_len > 0) {
    if (check_sane_need_copy(exfrom, exto, start, pre_len)) {
      main_len += pre_len; // also copy pre_len
    } else {
      start += pre_len; // skip, already there
    }
  }
  if (post_len > 0) {
    if (check_sane_need_copy(exfrom, exto, start + main_len, post_len)) {
      main_len += post_len; // also copy post_len
    } else {
      // skip, already there
    }
  }
  // it is possible that here is nothing to copy
  if (main_len > 0) {
    copy_extents_over_empty(cct, from, start, main_len);
  }
}

// assumes that target (this->extents) has hole in relevant location
void bluestore::Blob::copy_extents_over_empty(
  CephContext* cct, const Blob& from, uint32_t start, uint32_t len)
{
  dout(20) << __func__ << " to=" << *this << " from=" << from
	   << "[0x" << std::hex << start << "~" << len << std::dec << "]" << dendl;
  uint32_t padding;
  auto& exto = blob.dirty_extents();
  auto ito = exto.begin();
  PExtentVector::iterator prev = exto.end();
  uint32_t sto = start;

  auto try_append = [&](PExtentVector::iterator& it, uint64_t disk_offset, uint32_t disk_len) {
    if (prev != exto.end()) {
      if (prev->is_valid()) {
	if (prev->offset + prev->length == disk_offset) {
	  get_dirty_shared_blob()->get_ref(disk_offset, disk_len);
	  prev->length += disk_len;
	  return;
	}
      }
    }
    it = exto.insert(it, bluestore_pextent_t(disk_offset, disk_len));
    prev = it;
    ++it;
    get_dirty_shared_blob()->get_ref(disk_offset, disk_len);
  };

  while (ito != exto.end() && sto >= ito->length) {
    sto -= ito->length;
    prev = ito;
    ++ito;
  }
  if (ito == exto.end()) {
    // putting data after end, just expand / push back
    if (sto > 0) {
      exto.emplace_back(bluestore_pextent_t::INVALID_OFFSET, sto);
      ito = exto.end();
      prev = ito;
    }
    padding = 0;
  } else {
    ceph_assert(!ito->is_valid()); // there can be no collision
    ceph_assert(ito->length >= sto + len); // for at least len, starting with remainder sto
    padding = ito->length - (sto + len); // add this much after copying
    ito = exto.erase(ito); // cut a hole
    if (sto > 0) {
      ito = exto.insert(ito, bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, sto));
      prev = ito;
      ++ito;
    }
  }

  const auto& exfrom = from.blob.get_extents();
  auto itf = exfrom.begin();
  uint32_t sf = start;
  while (itf != exfrom.end() && sf >= itf->length) {
    sf -= itf->length;
    ++itf;
  }

  uint32_t skip_on_first = sf;
  while (itf != exfrom.end() && len > 0) {
    ceph_assert(itf->is_valid());
    uint32_t to_copy = std::min<uint32_t>(itf->length - skip_on_first, len);
    try_append(ito, itf->offset + skip_on_first, to_copy);
    len -= to_copy;
    skip_on_first = 0;
    ++itf;
  }
  ceph_assert(len == 0);

  if (padding > 0) {
    exto.insert(ito, bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, padding));
  }
  dout(20) << __func__ << " result=" << *this << dendl;
}

// Checks if two Blobs can be joined together.
// The important (unchecked) condition is that both Blobs belong to the same object.
// Verifies if 'other' Blob can be deleted but its content moved to 'this' Blob.
// Requirements:
// 1) checksums: same type and size
// 2) tracker: same au size
// 3) extents: must be disjointed
// 4) unused: ignored, will be cleared
//
// Returns:
// false - Blobs are incompatible
// true - Blobs can be merged
//
// Returned blob_width is a distance between 'other' Blob's blob_start() and last logical_offset
// that can refer to 'other' Blob extents. It is used to limit iteration on ExtentMap.
bool bluestore::Blob::can_merge_blob(const Blob* other, uint32_t& blob_width) const
{
  const Blob* x = other;
  const Blob* y = this;
  // checksums
  const bluestore_blob_t& xb = x->get_blob();
  const bluestore_blob_t& yb = y->get_blob();
  if (xb.has_csum() != yb.has_csum()) return false;
  if (xb.has_csum()) {
    if (xb.csum_type != yb.csum_type) return false;
    if (xb.csum_chunk_order != yb.csum_chunk_order) return false;
  }
  // trackers
  const bluestore_blob_use_tracker_t& xtr = x->get_blob_use_tracker();
  const bluestore_blob_use_tracker_t& ytr = y->get_blob_use_tracker();
  if (xtr.au_size != ytr.au_size) return false;
  // unused
  // ignore unused, we will clear it up anyway
  // extents
  // the success is when there is no offset that is used by both blobs
  auto skip_empty = [&](const PExtentVector& list, PExtentVector::const_iterator& it, uint32_t& pos) {
    while (it != list.end() && !it->is_valid()) {
      pos += it->length;
      ++it;
    }
  };
  bool can_merge = true;
  const PExtentVector& xe = x->get_blob().get_extents();
  const PExtentVector& ye = y->get_blob().get_extents();
  PExtentVector::const_iterator xi = xe.begin();
  PExtentVector::const_iterator yi = ye.begin();
  uint32_t xp = 0;
  uint32_t yp = 0;

  skip_empty(xe, xi, xp);
  skip_empty(ye, yi, yp);

  while (xi != xe.end() && yi != ye.end()) {
    if (xp <= yp) {
      if (yp < xp + xi->length) {
	// collision
	can_merge = false;
	break;
      }
      xp += xi->length;
      ++xi;
      skip_empty(xe, xi, xp);
    } else {
      if (xp < yp + yi->length) {
	// collision
	can_merge = false;
	break;
      }
      yp += yi->length;
      ++yi;
      skip_empty(ye, yi, yp);
    }
  }
  if (can_merge) {
    // scan remaining extents in x
    while (xi != xe.end()) {
      xp += xi->length;
      ++xi;
    }
    blob_width = xp;
  }
  return can_merge;
}

// Merges 2 blobs together. Move extents, csum, tracker from src to dst.
uint32_t bluestore::Blob::merge_blob(CephContext* cct, Blob* blob_to_dissolve)
{
  Blob* dst = this;
  Blob* src = blob_to_dissolve;
  const bluestore_blob_t& src_blob = src->get_blob();
  bluestore_blob_t& dst_blob = dst->dirty_blob();
  dout(20) << __func__ << " to=" << *dst << " from" << *src << dendl;

  // drop unused, do not recalc it, unlikely those chunks could be used in future
  dst_blob.clear_flag(bluestore_blob_t::FLAG_HAS_UNUSED);
  if (dst_blob.get_logical_length() < src_blob.get_logical_length()) {
    // expand to accomodate
    ceph_assert(!dst_blob.is_compressed());
    dst_blob.add_tail(src_blob.get_logical_length());
    used_in_blob.add_tail(src_blob.get_logical_length(), used_in_blob.au_size);
  }
  const PExtentVector& src_extents = src_blob.get_extents();
  const PExtentVector& dst_extents = dst_blob.get_extents();
  PExtentVector tmp_extents;
  tmp_extents.reserve(src_extents.size() + dst_extents.size());

  uint32_t csum_chunk_order = src_blob.csum_chunk_order;
  uint32_t csum_value_size = 0;
  const char* src_csum_ptr = nullptr;
  char* dst_csum_ptr = nullptr;
  if (src_blob.has_csum()) {
    ceph_assert(src_blob.csum_type == dst_blob.csum_type);
    ceph_assert(src_blob.csum_chunk_order == dst_blob.csum_chunk_order);
    csum_value_size = src_blob.get_csum_value_size();
    src_csum_ptr = src_blob.csum_data.c_str();
    dst_csum_ptr = dst_blob.csum_data.c_str();
  }
  const bluestore_blob_use_tracker_t& src_tracker = src->get_blob_use_tracker();
  bluestore_blob_use_tracker_t& dst_tracker = dst->dirty_blob_use_tracker();
  ceph_assert(src_tracker.au_size == dst_tracker.au_size);
  uint32_t tracker_au_size = src_tracker.au_size;
  const uint32_t* src_tracker_aus = src_tracker.get_au_array();
  uint32_t* dst_tracker_aus = dst_tracker.dirty_au_array();

  auto skip_empty = [&](const PExtentVector& list, PExtentVector::const_iterator& it, uint32_t& pos) {
    while (it != list.end()) {
      if (it->is_valid()) {
	return;
      }
      pos += it->length;
      ++it;
    }
    pos = std::numeric_limits<uint32_t>::max();
    return;
  };

  auto move_data = [&](uint32_t pos, uint32_t len) {
    if (src_blob.has_csum()) {
      // copy csum
      ceph_assert((pos % (1 << csum_chunk_order)) == 0);
      ceph_assert((len % (1 << csum_chunk_order)) == 0);
      uint32_t start = p2align(pos, uint32_t(1 << csum_chunk_order));
      uint32_t end = p2roundup(pos + len, uint32_t(1 << csum_chunk_order));
      uint32_t item_no = start >> csum_chunk_order;
      uint32_t item_cnt = (end - start) >> csum_chunk_order;
      ceph_assert(dst_blob.csum_data.length() >= (item_no + item_cnt) * csum_value_size);
      memcpy(dst_csum_ptr + item_no * csum_value_size,
	     src_csum_ptr + item_no * csum_value_size,
	     item_cnt * csum_value_size);
    }
    uint32_t start = p2align(pos, tracker_au_size) / tracker_au_size;
    uint32_t end = p2roundup(pos + len, tracker_au_size) / tracker_au_size;
    for (uint32_t i = start; i < end; i++) {
      ceph_assert(i < dst_tracker.get_num_au());
      dst_tracker_aus[i] += src_tracker_aus[i];
    }
  };

  // Main loop creates new PExtentVector by merging src and dst PExtentVectors.
  // It will replace dst's PExtentVector.
  // When we process extent from dst, csum and tracer data is already in place.
  // When we process extent from src, we need to copy csum and tracer to dst.

  uint32_t src_pos = 0; //offset of next non-empty extent
  uint32_t dst_pos = 0;
  uint32_t pos = 0; //already processed amount
  auto src_it = src_extents.begin(); // iterator to next non-empty extent
  auto dst_it = dst_extents.begin();

  skip_empty(src_extents, src_it, src_pos);
  skip_empty(dst_extents, dst_it, dst_pos);
  while (src_it != src_extents.end() || dst_it != dst_extents.end()) {
    if (src_pos > pos) {
      if (dst_pos > pos) {
	// empty space
	uint32_t m = std::min(src_pos - pos, dst_pos - pos);
	// emit empty
	tmp_extents.emplace_back(bluestore_pextent_t::INVALID_OFFSET, m);
	pos += m;
      } else {
	// copy from dst, src must not have conflicting extent
	ceph_assert(src_pos >= dst_pos + dst_it->length);
	// use extent from destination
	tmp_extents.push_back(*dst_it);
	dst_pos += dst_it->length;
	pos = dst_pos;
	++dst_it;
	skip_empty(dst_extents, dst_it, dst_pos);
      }
    } else {
      // copy from src, dst must not have conflicting extent
      ceph_assert(dst_pos >= src_pos + src_it->length);
      // use extent from source
      tmp_extents.push_back(*src_it);
      // copy blob data
      move_data(src_pos, src_it->length);
      src_pos += src_it->length;
      pos = src_pos;
      ++src_it;
      skip_empty(src_extents, src_it, src_pos);
    }
  }
  if (pos < dst_blob.get_logical_length()) {
    // this is a candidate for improvement;
    // instead of artifically add extents, trim blob
    tmp_extents.emplace_back(bluestore_pextent_t::INVALID_OFFSET, dst_blob.get_logical_length() - pos);
  }
  // now apply freshly merged tmp_extents into dst blob
  dst_blob.dirty_extents().swap(tmp_extents);

  dout(20) << __func__ << " result=" << *dst << dendl;
  return dst_blob.get_logical_length();
}

#undef dout_context
#define dout_context collection->store->cct

void bluestore::Blob::split(BlueStore::Collection *coll, uint32_t blob_offset, Blob *r)
{
  dout(10) << __func__ << " 0x" << std::hex << blob_offset << std::dec
	   << " start " << *this << dendl;
  ceph_assert(r);
  ceph_assert(blob.can_split());
  ceph_assert(used_in_blob.can_split());
  bluestore_blob_t &lb = dirty_blob();
  bluestore_blob_t &rb = r->dirty_blob();

  used_in_blob.split(
    blob_offset,
    &(r->used_in_blob));

  lb.split(blob_offset, rb);

  maybe_prune_tail(); // we might get tail-to-prune after splitting
  r->maybe_prune_tail(); // likely redundant (as we tend to prune original blob beforehand)
                         // but let it be

  dout(10) << __func__ << " 0x" << std::hex << blob_offset << std::dec
	   << " finish " << *this << dendl;
  dout(10) << __func__ << " 0x" << std::hex << blob_offset << std::dec
	   << "    and " << *r << dendl;
}

void bluestore::Blob::maybe_prune_tail() {
  if (get_blob().can_prune_tail()) {
    dirty_blob().prune_tail();
    used_in_blob.prune_tail(get_blob().get_ondisk_capacity());
    dout(20) << __func__ << " pruned tail, now " << get_blob() << dendl;
  }
}

template <bool decode_csum>
void bluestore::Blob::decode(
  ceph::buffer::ptr::const_iterator& p,
  uint64_t struct_v,
  uint64_t* sbid,
  bool include_ref_map,
  BlueStore::Collection *coll) {
  if constexpr (decode_csum)
    blob.decode<true>(p, struct_v);
  else
    blob.decode<false>(p, struct_v);
  if (blob.is_shared()) {
    denc(*sbid, p);
  }
  if (include_ref_map) {
    if (struct_v > 1) {
      used_in_blob.decode(p);
    } else {
      used_in_blob.clear();
      bluestore_extent_ref_map_t legacy_ref_map;
      legacy_ref_map.decode(p);
      if (coll) {
        for (const auto& r : legacy_ref_map.ref_map) {
          get_ref(coll, r.first, r.second.refs * r.second.length);
        }
      }
    }
  }
}

template void bluestore::Blob::decode<true>(
  ceph::buffer::ptr::const_iterator&,
  uint64_t,
  uint64_t*,
  bool,
  BlueStore::Collection*);

template void bluestore::Blob::decode<false>(
  ceph::buffer::ptr::const_iterator&,
  uint64_t,
  uint64_t*,
  bool,
  BlueStore::Collection*);

// Onode
//
// Mapping blobs over Onode's logical offsets.
//
// Blob is always continous. Blobs may overlap.
// Non-mapped regions are "0" when read.
//                 1               2               3
// 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
// <blob.a.blob.a><blob.b.blo>        <blob.c.blob.c.blob.c.blob>
//       <blob.d.blob.d.b>                      <blob.e.blob.e>
// blob.a starts at 0x0 length 0xe
// blob.b starts at 0xf length 0xb
// blob.c starts at 0x23 length 0x1b
// blob.d starts at 0x06 length 0x12
// blob.e starts at 0x2d length 0xf
//
// Blobs can have non-encoded parts:
//                 1               2               3
// 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
// aaaaaa......aaabbbbb...bbbb        ccccccccccccccc..........cc
//       dddddd........ddd                      .....eeeeeeeeee
// "." - non-encoded parts of blob (holes)
//
// Mapping logical to blob:
// extent_map maps {Onode's logical offset, length}=>{Blob, in-blob offset}
// {0x0, 0x6}=>{blob.a, 0x0}
// {0x6, 0x6}=>{blob.d, 0x0}
// {0xc, 0x3}=>{blob.a, 0xc}
// {0xf, 0x5}=>{blob.b, 0x0}
// {0x14, 0x3}=>{blob.d, 0xe}
// {0x17, 0x4}=>{blob.b, 0x8}
// a hole here
// {0x23, 0xe}=>{blob.c, 0x0}
// and so on...
//
// Compressed blobs do not have non-encoded parts.
// Same example as above but all blobs are compressed:
//                 1               2               3
// 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
// aaaaaaAAAAAAaaabbbbbBBBbbbb        cccccccccccccccCCCCCCCCCCcc
//       ddddddDDDDDDDDddd                      EEEEEeeeeeeeeee
// A-E: parts of blobs that are never used.
// This can happen when a compressed blob is overwritten partially.
// The target ranges are no longer used, but are left there because they are necessary
// for successful decompression.
//
// In compressed blobs PExtentVector and csum refer to actually occupied disk space.
// Blob's logical length is larger then occupied disk space.
// Mapping from extent_map always uses offsets of decompressed data.

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.onode(" << this << ")." << __func__ << " "

bluestore::Onode::Onode(BlueStore::Collection *c, const ghobject_t& o,
  const mempool::bluestore_cache_meta::string& k) 
  : c(c),
	  oid(o),
	  key(k),
	  exists(false),
    cached(false),
	  extent_map(this,
      c->store->cct->_conf->
      bluestore_extent_map_inline_shard_prealloc_size),
    bc(*this) {
}
bluestore::Onode::Onode(CephContext* cct)
  : c(nullptr),
    exists(false),
    cached(false),
    extent_map(this,
      cct->_conf->
      bluestore_extent_map_inline_shard_prealloc_size),
    bc(*this) {
}

bluestore::Onode::~Onode() {
  if (c) {
    std::lock_guard l(c->cache->lock);
    bc._clear(c->cache);
    if (prev_spanning_cnt > 0) {
      c->store->logger->dec(l_bluestore_spanning_blobs, prev_spanning_cnt);
    }
  }
}

const std::string& bluestore::Onode::calc_omap_prefix(uint8_t flags)
{
  if (bluestore_onode_t::is_pgmeta_omap(flags)) {
    return PREFIX_PGMETA_OMAP;
  }
  if (bluestore_onode_t::is_perpg_omap(flags)) {
    return PREFIX_PERPG_OMAP;
  }
  if (bluestore_onode_t::is_perpool_omap(flags)) {
    return PREFIX_PERPOOL_OMAP;
  }
  return PREFIX_OMAP;
}

// '-' < '.' < '~'
void bluestore::Onode::calc_omap_header(
  uint8_t flags,
  const Onode* o,
  std::string* out)
{
  if (!bluestore_onode_t::is_pgmeta_omap(flags)) {
    if (bluestore_onode_t::is_perpg_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
      _key_encode_u32(o->oid.hobj.get_bitwise_key_u32(), out);
    } else if (bluestore_onode_t::is_perpool_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
    }
  }
  _key_encode_u64(o->onode.nid, out);
  out->push_back('-');
}

void bluestore::Onode::calc_omap_key(uint8_t flags,
				    const Onode* o,
				    const std::string& key,
				    std::string* out)
{
  if (!bluestore_onode_t::is_pgmeta_omap(flags)) {
    if (bluestore_onode_t::is_perpg_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
      _key_encode_u32(o->oid.hobj.get_bitwise_key_u32(), out);
    } else if (bluestore_onode_t::is_perpool_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
    }
  }
  _key_encode_u64(o->onode.nid, out);
  out->push_back('.');
  out->append(key);
}

void bluestore::Onode::calc_omap_tail(
  uint8_t flags,
  const Onode* o,
  std::string* out)
{
  if (!bluestore_onode_t::is_pgmeta_omap(flags)) {
    if (bluestore_onode_t::is_perpg_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
      _key_encode_u32(o->oid.hobj.get_bitwise_key_u32(), out);
    } else if (bluestore_onode_t::is_perpool_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
    }
  }
  _key_encode_u64(o->onode.nid, out);
  out->push_back('~');
}

void bluestore::Onode::get()
{
  ++nref;
  ++pin_nref;
}
void bluestore::Onode::put()
{
  if (--pin_nref == 1) {
    c->get_onode_cache()->maybe_unpin(this);
  }
  if (--nref == 0) {
    BLUE_SCOPE(onode_put);
    delete this;
  }
}

void bluestore::Onode::decode_raw(
  BlueStore::Onode* on,
  const bufferlist& v,
  BlueStore::ExtentMap::ExtentDecoder& edecoder,
  bool use_onode_segmentation)
{
  on->exists = true;
  auto p = v.front().begin_deep();
  on->onode.decode(p, use_onode_segmentation ? 0 : bluestore_onode_t::FLAG_DEBUG_FORCE_V2);

  // initialize extent_map
  edecoder.decode_spanning_blobs(p, on->c);
  ceph_assert(on->prev_spanning_cnt == 0);
  if (on->c) {
    on->prev_spanning_cnt = on->extent_map.spanning_blob_map.size();
    if (on->prev_spanning_cnt != 0) {
      on->c->store->logger->inc(l_bluestore_spanning_blobs, on->prev_spanning_cnt);
    }
  }
  if (on->onode.extent_map_shards.empty()) {
    denc(on->extent_map.inline_bl, p);
    edecoder.decode_some(on->extent_map.inline_bl, on->c);
  }
}

bluestore::Onode* bluestore::Onode::create_decode(
  BlueStore::CollectionRef c,
  const ghobject_t& oid,
  const string& key,
  const bufferlist& v,
  bool allow_empty,
  bool use_onode_segmentation)
{
  ceph_assert(v.length() || allow_empty);
  auto on = std::unique_ptr<Onode>(
    new Onode(c.get(), oid, (const mempool::bluestore_cache_meta::string)(key)));

  if (v.length()) {
    BlueStore::ExtentMap::ExtentDecoderFull edecoder(on->extent_map);
    decode_raw(on.get(), v, edecoder, use_onode_segmentation);

    for (auto& i : on->onode.attrs) {
      i.second.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
    }

    // initialize extent_map
    if (on->onode.extent_map_shards.empty()) {
      on->extent_map.inline_bl.reassign_to_mempool(
        mempool::mempool_bluestore_cache_data);
    } else {
      on->extent_map.init_shards(false, false);
    }
  } else {
    // init segment_size
    uint32_t segment_size = c->store->segment_size.load();
    if (segment_size != 0 &&
        c->comp_max_blob_size.has_value() &&
        segment_size < c->comp_max_blob_size.value()) {
      segment_size = c->comp_max_blob_size.value(); // compression larger than global segment_size, use it
    }
    on->onode.segment_size = segment_size;
  }
  return on.release();
}

void bluestore::Onode::flush()
{
  if (flushing_count.load()) {
    ldout(c->store->cct, 20) << __func__ << " cnt:" << flushing_count << dendl;
    waiting_count++;
    std::unique_lock l(flush_lock);
    while (flushing_count.load()) {
      flush_cond.wait(l);
    }
    waiting_count--;
  }
  ldout(c->store->cct, 20) << __func__ << " done" << dendl;
}

void bluestore::Onode::dump(Formatter* f) const
{
  onode.dump(f);
  extent_map.dump(f);
}

void bluestore::Onode::rewrite_omap_key(const string& old, string *out)
{
  if (!onode.is_pgmeta_omap()) {
    if (onode.is_perpg_omap()) {
      _key_encode_u64(c->pool(), out);
      _key_encode_u32(oid.hobj.get_bitwise_key_u32(), out);
    } else if (onode.is_perpool_omap()) {
      _key_encode_u64(c->pool(), out);
    }
  }
  _key_encode_u64(onode.nid, out);
  out->append(old.c_str() + out->length(), old.size() - out->length());
}

size_t bluestore::Onode::calc_userkey_offset_in_omap_key() const
{
  size_t pos = sizeof(uint64_t) + 1;
  if (!onode.is_pgmeta_omap()) {
    if (onode.is_perpg_omap()) {
      pos += sizeof(uint64_t) + sizeof(uint32_t);
    } else if (onode.is_perpool_omap()) {
      pos += sizeof(uint64_t);
    }
  }
  return pos;
}

void bluestore::Onode::decode_omap_key(const string& key, string *user_key)
{
  *user_key = key.substr(calc_userkey_offset_in_omap_key());
}

void bluestore::Onode::finish_write(BlueStore::TransContext* txc, uint32_t offset, uint32_t length)
{
  while (true) {
    BlueStore::BufferCacheShard *cache = c->cache;
    std::lock_guard l(cache->lock);
    if (cache != c->cache) {
      ldout(cache->cct, 20) << __func__
	       << " raced with sb cache update, was " << cache
	       << ", now " << c->cache << ", retrying"
	       << dendl;
      continue;
    }
    ldout(c->store->cct, 10) << __func__ << " txc " << txc << std::hex
                             << " 0x" << offset << "~" << length << std::dec
                             << dendl;
    bc._finish_write(cache, txc, offset, length);
    break;
  }
  ldout(c->store->cct, 10) << __func__ << " done " << txc << dendl;
}

int bluestore::Onode::get_fragmentation_score()
{
  FragMetric frag;

  std::unordered_set<BlueStore::BlobRef> visited_compressed_blobs;

  for (const auto& e : extent_map.extent_map) {
    if (e.blob->get_blob().is_compressed()) {
      if (visited_compressed_blobs.insert(e.blob).second) {
        e.blob->get_blob().map(
          0, e.blob->get_blob().get_ondisk_size(),
          [&](uint64_t offset, uint64_t length) {
            frag.note(offset, length);
            return 0;
          }
        );
      }
    } else {
      e.blob->get_blob().map(
        e.blob_offset,
        e.length,
        [&](uint64_t phys_offset, uint64_t len) {
          frag.note(phys_offset, len);
          return 0;
        }
      );
    }
  }
  return frag.frag_score;
}

// ExtentMap

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.extentmap(" << this << ") "
#undef dout_context
#define dout_context onode->c->store->cct

bluestore::ExtentMap::ExtentMap(Onode *o, size_t inline_shard_prealloc_size)
  : onode(o),
    inline_bl(inline_shard_prealloc_size) {
}

void bluestore::ExtentMap::dump(Formatter* f) const
{
  f->open_array_section("extents");

  for (auto& e : extent_map) {
      f->dump_object("extent", e);
  }
  f->close_section();
}

void bluestore::ExtentMap::scan_shared_blobs(
  uint64_t start, uint64_t length,
  std::multimap<uint64_t /*blob.logical_offset*/, Blob*>& candidates)
{
  BlueStore::Collection* c = onode->c;
  uint64_t end = start + length;
  // last_encoded_id will be used to process each blob only once
  // so reset them first
  auto ep_start = seek_lextent(start);
  for (auto ep = ep_start; ep != extent_map.end(); ++ep) {
    // ep->logical_offset and ep->blob_start() are different
    // ep->blob_start() allows us to include blobs that do have some empty space in the beginning
    if (ep->blob_start() >= end) {
      break;
    }
    ep->blob->last_encoded_id = -1;
  }

  // reuse, extent_map could not change
  for (auto ep = ep_start; ep != extent_map.end(); ++ep) {
    if (ep->blob_start() >= end) {
      break;
    }
    if (ep->blob->last_encoded_id == -1) {
      const bluestore_blob_t& blob = ep->blob->get_blob();
      if (blob.is_shared()) {
        // excellent time to load the blob
        c->load_shared_blob(ep->blob->get_shared_blob());
        if (!blob.is_compressed()) {
          // Restrict elastic shared blobs to non-compressed blobs.
          // Fsck cannot handle case when one shared blob contains refs to
          // both shared and non-shared blobs.

	  // todo consider change to emplace_hint
	  candidates.emplace(ep->blob_start(), ep->blob.get());
	}
      }
      // mark as processed
      ep->blob->last_encoded_id = 0;
    }
  }
}

bluestore::Blob* bluestore::ExtentMap::find_mergable_companion(
  Blob* blob_to_dissolve, uint32_t blob_start, uint32_t& blob_width,
  std::multimap<uint64_t /*blob_start*/, Blob*>& candidates)
{
  dout(30) << __func__ << std::hex << " blob_start=0x" << blob_start << std::dec << dendl;
  Blob* result = nullptr;
  for (auto it = candidates.find(blob_start);
       it != candidates.end() && it->first == blob_start;
       ++it) {
    dout(30) << __func__ << " trying " << it->second << dendl;
    if (it->second->can_merge_blob(blob_to_dissolve, blob_width)) {
      dout(20) << __func__ << " merging " << blob_to_dissolve << " to " << it->second << dendl;
      result = it->second;
      break;
    }
  }
  return result;
}

void bluestore::ExtentMap::reblob_extents(uint32_t blob_start, uint32_t blob_end,
					  BlueStore::BlobRef from_blob, BlueStore::BlobRef to_blob)
{
  if (from_blob->is_spanning()) {
    // Mark spanning blobs no longer spanning.
    // If needed will be re-spanned again in reshard().
    dout(20) << __func__ << " removing spanning blob" << dendl;
    spanning_blob_map.erase(from_blob->id);
    from_blob->id = -1;
  }
  auto prev = extent_map.end();
  for (auto ep = seek_lextent(blob_start); ep != extent_map.end();) {
    Extent* e = &(*ep);
    if (e->logical_offset > blob_end) break;
    if (e->blob == from_blob) {
      e->blob = to_blob;
    }
    if (prev != extent_map.end()) {
      if (prev->blob == e->blob &&
	  prev->blob_offset + prev->length == e->blob_offset &&
	  prev->logical_offset + prev->length == e->logical_offset) {
	prev->length += e->length;
	ep = extent_map.erase(ep);
	// we have to manually delete Extent, otherwise memory leak
	delete e;
	// prev still the same
	continue;
      }
    }
    prev = ep;
    ++ep;
  }
}

// Convert blobs in selected range to shared blobs.
void bluestore::ExtentMap::make_range_shared_maybe_merge(
  BlueStore::TransContext* txc, BlueStore::OnodeRef& onoderef, uint64_t srcoff, uint64_t length)
{
  ceph_assert(onoderef == onode);
  uint64_t end = srcoff + length;
  uint32_t dirty_range_begin = BlueStore::OBJECT_MAX_SIZE;
  uint32_t dirty_range_end = 0;
  BlueStore::Collection* c = onode->c;
  BlueStore* store = c->store;
  // load entire object; in most cases we clone entire object anyway
  fault_range(store->db, 0, BlueStore::OBJECT_MAX_SIZE);
  std::multimap<uint64_t /*blob_start*/, Blob*> candidates;
  scan_shared_blobs(srcoff, length, candidates);

  for (auto ep = seek_lextent(srcoff);
    ep != extent_map.end(); ) {
    auto& e = *ep;
    if (e.logical_offset >= end) {
      break;
    }
    dout(25) << __func__ << " src " << e << " bc=" << onoderef->bc << dendl;
    const bluestore_blob_t &blob = e.blob->get_blob();
    // make sure it is shared
    if (!blob.is_shared()) {
      dirty_range_begin = std::min<uint32_t>(dirty_range_begin, e.blob_start());
      // first try to find a shared blob nearby
      // that can accomodate extra extents
      uint32_t blob_width; // to signal when extents end
      dout(20) << __func__ << std::hex << " e.blob_start=" << e.blob_start()
               << " e.logical_offset=" << e.logical_offset << std::dec << dendl;
      Blob *b = blob.is_compressed() ? nullptr :
        find_mergable_companion(e.blob.get(), e.blob_start(), blob_width, candidates);
      if (b) {
        dout(20) << __func__ << " merging to: " << *b << " bc=" << onode->bc << dendl;
        uint32_t b_logical_length = b->merge_blob(store->cct, e.blob.get());
        for (auto p : blob.get_extents()) {
          if (p.is_valid()) {
            b->get_dirty_shared_blob()->get_ref(p.offset, p.length);
          }
        }
        // reblob extents might erase e
        dirty_range_end = std::max<uint32_t>(dirty_range_end, e.blob_start() + b_logical_length);
        uint32_t goto_logical_offset = e.logical_offset + e.length;
        reblob_extents(e.blob_start(), e.blob_start() + blob_width,
		       e.blob, b);
        ep = seek_lextent(goto_logical_offset);
        dout(20) << __func__ << " merged: " << *b << dendl;
      } else {
        // no candidate, has to convert to shared
        c->make_blob_shared(store->_assign_blobid(txc), e.blob);
        ceph_assert(e.logical_end() > 0);
        dirty_range_end = std::max<uint32_t>(dirty_range_end, e.logical_end());
        ++ep;
      }
    } else {
      c->load_shared_blob(e.blob->get_shared_blob());
      ++ep;
    }
  }
  if (dirty_range_begin < dirty_range_end) {
    // source onode got modified in the process
    dirty_range(dirty_range_begin, dirty_range_end - dirty_range_begin);
    maybe_reshard(dirty_range_begin, dirty_range_end);
    txc->write_onode(onoderef);
  }
}

void bluestore::ExtentMap::dup(BlueStore* b, BlueStore::TransContext* txc,
  BlueStore::CollectionRef& c, BlueStore::OnodeRef& oldo, BlueStore::OnodeRef& newo, uint64_t& srcoff,
  uint64_t& length, uint64_t& dstoff) {
  //_dup_writing needs cache lock
  BlueStore::BufferCacheShard* bcs = c->cache;
  bcs->lock.lock();
  while(bcs != c->cache) {
    bcs->lock.unlock();
    bcs = c->cache;
    bcs->lock.lock();
  }

  vector<BlueStore::BlobRef> id_to_blob(oldo->extent_map.extent_map.size());
  for (auto& e : oldo->extent_map.extent_map) {
    e.blob->last_encoded_id = -1;
  }

  int n = 0;
  uint64_t end = srcoff + length;
  uint32_t dirty_range_begin = 0;
  uint32_t dirty_range_end = 0;
  bool src_dirty = false;
  for (auto ep = oldo->extent_map.seek_lextent(srcoff);
    ep != oldo->extent_map.extent_map.end();
    ++ep) {
    auto& e = *ep;
    if (e.logical_offset >= end) {
      break;
    }
    dout(20) << __func__ << "  src " << e << dendl;
    BlueStore::BlobRef cb;
    bool blob_duped = true;
    if (e.blob->last_encoded_id >= 0) {
      cb = id_to_blob[e.blob->last_encoded_id];
      blob_duped = false;
    } else {
      // dup the blob
      const bluestore_blob_t& blob = e.blob->get_blob();
      // make sure it is shared
      if (!blob.is_shared()) {
        c->make_blob_shared(b->_assign_blobid(txc), e.blob);
	if (!src_dirty) {
          src_dirty = true;
          dirty_range_begin = e.logical_offset;
	}
        ceph_assert(e.logical_end() > 0);
        // -1 to exclude next potential shard
        dirty_range_end = e.logical_end() - 1;
      } else {
        c->load_shared_blob(e.blob->get_shared_blob());
      }
      cb = c->new_blob();
      e.blob->last_encoded_id = n;
      id_to_blob[n] = cb;
      e.blob->dup(*cb);

      // bump the extent refs on the copied blob's extents
      for (auto p : blob.get_extents()) {
        if (p.is_valid()) {
          e.blob->get_shared_blob()->get_ref(p.offset, p.length);
        }
      }
      txc->write_shared_blob(e.blob->get_shared_blob());
      dout(20) << __func__ << "    new " << *cb << dendl;
    }

    int skip_front, skip_back;
    if (e.logical_offset < srcoff) {
      skip_front = srcoff - e.logical_offset;
    } else {
      skip_front = 0;
    }
    if (e.logical_end() > end) {
      skip_back = e.logical_end() - end;
    } else {
      skip_back = 0;
    }

    Extent* ne = new Extent(e.logical_offset + skip_front + dstoff - srcoff,
      e.blob_offset + skip_front, e.length - skip_front - skip_back, cb);
    newo->extent_map.extent_map.insert(*ne);
    ne->blob->get_ref(c.get(), ne->blob_offset, ne->length);
    // fixme: we may leave parts of new blob unreferenced that could
    // be freed (relative to the shared_blob).
    txc->statfs_delta.stored() += ne->length;
    if (e.blob->get_blob().is_compressed()) {
      txc->statfs_delta.compressed_original() += ne->length;
      if (blob_duped) {
        txc->statfs_delta.compressed() +=
            cb->get_blob().get_compressed_payload_length();
      }
    }
    dout(20) << __func__ << "  dst " << *ne << dendl;
    ++n;
  }
  // By default do not copy buffers to clones, and let them read data by
  // themselves. The exception are 'writing' buffers, which are not yet
  // stable on device.
  oldo->bc._dup_writing(txc, newo->c, newo, dstoff, length);

  if (src_dirty) {
    oldo->extent_map.dirty_range(dirty_range_begin,
      dirty_range_end - dirty_range_begin);
    txc->write_onode(oldo);
  }
  txc->write_onode(newo);

  if (dstoff + length > newo->onode.size) {
    newo->onode.size = dstoff + length;
  }
  newo->extent_map.dirty_range(dstoff, length);
  //_dup_writing needs cache lock
  bcs->lock.unlock();
}

void bluestore::ExtentMap::dup_esb(BlueStore* b, BlueStore::TransContext* txc,
  BlueStore::CollectionRef& c, BlueStore::OnodeRef& oldo, BlueStore::OnodeRef& newo, uint64_t& srcoff,
  uint64_t& length, uint64_t& dstoff) {
  ceph_assert(onode == oldo);
  ceph_assert(onode->c == c);
  BlueStore::BufferCacheShard* bcs = c->cache;
  bcs->lock.lock();
  while(bcs != c->cache) {
    bcs->lock.unlock();
    bcs = c->cache;
    bcs->lock.lock();
  }

  dout(25) << __func__ << " start oldo=" << dendl;
  _dump_onode<25>(onode->c->store->cct, *oldo);
  dout(25) << __func__ << " start newo=" << dendl;
  _dump_onode<25>(onode->c->store->cct, *newo);

  make_range_shared_maybe_merge(txc, oldo, srcoff, length);
  vector<BlueStore::BlobRef> id_to_blob(extent_map.size());
  for (auto& e : extent_map) {
    e.blob->last_encoded_id = -1;
  }

  int n = 0;
  uint64_t end = srcoff + length;
  uint32_t dirty_range_begin = 0;
  uint32_t dirty_range_end = 0;
  bool src_dirty = false;
  for (auto ep = seek_lextent(srcoff); ep != extent_map.end(); ++ep) {
    auto& e = *ep;
    if (e.logical_offset >= end) {
      break;
    }
    dout(20) << __func__ << "  src " << e << dendl;
    BlueStore::BlobRef cb;
    bool blob_duped = true;
    if (e.blob->last_encoded_id >= 0) {
      cb = id_to_blob[e.blob->last_encoded_id];
      blob_duped = false;
    } else {
      // dup the blob
      const bluestore_blob_t& blob = e.blob->get_blob();
      ceph_assert(blob.is_shared());
      ceph_assert(e.blob->is_shared_loaded());
      ceph_assert(!blob.has_unused());
      cb = c->new_blob();
      e.blob->last_encoded_id = n;
      id_to_blob[n] = cb;
      ceph_assert(ep->blob_start() < end);
      // dup entire blob or dup parts only
      if (blob.is_compressed()) {
	// copy whole blob, but without used_in_blob
	cb->dup(*e.blob, false);
      } else if (e.blob_start() >= srcoff && e.blob_end() <= end) {
	// copy whole blob, including used_in_blob
	cb->dup(*e.blob, true);
      } else {
	// we must copy source blob diligently region-by-region
	// initialize shared_blob
	cb->dirty_blob().set_flag(bluestore_blob_t::FLAG_SHARED);
	cb->set_shared_blob(e.blob->get_shared_blob());
      }

      txc->write_shared_blob(e.blob->get_shared_blob());
      dout(20) << __func__ << "    new " << *cb << dendl;
    }

    int skip_front, skip_back;
    if (e.logical_offset < srcoff) {
      skip_front = srcoff - e.logical_offset;
    } else {
      skip_front = 0;
    }
    if (e.logical_end() > end) {
      skip_back = e.logical_end() - end;
    } else {
      skip_back = 0;
    }

    Extent* ne = new Extent(e.logical_offset + skip_front + dstoff - srcoff,
      e.blob_offset + skip_front, e.length - skip_front - skip_back, cb);
    newo->extent_map.extent_map.insert(*ne);
    if (e.blob->get_blob().is_compressed()) {
      // blob itself was copied, but used_in_blob was not
      cb->get_ref(c.get(), e.blob_offset + skip_front, e.length - skip_front - skip_back);
    } else
      if (e.blob_start() >= srcoff && e.blob_end() <= end) {
      // blob already copied
    } else {
      // copy part
      uint32_t min_release_size = e.blob->get_blob().get_release_size(c->store->min_alloc_size);
      cb->copy_from(b->cct, *e.blob, min_release_size,
		    e.blob_offset + skip_front, e.length - skip_front - skip_back);
    }

    // fixme: we may leave parts of new blob unreferenced that could
    // be freed (relative to the shared_blob).
    txc->statfs_delta.stored() += ne->length;
    if (e.blob->get_blob().is_compressed()) {
      txc->statfs_delta.compressed_original() += ne->length;
      if (blob_duped) {
        txc->statfs_delta.compressed() +=
          cb->get_blob().get_compressed_payload_length();
      }
    }
    dout(20) << __func__ << "  dst " << *ne << dendl;
    ++n;
  }
  // By default do not copy buffers to clones, and let them read data by
  // themselves. The exception are 'writing' buffers, which are not yet
  // stable on device.
  oldo->bc._dup_writing(txc, newo->c, newo, dstoff, length);

  if (src_dirty) {
    dirty_range(dirty_range_begin, dirty_range_end - dirty_range_begin);
    txc->write_onode(oldo);
  }

  if (dstoff + length > newo->onode.size) {
    newo->onode.size = dstoff + length;
  }
  newo->extent_map.dirty_range(dstoff, length);
  newo->extent_map.maybe_reshard(dstoff, dstoff + length);
  txc->write_onode(newo);
  dout(25) << __func__ << " end oldo=" << dendl;
  _dump_onode<25>(onode->c->store->cct, *oldo);
  dout(25) << __func__ << " end newo=" << dendl;
  _dump_onode<25>(onode->c->store->cct, *newo);
  bcs->lock.unlock();
}

void bluestore::ExtentMap::update(KeyValueDB::Transaction t,
                                  bool just_after_reshard)
{
  auto cct = onode->c->store->cct; //used by dout
  bool do_check = onode->c->store->debug_extent_map_encode_check;
  dout(20) << __func__ << " " << onode->oid << (just_after_reshard ? " force" : "") << dendl;
  if (onode->onode.extent_map_shards.empty()) {
    if (inline_bl.length() == 0) {
      unsigned n;
      // we need to encode inline_bl to measure encoded length
      bool never_happen = encode_some(0, BlueStore::OBJECT_MAX_SIZE, inline_bl, &n,
        do_check, do_check && just_after_reshard);
      inline_bl.reassign_to_mempool(mempool::mempool_bluestore_inline_bl);
      ceph_assert(!never_happen);
      size_t len = inline_bl.length();
      dout(20) << __func__ << "  inline shard " << len << " bytes from " << n
	       << " extents" << dendl;
      if (!just_after_reshard && len > cct->_conf->bluestore_extent_map_shard_max_size) {
	request_reshard(0, BlueStore::OBJECT_MAX_SIZE);
	return;
      }
    }
    // will persist in the onode key.
  } else {
    // pending shard update
    struct dirty_shard_t {
      Shard *shard;
      bufferlist bl;
      dirty_shard_t(Shard *s) : shard(s) {}
    };
    vector<dirty_shard_t> encoded_shards;
    // allocate slots for all shards in a single call instead of
    // doing multiple allocations - one per each dirty shard
    encoded_shards.reserve(shards.size());

    auto shard = shards.begin();
    auto previous_shard = shard;
    while (shard != shards.end()) {
      ceph_assert(shard->shard_info->offset >= previous_shard->shard_info->offset);
      auto next_shard = shard + 1;
      if (!shard->dirty) {
        previous_shard = shard;
        shard = next_shard;
        continue;
      }

      uint32_t endoff;
      if (next_shard == shards.end()) {
        endoff = BlueStore::OBJECT_MAX_SIZE;
      } else {
        endoff = next_shard->shard_info->offset;
      }
      encoded_shards.emplace_back(dirty_shard_t(&(*shard)));
      bufferlist& bl = encoded_shards.back().bl;
      if (encode_some(shard->shard_info->offset, endoff - shard->shard_info->offset,
          bl, &shard->extents, do_check, do_check && just_after_reshard)) {
        if (just_after_reshard) {
          _dump_extent_map<-1>(cct, *this);
          derr << __func__ << "  encode_some needs reshard" << dendl;
          ceph_assert(!just_after_reshard);
        }
      }
      size_t len = bl.length();

      dout(20) << __func__ << "  shard 0x" << std::hex
         << shard->shard_info->offset << std::dec << " is " << len
         << " bytes (was " << shard->shard_info->bytes << ") from "
         << shard->extents << " extents" << dendl;

      if (!just_after_reshard) {
        if (len > cct->_conf->bluestore_extent_map_shard_max_size) {
          // we are big; reshard ourselves
          request_reshard(shard->shard_info->offset, endoff);
        }
        // avoid resharding the trailing shard, even if it is small
        else if (next_shard != shards.end() &&
           len < g_conf()->bluestore_extent_map_shard_min_size) {
          ceph_assert(endoff != BlueStore::OBJECT_MAX_SIZE);
          if (shard == shards.begin()) {
            // we are the first shard, combine with next shard
            request_reshard(shard->shard_info->offset, endoff + 1);
          } else {
            // combine either with the previous shard or the next,
            // whichever is smaller
            if (previous_shard->shard_info->bytes > next_shard->shard_info->bytes) {
              request_reshard(shard->shard_info->offset, endoff + 1);
            } else {
              request_reshard(previous_shard->shard_info->offset, endoff);
            }
          }
        }
      }
      previous_shard = shard;
      shard = next_shard;
    }
    if (needs_reshard()) {
      return;
    }

    // schedule DB update for dirty shards
    string key;
    for (auto& it : encoded_shards) {
      dout(20) << __func__ << "  encoding key for shard 0x" << std::hex
	       << it.shard->shard_info->offset << std::dec << dendl;
      it.shard->dirty = false;
      it.shard->shard_info->bytes = it.bl.length();
      generate_extent_shard_key_and_apply(
	onode->key,
	it.shard->shard_info->offset,
	&key,
        [&](const string& final_key) {
          t->set(PREFIX_OBJ, final_key, it.bl);
        }
      );
    }
  }
}

bid_t bluestore::ExtentMap::allocate_spanning_blob_id()
{
  if (spanning_blob_map.empty())
    return 0;
  bid_t bid = spanning_blob_map.rbegin()->first + 1;
  // bid is valid and available.
  if (bid >= 0)
    return bid;
  // Find next unused bid;
  bid = rand() % (numeric_limits<bid_t>::max() + 1);
  const auto begin_bid = bid;
  do {
    if (!spanning_blob_map.count(bid))
      return bid;
    else {
      bid++;
      if (bid < 0) bid = 0;
    }
  } while (bid != begin_bid);
  auto cct = onode->c->store->cct; // used by dout
  _dump_onode<0>(cct, *onode);
  ceph_abort_msg("no available blob id");
}

bluestore::ExtentMap::ReshardPlan
bluestore::ExtentMap::reshard_decision(uint32_t segment_size) {
  ReshardPlan plan;
  auto cct = onode->c->store->cct; // used by dout

  dout(10) << __func__ << " 0x[" << std::hex << needs_reshard_begin << ","
	   << needs_reshard_end << ") segment 0x" << segment_size << std::dec
	   << " of " << onode->onode.extent_map_shards.size()
	   << " shards on " << onode->oid << dendl;
  const int span_blob_log_level = 20;
  if (cct->_conf->subsys.should_gather<ceph_subsys_bluestore, span_blob_log_level>()) {
    for (auto& p : spanning_blob_map) {
      dout(span_blob_log_level) << __func__
                                << "   spanning blob "
                                << p.first << " " << *p.second
	                        << dendl;
    }
  }
  // determine shard index range
  unsigned shard_index_begin = 0, shard_index_end = 0;
  if (!shards.empty()) {
    while (shard_index_begin + 1 < shards.size() &&
	   shards[shard_index_begin + 1].shard_info->offset <= needs_reshard_begin) {
      ++shard_index_begin;
    }
    needs_reshard_begin = shards[shard_index_begin].shard_info->offset;
    for (shard_index_end = shard_index_begin; shard_index_end < shards.size(); ++shard_index_end) {
      if (shards[shard_index_end].shard_info->offset >= needs_reshard_end) {
	needs_reshard_end = shards[shard_index_end].shard_info->offset;
	break;
      }
    }
    if (shard_index_end == shards.size()) {
      needs_reshard_end = BlueStore::OBJECT_MAX_SIZE;
    }
    dout(20) << __func__ << "   shards [" << shard_index_begin << "," << shard_index_end << ")"
	     << " over 0x[" << std::hex << needs_reshard_begin << ","
	     << needs_reshard_end << ")" << std::dec << dendl;
  } else {
    // When sharding is not applied yet, it is an error to request reshard on range.
    // The problem is that reshard() function will not touch any extent outside the range.
    // Thus initial reshard() must encompass whole object.
    needs_reshard_begin = 0;
    needs_reshard_end = BlueStore::OBJECT_MAX_SIZE;
  }

  uint64_t data_reshard_end = needs_reshard_end;
  if (needs_reshard_end == BlueStore::OBJECT_MAX_SIZE && !extent_map.empty()) {
    data_reshard_end = extent_map.rbegin()->blob_end();
  }

  // we may need to fault in a larger interval later must have all
  // referring extents for spanning blobs loaded in order to have
  // accurate use_tracker values.
  uint32_t spanning_scan_begin = needs_reshard_begin;
  uint32_t spanning_scan_end = needs_reshard_end;

  // calculate average extent size
  unsigned bytes = 0;
  unsigned extents = 0;
  if (onode->onode.extent_map_shards.empty()) {
    bytes = inline_bl.length();
    extents = extent_map.size();
  } else {
    for (unsigned i = shard_index_begin; i < shard_index_end; ++i) {
      bytes += shards[i].shard_info->bytes;
      extents += shards[i].extents;
    }
  }
  unsigned target = cct->_conf->bluestore_extent_map_shard_target_size;
  unsigned slop = target *
    cct->_conf->bluestore_extent_map_shard_target_size_slop;
  unsigned extent_avg = bytes / std::max(1u, extents);
  dout(20) << __func__ << "  extent_avg " << extent_avg << ", target " << target
	   << ", slop " << slop << dendl;

  uint32_t next_boundary = segment_size;
  uint32_t encoded_segment_estimate = 0;
  if (segment_size != 0) {
    if (data_reshard_end != needs_reshard_begin) {
      encoded_segment_estimate = bytes * segment_size / (data_reshard_end - needs_reshard_begin);
    } else {
      derr << __func__ << " 0 reshard-range doing 0x" << std::hex << needs_reshard_begin
        << "-0x" << needs_reshard_end << std::dec << " on"
        << pretty_binary_string(onode->oid.hobj.to_str()) << dendl;
      encoded_segment_estimate = 500; // just something, instead div0 ....
    }
  }

  // reshard
  unsigned estimate = 0;
  unsigned offset = needs_reshard_begin;
  vector<bluestore_onode_t::shard_info> new_shard_info;
  unsigned max_blob_end = 0;
  Extent dummy(needs_reshard_begin);
  for (auto extent = extent_map.lower_bound(dummy);
       extent != extent_map.end();
       ++extent) {
    if (extent->logical_offset >= needs_reshard_end) {
      break;
    }
    dout(30) << " extent " << *extent << dendl;

    bool make_shard_here = false;
    if (segment_size != 0) { //onode data has strict boundaries
      if (extent->blob_start() >= next_boundary) {
        // beginning of the extent is a place that might be a shard boundary
        // we want to decide whether to continue streaming to the current shard
        // or move to the next one
	if (estimate + encoded_segment_estimate/2 >= target /*it is better to go undersize*/) {
	  make_shard_here = true;
	}
	next_boundary = p2roundup(extent->blob_end(), segment_size);
      }
    } else {
      // disfavor shard boundaries that span a blob
      bool would_span = (extent->logical_offset < max_blob_end) || (extent->blob_offset != 0);
      if ((estimate > 0)
          && (estimate + extent_avg > target + (would_span ? slop : 0))) {
	make_shard_here = true;
      }
    }
    if (make_shard_here) {
      // new shard
      if (offset == needs_reshard_begin) {
	new_shard_info.emplace_back(bluestore_onode_t::shard_info());
	new_shard_info.back().offset = offset;
	dout(20) << __func__ << "  new shard 0x" << std::hex << offset
                 << std::dec << dendl;
      }
      offset = extent->logical_offset;
      new_shard_info.emplace_back(bluestore_onode_t::shard_info());
      new_shard_info.back().offset = offset;
      dout(20) << __func__ << "  new shard 0x" << std::hex << offset
	       << std::dec << dendl;
      estimate = 0;
    }
    estimate += extent_avg;
    unsigned blob_start = extent->blob_start();
    if (blob_start < spanning_scan_begin) {
      spanning_scan_begin = blob_start;
    }
    uint32_t blob_end = extent->blob_end();
    if (blob_end > max_blob_end) {
      max_blob_end = blob_end;
    }
    if (blob_end > spanning_scan_end) {
      spanning_scan_end = blob_end;
    }
  }
  if (new_shard_info.empty() && (shard_index_begin > 0 ||
				 shard_index_end < shards.size())) {
    // we resharded a partial range; we must produce at least one output
    // shard
    new_shard_info.emplace_back(bluestore_onode_t::shard_info());
    new_shard_info.back().offset = needs_reshard_begin;
    dout(20) << __func__ << "  new shard 0x" << std::hex << needs_reshard_begin
	     << std::dec << " (singleton degenerate case)" << dendl;
  }

  auto& extent_map_shards = onode->onode.extent_map_shards;
  dout(20) << __func__ << "  new " << new_shard_info << dendl;
  dout(20) << __func__ << "  old " << extent_map_shards << dendl;

  plan.shard_index_begin = shard_index_begin;
  plan.shard_index_end = shard_index_end;
  plan.spanning_scan_begin = spanning_scan_begin;
  plan.spanning_scan_end = spanning_scan_end;
  plan.new_shard_info = std::move(new_shard_info);
  return plan;
}


void bluestore::ExtentMap::reshard_action(
  ReshardPlan& plan,
  KeyValueDB *db,
  KeyValueDB::Transaction t) {
  auto cct = onode->c->store->cct; // For configuration and logging

  std::vector<bluestore_onode_t::shard_info> new_shard_info = plan.new_shard_info;
  unsigned shard_index_begin = plan.shard_index_begin;
  unsigned shard_index_end = plan.shard_index_end;
  uint32_t spanning_scan_begin = plan.spanning_scan_begin;
  uint32_t spanning_scan_end = plan.spanning_scan_end;

  dout(20) << __func__ << " applying plan with shards [" << shard_index_begin << ","
           << shard_index_end << ")" << dendl;

  // Fault the range
  if (db) {
    fault_range(db, needs_reshard_begin, (needs_reshard_end - needs_reshard_begin));
  }

  // Remove old shard keys
  string key;
  for (unsigned i = shard_index_begin; t && i < shard_index_end; ++i) {
    generate_extent_shard_key_and_apply(
      onode->key, shards[i].shard_info->offset, &key,
      [&](const string& final_key) {
	t->rmkey(PREFIX_OBJ, final_key);
      }
      );
  }

  // Update extent_map_shards and shards
  auto& extent_map_shards = onode->onode.extent_map_shards;
  if (extent_map_shards.empty()) {
    // no old shards to keep
    extent_map_shards.swap(new_shard_info);
    init_shards(true, true);
  } else {
    // splice in new shards
    extent_map_shards.erase(extent_map_shards.begin() + shard_index_begin, extent_map_shards.begin() + shard_index_end);
    shards.erase(shards.begin() + shard_index_begin, shards.begin() + shard_index_end);
    extent_map_shards.insert(
      extent_map_shards.begin() + shard_index_begin,
      new_shard_info.begin(),
      new_shard_info.end());
    shards.insert(shards.begin() + shard_index_begin, new_shard_info.size(), Shard());
    shard_index_end = shard_index_begin + new_shard_info.size();

    ceph_assert(extent_map_shards.size() == shards.size());

    // note that we need to update every shard_info of shards here,
    // as extent_map_shards might have been totally re-allocated above
    for (unsigned i = 0; i < shards.size(); i++) {
      shards[i].shard_info = &extent_map_shards[i];
    }

    // mark newly added shards as dirty
    for (unsigned i = shard_index_begin; i < shard_index_end; ++i) {
      shards[i].loaded = true;
      shards[i].dirty = true;
    }
  }
  dout(20) << __func__ << "  fin " << extent_map_shards << dendl;
  inline_bl.clear();

  if (extent_map_shards.empty()) {
    // no more shards; unspan all previously spanning blobs
    auto spanning_blob_it = spanning_blob_map.begin();
    while (spanning_blob_it != spanning_blob_map.end()) {
      spanning_blob_it->second->id = -1;
      dout(30) << __func__ << " un-spanning " << *spanning_blob_it->second << dendl;
      spanning_blob_it = spanning_blob_map.erase(spanning_blob_it);
    }
  } else {
    // identify new spanning blobs
    dout(20) << __func__ << " checking spanning blobs 0x[" << std::hex
	     << spanning_scan_begin << "," << spanning_scan_end << ")" << dendl;
    if (db) {
      if (spanning_scan_begin < needs_reshard_begin) {
        fault_range(db, spanning_scan_begin,
		    needs_reshard_begin - spanning_scan_begin);
      }
      if (spanning_scan_end > needs_reshard_end) {
        fault_range(db, needs_reshard_end,
		       spanning_scan_end - needs_reshard_end);
      }
    }
    auto current_shard = extent_map_shards.begin() + shard_index_begin;
    auto end_shard = extent_map_shards.end();
    unsigned shard_start = current_shard->offset;
    unsigned shard_end;
    ++current_shard;
    if (current_shard == end_shard) {
      shard_end = BlueStore::OBJECT_MAX_SIZE;
    } else {
      shard_end = current_shard->offset;
    }

    bool was_too_many_blobs_check = false;
    auto too_many_blobs_threshold =
      g_conf()->bluestore_debug_too_many_blobs_threshold;
    auto& dumped_onodes = onode->c->onode_space.cache->dumped_onodes;
    decltype(onode->c->onode_space.cache->dumped_onodes)::value_type* oid_slot = nullptr;
    decltype(onode->c->onode_space.cache->dumped_onodes)::value_type* oldest_slot = nullptr;

    for (auto extent = extent_map.lower_bound(Extent(needs_reshard_begin)); extent != extent_map.end(); ++extent) {
      if (extent->logical_offset >= needs_reshard_end) {
	break;
      }
      dout(30) << __func__ << " extent " << *extent << dendl;
      while (extent->logical_offset >= shard_end) {
	shard_start = shard_end;
	ceph_assert(current_shard != end_shard);
	++current_shard;
	if (current_shard == end_shard) {
	  shard_end = BlueStore::OBJECT_MAX_SIZE;
	} else {
	  shard_end = current_shard->offset;
	}
	dout(30) << __func__ << "  shard 0x" << std::hex << shard_start
		 << " to 0x" << shard_end << std::dec << dendl;
      }

      if (extent->blob_escapes_range(shard_start, shard_end - shard_start)) {
	BlueStore::BlobRef b = extent->blob;
	uint32_t bstart = extent->blob_start();
	uint32_t bend = extent->blob_end();
	if (!b->is_spanning()) {
	  // We have two options: (1) split the blob into pieces at the
	  // shard boundaries (and adjust extents accordingly), or (2)
	  // mark it spanning.  We prefer to cut the blob if we can.  Note that
	  // we may have to split it multiple times--potentially at every
	  // shard boundary.
	  auto _make_spanning = [&](BlueStore::BlobRef& b) {
	    auto bid = allocate_spanning_blob_id();
	    b->id = bid;
	    spanning_blob_map[b->id] = b;
	    dout(20) << __func__ << "    adding spanning " << *b << dendl;
	    if (!was_too_many_blobs_check &&
	      too_many_blobs_threshold &&
	      spanning_blob_map.size() >= size_t(too_many_blobs_threshold)) {

	      was_too_many_blobs_check = true;
	      for (size_t i = 0; i < dumped_onodes.size(); ++i) {
		if (dumped_onodes[i].first == onode->oid) {
		  oid_slot = &dumped_onodes[i];
		  break;
		}
		if (!oldest_slot || (oldest_slot &&
		  dumped_onodes[i].second < oldest_slot->second)) {
		  oldest_slot = &dumped_onodes[i];
		}
	      }
	    }
	  };
	  if (b->can_split()) {
	    auto bstart1 = bstart;
	    for (const auto& sh : shards) {
	      if (bstart1 < sh.shard_info->offset &&
		  bend > sh.shard_info->offset) {
		uint32_t blob_offset = sh.shard_info->offset - bstart1;
		if (b->can_split_at(blob_offset)) {
		  dout(20) << __func__ << "    splitting blob, bstart 0x"
			   << std::hex << bstart1 << " blob_offset 0x"
			   << blob_offset << std::dec << " " << *b << dendl;
		  b = split_blob(b, blob_offset, sh.shard_info->offset);
                  if (b->get_blob().get_ondisk_size() == 0) {
                    // The blob b is empty; there are no extents that can reference it.
                    // It will be deleted as soon as it gets out of scope.
                    break;
                  }
		  // switch b to the new right-hand side, in case it
		  // *also* has to get split.
		  bstart1 = sh.shard_info->offset;
		  onode->c->store->logger->inc(l_bluestore_blob_split);
		} else {
		  _make_spanning(b);
		  break;
		}
	      }
	    }
	  } else {
	    _make_spanning(b);
	  }
	} // if (!extent->blob->is_spanning())
	// Make sure extent with a spanning blob doesn't span over shard boundary
	if (extent->blob->is_spanning()) {
	  BlueStore::BlobRef b = extent->blob;
	  uint32_t bstart = extent->blob_start();
	  for (const auto& sh : shards) {
	    if (bstart < sh.shard_info->offset && bend > sh.shard_info->offset) {
	      uint32_t blob_offset = sh.shard_info->offset - bstart;
	      auto pos = sh.shard_info->offset;
	      if (extent->logical_offset < pos && extent->logical_end() > pos) {
		// split extent
		size_t left = pos - extent->logical_offset;
		Extent* ne = new Extent(pos, blob_offset, extent->length - left, b);
		extent_map.insert(*ne);
		extent->length = left;
		dout(20) << __func__ << "  split " << *extent << dendl;
		dout(20) << __func__ << "     to " << *ne << dendl;
	      }
	    }
	  }
	}
      } else {
	if (extent->blob->is_spanning()) {
	  spanning_blob_map.erase(extent->blob->id);
	  extent->blob->id = -1;
	  dout(20) << __func__ << "    un-spanning " << *extent->blob << dendl;
	}
      }
    }
    bool do_dump = (!oid_slot && was_too_many_blobs_check) ||
      (oid_slot &&
	(mono_clock::now() - oid_slot->second >= make_timespan(5 * 60)));
    if (do_dump) {
      dout(0) << __func__
	      << " spanning blob count exceeds threshold, "
	      << spanning_blob_map.size() << " spanning blobs"
	      << dendl;
      _dump_onode<0>(cct, *onode);
      if (oid_slot) {
	oid_slot->second = mono_clock::now();
      } else {
	ceph_assert(oldest_slot);
	oldest_slot->first = onode->oid;
	oldest_slot->second = mono_clock::now();
      }
    }
  }

  clear_needs_reshard();
}

void bluestore::ExtentMap::reshard(
  KeyValueDB *db,
  KeyValueDB::Transaction t,
  uint32_t segment_size) {
  auto plan = reshard_decision(segment_size);
  reshard_action(plan, db, t);
}

bool bluestore::ExtentMap::encode_some(
  uint32_t offset,
  uint32_t length,
  bufferlist& bl,
  unsigned *pn,
  bool complain_extent_overlap,
  bool complain_shard_spanning)
{
  Extent dummy(offset);
  auto start = extent_map.lower_bound(dummy);
  uint32_t end = offset + length;

  __u8 struct_v = 2; // Version 2 differs from v1 in blob's ref_map
                     // serialization only. Hence there is no specific
                     // handling at ExtentMap level.

  unsigned n = 0;
  size_t bound = 0;
  uint32_t prev_offset_end = 0;
  for (auto p = start;
       p != extent_map.end() && p->logical_offset < end;
       ++p, ++n) {
    ceph_assert(p->logical_offset >= offset);
    if (complain_extent_overlap) {
      if (p->logical_offset < prev_offset_end) {
        using P = bluestore::printer;
        dout(-1) << __func__ << " extents overlap: "
                 << std::hex << offset <<"~" << length
                 << " " << p->logical_offset <<"~" << p->length
                 << std::dec << std::endl
                 << onode->print(P::NICK + P::SDISK + P::SUSE + P::SBUF)
                 << dendl;
	ceph_abort_msg("extents overlaps");
      }
      prev_offset_end = p->logical_end();
    }
    p->blob->last_encoded_id = -1;
    if (!p->blob->is_spanning() && p->blob_escapes_range(offset, length)) {
      dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	       << std::dec << " hit new spanning blob " << *p << dendl;
      request_reshard(p->blob_start(), p->blob_end());
      return true;
    } else if (p->blob->is_spanning() && p->logical_end() > end) {
      dout(20) << __func__ << std::hex << offset << "~" << length
               << std::dec << " extent stands out " << *p << dendl;
      request_reshard(p->blob_start(), p->blob_end());
      return true;
    } else {
      denc_varint(0, bound); // blobid
      denc_varint(0, bound); // logical_offset
      denc_varint(0, bound); // len
      denc_varint(0, bound); // blob_offset

      p->blob->bound_encode(
        bound,
        struct_v,
        p->blob->get_sbid(),
        false);
    }
  }

  denc(struct_v, bound);
  denc_varint(0, bound); // number of extents

  {
    auto app = bl.get_contiguous_appender(bound);
    denc(struct_v, app);
    denc_varint(n, app);
    if (pn) {
      *pn = n;
    }

    n = 0;
    uint64_t pos = 0;
    uint64_t prev_len = 0;
    for (auto p = start;
	 p != extent_map.end() && p->logical_offset < end;
	 ++p, ++n) {
      unsigned blobid;
      if (complain_shard_spanning) {
        if (p->logical_end() > end) {
          using P = bluestore::printer;
          dout(-1) << __func__ << " extent spans shard after reshard " << ": " << std::endl
            << onode->print(P::NICK + P::SDISK + P::SUSE + P::SBUF) << dendl;
          ceph_abort();
        }
      }
      bool include_blob = false;
      if (p->blob->is_spanning()) {
	blobid = p->blob->id << BLOBID_SHIFT_BITS;
	blobid |= BLOBID_FLAG_SPANNING;
      } else if (p->blob->last_encoded_id < 0) {
	p->blob->last_encoded_id = n + 1;  // so it is always non-zero
	include_blob = true;
	blobid = 0;  // the decoder will infer the id from n
      } else {
	blobid = p->blob->last_encoded_id << BLOBID_SHIFT_BITS;
      }
      if (p->logical_offset == pos) {
	blobid |= BLOBID_FLAG_CONTIGUOUS;
      }
      if (p->blob_offset == 0) {
	blobid |= BLOBID_FLAG_ZEROOFFSET;
      }
      if (p->length == prev_len) {
	blobid |= BLOBID_FLAG_SAMELENGTH;
      } else {
	prev_len = p->length;
      }
      denc_varint(blobid, app);
      if ((blobid & BLOBID_FLAG_CONTIGUOUS) == 0) {
	denc_varint_lowz(p->logical_offset - pos, app);
      }
      if ((blobid & BLOBID_FLAG_ZEROOFFSET) == 0) {
	denc_varint_lowz(p->blob_offset, app);
      }
      if ((blobid & BLOBID_FLAG_SAMELENGTH) == 0) {
	denc_varint_lowz(p->length, app);
      }
      pos = p->logical_end();
      if (include_blob) {
	p->blob->encode(app, struct_v, p->blob->get_sbid(), false);
      }
    }
  }
  /*derr << __func__ << bl << dendl;
  derr << __func__ << ":";
  bl.hexdump(*_dout);
  *_dout << dendl;
  */
  return false;
}

/////////////////// BlueStore::ExtentMap::DecoderExtent ///////////
void bluestore::ExtentMap::ExtentDecoder::decode_extent(
  Extent* le,
  __u8 struct_v,
  bptr_c_it_t& p,
  BlueStore::Collection* c)
{
  uint64_t blobid;
  denc_varint(blobid, p);
  if ((blobid & BLOBID_FLAG_CONTIGUOUS) == 0) {
    uint64_t gap;
    denc_varint_lowz(gap, p);
    pos += gap;
  }
  le->logical_offset = pos;
  if ((blobid & BLOBID_FLAG_ZEROOFFSET) == 0) {
    denc_varint_lowz(le->blob_offset, p);
  } else {
    le->blob_offset = 0;
  }
  if ((blobid & BLOBID_FLAG_SAMELENGTH) == 0) {
    denc_varint_lowz(prev_len, p);
  }
  le->length = prev_len;
  if (blobid & BLOBID_FLAG_SPANNING) {
    consume_blobid(le, true, blobid >> BLOBID_SHIFT_BITS);
  } else {
    blobid >>= BLOBID_SHIFT_BITS;
    if (blobid) {
      consume_blobid(le, false, blobid - 1);
    } else {
      // dummy onodes might not have collections, we need a check for it.
      uint64_t sbid = 0;
      BlueStore::BlobRef b = decode_create_blob(p, struct_v, &sbid, false, c);
      consume_blob(le, extent_pos, sbid, b);
    }
  }
  pos += prev_len;
  ++extent_pos;
}

unsigned bluestore::ExtentMap::ExtentDecoder::decode_some(
  const bufferlist& bl, BlueStore::Collection* c)
{
  __u8 struct_v;
  uint32_t num;

  ceph_assert(bl.get_num_buffers() <= 1);
  auto p = bl.front().begin_deep();
  denc(struct_v, p);
  // Version 2 differs from v1 in blob's ref_map
  // serialization only. Hence there is no specific
  // handling at ExtentMap level below.
  ceph_assert_decode(struct_v == 1 || struct_v == 2);
  denc_varint(num, p);

  extent_pos = 0;
  while (!p.end()) {
    Extent* le = get_next_extent();
    decode_extent(le, struct_v, p, c);
    add_extent(le);
  }
  ceph_assert_decode(extent_pos == num);
  return num;
}

void BlueStore::ExtentMap::ExtentDecoder::decode_spanning_blobs(
  bptr_c_it_t& p, BlueStore::Collection* c)
{
  __u8 struct_v;
  denc(struct_v, p);
  // Version 2 differs from v1 in blob's ref_map
  // serialization only. Hence there is no specific
  // handling at ExtentMap level.
  ceph_assert_decode(struct_v == 1 || struct_v == 2);

  unsigned n;
  denc_varint(n, p);
  while (n--) {
    decltype(Blob::id) id;
    denc_varint(id, p);

    uint64_t sbid = 0;
    BlueStore::BlobRef b = decode_create_blob(p, struct_v, &sbid, true, c);
    b->id = id;
    consume_spanning_blob(sbid, b);
  }
}

/////////////////// BlueStore::ExtentMap::DecoderExtentFull ///////////
BlueStore::BlobRef bluestore::ExtentMap::ExtentDecoderFull::decode_create_blob(
  bptr_c_it_t& p,
  __u8 struct_v,
  uint64_t* sbid,
  bool include_ref_map,
  BlueStore::Collection* c) {
  BlueStore::BlobRef b = c ? c->new_blob() : new BlueStore::Blob(nullptr);
  b->decode<true>(p, struct_v, sbid, include_ref_map, c);
  return b;
}

void bluestore::ExtentMap::ExtentDecoderFull::consume_blobid(
  bluestore::Extent* le, bool spanning, uint64_t blobid) {
  ceph_assert(le);
  if (spanning) {
    le->assign_blob(extent_map.get_spanning_blob(blobid));
  } else {
    ceph_assert_decode(blobid < blobs.size());
    le->assign_blob(blobs[blobid]);
    // we build ref_map dynamically for non-spanning blobs
    le->blob->get_ref(
      extent_map.onode->c,
      le->blob_offset,
      le->length);
  }
}

void bluestore::ExtentMap::ExtentDecoderFull::consume_blob(
  bluestore::Extent* le, uint64_t extent_no, uint64_t sbid, BlueStore::BlobRef b) {
  ceph_assert(le);
  blobs.resize(extent_no + 1);
  blobs[extent_no] = b;
  extent_map.onode->c->open_shared_blob(sbid, b);
  le->assign_blob(b);
  le->blob->get_ref(
    extent_map.onode->c,
    le->blob_offset,
    le->length);
}

void BlueStore::ExtentMap::ExtentDecoderFull::consume_spanning_blob(
  uint64_t sbid, BlueStore::BlobRef b) {
  extent_map.spanning_blob_map[b->id] = b;
  extent_map.onode->c->open_shared_blob(sbid, b);
}

bluestore::Extent* bluestore::ExtentMap::ExtentDecoderFull::get_next_extent()
{
  pending_extent = std::make_unique<Extent>();
  return pending_extent.get();
}

void bluestore::ExtentMap::ExtentDecoderFull::add_extent(bluestore::Extent* le)
{
  ceph_assert(le == pending_extent.get());
  extent_map.extent_map.insert(*le);
  pending_extent.release();     // ownership now with the intrusive set
}

unsigned bluestore::ExtentMap::decode_some(bufferlist& bl)
{
  ExtentDecoderFull edecoder(*this);
  unsigned n = edecoder.decode_some(bl, onode->c);
  return n;
}

void bluestore::ExtentMap::bound_encode_spanning_blobs(size_t& p)
{
  // Version 2 differs from v1 in blob's ref_map
  // serialization only. Hence there is no specific
  // handling at ExtentMap level.
  __u8 struct_v = 2;

  denc(struct_v, p);
  denc_varint((uint32_t)0, p);
  size_t key_size = 0;
  denc_varint((uint32_t)0, key_size);
  p += spanning_blob_map.size() * key_size;
  for (const auto& i : spanning_blob_map) {
    i.second->bound_encode(p, struct_v, i.second->get_sbid(), true);
  }
}

void bluestore::ExtentMap::encode_spanning_blobs(
  bufferlist::contiguous_appender& p)
{
  // Version 2 differs from v1 in blob's ref_map
  // serialization only. Hence there is no specific
  // handling at ExtentMap level.
  __u8 struct_v = 2;

  denc(struct_v, p);
  denc_varint(spanning_blob_map.size(), p);
  for (auto& i : spanning_blob_map) {
    denc_varint(i.second->id, p);
    i.second->encode(p, struct_v, i.second->get_sbid(), true);
  }
}

void bluestore::ExtentMap::init_shards(bool loaded, bool dirty)
{
  shards.resize(onode->onode.extent_map_shards.size());
  unsigned i = 0;
  for (auto &s : onode->onode.extent_map_shards) {
    shards[i].shard_info = &s;
    shards[i].loaded = loaded;
    shards[i].dirty = dirty;
    ++i;
  }
}

std::pair<uint32_t, uint32_t> bluestore::ExtentMap::fault_range_ex(
  KeyValueDB *db,
  uint32_t offset,
  uint32_t length)
{
  dout(30) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  if (shards.size() == 0) {
    // no sharding yet; everyting is loaded
    return {0, BlueStore::OBJECT_MAX_SIZE};
  }
  auto start = seek_shard(offset);
  auto last = seek_shard(offset + length);
  maybe_load_shard(db, start, last);
  uint32_t left_bound = shards[start].shard_info->offset;
  uint32_t right_bound =  (size_t)last + 1 < shards.size() ?
                          shards[last + 1].shard_info->offset : BlueStore::OBJECT_MAX_SIZE;
  dout(20) << __func__ << " start=" << start << " last=" << last
    << " -> 0x" << std::hex << left_bound << "~" << right_bound
    << std::dec << dendl;
  return {left_bound, right_bound};
}

void bluestore::ExtentMap::fault_range(
  KeyValueDB *db,
  uint32_t offset,
  uint32_t length)
{
  dout(30) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  if (shards.size() == 0) {
    // no sharding yet; everyting is loaded
    return;
  }
  auto start = seek_shard(offset);
  auto last = seek_shard(offset + length);
  maybe_load_shard(db, start, last);
}

void bluestore::ExtentMap::maybe_load_shard(
  KeyValueDB *db,
  int start,
  int last)
{
  ceph_assert(last >= start);
  ceph_assert(start >= 0);

  string key;
  while (start <= last) {
    ceph_assert((size_t)start < shards.size());
    auto p = &shards[start];
    if (!p->loaded) {
      BLUE_SCOPE(maybe_load_shard);
      dout(30) << __func__ << " opening shard 0x" << std::hex
	       << p->shard_info->offset << std::dec << dendl;
      bufferlist v;
      generate_extent_shard_key_and_apply(
	onode->key, p->shard_info->offset, &key,
        [&](const string& final_key) {
          int r = db->get(PREFIX_OBJ, final_key, &v);
          if (r < 0) {
	    derr << __func__ << " missing shard 0x" << std::hex
		 << p->shard_info->offset << std::dec << " for " << onode->oid
		 << dendl;
	    ceph_assert(r >= 0);
          }
        }
      );
      p->extents = decode_some(v);
      p->loaded = true;
      uint32_t shard_end =
        (size_t)start + 1 < shards.size() ? (p + 1)->shard_info->offset : BlueStore::OBJECT_MAX_SIZE;
      dout(20) << __func__ << " open shard for range 0x"
               << std::hex << p->shard_info->offset << "~" << shard_end << std::dec
	       << " (" << v.length() << " bytes)" << dendl;
      ceph_assert(p->dirty == false);
      ceph_assert(v.length() == p->shard_info->bytes);
      onode->c->store->logger->inc(l_bluestore_onode_shard_misses);
    } else {
      onode->c->store->logger->inc(l_bluestore_onode_shard_hits);
    }
    ++start;
  }
}

void bluestore::ExtentMap::dirty_range(
  uint32_t offset,
  uint32_t length)
{
  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  if (shards.empty()) {
    dout(20) << __func__ << " mark inline shard dirty" << dendl;
    inline_bl.clear();
    return;
  }
  auto start = seek_shard(offset);
  if (length == 0) {
    length = 1;
  }
  auto last = seek_shard(offset + length - 1);
  if (start < 0)
    return;

  ceph_assert(last >= start);
  while (start <= last) {
    ceph_assert((size_t)start < shards.size());
    auto p = &shards[start];
    if (!p->loaded) {
      derr << __func__ << "on write 0x" << std::hex << offset
	   << "~" << length << " shard 0x" << p->shard_info->offset
	   << std::dec << " is not loaded, can't mark dirty" << dendl;
      ceph_abort_msg("can't mark unloaded shard dirty");
    }
    if (!p->dirty) {
      dout(20) << __func__ << " mark shard 0x" << std::hex
	       << p->shard_info->offset << std::dec << " dirty" << dendl;
      p->dirty = true;
    }
    ++start;
  }
}

BlueStore::extent_map_t::iterator bluestore::ExtentMap::find(
  uint64_t offset)
{
  Extent dummy(offset);
  return extent_map.find(dummy);
}

BlueStore::extent_map_t::iterator bluestore::ExtentMap::seek_lextent(
  uint64_t offset)
{
  Extent dummy(offset);
  auto fp = extent_map.lower_bound(dummy);
  if (fp != extent_map.begin()) {
    --fp;
    if (fp->logical_end() <= offset) {
      ++fp;
    }
  }
  return fp;
}

BlueStore::extent_map_t::const_iterator bluestore::ExtentMap::seek_lextent(
  uint64_t offset) const
{
  Extent dummy(offset);
  auto fp = extent_map.lower_bound(dummy);
  if (fp != extent_map.begin()) {
    --fp;
    if (fp->logical_end() <= offset) {
      ++fp;
    }
  }
  return fp;
}

// Split extent at desired offset.
// Returns iterator to the right part.
BlueStore::extent_map_t::iterator bluestore::ExtentMap::split_at(
  BlueStore::extent_map_t::iterator p, uint32_t offset)
{
  ceph_assert(p != extent_map.end());
  ceph_assert(p->logical_offset < offset);
  ceph_assert(offset < p->logical_end());
  add(offset, p->blob_offset + (offset - p->logical_offset),
      p->logical_end() - offset, p->blob);
  p->length = offset - p->logical_offset;
  ++p;
  return p;
}

// If inside extent split it, and return right part.
// If not inside extent return extent on right.
BlueStore::extent_map_t::iterator bluestore::ExtentMap::maybe_split_at(uint32_t offset)
{
  auto p = seek_lextent(offset);
  if (p != extent_map.end()) {
    if (p->logical_offset < offset && offset < p->logical_end()) {
      // need to split
      add(offset, p->blob_offset + (offset - p->logical_offset),
          p->logical_end() - offset, p->blob);
      p->length = offset - p->logical_offset;
      ++p;
      // check that we moved to proper extent
      ceph_assert(p->logical_offset == offset);
    } else {
      // the extent is either outside offset or exactly at
    }
  }
  return p;
}

// If there exist extent at `offset` return it,
// otherwise return smallest that `offset < logical_offset`.
BlueStore::extent_map_t::iterator bluestore::ExtentMap::seek_nextent(
  uint64_t offset)
{
  Extent dummy(offset);
  auto p = extent_map.lower_bound(dummy);
  return p;
}

bool bluestore::ExtentMap::has_any_lextents(uint64_t offset, uint64_t length)
{
  auto fp = seek_lextent(offset);
  if (fp == extent_map.end() || fp->logical_offset >= offset + length) {
    return false;
  }
  return true;
}

int bluestore::ExtentMap::compress_extent_map(
  uint64_t offset,
  uint64_t length)
{
  if (extent_map.empty())
    return 0;
  int removed = 0;
  auto p = seek_lextent(offset);
  if (p != extent_map.begin()) {
    --p;  // start to the left of offset
  }
  // the caller should have just written to this region
  ceph_assert(p != extent_map.end());

  // identify the *next* shard
  auto pshard = shards.begin();
  while (pshard != shards.end() &&
	 p->logical_offset >= pshard->shard_info->offset) {
    ++pshard;
  }
  uint64_t shard_end;
  if (pshard != shards.end()) {
    shard_end = pshard->shard_info->offset;
  } else {
    shard_end = BlueStore::OBJECT_MAX_SIZE;
  }

  auto n = p;
  for (++n; n != extent_map.end(); p = n++) {
    if (n->logical_offset > offset + length) {
      break;  // stop after end
    }
    while (n != extent_map.end() &&
	   p->logical_end() == n->logical_offset &&
	   p->blob == n->blob &&
	   p->blob_offset + p->length == n->blob_offset &&
	   n->logical_offset < shard_end) {
      dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	       << " next shard 0x" << shard_end << std::dec
	       << " merging " << *p << " and " << *n << dendl;
      p->length += n->length;
      rm(n++);
      ++removed;
    }
    if (n == extent_map.end()) {
      break;
    }
    if (n->logical_offset >= shard_end) {
      ceph_assert(pshard != shards.end());
      ++pshard;
      if (pshard != shards.end()) {
	shard_end = pshard->shard_info->offset;
      } else {
	shard_end = BlueStore::OBJECT_MAX_SIZE;
      }
    }
  }
  if (removed) {
    onode->c->store->logger->inc(l_bluestore_extent_compress, removed);
  }
  return removed;
}

void bluestore::ExtentMap::punch_hole(
  BlueStore::CollectionRef &c,
  uint64_t offset,
  uint64_t length,
  BlueStore::old_extent_map_t *old_extents)
{
  auto p = seek_lextent(offset);
  uint64_t end = offset + length;
  while (p != extent_map.end()) {
    if (p->logical_offset >= end) {
      break;
    }
    if (p->logical_offset < offset) {
      if (p->logical_end() > end) {
	// split and deref middle
	uint64_t front = offset - p->logical_offset;
	BlueStore::OldExtent* oe = BlueStore::OldExtent::create(c, offset, p->blob_offset + front,
					  length, p->blob);
	old_extents->push_back(*oe);
	add(end,
	    p->blob_offset + front + length,
	    p->length - front - length,
	    p->blob);
	p->length = front;
	break;
      } else {
	// deref tail
	ceph_assert(p->logical_end() > offset); // else seek_lextent bug
	uint64_t keep = offset - p->logical_offset;
	BlueStore::OldExtent* oe = BlueStore::OldExtent::create(c, offset, p->blob_offset + keep,
					  p->length - keep, p->blob);
	old_extents->push_back(*oe);
	p->length = keep;
	++p;
	continue;
      }
    }
    if (p->logical_offset + p->length <= end) {
      // deref whole lextent
      BlueStore::OldExtent* oe = BlueStore::OldExtent::create(c, p->logical_offset, p->blob_offset,
				        p->length, p->blob);
      old_extents->push_back(*oe);
      rm(p++);
      continue;
    }
    // deref head
    uint64_t keep = p->logical_end() - end;
    BlueStore::BlobRef b = p->blob;
    BlueStore::OldExtent* oe = BlueStore::OldExtent::create(c, p->logical_offset, p->blob_offset,
				      p->length - keep, b);
    old_extents->push_back(*oe);

    add(end, p->blob_offset + p->length - keep, keep, p->blob);
    rm(p);
    break;
  }
}

bluestore::Extent *bluestore::ExtentMap::set_lextent(
  BlueStore::CollectionRef &c,
  uint64_t logical_offset,
  uint64_t blob_offset, uint64_t length, BlueStore::BlobRef b,
  BlueStore::old_extent_map_t *old_extents)
{
  // We need to have completely initialized Blob to increment its ref counters.
  ceph_assert(b->get_blob().get_logical_length() != 0);

  // Do get_ref prior to punch_hole to prevent from putting reused blob into
  // old_extents list if we overwre the blob totally
  // This might happen during WAL overwrite.
  b->get_ref(onode->c, blob_offset, length);

  if (old_extents) {
    punch_hole(c, logical_offset, length, old_extents);
  }

  Extent *le = new Extent(logical_offset, blob_offset, length, b);
  extent_map.insert(*le);
  maybe_reshard(logical_offset, logical_offset + length);
  return le;
}

BlueStore::BlobRef bluestore::ExtentMap::split_blob(
  BlueStore::BlobRef lb,
  uint32_t blob_offset,
  uint32_t pos)
{
  uint32_t end_pos = pos + lb->get_blob().get_logical_length() - blob_offset;
  dout(20) << __func__ << " 0x" << std::hex << pos << " end 0x" << end_pos
	   << " blob_offset 0x" << blob_offset << std::dec << " " << *lb
	   << dendl;
  BlueStore::BlobRef rb = onode->c->new_blob();
  lb->split(onode->c, blob_offset, rb.get());

  for (auto ep = seek_lextent(pos);
       ep != extent_map.end() && ep->logical_offset < end_pos;
       ++ep) {
    if (ep->blob != lb) {
      continue;
    }
    if (ep->logical_offset < pos) {
      // split extent
      size_t left = pos - ep->logical_offset;
      Extent *ne = new Extent(pos, 0, ep->length - left, rb);
      extent_map.insert(*ne);
      ep->length = left;
      dout(30) << __func__ << "  split " << *ep << dendl;
      dout(30) << __func__ << "     to " << *ne << dendl;
    } else {
      // switch blob
      ceph_assert(ep->blob_offset >= blob_offset);

      ep->blob = rb;
      ep->blob_offset -= blob_offset;
      dout(30) << __func__ << "  adjusted " << *ep << dendl;
    }
  }
  return rb;
}

bluestore::ExtentMap::debug_au_vector_t
bluestore::ExtentMap::debug_list_disk_layout()
{
  bluestore::ExtentMap::debug_au_vector_t res;
  uint32_t l_pos = 0;
  for (auto ep = extent_map.begin(); ep != extent_map.end(); ++ep) {
    if (l_pos < ep->logical_offset) {
      // a hole in logical mapping, mark it
      res.emplace_back(-1ULL, ep->logical_offset - l_pos, 0, 0);
    }
    l_pos = ep->logical_offset + ep->length;
    const bluestore_blob_t& bblob = ep->blob->get_blob();
    uint32_t chunk_size = bblob.get_chunk_size(onode->c->store->block_size);
    uint32_t length_left = ep->length;

    bluestore_extent_ref_map_t* ref_map = nullptr;
    if (bblob.is_shared()) {
      ceph_assert(ep->blob->is_shared_loaded());
      bluestore_shared_blob_t* bsblob = ep->blob->get_shared_blob()->persistent;
      ref_map = &bsblob->ref_map;
    }

    unsigned csum_i = 0;
    size_t csum_cnt = 0;
    uint32_t length;
    if (bblob.has_csum()) {
      csum_cnt = bblob.get_csum_count();
      uint32_t csum_chunk_size = bblob.get_csum_chunk_size();
      uint64_t csum_offset_align = p2align(ep->blob_offset, csum_chunk_size);
      csum_i = csum_offset_align / csum_chunk_size;
      // size of first chunk
      length = p2align(ep->blob_offset + csum_chunk_size, csum_chunk_size) - ep->blob_offset;
      length = std::min<uint32_t>(length_left, length);
      if (csum_chunk_size < chunk_size) {
	chunk_size = csum_chunk_size;
      }
    } else {
      length = p2align(ep->blob_offset + chunk_size, chunk_size) - ep->blob_offset;
      length = std::min<uint32_t>(length_left, length);
    }

    uint32_t bo = ep->blob_offset;
    while (length_left > 0) {
      uint64_t csum_val = 0;
      if (bblob.has_csum()) {
	ceph_assert(csum_cnt > csum_i);
	csum_val = bblob.get_csum_item(csum_i);
	++csum_i;
      }
      //extract AU from extents
      uint64_t disk_extent_left; // length till the end of disk extent
      uint64_t disk_offset = bblob.calc_offset(bo, &disk_extent_left);
      bluestore_extent_ref_map_t::debug_len_cnt l_c = {0, std::numeric_limits<uint32_t>::max()};
      if (bblob.is_shared()) {
	l_c = ref_map->debug_peek(disk_offset);
	if (l_c.len < length) {
	  length = l_c.len;
	}
      }
      res.emplace_back(disk_offset, length, csum_val, l_c.cnt);
      bo += length;
      length_left -= length;
      length = chunk_size;
    };
  }
  return res;
}

namespace bluestore {
std::ostream& operator<<(std::ostream& out, const bluestore::ExtentMap::debug_au_vector_t& auv)
{
  out << "[";
  for (size_t i = 0; i < auv.size(); ++i) {
    if (i != 0) {
      out << " ";
    }
    out << "0x" << std::hex;
    if (auv[i].disk_offset != -1ULL) {
      out << auv[i].disk_offset << "~" << auv[i].disk_length
	  << "(" << std::dec << int32_t(auv[i].ref_cnts)
	  << "):" << std::hex << auv[i].chksum;
    } else {
      out << "~" << auv[i].disk_length << std::dec;
    }
  }
  out << "]" << std::dec;
  return out;
}
}

// SharedBlob

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.sharedblob(" << this << ") "
#undef dout_context
#define dout_context collection->store->cct

void bluestore::SharedBlob::dump(Formatter* f) const
{
  f->dump_bool("loaded", loaded);
  if (loaded) {
    persistent->dump(f);
  } else {
    f->dump_unsigned("sbid_unloaded", sbid_unloaded);
  }
}

namespace bluestore {
ostream& operator<<(ostream& out, const bluestore::SharedBlob& sb)
{
  out << "SharedBlob(" << &sb;
  
  if (sb.loaded) {
    out << " loaded " << *sb.persistent;
  } else {
    out << " sbid 0x" << std::hex << sb.sbid_unloaded << std::dec;
  }
  return out << ")";
}
}

bluestore::SharedBlob::SharedBlob(uint64_t i, BlueStore::Collection *_coll)
  : collection(_coll), sbid_unloaded(i)
{
  ceph_assert(sbid_unloaded > 0);
}

bluestore::SharedBlob::~SharedBlob()
{
  if (loaded && persistent) {
    delete persistent; 
  }
}

void bluestore::SharedBlob::put()
{
  if (--nref == 0) {
    dout(20) << __func__ << " " << this
	     << " removing self from set " << get_parent()
	     << dendl;
  again:
    auto coll_snap = collection;
    if (coll_snap) {
      std::lock_guard l(coll_snap->cache->lock);
      if (coll_snap != collection) {
	goto again;
      }
      if (!coll_snap->shared_blob_set.remove(this, true)) {
	// race with lookup
	return;
      }
    }
    delete this;
  }
}

void bluestore::SharedBlob::get_ref(uint64_t offset, uint32_t length)
{
  ceph_assert(persistent);
  persistent->ref_map.get(offset, length);
}

void bluestore::SharedBlob::put_ref(uint64_t offset, uint32_t length,
				    PExtentVector *r,
				    bool *unshare)
{
  ceph_assert(persistent);
  persistent->ref_map.put(offset, length, r,
    unshare && !*unshare ? unshare : nullptr);
}