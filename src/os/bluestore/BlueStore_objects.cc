// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <bit>
#include <utility>
#include <memory>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>

#include <boost/container/flat_set.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_real.hpp>

#include "common/dout.h"
#include "include/cpp-btree/btree_set.h"

#include "BlueStore.h"
#include "BlueStore_objects.h"
#include "bluestore_common.h"
#include "os/bluestore/bluestore_types.h"
#include "simple_bitmap.h"
#include "os/kv.h"
#include "include/compat.h"
#include "include/intarith.h"
#include "include/stringify.h"
#include "include/str_map.h"
#include "include/util.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/PriorityCache.h"
#include "common/url_escape.h"
#include "Allocator.h"
#include "FreelistManager.h"
#include "BlueFS.h"
#include "BlueRocksEnv.h"
#include "auth/Crypto.h"
#include "common/EventTrace.h"
#include "perfglue/heap_profiler.h"
#include "common/blkdev.h"
#include "common/numa.h"
#include "common/pretty_binary.h"
#include "common/WorkQueue.h"
#include "kv/KeyValueHistogram.h"
#include "Writer.h"
#include "Compression.h"
#include "BlueAdmin.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore

using std::byte;
using std::deque;
using std::min;
using std::make_pair;
using std::numeric_limits;
using std::pair;
using std::less;
using std::list;
using std::make_unique;
using std::map;
using std::max;
using std::ostream;
using std::ostringstream;
using std::set;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::coarse_mono_clock;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::make_timespan;
using ceph::mono_clock;
using ceph::mono_time;
using ceph::timespan_str;

using namespace std::literals;

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
  ostream& operator<<(ostream& out, const Blob& b)
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
  CephContext* cct = coll->store->cct;
  ceph_assert(get_blob().get_logical_length() != 0);
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
  CephContext* cct = coll->store->cct;
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
  ceph_assert(blob.can_split());
  ceph_assert(used_in_blob.can_split());
  bluestore_blob_t &lb = dirty_blob();
  bluestore_blob_t &rb = r->dirty_blob();

  used_in_blob.split(
    blob_offset,
    &(r->used_in_blob));

  lb.split(blob_offset, rb);

  dout(10) << __func__ << " 0x" << std::hex << blob_offset << std::dec
	   << " finish " << *this << dendl;
  dout(10) << __func__ << " 0x" << std::hex << blob_offset << std::dec
	   << "    and " << *r << dendl;
}


void bluestore::Blob::maybe_prune_tail() {
  if (get_blob().can_prune_tail()) {
    dirty_blob().prune_tail();
    used_in_blob.prune_tail(get_blob().get_ondisk_length());
    dout(20) << __func__ << " pruned tail, now " << get_blob() << dendl;
  }
}

void bluestore::Blob::decode(
  bufferptr::const_iterator& p,
  uint64_t struct_v,
  uint64_t* sbid,
  bool include_ref_map,
  BlueStore::Collection *coll)
{
  denc(blob, p, struct_v);
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
        for (auto r : legacy_ref_map.ref_map) {
          get_ref(
            coll,
            r.first,
            r.second.refs * r.second.length);
        }
      }
    }
  }
}
