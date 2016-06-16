// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "Striper.h"

#include "include/types.h"
#include "include/buffer.h"
#include "osd/OSDMap.h"

#include "common/config.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_striper
#undef dout_prefix
#define dout_prefix *_dout << "striper "


void Striper::file_to_extents(CephContext *cct, const char *object_format,
			      const file_layout_t *layout,
			      uint64_t offset, uint64_t len,
			      uint64_t trunc_size,
			      vector<ObjectExtent>& extents,
			      uint64_t buffer_offset)
{
  map<object_t,vector<ObjectExtent> > object_extents;
  file_to_extents(cct, object_format, layout, offset, len, trunc_size,
		  object_extents, buffer_offset);
  assimilate_extents(object_extents, extents);
}

void Striper::file_to_extents(
  CephContext *cct, const char *object_format,
  const file_layout_t *layout,
  uint64_t offset, uint64_t len,
  uint64_t trunc_size,
  map<object_t,vector<ObjectExtent> >& object_extents,
  uint64_t buffer_offset)
{
  ldout(cct, 10) << "file_to_extents " << offset << "~" << len
		 << " format " << object_format
		 << dendl;
  assert(len > 0);

  /*
   * we want only one extent per object!  this means that each extent
   * we read may map into different bits of the final read
   * buffer.. hence ObjectExtent.buffer_extents
   */

  __u32 object_size = layout->object_size;
  __u32 su = layout->stripe_unit;
  __u32 stripe_count = layout->stripe_count;
  assert(object_size >= su);
  if (stripe_count == 1) {
    ldout(cct, 20) << " sc is one, reset su to os" << dendl;
    su = object_size;
  }
  uint64_t stripes_per_object = object_size / su;
  ldout(cct, 20) << " su " << su << " sc " << stripe_count << " os "
		 << object_size << " stripes_per_object " << stripes_per_object
		 << dendl;

  uint64_t cur = offset;
  uint64_t left = len;
  while (left > 0) {
    // layout into objects
    uint64_t blockno = cur / su; // which block
    // which horizontal stripe (Y)
    uint64_t stripeno = blockno / stripe_count;
    // which object in the object set (X)
    uint64_t stripepos = blockno % stripe_count;
    // which object set
    uint64_t objectsetno = stripeno / stripes_per_object;
    // object id
    uint64_t objectno = objectsetno * stripe_count + stripepos;

    // find oid, extent
    char buf[strlen(object_format) + 32];
    snprintf(buf, sizeof(buf), object_format, (long long unsigned)objectno);
    object_t oid = buf;

    // map range into object
    uint64_t block_start = (stripeno % stripes_per_object) * su;
    uint64_t block_off = cur % su;
    uint64_t max = su - block_off;

    uint64_t x_offset = block_start + block_off;
    uint64_t x_len;
    if (left > max)
      x_len = max;
    else
      x_len = left;

    ldout(cct, 20) << " off " << cur << " blockno " << blockno << " stripeno "
		   << stripeno << " stripepos " << stripepos << " objectsetno "
		   << objectsetno << " objectno " << objectno
		   << " block_start " << block_start << " block_off "
		   << block_off << " " << x_offset << "~" << x_len
		   << dendl;

    ObjectExtent *ex = 0;
    vector<ObjectExtent>& exv = object_extents[oid];
    if (exv.empty() || exv.back().offset + exv.back().length != x_offset) {
      exv.resize(exv.size() + 1);
      ex = &exv.back();
      ex->oid = oid;
      ex->objectno = objectno;
      ex->oloc = OSDMap::file_to_object_locator(*layout);

      ex->offset = x_offset;
      ex->length = x_len;
      ex->truncate_size = object_truncate_size(cct, layout, objectno,
					       trunc_size);

      ldout(cct, 20) << " added new " << *ex << dendl;
    } else {
      // add to extent
      ex = &exv.back();
      ldout(cct, 20) << " adding in to " << *ex << dendl;
      ex->length += x_len;
    }
    ex->buffer_extents.push_back(make_pair(cur - offset + buffer_offset,
					   x_len));

    ldout(cct, 15) << "file_to_extents  " << *ex << " in " << ex->oloc
		   << dendl;
    // ldout(cct, 0) << "map: ino " << ino << " oid " << ex.oid << " osd "
    //		  << ex.osd << " offset " << ex.offset << " len " << ex.len
    //		  << " ... left " << left << dendl;

    left -= x_len;
    cur += x_len;
  }
}

void Striper::assimilate_extents(
  map<object_t,vector<ObjectExtent> >& object_extents,
  vector<ObjectExtent>& extents)
{
  // make final list
  for (map<object_t, vector<ObjectExtent> >::iterator it
	 = object_extents.begin();
       it != object_extents.end();
       ++it) {
    for (vector<ObjectExtent>::iterator p = it->second.begin();
	 p != it->second.end();
	 ++p) {
      extents.push_back(*p);
    }
  }
}

void Striper::extent_to_file(CephContext *cct, file_layout_t *layout,
			   uint64_t objectno, uint64_t off, uint64_t len,
			   vector<pair<uint64_t, uint64_t> >& extents)
{
  ldout(cct, 10) << "extent_to_file " << objectno << " " << off << "~"
		 << len << dendl;

  __u32 object_size = layout->object_size;
  __u32 su = layout->stripe_unit;
  __u32 stripe_count = layout->stripe_count;
  assert(object_size >= su);
  uint64_t stripes_per_object = object_size / su;
  ldout(cct, 20) << " stripes_per_object " << stripes_per_object << dendl;

  uint64_t off_in_block = off % su;

  extents.reserve(len / su + 1);

  while (len > 0) {
    uint64_t stripepos = objectno % stripe_count;
    uint64_t objectsetno = objectno / stripe_count;
    uint64_t stripeno = off / su + objectsetno * stripes_per_object;
    uint64_t blockno = stripeno * stripe_count + stripepos;
    uint64_t extent_off = blockno * su + off_in_block;
    uint64_t extent_len = MIN(len, su - off_in_block);
    extents.push_back(make_pair(extent_off, extent_len));

    ldout(cct, 20) << " object " << off << "~" << extent_len
		   << " -> file " << extent_off << "~" << extent_len
		   << dendl;

    off_in_block = 0;
    off += extent_len;
    len -= extent_len;
  }
}

uint64_t Striper::object_truncate_size(CephContext *cct,
				       const file_layout_t *layout,
				       uint64_t objectno, uint64_t trunc_size)
{
  uint64_t obj_trunc_size;
  if (trunc_size == 0 || trunc_size == (uint64_t)-1) {
    obj_trunc_size = trunc_size;
  } else {
    __u32 object_size = layout->object_size;
    __u32 su = layout->stripe_unit;
    __u32 stripe_count = layout->stripe_count;
    assert(object_size >= su);
    uint64_t stripes_per_object = object_size / su;

    uint64_t objectsetno = objectno / stripe_count;
    uint64_t trunc_objectsetno = trunc_size / object_size / stripe_count;
    if (objectsetno > trunc_objectsetno)
      obj_trunc_size = 0;
    else if (objectsetno < trunc_objectsetno)
      obj_trunc_size = object_size;
    else {
      uint64_t trunc_blockno = trunc_size / su;
      uint64_t trunc_stripeno = trunc_blockno / stripe_count;
      uint64_t trunc_stripepos = trunc_blockno % stripe_count;
      uint64_t trunc_objectno = trunc_objectsetno * stripe_count
	+ trunc_stripepos;
      if (objectno < trunc_objectno)
	obj_trunc_size = ((trunc_stripeno % stripes_per_object) + 1) * su;
      else if (objectno > trunc_objectno)
	obj_trunc_size = (trunc_stripeno % stripes_per_object) * su;
      else
	obj_trunc_size = (trunc_stripeno % stripes_per_object) * su
	  + (trunc_size % su);
    }
  }
  ldout(cct, 20) << "object_truncate_size " << objectno << " "
		 << trunc_size << "->" << obj_trunc_size << dendl;
  return obj_trunc_size;
}

uint64_t Striper::get_num_objects(const file_layout_t& layout,
				  uint64_t size)
{
  __u32 stripe_unit = layout.stripe_unit;
  __u32 stripe_count = layout.stripe_count;
  uint64_t period = layout.get_period();
  uint64_t num_periods = (size + period - 1) / period;
  uint64_t remainder_bytes = size % period;
  uint64_t remainder_objs = 0;
  if ((remainder_bytes > 0) && (remainder_bytes < (uint64_t)stripe_count
				* stripe_unit))
    remainder_objs = stripe_count - ((remainder_bytes + stripe_unit - 1)
				     / stripe_unit);
  return num_periods * stripe_count - remainder_objs;
}

// StripedReadResult

void Striper::StripedReadResult::add_partial_result(
  CephContext *cct, bufferlist& bl,
  const vector<pair<uint64_t,uint64_t> >& buffer_extents)
{
  ldout(cct, 10) << "add_partial_result(" << this << ") " << bl.length()
		 << " to " << buffer_extents << dendl;
  for (vector<pair<uint64_t,uint64_t> >::const_iterator p
	 = buffer_extents.begin();
       p != buffer_extents.end();
       ++p) {
    pair<bufferlist, uint64_t>& r = partial[p->first];
    size_t actual = MIN(bl.length(), p->second);
    bl.splice(0, actual, &r.first);
    r.second = p->second;
    total_intended_len += r.second;
  }
}

void Striper::StripedReadResult::add_partial_sparse_result(
  CephContext *cct, bufferlist& bl, const map<uint64_t, uint64_t>& bl_map,
  uint64_t bl_off, const vector<pair<uint64_t,uint64_t> >& buffer_extents)
{
  ldout(cct, 10) << "add_partial_sparse_result(" << this << ") " << bl.length()
		 << " covering " << bl_map << " (offset " << bl_off << ")"
		 << " to " << buffer_extents << dendl;
  map<uint64_t, uint64_t>::const_iterator s = bl_map.begin();
  for (vector<pair<uint64_t,uint64_t> >::const_iterator p
	 = buffer_extents.begin();
       p != buffer_extents.end();
       ++p) {
    uint64_t tofs = p->first;
    uint64_t tlen = p->second;
    ldout(cct, 30) << " be " << tofs << "~" << tlen << dendl;
    while (tlen > 0) {
      ldout(cct, 20) << "  t " << tofs << "~" << tlen
		     << " bl has " << bl.length()
		     << " off " << bl_off
		     << dendl;
      if (s == bl_map.end()) {
	ldout(cct, 20) << "  s at end" << dendl;
	pair<bufferlist, uint64_t>& r = partial[tofs];
	r.second = tlen;
	total_intended_len += r.second;
	break;
      }

      ldout(cct, 30) << "  s " << s->first << "~" << s->second << dendl;

      // skip zero-length extent
      if (s->second == 0) {
	ldout(cct, 30) << "  s len 0, skipping" << dendl;
	++s;
	continue;
      }

      if (s->first > bl_off) {
	// gap in sparse read result
	pair<bufferlist, uint64_t>& r = partial[tofs];
	size_t gap = MIN(s->first - bl_off, tlen);
	ldout(cct, 20) << "  s gap " << gap << ", skipping" << dendl;
	r.second = gap;
	total_intended_len += r.second;
	bl_off += gap;
	tofs += gap;
	tlen -= gap;
	if (tlen == 0) {
	  continue;
	}
      }

      assert(s->first <= bl_off);
      size_t left = (s->first + s->second) - bl_off;
      size_t actual = MIN(left, tlen);

      if (actual > 0) {
	ldout(cct, 20) << "  s has " << actual << ", copying" << dendl;
	pair<bufferlist, uint64_t>& r = partial[tofs];
	bl.splice(0, actual, &r.first);
	r.second = actual;
	total_intended_len += r.second;
	bl_off += actual;
	tofs += actual;
	tlen -= actual;
      }
      if (actual == left) {
	ldout(cct, 30) << "  s advancing" << dendl;
	++s;
      }
    }
  }
}

void Striper::StripedReadResult::assemble_result(CephContext *cct,
						 bufferlist& bl,
						 bool zero_tail)
{
  ldout(cct, 10) << "assemble_result(" << this << ") zero_tail=" << zero_tail
		 << dendl;

  // go backwards, so that we can efficiently discard zeros
  map<uint64_t,pair<bufferlist,uint64_t> >::reverse_iterator p
    = partial.rbegin();
  if (p == partial.rend())
    return;

  uint64_t end = p->first + p->second.second;
  while (p != partial.rend()) {
    // sanity check
    ldout(cct, 20) << "assemble_result(" << this << ") " << p->first << "~"
		   << p->second.second << " " << p->second.first.length()
		   << " bytes" << dendl;
    assert(p->first == end - p->second.second);
    end = p->first;

    size_t len = p->second.first.length();
    if (len < p->second.second) {
      if (zero_tail || bl.length()) {
        bufferptr bp(p->second.second - len);
        bp.zero();
        bl.push_front(std::move(bp));
	bl.claim_prepend(p->second.first);
      } else {
	bl.claim_prepend(p->second.first);
      }
    } else {
      bl.claim_prepend(p->second.first);
    }
    ++p;
  }
  partial.clear();
}

void Striper::StripedReadResult::assemble_result(CephContext *cct, char *buffer, size_t length)
{

  assert(buffer && length == total_intended_len);

  map<uint64_t,pair<bufferlist,uint64_t> >::reverse_iterator p = partial.rbegin();
  if (p == partial.rend())
    return;

  uint64_t curr = length;
  uint64_t end = p->first + p->second.second;
  while (p != partial.rend()) {
    // sanity check
    ldout(cct, 20) << "assemble_result(" << this << ") " << p->first << "~" << p->second.second
		   << " " << p->second.first.length() << " bytes"
		   << dendl;
    assert(p->first == end - p->second.second);
    end = p->first;

    size_t len = p->second.first.length();
    assert(curr >= p->second.second);
    curr -= p->second.second;
    if (len < p->second.second) {
      if (len)
	p->second.first.copy(0, len, buffer + curr);
      memset(buffer + curr + len, 0, p->second.second - len);
    } else {
      p->second.first.copy(0, len, buffer + curr);
    }
    ++p;
  }
  partial.clear();
  assert(curr == 0);
}

