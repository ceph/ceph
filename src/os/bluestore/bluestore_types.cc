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

#include "bluestore_types.h"
#include "common/Formatter.h"
#include "common/Checksummer.h"
#include "include/stringify.h"

using std::list;
using std::map;
using std::make_pair;
using std::ostream;
using std::string;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::Formatter;

// bluestore_bdev_label_t

void bluestore_bdev_label_t::encode(bufferlist& bl) const
{
  // be slightly friendly to someone who looks at the device
  bl.append("bluestore block device\n");
  bl.append(stringify(osd_uuid));
  bl.append("\n");
  ENCODE_START(2, 1, bl);
  encode(osd_uuid, bl);
  encode(size, bl);
  encode(btime, bl);
  encode(description, bl);
  encode(meta, bl);
  ENCODE_FINISH(bl);
}

void bluestore_bdev_label_t::decode(bufferlist::const_iterator& p)
{
  p += 60u; // see above
  DECODE_START(2, p);
  decode(osd_uuid, p);
  decode(size, p);
  decode(btime, p);
  decode(description, p);
  if (struct_v >= 2) {
    decode(meta, p);
  }
  DECODE_FINISH(p);
}

void bluestore_bdev_label_t::dump(Formatter *f) const
{
  f->dump_stream("osd_uuid") << osd_uuid;
  f->dump_unsigned("size", size);
  f->dump_stream("btime") << btime;
  f->dump_string("description", description);
  for (auto& i : meta) {
    f->dump_string(i.first.c_str(), i.second);
  }
}

void bluestore_bdev_label_t::generate_test_instances(
  list<bluestore_bdev_label_t*>& o)
{
  o.push_back(new bluestore_bdev_label_t);
  o.push_back(new bluestore_bdev_label_t);
  o.back()->size = 123;
  o.back()->btime = utime_t(4, 5);
  o.back()->description = "fakey";
  o.back()->meta["foo"] = "bar";
}

ostream& operator<<(ostream& out, const bluestore_bdev_label_t& l)
{
  return out << "bdev(osd_uuid " << l.osd_uuid
	     << ", size 0x" << std::hex << l.size << std::dec
	     << ", btime " << l.btime
	     << ", desc " << l.description
	     << ", " << l.meta.size() << " meta"
	     << ")";
}

// cnode_t

void bluestore_cnode_t::dump(Formatter *f) const
{
  f->dump_unsigned("bits", bits);
}

void bluestore_cnode_t::generate_test_instances(list<bluestore_cnode_t*>& o)
{
  o.push_back(new bluestore_cnode_t());
  o.push_back(new bluestore_cnode_t(0));
  o.push_back(new bluestore_cnode_t(123));
}

ostream& operator<<(ostream& out, const bluestore_cnode_t& l)
{
  return out << "cnode(bits " << l.bits << ")";
}

// bluestore_extent_ref_map_t

void bluestore_extent_ref_map_t::_check() const
{
  uint64_t pos = 0;
  unsigned refs = 0;
  for (const auto &p : ref_map) {
    if (p.first < pos)
      ceph_abort_msg("overlap");
    if (p.first == pos && p.second.refs == refs)
      ceph_abort_msg("unmerged");
    pos = p.first + p.second.length;
    refs = p.second.refs;
  }
}

void bluestore_extent_ref_map_t::_maybe_merge_left(
  map<uint64_t,record_t>::iterator& p)
{
  if (p == ref_map.begin())
    return;
  auto q = p;
  --q;
  if (q->second.refs == p->second.refs &&
      q->first + q->second.length == p->first) {
    q->second.length += p->second.length;
    ref_map.erase(p);
    p = q;
  }
}

void bluestore_extent_ref_map_t::get(uint64_t offset, uint32_t length)
{
  auto p = ref_map.lower_bound(offset);
  if (p != ref_map.begin()) {
    --p;
    if (p->first + p->second.length <= offset) {
      ++p;
    }
  }
  while (length > 0) {
    if (p == ref_map.end()) {
      // nothing after offset; add the whole thing.
      p = ref_map.insert(
	map<uint64_t,record_t>::value_type(offset, record_t(length, 1))).first;
      break;
    }
    if (p->first > offset) {
      // gap
      uint64_t newlen = std::min<uint64_t>(p->first - offset, length);
      p = ref_map.insert(
	map<uint64_t,record_t>::value_type(offset,
					   record_t(newlen, 1))).first;
      offset += newlen;
      length -= newlen;
      _maybe_merge_left(p);
      ++p;
      continue;
    }
    if (p->first < offset) {
      // split off the portion before offset
      ceph_assert(p->first + p->second.length > offset);
      uint64_t left = p->first + p->second.length - offset;
      p->second.length = offset - p->first;
      p = ref_map.insert(map<uint64_t,record_t>::value_type(
			   offset, record_t(left, p->second.refs))).first;
      // continue below
    }
    ceph_assert(p->first == offset);
    if (length < p->second.length) {
      ref_map.insert(make_pair(offset + length,
			       record_t(p->second.length - length,
					p->second.refs)));
      p->second.length = length;
      ++p->second.refs;
      break;
    }
    ++p->second.refs;
    offset += p->second.length;
    length -= p->second.length;
    _maybe_merge_left(p);
    ++p;
  }
  if (p != ref_map.end())
    _maybe_merge_left(p);
  //_check();
}

void bluestore_extent_ref_map_t::put(
  uint64_t offset, uint32_t length,
  PExtentVector *release,
  bool *maybe_unshared)
{
  //NB: existing entries in 'release' container must be preserved!
  bool unshared = true;
  auto p = ref_map.lower_bound(offset);
  if (p == ref_map.end() || p->first > offset) {
    if (p == ref_map.begin()) {
      ceph_abort_msg("put on missing extent (nothing before)");
    }
    --p;
    if (p->first + p->second.length <= offset) {
      ceph_abort_msg("put on missing extent (gap)");
    }
  }
  if (p->first < offset) {
    uint64_t left = p->first + p->second.length - offset;
    p->second.length = offset - p->first;
    if (p->second.refs != 1) {
      unshared = false;
    }
    p = ref_map.insert(map<uint64_t,record_t>::value_type(
			 offset, record_t(left, p->second.refs))).first;
  }
  while (length > 0) {
    ceph_assert(p->first == offset);
    if (length < p->second.length) {
      if (p->second.refs != 1) {
	unshared = false;
      }
      ref_map.insert(make_pair(offset + length,
			       record_t(p->second.length - length,
					p->second.refs)));
      if (p->second.refs > 1) {
	p->second.length = length;
	--p->second.refs;
	if (p->second.refs != 1) {
	  unshared = false;
	}
	_maybe_merge_left(p);
      } else {
	if (release)
	  release->push_back(bluestore_pextent_t(p->first, length));
	ref_map.erase(p);
      }
      goto out;
    }
    offset += p->second.length;
    length -= p->second.length;
    if (p->second.refs > 1) {
      --p->second.refs;
      if (p->second.refs != 1) {
	unshared = false;
      }
      _maybe_merge_left(p);
      ++p;
    } else {
      if (release)
	release->push_back(bluestore_pextent_t(p->first, p->second.length));
      ref_map.erase(p++);
    }
  }
  if (p != ref_map.end())
    _maybe_merge_left(p);
  //_check();
out:
  if (maybe_unshared) {
    if (unshared) {
      // we haven't seen a ref != 1 yet; check the whole map.
      for (auto& p : ref_map) {
	if (p.second.refs != 1) {
	  unshared = false;
	  break;
	}
      }
    }
    *maybe_unshared = unshared;
  }
}

/// Inspects reference region at specified offset.
/// If reference region is located, returns its reference count and
/// distance from offset to the end of the region.
/// If reference region is not located, returns 0 reference count and
/// distance to the next region.
/// If offset is beyond last region, returns 0 for both
/// reference count and the distance.
bluestore_extent_ref_map_t::debug_len_cnt bluestore_extent_ref_map_t::debug_peek(uint64_t offset) const
{
  // locate offset
  auto p = ref_map.lower_bound(offset);
  if (p != ref_map.end() && p->first == offset) {
    // direct request for us
    return {p->second.length, p->second.refs};
  }
  if (p != ref_map.begin()) {
    --p;
    if (p->first + p->second.length <= offset) {
      // nah, it ends too soon, we landed in a hole
      ++p;
      if (p != ref_map.end()) {
	// there is a region after
	return {uint32_t(p->first - offset), 0};
      } else {
	// nothing after
	return {0, 0};
      }
    } else {
      // we're in the range
      return {uint32_t((p->first + p->second.length) - offset), p->second.refs};
    }
  } else {
    return {uint32_t(p->first - offset), 0};
  }
}

bool bluestore_extent_ref_map_t::contains(uint64_t offset, uint32_t length) const
{
  auto p = ref_map.lower_bound(offset);
  if (p == ref_map.end() || p->first > offset) {
    if (p == ref_map.begin()) {
      return false; // nothing before
    }
    --p;
    if (p->first + p->second.length <= offset) {
      return false; // gap
    }
  }
  while (length > 0) {
    if (p == ref_map.end())
      return false;
    if (p->first > offset)
      return false;
    if (p->first + p->second.length >= offset + length)
      return true;
    uint64_t overlap = p->first + p->second.length - offset;
    offset += overlap;
    length -= overlap;
    ++p;
  }
  return true;
}

bool bluestore_extent_ref_map_t::intersects(
  uint64_t offset,
  uint32_t length) const
{
  auto p = ref_map.lower_bound(offset);
  if (p != ref_map.begin()) {
    --p;
    if (p->first + p->second.length <= offset) {
      ++p;
    }
  }
  if (p == ref_map.end())
    return false;
  if (p->first >= offset + length)
    return false;
  return true;  // intersects p!
}

void bluestore_extent_ref_map_t::dump(Formatter *f) const
{
  f->open_array_section("ref_map");
  for (auto& p : ref_map) {
    f->open_object_section("ref");
    f->dump_unsigned("offset", p.first);
    f->dump_unsigned("length", p.second.length);
    f->dump_unsigned("refs", p.second.refs);
    f->close_section();
  }
  f->close_section();
}

void bluestore_extent_ref_map_t::generate_test_instances(
  list<bluestore_extent_ref_map_t*>& o)
{
  o.push_back(new bluestore_extent_ref_map_t);
  o.push_back(new bluestore_extent_ref_map_t);
  o.back()->get(10, 10);
  o.back()->get(18, 22);
  o.back()->get(20, 20);
  o.back()->get(10, 25);
  o.back()->get(15, 20);
}

ostream& operator<<(ostream& out, const bluestore_extent_ref_map_t& m)
{
  out << "ref_map(";
  for (auto p = m.ref_map.begin(); p != m.ref_map.end(); ++p) {
    if (p != m.ref_map.begin())
      out << ",";
    out << std::hex << "0x" << p->first << "~" << p->second.length << std::dec
	<< "=" << p->second.refs;
  }
  out << ")";
  return out;
}

// bluestore_blob_use_tracker_t
bluestore_blob_use_tracker_t::bluestore_blob_use_tracker_t(
  const bluestore_blob_use_tracker_t& tracker)
 : au_size{tracker.au_size},
   num_au(0),
   alloc_au(0),
   bytes_per_au{nullptr}
{
  if (tracker.num_au > 0) {
    allocate(tracker.num_au);
    std::copy(tracker.bytes_per_au, tracker.bytes_per_au + num_au, bytes_per_au);
  } else {
    total_bytes = tracker.total_bytes;
  }
}

bluestore_blob_use_tracker_t&
bluestore_blob_use_tracker_t::operator=(const bluestore_blob_use_tracker_t& rhs)
{
  if (this == &rhs) {
    return *this;
  }
  clear();
  au_size = rhs.au_size;
  au_size_exponent = std::countr_zero(au_size);
  if (rhs.num_au > 0) {
    allocate( rhs.num_au);
    std::copy(rhs.bytes_per_au, rhs.bytes_per_au + num_au, bytes_per_au);
  } else {
    total_bytes = rhs.total_bytes;
  }
  return *this;
}

void bluestore_blob_use_tracker_t::allocate(uint32_t au_count)
{
  ceph_assert(au_count != 0);
  ceph_assert(num_au == 0);
  ceph_assert(alloc_au == 0);
  num_au = alloc_au = au_count;
  bytes_per_au = new uint32_t[alloc_au];
  mempool::get_pool(
    mempool::pool_index_t(mempool::mempool_bluestore_cache_other)).
      adjust_count(alloc_au, sizeof(uint32_t) * alloc_au);

  for (uint32_t i = 0; i < num_au; ++i) {
    bytes_per_au[i] = 0;
  }
}

void bluestore_blob_use_tracker_t::release(uint32_t au_count, uint32_t* ptr) {
  if (au_count) {
    delete[] ptr;
    mempool::get_pool(
      mempool::pool_index_t(mempool::mempool_bluestore_cache_other)).
        adjust_count(-(int32_t)au_count, -(int32_t)(sizeof(uint32_t) * au_count));
  }
}

void bluestore_blob_use_tracker_t::init(
  uint32_t full_length, uint32_t _au_size) {
  ceph_assert(!au_size || is_empty()); 
  ceph_assert(_au_size > 0);
  ceph_assert(full_length > 0);
  clear();  
  au_size_exponent = std::countr_zero(_au_size);
  uint32_t _num_au = round_up_to(full_length, _au_size) >> au_size_exponent;
  au_size = _au_size;
  if ( _num_au > 1 ) {
    // in case of more than one allocation we assume that allocation unit is a power of 2
    // so that we can optimize division.
    ceph_assert((_au_size & (_au_size - 1)) == 0);
    allocate(_num_au);
  }
}

void bluestore_blob_use_tracker_t::get(
  uint32_t offset, uint32_t length)
{
  ceph_assert(au_size);
  if (!num_au) {
    total_bytes += length;
  } else {
    auto end = offset + length;
    if (end >> au_size_exponent >= num_au) {
      add_tail(end, au_size);
    }
    while (offset < end) {
      auto phase = offset & (au_size - 1);
      bytes_per_au[offset >> au_size_exponent] += 
	std::min(au_size - phase, end - offset);
      offset += (phase ? au_size - phase : au_size);
    }
  }
}

bool bluestore_blob_use_tracker_t::put(
  uint32_t offset, uint32_t length,
  PExtentVector *release_units)
{
  ceph_assert(au_size);
  if (release_units) {
    release_units->clear();
  }
  bool maybe_empty = true;
  if (!num_au) {
    ceph_assert(total_bytes >= length);
    total_bytes -= length;
  } else {
    auto end = offset + length;
    uint64_t next_offs = 0;
    while (offset < end) {
      auto phase = offset % au_size;
      size_t pos = offset >> au_size_exponent;
      auto diff = std::min(au_size - phase, end - offset);
      ceph_assert(diff <= bytes_per_au[pos]);
      bytes_per_au[pos] -= diff;
      offset += (phase ? au_size - phase : au_size);
      if (bytes_per_au[pos] == 0) {
	if (release_units) {
          if (release_units->empty() || next_offs != pos * au_size) {
  	    release_units->emplace_back(pos * au_size, au_size);
            next_offs = pos * au_size;
          } else {
            release_units->back().length += au_size;
          }
          next_offs += au_size;
	}
      } else {
	maybe_empty = false; // micro optimization detecting we aren't empty 
	                     // even in the affected extent
      }
    }
  }
  bool empty = maybe_empty ? !is_not_empty() : false;
  if (empty && release_units) {
    release_units->clear();
  }
  return empty;
}

bool bluestore_blob_use_tracker_t::can_split() const
{
  return num_au > 0;
}

bool bluestore_blob_use_tracker_t::can_split_at(uint32_t blob_offset) const
{
  ceph_assert(au_size);
  return (blob_offset % au_size) == 0 &&
         blob_offset < num_au * au_size;
}

void bluestore_blob_use_tracker_t::split(
  uint32_t blob_offset,
  bluestore_blob_use_tracker_t* r)
{
  ceph_assert(au_size);
  ceph_assert(can_split());
  ceph_assert(can_split_at(blob_offset));
  ceph_assert(r->is_empty());
  
  uint32_t new_num_au = blob_offset >> au_size_exponent;
  r->init( (num_au - new_num_au) * au_size, au_size);

  for (auto i = new_num_au; i < num_au; i++) {
    r->get((i - new_num_au) * au_size, bytes_per_au[i]);
    bytes_per_au[i] = 0;
  }
  if (new_num_au == 0) {
    clear();
  } else if (new_num_au == 1) {
    uint32_t tmp = bytes_per_au[0];
    uint32_t _au_size = au_size;
    clear();
    au_size = _au_size;
    au_size_exponent = std::countr_zero(au_size);
    total_bytes = tmp;
  } else {
    num_au = new_num_au;
  }
}

void bluestore_blob_use_tracker_t::dup(const bluestore_blob_use_tracker_t& from,
				       uint32_t start, uint32_t len)
{
  uint32_t end = start + len;
  ceph_assert(from.total_bytes >= end);
  init(end, from.au_size);
  uint32_t* array = dirty_au_array();
  const uint32_t* afrom = from.get_au_array();
  for (uint32_t i = start >> au_size_exponent, pos = start; pos < end; i++, pos += au_size) {
    array[i] = afrom[i];
  }
}

bool bluestore_blob_use_tracker_t::equal(
  const bluestore_blob_use_tracker_t& other) const
{
  if (!num_au && !other.num_au) {
    return total_bytes == other.total_bytes && au_size == other.au_size;
  } else if (num_au && other.num_au) {
    if (num_au != other.num_au || au_size != other.au_size) {
      return false;
    }
    for (size_t i = 0; i < num_au; i++) {
      if (bytes_per_au[i] != other.bytes_per_au[i]) {
        return false;
      }
    }
    return true;
  }

  uint32_t n = num_au ? num_au : other.num_au;
  uint32_t referenced = 
    num_au ? other.get_referenced_bytes() : get_referenced_bytes();
   auto bytes_per_au_tmp = num_au ? bytes_per_au : other.bytes_per_au;
  uint32_t my_referenced = 0;
  for (size_t i = 0; i < n; i++) {
    my_referenced += bytes_per_au_tmp[i];
    if (my_referenced > referenced) {
      return false;
    }
  }
  return my_referenced == referenced;
}

void bluestore_blob_use_tracker_t::dump(Formatter *f) const
{
  f->dump_unsigned("num_au", num_au);
  f->dump_unsigned("au_size", au_size);
  if (!num_au) {
    f->dump_unsigned("total_bytes", total_bytes);
  } else {
    f->open_array_section("bytes_per_au");
    for (size_t i = 0; i < num_au; ++i) {
      f->dump_unsigned("", bytes_per_au[i]);
    }
    f->close_section();
  }
}

void bluestore_blob_use_tracker_t::generate_test_instances(
  list<bluestore_blob_use_tracker_t*>& o)
{
  o.push_back(new bluestore_blob_use_tracker_t());
  o.back()->init(16, 16);
  o.back()->get(10, 10);
  o.back()->get(10, 5);
  o.push_back(new bluestore_blob_use_tracker_t());
  o.back()->init(60, 16);
  o.back()->get(18, 22);
  o.back()->get(20, 20);
  o.back()->get(15, 20);
}

ostream& operator<<(ostream& out, const bluestore_blob_use_tracker_t& m)
{
  out << "use_tracker(" << std::hex;
  if (!m.num_au) {
    out << "0x" << m.au_size 
        << " "
        << "0x" << m.total_bytes;
  } else {
    out << "0x" << m.num_au 
        << "*0x" << m.au_size 
	<< " 0x[";
    for (size_t i = 0; i < m.num_au; ++i) {
      if (i != 0)
	out << ",";
      out << m.bytes_per_au[i];
    }
    out << "]";
  }
  out << std::dec << ")";
  return out;
}

// bluestore_pextent_t

void bluestore_pextent_t::dump(Formatter *f) const
{
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

ostream& operator<<(ostream& out, const bluestore_pextent_t& o) {
  if (o.is_valid())
    return out << "0x" << std::hex << o.offset << "~" << o.length << std::dec;
  else
    return out << "!~" << std::hex << o.length << std::dec;
}

void bluestore_pextent_t::generate_test_instances(list<bluestore_pextent_t*>& ls)
{
  ls.push_back(new bluestore_pextent_t);
  ls.push_back(new bluestore_pextent_t(1, 2));
}

// bluestore_blob_t

string bluestore_blob_t::get_flags_string(unsigned flags)
{
  string s;
  if (flags & FLAG_COMPRESSED) {
    if (s.length())
      s += '+';
    s += "compressed";
  }
  if (flags & FLAG_CSUM) {
    if (s.length())
      s += '+';
    s += "csum";
  }
  if (flags & FLAG_HAS_UNUSED) {
    if (s.length())
      s += '+';
    s += "has_unused";
  }
  if (flags & FLAG_SHARED) {
    if (s.length())
      s += '+';
    s += "shared";
  }

  return s;
}

void bluestore_blob_t::adjust_to(const bluestore_blob_t& other, uint32_t target_length)
{
  // there is no way to expand compressed
  ceph_assert(!is_compressed());
  // never import data from other compressed
  ceph_assert(!other.is_compressed());
  // unused is wanky, as it is based on logical_length size
  // it could be cleared here, but it feels better to force caller
  // to be aware that unused is inacceptable
  ceph_assert(!has_unused());
  ceph_assert(logical_length == 0); // not initialized yet
  ceph_assert(target_length <= other.logical_length);

  logical_length = target_length;
  ceph_assert(!has_csum());
  if (other.has_csum()) {
    init_csum(other.csum_type, other.csum_chunk_order, logical_length);
    ceph_assert(csum_data.length() <= other.csum_data.length());
    memcpy(csum_data.c_str(), other.csum_data.c_str(), csum_data.length());
  }
  compressed_length = 0;
}

size_t bluestore_blob_t::get_csum_value_size() const 
{
  return Checksummer::get_csum_value_size(csum_type);
}

void bluestore_blob_t::dump(Formatter *f) const
{
  f->open_array_section("extents");
  for (auto& p : extents) {
    f->dump_object("extent", p);
  }
  f->close_section();
  f->dump_unsigned("logical_length", logical_length);
  f->dump_unsigned("compressed_length", compressed_length);
  f->dump_unsigned("flags", flags);
  f->dump_unsigned("csum_type", csum_type);
  f->dump_unsigned("csum_chunk_order", csum_chunk_order);
  f->open_array_section("csum_data");
  size_t n = get_csum_count();
  for (unsigned i = 0; i < n; ++i)
    f->dump_unsigned("csum", get_csum_item(i));
  f->close_section();
  f->dump_unsigned("unused", unused);
}

void bluestore_blob_t::generate_test_instances(list<bluestore_blob_t*>& ls)
{
  ls.push_back(new bluestore_blob_t);
  ls.push_back(new bluestore_blob_t(0));
  ls.push_back(new bluestore_blob_t);
  ls.back()->allocated_test(bluestore_pextent_t(111, 222));
  ls.push_back(new bluestore_blob_t);
  ls.back()->init_csum(Checksummer::CSUM_XXHASH32, 16, 65536);
  ls.back()->csum_data = ceph::buffer::claim_malloc(4, strdup("abcd"));
  ls.back()->add_unused(0, 3);
  ls.back()->add_unused(8, 8);
  ls.back()->allocated_test(bluestore_pextent_t(0x40100000, 0x10000));
  ls.back()->allocated_test(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x1000));
  ls.back()->allocated_test(bluestore_pextent_t(0x40120000, 0x10000));
}

ostream& operator<<(ostream& out, const bluestore_blob_t& o)
{
  out << "blob(" << o.get_extents();
  if (o.is_compressed()) {
    out << " clen 0x" << std::hex
	<< o.get_logical_length()
	<< " -> 0x"
	<< o.get_compressed_payload_length()
	<< std::dec;
  } else {
    out << " llen=0x" << std::hex << o.get_logical_length() << std::dec;
  }
  if (o.flags) {
    out << " " << o.get_flags_string();
  }
  if (o.has_csum()) {
    out << " " << Checksummer::get_csum_type_string(o.csum_type)
	<< "/0x" << std::hex << (1ull << o.csum_chunk_order) << std::dec
	<< "/" << o.csum_data.length();
  }
  if (o.has_unused())
    out << " unused=0x" << std::hex << o.unused << std::dec;
  out << ")";
  return out;
}

void bluestore_blob_t::calc_csum(uint64_t b_off, const bufferlist& bl)
{
  switch (csum_type) {
  case Checksummer::CSUM_XXHASH32:
    Checksummer::calculate<Checksummer::xxhash32>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  case Checksummer::CSUM_XXHASH64:
    Checksummer::calculate<Checksummer::xxhash64>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;;
  case Checksummer::CSUM_CRC32C:
    Checksummer::calculate<Checksummer::crc32c>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  case Checksummer::CSUM_CRC32C_16:
    Checksummer::calculate<Checksummer::crc32c_16>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  case Checksummer::CSUM_CRC32C_8:
    Checksummer::calculate<Checksummer::crc32c_8>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  }
}

int bluestore_blob_t::verify_csum(uint64_t b_off, const bufferlist& bl,
				  int* b_bad_off, uint64_t *bad_csum) const
{
  int r = 0;

  *b_bad_off = -1;
  switch (csum_type) {
  case Checksummer::CSUM_NONE:
    break;
  case Checksummer::CSUM_XXHASH32:
    *b_bad_off = Checksummer::verify<Checksummer::xxhash32>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case Checksummer::CSUM_XXHASH64:
    *b_bad_off = Checksummer::verify<Checksummer::xxhash64>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case Checksummer::CSUM_CRC32C:
    *b_bad_off = Checksummer::verify<Checksummer::crc32c>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case Checksummer::CSUM_CRC32C_16:
    *b_bad_off = Checksummer::verify<Checksummer::crc32c_16>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case Checksummer::CSUM_CRC32C_8:
    *b_bad_off = Checksummer::verify<Checksummer::crc32c_8>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  default:
    r = -EOPNOTSUPP;
    break;
  }

  if (r < 0)
    return r;
  else if (*b_bad_off >= 0)
    return -1; // bad checksum
  else
    return 0;
}

void bluestore_blob_t::allocated(uint32_t b_off, uint32_t length, const PExtentVector& allocs)
{
  if (extents.size() == 0) {
    // if blob is compressed then logical length to be already configured
    // otherwise - to be unset.
    ceph_assert((is_compressed() && logical_length != 0) ||
      (!is_compressed() && logical_length == 0));

    extents.reserve(allocs.size() + (b_off ? 1 : 0));
    if (b_off) {
      extents.emplace_back(
        bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, b_off));

    }
    uint32_t new_len = b_off;
    for (auto& a : allocs) {
      extents.emplace_back(a.offset, a.length);
      new_len += a.length;
    }
    if (!is_compressed()) {
      logical_length = new_len;
    }
  } else {
    ceph_assert(!is_compressed()); // partial allocations are forbidden when 
                              // compressed
    ceph_assert(b_off < logical_length);
    uint32_t cur_offs = 0;
    auto start_it = extents.begin();
    size_t pos = 0;
    while (true) {
      ceph_assert(start_it != extents.end());
      if (cur_offs + start_it->length > b_off) {
	break;
      }
      cur_offs += start_it->length;
      ++start_it;
      ++pos;
    }
    uint32_t head = b_off - cur_offs;
    uint32_t end_off = b_off + length;
    auto end_it = start_it;

    while (true) {
      ceph_assert(end_it != extents.end());
      ceph_assert(!end_it->is_valid());
      if (cur_offs + end_it->length >= end_off) {
	break;
      }
      cur_offs += end_it->length;
      ++end_it;
    }
    ceph_assert(cur_offs + end_it->length >= end_off);
    uint32_t tail = cur_offs + end_it->length - end_off;

    start_it = extents.erase(start_it, end_it + 1);
    size_t count = allocs.size();
    count += head ? 1 : 0;
    count += tail ? 1 : 0;
    extents.insert(start_it,
                   count,
                   bluestore_pextent_t(
                     bluestore_pextent_t::INVALID_OFFSET, 0));
   
    // Workaround to resolve lack of proper iterator return in vector::insert
    // Looks like some gcc/stl implementations still lack it despite c++11
    // support claim
    start_it = extents.begin() + pos;

    if (head) {
      start_it->length = head;
      ++start_it;
    }
    for(auto& e : allocs) {
      *start_it = e;
      ++start_it;
    }
    if (tail) {
      start_it->length = tail;
    } 
  }
}

// cut it out of extents
struct vecbuilder {
  PExtentVector v;
  uint64_t invalid = 0;

  void add_invalid(uint64_t length) {
    invalid += length;
  }
  void flush() {
    if (invalid) {
      v.emplace_back(bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET,
        invalid));

      invalid = 0;
    }
  }
  void add(uint64_t offset, uint64_t length) {
    if (offset == bluestore_pextent_t::INVALID_OFFSET) {
      add_invalid(length);
    }
    else {
      flush();
      v.emplace_back(offset, length);
    }
  }
};

void bluestore_blob_t::allocated_test(const bluestore_pextent_t& alloc)
{
  extents.emplace_back(alloc);
  if (!is_compressed()) {
    logical_length += alloc.length;
  }
}

bool bluestore_blob_t::release_extents(bool all,
				       const PExtentVector& logical,
				       PExtentVector* r)
{
  // common case: all of it?
  if (all) {
    uint64_t pos = 0;
    for (auto& e : extents) {
      if (e.is_valid()) {
	r->push_back(e);
      }
      pos += e.length;
    }
    ceph_assert(is_compressed() || get_logical_length() == pos);
    extents.resize(1);
    extents[0].offset = bluestore_pextent_t::INVALID_OFFSET;
    extents[0].length = pos;
    return true;
  }
  // remove from pextents according to logical release list
  vecbuilder vb;
  auto loffs_it = logical.begin();
  auto lend = logical.end();
  uint32_t pext_loffs_start = 0; //starting loffset of the current pextent
  uint32_t pext_loffs = 0; //current loffset
  auto pext_it = extents.begin();
  auto pext_end = extents.end();
  while (pext_it != pext_end) {
    if (loffs_it == lend ||
        pext_loffs_start + pext_it->length <= loffs_it->offset) {
      int delta0 = pext_loffs - pext_loffs_start;
      ceph_assert(delta0 >= 0);
      if ((uint32_t)delta0 < pext_it->length) {
	vb.add(pext_it->offset + delta0, pext_it->length - delta0);
      }
      pext_loffs_start += pext_it->length;
      pext_loffs = pext_loffs_start;
      ++pext_it;
    }
    else {
      //assert(pext_loffs == pext_loffs_start);
      int delta0 = pext_loffs - pext_loffs_start;
      ceph_assert(delta0 >= 0);

      int delta = loffs_it->offset - pext_loffs;
      ceph_assert(delta >= 0);
      if (delta > 0) {
	vb.add(pext_it->offset + delta0, delta);
	pext_loffs += delta;
      }

      PExtentVector::iterator last_r = r->end();
      if (r->begin() != last_r) {
	--last_r;
      }
      uint32_t to_release = loffs_it->length;
      do {
	uint32_t to_release_part =
	  std::min(pext_it->length - delta0 - delta, to_release);
	auto o = pext_it->offset + delta0 + delta;
	if (last_r != r->end() && last_r->offset + last_r->length == o) {
	  last_r->length += to_release_part;
	}
	else {
	  last_r = r->emplace(r->end(), o, to_release_part);
	}
	to_release -= to_release_part;
	pext_loffs += to_release_part;
	if (pext_loffs == pext_loffs_start + pext_it->length) {
	  pext_loffs_start += pext_it->length;
	  pext_loffs = pext_loffs_start;
	  pext_it++;
	  delta0 = delta = 0;
	}
      } while (to_release > 0 && pext_it != pext_end);
      vb.add_invalid(loffs_it->length - to_release);
      ++loffs_it;
    }
  }
  vb.flush();
  extents.swap(vb.v);
  return false;
}

void bluestore_blob_t::split(uint32_t blob_offset, bluestore_blob_t& rb)
{
  size_t left = blob_offset;
  uint32_t llen_lb = 0;
  uint32_t llen_rb = 0;
  unsigned i = 0;
  for (auto p = extents.begin(); p != extents.end(); ++p, ++i) {
    if (p->length <= left) {
      left -= p->length;
      llen_lb += p->length;
      continue;
    }
    if (left) {
      if (p->is_valid()) {
	rb.extents.emplace_back(bluestore_pextent_t(p->offset + left,
	  p->length - left));
      }
      else {
	rb.extents.emplace_back(bluestore_pextent_t(
	  bluestore_pextent_t::INVALID_OFFSET,
	  p->length - left));
      }
      llen_rb += p->length - left;
      llen_lb += left;
      p->length = left;
      ++i;
      ++p;
    }
    while (p != extents.end()) {
      llen_rb += p->length;
      rb.extents.push_back(*p++);
    }
    extents.resize(i);
    logical_length = llen_lb;
    rb.logical_length = llen_rb;
    break;
  }
  rb.flags = flags;

  if (has_csum()) {
    rb.csum_type = csum_type;
    rb.csum_chunk_order = csum_chunk_order;
    size_t csum_order = get_csum_chunk_size();
    ceph_assert(blob_offset % csum_order == 0);
    size_t pos = (blob_offset / csum_order) * get_csum_value_size();
    // deep copy csum data
    bufferptr old;
    old.swap(csum_data);
    rb.csum_data = bufferptr(old.c_str() + pos, old.length() - pos);
    csum_data = bufferptr(old.c_str(), pos);
  }
}

void bluestore_blob_t::dup(const bluestore_blob_t& from)
{
  extents = from.extents;
  logical_length = from.logical_length;
  compressed_length = from.compressed_length;
  flags = from.flags;
  unused = from.unused;
  csum_type = from.csum_type;
  csum_chunk_order = from.csum_chunk_order;
  if (from.csum_data.length()) {
    csum_data = ceph::buffer::ptr(from.csum_data.c_str(), from.csum_data.length());
    csum_data.reassign_to_mempool(mempool::mempool_bluestore_cache_other);
  } else {
    csum_data = ceph::buffer::ptr();
  }
}

// bluestore_shared_blob_t
MEMPOOL_DEFINE_OBJECT_FACTORY(bluestore_shared_blob_t, bluestore_shared_blob_t,
	          bluestore_shared_blob);

void bluestore_shared_blob_t::dump(Formatter *f) const
{
  f->dump_int("sbid", sbid);
  f->dump_object("ref_map", ref_map);
}

void bluestore_shared_blob_t::generate_test_instances(
  list<bluestore_shared_blob_t*>& ls)
{
  ls.push_back(new bluestore_shared_blob_t(1));
}

ostream& operator<<(ostream& out, const bluestore_shared_blob_t& sb)
{
  out << "(sbid 0x" << std::hex << sb.sbid << std::dec;
  out << " " << sb.ref_map << ")";
  return out;
}

// bluestore_onode_t

void bluestore_onode_t::shard_info::dump(Formatter *f) const
{
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("bytes", bytes);
}

void bluestore_onode_t::shard_info::generate_test_instances(
  list<shard_info*>& o)
{
  o.push_back(new shard_info);
  o.push_back(new shard_info);
  o.back()->offset = 123;
  o.back()->bytes = 456;
}

ostream& operator<<(ostream& out, const bluestore_onode_t::shard_info& si)
{
  return out << std::hex << "0x" << si.offset << "(0x" << si.bytes << " bytes"
	     << std::dec << ")";
}

void bluestore_onode_t::dump(Formatter *f) const
{
  f->dump_unsigned("nid", nid);
  f->dump_unsigned("size", size);
  f->open_object_section("attrs");
  for (auto p = attrs.begin(); p != attrs.end(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first.c_str());  // it's not quite std::string
    f->dump_unsigned("len", p->second.length());
    f->close_section();
  }
  f->close_section();
  f->dump_string("flags", get_flags_string());
  f->open_array_section("extent_map_shards");
  for (auto si : extent_map_shards) {
    f->dump_object("shard", si);
  }
  f->close_section();
  f->dump_unsigned("expected_object_size", expected_object_size);
  f->dump_unsigned("expected_write_size", expected_write_size);
  f->dump_unsigned("alloc_hint_flags", alloc_hint_flags);
}

void bluestore_onode_t::generate_test_instances(list<bluestore_onode_t*>& o)
{
  o.push_back(new bluestore_onode_t());
  // FIXME
}

// bluestore_deferred_op_t

void bluestore_deferred_op_t::dump(Formatter *f) const
{
  f->dump_unsigned("op", (int)op);
  f->dump_unsigned("data_len", data.length());
  f->open_array_section("extents");
  for (auto& e : extents) {
    f->dump_object("extent", e);
  }
  f->close_section();
}

void bluestore_deferred_op_t::generate_test_instances(list<bluestore_deferred_op_t*>& o)
{
  o.push_back(new bluestore_deferred_op_t);
  o.push_back(new bluestore_deferred_op_t);
  o.back()->op = OP_WRITE;
  o.back()->extents.push_back(bluestore_pextent_t(1, 2));
  o.back()->extents.push_back(bluestore_pextent_t(100, 5));
  o.back()->data.append("my data");
}

void bluestore_deferred_transaction_t::dump(Formatter *f) const
{
  f->dump_unsigned("seq", seq);
  f->open_array_section("ops");
  for (list<bluestore_deferred_op_t>::const_iterator p = ops.begin(); p != ops.end(); ++p) {
    f->dump_object("op", *p);
  }
  f->close_section();

  f->open_array_section("released extents");
  for (interval_set<uint64_t>::const_iterator p = released.begin(); p != released.end(); ++p) {
    f->open_object_section("extent");
    f->dump_unsigned("offset", p.get_start());
    f->dump_unsigned("length", p.get_len());
    f->close_section();
  }
  f->close_section();
}

void bluestore_deferred_transaction_t::generate_test_instances(list<bluestore_deferred_transaction_t*>& o)
{
  o.push_back(new bluestore_deferred_transaction_t());
  o.push_back(new bluestore_deferred_transaction_t());
  o.back()->seq = 123;
  o.back()->ops.push_back(bluestore_deferred_op_t());
  o.back()->ops.push_back(bluestore_deferred_op_t());
  o.back()->ops.back().op = bluestore_deferred_op_t::OP_WRITE;
  o.back()->ops.back().extents.push_back(bluestore_pextent_t(1,7));
  o.back()->ops.back().data.append("foodata");
}

void bluestore_compression_header_t::dump(Formatter *f) const
{
  f->dump_unsigned("type", type);
  f->dump_unsigned("length", length);
  if (compressor_message) {
    f->dump_int("compressor_message", *compressor_message);
  }
}

void bluestore_compression_header_t::generate_test_instances(
  list<bluestore_compression_header_t*>& o)
{
  o.push_back(new bluestore_compression_header_t);
  o.push_back(new bluestore_compression_header_t(1));
  o.back()->length = 1234;
}

// adds more salt to build a hash func input
shared_blob_2hash_tracker_t::hash_input_t
  shared_blob_2hash_tracker_t::build_hash_input(
    uint64_t sbid,
    uint64_t offset) const
{
  hash_input_t res = {
    sbid,
    offset >> au_void_bits,
    ((sbid & 0xffffffff) << 32) + ~(uint32_t((offset >> au_void_bits) & 0xffffffff))
  };
  return res;
}

void shared_blob_2hash_tracker_t::inc(
  uint64_t sbid,
  uint64_t offset,
  int n)
{
  auto hash_input = build_hash_input(sbid, offset);
  ref_counter_2hash_tracker_t::inc(
    (char*)hash_input.data(),
    get_hash_input_size(),
    n);
}

void shared_blob_2hash_tracker_t::inc_range(
  uint64_t sbid,
  uint64_t offset,
  uint32_t len,
  int n)
{
  uint32_t alloc_unit = 1 << au_void_bits;
  int64_t l = len;
  while (l > 0) {
    // don't care about ofset alignment as inc() trims it anyway
    inc(sbid, offset, n);
    offset += alloc_unit;
    l -= alloc_unit;
  }
}

bool shared_blob_2hash_tracker_t::test_hash_conflict(
  uint64_t sbid1,
  uint64_t offset1,
  uint64_t sbid2,
  uint64_t offset2) const
{
  auto hash_input1 = build_hash_input(sbid1, offset1);
  auto hash_input2 = build_hash_input(sbid2, offset2);
  return ref_counter_2hash_tracker_t::test_hash_conflict(
    (char*)hash_input1.data(),
    (char*)hash_input2.data(),
    get_hash_input_size());
}

bool shared_blob_2hash_tracker_t::test_all_zero(
  uint64_t sbid,
  uint64_t offset) const
{
  auto hash_input = build_hash_input(sbid, offset);
  return
    ref_counter_2hash_tracker_t::test_all_zero(
      (char*)hash_input.data(),
      get_hash_input_size());
}

bool shared_blob_2hash_tracker_t::test_all_zero_range(
  uint64_t sbid,
  uint64_t offset,
  uint32_t len) const
{
  uint32_t alloc_unit = 1 << au_void_bits;
  int64_t l = len;
  while (l > 0) {
    // don't care about ofset alignment as inc() trims it anyway
    if (!test_all_zero(sbid, offset)) {
      return false;
    }
    offset += alloc_unit;
    l -= alloc_unit;
  }
  return true;
}
