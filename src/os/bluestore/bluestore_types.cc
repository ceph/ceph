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

size_t get_squeezed_size(const PExtentArray& a)
{
  size_t size = 0;
  for (auto& i : a) {
    if (i == INVALID_OFFSET) {
      break;
    }
    ++size;
  }
  return size;
}

uint64_t squeeze_extent(const bluestore_pextent_t& e)
{

  //ceph_assert(e.offset == INVALID_OFFSET || ctz(e.offset) >= MIN_ALLOC_SIZE_ORDER);
  //ceph_assert(ctz(e.length) >= MIN_ALLOC_SIZE_ORDER);
  //ceph_assert(clz(e.length) >= 64 - MIN_ALLOC_SIZE_ORDER * 2);

  uint64_t v = (e.offset & ~MIN_ALLOC_SIZE_MASK) +
    (e.length >> MIN_ALLOC_SIZE_ORDER);
  return v;
}

bluestore_pextent_t unsqueeze_extent(uint64_t v)
{
  bluestore_pextent_t res;
  res.length = v & MIN_ALLOC_SIZE_MASK;

  res.offset = v - res.length;
  if (res.offset == INVALID_OFFSET_SQUEEZED)
    res.offset = INVALID_OFFSET;

  res.length <<= MIN_ALLOC_SIZE_ORDER;

  return res;
}

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
   num_au{tracker.num_au},
   bytes_per_au{nullptr}
{
  if (num_au > 0) {
    allocate();
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
  num_au = rhs.num_au;
  if (rhs.num_au > 0) {
    allocate();
    std::copy(rhs.bytes_per_au, rhs.bytes_per_au + num_au, bytes_per_au);
  } else {
    total_bytes = rhs.total_bytes;
  }
  return *this;
}

void bluestore_blob_use_tracker_t::allocate()
{
  ceph_assert(num_au != 0);
  bytes_per_au = new uint32_t[num_au];
  mempool::get_pool(
    mempool::pool_index_t(mempool::mempool_bluestore_cache_other)).
      adjust_count(1, sizeof(uint32_t) * num_au);

  for (uint32_t i = 0; i < num_au; ++i) {
    bytes_per_au[i] = 0;
  }
}

void bluestore_blob_use_tracker_t::init(
  uint32_t full_length, uint32_t _au_size) {
  ceph_assert(!au_size || is_empty()); 
  ceph_assert(_au_size > 0);
  ceph_assert(full_length > 0);
  clear();  
  uint32_t _num_au = round_up_to(full_length, _au_size) / _au_size;
  au_size = _au_size;
  if ( _num_au > 1 ) {
    num_au = _num_au;
    allocate();
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

    while (offset < end) {
      auto phase = offset % au_size;
      bytes_per_au[offset / au_size] += 
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
      size_t pos = offset / au_size;
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
  
  uint32_t new_num_au = blob_offset / au_size;
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
    total_bytes = tmp;
  } else {
    num_au = new_num_au;
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
bluestore_blob_t::bluestore_blob_t(uint32_t f) : flags(f)
{
  _squeezed_extents.fill(INVALID_OFFSET);
  set_flag(FLAG_SHORT_EXTENTS_LIST);
}
bluestore_blob_t::~bluestore_blob_t() {
  if (!has_short_extents_list()) {
    _extents.~PExtentVector();
  }
}


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
  if (flags & FLAG_SHORT_EXTENTS_LIST) {
    if (s.length())
      s += '+';
    s += "squeezed";
  }

  return s;
}

size_t bluestore_blob_t::get_csum_value_size() const 
{
  return Checksummer::get_csum_value_size(csum_type);
}

void bluestore_blob_t::dump(Formatter *f) const
{
  f->open_array_section("extents");
  for_each_extent(true,
    [&](const bluestore_pextent_t& p) {
      f->dump_object("extent", p);
      return 0;
    });
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
    bluestore_pextent_t(INVALID_OFFSET, 0x1000));
  ls.back()->allocated_test(bluestore_pextent_t(0x40120000, 0x10000));
}

ostream& operator<<(ostream& out, const bluestore_blob_t& o)
{
  out << "blob([";
  bool first = true;
  o.for_each_extent(true,
    [&](const bluestore_pextent_t& p) {
      if (!first) {
        out << ',';
      } else {
        first = false;
      }
      out << p;
      return 0;
    });
  out << ']';
  if (o.is_compressed()) {
    out << " clen 0x" << std::hex
	<< o.get_logical_length()
	<< " -> 0x"
	<< o.get_compressed_payload_length()
	<< std::dec;
  }
  if (o.flags) {
    out << " " << o.get_flags_string();
  }
  if (o.has_csum()) {
    out << " " << Checksummer::get_csum_type_string(o.csum_type)
	<< "/0x" << std::hex << (1ull << o.csum_chunk_order) << std::dec;
  }
  if (o.has_unused())
    out << " unused=0x" << std::hex << o.unused << std::dec;
  out << ")";
  return out;
}

void bluestore_blob_t::bound_encode(size_t& p, uint64_t struct_v) const
{
  ceph_assert(struct_v == 1 || struct_v == 2);

  if (has_short_extents_list()) {
    denc(_squeezed_extents, p);
  } else {
    denc(_extents, p);
  }
  denc_varint(flags, p);
  denc_varint_lowz(logical_length, p);
  denc_varint_lowz(compressed_length, p);
  denc(csum_type, p);
  denc(csum_chunk_order, p);
  denc_varint(csum_data.length(), p);
  p += csum_data.length();
  p += sizeof(unused_t);
}

void bluestore_blob_t::encode(bufferlist::contiguous_appender& p,
                              uint64_t struct_v) const
{
  ceph_assert(struct_v == 1 || struct_v == 2);

  if (has_short_extents_list()) {
    denc(_squeezed_extents, p);
  } else {
    denc(_extents, p);
  }
  denc_varint(flags, p);
  if (is_compressed()) {
    denc_varint_lowz(logical_length, p);
    denc_varint_lowz(compressed_length, p);
  }
  if (has_csum()) {
    denc(csum_type, p);
    denc(csum_chunk_order, p);
    denc_varint(csum_data.length(), p);
    memcpy(p.get_pos_add(csum_data.length()), csum_data.c_str(),
      csum_data.length());
  }
  if (has_unused()) {
    denc(unused, p);
  }
}

void bluestore_blob_t::decode(bufferptr::const_iterator& p,
                              uint64_t struct_v)
{
  ceph_assert(struct_v == 1 || struct_v == 2);
  bool was_short = has_short_extents_list();

  // we can't decode from squeezed format since flags aren't set
  // at this point hence doing via PExtentVector
  PExtentVector extents;
  denc(extents, p);

  denc_varint(flags, p);
  if (is_compressed()) {
    denc_varint_lowz(logical_length, p);
    denc_varint_lowz(compressed_length, p);
  }
  if (has_csum()) {
    denc(csum_type, p);
    denc(csum_chunk_order, p);
    int len;
    denc_varint(len, p);
    csum_data = p.get_ptr(len);
    csum_data.reassign_to_mempool(mempool::mempool_bluestore_cache_other);
  }
  if (has_unused()) {
    denc(unused, p);
  }
  // the code below additionally checks if obtained extents list
  // can be preserved in the short mode. The rationale is to ensure
  // correct handling after OSD upgrade -> downgrage -> upgrade procedure
  // when FLAG_SHORT_EXTENTS_LIST might be out of sync.
  //
  bool done = false;
  if (has_short_extents_list() && extents.size() <= MAX_SQUEEZED) {
    if (!was_short) {
      _extents.~PExtentVector();
    }
    size_t pos = 0;
    done = true;
    for (auto& e : extents) {
      if (e.length < MAX_SQUEEZED_LEN) {
        _squeezed_extents[pos++] = squeeze_extent(e);
      } else {
        done = false;
        break;
      }
    }
    while (done && pos < MAX_SQUEEZED) {
      _squeezed_extents[pos++] = INVALID_OFFSET;
    }
  }
  if (!done) {
    if (was_short) {
      new (&_extents) PExtentVector;
    }
    _extents.swap(extents);
  }
  // doing postponed length calculation
  if (!is_compressed()) {
    logical_length = get_ondisk_length();
  }
}

uint64_t bluestore_blob_t::calc_offset(uint64_t x_off, uint64_t* plen) const
{
  if (has_short_extents_list()) {
    for (auto i : _squeezed_extents) {
      ceph_assert(i != INVALID_OFFSET);
      bluestore_pextent_t p = unsqueeze_extent(i);
      if (x_off >= p.length) {
        x_off -= p.length;
      } else {
        if (plen)
          *plen = p.length - x_off;
        return p.offset + x_off;
      }
    }
    ceph_abort_msg("calc_offset() reached the end, failed to find any pextent");
  }
  auto p = _extents.begin();
  ceph_assert(p != _extents.end());
  while (x_off >= p->length) {
    x_off -= p->length;
    ++p;
    ceph_assert(p != _extents.end());
  }
  if (plen)
    *plen = p->length - x_off;
  return p->offset + x_off;
}

// validates whether or not the status of pextents within the given range
// meets the requirement(allocated or unallocated).
bool bluestore_blob_t::_validate_range(uint64_t b_off, uint64_t b_len,
  bool require_allocated) const
{
  if (has_short_extents_list()) {
    bool within = false;
    for (auto i : _squeezed_extents) {
      ceph_assert(i != INVALID_OFFSET);
      bluestore_pextent_t p = unsqueeze_extent(i);
      if (!within && b_off >= p.length) {
        b_off -= p.length;
      } else {
        if (!within) {
          within = true;
          b_len += b_off;
        }
        if (b_len) {
          if (require_allocated != p.is_valid()) {
            return false;
          }
          if (p.length >= b_len) {
            return true;
          }
          b_len -= p.length;
        }
      }
    }
    ceph_abort_msg("_validate_range() reached the end, failed to find any pextent");
  }

  auto p = _extents.begin();
  ceph_assert(p != _extents.end());
  while (b_off >= p->length) {
    b_off -= p->length;
    if (++p == _extents.end())
      return false;    
  }
  b_len += b_off;
  while (b_len) {
    ceph_assert(p != _extents.end());
    if (require_allocated != p->is_valid()) {
      return false;
    }

    if (p->length >= b_len) {
      return true;
    }
    b_len -= p->length;
    if (++p == _extents.end())
      return false;    
  }
  ceph_abort_msg("we should not get here");
  return false;
}

uint32_t bluestore_blob_t::get_ondisk_length() const
{
  uint32_t len = 0;
  if (has_short_extents_list()) {
    for (auto i : _squeezed_extents) {
      // skip tailing 'non-present' extents
      if (i == INVALID_OFFSET) {
        break;
      }
      bluestore_pextent_t p = unsqueeze_extent(i);
      len += p.length;
    }
  } else {
    for (auto& p : _extents) {
      len += p.length;
    }
  }
  return len;
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


bool bluestore_blob_t::try_prune_tail()
{
  bool res = false;
  if (has_unused()) {
    return res;
  }

  if (has_short_extents_list()) {
    for (auto it = _squeezed_extents.rbegin(); it != _squeezed_extents.rend(); ++it) {
      if (*it != INVALID_OFFSET) {
        bluestore_pextent_t p = unsqueeze_extent(*it);
        if (logical_length > p.length && // if it's all invalid it's not pruning.
          !p.is_valid()) {
          logical_length -= p.length;
          *it = INVALID_OFFSET;
          res = true;
        }
        break;
      }
    }
  } else {
    if (_extents.size() > 1 &&  // if it's all invalid it's not pruning.
      !_extents.back().is_valid()) {
      const auto& p = _extents.back();
      logical_length -= p.length;
      _extents.pop_back();
      res = true;
    }
    //FIXME minor: switch to short?
  }
  if (res && has_csum()) {
    bufferptr t;
    t.swap(csum_data);
    csum_data = bufferptr(t.c_str(),
      get_logical_length() / get_csum_chunk_size() *
      get_csum_value_size());
  }
  return res;
}

void bluestore_blob_t::add_tail(uint32_t new_len)
{
  ceph_assert(is_mutable());
  ceph_assert(!has_unused());
  ceph_assert(new_len > logical_length);

  bool done = false;
  auto l = new_len - logical_length;
  if (has_short_extents_list()) {
    auto size = get_squeezed_size(_squeezed_extents);

    if (size > 0) {
      bluestore_pextent_t p0 =
        unsqueeze_extent(_squeezed_extents[size - 1]);
      if (p0.offset == INVALID_OFFSET && p0.offset + l < MAX_SQUEEZED_LEN) {
        p0.length += l;
        _squeezed_extents[size - 1] = squeeze_extent(p0);
        done = true;
      }
    }
    if (!done && l < MAX_SQUEEZED_LEN && size < _squeezed_extents.size()) {
      ceph_assert(ctz(l) >= MIN_ALLOC_SIZE_ORDER);
      bluestore_pextent_t p(INVALID_OFFSET, l);
      auto v = squeeze_extent(p);
      _squeezed_extents[size] = v;
      done = true;
    } else {
      unsqueeze_extents();
    }
  }
  if (!done) {
    if (!_extents.empty() && _extents.back().offset == INVALID_OFFSET) {
      _extents.back().length += l;
    } else {
      _extents.emplace_back(
        bluestore_pextent_t(
          INVALID_OFFSET,
          l));
    }
  }
  logical_length = new_len;

  if (has_csum()) {
    bufferptr t;
    t.swap(csum_data);
    csum_data = buffer::create(
      get_csum_value_size() * logical_length / get_csum_chunk_size());
    csum_data.copy_in(0, t.length(), t.c_str());
    csum_data.zero(t.length(), csum_data.length() - t.length());
  }
}

void bluestore_blob_t::allocated(uint32_t b_off, uint32_t length, const PExtentVector& allocs)
{
  if (get_ondisk_length() == 0) {
    // if blob is compressed then logical length to be already configured
    // otherwise - to be unset.
    ceph_assert((is_compressed() && logical_length != 0) ||
      (!is_compressed() && logical_length == 0));

    size_t expected_size = allocs.size() + (b_off ? 1 : 0);
    bool can_use_short = false;
    if (expected_size <= MAX_SQUEEZED && b_off < MAX_SQUEEZED_LEN) {
      can_use_short = true;
      for (auto& p : allocs) {
        if (p.length >= MAX_SQUEEZED_LEN) {
          can_use_short = false;
          break;
        }
      }
    }

    reset_extents(can_use_short);
    uint32_t new_len = b_off;
    size_t i = 0;
    if (can_use_short) {
      if (b_off) {
        _squeezed_extents[i++] = squeeze_extent(bluestore_pextent_t(INVALID_OFFSET, b_off));
      }
      for (auto& p : allocs) {
        _squeezed_extents[i++] = squeeze_extent(p);
        new_len += p.length;
      }
    } else {
      _extents.resize(expected_size);
      if (b_off) {
        _extents[i++] = bluestore_pextent_t(INVALID_OFFSET, b_off);
      }
      for (auto& a : allocs) {
        _extents[i++] = bluestore_pextent_t(a.offset, a.length);
        new_len += a.length;
      }
    }
    if (!is_compressed()) {
      logical_length = new_len;
    }
  } else {
    ceph_assert(!is_compressed()); // partial allocations are forbidden when 
                              // compressed
    ceph_assert(b_off < logical_length);
    uint32_t cur_offs = 0;
    size_t pos = 0;

    uint32_t end_off = b_off + length;
    bool found = false;
    uint32_t head = 0;
    uint32_t tail = 0;

    size_t expected_size = allocs.size();
    for_each_extent(true,
      [&] (const bluestore_pextent_t& p) {
        if (found) {
          expected_size++;
        } else if (cur_offs + p.length <= b_off) {
          cur_offs += p.length;
          pos++;
          expected_size++;
        } else {
          // single invalid pextent is expected
          ceph_assert(cur_offs <= b_off);
          ceph_assert(cur_offs + p.length >= end_off);
          ceph_assert(!p.is_valid());
          found = true;
          head = b_off - cur_offs;
          tail = cur_offs + p.length - end_off;
          expected_size += head ? 1 : 0;
          expected_size += tail ? 1 : 0;
        }
        return 0;
      });

      bool can_use_short = false;
    if (has_short_extents_list() &&
        expected_size <= MAX_SQUEEZED && b_off < MAX_SQUEEZED_LEN) {
      can_use_short = true;
      for (auto& p : allocs) {
        if (p.length >= MAX_SQUEEZED_LEN) {
          can_use_short = false;
          break;
        }
      }
    }

    if (found) {
      size_t delta = allocs.size() + (tail ? 1 : 0) + (head ? 1 : 0) - 1;
      auto pos0 = pos;
      if (can_use_short) {
        // we are already in short mode
        if (head) {
          _squeezed_extents[pos++] =
            squeeze_extent(bluestore_pextent_t(INVALID_OFFSET, head));
        }

        // shift preserved extents right
        for (size_t i = _squeezed_extents.size() - 1; i > pos0 + delta; i--) {
          _squeezed_extents[i] = _squeezed_extents[i - delta];
        }
        for (auto& p : allocs) {
          _squeezed_extents[pos++] = squeeze_extent(p);
        }

        if (tail) {
          _squeezed_extents[pos++] =
            squeeze_extent(bluestore_pextent_t(INVALID_OFFSET, tail));
        }
      } else {
        if (has_short_extents_list()) {
          unsqueeze_extents();
        }
        _extents.resize(expected_size);
        if (head) {
          _extents[pos++] = bluestore_pextent_t(INVALID_OFFSET, head);
        }
        // shift preserved extents right
        for (size_t i = _extents.size() - 1; i > pos0 + delta; i--) {
          _extents[i] = _extents[i - delta];
        }
        for (auto& p : allocs) {
          _extents[pos++] = p;
        }

        if (tail) {
          _extents[pos++] = bluestore_pextent_t(INVALID_OFFSET, tail);
        }
      }
    } else {
      // appeding more extents to existing one isn't implemented for now
      ceph_assert(false);
    }
  }
}

// cut it out of extents
struct vecbuilder {
  size_t short_list_len = 0;
  PExtentArray short_list;
  PExtentVector v;

  vecbuilder() {
    short_list.fill(INVALID_OFFSET);
  }

  bool has_short_list() const {
    return v.empty();
  }
  void add_invalid(uint64_t length) {
    if (!v.empty()) {
      bluestore_pextent_t& p = v.back();
      if (p.offset == INVALID_OFFSET) {
        p.length += length;
      } else {
        v.emplace_back(INVALID_OFFSET, length);
      }
    } else if (short_list_len == 0) {
      _add_new(INVALID_OFFSET, length);
    } else {
      bluestore_pextent_t p = unsqueeze_extent(short_list[short_list_len - 1]);
      if (p.offset == INVALID_OFFSET && p.length + length < MAX_SQUEEZED_LEN) {
        p.length += length;
        short_list[short_list_len - 1] = squeeze_extent(p);
      } else {
        _add_new(INVALID_OFFSET, length);
      }
    }
  }
  void add(uint64_t offset, uint64_t length) {
    if (offset == INVALID_OFFSET) {
      add_invalid(length);
    } else {
      _add_new(offset, length);
    }
  }
private:
  void _add_new(uint64_t offset, uint64_t length) {
    if (v.empty() && length < MAX_SQUEEZED_LEN &&
      short_list_len < short_list.size()) {
      short_list[short_list_len++] =
        squeeze_extent(bluestore_pextent_t(offset, length));
    } else {
      if (short_list_len) {
        for (size_t i = 0; i < short_list_len; i++) {
          v.emplace_back(unsqueeze_extent(short_list[i]));
        }
        short_list_len = 0;
      }
      v.emplace_back(offset, length);
    }
  }
};

void bluestore_blob_t::allocated_test(const bluestore_pextent_t& alloc)
{
  bool done = false;
  if (has_short_extents_list()) {
    auto size = get_squeezed_size(_squeezed_extents);
    if (size < _squeezed_extents.size() && alloc.length < MAX_SQUEEZED_LEN) {
      _squeezed_extents[size] = squeeze_extent(alloc);
      done = true;
    } else {
      unsqueeze_extents();
    }
  }
  if (!done) {
    _extents.emplace_back(alloc);
  }
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
    for_each_extent(true,
      [&](const bluestore_pextent_t& p) {
        if (p.is_valid()) {
          r->push_back(p);
        }
        pos += p.length;
        return 0;
      });
    ceph_assert(is_compressed() || get_logical_length() == pos);

    if (!has_short_extents_list()) {
      _extents.~PExtentVector();
      set_flag(FLAG_SHORT_EXTENTS_LIST);
    }
    _squeezed_extents.fill(INVALID_OFFSET);
    _squeezed_extents[0] = squeeze_extent(bluestore_pextent_t(INVALID_OFFSET, pos));
    return true;
  }
  if (logical.empty()) {
    return false;
  }
  // remove from pextents according to logical release list
  vecbuilder vb;

  auto loffs_it = logical.begin();
  uint64_t loffs_cur = loffs_it->offset;
  auto lend = logical.end();

  uint32_t pext_loffs = 0; //current loffset

  PExtentVector::iterator last_r = r->end();
  if (r->begin() != last_r) {
    --last_r;
  }
  for_each_extent(true,
    [&](const bluestore_pextent_t& p) {

      uint32_t pext_loffs_start = pext_loffs; //starting loffset of the current pextent
      uint32_t pext_len = p.length;
      while (pext_len) {
        int delta0 = pext_loffs - pext_loffs_start;
        ceph_assert(delta0 >= 0);
        ceph_assert(p.is_valid() || delta0 == 0);
        if (loffs_it == lend || pext_loffs + pext_len <= loffs_cur) {
          ceph_assert(p.is_valid() || delta0 == 0);
          if (p.is_valid()) {
            vb.add(p.offset + delta0, pext_len);
          } else {
            // we don't expect any release for invalid pextents
            ceph_assert(delta0 == 0);
            ceph_assert(pext_len == p.length);
            vb.add_invalid(pext_len);
          }
          pext_loffs += pext_len;
          pext_len = 0;
        } else {
          // we don't expect any release for invalid pextents
          ceph_assert(p.is_valid());
          int delta = loffs_cur - pext_loffs;
          ceph_assert(delta >= 0);
          if (delta > 0) {
            vb.add(p.offset + delta0, delta);
            pext_loffs += delta;
            pext_len -= delta;
          }

          uint32_t log_len = loffs_it->length - (loffs_cur - loffs_it->offset);
          ceph_assert(log_len);
          uint32_t to_release = std::min(pext_len, log_len);
          if (to_release) {
            vb.add_invalid(to_release);

            auto o = p.offset + delta0 + delta;
            if (last_r != r->end() && last_r->offset + last_r->length == o) {
              last_r->length += to_release;
            } else {
              last_r = r->emplace(r->end(), o, to_release);
            }

            pext_loffs += to_release;
            pext_len -= to_release;
            loffs_cur += to_release;
            if (log_len == to_release) {
              loffs_it++;
              if (loffs_it != lend) {
                loffs_cur = loffs_it->offset;
              }
            }
          }
        }
      }
      ceph_assert(pext_loffs == pext_loffs_start + p.length);
      return 0;
    });
  reset_extents(vb.has_short_list());
  if (vb.has_short_list()) {
    _squeezed_extents.swap(vb.short_list);
  } else {
    _extents.swap(vb.v);
  }
  return false;
}

void bluestore_blob_t::split(uint32_t blob_offset, bluestore_blob_t& rb)
{
  size_t left = blob_offset;
  uint32_t llen_lb = 0;
  uint32_t llen_rb = 0;
  unsigned i = 0;
  PExtentVector to_split;
  bool found = false;
  bool can_use_short_list = true;
  for_each_extent(true,
    [&] (const bluestore_pextent_t& p) {
      if (!found && p.length <= left) {
        left -= p.length;
        llen_lb += p.length;
        ++i;
        return 0;
      }
      found = true;
      if (left) {
        auto l = p.length - left;
        can_use_short_list = can_use_short_list && (l < MAX_SQUEEZED_LEN);
        if (p.is_valid()) {
          to_split.emplace_back(bluestore_pextent_t(p.offset + left, l));
        } else {
          to_split.emplace_back(bluestore_pextent_t(INVALID_OFFSET, l));
        }
        llen_rb += p.length - left;
        llen_lb += left;
        _update_extent(i++, bluestore_pextent_t(p.offset, left));
        left = 0;
      } else {
        llen_rb += p.length;
        to_split.push_back(p);
      }
      return 0;
    });

  _prune_tailing_extents(i);
  logical_length = llen_lb;
  rb.logical_length = llen_rb;

  if (!to_split.empty()) {
    bool do_long = true;
    if (rb.has_short_extents_list()) {
      size_t sz = get_squeezed_size(rb._squeezed_extents);
      if (sz + to_split.size() <= MAX_SQUEEZED && can_use_short_list) {
        for (size_t i = 0; i < to_split.size(); i++) {
          rb._squeezed_extents[sz + i] = squeeze_extent(to_split[i]);
        }
        rb.flags = flags;
        rb.set_flag(FLAG_SHORT_EXTENTS_LIST); // for sure
        do_long = false;
      } else {
        sz += to_split.size();
        rb.unsqueeze_extents();
      }
    }
    if (do_long) {
      if (rb._extents.empty()) {
        rb._extents.swap(to_split);
      } else {
        rb._extents.insert(rb._extents.end(), to_split.begin(), to_split.end());
      }
      rb.flags = flags;
      rb.clear_flag(FLAG_SHORT_EXTENTS_LIST); // for sure
    }
  } else {
    bool is_short = rb.has_short_extents_list();
    rb.flags = flags;
    if (is_short) {
      rb.set_flag(FLAG_SHORT_EXTENTS_LIST); // for sure
    } else {
      rb.clear_flag(FLAG_SHORT_EXTENTS_LIST); // for sure
    }
  }

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

int bluestore_blob_t::for_each_extent(bool include_invalid,
  std::function<int (const bluestore_pextent_t&)> fn) const
{
  if (has_short_extents_list()) {
    for (auto& v : _squeezed_extents) {
      if (v == INVALID_OFFSET) {
        break;
      }
      bluestore_pextent_t e = unsqueeze_extent(v);
      if (include_invalid || e.is_valid()) {
        int r = fn(e);
        if (r < 0)
          return r;
      }
    }
  } else {
    for (auto& e : _extents) {
      if (include_invalid || e.is_valid()) {
        int r = fn(e);
        if (r < 0)
          return r;
      }
    }
  }
  return 0;
}

// bluestore_shared_blob_t
MEMPOOL_DEFINE_OBJECT_FACTORY(bluestore_shared_blob_t, bluestore_shared_blob_t,
	          bluestore_cache_other);

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
