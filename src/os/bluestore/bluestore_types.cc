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

void ExtentList::add_extents(int64_t start, int64_t count) {
  AllocExtent *last_extent = NULL;
  bool can_merge = false;

  if (m_num_extents > 0) {
    last_extent = &((*m_extents)[m_num_extents - 1]);
    uint64_t last_offset = (last_extent->offset + last_extent->length) / 
			m_block_size; 
    uint32_t last_length = last_extent->length / m_block_size; 
    int64_t max_blocks = m_max_alloc_size / m_block_size;
    if ((last_offset == (uint64_t) start) &&
        (!max_blocks || (last_length + count) <= max_blocks)) {
      can_merge = true;
    }
  }

  if (can_merge) {
    last_extent->length += (count * m_block_size);
  } else {
    (*m_extents)[m_num_extents].offset = start * m_block_size;
    (*m_extents)[m_num_extents].length = count * m_block_size;
    m_num_extents++;
  }
  assert((int64_t) m_extents->size() >= m_num_extents);
}

// bluestore_bdev_label_t

void bluestore_bdev_label_t::encode(bufferlist& bl) const
{
  // be slightly friendly to someone who looks at the device
  bl.append("bluestore block device\n");
  bl.append(stringify(osd_uuid));
  bl.append("\n");
  ENCODE_START(1, 1, bl);
  ::encode(osd_uuid, bl);
  ::encode(size, bl);
  ::encode(btime, bl);
  ::encode(description, bl);
  ENCODE_FINISH(bl);
}

void bluestore_bdev_label_t::decode(bufferlist::iterator& p)
{
  p.advance(60); // see above
  DECODE_START(1, p);
  ::decode(osd_uuid, p);
  ::decode(size, p);
  ::decode(btime, p);
  ::decode(description, p);
  DECODE_FINISH(p);
}

void bluestore_bdev_label_t::dump(Formatter *f) const
{
  f->dump_stream("osd_uuid") << osd_uuid;
  f->dump_unsigned("size", size);
  f->dump_stream("btime") << btime;
  f->dump_string("description", description);
}

void bluestore_bdev_label_t::generate_test_instances(
  list<bluestore_bdev_label_t*>& o)
{
  o.push_back(new bluestore_bdev_label_t);
  o.push_back(new bluestore_bdev_label_t);
  o.back()->size = 123;
  o.back()->btime = utime_t(4, 5);
  o.back()->description = "fakey";
}

ostream& operator<<(ostream& out, const bluestore_bdev_label_t& l)
{
  return out << "bdev(osd_uuid " << l.osd_uuid
	     << " size 0x" << std::hex << l.size << std::dec
	     << " btime " << l.btime
	     << " desc " << l.description << ")";
}

// cnode_t

void bluestore_cnode_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(bits, bl);
  ENCODE_FINISH(bl);
}

void bluestore_cnode_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(bits, p);
  DECODE_FINISH(p);
}

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

// bluestore_pextent_t

void small_encode(const vector<bluestore_pextent_t>& v, bufferlist& bl)
{
  size_t n = v.size();
  small_encode_varint(n, bl);
  for (auto e : v) {
    e.encode(bl);
  }
}

void small_decode(vector<bluestore_pextent_t>& v, bufferlist::iterator& p)
{
  size_t n;
  small_decode_varint(n, p);
  v.clear();
  v.reserve(n);
  while (n--) {
    v.push_back(bluestore_pextent_t());
    ::decode(v.back(), p);
  }
}

// bluestore_extent_ref_map_t

void bluestore_extent_ref_map_t::_check() const
{
  uint64_t pos = 0;
  unsigned refs = 0;
  for (const auto &p : ref_map) {
    if (p.first < pos)
      assert(0 == "overlap");
    if (p.first == pos && p.second.refs == refs)
      assert(0 == "unmerged");
    pos = p.first + p.second.length;
    refs = p.second.refs;
  }
}

void bluestore_extent_ref_map_t::_maybe_merge_left(map<uint32_t,record_t>::iterator& p)
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

void bluestore_extent_ref_map_t::get(uint32_t offset, uint32_t length)
{
  map<uint32_t,record_t>::iterator p = ref_map.lower_bound(offset);
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
      uint32_t newlen = MIN(p->first - offset, length);
      p = ref_map.insert(
	map<uint32_t,record_t>::value_type(offset,
					   record_t(newlen, 1))).first;
      offset += newlen;
      length -= newlen;
      _maybe_merge_left(p);
      ++p;
      continue;
    }
    if (p->first < offset) {
      // split off the portion before offset
      assert(p->first + p->second.length > offset);
      uint32_t left = p->first + p->second.length - offset;
      p->second.length = offset - p->first;
      p = ref_map.insert(map<uint32_t,record_t>::value_type(
			   offset, record_t(left, p->second.refs))).first;
      // continue below
    }
    assert(p->first == offset);
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
  _check();
}

void bluestore_extent_ref_map_t::put(
  uint32_t offset, uint32_t length,
  vector<bluestore_pextent_t> *release)
{
  map<uint32_t,record_t>::iterator p = ref_map.lower_bound(offset);
  if (p == ref_map.end() || p->first > offset) {
    if (p == ref_map.begin()) {
      assert(0 == "put on missing extent (nothing before)");
    }
    --p;
    if (p->first + p->second.length <= offset) {
      assert(0 == "put on missing extent (gap)");
    }
  }
  if (p->first < offset) {
    uint32_t left = p->first + p->second.length - offset;
    p->second.length = offset - p->first;
    p = ref_map.insert(map<uint32_t,record_t>::value_type(
			 offset, record_t(left, p->second.refs))).first;
  }
  while (length > 0) {
    assert(p->first == offset);
    if (length < p->second.length) {
      ref_map.insert(make_pair(offset + length,
			       record_t(p->second.length - length,
					p->second.refs)));
      if (p->second.refs > 1) {
	p->second.length = length;
	--p->second.refs;
	_maybe_merge_left(p);
      } else {
	if (release)
	  release->push_back(bluestore_pextent_t(p->first, length));
	ref_map.erase(p);
      }
      return;
    }
    offset += p->second.length;
    length -= p->second.length;
    if (p->second.refs > 1) {
      --p->second.refs;
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
  _check();
}

bool bluestore_extent_ref_map_t::contains(uint32_t offset, uint32_t length) const
{
  map<uint32_t,record_t>::const_iterator p = ref_map.lower_bound(offset);
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
    uint32_t overlap = p->first + p->second.length - offset;
    offset += overlap;
    length -= overlap;
    ++p;
  }
  return true;
}

bool bluestore_extent_ref_map_t::intersects(
  uint32_t offset,
  uint32_t length) const
{
  map<uint32_t,record_t>::const_iterator p = ref_map.lower_bound(offset);
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

void bluestore_extent_ref_map_t::encode(bufferlist& bl) const
{
  uint32_t n = ref_map.size();
  small_encode_varint(n, bl);
  if (n) {
    auto p = ref_map.begin();
    small_encode_varint_lowz(p->first, bl);
    p->second.encode(bl);
    int32_t pos = p->first;
    while (--n) {
      ++p;
      small_encode_varint_lowz((int64_t)p->first - pos, bl);
      p->second.encode(bl);
      pos = p->first;
    }
  }
}

void bluestore_extent_ref_map_t::decode(bufferlist::iterator& p)
{
  uint32_t n;
  small_decode_varint(n, p);
  if (n) {
    int64_t pos;
    small_decode_varint_lowz(pos, p);
    ref_map[pos].decode(p);
    while (--n) {
      int64_t delta;
      small_decode_varint_lowz(delta, p);
      pos += delta;
      ref_map[pos].decode(p);
    }
  }
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

void bluestore_extent_ref_map_t::generate_test_instances(list<bluestore_extent_ref_map_t*>& o)
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
  if (flags & FLAG_MUTABLE) {
    s = "mutable";
  }
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
  if (flags & FLAG_HAS_REFMAP) {
    if (s.length())
      s += '+';
    s += "has_refmap";
  }

  return s;
}

void bluestore_blob_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  small_encode(extents, bl);
  small_encode_varint(flags, bl);
  if (is_compressed()) {
    small_encode_varint_lowz(compressed_length_orig, bl);
    small_encode_varint_lowz(compressed_length, bl);
  }
  if (has_csum()) {
    ::encode(csum_type, bl);
    ::encode(csum_chunk_order, bl);
    small_encode_buf_lowz(csum_data, bl);
  }
  if (has_refmap()) {
    ::encode(ref_map, bl);
  }
  if (has_unused()) {
    ::encode(unused_uint_t(unused.to_ullong()), bl);
  }
  ENCODE_FINISH(bl);
}

void bluestore_blob_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  small_decode(extents, p);
  small_decode_varint(flags, p);
  if (is_compressed()) {
    small_decode_varint_lowz(compressed_length_orig, p);
    small_decode_varint_lowz(compressed_length, p);
  } else {
    compressed_length_orig = compressed_length = 0;
  }
  if (has_csum()) {
    ::decode(csum_type, p);
    ::decode(csum_chunk_order, p);
    small_decode_buf_lowz(csum_data, p);
  } else {
    csum_type = CSUM_NONE;
    csum_chunk_order = 0;
  }
  if (has_refmap()) {
    ::decode(ref_map, p);
  }
  if (has_unused()) {
    unused_uint_t val;
    ::decode(val, p);
    unused = unused_t(val);
  }
  DECODE_FINISH(p);
}

void bluestore_blob_t::dump(Formatter *f) const
{
  f->open_array_section("extents");
  for (auto& p : extents) {
    f->dump_object("extent", p);
  }
  f->close_section();
  f->dump_unsigned("compressed_length_original", compressed_length_orig);
  f->dump_unsigned("compressed_length", compressed_length);
  f->dump_unsigned("flags", flags);
  f->dump_unsigned("csum_type", csum_type);
  f->dump_unsigned("csum_chunk_order", csum_chunk_order);
  f->dump_object("ref_map", ref_map);
  f->open_array_section("csum_data");
  size_t n = get_csum_count();
  for (unsigned i = 0; i < n; ++i)
    f->dump_unsigned("csum", get_csum_item(i));
  f->close_section();
  f->dump_unsigned("unused", unused.to_ullong());
}

void bluestore_blob_t::generate_test_instances(list<bluestore_blob_t*>& ls)
{
  ls.push_back(new bluestore_blob_t);
  ls.push_back(new bluestore_blob_t(0));
  ls.push_back(new bluestore_blob_t);
  ls.back()->extents.push_back(bluestore_pextent_t(111, 222));
  ls.push_back(new bluestore_blob_t);
  ls.back()->init_csum(CSUM_XXHASH32, 16, 65536);
  ls.back()->csum_data = buffer::claim_malloc(4, strdup("abcd"));
  ls.back()->ref_map.get(3, 5);
  ls.back()->add_unused(0, 3, 4096);
  ls.back()->add_unused(8, 8, 4096);
  ls.back()->extents.emplace_back(bluestore_pextent_t(0x40100000, 0x10000));
  ls.back()->extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x1000));
  ls.back()->extents.emplace_back(bluestore_pextent_t(0x40120000, 0x10000));
}

ostream& operator<<(ostream& out, const bluestore_blob_t& o)
{
  out << "blob(" << o.extents
      << " clen 0x" << std::hex
      << o.compressed_length_orig
      << " -> 0x"
      << o.compressed_length
      << std::dec;
  if (o.flags) {
    out << " " << o.get_flags_string();
  }
  if (o.csum_type) {
    out << " " << o.get_csum_type_string(o.csum_type)
	<< "/0x" << std::hex << (1ull << o.csum_chunk_order) << std::dec;
  }
  if (!o.ref_map.empty()) {
    out << " " << o.ref_map;
  }
  if (o.has_unused())
    out << " unused=0x" << std::hex << o.unused.to_ullong() << std::dec;
  out << ")";
  return out;
}

void bluestore_blob_t::get_ref(
  uint64_t offset,
  uint64_t length)
{
  assert(has_refmap());
  ref_map.get(offset, length);
}

bool bluestore_blob_t::put_ref(
  uint64_t offset,
  uint64_t length,
  uint64_t min_release_size,
  vector<bluestore_pextent_t> *r)
{
  assert(has_refmap());
  return put_ref_external(
    ref_map,
    offset,
    length,
    min_release_size,
    r);
}


bool bluestore_blob_t::put_ref_external(
  bluestore_extent_ref_map_t& ref_map,
  uint64_t offset,
  uint64_t length,
  uint64_t min_release_size,
  vector<bluestore_pextent_t> *r)
{
  vector<bluestore_pextent_t> logical;
  ref_map.put(offset, length, &logical);

  r->clear();

  // common case: all of it?
  if (ref_map.empty()) {
    uint64_t pos = 0;
    for (auto& e : extents) {
      if (e.is_valid()) {
	r->push_back(e);
      }
      pos += e.length;
    }
    extents.resize(1);
    extents[0].offset = bluestore_pextent_t::INVALID_OFFSET;
    extents[0].length = pos;
    return true;
  }

  // we cannot do partial deallocation on compressed blobs
  if (has_flag(FLAG_COMPRESSED)) {
    return false;
  }

  // we cannot release something smaller than our csum chunk size
  if (has_csum() && get_csum_chunk_size() > min_release_size) {
    min_release_size = get_csum_chunk_size();
  }

  // search from logical releases
  for (auto le : logical) {
    uint64_t r_off = le.offset;
    auto p = ref_map.ref_map.lower_bound(le.offset);
    if (p != ref_map.ref_map.begin()) {
      --p;
      r_off = p->first + p->second.length;
      ++p;
    } else {
      r_off = 0;
    }
    uint64_t end;
    if (p == ref_map.ref_map.end()) {
      end = this->get_ondisk_length();
    } else {
      end = p->first;
    }
    r_off = ROUND_UP_TO(r_off, min_release_size);
    end -= end % min_release_size;
    if (r_off >= end) {
      continue;
    }
    uint64_t r_len = end - r_off;

    // cut it out of extents
    struct vecbuilder {
      vector<bluestore_pextent_t> v;
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
	} else {
	  flush();
	  v.emplace_back(bluestore_pextent_t(offset, length));
	}
      }
    } vb;

    assert(r_len > 0);
    auto q = extents.begin();
    assert(q != extents.end());
    while (r_off >= q->length) {
      vb.add(q->offset, q->length);
      r_off -= q->length;
      ++q;
      assert(q != extents.end());
    }
    while (r_len > 0) {
      uint64_t l = MIN(r_len, q->length - r_off);
      if (q->is_valid()) {
	r->push_back(bluestore_pextent_t(q->offset + r_off, l));
      }
      if (r_off) {
	vb.add(q->offset, r_off);
      }
      vb.add_invalid(l);
      if (r_off + l < q->length) {
	vb.add(q->offset + r_off + l, q->length - (r_off + l));
      }
      r_len -= l;
      r_off = 0;
      ++q;
      assert(q != extents.end() || r_len == 0);
    }
    while (q != extents.end()) {
      vb.add(q->offset, q->length);
      ++q;
    }
    vb.flush();
    extents.swap(vb.v);
  }
  return false;
}

void bluestore_blob_t::calc_csum(uint64_t b_off, const bufferlist& bl)
{
  switch (csum_type) {
  case CSUM_XXHASH32:
    Checksummer::calculate<Checksummer::xxhash32>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  case CSUM_XXHASH64:
    Checksummer::calculate<Checksummer::xxhash64>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;;
  case CSUM_CRC32C:
    Checksummer::calculate<Checksummer::crc32c>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  case CSUM_CRC32C_16:
    Checksummer::calculate<Checksummer::crc32c_16>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  case CSUM_CRC32C_8:
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
  case CSUM_NONE:
    break;
  case CSUM_XXHASH32:
    *b_bad_off = Checksummer::verify<Checksummer::xxhash32>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case CSUM_XXHASH64:
    *b_bad_off = Checksummer::verify<Checksummer::xxhash64>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case CSUM_CRC32C:
    *b_bad_off = Checksummer::verify<Checksummer::crc32c>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case CSUM_CRC32C_16:
    *b_bad_off = Checksummer::verify<Checksummer::crc32c_16>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case CSUM_CRC32C_8:
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

// bluestore_lextent_t
void bluestore_lextent_t::encode(bufferlist& bl) const
{
  small_encode_signed_varint(blob, bl);
  small_encode_varint_lowz(offset, bl);
  small_encode_varint_lowz(length, bl);
}
void bluestore_lextent_t::decode(bufferlist::iterator& p)
{
  small_decode_signed_varint(blob, p);
  small_decode_varint_lowz(offset, p);
  small_decode_varint_lowz(length, p);
}

void bluestore_lextent_t::dump(Formatter *f) const
{
  f->dump_int("blob", blob);
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

void bluestore_lextent_t::generate_test_instances(list<bluestore_lextent_t*>& ls)
{
  ls.push_back(new bluestore_lextent_t);
  ls.push_back(new bluestore_lextent_t(23232, 0, 4096));
  ls.push_back(new bluestore_lextent_t(23232, 16384, 8192));
}

ostream& operator<<(ostream& out, const bluestore_lextent_t& lb)
{
  return out << "0x" << std::hex << lb.offset << "~" << lb.length << std::dec
	     << "->" << lb.blob;
}

// bluestore_onode_t
void small_encode(const map<uint64_t,bluestore_lextent_t>& extents, bufferlist& bl)
{
  size_t n = extents.size();
  small_encode_varint(n, bl);
  if (n) {
    auto p = extents.begin();
    small_encode_varint_lowz(p->first, bl);
    p->second.encode(bl);
    uint64_t pos = p->first;
    while (--n) {
      ++p;
      small_encode_varint_lowz((uint64_t)p->first - pos, bl);
      p->second.encode(bl);
      pos = p->first;
    }
  }
}

void small_decode(map<uint64_t,bluestore_lextent_t>& extents, bufferlist::iterator& p)
{
  size_t n;
  extents.clear();
  small_decode_varint(n, p);
  if (n) {
    uint64_t pos;
    small_decode_varint_lowz(pos, p);
    extents[pos].decode(p);
    while (--n) {
      uint64_t delta;
      small_decode_varint_lowz(delta, p);
      pos += delta;
      extents[pos].decode(p);
    }
  }
}

void bluestore_onode_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(nid, bl);
  ::encode(size, bl);
  ::encode(attrs, bl);
  small_encode(extent_map, bl);
  ::encode(omap_head, bl);
  ::encode(expected_object_size, bl);
  ::encode(expected_write_size, bl);
  ::encode(alloc_hint_flags, bl);
  ENCODE_FINISH(bl);
}

void bluestore_onode_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(nid, p);
  ::decode(size, p);
  ::decode(attrs, p);
  small_decode(extent_map, p);
  ::decode(omap_head, p);
  ::decode(expected_object_size, p);
  ::decode(expected_write_size, p);
  ::decode(alloc_hint_flags, p);
  DECODE_FINISH(p);
}

void bluestore_onode_t::dump(Formatter *f) const
{
  f->dump_unsigned("nid", nid);
  f->dump_unsigned("size", size);
  f->open_object_section("attrs");
  for (map<string,bufferptr>::const_iterator p = attrs.begin();
       p != attrs.end(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first);
    f->dump_unsigned("len", p->second.length());
    f->close_section();
  }
  f->close_section();
  f->open_object_section("extent_map");
  for (const auto& p : extent_map) {
    f->open_object_section("extent");
    f->dump_unsigned("logical_offset", p.first);
    p.second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("omap_head", omap_head);
  f->dump_unsigned("expected_object_size", expected_object_size);
  f->dump_unsigned("expected_write_size", expected_write_size);
  f->dump_unsigned("alloc_hint_flags", alloc_hint_flags);
}

void bluestore_onode_t::generate_test_instances(list<bluestore_onode_t*>& o)
{
  o.push_back(new bluestore_onode_t());
  // FIXME
}

// FIXME: Using this to compute the ctx.csum_order can lead to poor small
// random read performance when initial writes are large.
size_t bluestore_onode_t::get_preferred_csum_order() const
{
  uint32_t t = expected_write_size;
  if (!t) {
    return 0;
  }
  return ctz(expected_write_size);
}

int bluestore_onode_t::compress_extent_map()
{
  if (extent_map.empty())
    return 0;
  int removed = 0;
  auto p = extent_map.begin();
  auto n = p;
  for (++n; n != extent_map.end(); p = n++) {
    while (n != extent_map.end() &&
	   p->first + p->second.length == n->first &&
	   p->second.blob == n->second.blob &&
	   p->second.offset + p->second.length == n->second.offset) {
      p->second.length += n->second.length;
      extent_map.erase(n++);
      ++removed;
    }
    if (n == extent_map.end()) {
      break;
    }
  }
  return removed;
}

void bluestore_onode_t::punch_hole(
  uint64_t offset,
  uint64_t length,
  vector<std::pair<uint64_t, bluestore_lextent_t> >*deref)
{
  auto p = seek_lextent(offset);
  uint64_t end = offset + length;
  while (p != extent_map.end()) {
    if (p->first >= end) {
      break;
    }
    if (p->first < offset) {
      if (p->first + p->second.length > end) {
	// split and deref middle
	uint64_t front = offset - p->first;
	deref->emplace_back(
          std::make_pair(
            offset,
            bluestore_lextent_t(
              p->second.blob,
              p->second.offset + front,
              length)));
	extent_map[end] = bluestore_lextent_t(
	  p->second.blob,
	  p->second.offset + front + length,
	  p->second.length - front - length);
	p->second.length = front;
	break;
      } else {
	// deref tail
	assert(p->first + p->second.length > offset); // else bug in seek_lextent
	uint64_t keep = offset - p->first;
	deref->emplace_back(
          std::make_pair(
            offset,
            bluestore_lextent_t(
              p->second.blob,
              p->second.offset + keep,
              p->second.length - keep)));
	p->second.length = keep;
	++p;
	continue;
      }
    }
    if (p->first + p->second.length <= end) {
      // deref whole lextent
      deref->push_back(std::make_pair(p->first, p->second));
      extent_map.erase(p++);
      continue;
    }
    // deref head
    uint64_t keep = (p->first + p->second.length) - end;
    deref->emplace_back(
      std::make_pair(
        p->first,
        bluestore_lextent_t(
          p->second.blob,
          p->second.offset,
          p->second.length - keep)));
    extent_map[end] = bluestore_lextent_t(
      p->second.blob,
      p->second.offset + p->second.length - keep,
      keep);
    extent_map.erase(p);
    break;
  }
}

void bluestore_onode_t::set_lextent(uint64_t offset,
                   const bluestore_lextent_t& lext,
                   bluestore_blob_t* b,
                   vector<std::pair<uint64_t, bluestore_lextent_t> >*deref)
{
  punch_hole(offset, lext.length, deref);
  extent_map[offset] = lext;
  //increment reference for shared blobs only
  if (b->has_refmap()) {
    b->get_ref(lext.offset, lext.length);
  }
}

bool bluestore_onode_t::deref_lextent( uint64_t offset,
                         bluestore_lextent_t& lext,
                         bluestore_blob_t* b,
                         uint64_t min_alloc_size,
                         vector<bluestore_pextent_t>* r)
{
  bool empty = false;
  if (b->has_refmap()) {
    empty = b->put_ref(lext.offset, lext.length, min_alloc_size, r);
  } else {
    bluestore_extent_ref_map_t temp_ref_map;
    assert(offset >= lext.offset);
    //determine the range in lextents map where specific blob can be referenced to.
    uint64_t search_offset = offset - lext.offset;
    uint64_t search_end = search_offset +
                          (b->is_compressed() ?
                             b->get_compressed_payload_original_length() :
                             b->get_ondisk_length());
    auto lp = seek_lextent(search_offset);
    while (lp != extent_map.end() &&
           lp->first < search_end) {
      if (lp->second.blob == lext.blob) {
        temp_ref_map.fill(lp->second.offset, lp->second.length);
      }
      ++lp;
    }
    temp_ref_map.get(lext.offset, lext.length); //insert a fake reference for the removed lextent
    empty = b->put_ref_external( temp_ref_map, lext.offset, lext.length, min_alloc_size, r);
  }
  return empty;
}
// bluestore_wal_op_t

void bluestore_wal_op_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(op, bl);
  ::encode(extents, bl);
  ::encode(data, bl);
  ENCODE_FINISH(bl);
}

void bluestore_wal_op_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(op, p);
  ::decode(extents, p);
  ::decode(data, p);
  DECODE_FINISH(p);
}

void bluestore_wal_op_t::dump(Formatter *f) const
{
  f->dump_unsigned("op", (int)op);
  f->dump_unsigned("data_len", data.length());
  f->open_array_section("extents");
  for (auto& e : extents) {
    f->dump_object("extent", e);
  }
  f->close_section();
}

void bluestore_wal_op_t::generate_test_instances(list<bluestore_wal_op_t*>& o)
{
  o.push_back(new bluestore_wal_op_t);
  o.push_back(new bluestore_wal_op_t);
  o.back()->op = OP_WRITE;
  o.back()->extents.push_back(bluestore_pextent_t(1, 2));
  o.back()->extents.push_back(bluestore_pextent_t(100, 5));
  o.back()->data.append("my data");
}

void bluestore_wal_transaction_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(seq, bl);
  ::encode(ops, bl);
  ::encode(released, bl);
  ENCODE_FINISH(bl);
}

void bluestore_wal_transaction_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(seq, p);
  ::decode(ops, p);
  ::decode(released, p);
  DECODE_FINISH(p);
}

void bluestore_wal_transaction_t::dump(Formatter *f) const
{
  f->dump_unsigned("seq", seq);
  f->open_array_section("ops");
  for (list<bluestore_wal_op_t>::const_iterator p = ops.begin(); p != ops.end(); ++p) {
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

void bluestore_wal_transaction_t::generate_test_instances(list<bluestore_wal_transaction_t*>& o)
{
  o.push_back(new bluestore_wal_transaction_t());
  o.push_back(new bluestore_wal_transaction_t());
  o.back()->seq = 123;
  o.back()->ops.push_back(bluestore_wal_op_t());
  o.back()->ops.push_back(bluestore_wal_op_t());
  o.back()->ops.back().op = bluestore_wal_op_t::OP_WRITE;
  o.back()->ops.back().extents.push_back(bluestore_pextent_t(1,7));
  o.back()->ops.back().data.append("foodata");
}

void bluestore_compression_header_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(type, bl);
  ::encode(length, bl);
  ENCODE_FINISH(bl);
}

void bluestore_compression_header_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(type, p);
  ::decode(length, p);
  DECODE_FINISH(p);
}

void bluestore_compression_header_t::dump(Formatter *f) const
{
  f->dump_unsigned("type", type);
  f->dump_unsigned("length", length);
}

void bluestore_compression_header_t::generate_test_instances(
  list<bluestore_compression_header_t*>& o)
{
  o.push_back(new bluestore_compression_header_t);
  o.push_back(new bluestore_compression_header_t(1));
  o.back()->length = 1234;
}
