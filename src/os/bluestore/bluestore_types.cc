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
#include "include/stringify.h"

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
	     << " size " << l.size
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

// bluestore_extent_t

string bluestore_extent_t::get_flags_string(unsigned flags)
{
  string s;
  if (flags & FLAG_SHARED) {
    if (s.length())
      s += '+';
    s += "shared";
  }
  return s;
}

void bluestore_extent_t::dump(Formatter *f) const
{
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
  f->dump_unsigned("flags", flags);
}

void bluestore_extent_t::generate_test_instances(list<bluestore_extent_t*>& o)
{
  o.push_back(new bluestore_extent_t());
  o.push_back(new bluestore_extent_t(123, 456));
  o.push_back(new bluestore_extent_t(789, 1024, 322));
}

ostream& operator<<(ostream& out, const bluestore_extent_t& e)
{
  out << e.offset << "~" << e.length;
  if (e.flags)
    out << ":" << bluestore_extent_t::get_flags_string(e.flags);
  return out;
}

// bluestore_extent_ref_map_t

void bluestore_extent_ref_map_t::add(uint64_t offset, uint32_t len, unsigned ref)
{
  map<uint64_t,record_t>::iterator p = ref_map.insert(
    map<uint64_t,record_t>::value_type(offset, record_t(len, ref))).first;
  _maybe_merge_left(p);
  ++p;
  if (p != ref_map.end())
    _maybe_merge_left(p);
  _check();
}

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

void bluestore_extent_ref_map_t::_maybe_merge_left(map<uint64_t,record_t>::iterator& p)
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
  map<uint64_t,record_t>::iterator p = ref_map.lower_bound(offset);
  if (p == ref_map.end() || p->first > offset) {
    if (p == ref_map.begin()) {
      assert(0 == "get on missing extent (nothing before)");
    }
    --p;
    if (p->first + p->second.length <= offset) {
      assert(0 == "get on missing extent (gap)");
    }
  }
  if (p->first < offset) {
    uint64_t left = p->first + p->second.length - offset;
    p->second.length = offset - p->first;
    p = ref_map.insert(map<uint64_t,record_t>::value_type(
			 offset, record_t(left, p->second.refs))).first;
  }
  while (length > 0) {
    assert(p->first == offset);
    if (length < p->second.length) {
      ref_map.insert(make_pair(offset + length,
			       record_t(p->second.length - length,
					p->second.refs)));
      p->second.length = length;
      ++p->second.refs;
      _maybe_merge_left(p);
      return;
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

void bluestore_extent_ref_map_t::put(uint64_t offset, uint32_t length,
			   vector<bluestore_extent_t> *release)
{
  map<uint64_t,record_t>::iterator p = ref_map.lower_bound(offset);
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
    uint64_t left = p->first + p->second.length - offset;
    p->second.length = offset - p->first;
    p = ref_map.insert(map<uint64_t,record_t>::value_type(
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
	release->push_back(bluestore_extent_t(p->first, length));
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
      release->push_back(bluestore_extent_t(p->first, p->second.length));
      ref_map.erase(p++);
    }
  }
  if (p != ref_map.end())
    _maybe_merge_left(p);
  _check();
}

bool bluestore_extent_ref_map_t::contains(uint64_t offset, uint32_t length) const
{
  map<uint64_t,record_t>::const_iterator p = ref_map.lower_bound(offset);
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

void bluestore_extent_ref_map_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(ref_map, bl);
  ENCODE_FINISH(bl);
}

void bluestore_extent_ref_map_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(ref_map, p);
  DECODE_FINISH(p);
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
  o.back()->add(10, 10);
  o.back()->add(20, 20, 3);
  o.back()->get(15, 20);
}

ostream& operator<<(ostream& out, const bluestore_extent_ref_map_t& m)
{
  out << "ref_map(";
  for (auto p = m.ref_map.begin(); p != m.ref_map.end(); ++p) {
    if (p != m.ref_map.begin())
      out << ",";
    out << p->first << "~" << p->second.length << "=" << p->second.refs;
  }
  out << ")";
  return out;
}

// bluestore_overlay_t

void bluestore_overlay_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(key, bl);
  ::encode(value_offset, bl);
  ::encode(length, bl);
  ENCODE_FINISH(bl);
}

void bluestore_overlay_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(key, p);
  ::decode(value_offset, p);
  ::decode(length, p);
  DECODE_FINISH(p);
}

void bluestore_overlay_t::dump(Formatter *f) const
{
  f->dump_unsigned("key", key);
  f->dump_unsigned("value_offset", value_offset);
  f->dump_unsigned("length", length);
}

void bluestore_overlay_t::generate_test_instances(list<bluestore_overlay_t*>& o)
{
  o.push_back(new bluestore_overlay_t());
  o.push_back(new bluestore_overlay_t(789, 1024, 1232232));
}

ostream& operator<<(ostream& out, const bluestore_overlay_t& o)
{
  out << "overlay(" << o.value_offset << "~" << o.length
      << " key " << o.key << ")";
  return out;
}

// bluestore_onode_t

void bluestore_onode_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(nid, bl);
  ::encode(size, bl);
  ::encode(attrs, bl);
  ::encode(block_map, bl);
  ::encode(overlay_map, bl);
  ::encode(overlay_refs, bl);
  ::encode(last_overlay_key, bl);
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
  ::decode(block_map, p);
  ::decode(overlay_map, p);
  ::decode(overlay_refs, p);
  ::decode(last_overlay_key, p);
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
  f->open_object_section("block_map");
  for (map<uint64_t, bluestore_extent_t>::const_iterator p = block_map.begin();
       p != block_map.end(); ++p) {
    f->open_object_section("extent");
    f->dump_unsigned("extent_offset", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_object_section("overlays");
  for (map<uint64_t, bluestore_overlay_t>::const_iterator p = overlay_map.begin();
       p != overlay_map.end(); ++p) {
    f->open_object_section("overlay");
    f->dump_unsigned("offset", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("overlay_refs");
  for (map<uint64_t,uint16_t>::const_iterator p = overlay_refs.begin();
       p != overlay_refs.end(); ++p) {
    f->open_object_section("overlay");
    f->dump_unsigned("offset", p->first);
    f->dump_unsigned("refs", p->second);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("last_overlay_key", last_overlay_key);
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

// bluestore_wal_op_t

void bluestore_wal_op_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(op, bl);
  ::encode(extent, bl);
  ::encode(src_extent, bl);
  ::encode(src_rmw_head, bl);
  ::encode(src_rmw_tail, bl);
  ::encode(nid, bl);
  ::encode(overlays, bl);
  if (!overlays.size()) {
    ::encode(data, bl);
  }
  ::encode(removed_overlays, bl);
  ENCODE_FINISH(bl);
}

void bluestore_wal_op_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(op, p);
  ::decode(extent, p);
  ::decode(src_extent, p);
  ::decode(src_rmw_head, p);
  ::decode(src_rmw_tail, p);
  ::decode(nid, p);
  ::decode(overlays, p);
  if (!overlays.size()) {
    ::decode(data, p);
  }
  ::decode(removed_overlays, p);
  DECODE_FINISH(p);
}

void bluestore_wal_op_t::dump(Formatter *f) const
{
  f->dump_unsigned("op", (int)op);
  f->dump_object("extent", extent);
  f->dump_object("src_extent", src_extent);
  f->dump_unsigned("src_rmw_head", src_rmw_head);
  f->dump_unsigned("src_rmw_tail", src_rmw_tail);
  f->dump_unsigned("nid", nid);
  f->open_array_section("overlays");
  for (vector<bluestore_overlay_t>::const_iterator p = overlays.begin();
       p != overlays.end(); ++p) {
    f->dump_object("overlay", *p);
  }
  f->close_section();
  f->open_array_section("removed_overlays");
  for (vector<uint64_t>::const_iterator p = removed_overlays.begin();
       p != removed_overlays.end(); ++p) {
    f->dump_unsigned("key", *p);
  }
  f->close_section();
}

void bluestore_wal_op_t::generate_test_instances(list<bluestore_wal_op_t*>& o)
{
  o.push_back(new bluestore_wal_op_t);
  o.push_back(new bluestore_wal_op_t);
  o.back()->op = OP_WRITE;
  o.back()->extent.offset = 1;
  o.back()->extent.length = 2;
  o.back()->src_extent.offset = 10000;
  o.back()->src_extent.length = 2;
  o.back()->src_rmw_head = 22;
  o.back()->src_rmw_tail = 88;
  o.back()->data.append("my data");
  o.back()->nid = 3;
  o.back()->overlays.push_back(bluestore_overlay_t());
  o.back()->overlays.push_back(bluestore_overlay_t());
  o.back()->overlays.back().key = 4;
  o.back()->overlays.back().value_offset = 5;
  o.back()->overlays.back().length = 6;
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
  o.back()->ops.back().extent.offset = 2;
  o.back()->ops.back().extent.length = 3;
  o.back()->ops.back().data.append("foodata");
  o.back()->ops.back().nid = 4;
}
