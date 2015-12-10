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

void cnode_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(bits, bl);
  ENCODE_FINISH(bl);
}

void cnode_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(bits, p);
  DECODE_FINISH(p);
}

void cnode_t::dump(Formatter *f) const
{
  f->dump_unsigned("bits", bits);
}

void cnode_t::generate_test_instances(list<cnode_t*>& o)
{
  o.push_back(new cnode_t());
  o.push_back(new cnode_t(0));
  o.push_back(new cnode_t(123));
}

// extent_t

string extent_t::get_flags_string(unsigned flags)
{
  string s;
  if (flags & FLAG_UNWRITTEN) {
    if (s.length())
      s += '+';
    s += "unwritten";
  }
  return s;
}

void extent_t::dump(Formatter *f) const
{
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
  f->dump_unsigned("flags", flags);
}

void extent_t::generate_test_instances(list<extent_t*>& o)
{
  o.push_back(new extent_t());
  o.push_back(new extent_t(123, 456));
  o.push_back(new extent_t(789, 1024, 322));
}

ostream& operator<<(ostream& out, const extent_t& e)
{
  out << e.offset << "~" << e.length;
  if (e.flags)
    out << ":" << extent_t::get_flags_string(e.flags);
  return out;
}

// overlay_t

void overlay_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(key, bl);
  ::encode(value_offset, bl);
  ::encode(length, bl);
  ENCODE_FINISH(bl);
}

void overlay_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(key, p);
  ::decode(value_offset, p);
  ::decode(length, p);
  DECODE_FINISH(p);
}

void overlay_t::dump(Formatter *f) const
{
  f->dump_unsigned("key", key);
  f->dump_unsigned("value_offset", value_offset);
  f->dump_unsigned("length", length);
}

void overlay_t::generate_test_instances(list<overlay_t*>& o)
{
  o.push_back(new overlay_t());
  o.push_back(new overlay_t(789, 1024, 1232232));
}

ostream& operator<<(ostream& out, const overlay_t& o)
{
  out << "overlay(" << o.value_offset << "~" << o.length
      << " key " << o.key << ")";
  return out;
}

// onode_t

void onode_t::encode(bufferlist& bl) const
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
  ENCODE_FINISH(bl);
}

void onode_t::decode(bufferlist::iterator& p)
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
  DECODE_FINISH(p);
}

void onode_t::dump(Formatter *f) const
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
  f->open_object_section("block_map");
  for (map<uint64_t, extent_t>::const_iterator p = block_map.begin();
       p != block_map.end(); ++p) {
    f->open_object_section("extent");
    f->dump_unsigned("extent_offset", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_object_section("overlays");
  for (map<uint64_t, overlay_t>::const_iterator p = overlay_map.begin();
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
}

void onode_t::generate_test_instances(list<onode_t*>& o)
{
  o.push_back(new onode_t());
  // FIXME
}

// wal_op_t

void wal_op_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(op, bl);
  ::encode(extent, bl);
  ::encode(nid, bl);
  ::encode(overlays, bl);
  if (!overlays.size()) {
    ::encode(data, bl);
  }
  ::encode(removed_overlays, bl);
  ENCODE_FINISH(bl);
}

void wal_op_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(op, p);
  ::decode(extent, p);
  ::decode(nid, p);
  ::decode(overlays, p);
  if (!overlays.size()) {
    ::decode(data, p);
  }
  ::decode(removed_overlays, p);
  DECODE_FINISH(p);
}

void wal_op_t::dump(Formatter *f) const
{
  f->dump_unsigned("op", (int)op);
  f->dump_object("extent", extent);
  f->dump_unsigned("nid", nid);
  f->open_array_section("overlays");
  for (vector<overlay_t>::const_iterator p = overlays.begin();
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

void wal_op_t::generate_test_instances(list<wal_op_t*>& o)
{
  o.push_back(new wal_op_t);
  o.push_back(new wal_op_t);
  o.back()->op = OP_WRITE;
  o.back()->extent.offset = 1;
  o.back()->extent.length = 2;
  o.back()->data.append("my data");
  o.back()->nid = 3;
  o.back()->overlays.push_back(overlay_t());
  o.back()->overlays.push_back(overlay_t());
  o.back()->overlays.back().key = 4;
  o.back()->overlays.back().value_offset = 5;
  o.back()->overlays.back().length = 6;
}

void wal_transaction_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(seq, bl);
  ::encode(ops, bl);
  ::encode(released, bl);
  ENCODE_FINISH(bl);
}

void wal_transaction_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(seq, p);
  ::decode(ops, p);
  ::decode(released, p);
  DECODE_FINISH(p);
}

void wal_transaction_t::dump(Formatter *f) const
{
  f->dump_unsigned("seq", seq);
  f->open_array_section("ops");
  for (list<wal_op_t>::const_iterator p = ops.begin(); p != ops.end(); ++p) {
    f->dump_object("op", *p);
  }
  f->close_section();
}

void wal_transaction_t::generate_test_instances(list<wal_transaction_t*>& o)
{
  o.push_back(new wal_transaction_t());
  o.push_back(new wal_transaction_t());
  o.back()->seq = 123;
  o.back()->ops.push_back(wal_op_t());
  o.back()->ops.push_back(wal_op_t());
  o.back()->ops.back().op = wal_op_t::OP_WRITE;
  o.back()->ops.back().extent.offset = 2;
  o.back()->ops.back().extent.length = 3;
  o.back()->ops.back().data.append("foodata");
  o.back()->ops.back().nid = 4;
}
