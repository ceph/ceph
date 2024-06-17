// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include "bluefs_types.h"
#include "BlueFS.h"
#include "common/Formatter.h"
#include "include/denc.h"
#include "include/uuid.h"
#include "include/stringify.h"

using std::list;
using std::ostream;

using ceph::bufferlist;
using ceph::Formatter;

// bluefs_extent_t
void bluefs_extent_t::dump(Formatter *f) const
{
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
  f->dump_unsigned("bdev", bdev);
}

void bluefs_extent_t::generate_test_instances(list<bluefs_extent_t*>& ls)
{
  ls.push_back(new bluefs_extent_t);
  ls.push_back(new bluefs_extent_t);
  ls.back()->offset = 1;
  ls.back()->length = 2;
  ls.back()->bdev = 1;
}

ostream& operator<<(ostream& out, const bluefs_extent_t& e)
{
  return out << (int)e.bdev << ":0x" << std::hex << e.offset << "~" << e.length
	     << std::dec;
}

// bluefs_layout_t

void bluefs_layout_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(shared_bdev, bl);
  encode(dedicated_db, bl);
  encode(dedicated_wal, bl);
  ENCODE_FINISH(bl);
}

void bluefs_layout_t::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(shared_bdev, p);
  decode(dedicated_db, p);
  decode(dedicated_wal, p);
  DECODE_FINISH(p);
}

void bluefs_layout_t::dump(Formatter *f) const
{
  f->dump_stream("shared_bdev") << shared_bdev;
  f->dump_stream("dedicated_db") << dedicated_db;
  f->dump_stream("dedicated_wal") << dedicated_wal;
}

void bluefs_layout_t::generate_test_instances(list<bluefs_layout_t*>& ls)
{
  ls.push_back(new bluefs_layout_t);
  ls.push_back(new bluefs_layout_t);
  ls.back()->shared_bdev = 1;
  ls.back()->dedicated_db = true;
  ls.back()->dedicated_wal = true;
}

// bluefs_super_t
bluefs_super_t::bluefs_super_t() : version(0), block_size(4096) {
  bluefs_max_alloc_size.resize(BlueFS::MAX_BDEV, 0);
}

void bluefs_super_t::encode(bufferlist& bl) const
{
  ENCODE_START(3, 1, bl);
  encode(uuid, bl);
  encode(osd_uuid, bl);
  encode(version, bl);
  encode(block_size, bl);
  encode(log_fnode, bl);
  encode(memorized_layout, bl);
  encode(bluefs_max_alloc_size, bl);
  ENCODE_FINISH(bl);
}

void bluefs_super_t::decode(bufferlist::const_iterator& p)
{
  DECODE_START(3, p);
  decode(uuid, p);
  decode(osd_uuid, p);
  decode(version, p);
  decode(block_size, p);
  decode(log_fnode, p);
  if (struct_v >= 2) {
    decode(memorized_layout, p);
  }
  if (struct_v >= 3) {
    decode(bluefs_max_alloc_size, p);
  } else {
    std::fill(bluefs_max_alloc_size.begin(), bluefs_max_alloc_size.end(), 0);
  }
  DECODE_FINISH(p);
}

void bluefs_super_t::dump(Formatter *f) const
{
  f->dump_stream("uuid") << uuid;
  f->dump_stream("osd_uuid") << osd_uuid;
  f->dump_unsigned("version", version);
  f->dump_unsigned("block_size", block_size);
  f->dump_object("log_fnode", log_fnode);
  for (auto& p : bluefs_max_alloc_size)
    f->dump_unsigned("max_alloc_size", p);
}

void bluefs_super_t::generate_test_instances(list<bluefs_super_t*>& ls)
{
  ls.push_back(new bluefs_super_t);
  ls.push_back(new bluefs_super_t);
  ls.back()->version = 1;
  ls.back()->block_size = 4096;
}

ostream& operator<<(ostream& out, const bluefs_super_t& s)
{
  return out << "super(uuid " << s.uuid
	     << " osd " << s.osd_uuid
	     << " v " << s.version
	     << " block_size 0x" << std::hex << s.block_size
	     << " log_fnode 0x" << s.log_fnode
	     << " max_alloc_size " << s.bluefs_max_alloc_size
	     << std::dec << ")";
}

// bluefs_fnode_t

mempool::bluefs::vector<bluefs_extent_t>::iterator bluefs_fnode_t::seek(
  uint64_t offset, uint64_t *x_off)
{
  auto p = extents.begin();

  if (extents_index.size() > 4) {
    auto it = std::upper_bound(extents_index.begin(), extents_index.end(),
      offset);
    assert(it != extents_index.begin());
    --it;
    assert(offset >= *it);
    p += it - extents_index.begin();
    offset -= *it;
  }

  while (p != extents.end()) {
    if ((int64_t) offset >= p->length) {
      offset -= p->length;
      ++p;
    } else {
      break;
    }
  }
  *x_off = offset;
  return p;
}

bluefs_fnode_delta_t* bluefs_fnode_t::make_delta(bluefs_fnode_delta_t* delta) {
  ceph_assert(delta);
  delta->ino = ino;
  delta->size = size;
  delta->mtime = mtime;
  delta->offset = allocated_commited;
  delta->extents.clear();
  if (allocated_commited < allocated) {
    uint64_t x_off = 0;
    auto p = seek(allocated_commited, &x_off);
    ceph_assert(p != extents.end());
    if (x_off > 0) {
      ceph_assert(x_off < p->length);
      delta->extents.emplace_back(p->bdev, p->offset + x_off, p->length - x_off);
      ++p;
    }
    while (p != extents.end()) {
      delta->extents.push_back(*p);
      ++p;
    }
  }
  return delta;
}

void bluefs_fnode_t::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_unsigned("size", size);
  f->dump_stream("mtime") << mtime;
  f->open_array_section("extents");
  for (auto& p : extents)
    f->dump_object("extent", p);
  f->close_section();
}

void bluefs_fnode_t::generate_test_instances(list<bluefs_fnode_t*>& ls)
{
  ls.push_back(new bluefs_fnode_t);
  ls.push_back(new bluefs_fnode_t);
  ls.back()->ino = 123;
  ls.back()->size = 1048576;
  ls.back()->mtime = utime_t(123,45);
  ls.back()->extents.push_back(bluefs_extent_t(0, 1048576, 4096));
  ls.back()->__unused__ = 1;
}

ostream& operator<<(ostream& out, const bluefs_fnode_t& file)
{
  return out << "file(ino " << file.ino
	     << " size 0x" << std::hex << file.size << std::dec
	     << " mtime " << file.mtime
	     << " allocated " << std::hex << file.allocated << std::dec
	     << " alloc_commit " << std::hex << file.allocated_commited << std::dec
	     << " extents " << file.extents
	     << ")";
}

// bluefs_fnode_delta_t

std::ostream& operator<<(std::ostream& out, const bluefs_fnode_delta_t& delta)
{
  return out << "delta(ino " << delta.ino
	     << " size 0x" << std::hex << delta.size << std::dec
	     << " mtime " << delta.mtime
	     << " offset " << std::hex << delta.offset << std::dec
	     << " extents " << delta.extents
	     << ")";
}

// bluefs_transaction_t

void bluefs_transaction_t::bound_encode(size_t &s) const {
  uint32_t crc = -1;
  s += 1; // version
  s += 1; // compat
  s += 4; // size
  denc(uuid, s);
  denc(seq, s);
  denc(op_bl, s);
  denc(crc, s);
}

void bluefs_transaction_t::encode(bufferlist& bl) const
{
  uint32_t crc = op_bl.crc32c(-1);
  ENCODE_START(1, 1, bl);
  encode(uuid, bl);
  encode(seq, bl);
  // not using bufferlist encode method, as it merely copies the bufferptr and not
  // contents, meaning we're left with fragmented target bl
  __u32 len = op_bl.length();
  encode(len, bl);
  for (auto& it : op_bl.buffers()) {
    bl.append(it.c_str(),  it.length());
  }
  encode(crc, bl);
  ENCODE_FINISH(bl);
}

void bluefs_transaction_t::decode(bufferlist::const_iterator& p)
{
  uint32_t crc;
  DECODE_START(1, p);
  decode(uuid, p);
  decode(seq, p);
  decode(op_bl, p);
  decode(crc, p);
  DECODE_FINISH(p);
  uint32_t actual = op_bl.crc32c(-1);
  if (actual != crc)
    throw ceph::buffer::malformed_input("bad crc " + stringify(actual)
				  + " expected " + stringify(crc));
}

void bluefs_transaction_t::dump(Formatter *f) const
{
  f->dump_stream("uuid") << uuid;
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("op_bl_length", op_bl.length());
  f->dump_unsigned("crc", op_bl.crc32c(-1));
}

void bluefs_transaction_t::generate_test_instances(
  list<bluefs_transaction_t*>& ls)
{
  ls.push_back(new bluefs_transaction_t);
  ls.push_back(new bluefs_transaction_t);
  ls.back()->op_init();
  ls.back()->op_dir_create("dir");
  ls.back()->op_dir_create("dir2");
  bluefs_fnode_t fnode;
  fnode.ino = 2;
  ls.back()->op_file_update(fnode);
  ls.back()->op_dir_link("dir", "file1", 2);
  ls.back()->op_dir_unlink("dir", "file1");
  ls.back()->op_file_remove(2);
  ls.back()->op_dir_remove("dir2");
}

ostream& operator<<(ostream& out, const bluefs_transaction_t& t)
{
  return out << "txn(seq " << t.seq
	     << " len 0x" << std::hex << t.op_bl.length()
	     << " crc 0x" << t.op_bl.crc32c(-1)
	     << std::dec << ")";
}

