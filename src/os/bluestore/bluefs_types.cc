// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "bluefs_types.h"
#include "common/Formatter.h"
#include "include/uuid.h"
#include "include/stringify.h"

// bluefs_extent_t

void bluefs_extent_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(offset, bl);
  ::encode(length, bl);
  ::encode(bdev, bl);
  ENCODE_FINISH(bl);
}

void bluefs_extent_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(offset, p);
  ::decode(length, p);
  ::decode(bdev, p);
  DECODE_FINISH(p);
}

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

ostream& operator<<(ostream& out, bluefs_extent_t e)
{
  return out << e.bdev << ":" << e.offset << "+" << e.length;
}

// bluefs_super_t

void bluefs_super_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(uuid, bl);
  ::encode(osd_uuid, bl);
  ::encode(version, bl);
  ::encode(block_size, bl);
  ::encode(log_fnode, bl);
  ENCODE_FINISH(bl);
}

void bluefs_super_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(uuid, p);
  ::decode(osd_uuid, p);
  ::decode(version, p);
  ::decode(block_size, p);
  ::decode(log_fnode, p);
  DECODE_FINISH(p);
}

void bluefs_super_t::dump(Formatter *f) const
{
  f->dump_stream("uuid") << uuid;
  f->dump_stream("osd_uuid") << osd_uuid;
  f->dump_unsigned("version", version);
  f->dump_unsigned("block_size", block_size);
  f->dump_object("log_fnode", log_fnode);
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
	     << " block_size " << s.block_size
	     << " log_fnode " << s.log_fnode
	     << ")";
}

// bluefs_fnode_t

vector<bluefs_extent_t>::iterator bluefs_fnode_t::seek(
  uint64_t offset, uint64_t *x_off)
{
  vector<bluefs_extent_t>::iterator p = extents.begin();
  while (p != extents.end()) {
    if (offset >= p->length) {
      offset -= p->length;
      ++p;
    } else {
      break;
    }
  }
  *x_off = offset;
  return p;
}

void bluefs_fnode_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(ino, bl);
  ::encode(size, bl);
  ::encode(mtime, bl);
  ::encode(prefer_bdev, bl);
  ::encode(extents, bl);
  ENCODE_FINISH(bl);
}

void bluefs_fnode_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(ino, p);
  ::decode(size, p);
  ::decode(mtime, p);
  ::decode(prefer_bdev, p);
  ::decode(extents, p);
  DECODE_FINISH(p);
}

void bluefs_fnode_t::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_unsigned("size", size);
  f->dump_stream("mtime") << mtime;
  f->dump_unsigned("prefer_bdev", prefer_bdev);
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
  ls.back()->prefer_bdev = 1;
}

ostream& operator<<(ostream& out, const bluefs_fnode_t& file)
{
  return out << "file(ino " << file.ino
	     << " size " << file.size
	     << " mtime " << file.mtime
	     << " bdev " << (int)file.prefer_bdev
	     << " extents " << file.extents
	     << ")";
}


// bluefs_transaction_t

void bluefs_transaction_t::encode(bufferlist& bl) const
{
  uint32_t crc = op_bl.crc32c(-1);
  ENCODE_START(1, 1, bl);
  ::encode(uuid, bl);
  ::encode(seq, bl);
  ::encode(op_bl, bl);
  ::encode(crc, bl);
  ENCODE_FINISH(bl);
}

void bluefs_transaction_t::decode(bufferlist::iterator& p)
{
  uint32_t crc;
  DECODE_START(1, p);
  ::decode(uuid, p);
  ::decode(seq, p);
  ::decode(op_bl, p);
  ::decode(crc, p);
  DECODE_FINISH(p);
  uint32_t actual = op_bl.crc32c(-1);
  if (actual != crc)
    throw buffer::malformed_input("bad crc " + stringify(actual)
				  + " expected " + stringify(crc));
}

void bluefs_transaction_t::dump(Formatter *f) const
{
  f->dump_stream("uuid") << uuid;
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("op_bl_length", op_bl.length());
  f->dump_unsigned("crc", op_bl.crc32c(-1));
}

void bluefs_transaction_t::generate_test_instance(
  list<bluefs_transaction_t*>& ls)
{
  ls.push_back(new bluefs_transaction_t);
  ls.push_back(new bluefs_transaction_t);
  ls.back()->op_init();
  ls.back()->op_alloc_add(0, 0, 123123211);
  ls.back()->op_alloc_rm(1, 0, 123);
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
	     << " len " << t.op_bl.length()
	     << " crc " << t.op_bl.crc32c(-1)
	     << ")";
}
