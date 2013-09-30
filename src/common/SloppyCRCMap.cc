// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "common/SloppyCRCMap.h"
#include "common/Formatter.h"

void SloppyCRCMap::write(uint64_t offset, uint64_t len, const bufferlist& bl,
			 std::ostream *out)
{
  int64_t left = len;
  uint64_t pos = offset;
  unsigned o = offset % block_size;
  if (o) {
    crc_map.erase(offset - o);
    if (out)
      *out << "write invalidate " << (offset - o) << "\n";
    pos += (block_size - o);
    left -= (block_size - o);
  }
  while (left >= block_size) {
    bufferlist t;
    t.substr_of(bl, pos - offset, block_size);
    crc_map[pos] = t.crc32c(crc_iv);
    if (out)
      *out << "write set " << pos << " " << crc_map[pos] << "\n";
    pos += block_size;
    left -= block_size;
  }
  if (left > 0) {
    crc_map.erase(pos);
    if (out)
      *out << "write invalidate " << pos << "\n";
  }
}

int SloppyCRCMap::read(uint64_t offset, uint64_t len, const bufferlist& bl,
		       std::ostream *err)
{
  int errors = 0;
  int64_t left = len;
  uint64_t pos = offset;
  unsigned o = offset % block_size;
  if (o) {
    pos += (block_size - o);
    left -= (block_size - o);
  }
  while (left >= block_size) {
    // FIXME: this could be more efficient if we avoid doing a find()
    // on each iteration
    std::map<uint64_t,uint32_t>::iterator p = crc_map.find(pos);
    if (p != crc_map.end()) {
      bufferlist t;
      t.substr_of(bl, pos - offset, block_size);
      uint32_t crc = t.crc32c(crc_iv);
      if (p->second != crc) {
	errors++;
	if (err)
	  *err << "offset " << pos << " len " << block_size
	       << " has crc " << crc << " expected " << p->second << "\n";
      }
    }
    pos += block_size;
    left -= block_size;
  }
  return errors;  
}

void SloppyCRCMap::truncate(uint64_t offset)
{
  offset -= offset % block_size;
  std::map<uint64_t,uint32_t>::iterator p = crc_map.lower_bound(offset);
  while (p != crc_map.end())
    crc_map.erase(p++);
}

void SloppyCRCMap::zero(uint64_t offset, uint64_t len)
{
  int64_t left = len;
  uint64_t pos = offset;
  unsigned o = offset % block_size;
  if (o) {
    crc_map.erase(offset - o);
    pos += (block_size - o);
    left -= (block_size - o);
  }
  while (left >= block_size) {
    crc_map[pos] = zero_crc;
    pos += block_size;
    left -= block_size;
  }
  if (left > 0)
    crc_map.erase(pos);
}

void SloppyCRCMap::clone_range(uint64_t offset, uint64_t len,
			       uint64_t srcoff, const SloppyCRCMap& src,
			       std::ostream *out)
{
  int64_t left = len;
  uint64_t pos = offset;
  uint64_t srcpos = srcoff;
  unsigned o = offset % block_size;
  if (o) {
    crc_map.erase(offset - o);
    pos += (block_size - o);
    srcpos += (block_size - o);
    left -= (block_size - o);
    if (out)
      *out << "clone_range invalidate " << (offset - o) << "\n";
  }
  while (left >= block_size) {
    // FIXME: this could be more efficient.
    if (block_size == src.block_size) {
      map<uint64_t,uint32_t>::const_iterator p = src.crc_map.find(srcpos);
      if (p != src.crc_map.end()) {
	crc_map[pos] = p->second;
	if (out)
	  *out << "clone_range copy " << pos << " " << p->second << "\n";
      } else {
	crc_map.erase(pos);
	if (out)
	  *out << "clone_range invalidate " << pos << "\n";
      }
    } else {
      crc_map.erase(pos);
      if (out)
	*out << "clone_range invalidate " << pos << "\n";
    }
    pos += block_size;
    srcpos += block_size;
    left -= block_size;
  }
  if (left > 0) {
    crc_map.erase(pos);
    if (out)
      *out << "clone_range invalidate " << pos << "\n";
  }
}

void SloppyCRCMap::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(block_size, bl);
  ::encode(crc_map, bl);
  ENCODE_FINISH(bl);
}

void SloppyCRCMap::decode(bufferlist::iterator& bl)
{
  DECODE_START(1, bl);
  uint32_t bs;
  ::decode(bs, bl);
  set_block_size(bs);
  ::decode(crc_map, bl);
  DECODE_FINISH(bl);
}

void SloppyCRCMap::dump(Formatter *f) const
{
  f->dump_unsigned("block_size", block_size);
  f->open_array_section("crc_map");
  for (map<uint64_t,uint32_t>::const_iterator p = crc_map.begin(); p != crc_map.end(); ++p) {
    f->open_object_section("crc");
    f->dump_unsigned("offset", p->first);
    f->dump_unsigned("crc", p->second);
    f->close_section();
  }
  f->close_section();
}

void SloppyCRCMap::generate_test_instances(list<SloppyCRCMap*>& ls)
{
  ls.push_back(new SloppyCRCMap);
  ls.push_back(new SloppyCRCMap(2));
  bufferlist bl;
  bl.append("some data");
  ls.back()->write(1, bl.length(), bl);
  ls.back()->write(10, bl.length(), bl);
  ls.back()->zero(4, 2);
}
