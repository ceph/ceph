// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/bloom_filter.hpp"

#include <bit>
#include <numeric>

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::Formatter;

double bloom_filter::density() const
{
  // TODO: use transform_reduce() in GCC-9 and up
  unsigned set = std::accumulate(
    bit_table_.begin(),
    bit_table_.begin() + table_size_,
    0u, [](unsigned set, cell_type cell) {
      return set + std::popcount(cell);
    });
  return (double)set / (table_size_ * sizeof(cell_type) * CHAR_BIT);
}

void bloom_filter::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  encode((uint64_t)salt_count_, bl);
  encode((uint64_t)insert_count_, bl);
  encode((uint64_t)target_element_count_, bl);
  encode((uint64_t)random_seed_, bl);
  encode(bit_table_, bl);
  ENCODE_FINISH(bl);
}

void bloom_filter::decode(bufferlist::const_iterator& p)
{
  DECODE_START(2, p);
  uint64_t v;
  decode(v, p);
  salt_count_ = v;
  decode(v, p);
  insert_count_ = v;
  decode(v, p);
  target_element_count_ = v;
  decode(v, p);
  random_seed_ = v;
  salt_.clear();
  generate_unique_salt();
  decode(bit_table_, p);
  table_size_ = bit_table_.size();
  DECODE_FINISH(p);
}

void bloom_filter::dump(Formatter *f) const
{
  f->dump_unsigned("salt_count", salt_count_);
  f->dump_unsigned("table_size", table_size_);
  f->dump_unsigned("insert_count", insert_count_);
  f->dump_unsigned("target_element_count", target_element_count_);
  f->dump_unsigned("random_seed", random_seed_);

  f->open_array_section("salt_table");
  for (std::vector<bloom_type>::const_iterator i = salt_.begin(); i != salt_.end(); ++i)
    f->dump_unsigned("salt", *i);
  f->close_section();

  f->open_array_section("bit_table");
  for (auto byte : bit_table_) {
    f->dump_unsigned("byte", (unsigned)byte);
  }
  f->close_section();
}

void bloom_filter::generate_test_instances(std::list<bloom_filter*>& ls)
{
  ls.push_back(new bloom_filter(10, .5, 1));
  ls.push_back(new bloom_filter(10, .5, 1));
  ls.back()->insert("foo");
  ls.back()->insert("bar");
  ls.push_back(new bloom_filter(50, .5, 1));
  ls.back()->insert("foo");
  ls.back()->insert("bar");
  ls.back()->insert("baz");
  ls.back()->insert("boof");
  ls.back()->insert("boogggg");
}


void compressible_bloom_filter::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  bloom_filter::encode(bl);

  uint32_t s = size_list.size();
  encode(s, bl);
  for (std::vector<size_t>::const_iterator p = size_list.begin();
       p != size_list.end(); ++p)
    encode((uint64_t)*p, bl);

  ENCODE_FINISH(bl);
}

void compressible_bloom_filter::decode(bufferlist::const_iterator& p)
{
  DECODE_START(2, p);
  bloom_filter::decode(p);

  uint32_t s;
  decode(s, p);
  size_list.resize(s);
  for (unsigned i = 0; i < s; i++) {
    uint64_t v;
    decode(v, p);
    size_list[i] = v;
  }

  DECODE_FINISH(p);
}

void compressible_bloom_filter::dump(Formatter *f) const
{
  bloom_filter::dump(f);

  f->open_array_section("table_sizes");
  for (std::vector<size_t>::const_iterator p = size_list.begin();
       p != size_list.end(); ++p)
    f->dump_unsigned("size", (uint64_t)*p);
  f->close_section();
}

void compressible_bloom_filter::generate_test_instances(std::list<compressible_bloom_filter*>& ls)
{
  ls.push_back(new compressible_bloom_filter(10, .5, 1));
  ls.push_back(new compressible_bloom_filter(10, .5, 1));
  ls.back()->insert("foo");
  ls.back()->insert("bar");
  ls.push_back(new compressible_bloom_filter(50, .5, 1));
  ls.back()->insert("foo");
  ls.back()->insert("bar");
  ls.back()->insert("baz");
  ls.back()->insert("boof");
  ls.back()->compress(20);
  ls.back()->insert("boogggg");
}
