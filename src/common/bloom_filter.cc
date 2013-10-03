// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "common/bloom_filter.hpp"

void bloom_filter::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode((uint64_t)salt_count_, bl);
  ::encode((uint64_t)table_size_, bl);
  ::encode((uint64_t)inserted_element_count_, bl);
  ::encode((uint64_t)random_seed_, bl);
  bufferptr bp((const char*)bit_table_, raw_table_size_);
  ::encode(bp, bl);
  ENCODE_FINISH(bl);
}

void bloom_filter::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  uint64_t v;
  ::decode(v, p);
  salt_count_ = v;
  ::decode(v, p);
  table_size_ = v;
  ::decode(v, p);
  inserted_element_count_ = v;
  ::decode(v, p);
  random_seed_ = v;
  bufferlist t;
  ::decode(t, p);

  salt_.clear();
  generate_unique_salt();
  raw_table_size_ = t.length();
  assert(raw_table_size_ == table_size_ / bits_per_char);
  delete bit_table_;
  bit_table_ = new cell_type[raw_table_size_];
  t.copy(0, raw_table_size_, (char *)bit_table_);

  DECODE_FINISH(p);
}

void bloom_filter::dump(Formatter *f) const
{
  f->dump_unsigned("salt_count", salt_count_);
  f->dump_unsigned("table_size", table_size_);
  f->dump_unsigned("raw_table_size", raw_table_size_);
  f->dump_unsigned("insert_count", inserted_element_count_);
  f->dump_unsigned("random_seed", random_seed_);

  f->open_array_section("salt_table");
  for (std::vector<bloom_type>::const_iterator i = salt_.begin(); i != salt_.end(); ++i)
    f->dump_unsigned("salt", *i);
  f->close_section();

  f->open_array_section("bit_table");
  for (unsigned i = 0; i < raw_table_size_; ++i)
    f->dump_unsigned("byte", (unsigned)bit_table_[i]);
  f->close_section();
}

void bloom_filter::generate_test_instances(list<bloom_filter*>& ls)
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
