// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "KeyValueHistogram.h"
using std::map;
using std::string;
using ceph::Formatter;

#define KEY_SLAB 32
#define VALUE_SLAB 64

int KeyValueHistogram::get_key_slab(size_t sz)
{
  return (sz / KEY_SLAB);
}

string KeyValueHistogram::get_key_slab_to_range(int slab)
{
  int lower_bound = slab * KEY_SLAB;
  int upper_bound = (slab + 1) * KEY_SLAB;
  string ret = "[" + stringify(lower_bound) + "," + stringify(upper_bound) + ")";
  return ret;
}

int KeyValueHistogram::get_value_slab(size_t sz)
{
  return (sz / VALUE_SLAB);
}

string KeyValueHistogram::get_value_slab_to_range(int slab)
{
  int lower_bound = slab * VALUE_SLAB;
  int upper_bound = (slab + 1) * VALUE_SLAB;
  string ret = "[" + stringify(lower_bound) + "," + stringify(upper_bound) + ")";
  return ret;
}

void KeyValueHistogram::update_hist_entry(map<string, map<int, struct key_dist> >& key_hist,
  const string& prefix, size_t key_size, size_t value_size)
{
  uint32_t key_slab = get_key_slab(key_size);
  uint32_t value_slab = get_value_slab(value_size);
  key_hist[prefix][key_slab].count++;
  key_hist[prefix][key_slab].max_len =
    std::max<size_t>(key_size, key_hist[prefix][key_slab].max_len);
  key_hist[prefix][key_slab].val_map[value_slab].count++;
  key_hist[prefix][key_slab].val_map[value_slab].max_len =
    std::max<size_t>(value_size,
      key_hist[prefix][key_slab].val_map[value_slab].max_len);
}

void KeyValueHistogram::dump(Formatter* f)
{
  f->open_object_section("rocksdb_value_distribution");
  for (auto i : value_hist) {
    f->dump_unsigned(get_value_slab_to_range(i.first).data(), i.second);
  }
  f->close_section();

  f->open_object_section("rocksdb_key_value_histogram");
  for (auto i : key_hist) {
    f->dump_string("prefix", i.first);
    f->open_object_section("key_hist");
    for (auto k : i.second) {
      f->dump_unsigned(get_key_slab_to_range(k.first).data(), k.second.count);
      f->dump_unsigned("max_len", k.second.max_len);
      f->open_object_section("value_hist");
      for (auto j : k.second.val_map) {
        f->dump_unsigned(get_value_slab_to_range(j.first).data(), j.second.count);
        f->dump_unsigned("max_len", j.second.max_len);
      }
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}
