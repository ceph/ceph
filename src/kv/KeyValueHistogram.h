// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef KeyValueHistogram_H
#define KeyValueHistogram_H

#include <map>
#include <string>
#include "common/Formatter.h"

/**
 *
 * Key Value DB Histogram generator
 *
 */
struct KeyValueHistogram {
  struct value_dist {
    uint64_t count;
    uint32_t max_len;
  };

  struct key_dist {
    uint64_t count;
    uint32_t max_len;
    std::map<int, struct value_dist> val_map; ///< slab id to count, max length of value and key
  };

  std::map<std::string, std::map<int, struct key_dist> > key_hist;
  std::map<int, uint64_t> value_hist;
  int get_key_slab(size_t sz);
  std::string get_key_slab_to_range(int slab);
  int get_value_slab(size_t sz);
  std::string get_value_slab_to_range(int slab);
  void update_hist_entry(std::map<std::string, std::map<int, struct key_dist> >& key_hist,
    const std::string& prefix, size_t key_size, size_t value_size);
  void dump(ceph::Formatter* f);
};

#endif
