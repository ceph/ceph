// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef COMPRESSION_H_INCLUDED
#define COMPRESSION_H_INCLUDED

#include "BlueStore.h"
#include "Writer.h"

class BlueStore::Estimator {
  BlueStore* bluestore;
public:
  Estimator(BlueStore* bluestore,
            uint32_t min_alloc_size,
            uint32_t max_blob_size)
  :bluestore(bluestore)
  ,min_alloc_size(min_alloc_size)
  ,max_blob_size(max_blob_size) {}

  // return estimated size if extent gets compressed / recompressed
  uint32_t estimate(const BlueStore::Extent* recompress_candidate);
  // return estimated size if data gets compressed
  uint32_t estimate(uint32_t new_data);
  // make a decision about including region to recompression
  // gain = size on disk (after release or taken if no compression)
  // cost = size estimated on disk after compression
  bool is_worth(uint32_t gain, uint32_t cost);

  double expected_compression_factor = 0.5;
  uint32_t min_alloc_size;
  uint32_t max_blob_size;

  struct region_t {
    uint32_t offset; // offset of region
    uint32_t length; // size of region
    std::vector<uint32_t> blob_sizes; //sizes of blobs to split into
  };

  void split(uint32_t size);
  void split(uint32_t raw_size, uint32_t compr_size);
  void mark_recompress(const BlueStore::Extent* e);
  void mark_main(uint32_t location, uint32_t length);
  void get_regions(std::vector<region_t>& regions);

  void split_and_compress(
    CompressorRef compr,
    ceph::buffer::list& data_bl,
    Writer::blob_vec& bd);

  private:
  struct is_less {
    bool operator() (
    const BlueStore::Extent* l,
    const BlueStore::Extent* r) const {
      return l->logical_offset < r->logical_offset;
    }
  };
  std::map<uint32_t, uint32_t> extra_recompress;
};

class BlueStore::Scanner {
  BlueStore* bluestore;
public:
  Scanner(BlueStore* bluestore)
  :bluestore(bluestore) {}

  void write_lookaround(
    BlueStore::ExtentMap* extent_map,
    uint32_t offset, uint32_t length,
    uint32_t left_limit, uint32_t right_limit,
    Estimator* estimator);
  class Scan;
};

#endif
