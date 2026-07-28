// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "erasure-code/ErasureCodeInterface.h"
#include "gtest/gtest.h"
#include <vector>
#include <utility>

/**
 * MockErasureCode - minimal ErasureCodeInterface implementation for testing.
 *
 * Uses ADD_FAILURE() for deprecated methods to catch incorrect usage.
 */
class MockErasureCode : public ErasureCodeInterface {
public:
  MockErasureCode(int data_chunks = 4, int total_chunks = 6)
    : data_chunk_count(data_chunks), chunk_count(total_chunks) {}

  uint64_t get_supported_optimizations() const override {
    return FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION |
          FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION |
          FLAG_EC_PLUGIN_ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION |
          FLAG_EC_PLUGIN_ZERO_PADDING_OPTIMIZATION |
          FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION;
  }

  int init(ErasureCodeProfile &profile, std::ostream *ss) override {
    return 0;
  }

  const ErasureCodeProfile &get_profile() const override {
    return _profile;
  }

  int create_rule(const std::string &name, CrushWrapper &crush, std::ostream *ss) const override {
    return 0;
  }

  unsigned int get_chunk_count() const override {
    return chunk_count;
  }

  unsigned int get_data_chunk_count() const override {
    return data_chunk_count;
  }

  unsigned int get_coding_chunk_count() const override {
    return chunk_count - data_chunk_count;
  }

  int get_sub_chunk_count() override {
    return 1;
  }

  unsigned int get_chunk_size(unsigned int stripe_width) const override {
    return stripe_width / data_chunk_count;
  }

  int minimum_to_decode(const shard_id_set &want_to_read, const shard_id_set &available,
                        shard_id_set &minimum_set,
		shard_id_map<std::vector<std::pair<int, int>>> *minimum_sub_chunks) override {
    bool recover = false;
    for (shard_id_t shard : want_to_read) {
      if (available.contains(shard)) {
        minimum_set.insert(shard);
      } else {
        recover = true;
        break;
      }
    }

    if (recover) {
      minimum_set.clear();

      // Shard is missing. Collect data_chunk_count shards from available
      // shards for recovery.
      for (auto a : available) {
        minimum_set.insert(a);
        if (std::cmp_equal(minimum_set.size(), data_chunk_count)) {
          break;
        }
      }

      if (std::cmp_not_equal(minimum_set.size(), data_chunk_count)) {
        minimum_set.clear();
        return -EIO; // Cannot recover.
      }
    }

    if (minimum_sub_chunks) {
      for (auto &&shard : minimum_set) {
        minimum_sub_chunks->emplace(shard, default_sub_chunk);
      }
    }
    return 0;
  }

  [[deprecated]]
  int minimum_to_decode(const std::set<int> &want_to_read,
    const std::set<int> &available,
    std::map<int, std::vector<std::pair<int, int>>> *minimum) override
  {
    ADD_FAILURE();
    return 0;
  }

  [[deprecated]]
  int minimum_to_decode_with_cost(const std::set<int> &want_to_read,
      const std::map<int, int> &available, std::set<int> *minimum) override {
    ADD_FAILURE();
    return 0;
  }

  int minimum_to_decode_with_cost(const shard_id_set &want_to_read, const shard_id_map<int> &available,
                                shard_id_set *minimum) override {
    return 0;
  }

  int encode(const shard_id_set &want_to_encode, const bufferlist &in, shard_id_map<bufferlist> *encoded) override {
    return 0;
  }

  [[deprecated]]
  int encode(const std::set<int> &want_to_encode, const bufferlist &in
    , std::map<int, bufferlist> *encoded) override
  {
    ADD_FAILURE();
    return 0;
  }

  [[deprecated]]
  int encode_chunks(const std::set<int> &want_to_encode,
                    std::map<int, bufferlist> *encoded) override
  {
    ADD_FAILURE();
    return 0;
  }

  int encode_chunks(const shard_id_map<bufferptr> &in, shard_id_map<bufferptr> &out) override {
    return 0;
  }

  int decode(const shard_id_set &want_to_read, const shard_id_map<bufferlist> &chunks, shard_id_map<bufferlist> *decoded,
	     int chunk_size) override {
    return 0;
  }

  [[deprecated]]
  int decode(const std::set<int> &want_to_read, const std::map<int, bufferlist> &chunks,
    std::map<int, bufferlist> *decoded, int chunk_size) override
  {
    ADD_FAILURE();
    return 0;
  }

  [[deprecated]]
  int decode_chunks(const std::set<int> &want_to_read,
                    const std::map<int, bufferlist> &chunks,
                    std::map<int, bufferlist> *decoded) override {
    ADD_FAILURE();
    return 0;
  }

  int decode_chunks(const shard_id_set &want_to_read,
                    shard_id_map<bufferptr> &in, shard_id_map<bufferptr> &out) override
  {
    if (std::cmp_less(in.size(), data_chunk_count)) {
      ADD_FAILURE();
    }
    uint64_t len = 0;
    for (auto &&[shard, bp] : in) {
      if (len == 0) {
        len = bp.length();
      } else if (len != bp.length()) {
        ADD_FAILURE();
      }
    }
    if (len == 0) {
      ADD_FAILURE();
    }
    if (out.size() == 0) {
      ADD_FAILURE();
    }
    for (auto &&[shard, bp] : out) {
      if (len != bp.length()) {
        ADD_FAILURE();
      }
      if (bp.is_zero_fast()) {
        ADD_FAILURE();
      }
    }
    return 0;
  }

  const std::vector<shard_id_t> &get_chunk_mapping() const override {
    return chunk_mapping;
  }

  [[deprecated]]
  int decode_concat(const std::set<int> &want_to_read,
                    const std::map<int, bufferlist> &chunks, bufferlist *decoded) override {
    ADD_FAILURE();
    return 0;
  }

  [[deprecated]]
  int decode_concat(const std::map<int, bufferlist> &chunks,
                    bufferlist *decoded) override {
    ADD_FAILURE();
    return 0;
  }

  size_t get_minimum_granularity() override { return 0; }
  
  void encode_delta(const bufferptr &old_data, const bufferptr &new_data
    , bufferptr *delta) override {}
  
  void apply_delta(const shard_id_map<bufferptr> &in
    , shard_id_map<bufferptr> &out) override {}

  std::vector<std::pair<int, int>> default_sub_chunk = {std::pair(0,1)};

private:
  ErasureCodeProfile _profile;
  const std::vector<shard_id_t> chunk_mapping = {}; // no remapping
  int data_chunk_count;
  int chunk_count;
};

