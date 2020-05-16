// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "CDC.h"

// Based on this paper:
//   https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf
//
// Changes:
//   - window size fixed at 64 bytes (to match our word size)
//   - use XOR instead of +
//   - match mask instead of 0
//   - use target mask when close to target size (instead of
//     small/large mask).  The idea here is to try to use a consistent (target)
//     mask for most cut points if we can, and only resort to small/large mask
//     when we are (very) small or (very) large.

// Note about the target_bits: The goal is an average chunk size of 1
// << target_bits.  However, in reality the average is ~1.25x that
// because of the hard mininum chunk size.

class FastCDC : public CDC {
private:
  int target_bits, min_bits, max_bits;
  uint64_t target_mask, small_mask, large_mask;
  uint64_t table[256];

  const size_t window = 64;

  void _setup(int target, int window_bits);

public:
  FastCDC(int target = 18, int window_bits = 0) {
    _setup(target, window_bits);
  };

  void set_target_bits(int target, int window_bits) override {
    _setup(target, window_bits);
  }

  void calc_chunks(
    const bufferlist& bl,
    std::vector<std::pair<uint64_t, uint64_t>> *chunks) const override;
};
