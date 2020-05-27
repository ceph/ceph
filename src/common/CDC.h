// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>
#include <string>

#include "include/buffer.h"

class CDC {
public:
  virtual ~CDC() = default;

  /// calculate chunk boundaries as vector of (offset, length) pairs
  virtual void calc_chunks(
    const bufferlist& inputdata,
    std::vector<std::pair<uint64_t, uint64_t>> *chunks) const = 0;

  /// set target chunk size as a power of 2, and number of bits for hard min/max
  virtual void set_target_bits(int bits, int windowbits = 2) = 0;

  static std::unique_ptr<CDC> create(
    const std::string& type,
    int bits,
    int windowbits = 0);
};
