// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>
#include <string>

#include "include/buffer.h"

class CDC {
public:
  virtual ~CDC() = default;

  virtual void calc_chunks(
    ceph::buffer::list& inputdata,
    std::vector<std::pair<uint64_t, uint64_t>> *chunks) = 0;

  virtual void set_target_bits(int bits, int windowbits = 2) = 0;

  static std::unique_ptr<CDC> create(
    const std::string& type,
    int bits,
    int windowbits = 0);
};
