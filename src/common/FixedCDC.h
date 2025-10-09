// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "CDC.h"

class FixedCDC : public CDC {
private:
  size_t chunk_size;

public:
  FixedCDC(int target = 18, int window_bits = 0) {
    set_target_bits(target, window_bits);
  };

  void set_target_bits(int target, int window_bits) override {
    chunk_size = 1ul << target;
  }
  void calc_chunks(
    const bufferlist& bl,
    std::vector<std::pair<uint64_t, uint64_t>> *chunks) const override;
};
