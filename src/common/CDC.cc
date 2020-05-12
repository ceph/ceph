// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <random>

#include "CDC.h"
#include "rabin.h"

std::unique_ptr<CDC> CDC::create(
  const std::string& type,
  int bits,
  int windowbits)
{
  if (type == "rabin") {
    auto p = new RabinChunk();
    p->set_target_bits(bits, windowbits);
    return std::unique_ptr<CDC>(p);
  }
  return nullptr;
}
