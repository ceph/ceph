// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <random>

#include "CDC.h"
#include "FastCDC.h"
#include "FixedCDC.h"

std::unique_ptr<CDC> CDC::create(
  const std::string& type,
  int bits,
  int windowbits)
{
  if (type == "fastcdc") {
    return std::unique_ptr<CDC>(new FastCDC(bits, windowbits));
  }
  if (type == "fixed") {
    return std::unique_ptr<CDC>(new FixedCDC(bits, windowbits));
  }
  return nullptr;
}

void generate_buffer(int size, bufferlist *outbl, int seed)
{
  std::mt19937_64 engine, engine2;
  engine.seed(seed);
  engine2.seed(seed);

  // assemble from randomly-sized segments!
  outbl->clear();
  auto left = size;
  while (left) {
    size_t l = std::min<size_t>((engine2() & 0xffff0) + 16, left);
    left -= l;
    bufferptr p(l);
    p.set_length(l);
    char *b = p.c_str();
    for (size_t i = 0; i < l / sizeof(uint64_t); ++i) {
      ((ceph_le64 *)b)[i] = ceph_le64(engine());
    }
    outbl->append(p);
  }
}

