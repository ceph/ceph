// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/crc64nvme.h"

#include "include/buffer.h"

extern "C" {
#include "common/madler/crc64nvme.h"
}

namespace ceph {

uint64_t crc64nvme(uint64_t crc, const void* data, size_t len)
{
  return crc64nvme_word(crc, data, len);
}

uint64_t crc64nvme(const ceph::buffer::list& bl)
{
  uint64_t crc = 0;
  for (const auto& ptr : bl.buffers()) {
    crc = crc64nvme_word(crc, ptr.c_str(), ptr.length());
  }
  return crc;
}

uint64_t crc64nvme_combine(uint64_t crc_a, uint64_t crc_b, uint64_t len_b)
{
  return crc64nvme_comb(crc_a, crc_b, len_b);
}

} // namespace ceph
