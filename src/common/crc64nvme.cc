// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/crc64nvme.h"

#include "include/buffer.h"

extern "C" {
#include "common/madler/crc64nvme.h"
}

#ifdef HAVE_ISAL_CRC64
#include "isa-l/include/crc64.h"
#endif

namespace ceph {

// isa-l provides carry-less-multiply checksumming with runtime SIMD
// dispatch but no combine, so the madler tables stay for crc64nvme_comb
static inline uint64_t crc64nvme_impl(uint64_t crc, const void* data,
				      size_t len)
{
#ifdef HAVE_ISAL_CRC64
  return crc64_rocksoft_refl(crc, static_cast<const unsigned char*>(data),
			     len);
#else
  return crc64nvme_word(crc, data, len);
#endif
}

uint64_t crc64nvme(uint64_t crc, const void* data, size_t len)
{
  return crc64nvme_impl(crc, data, len);
}

uint64_t crc64nvme(const ceph::buffer::list& bl)
{
  uint64_t crc = 0;
  for (const auto& ptr : bl.buffers()) {
    crc = crc64nvme_impl(crc, ptr.c_str(), ptr.length());
  }
  return crc;
}

uint64_t crc64nvme_combine(uint64_t crc_a, uint64_t crc_b, uint64_t len_b)
{
  return crc64nvme_comb(crc_a, crc_b, len_b);
}

} // namespace ceph
