// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/crc64nvme.h"

#include <bit>

#include "acconfig.h"
#include "arch/intel.h"
#include "arch/probe.h"
#include "include/buffer.h"
#include "include/crc32c.h"

// ISA-L's CRC-64 folding kernels for x86-64, which the crc32 object
// library assembles from src/isa-l. CRC-64/NVME is ISA-L's "rocksoft"
// polynomial, reflected. aarch64 takes madler's slicing-by-8 routine.
#if defined(__x86_64__) && defined(HAVE_NASM_X64)
#define CRC64NVME_ISAL
extern "C" uint64_t crc64_rocksoft_refl_by8(
  uint64_t crc, const unsigned char* buf, uint64_t len)
  asm("crc64_rocksoft_refl_by8");
# ifdef HAVE_NASM_X64_AVX512_VPCLMUL
extern "C" uint64_t crc64_rocksoft_refl_by16_10(
  uint64_t crc, const unsigned char* buf, uint64_t len)
  asm("crc64_rocksoft_refl_by16_10");
# endif
#endif

extern "C" {
#include "common/madler/crc64nvme.h"
}

namespace ceph {

namespace {

uint64_t crc64nvme_madler(uint64_t crc, const void* data, size_t len)
{
  // the slicing-by-8 routine loads 8 bytes at a time as a native
  // uint64_t, which its tables take to be little-endian; a big-endian
  // host takes the byte-at-a-time routine
  if constexpr (std::endian::native == std::endian::little) {
    return crc64nvme_word(crc, data, len);
  } else {
    return crc64nvme_byte(crc, data, len);
  }
}

uint64_t crc64nvme_madler_byte(uint64_t crc, const void* data, size_t len)
{
  return crc64nvme_byte(crc, data, len);
}

#ifdef CRC64NVME_ISAL
// like madler's, their seed is the previous canonical value: they invert
// it on entry and the result on exit
uint64_t crc64nvme_isal_by8(uint64_t crc, const void* data, size_t len)
{
  return crc64_rocksoft_refl_by8(
    crc, static_cast<const unsigned char*>(data), len);
}
# ifdef HAVE_NASM_X64_AVX512_VPCLMUL
uint64_t crc64nvme_isal_by16_10(uint64_t crc, const void* data, size_t len)
{
  return crc64_rocksoft_refl_by16_10(
    crc, static_cast<const unsigned char*>(data), len);
}
# endif
#endif

std::vector<std::pair<std::string_view, crc64nvme_func_t>> kernels()
{
  ceph_arch_probe();
  std::vector<std::pair<std::string_view, crc64nvme_func_t>> k;
#ifdef CRC64NVME_ISAL
# ifdef HAVE_NASM_X64_AVX512_VPCLMUL
  if (ceph_arch_intel_avx512_vpclmul) {
    k.emplace_back("isal_by16_10", crc64nvme_isal_by16_10);
  }
# endif
  if (ceph_arch_intel_pclmul && ceph_arch_intel_sse41) {
    k.emplace_back("isal_by8", crc64nvme_isal_by8);
  }
#endif
  k.emplace_back("madler", crc64nvme_madler);
  k.emplace_back("madler_byte", crc64nvme_madler_byte);
  return k;
}

crc64nvme_func_t crc64nvme_func()
{
  static const crc64nvme_func_t f = kernels().front().second;
  return f;
}

} // anonymous namespace

uint64_t crc64nvme(uint64_t crc, const void* data, size_t len)
{
  if (len == 0) {
    // madler returns 0 for a null buffer, ISA-L the seed
    return crc;
  }
  return crc64nvme_func()(crc, data, len);
}

uint64_t crc64nvme(const ceph::buffer::list& bl)
{
  const auto f = crc64nvme_func();
  uint64_t crc = 0;
  for (const auto& ptr : bl.buffers()) {
    if (ptr.length()) {
      crc = f(crc, ptr.c_str(), ptr.length());
    }
  }
  return crc;
}

uint64_t crc64nvme_crc32c(const ceph::buffer::list& bl, uint32_t* crc32c)
{
  // reaching a buffer's CRC cache lists the buffer on its own, a heap
  // node and two atomic updates of its reference count. On Zen 5 that
  // costs less than computing the CRC-32C from 3 KiB on, more under 2 KiB
  constexpr unsigned min_cached = 4096;
  const auto f = crc64nvme_func();
  uint64_t crc = 0;
  uint32_t crc32 = *crc32c;
  for (const auto& ptr : bl.buffers()) {
    if (!ptr.length()) {
      continue;
    }
    crc = f(crc, ptr.c_str(), ptr.length());
    if (ptr.length() >= min_cached) {
      // a list of this one buffer finds and leaves its CRC-32C in the
      // cache of the buffer's memory, under the range and the seed that
      // bl.crc32c() would use
      ceph::buffer::list one;
      one.append(ptr);
      crc32 = one.crc32c(crc32);
    } else {
      crc32 = ceph_crc32c(crc32,
                          reinterpret_cast<const unsigned char*>(ptr.c_str()),
                          ptr.length());
    }
  }
  *crc32c = crc32;
  return crc;
}

uint64_t crc64nvme_combine(uint64_t crc_a, uint64_t crc_b, uint64_t len_b)
{
  return crc64nvme_comb(crc_a, crc_b, len_b);
}

std::vector<std::pair<std::string_view, crc64nvme_func_t>> crc64nvme_kernels()
{
  return kernels();
}

} // namespace ceph
