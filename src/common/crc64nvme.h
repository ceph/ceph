// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "include/buffer_fwd.h"

namespace ceph {

/**
 * Canonical CRC-64/NVME (a.k.a. Rocksoft) helpers.
 *
 * Values are the canonical checksum (seed 0 yields the standard
 * result, e.g. "123456789" -> 0xae8b14860a799888); callers that need
 * the AWS/at-rest byte order (rgw's stored digests) byteswap at that
 * boundary. Incremental use: pass the previous return value as crc;
 * empty input returns crc unchanged.
 *
 * Computed with ISA-L's carry-less multiply kernels on x86-64, with
 * AVX-512 VPCLMUL or with PCLMUL and SSE4.1, when the build can assemble
 * them; else with madler's tables, eight bytes at a time on a
 * little-endian host and one at a time on a big-endian one. The values
 * are the same.
 */
uint64_t crc64nvme(uint64_t crc, const void* data, size_t len);
uint64_t crc64nvme(const ceph::buffer::list& bl);

/**
 * crc64nvme(bl), and bl.crc32c(*crc32c) into *crc32c, in one walk:
 * each buffer of bl is read for its CRC-64/NVME and then at once for
 * its CRC-32C, so that a buffer that fits in the CPU's cache is read
 * from memory once, where a pass over all of a large bl for each CRC
 * reads it twice. The CRC-32C of a buffer of 4 KiB or more goes through
 * its CRC cache, as bl.crc32c() does: a buffer whose CRC-32C is cached
 * costs only its CRC-64, and one whose is not leaves it cached, for a
 * data digest of the same bytes later. A smaller buffer leaves its cache
 * alone: reaching it costs more than computing its CRC-32C.
 */
uint64_t crc64nvme_crc32c(const ceph::buffer::list& bl, uint32_t* crc32c);

/// checksum of the concatenation A||B given crc(A), crc(B) and len(B)
uint64_t crc64nvme_combine(uint64_t crc_a, uint64_t crc_b, uint64_t len_b);

/// each implementation this host can run, by name, for tests to check
/// against each other; crc64nvme() uses the first
using crc64nvme_func_t = uint64_t (*)(uint64_t crc, const void* data,
                                      size_t len);
std::vector<std::pair<std::string_view, crc64nvme_func_t>> crc64nvme_kernels();

} // namespace ceph
