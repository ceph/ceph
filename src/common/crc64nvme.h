// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstddef>
#include <cstdint>

#include "include/buffer_fwd.h"

namespace ceph {

/**
 * Canonical CRC-64/NVME (a.k.a. Rocksoft) helpers.
 *
 * Values are the canonical checksum (seed 0 yields the standard
 * result, e.g. "123456789" -> 0xae8b14860a799888); callers that need
 * the AWS/at-rest byte order (rgw's stored digests) byteswap at that
 * boundary. Incremental use: pass the previous return value as crc.
 */
uint64_t crc64nvme(uint64_t crc, const void* data, size_t len);
uint64_t crc64nvme(const ceph::buffer::list& bl);

/// checksum of the concatenation A||B given crc(A), crc(B) and len(B)
uint64_t crc64nvme_combine(uint64_t crc_a, uint64_t crc_b, uint64_t len_b);

/// checksum of len zero bytes in O(log len) time
uint64_t crc64nvme_zeros(uint64_t len);

} // namespace ceph
