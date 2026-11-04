// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <algorithm>
#include <cstdint>
#include <span>
#include <string_view>

#include "include/intarith.h"
#include "include/rados.h"

/**
 * osd_oob_direct_read: whether the OSD reads the data of a read that
 * goes out of band straight into a buffer of its arena, from which the
 * transport writes it with no copy.
 *
 * ECListener.h includes this header, and crimson includes ECListener.h:
 * nothing here may need the config. The OSD's config observer of the
 * option is in oob_direct_read_cacher.h.
 */
namespace ceph::osd::oob {

enum class direct_read_t : uint8_t {
  off,
  cached,   ///< and the store keeps a copy in its cache, as for any read
  nocache,  ///< and the store keeps no copy
};

inline direct_read_t to_direct_read(std::string_view v)
{
  if (v == "cached") {
    return direct_read_t::cached;
  }
  if (v == "nocache") {
    return direct_read_t::nocache;
  }
  return direct_read_t::off;
}

/// the op flags that a read into the arena gives the store under mode:
/// with nocache the store keeps no copy, even of a read whose client
/// asked for one (FADVISE_WILLNEED, which BlueStore checks first)
inline uint32_t direct_read_flags(direct_read_t mode, uint32_t flags)
{
  if (mode == direct_read_t::nocache) {
    flags &= ~CEPH_OSD_OP_FLAG_FADVISE_WILLNEED;
    flags |= CEPH_OSD_OP_FLAG_FADVISE_NOCACHE;
  }
  return flags;
}

/**
 * A read into a buffer of the arena: the store reads read_len bytes from
 * read_off into the buffer at pos, and the data asked for, len bytes
 * from off, starts pad bytes into them.
 */
struct arena_read_t {
  uint64_t off = 0;
  uint64_t len = 0;
  uint64_t read_off = 0;
  uint64_t read_len = 0;
  uint64_t pos = 0;
  uint64_t pad = 0;

  /// the bytes of the data asked for that the r bytes the store read
  /// hold: fewer than len when the object ends first
  uint64_t data_len(uint64_t r) const {
    return r > pad ? std::min(r - pad, len) : 0;
  }
};

namespace detail {
/// arena_layout() of the whole blocks of align bytes, or of the data
/// alone with align 0
inline uint64_t arena_layout_blocks(std::span<arena_read_t> reads,
                                    uint64_t align)
{
  uint64_t total = 0;
  for (auto& r : reads) {
    r.read_off = r.off;
    r.read_len = r.len;
    if (align && r.len) {
      r.read_off = p2align(r.off, align);
      r.read_len = p2roundup(r.off + r.len, align) - r.read_off;
    }
    r.pad = r.off - r.read_off;
    r.pos = total;
    total += r.read_len;
  }
  return total;
}
} // namespace detail

/**
 * Lay out in one buffer of the arena the reads of reads, whose off and
 * len say what each asks for, one after the other in that order. Each
 * reads the whole blocks of align bytes that hold its data, align being
 * the store's block size (ObjectStore::get_read_into_block_size(), a
 * power of two), and so starts on a block of the buffer. A store that
 * reads whole blocks straight into memory (BlueStore) then reads all of
 * it there, with no part of a block to copy. With align 0, each reads
 * just its data. A read of no bytes takes no room.
 *
 * The blocks hold more bytes than the data. When they would need a
 * bigger buffer than the data alone (buffer_size(), Arena::round(), is
 * the next power of two, or none past Arena::MAX), each read reads just
 * its data, back to back, and the store copies what does not land on a
 * block of the buffer: all of a read that starts inside a block. A read
 * of 4 MiB from inside a block would otherwise hold a buffer of 8 MiB,
 * and one of Arena::MAX would get no buffer at all.
 *
 * Sets the rest of each read, and returns the bytes the buffer must
 * hold. The caller reads with the result, and nothing works the places
 * out again.
 */
inline uint64_t arena_layout(std::span<arena_read_t> reads, uint64_t align,
                             uint64_t (*buffer_size)(uint64_t))
{
  uint64_t bytes = 0;
  for (const auto& r : reads) {
    bytes += r.len;
  }
  const uint64_t total = detail::arena_layout_blocks(reads, align);
  if (total != bytes && buffer_size(total) != buffer_size(bytes)) {
    return detail::arena_layout_blocks(reads, 0);
  }
  return total;
}

} // namespace ceph::osd::oob
