// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_COMMON_SLOPPYCRCMAP_H
#define CEPH_COMMON_SLOPPYCRCMAP_H

#include "include/types.h"
#include "include/encoding.h"

#include <map>
#include <ostream>

/**
 * SloppyCRCMap
 *
 * Opportunistically track CRCs on any reads or writes that cover full
 * blocks.  Verify read results when we have CRC data available for
 * the given extent.
 */
class SloppyCRCMap {
  static const int crc_iv = 0xffffffff;

  std::map<uint64_t, uint32_t> crc_map;  // offset -> crc(-1)
  uint32_t block_size;
  uint32_t zero_crc;

public:
  SloppyCRCMap(uint32_t b=0) {
    set_block_size(b);
  }

  void set_block_size(uint32_t b) {
    block_size = b;
    //zero_crc = ceph_crc32c(0xffffffff, NULL, block_size);
    if (b) {
      bufferlist bl;
      bl.append_zero(block_size);
      zero_crc = bl.crc32c(crc_iv);
    } else {
      zero_crc = crc_iv;
    }
  }

  /// update based on a write
  void write(uint64_t offset, uint64_t len, const bufferlist& bl,
	     std::ostream *out = NULL);

  /// update based on a truncate
  void truncate(uint64_t offset);

  /// update based on a zero/punch_hole
  void zero(uint64_t offset, uint64_t len);

  /// update based on a zero/punch_hole
  void clone_range(uint64_t offset, uint64_t len, uint64_t srcoff, const SloppyCRCMap& src,
		   std::ostream *out = NULL);

  /**
   * validate a read result
   *
   * @param offset offset
   * @param length length
   * @param bl data read
   * @param err option ostream to describe errors in detail
   * @returns error count, 0 for success
   */
  int read(uint64_t offset, uint64_t len, const bufferlist& bl, std::ostream *err);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<SloppyCRCMap*>& ls);
};
WRITE_CLASS_ENCODER(SloppyCRCMap)

#endif
